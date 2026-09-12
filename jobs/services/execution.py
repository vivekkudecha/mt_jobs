from datetime import timedelta
import socket
import traceback

from django.db import transaction
from django.utils import timezone

from jobs.domain.exceptions import PermanentJobError, RetryableJobError
from jobs.models import (
    Job,
    JobAttempt,
    JobStatus,
)

from jobs.handlers import get_handler
from jobs.models.attempt import AttemptStatus, FailureType
from jobs.models.dead_letter import DeadLetterJob, DeadLetterReason
from jobs.models.reservation import JobExecutionReservation
from jobs.models.scheduler_state import TenantSchedulerState
from jobs.handlers.context import JobContext
from jobs.services.overlap import release_overlaps


def execute(job_id):
    attempt = claim_job(job_id)

    if not attempt:
        return

    try:
        context = JobContext(attempt.job, attempt)
        handler = get_handler(attempt.job.job_type)
        result = handler.execute(attempt.job.payload, context)
        complete_job(attempt.id, result)

    except RetryableJobError as exc:
        fail_job(
            attempt.id,
            exc,
            FailureType.TRANSIENT,
        )

    except PermanentJobError as exc:
        fail_job(
            attempt.id,
            exc,
            FailureType.PERMANENT,
        )

    except Exception as exc:
        fail_job(
            attempt.id,
            exc,
            FailureType.UNKNOWN,
        )


@transaction.atomic
def claim_job(job_id):
    now = timezone.now()

    job = (
        Job.objects
        .select_for_update()
        .filter(id=job_id)
        .first()
    )

    if not job or job.status != JobStatus.QUEUED:
        return None

    job.attempt_count += 1
    job.status = JobStatus.EXECUTING
    job.started_at = job.started_at or now

    job.save(
        update_fields=[
            "status",
            "attempt_count",
            "started_at",
            "updated_at",
        ]
    )

    return JobAttempt.objects.create(
        job=job,
        attempt_number=job.attempt_count,
        worker_id=socket.gethostname(),
        started_at=now,
        heartbeat_at=now,
        lease_expires_at=now + timedelta(minutes=5),
    )

@transaction.atomic
def complete_job(attempt_id, result):
    now = timezone.now()

    attempt = JobAttempt.objects.select_for_update().get(id=attempt_id)
    job = Job.objects.select_for_update().get(id=attempt.job_id)

    attempt.status = AttemptStatus.SUCCEEDED
    attempt.finished_at = now
    attempt.save(update_fields=["status", "finished_at"])

    job.status = JobStatus.SUCCEEDED
    job.result = result
    job.completed_at = now
    job.save(
        update_fields=[
            "status",
            "result",
            "completed_at",
            "updated_at",
        ]
    )

    release_capacity(job)


RETRY_DELAYS = {
    1: 3,
    2: 5,
    3: 10,
}


@transaction.atomic
def fail_job(attempt_id, exc, failure_type):
    now = timezone.now()

    attempt = JobAttempt.objects.select_for_update().get(id=attempt_id)
    job = (
        Job.objects
        .select_for_update()
        .get(id=attempt.job_id)
    )

    attempt.status = AttemptStatus.FAILED
    attempt.failure_type = failure_type
    attempt.error_message = str(exc)
    attempt.traceback = traceback.format_exc()
    attempt.finished_at = now
    attempt.save()

    can_retry = (
        failure_type in {
            FailureType.TRANSIENT,
            FailureType.INFRASTRUCTURE,
            FailureType.TIMEOUT,
            FailureType.UNKNOWN,
        }
        and job.attempt_count in RETRY_DELAYS
    )

    if can_retry:
        delay = RETRY_DELAYS[job.attempt_count]

        job.status = JobStatus.RETRY_WAIT
        job.available_at = now + timedelta(seconds=delay)
        job.ready_since = job.available_at
    else:
        job.status = JobStatus.FAILED_FINAL
        job.completed_at = now

        reason = (
            DeadLetterReason.PERMANENT_FAILURE
            if failure_type == FailureType.PERMANENT
            else DeadLetterReason.RETRIES_EXHAUSTED
        )

        DeadLetterJob.objects.get_or_create(
            job=job,
            defaults={
                "tenant": job.tenant,
                "reason": reason,
                "error_message": str(exc),
                "last_attempt_number": job.attempt_count,
            },
        )

    job.last_error_message = str(exc)
    job.save()

    release_capacity(job)


def retry_delay(job):
    return RETRY_DELAYS.get(job.attempt_count, 10)


def release_capacity(job):
    JobExecutionReservation.objects.filter(job=job).delete()
    release_overlaps(job)

    state = TenantSchedulerState.objects.select_for_update().get(
        tenant=job.tenant
    )

    if state.active_reservations:
        state.active_reservations -= 1
        state.save(
            update_fields=["active_reservations", "updated_at"]
        )