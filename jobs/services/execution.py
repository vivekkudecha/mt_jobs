"""
Job Execution Service

Purpose:
    The core execution engine running inside Celery workers. Claims jobs, executes registered handlers,
    handles retries and DLQ routing, and releases tenant capacity slots upon completion.

Use Case:
    When a Celery worker processes `execute_job(job_id)`, this service manages the full lifecycle:
    QUEUED -> EXECUTING -> SUCCEEDED / RETRY_WAIT / FAILED_FINAL.
"""

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


# Backoff delays in seconds for attempts 1, 2, and 3
RETRY_DELAYS = {
    1: 3,
    2: 5,
    3: 10,
}


def execute(job_id):
    """
    Main entry point invoked by the Celery task to execute a claimed job.

    Args:
        job_id (str or UUID): ID of the job to execute.
    """
    # Step 1: Claim the job atomically in the database
    attempt = claim_job(job_id)

    # If job is already cancelled or claimed by another worker, exit cleanly
    if not attempt:
        return

    # Step 2: Look up registered handler and run the business logic
    try:
        context = JobContext(attempt.job, attempt)
        handler = get_handler(attempt.job.job_type)
        result = handler.execute(attempt.job.payload, context)

        # Step 3a: Success path
        complete_job(attempt.id, result)

    except RetryableJobError as exc:
        # Step 3b: Transient failure path (retryable)
        fail_job(
            attempt.id,
            exc,
            FailureType.TRANSIENT,
        )

    except PermanentJobError as exc:
        # Step 3c: Fatal permanent failure path (non-retryable, moves to DLQ)
        fail_job(
            attempt.id,
            exc,
            FailureType.PERMANENT,
        )

    except Exception as exc:
        # Step 3d: Unexpected exception path
        fail_job(
            attempt.id,
            exc,
            FailureType.UNKNOWN,
        )


@transaction.atomic
def claim_job(job_id):
    """
    Atomically transitions a job from QUEUED to EXECUTING and creates a JobAttempt record.

    Returns:
        JobAttempt or None: The created attempt record, or None if job is not in QUEUED state.
    """
    now = timezone.now()

    # Lock the job row
    job = (
        Job.objects
        .select_for_update()
        .filter(id=job_id)
        .first()
    )

    if not job or job.status != JobStatus.QUEUED:
        return None

    # Update job state
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

    # Create audit attempt record with 5-minute initial worker lease
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
    """
    Marks a job attempt and parent Job as SUCCEEDED, saves result, and frees capacity.

    Args:
        attempt_id (UUID): ID of the successful JobAttempt.
        result (dict): The returned output payload from the handler.
    """
    now = timezone.now()

    attempt = JobAttempt.objects.select_for_update().get(id=attempt_id)
    job = Job.objects.select_for_update().get(id=attempt.job_id)

    # Update attempt status
    attempt.status = AttemptStatus.SUCCEEDED
    attempt.finished_at = now
    attempt.save(update_fields=["status", "finished_at"])

    # Update job status and output result
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

    # Release reservation and overlap locks
    release_capacity(job)


@transaction.atomic
def fail_job(attempt_id, exc, failure_type):
    """
    Handles failure for a job attempt: schedules retry backoff if eligible, or moves to DLQ.

    Args:
        attempt_id (UUID): ID of the failing JobAttempt.
        exc (Exception): The caught exception instance.
        failure_type (FailureType): Classification of the failure.
    """
    now = timezone.now()

    attempt = JobAttempt.objects.select_for_update().get(id=attempt_id)
    job = (
        Job.objects
        .select_for_update()
        .get(id=attempt.job_id)
    )

    # Record failure diagnostics on the attempt record
    attempt.status = AttemptStatus.FAILED
    attempt.failure_type = failure_type
    attempt.error_message = str(exc)
    attempt.traceback = traceback.format_exc()
    attempt.finished_at = now
    attempt.save()

    # Determine whether job can be retried
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
        # Schedule next retry with exponential backoff delay
        delay = RETRY_DELAYS[job.attempt_count]
        job.status = JobStatus.RETRY_WAIT
        job.available_at = now + timedelta(seconds=delay)
        job.ready_since = job.available_at
    else:
        # Retries exhausted or permanent failure: Route to Dead Letter Queue
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

    # Release the tenant concurrency reservation so another job can run
    release_capacity(job)


def retry_delay(job):
    """Returns backoff delay in seconds for the given job's attempt count."""
    return RETRY_DELAYS.get(job.attempt_count, 10)


def release_capacity(job):
    """
    Frees the tenant's execution reservation slot and any held resource overlap locks.
    """
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