from jobs.models import AttemptStatus
from jobs.models import JobAttempt
from datetime import timedelta

from django.db import transaction
from django.utils import timezone

from jobs.models import (
    DeadLetterJob,
    DeadLetterReason,
    FailureType,
    Job,
    JobStatus,
)
from jobs.services.execution import release_capacity


RETRYABLE = {
    FailureType.TRANSIENT,
    FailureType.INFRASTRUCTURE,
    FailureType.TIMEOUT,
    FailureType.UNKNOWN,
}


RETRY_DELAYS = {
    1: 3,
    2: 5,
    3: 10,
}


@transaction.atomic
def finalize_failure(job_id, failure_type, message):
    job = (
        Job.objects
        .select_for_update()
        .get(id=job_id)
    )

    now = timezone.now()
    can_retry = (
        failure_type in RETRYABLE
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

        DeadLetterJob.objects.get_or_create(
            job=job,
            defaults={
                "tenant": job.tenant,
                "reason": (
                    DeadLetterReason.PERMANENT_FAILURE
                    if failure_type == FailureType.PERMANENT
                    else DeadLetterReason.RETRIES_EXHAUSTED
                ),
                "error_message": message,
                "last_attempt_number": job.attempt_count,
            },
        )

    job.last_error_message = message
    job.save()

    release_capacity(job)


@transaction.atomic
def fail_attempt(attempt_id, exc, failure_type):
    attempt = JobAttempt.objects.select_for_update().get(id=attempt_id)

    attempt.status = AttemptStatus.FAILED
    attempt.failure_type = failure_type
    attempt.error_message = str(exc)
    attempt.finished_at = timezone.now()
    attempt.save()

    finalize_failure(
        attempt.job_id,
        failure_type,
        str(exc),
    )