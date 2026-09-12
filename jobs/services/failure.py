"""
Job Failure and Retry Decision Service

Purpose:
    Encapsulates the decision logic for handling job execution failures. Determines whether a failure
    is transient (qualifying for exponential backoff retry) or permanent (routing immediately to DeadLetterJob).

Use Case:
    Used by execution service, reaper/reconciliation, and timeout detectors when an attempt fails.
"""

from datetime import timedelta

from django.db import transaction
from django.utils import timezone

from jobs.models import (
    AttemptStatus,
    DeadLetterJob,
    DeadLetterReason,
    FailureType,
    Job,
    JobAttempt,
    JobStatus,
)
from jobs.services.execution import release_capacity


# Set of failure types eligible for automatic retries
RETRYABLE = {
    FailureType.TRANSIENT,
    FailureType.INFRASTRUCTURE,
    FailureType.TIMEOUT,
    FailureType.UNKNOWN,
}

# Retry backoff schedule mapping attempt_count -> delay in seconds
RETRY_DELAYS = {
    1: 3,
    2: 5,
    3: 10,
}


@transaction.atomic
def finalize_failure(job_id, failure_type, message):
    """
    Decides the final status of a failing job: sets RETRY_WAIT if eligible, or FAILED_FINAL with DLQ record.

    Args:
        job_id (UUID): The failing job ID.
        failure_type (FailureType): Classification of the error.
        message (str): Error message description.
    """
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
        # Calculate backoff delay
        delay = RETRY_DELAYS[job.attempt_count]

        job.status = JobStatus.RETRY_WAIT
        job.available_at = now + timedelta(seconds=delay)
        job.ready_since = job.available_at

    else:
        # Mark permanently failed and write to DLQ
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

    # Free the tenant concurrency reservation
    release_capacity(job)


@transaction.atomic
def fail_attempt(attempt_id, exc, failure_type):
    """
    Convenience function to mark an individual JobAttempt as FAILED and trigger finalize_failure on the Job.

    Args:
        attempt_id (UUID): ID of the failing attempt.
        exc (Exception): The exception that caused failure.
        failure_type (FailureType): Classification of the failure.
    """
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