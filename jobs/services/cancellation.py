"""
Job Cancellation Service

Purpose:
    Handles safe job cancellation across all lifecycle states while keeping tenant concurrency balanced.

Use Case:
    When a user or admin requests to cancel a job via API:
    - If WAITING / RETRY_WAIT -> directly marks CANCELLED.
    - If QUEUED -> marks CANCELLED and immediately releases the reserved tenant concurrency slot.
    - If EXECUTING -> marks CANCEL_REQUESTED so the worker aborts gracefully.
    - If already finished (SUCCEEDED / FAILED_FINAL / CANCELLED) -> no-op.
"""

from django.db import transaction
from django.utils import timezone

from jobs.models import Job, JobStatus
from jobs.services.execution import release_capacity


@transaction.atomic
def cancel_job(job_id):
    """
    Cancels a job according to its current lifecycle state.

    Args:
        job_id (UUID): ID of the job to cancel.

    Returns:
        Job: The updated Job instance.
    """
    job = Job.objects.select_for_update().get(id=job_id)

    # 1. Ignore terminal states
    if job.status in {JobStatus.SUCCEEDED, JobStatus.FAILED_FINAL, JobStatus.CANCELLED}:
        return job

    # 2. Cancel non-dispatched waiting jobs
    if job.status in {JobStatus.WAITING, JobStatus.RETRY_WAIT}:
        job.status = JobStatus.CANCELLED
        job.cancelled_at = timezone.now()

    # 3. Cancel queued jobs and immediately free their reserved capacity slot
    elif job.status == JobStatus.QUEUED:
        job.status = JobStatus.CANCELLED
        job.cancelled_at = timezone.now()
        release_capacity(job)

    # 4. Signal executing jobs to terminate at the next checkpoint
    elif job.status == JobStatus.EXECUTING:
        job.status = JobStatus.CANCEL_REQUESTED

    job.save(
        update_fields=[
            "status",
            "cancelled_at",
            "updated_at",
        ]
    )

    return job