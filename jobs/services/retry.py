"""
Job Retry Promotion Service

Purpose:
    Scans for jobs currently waiting in `RETRY_WAIT` status whose backoff delay has expired
    and promotes them back into `WAITING` status so the dispatcher can schedule them.

Use Case:
    Executed periodically (e.g. every few seconds) by the `process_retries` Celery task.
"""

from django.db import transaction
from django.utils import timezone

from jobs.models import Job, JobStatus
from jobs.services.scheduler_state import refresh_scheduler_state


@transaction.atomic
def promote_ready_retries(limit=100):
    """
    Promotes jobs in RETRY_WAIT whose available_at timestamp is <= now() to WAITING.

    Args:
        limit (int, optional): Max number of jobs to promote in a single batch. Defaults to 100.

    Returns:
        int: Total number of jobs successfully promoted.
    """
    now = timezone.now()

    # Find jobs in RETRY_WAIT ready to run, skipping locked rows to avoid blocking
    jobs = (
        Job.objects
        .select_for_update(skip_locked=True)
        .filter(
            status=JobStatus.RETRY_WAIT,
            available_at__lte=now,
        )
        .order_by("available_at")[:limit]
    )

    count = 0
    tenant_ids = set()

    # Promote each job to WAITING and reset ready_since timestamp for fair FIFO dispatch
    for job in jobs:
        job.status = JobStatus.WAITING
        job.ready_since = now
        job.save(
            update_fields=[
                "status",
                "ready_since",
                "updated_at",
            ]
        )
        tenant_ids.add(job.tenant_id)
        count += 1

    for tenant_id in tenant_ids:
        refresh_scheduler_state(tenant_id)

    return count