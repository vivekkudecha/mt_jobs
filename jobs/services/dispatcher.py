"""
Job Dispatcher Service

Purpose:
    Selects the next eligible job for a tenant, checks concurrency limits, acquires resource locks,
    reserves execution capacity, and creates outbox events for Celery.

Use Case:
    When the scheduler runs, it calls `dispatch_next(tenant_id)`. If the tenant is under their max
    concurrency limit (e.g. max 2 running jobs), the highest priority ready job is moved to `QUEUED`
    and dispatched.
"""

from datetime import timedelta

from django.db import transaction
from django.utils import timezone

from jobs.models import (
    Job,
    JobExecutionReservation,
    JobOutbox,
    JobStatus,
    TenantSchedulerState,
)
from jobs.services.overlap import acquire_overlap


# Maximum candidate jobs inspected per dispatch cycle to keep lock times fast
CANDIDATE_LIMIT = 10


@transaction.atomic
def dispatch_next(tenant_id):
    """
    Attempts to dispatch the next ready job for the given tenant.

    Workflow:
        1. Locks the TenantSchedulerState row (`select_for_update`) to prevent race conditions.
        2. Checks if active_reservations < max_concurrent_jobs.
        3. Queries top candidate WAITING jobs ordered by (priority ASC, ready_since ASC).
        4. Tries to acquire overlap lock if requested in payload.
        5. Reserves the job and writes to JobOutbox.

    Args:
        tenant_id (int): ID of the tenant to dispatch work for.

    Returns:
        Job or None: The dispatched Job instance, or None if no work could be dispatched.
    """
    now = timezone.now()

    # Step 1: Lock the tenant scheduler state to serialize dispatching for this tenant
    state = (
        TenantSchedulerState.objects
        .select_for_update()
        .select_related("tenant")
        .get(tenant_id=tenant_id)
    )

    # Step 2: Enforce tenant concurrency limits (e.g., max 2 running jobs)
    if state.active_reservations >= state.tenant.max_concurrent_jobs:
        return None

    # Step 3: Fetch candidate jobs in priority order (1=Highest, 5=Lowest), skipping locked rows
    candidates = (
        Job.objects
        .select_for_update(skip_locked=True)
        .filter(
            tenant_id=tenant_id,
            status=JobStatus.WAITING,
            available_at__lte=now,
        )
        .order_by("priority", "ready_since")[:CANDIDATE_LIMIT]
    )

    # Step 4: Find the first candidate that can acquire necessary resource locks
    for job in candidates:
        overlap_key = job.payload.get("overlap_key")

        # If job requires a resource lock and it is already locked by another job, skip to next candidate
        if overlap_key and not acquire_overlap(job, overlap_key):
            continue

        # Step 5: Reserve and enqueue the job
        _reserve_job(job, state, now)
        return job

    return None


def _reserve_job(job, state, now):
    """
    Helper function to record execution reservation, advance job status to QUEUED,
    and create a transactional outbox entry.
    """
    # Create reservation slot with 5-minute safety lease
    JobExecutionReservation.objects.create(
        job=job,
        tenant=state.tenant,
        expires_at=now + timedelta(minutes=5),
    )

    # Update job status to QUEUED
    job.status = JobStatus.QUEUED
    job.queued_at = now
    job.save(
        update_fields=["status", "queued_at", "updated_at"]
    )

    # Increment tenant active reservation count and update timestamp
    state.active_reservations += 1
    state.last_dispatched_at = now
    state.save(
        update_fields=[
            "active_reservations",
            "last_dispatched_at",
            "updated_at",
        ]
    )

    # Record outbox entry for asynchronous publishing to Celery
    JobOutbox.objects.create(
        job=job,
        available_at=now,
    )