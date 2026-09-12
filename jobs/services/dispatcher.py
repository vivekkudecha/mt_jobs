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


CANDIDATE_LIMIT = 10


@transaction.atomic
def dispatch_next(tenant_id):
    now = timezone.now()

    state = (
        TenantSchedulerState.objects
        .select_for_update()
        .select_related("tenant")
        .get(tenant_id=tenant_id)
    )

    if state.active_reservations >= state.tenant.max_concurrent_jobs:
        return None

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

    for job in candidates:
        overlap_key = job.payload.get("overlap_key")

        if overlap_key and not acquire_overlap(job, overlap_key):
            continue

        _reserve_job(job, state, now)
        return job

    return None


def _reserve_job(job, state, now):
    JobExecutionReservation.objects.create(
        job=job,
        tenant=state.tenant,
        expires_at=now + timedelta(minutes=5),
    )

    job.status = JobStatus.QUEUED
    job.queued_at = now
    job.save(
        update_fields=["status", "queued_at", "updated_at"]
    )

    state.active_reservations += 1
    state.last_dispatched_at = now
    state.save(
        update_fields=[
            "active_reservations",
            "last_dispatched_at",
            "updated_at",
        ]
    )

    JobOutbox.objects.create(
        job=job,
        available_at=now,
    )