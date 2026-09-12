"""
Tenant Scheduler State Refresh Service

Purpose:
    Recalculates and caches the scheduling readiness metrics (`has_ready_work`, `highest_ready_priority`,
    and `next_ready_at`) in `TenantSchedulerState`.

Use Case:
    Called whenever a new job is created, retried, or replayed to immediately update the scheduler's index.
"""

from django.utils import timezone

from jobs.models import Job, JobStatus, TenantSchedulerState


def refresh_scheduler_state(tenant_id):
    """
    Recalculates and persists cached readiness indicators for the given tenant.

    Args:
        tenant_id (int): ID of the tenant whose scheduler state should be refreshed.
    """
    now = timezone.now()

    # Query ready waiting jobs for this tenant
    jobs = Job.objects.filter(
        tenant_id=tenant_id,
        status=JobStatus.WAITING,
        available_at__lte=now,
    )

    state, _ = TenantSchedulerState.objects.get_or_create(
        tenant_id=tenant_id
    )

    # Find highest priority ready job
    first = jobs.order_by("priority", "ready_since").first()

    state.has_ready_work = bool(first)
    state.highest_ready_priority = first.priority if first else None
    state.next_ready_at = (
        Job.objects.filter(
            tenant_id=tenant_id,
            status=JobStatus.WAITING,
        )
        .order_by("available_at")
        .values_list("available_at", flat=True)
        .first()
    )

    state.save(
        update_fields=[
            "has_ready_work",
            "highest_ready_priority",
            "next_ready_at",
            "updated_at",
        ]
    )