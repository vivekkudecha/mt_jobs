"""
Worker Reconciliation and Self-Healing Reaper Service

Purpose:
    Detects abandoned/zombie jobs where the worker died, lost power, or disconnected,
    and recovers stuck QUEUED jobs where dispatch messages failed to reach workers.

Use Case:
    If a cloud VM running a Celery worker terminates abruptly during execution, its heartbeat
    lease will expire (`lease_expires_at < now`). This service detects the expired lease,
    marks the attempt as `ABANDONED`, and triggers retry recovery.
"""

from django.db import transaction
from django.utils import timezone

from jobs.models import (
    AttemptStatus,
    FailureType,
    JobAttempt,
    JobExecutionReservation,
    JobStatus,
)
from jobs.services.failure import finalize_failure


def reconcile_expired(limit=100):
    """
    Finds and reconciles running attempts whose worker leases have expired,
    and recovers queued jobs whose reservation leases expired.

    Args:
        limit (int, optional): Maximum records to process per run. Defaults to 100.
    """
    now = timezone.now()

    # Step 1: Query IDs of running attempts with expired worker leases
    attempt_ids = list(
        JobAttempt.objects.filter(
            status=AttemptStatus.RUNNING,
            lease_expires_at__lt=now,
        ).values_list("id", flat=True)[:limit]
    )

    for attempt_id in attempt_ids:
        reconcile_attempt(attempt_id)

    # Step 2: Recover stuck queued jobs
    reconcile_queued(now, limit)


@transaction.atomic
def reconcile_attempt(attempt_id):
    """
    Marks an expired attempt as ABANDONED (INFRASTRUCTURE failure) and delegates failure retry handling.

    Args:
        attempt_id (UUID): ID of the expired JobAttempt.
    """
    attempt = JobAttempt.objects.select_for_update().get(id=attempt_id)

    if attempt.status != AttemptStatus.RUNNING:
        return

    # Mark the attempt abandoned due to infrastructure crash
    attempt.status = AttemptStatus.ABANDONED
    attempt.finished_at = timezone.now()
    attempt.failure_type = FailureType.INFRASTRUCTURE
    attempt.save()

    # Trigger failure recovery on the parent Job
    finalize_failure(
        attempt.job_id,
        FailureType.INFRASTRUCTURE,
        "Worker lease expired",
    )


def reconcile_queued(now, limit):
    """
    Recovers jobs stuck in QUEUED status whose execution reservations expired without being claimed.
    """
    reservations = (
        JobExecutionReservation.objects
        .filter(
            expires_at__lt=now,
            job__status=JobStatus.QUEUED,
        )
        .select_related("job")[:limit]
    )

    for reservation in reservations:
        recover_queued(reservation.job_id)


@transaction.atomic
def recover_queued(job_id):
    """
    Reverts a stuck QUEUED job back to WAITING status and clears its stale reservation.

    Args:
        job_id (UUID): ID of the stuck Job.
    """
    from jobs.models import Job
    from jobs.services.execution import release_capacity
    from jobs.services.scheduler_state import refresh_scheduler_state

    job = Job.objects.select_for_update().get(id=job_id)

    if job.status != JobStatus.QUEUED:
        return

    # Return job to WAITING pool
    job.status = JobStatus.WAITING
    job.ready_since = timezone.now()
    job.save(
        update_fields=[
            "status",
            "ready_since",
            "updated_at",
        ]
    )

    # Release reservation slot and decrement active reservations
    release_capacity(job)
    refresh_scheduler_state(job.tenant_id)