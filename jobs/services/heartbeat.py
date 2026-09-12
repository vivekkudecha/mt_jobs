"""
Job Worker Heartbeat Service

Purpose:
    Allows active workers executing long-running jobs to extend their execution lease.

Use Case:
    If a job takes 10 minutes to run, the handler calls `context.heartbeat()` periodically.
    This service extends the lease expiry timestamp by 5 minutes (`300` seconds) so the
    reconciliation reaper does not assume the worker crashed.
"""

from datetime import timedelta

from django.utils import timezone

from jobs.models import (
    AttemptStatus,
    JobAttempt,
    JobExecutionReservation,
)


# Number of seconds to extend the lease on each heartbeat signal
LEASE_SECONDS = 300


def heartbeat(attempt_id):
    """
    Extends the worker lease for an active running attempt and its tenant execution reservation.

    Args:
        attempt_id (UUID): ID of the running JobAttempt.

    Returns:
        bool: True if heartbeat successfully renewed the lease, False if attempt is not RUNNING.
    """
    now = timezone.now()
    expires_at = now + timedelta(seconds=LEASE_SECONDS)

    # Verify that the attempt is currently in RUNNING status
    attempt = JobAttempt.objects.filter(
        id=attempt_id,
        status=AttemptStatus.RUNNING,
    ).first()

    if not attempt:
        return False

    # Update attempt heartbeat and lease expiry timestamp
    JobAttempt.objects.filter(id=attempt_id).update(
        heartbeat_at=now,
        lease_expires_at=expires_at,
    )

    # Also extend the tenant capacity reservation expiry
    JobExecutionReservation.objects.filter(
        job_id=attempt.job_id,
    ).update(
        expires_at=expires_at,
    )

    return True