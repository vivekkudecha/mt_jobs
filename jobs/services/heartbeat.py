from datetime import timedelta

from django.utils import timezone

from jobs.models import (
    AttemptStatus,
    JobAttempt,
    JobExecutionReservation,
)


LEASE_SECONDS = 300


def heartbeat(attempt_id):
    now = timezone.now()
    expires_at = now + timedelta(seconds=LEASE_SECONDS)

    attempt = JobAttempt.objects.filter(
        id=attempt_id,
        status=AttemptStatus.RUNNING,
    ).first()

    if not attempt:
        return False

    JobAttempt.objects.filter(id=attempt_id).update(
        heartbeat_at=now,
        lease_expires_at=expires_at,
    )

    JobExecutionReservation.objects.filter(
        job_id=attempt.job_id,
    ).update(
        expires_at=expires_at,
    )

    return True