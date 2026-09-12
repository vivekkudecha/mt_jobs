from jobs.services.failure import finalize_failure
from django.db import transaction
from django.utils import timezone

from jobs.models import (
    AttemptStatus,
    FailureType,
    JobAttempt,
    JobExecutionReservation,
    JobStatus,
)
from jobs.services.execution import fail_job


def reconcile_expired(limit=100):
    now = timezone.now()

    attempt_ids = list(
        JobAttempt.objects.filter(
            status=AttemptStatus.RUNNING,
            lease_expires_at__lt=now,
        ).values_list("id", flat=True)[:limit]
    )

    for attempt_id in attempt_ids:
        reconcile_attempt(attempt_id)

    reconcile_queued(now, limit)


@transaction.atomic
def reconcile_attempt(attempt_id):
    attempt = JobAttempt.objects.select_for_update().get(id=attempt_id)

    if attempt.status != AttemptStatus.RUNNING:
        return

    attempt.status = AttemptStatus.ABANDONED
    attempt.finished_at = timezone.now()
    attempt.failure_type = FailureType.INFRASTRUCTURE
    attempt.save()

    finalize_failure(
        attempt.job_id,
        FailureType.INFRASTRUCTURE,
        "Worker lease expired",
    )


def reconcile_queued(now, limit):
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
    from jobs.models import Job

    job = Job.objects.select_for_update().get(id=job_id)

    if job.status != JobStatus.QUEUED:
        return

    job.status = JobStatus.WAITING
    job.ready_since = timezone.now()
    job.save(
        update_fields=[
            "status",
            "ready_since",
            "updated_at",
        ]
    )

    JobExecutionReservation.objects.filter(job=job).delete()