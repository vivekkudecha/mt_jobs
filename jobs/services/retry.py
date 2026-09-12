from django.db import transaction
from django.utils import timezone

from jobs.models import Job, JobStatus


@transaction.atomic
def promote_ready_retries(limit=100):
    now = timezone.now()

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
        count += 1

    return count