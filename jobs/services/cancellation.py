from django.db import transaction
from django.utils import timezone

from jobs.models import Job, JobStatus
from jobs.services.execution import release_capacity


@transaction.atomic
def cancel_job(job_id):
    job = Job.objects.select_for_update().get(id=job_id)

    if job.status in {JobStatus.SUCCEEDED, JobStatus.FAILED_FINAL}:
        return job

    if job.status == JobStatus.CANCELLED:
        return job

    if job.status in {JobStatus.WAITING, JobStatus.RETRY_WAIT}:
        job.status = JobStatus.CANCELLED
        job.cancelled_at = timezone.now()

    elif job.status == JobStatus.QUEUED:
        job.status = JobStatus.CANCELLED
        job.cancelled_at = timezone.now()
        release_capacity(job)

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