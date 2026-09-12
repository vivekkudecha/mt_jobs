from django.db import transaction
from django.utils import timezone

from jobs.models import DeadLetterJob, DeadLetterStatus, Job
from jobs.services.scheduler_state import refresh_scheduler_state


@transaction.atomic
def replay_dead_letter(dead_letter_id):
    dlq = (
        DeadLetterJob.objects
        .select_for_update()
        .select_related("job")
        .get(id=dead_letter_id)
    )

    source = dlq.job

    new_job = Job.objects.create(
        tenant=source.tenant,
        job_type=source.job_type,
        priority=source.priority,
        payload=source.payload,
        available_at=timezone.now(),
        ready_since=timezone.now(),
    )

    dlq.status = DeadLetterStatus.REPLAYED
    dlq.replayed_job = new_job
    dlq.replayed_at = timezone.now()
    dlq.save(
        update_fields=[
            "status",
            "replayed_job",
            "replayed_at",
        ]
    )

    refresh_scheduler_state(new_job.tenant_id)

    return new_job