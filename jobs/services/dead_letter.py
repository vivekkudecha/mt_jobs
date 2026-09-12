"""
Dead Letter Replay Service

Purpose:
    Allows failed jobs in the Dead Letter Queue (DLQ) to be replayed as brand-new jobs.

Use Case:
    When an outage or bug in production is fixed, an engineer calls `replay_dead_letter(dlq_id)`
    via the REST API. This creates a fresh Job with the original payload, updates the DLQ entry,
    and refreshes the tenant's scheduler state so the job executes immediately.
"""

from django.db import transaction
from django.utils import timezone

from jobs.models import DeadLetterJob, DeadLetterStatus, Job
from jobs.services.scheduler_state import refresh_scheduler_state


@transaction.atomic
def replay_dead_letter(dead_letter_id):
    """
    Replays a dead-lettered job by creating a new Job instance from the original configuration.

    Args:
        dead_letter_id (UUID): Primary key of the DeadLetterJob record.

    Returns:
        Job: The newly created Job instance.
    """
    now = timezone.now()

    # Step 1: Lock the dead letter record
    dlq = (
        DeadLetterJob.objects
        .select_for_update()
        .select_related("job")
        .get(id=dead_letter_id)
    )

    source = dlq.job

    # Step 2: Clone original job attributes into a new Job in WAITING state
    new_job = Job.objects.create(
        tenant=source.tenant,
        job_type=source.job_type,
        priority=source.priority,
        payload=source.payload,
        available_at=now,
        ready_since=now,
    )

    # Step 3: Update DLQ record to mark it as REPLAYED and link new job
    dlq.status = DeadLetterStatus.REPLAYED
    dlq.replayed_job = new_job
    dlq.replayed_at = now
    dlq.save(
        update_fields=[
            "status",
            "replayed_job",
            "replayed_at",
        ]
    )

    # Step 4: Notify the scheduler that this tenant has ready work
    refresh_scheduler_state(new_job.tenant_id)

    return new_job