"""
Transactional Outbox Publisher Service

Purpose:
    Drains pending dispatch events from the `JobOutbox` table and delivers them to the Celery broker (Redis).

Use Case:
    Executed periodically by the `publish_outbox` Celery task. Decouples the database transaction
    of reserving a job from the network call to Redis.
"""

from django.db import transaction
from django.utils import timezone

from jobs.models import JobOutbox, OutboxStatus


def publish_pending(limit=100):
    """
    Finds pending outbox records and sends each to Celery.

    Args:
        limit (int, optional): Max records to process in a single batch. Defaults to 100.
    """
    event_ids = list(
        JobOutbox.objects.filter(
            status=OutboxStatus.PENDING,
            available_at__lte=timezone.now(),
        )
        .order_by("created_at")
        .values_list("id", flat=True)[:limit]
    )

    for event_id in event_ids:
        publish_event(event_id)


@transaction.atomic
def publish_event(event_id):
    """
    Atomically locks an outbox event, enqueues the Celery `execute_job` task, and updates status to PUBLISHED.

    Args:
        event_id (UUID): ID of the JobOutbox event to publish.
    """
    event = (
        JobOutbox.objects
        .select_for_update()
        .get(id=event_id)
    )

    if event.status != OutboxStatus.PENDING:
        return

    try:
        from jobs.tasks import execute_job

        # Enqueue job to Celery worker pool
        execute_job.delay(str(event.job_id))

        event.status = OutboxStatus.PUBLISHED
        event.published_at = timezone.now()

    except Exception as exc:
        event.publish_attempts += 1
        event.last_error = str(exc)
        raise

    finally:
        event.save(
            update_fields=[
                "status",
                "published_at",
                "publish_attempts",
                "last_error",
                "updated_at",
            ]
        )