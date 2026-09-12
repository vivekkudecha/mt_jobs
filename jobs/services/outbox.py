from django.db import transaction
from django.utils import timezone

from jobs.models import JobOutbox, OutboxStatus
from jobs.tasks import execute_job


def publish_pending(limit=100):
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
    event = (
        JobOutbox.objects
        .select_for_update()
        .get(id=event_id)
    )

    if event.status != OutboxStatus.PENDING:
        return

    try:
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