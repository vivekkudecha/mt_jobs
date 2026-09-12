import uuid

from django.db import models


class OutboxEventType(models.TextChoices):
    DISPATCH_JOB = "DISPATCH_JOB", "Dispatch Job"


class OutboxStatus(models.TextChoices):
    PENDING = "PENDING", "Pending"
    PUBLISHED = "PUBLISHED", "Published"


class JobOutbox(models.Model):
    id = models.UUIDField(
        primary_key=True,
        default=uuid.uuid4,
        editable=False,
    )

    job = models.ForeignKey(
        "jobs.Job",
        on_delete=models.CASCADE,
        related_name="outbox_events",
    )

    event_type = models.CharField(
        max_length=32,
        choices=OutboxEventType.choices,
        default=OutboxEventType.DISPATCH_JOB,
    )

    status = models.CharField(
        max_length=16,
        choices=OutboxStatus.choices,
        default=OutboxStatus.PENDING,
    )

    payload = models.JSONField(
        default=dict,
        blank=True,
    )

    publish_attempts = models.PositiveIntegerField(
        default=0,
    )

    available_at = models.DateTimeField()

    published_at = models.DateTimeField(
        null=True,
        blank=True,
    )

    last_error = models.TextField(
        null=True,
        blank=True,
    )

    created_at = models.DateTimeField(
        auto_now_add=True,
    )

    updated_at = models.DateTimeField(
        auto_now=True,
    )

    class Meta:
        db_table = "job_outbox"

        indexes = [
            models.Index(
                fields=[
                    "status",
                    "available_at",
                    "created_at",
                ],
                name="idx_outbox_pending",
            ),
            models.Index(
                fields=[
                    "job",
                    "status",
                ],
                name="idx_outbox_job_status",
            ),
        ]

    def __str__(self) -> str:
        return f"{self.event_type}:{self.job_id}"