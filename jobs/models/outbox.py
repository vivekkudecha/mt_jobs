"""
Transactional Outbox Model

Purpose:
    Implements the Transactional Outbox Pattern to guarantee reliable message delivery from PostgreSQL
    to the Celery / Redis message broker.

Use Case:
    When a job is queued inside a database transaction, an outbox row is also created in the same transaction.
    If the worker network blips, the message is never lost—a background publisher polls this table and delivers it.
"""

import uuid

from django.db import models


class OutboxEventType(models.TextChoices):
    """Types of outbox message events."""
    DISPATCH_JOB = "DISPATCH_JOB", "Dispatch Job"


class OutboxStatus(models.TextChoices):
    """Delivery status of the outbox message."""
    PENDING = "PENDING", "Pending"          # Waiting to be sent to Celery/Redis
    PUBLISHED = "PUBLISHED", "Published"    # Successfully enqueued to Celery/Redis


class JobOutbox(models.Model):
    """
    Outbox event table ensuring at-least-once message dispatching.
    """
    id = models.UUIDField(
        primary_key=True,
        default=uuid.uuid4,
        editable=False,
    )

    # Link to the Job being dispatched
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

    # Retry and tracking
    publish_attempts = models.PositiveIntegerField(
        default=0,
        help_text="Number of attempts made by the outbox publisher to send to Redis.",
    )

    available_at = models.DateTimeField(
        help_text="Earliest time this event should be published.",
    )

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
            # High-performance index for the outbox polling daemon
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
        return f"{self.event_type}:{self.job_id} ({self.status})"