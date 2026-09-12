"""
Dead Letter Queue (DLQ) Model

Purpose:
    Holds jobs that have permanently failed or exhausted all retry attempts for forensic analysis,
    manual intervention, and safe replaying.

Use Case:
    When a job fails after 3 retries, rather than silently disappearing, it is recorded in `dead_letter_jobs`.
    Operations engineers can inspect the error details and click Replay via the API.
"""

import uuid

from django.db import models


class DeadLetterReason(models.TextChoices):
    """Why a job was routed to the dead-letter queue."""
    RETRIES_EXHAUSTED = "RETRIES_EXHAUSTED", "Retries Exhausted"        # Maximum attempt count reached
    PERMANENT_FAILURE = "PERMANENT_FAILURE", "Permanent Failure"        # PermanentJobError was raised
    HANDLER_NOT_FOUND = "HANDLER_NOT_FOUND", "Handler Not Found"        # No registered code handler for job_type
    INVALID_CONFIGURATION = "INVALID_CONFIGURATION", "Invalid Configuration"  # Missing tenant/job config
    POISON_JOB = "POISON_JOB", "Poison Job"                            # Crashing worker repeatedly


class DeadLetterStatus(models.TextChoices):
    """Current triage state of the dead-letter record."""
    OPEN = "OPEN", "Open"              # Unresolved; awaiting developer/ops action
    REPLAYED = "REPLAYED", "Replayed"  # Re-submitted as a new job
    DISMISSED = "DISMISSED", "Dismissed"  # Closed without replaying (e.g. invalid request)


class DeadLetterJob(models.Model):
    """
    Dead-letter record holding diagnostic context for failed jobs.
    """
    id = models.UUIDField(
        primary_key=True,
        default=uuid.uuid4,
        editable=False,
    )

    # One-to-one link to the failed Job
    job = models.OneToOneField(
        "jobs.Job",
        on_delete=models.CASCADE,
        related_name="dead_letter",
    )

    # Multi-tenant scoping
    tenant = models.ForeignKey(
        "tenants.Tenant",
        on_delete=models.PROTECT,
        related_name="dead_letter_jobs",
    )

    reason = models.CharField(
        max_length=64,
        choices=DeadLetterReason.choices,
    )

    status = models.CharField(
        max_length=16,
        choices=DeadLetterStatus.choices,
        default=DeadLetterStatus.OPEN,
    )

    # Error diagnostics
    error_code = models.CharField(
        max_length=100,
        null=True,
        blank=True,
    )

    error_message = models.TextField(
        null=True,
        blank=True,
    )

    last_attempt_number = models.PositiveSmallIntegerField(
        null=True,
        blank=True,
    )

    metadata = models.JSONField(
        default=dict,
        blank=True,
        help_text="Diagnostic context captured at the moment of failure.",
    )

    # Link to newly created Job when replayed
    replayed_job = models.ForeignKey(
        "jobs.Job",
        on_delete=models.SET_NULL,
        related_name="replayed_from_dead_letters",
        null=True,
        blank=True,
    )

    # Timestamps
    created_at = models.DateTimeField(
        auto_now_add=True,
    )

    replayed_at = models.DateTimeField(
        null=True,
        blank=True,
    )

    dismissed_at = models.DateTimeField(
        null=True,
        blank=True,
    )

    class Meta:
        db_table = "dead_letter_jobs"

        indexes = [
            models.Index(
                fields=[
                    "tenant",
                    "status",
                    "created_at",
                ],
                name="idx_dlq_tenant_status",
            ),
            models.Index(
                fields=[
                    "status",
                    "created_at",
                ],
                name="idx_dlq_status_created",
            ),
        ]

    def __str__(self) -> str:
        return f"DLQ:{self.job_id} ({self.status} - {self.reason})"