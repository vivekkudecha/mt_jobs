import uuid

from django.db import models


class DeadLetterReason(models.TextChoices):
    RETRIES_EXHAUSTED = "RETRIES_EXHAUSTED", "Retries Exhausted"
    PERMANENT_FAILURE = "PERMANENT_FAILURE", "Permanent Failure"
    HANDLER_NOT_FOUND = "HANDLER_NOT_FOUND", "Handler Not Found"
    INVALID_CONFIGURATION = (
        "INVALID_CONFIGURATION",
        "Invalid Configuration",
    )
    POISON_JOB = "POISON_JOB", "Poison Job"


class DeadLetterStatus(models.TextChoices):
    OPEN = "OPEN", "Open"
    REPLAYED = "REPLAYED", "Replayed"
    DISMISSED = "DISMISSED", "Dismissed"


class DeadLetterJob(models.Model):
    id = models.UUIDField(
        primary_key=True,
        default=uuid.uuid4,
        editable=False,
    )

    job = models.OneToOneField(
        "jobs.Job",
        on_delete=models.CASCADE,
        related_name="dead_letter",
    )

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
    )

    replayed_job = models.ForeignKey(
        "jobs.Job",
        on_delete=models.SET_NULL,
        related_name="replayed_from_dead_letters",
        null=True,
        blank=True,
    )

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
        return f"DLQ:{self.job_id}"