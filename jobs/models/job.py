import uuid

from django.db import models

from tenants.models import Tenant


class JobStatus(models.TextChoices):
    WAITING = "WAITING", "Waiting"
    QUEUED = "QUEUED", "Queued"
    EXECUTING = "EXECUTING", "Executing"
    RETRY_WAIT = "RETRY_WAIT", "Retry Wait"
    CANCEL_REQUESTED = "CANCEL_REQUESTED", "Cancel Requested"

    SUCCEEDED = "SUCCEEDED", "Succeeded"
    FAILED_FINAL = "FAILED_FINAL", "Failed Final"
    CANCELLED = "CANCELLED", "Cancelled"


class JobPriority(models.IntegerChoices):
    HIGHEST = 1, "Highest"
    HIGH = 2, "High"
    NORMAL = 3, "Normal"
    LOW = 4, "Low"
    LOWEST = 5, "Lowest"


class JobType(models.TextChoices):
    REPORTS = "REPORTS", "Reports"
    DATA_PROCESSING = "DATA_PROCESSING", "Data Processing"
    SYNCHRONIZATION = "SYNCHRONIZATION", "Synchronization"
    AI_PROCESSING = "AI_PROCESSING", "AI Processing"
    NOTIFICATION = "NOTIFICATION", "Notification"


class Job(models.Model):
    id = models.UUIDField(
        primary_key=True,
        default=uuid.uuid4,
        editable=False,
    )

    tenant = models.ForeignKey(
        Tenant,
        on_delete=models.PROTECT,
        related_name="jobs",
    )

    job_type = models.CharField(
        max_length=50,
        choices=JobType.choices,
    )

    status = models.CharField(
        max_length=32,
        choices=JobStatus.choices,
        default=JobStatus.WAITING,
    )

    priority = models.PositiveSmallIntegerField(
        choices=JobPriority.choices,
        default=JobPriority.NORMAL,
    )

    payload = models.JSONField(
        default=dict,
    )

    result = models.JSONField(
        null=True,
        blank=True,
    )

    idempotency_key = models.CharField(
        max_length=255,
        default=uuid.uuid4,
        null=True,
        blank=True,
    )

    available_at = models.DateTimeField()

    ready_since = models.DateTimeField()

    attempt_count = models.PositiveSmallIntegerField(
        default=0,
    )

    last_error_code = models.CharField(
        max_length=100,
        null=True,
        blank=True,
    )

    last_error_message = models.TextField(
        null=True,
        blank=True,
    )

    queued_at = models.DateTimeField(
        null=True,
        blank=True,
    )

    started_at = models.DateTimeField(
        null=True,
        blank=True,
    )

    completed_at = models.DateTimeField(
        null=True,
        blank=True,
    )

    cancelled_at = models.DateTimeField(
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
        db_table = "jobs"

        constraints = [
            models.UniqueConstraint(
                fields=[
                    "tenant",
                    "idempotency_key",
                ],
                condition=models.Q(
                    idempotency_key__isnull=False,
                ),
                name="uq_job_tenant_idempotency_key",
            ),
        ]

        indexes = [
            models.Index(
                fields=[
                    "tenant",
                    "status",
                    "priority",
                    "available_at",
                ],
                name="idx_job_dispatch_lookup",
            ),
            models.Index(
                fields=[
                    "status",
                    "available_at",
                ],
                name="idx_job_ready_lookup",
            ),
        ]

    def __str__(self) -> str:
        return f"{self.id} - {self.job_type}"