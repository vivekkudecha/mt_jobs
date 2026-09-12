"""
Job Data Model

Purpose:
    Core model representing an asynchronous background job owned by a tenant.

Use Case:
    Stores the job metadata, execution payload, current status, scheduling timestamps,
    idempotency key, and result or error details.
"""

import uuid

from django.db import models

from tenants.models import Tenant


class JobStatus(models.TextChoices):
    """Lifecycle states of a job."""
    WAITING = "WAITING", "Waiting"                    # Job created or retry promoted; ready to be picked up
    QUEUED = "QUEUED", "Queued"                      # Concurrency slot reserved; queued for Celery worker
    EXECUTING = "EXECUTING", "Executing"              # Worker is currently executing the job
    RETRY_WAIT = "RETRY_WAIT", "Retry Wait"          # Transient error; waiting for retry timer to expire
    CANCEL_REQUESTED = "CANCEL_REQUESTED", "Cancel Requested"  # Cancel requested while running

    # Terminal States:
    SUCCEEDED = "SUCCEEDED", "Succeeded"              # Completed successfully
    FAILED_FINAL = "FAILED_FINAL", "Failed Final"    # Retries exhausted or permanent failure (sent to DLQ)
    CANCELLED = "CANCELLED", "Cancelled"              # Cancelled before or during execution


class JobPriority(models.IntegerChoices):
    """Priority levels for jobs: 1 is the highest priority, 5 is the lowest."""
    HIGHEST = 1, "Highest"
    HIGH = 2, "High"
    NORMAL = 3, "Normal"
    LOW = 4, "Low"
    LOWEST = 5, "Lowest"


class JobType(models.TextChoices):
    """Supported job types in the system."""
    REPORTS = "REPORTS", "Reports"
    DATA_PROCESSING = "DATA_PROCESSING", "Data Processing"
    SYNCHRONIZATION = "SYNCHRONIZATION", "Synchronization"
    AI_PROCESSING = "AI_PROCESSING", "AI Processing"
    NOTIFICATION = "NOTIFICATION", "Notification"


class Job(models.Model):
    """
    Main job record representing a unit of asynchronous work.
    """
    # Primary Key
    id = models.UUIDField(
        primary_key=True,
        default=uuid.uuid4,
        editable=False,
    )

    # Multi-tenancy link
    tenant = models.ForeignKey(
        Tenant,
        on_delete=models.PROTECT,
        related_name="jobs",
    )

    # Classification & Status
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

    # Input & Output data
    payload = models.JSONField(
        default=dict,
        help_text="Arbitrary JSON payload parameters passed to the job handler.",
    )

    result = models.JSONField(
        null=True,
        blank=True,
        help_text="Output JSON returned by the job handler upon success.",
    )

    # Idempotency token to prevent duplicate submissions from retrying clients
    idempotency_key = models.CharField(
        max_length=255,
        default=uuid.uuid4,
        null=True,
        blank=True,
    )

    # Scheduling Timestamps
    available_at = models.DateTimeField(
        help_text="Earliest time this job can be executed (used for delayed jobs or retry backoff).",
    )

    ready_since = models.DateTimeField(
        help_text="Timestamp when job became ready (used for fair FIFO dispatching within the same priority).",
    )

    attempt_count = models.PositiveSmallIntegerField(
        default=0,
        help_text="Number of execution attempts executed so far.",
    )

    # Error Diagnostics
    last_error_code = models.CharField(
        max_length=100,
        null=True,
        blank=True,
    )

    last_error_message = models.TextField(
        null=True,
        blank=True,
    )

    # Lifecycle Timestamps
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

        # Unique constraint per tenant + idempotency_key to prevent duplicate submissions
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

        # Indexes tailored for high-speed scheduler queries and dispatching
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
        return f"{self.id} - {self.job_type} ({self.status})"