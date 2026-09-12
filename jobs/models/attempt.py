"""
Job Attempt Model

Purpose:
    Stores the execution history and diagnostic details of every single run attempt for a job.

Use Case:
    If a job is retried 3 times, there will be 3 distinct `JobAttempt` records documenting
    each worker, start/finish times, error messages, stack traces, and heartbeat leases.
"""

import uuid

from django.db import models


class AttemptStatus(models.TextChoices):
    """Execution status of an individual job attempt."""
    RUNNING = "RUNNING", "Running"          # Worker is currently executing this attempt
    SUCCEEDED = "SUCCEEDED", "Succeeded"    # Attempt completed successfully
    FAILED = "FAILED", "Failed"            # Attempt raised an exception or failed
    TIMED_OUT = "TIMED_OUT", "Timed Out"    # Execution exceeded maximum allowed runtime
    ABANDONED = "ABANDONED", "Abandoned"    # Worker heartbeat expired without completing (crashed worker)
    CANCELLED = "CANCELLED", "Cancelled"    # Execution cancelled while running


class FailureType(models.TextChoices):
    """Categorization of failure causes to guide retry behavior."""
    TRANSIENT = "TRANSIENT", "Transient"                # Temporary issue (e.g. network blip); eligible for retry
    PERMANENT = "PERMANENT", "Permanent"                # Unrecoverable error (e.g. bad schema); sent directly to DLQ
    INFRASTRUCTURE = "INFRASTRUCTURE", "Infrastructure"  # Worker host died or node lost power; eligible for retry
    TIMEOUT = "TIMEOUT", "Timeout"                      # Execution took too long; eligible for retry
    UNKNOWN = "UNKNOWN", "Unknown"                      # Unhandled general exception; eligible for retry


class JobAttempt(models.Model):
    """
    Individual attempt record for a parent Job.
    """
    id = models.UUIDField(
        primary_key=True,
        default=uuid.uuid4,
        editable=False,
    )

    # Link back to parent Job
    job = models.ForeignKey(
        "jobs.Job",
        on_delete=models.CASCADE,
        related_name="attempts",
    )

    # 1-indexed attempt sequence number (1, 2, 3...)
    attempt_number = models.PositiveSmallIntegerField()

    status = models.CharField(
        max_length=32,
        choices=AttemptStatus.choices,
        default=AttemptStatus.RUNNING,
    )

    # Hostname or container ID of the worker processing this attempt
    worker_id = models.CharField(
        max_length=255,
        null=True,
        blank=True,
    )

    # Classification & error diagnostics
    failure_type = models.CharField(
        max_length=32,
        choices=FailureType.choices,
        null=True,
        blank=True,
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

    traceback = models.TextField(
        null=True,
        blank=True,
        help_text="Full Python exception traceback for debugging.",
    )

    # Timing and Heartbeat Leases
    started_at = models.DateTimeField()

    finished_at = models.DateTimeField(
        null=True,
        blank=True,
    )

    heartbeat_at = models.DateTimeField(
        null=True,
        blank=True,
        help_text="Last timestamp worker verified it was actively working on this attempt.",
    )

    lease_expires_at = models.DateTimeField(
        null=True,
        blank=True,
        help_text="Worker lease expiry time. If now() > lease_expires_at, worker is deemed dead.",
    )

    created_at = models.DateTimeField(
        auto_now_add=True,
    )

    class Meta:
        db_table = "job_attempts"

        # Ensure no two attempts have the same attempt_number for the same job
        constraints = [
            models.UniqueConstraint(
                fields=[
                    "job",
                    "attempt_number",
                ],
                name="uq_job_attempt_number",
            ),
        ]

        indexes = [
            models.Index(
                fields=[
                    "job",
                    "status",
                ],
                name="idx_attempt_job_status",
            ),
            # Used by reaper/reconciliation to quickly find expired worker leases
            models.Index(
                fields=[
                    "status",
                    "lease_expires_at",
                ],
                name="idx_attempt_lease",
            ),
        ]

    def __str__(self) -> str:
        return f"{self.job_id} / attempt {self.attempt_number} ({self.status})"