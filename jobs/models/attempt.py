import uuid

from django.db import models


class AttemptStatus(models.TextChoices):
    RUNNING = "RUNNING", "Running"
    SUCCEEDED = "SUCCEEDED", "Succeeded"
    FAILED = "FAILED", "Failed"
    TIMED_OUT = "TIMED_OUT", "Timed Out"
    ABANDONED = "ABANDONED", "Abandoned"
    CANCELLED = "CANCELLED", "Cancelled"


class FailureType(models.TextChoices):
    TRANSIENT = "TRANSIENT", "Transient"
    PERMANENT = "PERMANENT", "Permanent"
    INFRASTRUCTURE = "INFRASTRUCTURE", "Infrastructure"
    TIMEOUT = "TIMEOUT", "Timeout"
    UNKNOWN = "UNKNOWN", "Unknown"


class JobAttempt(models.Model):
    id = models.UUIDField(
        primary_key=True,
        default=uuid.uuid4,
        editable=False,
    )

    job = models.ForeignKey(
        "jobs.Job",
        on_delete=models.CASCADE,
        related_name="attempts",
    )

    attempt_number = models.PositiveSmallIntegerField()

    status = models.CharField(
        max_length=32,
        choices=AttemptStatus.choices,
        default=AttemptStatus.RUNNING,
    )

    worker_id = models.CharField(
        max_length=255,
        null=True,
        blank=True,
    )

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
    )

    started_at = models.DateTimeField()

    finished_at = models.DateTimeField(
        null=True,
        blank=True,
    )

    heartbeat_at = models.DateTimeField(
        null=True,
        blank=True,
    )

    lease_expires_at = models.DateTimeField(
        null=True,
        blank=True,
    )

    created_at = models.DateTimeField(
        auto_now_add=True,
    )

    class Meta:
        db_table = "job_attempts"

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
            models.Index(
                fields=[
                    "status",
                    "lease_expires_at",
                ],
                name="idx_attempt_lease",
            ),
        ]

    def __str__(self) -> str:
        return f"{self.job_id} / attempt {self.attempt_number}"