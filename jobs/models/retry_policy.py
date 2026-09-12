from django.db import models


class RetryPolicy(models.Model):
    name = models.CharField(
        max_length=100,
        unique=True,
    )

    max_attempts = models.PositiveSmallIntegerField(
        default=3,
    )

    initial_delay_seconds = models.PositiveIntegerField(
        default=5,
    )

    backoff_multiplier = models.DecimalField(
        max_digits=5,
        decimal_places=2,
        default=2.0,
    )

    max_delay_seconds = models.PositiveIntegerField(
        default=300,
    )

    is_active = models.BooleanField(
        default=True,
    )

    created_at = models.DateTimeField(
        auto_now_add=True,
    )

    updated_at = models.DateTimeField(
        auto_now=True,
    )

    class Meta:
        db_table = "job_retry_policies"

    def __str__(self) -> str:
        return self.name