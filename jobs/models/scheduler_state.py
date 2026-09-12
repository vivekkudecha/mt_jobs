from django.db import models


class TenantSchedulerState(models.Model):
    tenant = models.OneToOneField(
        "tenants.Tenant",
        on_delete=models.CASCADE,
        related_name="scheduler_state",
        primary_key=True,
    )

    active_reservations = models.PositiveSmallIntegerField(
        default=0,
    )

    has_ready_work = models.BooleanField(
        default=False,
    )

    highest_ready_priority = models.PositiveSmallIntegerField(
        null=True,
        blank=True,
    )

    next_ready_at = models.DateTimeField(
        null=True,
        blank=True,
    )

    last_dispatched_at = models.DateTimeField(
        null=True,
        blank=True,
    )

    updated_at = models.DateTimeField(
        auto_now=True,
    )

    class Meta:
        db_table = "tenant_scheduler_state"

        indexes = [
            models.Index(
                fields=[
                    "has_ready_work",
                    "highest_ready_priority",
                    "last_dispatched_at",
                ],
                name="idx_scheduler_ready",
            ),
            models.Index(
                fields=[
                    "next_ready_at",
                ],
                name="idx_scheduler_next_ready",
            ),
        ]

    def __str__(self) -> str:
        return f"Scheduler:{self.tenant_id}"