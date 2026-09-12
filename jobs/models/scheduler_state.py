"""
Tenant Scheduler State Model

Purpose:
    Maintains a cached, denormalized summary of each tenant's active job counts and pending work.

Use Case:
    Enables the multi-tenant scheduler to find tenants with waiting jobs in a single, index-backed query
    without doing expensive aggregations across millions of job rows.
"""

from django.db import models


class TenantSchedulerState(models.Model):
    """
    Cached scheduling state per tenant for fast dispatch decisions.
    """
    # Tenant primary key one-to-one link
    tenant = models.OneToOneField(
        "tenants.Tenant",
        on_delete=models.CASCADE,
        related_name="scheduler_state",
        primary_key=True,
    )

    # Current count of active running jobs for this tenant
    active_reservations = models.PositiveSmallIntegerField(
        default=0,
        help_text="Current number of concurrently executing jobs for this tenant.",
    )

    # Fast boolean indicator for the dispatcher query
    has_ready_work = models.BooleanField(
        default=False,
        help_text="True if there are jobs in WAITING status ready to be executed for this tenant.",
    )

    highest_ready_priority = models.PositiveSmallIntegerField(
        null=True,
        blank=True,
        help_text="Highest priority value (1=Highest, 5=Lowest) among ready waiting jobs.",
    )

    next_ready_at = models.DateTimeField(
        null=True,
        blank=True,
        help_text="Earliest available_at timestamp among all waiting jobs for this tenant.",
    )

    last_dispatched_at = models.DateTimeField(
        null=True,
        blank=True,
        help_text="Timestamp of the most recent job dispatch (used for fair round-robin scheduling).",
    )

    updated_at = models.DateTimeField(
        auto_now=True,
    )

    class Meta:
        db_table = "tenant_scheduler_state"

        indexes = [
            # High-performance index for finding eligible tenants to dispatch next
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
        return f"SchedulerState: Tenant={self.tenant_id} (Active={self.active_reservations}, Ready={self.has_ready_work})"