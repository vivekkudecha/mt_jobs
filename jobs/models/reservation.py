"""
Job Execution Reservation Model

Purpose:
    Tracks active tenant concurrency slots to enforce limits (e.g. max 2 jobs executing at once per tenant).

Use Case:
    When a tenant's job is dispatched, a reservation row is created. If the tenant already has 2 active reservations,
    no more jobs are dispatched until one finishes and deletes its reservation.
"""

import uuid

from django.db import models


class JobExecutionReservation(models.Model):
    """
    Represents an active concurrency slot allocated to a running job for a tenant.
    """
    id = models.UUIDField(
        primary_key=True,
        default=uuid.uuid4,
        editable=False,
    )

    # One-to-one link to the reserved Job
    job = models.OneToOneField(
        "jobs.Job",
        on_delete=models.CASCADE,
        related_name="execution_reservation",
    )

    # Tenant that holds this reservation
    tenant = models.ForeignKey(
        "tenants.Tenant",
        on_delete=models.CASCADE,
        related_name="execution_reservations",
    )

    lease_token = models.UUIDField(
        default=uuid.uuid4,
        editable=False,
        unique=True,
    )

    # Automatic safety lease expiration
    expires_at = models.DateTimeField(
        help_text="Timestamp when this reservation expires if not refreshed by worker heartbeat.",
    )

    created_at = models.DateTimeField(
        auto_now_add=True,
    )

    class Meta:
        db_table = "job_execution_reservations"

        indexes = [
            models.Index(
                fields=[
                    "tenant",
                    "expires_at",
                ],
                name="idx_reservation_tenant_expiry",
            ),
        ]

    def __str__(self) -> str:
        return f"Reservation: Tenant={self.tenant_id} Job={self.job_id}"