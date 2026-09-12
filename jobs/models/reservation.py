import uuid

from django.db import models


class JobExecutionReservation(models.Model):
    id = models.UUIDField(
        primary_key=True,
        default=uuid.uuid4,
        editable=False,
    )

    job = models.OneToOneField(
        "jobs.Job",
        on_delete=models.CASCADE,
        related_name="execution_reservation",
    )

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

    expires_at = models.DateTimeField()

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
        return str(self.job_id)