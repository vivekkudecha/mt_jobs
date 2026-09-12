import uuid

from django.db import models


class OverlapScope(models.TextChoices):
    TENANT = "TENANT", "Tenant"
    GLOBAL = "GLOBAL", "Global"


class JobOverlapLock(models.Model):
    id = models.UUIDField(
        primary_key=True,
        default=uuid.uuid4,
        editable=False,
    )

    job = models.ForeignKey(
        "jobs.Job",
        on_delete=models.CASCADE,
        related_name="overlap_locks",
    )

    tenant = models.ForeignKey(
        "tenants.Tenant",
        on_delete=models.CASCADE,
        related_name="job_overlap_locks",
        null=True,
        blank=True,
    )

    scope = models.CharField(
        max_length=16,
        choices=OverlapScope.choices,
        default=OverlapScope.TENANT,
    )

    resource_key = models.CharField(
        max_length=255,
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
        db_table = "job_overlap_locks"

        constraints = [
            models.UniqueConstraint(
                fields=[
                    "tenant",
                    "resource_key",
                ],
                condition=models.Q(scope=OverlapScope.TENANT),
                name="uq_tenant_overlap_resource",
            ),
            models.UniqueConstraint(
                fields=[
                    "resource_key",
                ],
                condition=models.Q(scope=OverlapScope.GLOBAL),
                name="uq_global_overlap_resource",
            ),
        ]

        indexes = [
            models.Index(
                fields=[
                    "tenant",
                    "resource_key",
                ],
                name="idx_overlap_tenant_resource",
            ),
            models.Index(
                fields=[
                    "expires_at",
                ],
                name="idx_overlap_expiry",
            ),
        ]

    def __str__(self) -> str:
        return f"{self.scope}:{self.resource_key}"