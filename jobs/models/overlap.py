"""
Job Overlap Lock Model

Purpose:
    Provides mutual-exclusion (mutex) locking over specific resource keys to prevent overlapping job execution.

Use Case:
    If two jobs try to synchronize or modify the same customer record or external API resource at the same time,
    the second job will wait until the first job finishes and releases this lock.
"""

import uuid

from django.db import models


class OverlapScope(models.TextChoices):
    """Scope of the mutual exclusion lock."""
    TENANT = "TENANT", "Tenant"  # Locked only within the specific tenant
    GLOBAL = "GLOBAL", "Global"  # Locked system-wide across all tenants


class JobOverlapLock(models.Model):
    """
    Lock record holding mutual-exclusion leases on specific resource keys.
    """
    id = models.UUIDField(
        primary_key=True,
        default=uuid.uuid4,
        editable=False,
    )

    # The job holding this active lock
    job = models.ForeignKey(
        "jobs.Job",
        on_delete=models.CASCADE,
        related_name="overlap_locks",
    )

    # Scoping tenant (NULL if GLOBAL)
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

    # The resource key being locked (e.g. "inventory_update_sku_989")
    resource_key = models.CharField(
        max_length=255,
        help_text="Unique resource identifier that cannot be processed concurrently.",
    )

    lease_token = models.UUIDField(
        default=uuid.uuid4,
        editable=False,
        unique=True,
    )

    # Expiry timestamp to prevent permanent deadlocks if worker crashes
    expires_at = models.DateTimeField(
        help_text="Safety expiry timestamp after which this lock is automatically considered invalid.",
    )

    created_at = models.DateTimeField(
        auto_now_add=True,
    )

    class Meta:
        db_table = "job_overlap_locks"

        # Enforce that only ONE job can hold a lock on a resource_key at a time
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
        return f"{self.scope}:{self.resource_key} (Job: {self.job_id})"