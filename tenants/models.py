# apps/tenants/models.py

import uuid

from django.db import models


class Tenant(models.Model):
    """
    Represents one logical tenant in the platform.

    Job execution rules are scoped by this tenant.
    """

    id = models.UUIDField(
        primary_key=True,
        default=uuid.uuid4,
        editable=False,
    )

    name = models.CharField(
        max_length=255,
    )

    is_active = models.BooleanField(
        default=True,
    )

    # Default requirement is two concurrent jobs per tenant,
    # but keeping it configurable makes the platform reusable.
    max_concurrent_jobs = models.PositiveSmallIntegerField(
        default=2,
    )

    created_at = models.DateTimeField(
        auto_now_add=True,
    )

    updated_at = models.DateTimeField(
        auto_now=True,
    )

    class Meta:
        db_table = "tenants"

    def __str__(self) -> str:
        return self.name