"""
Job Overlap Lock Service

Purpose:
    Provides helpers to acquire and release mutual-exclusion locks on specific resources.

Use Case:
    Ensures jobs requiring exclusive access to a resource key (like syncing an external accounting ledger)
    run sequentially rather than concurrently.
"""

from datetime import timedelta

from django.db import IntegrityError, transaction
from django.utils import timezone

from jobs.models import JobOverlapLock, OverlapScope


def acquire_overlap(job, resource_key, scope=OverlapScope.TENANT):
    """
    Attempts to acquire a mutual-exclusion lock for the specified resource_key.

    Args:
        job (Job): The job requesting the lock.
        resource_key (str): The unique string identifier for the resource.
        scope (OverlapScope, optional): TENANT or GLOBAL. Defaults to TENANT.

    Returns:
        JobOverlapLock or None: Lock instance if acquired successfully, or None if already locked.
    """
    try:
        with transaction.atomic():
            return JobOverlapLock.objects.create(
                job=job,
                tenant=job.tenant if scope == OverlapScope.TENANT else None,
                scope=scope,
                resource_key=resource_key,
                expires_at=timezone.now() + timedelta(minutes=5),
            )
    except IntegrityError:
        # Another job already holds an active lock on this resource_key
        return None


def release_overlaps(job):
    """
    Releases all overlap locks currently held by the given job upon completion or failure.

    Args:
        job (Job): The job whose locks should be deleted.
    """
    JobOverlapLock.objects.filter(job=job).delete()