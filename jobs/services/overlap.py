from datetime import timedelta

from django.db import IntegrityError, transaction
from django.utils import timezone

from jobs.models import JobOverlapLock, OverlapScope


def acquire_overlap(job, resource_key, scope=OverlapScope.TENANT):
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
        return None


def release_overlaps(job):
    JobOverlapLock.objects.filter(job=job).delete()