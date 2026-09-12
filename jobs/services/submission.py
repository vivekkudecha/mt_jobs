import uuid
from django.db import IntegrityError, transaction
from django.utils import timezone

from jobs.models import Job, JobPriority


@transaction.atomic
def submit_job(
    *,
    tenant,
    job_type,
    payload=None,
    priority=JobPriority.NORMAL,
    idempotency_key=None,
    available_at=None,
    **kwargs
):
    payload = payload if payload is not None else {}
    available_at = available_at or timezone.now()
    if not idempotency_key:
        idempotency_key = str(uuid.uuid4())

    existing = Job.objects.filter(
        tenant=tenant,
        idempotency_key=idempotency_key,
    ).first()
    if existing:
        return existing, False

    try:
        with transaction.atomic():
            job = Job.objects.create(
                tenant=tenant,
                job_type=job_type,
                payload=payload,
                priority=priority,
                idempotency_key=idempotency_key,
                available_at=available_at,
                ready_since=available_at
            )

            return job, True

    except IntegrityError:
        # Handles concurrent duplicate requests safely.
        job = Job.objects.get(
            tenant=tenant,
            idempotency_key=idempotency_key,
        )
        return job, False