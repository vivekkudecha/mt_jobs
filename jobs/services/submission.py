"""
Job Submission Service

Purpose:
    Handles job creation and enforces strict idempotency across duplicate requests.

Use Case:
    When a client submits a new job (via API or internal service), this function guarantees that network
    retries, client reconnects, or duplicate requests sharing the same `idempotency_key` return the existing
    job safely without scheduling duplicate work.
"""

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
    """
    Submits a new job into the system with idempotency guarantees.

    Args:
        tenant (Tenant): The owning tenant instance.
        job_type (str): Category/type of work (e.g. REPORTS, DATA_PROCESSING).
        payload (dict, optional): Input parameters for the job handler. Defaults to {}.
        priority (int, optional): Execution priority (1=Highest, 5=Lowest). Defaults to NORMAL (3).
        idempotency_key (str, optional): Unique key for deduplication. Generated if omitted.
        available_at (datetime, optional): Earliest execution timestamp. Defaults to now().

    Returns:
        tuple[Job, bool]: (job_instance, created)
            - job_instance: The newly created or existing matching Job.
            - created: True if a new job was created, False if an existing duplicate was returned.
    """
    payload = payload if payload is not None else {}
    available_at = available_at or timezone.now()

    # Generate random UUID key if client didn't supply one
    if not idempotency_key:
        idempotency_key = str(uuid.uuid4())

    # Step 1: Check if job already exists (fast path)
    existing = Job.objects.filter(
        tenant=tenant,
        idempotency_key=idempotency_key,
    ).first()
    if existing:
        return existing, False

    # Step 2: Try creating the job inside a nested savepoint
    try:
        with transaction.atomic():
            job = Job.objects.create(
                tenant=tenant,
                job_type=job_type,
                payload=payload,
                priority=priority,
                idempotency_key=idempotency_key,
                available_at=available_at,
                ready_since=available_at,
            )

            return job, True

    except IntegrityError:
        # Step 3: Handle race condition where another concurrent request created it simultaneously
        job = Job.objects.get(
            tenant=tenant,
            idempotency_key=idempotency_key,
        )
        return job, False