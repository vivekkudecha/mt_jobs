# tests/test_job_orchestration.py

import threading
import uuid
from datetime import timedelta
from unittest.mock import patch

import pytest
from django.db import close_old_connections
from django.utils import timezone

from jobs.models import (
    AttemptStatus,
    DeadLetterJob,
    DeadLetterStatus,
    FailureType,
    Job,
    JobAttempt,
    JobExecutionReservation,
    JobOutbox,
    JobOverlapLock,
    JobStatus,
    RetryPolicy,
    TenantSchedulerState,
    WorkloadClass,
)
from jobs.services.cancellation import cancel_job
from jobs.services.dead_letter import replay_dead_letter
from jobs.services.dispatcher import dispatch_next
from jobs.services.execution import (
    claim_job,
    complete_job,
)
from jobs.services.failure import finalize_failure
from jobs.services.heartbeat import heartbeat
from jobs.services.reconciliation import (
    reconcile_attempt,
    recover_queued,
)
from jobs.services.retry import promote_ready_retries
from jobs.services.submission import submit_job
from tenants.models import Tenant


pytestmark = pytest.mark.django_db


# ---------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------


@pytest.fixture
def tenant():
    tenant = Tenant.objects.create(
        name="Tenant A",
        max_concurrent_jobs=2,
    )

    TenantSchedulerState.objects.create(
        tenant=tenant,
    )

    return tenant


@pytest.fixture
def tenant_b():
    tenant = Tenant.objects.create(
        name="Tenant B",
        max_concurrent_jobs=2,
    )

    TenantSchedulerState.objects.create(
        tenant=tenant,
    )

    return tenant


@pytest.fixture
def retry_policy():
    return RetryPolicy.objects.create(
        name="default",
        max_attempts=3,
        initial_delay_seconds=1,
        backoff_multiplier=2,
        max_delay_seconds=10,
    )


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------


def create_job(
    tenant,
    retry_policy,
    *,
    priority=3,
    job_type="REPORT",
    status=JobStatus.WAITING,
    payload=None,
    idempotency_key=None,
    available_at=None,
):
    now = timezone.now()
    available_at = available_at or now

    return Job.objects.create(
        tenant=tenant,
        retry_policy=retry_policy,
        job_type=job_type,
        workload_class=WorkloadClass.IO_BOUND,
        priority=priority,
        status=status,
        payload=payload or {},
        idempotency_key=idempotency_key,
        available_at=available_at,
        ready_since=available_at,
    )


def submit_data(tenant, retry_policy, key=None):
    return {
        "tenant": tenant,
        "retry_policy": retry_policy,
        "job_type": "REPORT",
        "payload": {},
        "priority": 3,
        "workload_class": WorkloadClass.IO_BOUND,
        "idempotency_key": key,
    }


# ---------------------------------------------------------------------
# Submission / Idempotency
# ---------------------------------------------------------------------


def test_job_submission(tenant, retry_policy):
    job, created = submit_job(
        **submit_data(tenant, retry_policy)
    )

    assert created is True
    assert job.status == JobStatus.WAITING


def test_duplicate_submission_returns_same_job(
    tenant,
    retry_policy,
):
    data = submit_data(
        tenant,
        retry_policy,
        "same-request",
    )

    job1, created1 = submit_job(**data)
    job2, created2 = submit_job(**data)

    assert created1 is True
    assert created2 is False
    assert job1.id == job2.id
    assert Job.objects.count() == 1


def test_idempotency_is_tenant_scoped(
    tenant,
    tenant_b,
    retry_policy,
):
    key = str(uuid.uuid4())

    job1, _ = submit_job(
        **submit_data(tenant, retry_policy, key)
    )

    job2, _ = submit_job(
        **submit_data(tenant_b, retry_policy, key)
    )

    assert job1.id != job2.id
    assert Job.objects.count() == 2


# ---------------------------------------------------------------------
# Priority / Dispatch
# ---------------------------------------------------------------------


def test_high_priority_runs_first(
    tenant,
    retry_policy,
):
    low = create_job(
        tenant,
        retry_policy,
        priority=5,
    )

    high = create_job(
        tenant,
        retry_policy,
        priority=1,
    )

    dispatched = dispatch_next(tenant.id)

    assert dispatched.id == high.id

    low.refresh_from_db()

    assert low.status == JobStatus.WAITING


def test_same_priority_uses_ready_order(
    tenant,
    retry_policy,
):
    first = create_job(
        tenant,
        retry_policy,
        priority=2,
    )

    first.ready_since = timezone.now() - timedelta(minutes=2)
    first.save()

    second = create_job(
        tenant,
        retry_policy,
        priority=2,
    )

    dispatched = dispatch_next(tenant.id)

    assert dispatched.id == first.id
    assert dispatched.id != second.id


def test_future_job_is_not_dispatched(
    tenant,
    retry_policy,
):
    job = create_job(
        tenant,
        retry_policy,
        available_at=timezone.now() + timedelta(hours=1),
    )

    assert dispatch_next(tenant.id) is None

    job.refresh_from_db()

    assert job.status == JobStatus.WAITING


def test_dispatch_creates_reservation(
    tenant,
    retry_policy,
):
    job = create_job(
        tenant,
        retry_policy,
    )

    dispatch_next(tenant.id)

    assert JobExecutionReservation.objects.filter(
        job=job,
        tenant=tenant,
    ).exists()


def test_dispatch_creates_outbox(
    tenant,
    retry_policy,
):
    job = create_job(
        tenant,
        retry_policy,
    )

    dispatch_next(tenant.id)

    assert JobOutbox.objects.filter(
        job=job
    ).exists()


# ---------------------------------------------------------------------
# Tenant Capacity
# ---------------------------------------------------------------------


def test_tenant_max_two_jobs(
    tenant,
    retry_policy,
):
    create_job(tenant, retry_policy)
    create_job(tenant, retry_policy)
    create_job(tenant, retry_policy)

    dispatch_next(tenant.id)
    dispatch_next(tenant.id)

    third = dispatch_next(tenant.id)

    assert third is None

    assert JobExecutionReservation.objects.filter(
        tenant=tenant
    ).count() == 2


def test_tenant_capacity_is_independent(
    tenant,
    tenant_b,
    retry_policy,
):
    create_job(tenant, retry_policy)
    create_job(tenant_b, retry_policy)

    assert dispatch_next(tenant.id)
    assert dispatch_next(tenant_b.id)


def test_retry_wait_does_not_consume_capacity(
    tenant,
    retry_policy,
):
    create_job(
        tenant,
        retry_policy,
        status=JobStatus.RETRY_WAIT,
    )

    state = TenantSchedulerState.objects.get(
        tenant=tenant
    )

    assert state.active_reservations == 0


# ---------------------------------------------------------------------
# Worker Claim / Duplicate Celery Delivery
# ---------------------------------------------------------------------


def test_job_can_be_claimed_once(
    tenant,
    retry_policy,
):
    job = create_job(
        tenant,
        retry_policy,
    )

    dispatch_next(tenant.id)

    first = claim_job(job.id)
    second = claim_job(job.id)

    assert first is not None
    assert second is None

    assert JobAttempt.objects.filter(
        job=job
    ).count() == 1


def test_claim_sets_executing(
    tenant,
    retry_policy,
):
    job = create_job(
        tenant,
        retry_policy,
    )

    dispatch_next(tenant.id)
    attempt = claim_job(job.id)

    job.refresh_from_db()

    assert job.status == JobStatus.EXECUTING
    assert job.attempt_count == 1
    assert attempt.status == AttemptStatus.RUNNING


def test_terminal_job_cannot_be_claimed(
    tenant,
    retry_policy,
):
    job = create_job(
        tenant,
        retry_policy,
        status=JobStatus.SUCCEEDED,
    )

    assert claim_job(job.id) is None


# ---------------------------------------------------------------------
# Successful Execution
# ---------------------------------------------------------------------


def test_complete_job(
    tenant,
    retry_policy,
):
    job = create_job(
        tenant,
        retry_policy,
    )

    dispatch_next(tenant.id)
    attempt = claim_job(job.id)

    complete_job(
        attempt.id,
        {"done": True},
    )

    job.refresh_from_db()
    attempt.refresh_from_db()

    assert job.status == JobStatus.SUCCEEDED
    assert job.result == {"done": True}
    assert attempt.status == AttemptStatus.SUCCEEDED
    assert job.completed_at is not None


def test_completion_releases_capacity(
    tenant,
    retry_policy,
):
    job = create_job(
        tenant,
        retry_policy,
    )

    dispatch_next(tenant.id)
    attempt = claim_job(job.id)

    complete_job(
        attempt.id,
        {},
    )

    state = TenantSchedulerState.objects.get(
        tenant=tenant
    )

    assert state.active_reservations == 0

    assert not JobExecutionReservation.objects.filter(
        job=job
    ).exists()


# ---------------------------------------------------------------------
# Retry
# ---------------------------------------------------------------------


def test_transient_failure_retries(
    tenant,
    retry_policy,
):
    job = create_job(
        tenant,
        retry_policy,
    )

    dispatch_next(tenant.id)
    claim_job(job.id)

    finalize_failure(
        job.id,
        FailureType.TRANSIENT,
        "temporary error",
    )

    job.refresh_from_db()

    assert job.status == JobStatus.RETRY_WAIT
    assert job.available_at > timezone.now()


@pytest.mark.parametrize(
    "failure_type",
    [
        FailureType.TRANSIENT,
        FailureType.INFRASTRUCTURE,
        FailureType.TIMEOUT,
        FailureType.UNKNOWN,
    ],
)
def test_retryable_failure_types(
    tenant,
    retry_policy,
    failure_type,
):
    job = create_job(
        tenant,
        retry_policy,
        status=JobStatus.EXECUTING,
    )

    job.attempt_count = 1
    job.save()

    finalize_failure(
        job.id,
        failure_type,
        "failure",
    )

    job.refresh_from_db()

    assert job.status == JobStatus.RETRY_WAIT


def test_retry_promotion(
    tenant,
    retry_policy,
):
    job = create_job(
        tenant,
        retry_policy,
        status=JobStatus.RETRY_WAIT,
    )

    job.available_at = (
        timezone.now() - timedelta(seconds=1)
    )
    job.save()

    count = promote_ready_retries()

    job.refresh_from_db()

    assert count == 1
    assert job.status == JobStatus.WAITING


def test_future_retry_not_promoted(
    tenant,
    retry_policy,
):
    job = create_job(
        tenant,
        retry_policy,
        status=JobStatus.RETRY_WAIT,
        available_at=timezone.now() + timedelta(minutes=5),
    )

    count = promote_ready_retries()

    job.refresh_from_db()

    assert count == 0
    assert job.status == JobStatus.RETRY_WAIT


# ---------------------------------------------------------------------
# Final Failure / DLQ
# ---------------------------------------------------------------------


def test_retry_exhaustion_goes_to_dlq(
    tenant,
    retry_policy,
):
    job = create_job(
        tenant,
        retry_policy,
        status=JobStatus.EXECUTING,
    )

    job.attempt_count = retry_policy.max_attempts
    job.save()

    finalize_failure(
        job.id,
        FailureType.TRANSIENT,
        "exhausted",
    )

    job.refresh_from_db()

    assert job.status == JobStatus.FAILED_FINAL

    assert DeadLetterJob.objects.filter(
        job=job
    ).exists()


def test_permanent_failure_goes_to_dlq(
    tenant,
    retry_policy,
):
    job = create_job(
        tenant,
        retry_policy,
        status=JobStatus.EXECUTING,
    )

    job.attempt_count = 1
    job.save()

    finalize_failure(
        job.id,
        FailureType.PERMANENT,
        "invalid request",
    )

    job.refresh_from_db()

    assert job.status == JobStatus.FAILED_FINAL

    dlq = DeadLetterJob.objects.get(job=job)

    assert dlq.error_message == "invalid request"


def test_dlq_replay_creates_new_job(
    tenant,
    retry_policy,
):
    source = create_job(
        tenant,
        retry_policy,
        status=JobStatus.FAILED_FINAL,
    )

    dlq = DeadLetterJob.objects.create(
        job=source,
        tenant=tenant,
        reason="RETRIES_EXHAUSTED",
    )

    replayed = replay_dead_letter(dlq.id)

    dlq.refresh_from_db()
    source.refresh_from_db()

    assert replayed.id != source.id
    assert replayed.status == JobStatus.WAITING

    assert source.status == JobStatus.FAILED_FINAL

    assert dlq.status == DeadLetterStatus.REPLAYED
    assert dlq.replayed_job_id == replayed.id


# ---------------------------------------------------------------------
# Cancellation
# ---------------------------------------------------------------------


def test_waiting_job_cancel(
    tenant,
    retry_policy,
):
    job = create_job(
        tenant,
        retry_policy,
    )

    cancel_job(job.id)

    job.refresh_from_db()

    assert job.status == JobStatus.CANCELLED


def test_retry_wait_job_cancel(
    tenant,
    retry_policy,
):
    job = create_job(
        tenant,
        retry_policy,
        status=JobStatus.RETRY_WAIT,
    )

    cancel_job(job.id)

    job.refresh_from_db()

    assert job.status == JobStatus.CANCELLED


def test_queued_job_cancel_releases_capacity(
    tenant,
    retry_policy,
):
    job = create_job(
        tenant,
        retry_policy,
    )

    dispatch_next(tenant.id)

    cancel_job(job.id)

    job.refresh_from_db()

    state = TenantSchedulerState.objects.get(
        tenant=tenant
    )

    assert job.status == JobStatus.CANCELLED
    assert state.active_reservations == 0

    assert not JobExecutionReservation.objects.filter(
        job=job
    ).exists()


def test_executing_job_requests_cancellation(
    tenant,
    retry_policy,
):
    job = create_job(
        tenant,
        retry_policy,
    )

    dispatch_next(tenant.id)
    claim_job(job.id)

    cancel_job(job.id)

    job.refresh_from_db()

    assert job.status == JobStatus.CANCEL_REQUESTED


def test_terminal_job_cancel_is_noop(
    tenant,
    retry_policy,
):
    job = create_job(
        tenant,
        retry_policy,
        status=JobStatus.SUCCEEDED,
    )

    cancel_job(job.id)

    job.refresh_from_db()

    assert job.status == JobStatus.SUCCEEDED


# ---------------------------------------------------------------------
# Overlap
# ---------------------------------------------------------------------


def test_same_resource_is_blocked(
    tenant,
    retry_policy,
):
    first = create_job(
        tenant,
        retry_policy,
        priority=1,
        payload={
            "overlap_key": "customer:100",
        },
    )

    second = create_job(
        tenant,
        retry_policy,
        priority=2,
        payload={
            "overlap_key": "customer:100",
        },
    )

    assert dispatch_next(tenant.id).id == first.id

    dispatched = dispatch_next(tenant.id)

    second.refresh_from_db()

    assert dispatched is None
    assert second.status == JobStatus.WAITING


def test_blocked_job_does_not_block_candidate_window(
    tenant,
    retry_policy,
):
    first = create_job(
        tenant,
        retry_policy,
        priority=1,
        payload={
            "overlap_key": "customer:100",
        },
    )

    blocked = create_job(
        tenant,
        retry_policy,
        priority=2,
        payload={
            "overlap_key": "customer:100",
        },
    )

    available = create_job(
        tenant,
        retry_policy,
        priority=3,
        payload={
            "overlap_key": "customer:200",
        },
    )

    dispatch_next(tenant.id)

    dispatched = dispatch_next(tenant.id)

    blocked.refresh_from_db()

    assert dispatched.id == available.id
    assert blocked.status == JobStatus.WAITING
    assert first.id != available.id


def test_different_resources_can_run(
    tenant,
    retry_policy,
):
    first = create_job(
        tenant,
        retry_policy,
        payload={
            "overlap_key": "customer:1",
        },
    )

    second = create_job(
        tenant,
        retry_policy,
        payload={
            "overlap_key": "customer:2",
        },
    )

    dispatch_next(tenant.id)
    dispatch_next(tenant.id)

    first.refresh_from_db()
    second.refresh_from_db()

    assert first.status == JobStatus.QUEUED
    assert second.status == JobStatus.QUEUED


def test_overlap_released_after_completion(
    tenant,
    retry_policy,
):
    job = create_job(
        tenant,
        retry_policy,
        payload={
            "overlap_key": "customer:100",
        },
    )

    dispatch_next(tenant.id)

    assert JobOverlapLock.objects.filter(
        job=job
    ).exists()

    attempt = claim_job(job.id)

    complete_job(
        attempt.id,
        {},
    )

    assert not JobOverlapLock.objects.filter(
        job=job
    ).exists()


# ---------------------------------------------------------------------
# Heartbeat / Leases
# ---------------------------------------------------------------------


def test_heartbeat_extends_attempt_and_reservation(
    tenant,
    retry_policy,
):
    job = create_job(
        tenant,
        retry_policy,
    )

    dispatch_next(tenant.id)
    attempt = claim_job(job.id)

    old_attempt_expiry = attempt.lease_expires_at

    reservation = JobExecutionReservation.objects.get(
        job=job
    )

    old_reservation_expiry = reservation.expires_at

    assert heartbeat(attempt.id) is True

    attempt.refresh_from_db()
    reservation.refresh_from_db()

    assert attempt.heartbeat_at is not None
    assert attempt.lease_expires_at >= old_attempt_expiry
    assert reservation.expires_at >= old_reservation_expiry


def test_heartbeat_ignores_completed_attempt(
    tenant,
    retry_policy,
):
    job = create_job(
        tenant,
        retry_policy,
    )

    dispatch_next(tenant.id)
    attempt = claim_job(job.id)

    attempt.status = AttemptStatus.SUCCEEDED
    attempt.save()

    assert heartbeat(attempt.id) is False


# ---------------------------------------------------------------------
# Crash / Reconciliation
# ---------------------------------------------------------------------


def test_expired_attempt_becomes_abandoned(
    tenant,
    retry_policy,
):
    job = create_job(
        tenant,
        retry_policy,
    )

    dispatch_next(tenant.id)

    attempt = claim_job(job.id)

    attempt.lease_expires_at = (
        timezone.now() - timedelta(seconds=1)
    )
    attempt.save()

    reconcile_attempt(attempt.id)

    attempt.refresh_from_db()
    job.refresh_from_db()

    assert attempt.status == AttemptStatus.ABANDONED
    assert job.status == JobStatus.RETRY_WAIT


def test_expired_queued_job_returns_to_waiting(
    tenant,
    retry_policy,
):
    job = create_job(
        tenant,
        retry_policy,
    )

    dispatch_next(tenant.id)

    reservation = JobExecutionReservation.objects.get(
        job=job
    )

    reservation.expires_at = (
        timezone.now() - timedelta(seconds=1)
    )
    reservation.save()

    recover_queued(job.id)

    job.refresh_from_db()

    assert job.status == JobStatus.WAITING

    assert not JobExecutionReservation.objects.filter(
        job=job
    ).exists()


# ---------------------------------------------------------------------
# Handler Execution
# ---------------------------------------------------------------------


@patch("jobs.handlers.simulated.time.sleep")
def test_temporary_failure_handler_eventually_succeeds(
    sleep,
    tenant,
    retry_policy,
):
    from jobs.handlers import get_handler
    from jobs.handlers.context import JobContext

    job = create_job(
        tenant,
        retry_policy,
        job_type="TEMPORARY_FAILURE",
        payload={
            "fail_until_attempt": 1,
        },
    )

    attempt = JobAttempt.objects.create(
        job=job,
        attempt_number=2,
        started_at=timezone.now(),
    )

    context = JobContext(
        job,
        attempt,
    )

    result = get_handler(
        "TEMPORARY_FAILURE"
    ).execute(
        job.payload,
        context,
    )

    assert result["success_after_retry"] is True


# ---------------------------------------------------------------------
# Concurrency Tests
#
# These require PostgreSQL.
# SQLite cannot correctly test select_for_update / skip_locked behavior.
# ---------------------------------------------------------------------


@pytest.mark.django_db(transaction=True)
def test_concurrent_dispatch_never_exceeds_tenant_limit():
    tenant = Tenant.objects.create(
        name="Concurrent Tenant",
        max_concurrent_jobs=2,
    )

    TenantSchedulerState.objects.create(
        tenant=tenant,
    )

    policy = RetryPolicy.objects.create(
        name="concurrent-policy",
        max_attempts=3,
    )

    for _ in range(10):
        create_job(
            tenant,
            policy,
        )

    results = []
    errors = []

    def worker():
        close_old_connections()

        try:
            job = dispatch_next(tenant.id)

            results.append(
                job.id if job else None
            )

        except Exception as exc:
            errors.append(exc)

        finally:
            close_old_connections()

    threads = [
        threading.Thread(target=worker)
        for _ in range(10)
    ]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    assert not errors

    assert JobExecutionReservation.objects.filter(
        tenant=tenant
    ).count() <= 2

    state = TenantSchedulerState.objects.get(
        tenant=tenant
    )

    assert state.active_reservations <= 2

    assert Job.objects.filter(
        tenant=tenant,
        status=JobStatus.QUEUED,
    ).count() <= 2


@pytest.mark.django_db(transaction=True)
def test_concurrent_claim_creates_only_one_attempt():
    tenant = Tenant.objects.create(
        name="Claim Race Tenant",
        max_concurrent_jobs=2,
    )

    TenantSchedulerState.objects.create(
        tenant=tenant,
    )

    policy = RetryPolicy.objects.create(
        name="claim-policy",
        max_attempts=3,
    )

    job = create_job(
        tenant,
        policy,
    )

    dispatch_next(tenant.id)

    results = []
    errors = []

    def worker():
        close_old_connections()

        try:
            attempt = claim_job(job.id)

            results.append(
                attempt.id if attempt else None
            )

        except Exception as exc:
            errors.append(exc)

        finally:
            close_old_connections()

    threads = [
        threading.Thread(target=worker)
        for _ in range(5)
    ]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    assert not errors

    successful = [
        result
        for result in results
        if result is not None
    ]

    assert len(successful) == 1

    assert JobAttempt.objects.filter(
        job=job
    ).count() == 1

    job.refresh_from_db()

    assert job.attempt_count == 1
    assert job.status == JobStatus.EXECUTING


@pytest.mark.django_db(transaction=True)
def test_concurrent_duplicate_submission_creates_one_job():
    tenant = Tenant.objects.create(
        name="Idempotent Tenant",
    )

    TenantSchedulerState.objects.create(
        tenant=tenant,
    )

    policy = RetryPolicy.objects.create(
        name="idempotency-policy",
    )

    key = str(uuid.uuid4())

    job_ids = []
    errors = []

    def worker():
        close_old_connections()

        try:
            job, _ = submit_job(
                **submit_data(
                    tenant,
                    policy,
                    key,
                )
            )

            job_ids.append(job.id)

        except Exception as exc:
            errors.append(exc)

        finally:
            close_old_connections()

    threads = [
        threading.Thread(target=worker)
        for _ in range(5)
    ]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    assert not errors

    assert len(set(job_ids)) == 1

    assert Job.objects.filter(
        tenant=tenant,
        idempotency_key=key,
    ).count() == 1


@pytest.mark.django_db(transaction=True)
def test_concurrent_overlap_allows_one_resource_owner():
    tenant = Tenant.objects.create(
        name="Overlap Race Tenant",
        max_concurrent_jobs=5,
    )

    TenantSchedulerState.objects.create(
        tenant=tenant,
    )

    policy = RetryPolicy.objects.create(
        name="overlap-policy",
    )

    for _ in range(5):
        create_job(
            tenant,
            policy,
            payload={
                "overlap_key": "shared-resource",
            },
        )

    errors = []

    def worker():
        close_old_connections()

        try:
            dispatch_next(tenant.id)

        except Exception as exc:
            errors.append(exc)

        finally:
            close_old_connections()

    threads = [
        threading.Thread(target=worker)
        for _ in range(5)
    ]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    assert not errors

    assert JobOverlapLock.objects.filter(
        tenant=tenant,
        resource_key="shared-resource",
    ).count() == 1

    assert Job.objects.filter(
        tenant=tenant,
        status=JobStatus.QUEUED,
    ).count() == 1


@pytest.mark.django_db(transaction=True)
def test_different_tenants_dispatch_concurrently():
    policy = RetryPolicy.objects.create(
        name="multi-tenant-race",
    )

    tenants = []

    for number in range(3):
        tenant = Tenant.objects.create(
            name=f"Tenant {number}",
            max_concurrent_jobs=2,
        )

        TenantSchedulerState.objects.create(
            tenant=tenant,
        )

        create_job(
            tenant,
            policy,
        )

        tenants.append(tenant)

    results = []
    errors = []

    def worker(tenant_id):
        close_old_connections()

        try:
            job = dispatch_next(
                tenant_id
            )

            results.append(
                job.id if job else None
            )

        except Exception as exc:
            errors.append(exc)

        finally:
            close_old_connections()

    threads = [
        threading.Thread(
            target=worker,
            args=(tenant.id,),
        )
        for tenant in tenants
    ]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    assert not errors
    assert len(
        [result for result in results if result]
    ) == 3


# ---------------------------------------------------------------------
# Database Integrity
# ---------------------------------------------------------------------


def test_attempt_numbers_are_unique(
    tenant,
    retry_policy,
):
    job = create_job(
        tenant,
        retry_policy,
    )

    JobAttempt.objects.create(
        job=job,
        attempt_number=1,
        started_at=timezone.now(),
    )

    with pytest.raises(Exception):
        JobAttempt.objects.create(
            job=job,
            attempt_number=1,
            started_at=timezone.now(),
        )


def test_one_execution_reservation_per_job(
    tenant,
    retry_policy,
):
    job = create_job(
        tenant,
        retry_policy,
    )

    now = timezone.now()

    JobExecutionReservation.objects.create(
        job=job,
        tenant=tenant,
        expires_at=now + timedelta(minutes=5),
    )

    with pytest.raises(Exception):
        JobExecutionReservation.objects.create(
            job=job,
            tenant=tenant,
            expires_at=now + timedelta(minutes=5),
        )