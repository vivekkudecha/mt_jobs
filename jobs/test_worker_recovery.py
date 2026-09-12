"""
Special Test Suite: Worker Termination, Retry, and Recovery Lifecycle

Demonstrates:
1. Schedule job (WAITING -> QUEUED -> EXECUTING)
2. Terminate Celery worker (crash mid-execution / lease expiration / heartbeat failure)
3. Check status during worker outage (lease expired, attempt running, capacity held)
4. Recover Celery worker (reconcile expired attempt -> ABANDONED, capacity released, RETRY_WAIT)
5. Continue scheduled job (promote ready retries -> WAITING -> QUEUED -> EXECUTING by recovered worker)
6. Maintain state (attempt logs preserved, retry delay respected, state transitions verified, clean completion)
"""

import os
import sys
import time
import uuid
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
django.setup()

import pytest
from django.utils import timezone
from rest_framework.test import APIClient

from jobs.models import (
    AttemptStatus,
    DeadLetterJob,
    DeadLetterReason,
    FailureType,
    Job,
    JobAttempt,
    JobExecutionReservation,
    JobOutbox,
    JobOverlapLock,
    JobPriority,
    JobStatus,
    JobType,
    TenantSchedulerState,
)
from jobs.services.dead_letter import replay_dead_letter
from jobs.services.dispatcher import dispatch_next
from jobs.services.execution import claim_job, complete_job, execute
from jobs.services.reconciliation import reconcile_attempt, reconcile_expired, recover_queued
from jobs.services.retry import promote_ready_retries
from jobs.services.submission import submit_job
from tenants.models import Tenant

pytestmark = pytest.mark.django_db


# ---------------------------------------------------------------------
# Fixtures & Helpers
# ---------------------------------------------------------------------


@pytest.fixture
def tenant():
    t = Tenant.objects.create(
        name="Recovery Test Tenant",
        max_concurrent_jobs=2,
    )
    TenantSchedulerState.objects.create(
        tenant=t,
    )
    return t


@pytest.fixture
def tenant_b():
    t = Tenant.objects.create(
        name="Recovery Test Tenant B",
        max_concurrent_jobs=2,
    )
    TenantSchedulerState.objects.create(
        tenant=t,
    )
    return t


@pytest.fixture
def api_client():
    return APIClient()


def create_test_job(
    tenant,
    *,
    priority=JobPriority.NORMAL,
    job_type=JobType.REPORTS,
    status=JobStatus.WAITING,
    payload=None,
    idempotency_key=None,
    available_at=None,
):
    now = timezone.now()
    return Job.objects.create(
        tenant=tenant,
        job_type=job_type,
        priority=priority,
        status=status,
        payload=payload or {"report_id": "rep-worker-recovery-001", "duration": 0.01},
        idempotency_key=idempotency_key or str(uuid.uuid4()),
        available_at=available_at or now,
        ready_since=available_at or now,
    )


# ---------------------------------------------------------------------
# 1. Primary Special Test Case: Step-by-Step Worker Termination & Recovery
# ---------------------------------------------------------------------


def test_demonstrate_retry_failure_worker_termination_and_recovery(tenant):
    """
    Detailed 6-stage lifecycle demonstrating:
    1. Schedule job (WAITING -> QUEUED -> EXECUTING)
    2. Terminate Celery worker (Worker crashes during execution; lease expires)
    3. Check status (Verify job is EXECUTING, attempt 1 is RUNNING with expired lease)
    4. Recover Celery worker (Reconciliation marks attempt 1 ABANDONED, releases capacity, sets RETRY_WAIT)
    5. Continue to scheduled job (Promotion moves job to WAITING; recovered worker dispatches & claims attempt 2)
    6. Maintain state (Attempt 1 preserved as ABANDONED, attempt 2 SUCCEEDED, result intact, capacity released)
    """

    # -----------------------------------------------------------------
    # STEP 1: Schedule Job
    # -----------------------------------------------------------------
    job, created = submit_job(
        tenant=tenant,
        job_type=JobType.REPORTS,
        priority=JobPriority.HIGH,
        payload={"report_id": "rep-999", "duration": 0.01},
        idempotency_key="idemp-worker-crash-test-1",
    )
    assert created is True
    assert job.status == JobStatus.WAITING
    assert job.attempt_count == 0

    # Verify initial scheduler state
    state = TenantSchedulerState.objects.get(tenant=tenant)
    assert state.active_reservations == 0

    # Dispatch to Celery queue
    dispatched_job = dispatch_next(tenant.id)
    assert dispatched_job is not None
    assert dispatched_job.id == job.id

    job.refresh_from_db()
    state.refresh_from_db()
    assert job.status == JobStatus.QUEUED
    assert state.active_reservations == 1
    assert JobExecutionReservation.objects.filter(job=job).exists()
    assert JobOutbox.objects.filter(job=job).exists()

    # -----------------------------------------------------------------
    # STEP 2: Celery Worker 1 claims and starts executing, then crashes
    # -----------------------------------------------------------------
    attempt_1 = claim_job(job.id)
    assert attempt_1 is not None
    assert attempt_1.attempt_number == 1
    assert attempt_1.status == AttemptStatus.RUNNING
    assert attempt_1.worker_id is not None

    job.refresh_from_db()
    assert job.status == JobStatus.EXECUTING
    assert job.attempt_count == 1

    # SIMULATE CELERY WORKER TERMINATION:
    # Worker 1 suddenly terminates/crashes (SIGKILL, OOM, power loss, network partition).
    # Heartbeat stops and the worker lease expires in the past.
    simulated_crash_time = timezone.now() - timedelta(seconds=10)
    attempt_1.lease_expires_at = simulated_crash_time
    attempt_1.heartbeat_at = simulated_crash_time
    attempt_1.save(update_fields=["lease_expires_at", "heartbeat_at"])

    # -----------------------------------------------------------------
    # STEP 3: Check Status during worker outage
    # -----------------------------------------------------------------
    job.refresh_from_db()
    attempt_1.refresh_from_db()
    state.refresh_from_db()

    # Job is still marked EXECUTING in DB, but attempt lease is expired
    assert job.status == JobStatus.EXECUTING
    assert attempt_1.status == AttemptStatus.RUNNING
    assert attempt_1.lease_expires_at < timezone.now()
    # Concurrency reservation is still occupied until recovery runs
    assert state.active_reservations == 1

    # -----------------------------------------------------------------
    # STEP 4: Recover Celery Worker (Reconciliation Engine)
    # -----------------------------------------------------------------
    # The reconciliation job (or newly booted Celery worker recovery) inspects stale leases
    reconcile_expired()

    attempt_1.refresh_from_db()
    job.refresh_from_db()
    state.refresh_from_db()

    # Attempt 1 is recorded as ABANDONED due to INFRASTRUCTURE failure
    assert attempt_1.status == AttemptStatus.ABANDONED
    assert attempt_1.failure_type == FailureType.INFRASTRUCTURE
    assert attempt_1.finished_at is not None

    # Job enters RETRY_WAIT with exponential backoff delay (3 seconds for attempt 1)
    assert job.status == JobStatus.RETRY_WAIT
    assert job.last_error_message == "Worker lease expired"
    assert job.available_at > timezone.now()

    # Capacity is freed so tenant is not blocked
    assert state.active_reservations == 0
    assert not JobExecutionReservation.objects.filter(job=job).exists()

    # -----------------------------------------------------------------
    # STEP 5: Continue to Scheduled Job (Promotion & Recovered Worker Execution)
    # -----------------------------------------------------------------
    # Fast forward past the retry delay
    now = timezone.now()
    Job.objects.filter(id=job.id).update(available_at=now - timedelta(seconds=1))

    # Scheduler promotion promotes ready retries to WAITING
    promoted_count = promote_ready_retries()
    assert promoted_count == 1

    job.refresh_from_db()
    assert job.status == JobStatus.WAITING

    # Recovered Celery worker / dispatcher picks up the job for attempt 2
    dispatched_again = dispatch_next(tenant.id)
    assert dispatched_again is not None
    assert dispatched_again.id == job.id

    job.refresh_from_db()
    assert job.status == JobStatus.QUEUED

    # Celery worker 2 claims the task
    attempt_2 = claim_job(job.id)
    assert attempt_2 is not None
    assert attempt_2.attempt_number == 2
    assert attempt_2.status == AttemptStatus.RUNNING

    job.refresh_from_db()
    assert job.status == JobStatus.EXECUTING
    assert job.attempt_count == 2

    # Recovered worker executes the job to successful completion
    result_data = {"report_id": "rep-999", "status": "generated", "worker": "recovered-worker-02"}
    complete_job(attempt_2.id, result_data)

    # -----------------------------------------------------------------
    # STEP 6: Maintain State & Integrity Verification
    # -----------------------------------------------------------------
    job.refresh_from_db()
    attempt_1.refresh_from_db()
    attempt_2.refresh_from_db()
    state.refresh_from_db()

    # Final job status
    assert job.status == JobStatus.SUCCEEDED
    assert job.completed_at is not None
    assert job.result == result_data
    assert job.attempt_count == 2

    # Full audit history preserved:
    # Attempt 1: ABANDONED, INFRASTRUCTURE failure
    assert attempt_1.status == AttemptStatus.ABANDONED
    assert attempt_1.attempt_number == 1
    assert attempt_1.failure_type == FailureType.INFRASTRUCTURE

    # Attempt 2: SUCCEEDED
    assert attempt_2.status == AttemptStatus.SUCCEEDED
    assert attempt_2.attempt_number == 2
    assert attempt_2.finished_at is not None

    # Total attempts count is exactly 2
    assert job.attempts.count() == 2

    # Concurrency capacity restored cleanly to 0
    assert state.active_reservations == 0
    assert not JobExecutionReservation.objects.filter(job=job).exists()


# ---------------------------------------------------------------------
# 2. Worker Crash in QUEUED State (Before Worker Claim)
# ---------------------------------------------------------------------


def test_worker_termination_during_queued_state_and_recovery(tenant):
    """
    Demonstrates recovery when Celery worker crashes while a task is QUEUED
    in the broker queue before `claim_job` is invoked.
    """
    job = create_test_job(tenant)
    dispatch_next(tenant.id)

    job.refresh_from_db()
    assert job.status == JobStatus.QUEUED

    reservation = JobExecutionReservation.objects.get(job=job)
    # Simulate worker crash / queue timeout by expiring reservation
    reservation.expires_at = timezone.now() - timedelta(seconds=1)
    reservation.save(update_fields=["expires_at"])

    # Reconcile detects expired queued reservation
    reconcile_expired()

    job.refresh_from_db()
    assert job.status == JobStatus.WAITING
    assert not JobExecutionReservation.objects.filter(job=job).exists()

    # Recovered worker re-dispatches and completes the job
    dispatch_next(tenant.id)
    attempt = claim_job(job.id)
    complete_job(attempt.id, {"status": "recovered_after_queued_timeout"})

    job.refresh_from_db()
    assert job.status == JobStatus.SUCCEEDED
    assert job.attempt_count == 1


# ---------------------------------------------------------------------
# 3. Concurrency Protection Across Worker Crash & Recovery
# ---------------------------------------------------------------------


def test_worker_crash_frees_capacity_for_waiting_jobs(tenant):
    """
    Tenant has max_concurrent_jobs=2.
    3 jobs submitted: J1, J2, J3.
    J1 and J2 are dispatched (active=2). J3 is blocked waiting.
    Worker executing J1 crashes.
    Reconciliation recovers J1 into RETRY_WAIT and releases its reservation slot.
    J3 is immediately dispatched into the freed capacity slot!
    """
    j1 = create_test_job(tenant, priority=JobPriority.HIGH)
    j2 = create_test_job(tenant, priority=JobPriority.NORMAL)
    j3 = create_test_job(tenant, priority=JobPriority.LOW)

    # Dispatch J1 and J2
    dispatch_next(tenant.id)
    dispatch_next(tenant.id)

    # J3 cannot dispatch because max concurrency (2) is reached
    blocked_dispatch = dispatch_next(tenant.id)
    assert blocked_dispatch is None

    j3.refresh_from_db()
    assert j3.status == JobStatus.WAITING

    # Worker on J1 claims and crashes
    att1 = claim_job(j1.id)
    att1.lease_expires_at = timezone.now() - timedelta(seconds=5)
    att1.save(update_fields=["lease_expires_at"])

    # Reconcile J1 crash
    reconcile_expired()

    j1.refresh_from_db()
    assert j1.status == JobStatus.RETRY_WAIT

    # Now J3 can be dispatched immediately!
    j3_dispatched = dispatch_next(tenant.id)
    assert j3_dispatched is not None
    assert j3_dispatched.id == j3.id

    j3.refresh_from_db()
    assert j3.status == JobStatus.QUEUED


# ---------------------------------------------------------------------
# 4. Overlap Key (Mutual Exclusion) Released on Worker Termination
# ---------------------------------------------------------------------


def test_overlap_lock_released_on_worker_crash_and_recovery(tenant):
    """
    Job 1 holds mutual exclusion lock for 'sync_resource_A'.
    Worker crashes mid-execution.
    Reconciliation releases the overlap lock so Job 2 (waiting on 'sync_resource_A')
    is not deadlocked and can proceed immediately.
    """
    j1 = create_test_job(
        tenant,
        job_type=JobType.SYNCHRONIZATION,
        payload={"overlap_key": "sync_resource_A", "duration": 0.01},
    )
    j2 = create_test_job(
        tenant,
        job_type=JobType.SYNCHRONIZATION,
        payload={"overlap_key": "sync_resource_A", "duration": 0.01},
    )

    # Dispatch J1 -> acquires overlap lock
    dispatch_next(tenant.id)
    assert JobOverlapLock.objects.filter(resource_key="sync_resource_A").exists()

    # J2 cannot dispatch because 'sync_resource_A' is held by J1
    assert dispatch_next(tenant.id) is None

    # Worker claims J1 and crashes
    att1 = claim_job(j1.id)
    att1.lease_expires_at = timezone.now() - timedelta(seconds=1)
    att1.save(update_fields=["lease_expires_at"])

    # Reconciliation recovers J1 and releases the overlap lock
    reconcile_expired()

    assert not JobOverlapLock.objects.filter(resource_key="sync_resource_A").exists()

    # J2 can now acquire the lock and dispatch!
    dispatched_j2 = dispatch_next(tenant.id)
    assert dispatched_j2 is not None
    assert dispatched_j2.id == j2.id
    assert JobOverlapLock.objects.filter(resource_key="sync_resource_A", job=j2).exists()


# ---------------------------------------------------------------------
# 5. Repeated Worker Crashes Exhaust Retries -> Move to Dead Letter Queue
# ---------------------------------------------------------------------


def test_repeated_worker_crashes_exhaust_retries_and_move_to_dlq(tenant):
    """
    Demonstrates that repeated worker crashes across all retry attempts (1, 2, 3)
    eventually exhaust retries on attempt 4, moving the job to FAILED_FINAL and DLQ.
    Then DLQ replay allows a healthy worker to execute it.
    """
    job = create_test_job(tenant)

    # Attempt 1: Crash & Reconcile (delay = 3s)
    dispatch_next(tenant.id)
    att1 = claim_job(job.id)
    att1.lease_expires_at = timezone.now() - timedelta(seconds=1)
    att1.save(update_fields=["lease_expires_at"])
    reconcile_expired()
    job.refresh_from_db()
    assert job.status == JobStatus.RETRY_WAIT
    assert job.attempt_count == 1

    # Attempt 2: Promote, Crash & Reconcile (delay = 5s)
    Job.objects.filter(id=job.id).update(available_at=timezone.now() - timedelta(seconds=1))
    promote_ready_retries()
    dispatch_next(tenant.id)
    att2 = claim_job(job.id)
    att2.lease_expires_at = timezone.now() - timedelta(seconds=1)
    att2.save(update_fields=["lease_expires_at"])
    reconcile_expired()
    job.refresh_from_db()
    assert job.status == JobStatus.RETRY_WAIT
    assert job.attempt_count == 2

    # Attempt 3: Promote, Crash & Reconcile (delay = 10s)
    Job.objects.filter(id=job.id).update(available_at=timezone.now() - timedelta(seconds=1))
    promote_ready_retries()
    dispatch_next(tenant.id)
    att3 = claim_job(job.id)
    att3.lease_expires_at = timezone.now() - timedelta(seconds=1)
    att3.save(update_fields=["lease_expires_at"])
    reconcile_expired()
    job.refresh_from_db()
    assert job.status == JobStatus.RETRY_WAIT
    assert job.attempt_count == 3

    # Attempt 4: Promote, Crash & Reconcile -> Max retries exhausted!
    Job.objects.filter(id=job.id).update(available_at=timezone.now() - timedelta(seconds=1))
    promote_ready_retries()
    dispatch_next(tenant.id)
    att4 = claim_job(job.id)
    att4.lease_expires_at = timezone.now() - timedelta(seconds=1)
    att4.save(update_fields=["lease_expires_at"])
    reconcile_expired()

    job.refresh_from_db()
    assert job.status == JobStatus.FAILED_FINAL
    assert job.attempt_count == 4

    # DLQ record created
    dlq_entry = DeadLetterJob.objects.get(job=job)
    assert dlq_entry.reason == DeadLetterReason.RETRIES_EXHAUSTED
    assert dlq_entry.last_attempt_number == 4

    # Replay from DLQ creates fresh retry job
    replayed_job = replay_dead_letter(dlq_entry.id)
    assert replayed_job is not None
    assert replayed_job.status == JobStatus.WAITING

    # Healthy worker processes the replayed job
    dispatch_next(tenant.id)
    healthy_att = claim_job(replayed_job.id)
    complete_job(healthy_att.id, {"replay_success": True})

    replayed_job.refresh_from_db()
    assert replayed_job.status == JobStatus.SUCCEEDED


# ---------------------------------------------------------------------
# 6. REST API View of Worker Termination & Recovery Flow
# ---------------------------------------------------------------------


def test_api_view_of_worker_termination_and_recovery(api_client, tenant):
    """
    Demonstrates the exact worker crash and recovery flow observed via REST API endpoints:
    - POST /jobs/ (submit)
    - GET /jobs/<id>/ (status transitions)
    - GET /jobs/<id>/attempts/ (attempt history after crash and recovery)
    """
    # 1. Submit via REST API
    post_res = api_client.post(
        "/jobs/",
        {
            "tenant": str(tenant.id),
            "job_type": JobType.REPORTS,
            "priority": JobPriority.HIGH,
            "payload": {"report_id": "api-crash-demo-123"},
            "idempotency_key": f"api-idem-{uuid.uuid4()}",
        },
        format="json",
    )
    assert post_res.status_code == 201
    job_id = post_res.data["id"]
    assert post_res.data["status"] == JobStatus.WAITING

    # 2. Worker 1 claims job
    dispatch_next(tenant.id)
    att1 = claim_job(job_id)

    # 3. Verify API shows EXECUTING
    status_res = api_client.get(f"/jobs/{job_id}/")
    assert status_res.status_code == 200
    assert status_res.data["status"] == JobStatus.EXECUTING
    assert status_res.data["attempt_count"] == 1

    # 4. Worker 1 terminates (simulate expired lease)
    att1.lease_expires_at = timezone.now() - timedelta(seconds=10)
    att1.save(update_fields=["lease_expires_at"])

    # 5. Recovery runs
    reconcile_expired()

    # 6. Verify API shows RETRY_WAIT
    status_res = api_client.get(f"/jobs/{job_id}/")
    assert status_res.status_code == 200
    assert status_res.data["status"] == JobStatus.RETRY_WAIT

    # 7. Advance time & promote
    Job.objects.filter(id=job_id).update(available_at=timezone.now() - timedelta(seconds=1))
    promote_ready_retries()

    # 8. API shows WAITING again
    status_res = api_client.get(f"/jobs/{job_id}/")
    assert status_res.data["status"] == JobStatus.WAITING

    # 9. Recovered Worker 2 claims and completes
    dispatch_next(tenant.id)
    att2 = claim_job(job_id)
    complete_job(att2.id, {"report_url": "https://s3.amazonaws.com/reports/demo123.pdf"})

    # 10. API shows SUCCEEDED with full result
    final_res = api_client.get(f"/jobs/{job_id}/")
    assert final_res.status_code == 200
    assert final_res.data["status"] == JobStatus.SUCCEEDED
    assert final_res.data["result"]["report_url"] == "https://s3.amazonaws.com/reports/demo123.pdf"
    assert final_res.data["attempt_count"] == 2

    # 11. API attempts history lists both attempts (ABANDONED and SUCCEEDED)
    attempts_res = api_client.get(f"/jobs/{job_id}/attempts/")
    assert attempts_res.status_code == 200
    assert len(attempts_res.data) == 2
    assert attempts_res.data[0]["status"] == AttemptStatus.ABANDONED
    assert attempts_res.data[0]["failure_type"] == FailureType.INFRASTRUCTURE
    assert attempts_res.data[1]["status"] == AttemptStatus.SUCCEEDED


# ---------------------------------------------------------------------
# Standalone Interactive / Live Demonstration Runner
# ---------------------------------------------------------------------


def create_demo_tenant(name_prefix="RecoveryDemo"):
    t = Tenant.objects.create(
        name=f"{name_prefix} [{uuid.uuid4().hex[:6]}]",
        max_concurrent_jobs=2,
    )
    TenantSchedulerState.objects.create(tenant=t)
    return t


def run_special_worker_recovery_suite():
    print("\n" + "=" * 85, flush=True)
    print("  SPECIAL TEST SUITE: CELERY WORKER TERMINATION, RETRY & RECOVERY", flush=True)
    print("=" * 85, flush=True)

    client = APIClient()

    tests_to_run = [
        (
            "1. End-to-End Worker Termination & Full Recovery Lifecycle",
            lambda: test_demonstrate_retry_failure_worker_termination_and_recovery(create_demo_tenant("FullLifeCycle")),
        ),
        (
            "2. Worker Termination During QUEUED State & Recovery",
            lambda: test_worker_termination_during_queued_state_and_recovery(create_demo_tenant("QueuedCrash")),
        ),
        (
            "3. Concurrency Slot Freed on Crash for Waiting Jobs",
            lambda: test_worker_crash_frees_capacity_for_waiting_jobs(create_demo_tenant("CapacityRecovery")),
        ),
        (
            "4. Overlap Mutual Exclusion Lock Released on Crash",
            lambda: test_overlap_lock_released_on_worker_crash_and_recovery(create_demo_tenant("OverlapLock")),
        ),
        (
            "5. Repeated Worker Crashes Exhaust Retries -> DLQ",
            lambda: test_repeated_worker_crashes_exhaust_retries_and_move_to_dlq(create_demo_tenant("DLQEscalation")),
        ),
        (
            "6. REST API Perspective of Crash & Recovery Transitions",
            lambda: test_api_view_of_worker_termination_and_recovery(client, create_demo_tenant("APICrash")),
        ),
    ]

    passed = 0
    failed = 0

    for idx, (title, test_fn) in enumerate(tests_to_run, 1):
        start = time.time()
        try:
            test_fn()
            dur = (time.time() - start) * 1000
            print(f"  \033[92m[PASS]\033[0m {title:<65} ({dur:6.1f}ms)", flush=True)
            passed += 1
        except Exception as e:
            dur = (time.time() - start) * 1000
            print(f"  \033[91m[FAIL]\033[0m {title:<65} ({dur:6.1f}ms)", flush=True)
            print(f"         Error: {type(e).__name__}: {e}", flush=True)
            failed += 1

    print("-" * 85, flush=True)
    print(f"  Results: {passed} passed, {failed} failed out of {len(tests_to_run)} special scenarios.", flush=True)
    print("=" * 85 + "\n", flush=True)

    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    run_special_worker_recovery_suite()

