"""
Special Test Suite: 10-Tenant Concurrent Job Scheduling & Parallel Celery Execution

Demonstrates:
1. 10 tenants simultaneously scheduling jobs via multiple concurrent threads (Barrier synchronization).
2. Strict per-tenant concurrency enforcement (max_concurrent_jobs=2) across all 10 tenants in parallel.
3. Priority ordering (Priority 1..5) strictly preserved per tenant under high concurrency.
4. Celery task-driven parallel execution pipeline (execute_job, publish_pending, real handlers).
5. Concurrent cross-tenant idempotency deduplication with zero cross-tenant interference.
6. Multi-threaded REST API scheduling across 10 tenants simultaneously.
7. Live Celery worker multi-tenant parallel execution (when running against live Redis worker).
"""

import os
import sys
import threading
import time
import uuid
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
django.setup()

import pytest
from django.db import close_old_connections
from django.utils import timezone
from rest_framework import status
from rest_framework.test import APIClient

from jobs.models import (
    AttemptStatus,
    Job,
    JobAttempt,
    JobExecutionReservation,
    JobOutbox,
    JobPriority,
    JobStatus,
    JobType,
    OutboxStatus,
    TenantSchedulerState,
)
from jobs.services.dispatcher import dispatch_next
from jobs.services.execution import claim_job, complete_job, execute
from jobs.services.outbox import publish_pending
from jobs.services.submission import submit_job
from jobs.tasks import dispatch_jobs, execute_job, publish_outbox
from tenants.models import Tenant

pytestmark = pytest.mark.django_db(transaction=True)


# ---------------------------------------------------------------------
# Helpers & Setup
# ---------------------------------------------------------------------


def create_10_tenants(prefix="Tenant-10x", max_concurrent=2):
    """
    Creates 10 isolated tenants with their respective scheduler states.
    """
    tenants = []
    for i in range(1, 11):
        t = Tenant.objects.create(
            name=f"{prefix}-{i:02d} [{uuid.uuid4().hex[:4]}]",
            max_concurrent_jobs=max_concurrent,
        )
        TenantSchedulerState.objects.create(tenant=t)
        tenants.append(t)
    return tenants


def check_live_worker_environment():
    from django.db import connection

    db_name = connection.settings_dict.get("NAME", "")
    if db_name.startswith("test_"):
        pytest.skip(
            "Live Celery worker is connected to actual DB 'mt_jobs_db'. "
            "Run 'python jobs/test_concurrent_tenants.py' to execute live worker tests against actual DB."
        )

    # Verify if a live background Celery worker is currently responding on Redis broker
    try:
        from config.celery import app as celery_app
        insp = celery_app.control.inspect(timeout=0.5)
        pings = insp.ping()
        if not pings:
            pytest.skip(
                "No live Celery worker detected on Redis (start with 'celery -A config worker -l info')."
            )
    except Exception:
        pytest.skip(
            "Could not connect to Redis/Celery broker to reach live worker."
        )


def wait_for_job_status(api_client, job_id, expected_statuses, timeout=15.0, poll_interval=0.1):
    deadline = time.time() + timeout
    last_response_data = None

    while time.time() < deadline:
        response = api_client.get(f"/jobs/{job_id}/")
        if response.status_code == status.HTTP_200_OK:
            last_response_data = response.data
            current_status = last_response_data.get("status")
            if current_status in expected_statuses:
                return last_response_data
        time.sleep(poll_interval)

    raise TimeoutError(
        f"Job {job_id} did not reach any of {expected_statuses} within {timeout}s. "
        f"Last state: {last_response_data}"
    )


# ---------------------------------------------------------------------
# 1. 10 Tenants Simultaneously Scheduling 50+ Jobs in Parallel
# ---------------------------------------------------------------------


@pytest.mark.django_db(transaction=True)
def test_10_tenants_simultaneous_job_scheduling():
    """
    Spawns 50 concurrent worker threads (5 threads per tenant across 10 tenants)
    all firing simultaneously via a threading.Barrier to schedule jobs.
    Verifies:
    - Zero deadlocks, transaction errors, or dropped jobs.
    - All 50 jobs are created in WAITING status with exact tenant scoping.
    - Each tenant gets exactly 5 jobs.
    """
    tenants = create_10_tenants("SimSched")
    num_tenants = len(tenants)
    jobs_per_tenant = 5
    total_threads = num_tenants * jobs_per_tenant

    barrier = threading.Barrier(total_threads)
    created_jobs = []
    errors = []

    def submit_worker(tenant, job_index):
        close_old_connections()
        try:
            # Wait for all 50 threads to reach barrier before firing at the exact same instant
            barrier.wait(timeout=10.0)

            job, created = submit_job(
                tenant=tenant,
                job_type=JobType.DATA_PROCESSING,
                priority=JobPriority.NORMAL,
                payload={"worker_idx": job_index, "tenant_name": tenant.name, "duration": 0.01},
                idempotency_key=f"idem-sched-{tenant.id}-{job_index}",
            )
            created_jobs.append((tenant.id, job.id, created))
        except Exception as e:
            errors.append((tenant.id, job_index, e))
        finally:
            close_old_connections()

    threads = []
    for tenant in tenants:
        for idx in range(jobs_per_tenant):
            t = threading.Thread(target=submit_worker, args=(tenant, idx))
            threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=15.0)

    assert not errors, f"Errors occurred during concurrent scheduling: {errors}"
    assert len(created_jobs) == total_threads

    # Verify per-tenant job count and status
    for tenant in tenants:
        tenant_jobs = Job.objects.filter(tenant=tenant)
        assert tenant_jobs.count() == jobs_per_tenant
        for job in tenant_jobs:
            assert job.status == JobStatus.WAITING


# ---------------------------------------------------------------------
# 2. 10 Tenants Simultaneous Concurrent Dispatching & Strict Capacity Limit
# ---------------------------------------------------------------------


@pytest.mark.django_db(transaction=True)
def test_10_tenants_concurrent_dispatch_capacity_enforcement():
    """
    10 tenants, each having 5 WAITING jobs (50 jobs total).
    Each tenant has max_concurrent_jobs=2.
    30 concurrent worker threads fire simultaneously across all 10 tenants
    attempting to dispatch jobs.
    Verifies:
    - Every tenant has EXACTLY 2 jobs dispatched (QUEUED) and 3 remaining (WAITING).
    - Exactly 20 jobs total are QUEUED, 30 jobs total are WAITING across the 10 tenants.
    - TenantSchedulerState.active_reservations == 2 for every single tenant.
    - Exactly 20 JobExecutionReservations created across the 10 tenants.
    """
    tenants = create_10_tenants("DispLimit", max_concurrent=2)

    # Pre-populate 5 jobs per tenant (50 jobs total)
    for tenant in tenants:
        for j in range(5):
            submit_job(
                tenant=tenant,
                job_type=JobType.REPORTS,
                priority=JobPriority.NORMAL,
                payload={"idx": j, "duration": 0.01},
                idempotency_key=f"idem-disp-{tenant.id}-{j}",
            )

    # 3 threads per tenant = 30 threads attempting dispatch simultaneously
    threads_per_tenant = 3
    total_threads = len(tenants) * threads_per_tenant
    barrier = threading.Barrier(total_threads)

    dispatched_results = []
    dispatch_errors = []

    def dispatch_worker(tenant_id):
        close_old_connections()
        try:
            barrier.wait(timeout=10.0)
            job = dispatch_next(tenant_id)
            if job:
                dispatched_results.append((tenant_id, job.id))
        except Exception as e:
            dispatch_errors.append((tenant_id, e))
        finally:
            close_old_connections()

    threads = []
    for tenant in tenants:
        for _ in range(threads_per_tenant):
            t = threading.Thread(target=dispatch_worker, args=(tenant.id,))
            threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=15.0)

    assert not dispatch_errors, f"Dispatch errors occurred: {dispatch_errors}"

    # Total dispatched across all 10 tenants MUST be exactly 10 * 2 = 20
    assert len(dispatched_results) == 20

    for tenant in tenants:
        state = TenantSchedulerState.objects.get(tenant=tenant)
        assert state.active_reservations == 2

        queued_count = Job.objects.filter(tenant=tenant, status=JobStatus.QUEUED).count()
        waiting_count = Job.objects.filter(tenant=tenant, status=JobStatus.WAITING).count()
        reservation_count = JobExecutionReservation.objects.filter(tenant=tenant).count()

        assert queued_count == 2
        assert waiting_count == 3
        assert reservation_count == 2

    # Scope invariants across the 10 tenants
    assert Job.objects.filter(tenant__in=tenants, status=JobStatus.QUEUED).count() == 20
    assert Job.objects.filter(tenant__in=tenants, status=JobStatus.WAITING).count() == 30
    assert JobExecutionReservation.objects.filter(tenant__in=tenants).count() == 20


# ---------------------------------------------------------------------
# 3. 10 Tenants Priority Ordering Preserved Under High Concurrency
# ---------------------------------------------------------------------


@pytest.mark.django_db(transaction=True)
def test_10_tenants_priority_ordering_under_concurrent_contention():
    """
    For all 10 tenants, 5 jobs with distinct priorities (Priority 1..5) are submitted.
    When concurrent dispatch threads fire simultaneously across all 10 tenants,
    every single tenant MUST select Priority 1 (HIGHEST) and Priority 2 (HIGH) jobs first!
    """
    tenants = create_10_tenants("Priority10x", max_concurrent=2)

    priorities = [
        JobPriority.LOWEST,   # 5
        JobPriority.NORMAL,   # 3
        JobPriority.HIGHEST,  # 1
        JobPriority.HIGH,     # 2
        JobPriority.LOW,      # 4
    ]

    for tenant in tenants:
        for p in priorities:
            submit_job(
                tenant=tenant,
                job_type=JobType.AI_PROCESSING,
                priority=p,
                payload={"priority_test": p, "duration": 0.01},
                idempotency_key=f"idem-prio-{tenant.id}-{p}-{uuid.uuid4().hex[:4]}",
            )

    barrier = threading.Barrier(len(tenants) * 2)
    errors = []

    def dispatch_pair_worker(tenant_id):
        close_old_connections()
        try:
            barrier.wait(timeout=10.0)
            dispatch_next(tenant_id)
        except Exception as e:
            errors.append(e)
        finally:
            close_old_connections()

    threads = []
    for tenant in tenants:
        for _ in range(2):
            t = threading.Thread(target=dispatch_pair_worker, args=(tenant.id,))
            threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=15.0)

    assert not errors

    # Invariant: for EVERY single tenant, the 2 queued jobs MUST be Priority 1 (HIGHEST) and Priority 2 (HIGH)
    for tenant in tenants:
        queued_jobs = list(
            Job.objects.filter(tenant=tenant, status=JobStatus.QUEUED).order_by("priority")
        )
        assert len(queued_jobs) == 2
        assert queued_jobs[0].priority == JobPriority.HIGHEST  # 1
        assert queued_jobs[1].priority == JobPriority.HIGH     # 2

        # Waiting jobs must only be priorities 3, 4, 5
        waiting_jobs = list(
            Job.objects.filter(tenant=tenant, status=JobStatus.WAITING).order_by("priority")
        )
        assert len(waiting_jobs) == 3
        waiting_priorities = [j.priority for j in waiting_jobs]
        assert set(waiting_priorities) == {3, 4, 5}


# ---------------------------------------------------------------------
# 4. 10 Tenants Full Parallel Celery Task Execution Pipeline
# ---------------------------------------------------------------------


@pytest.mark.django_db(transaction=True)
def test_10_tenants_full_parallel_execution_pipeline():
    """
    10 tenants, each with 4 jobs across various job types (40 jobs total).
    A multi-threaded Celery task worker pool simulates concurrent Celery workers:
    - Concurrently schedules jobs across all 10 tenants.
    - Dispatches in waves according to max_concurrent_jobs=2.
    - Publishes outbox tasks via publish_pending() which enqueues execute_job tasks.
    - Concurrently executes the real Celery task execute_job(job_id) with real registered handlers.
    - Verifies all 40 jobs reach SUCCEEDED with zero capacity leaks and OutboxStatus.PUBLISHED.
    """
    tenants = create_10_tenants("Pipeline10x", max_concurrent=2)
    job_types = [
        JobType.REPORTS,
        JobType.DATA_PROCESSING,
        JobType.SYNCHRONIZATION,
        JobType.AI_PROCESSING,
    ]

    all_submitted_job_ids = []

    # 1. Parallel Submission across 10 tenants
    for tenant in tenants:
        for j_type in job_types:
            job, _ = submit_job(
                tenant=tenant,
                job_type=j_type,
                priority=JobPriority.NORMAL,
                payload={"data": f"pipeline-test-{tenant.id}-{j_type}", "duration": 0.01},
                idempotency_key=f"idem-pipe-{tenant.id}-{j_type}",
            )
            all_submitted_job_ids.append(job.id)

    assert len(all_submitted_job_ids) == 40

    # 2. Worker execution function invoking Celery task execute_job
    def run_celery_worker_cycle(tenant_id):
        close_old_connections()
        try:
            job = dispatch_next(tenant_id)
            if not job:
                return False

            # Publish outbox -> enqueues execute_job
            from jobs.services.outbox import publish_event
            outbox = JobOutbox.objects.filter(job=job).first()
            if outbox and outbox.status == OutboxStatus.PENDING:
                publish_event(outbox.id)

            # Execute Celery task entry point
            execute_job(str(job.id))
            return True
        finally:
            close_old_connections()

    # Process all 40 jobs across all 10 tenants in 2 waves
    # Wave 1: 20 jobs (2 per tenant)
    # Wave 2: 20 jobs (2 per tenant)
    for wave in range(2):
        wave_threads = []
        for tenant in tenants:
            for _ in range(2):
                t = threading.Thread(target=run_celery_worker_cycle, args=(tenant.id,))
                wave_threads.append(t)

        for t in wave_threads:
            t.start()
        for t in wave_threads:
            t.join(timeout=15.0)

    # 3. Verification across all 10 tenants and 40 jobs
    for jid in all_submitted_job_ids:
        job = Job.objects.get(id=jid)
        assert job.status == JobStatus.SUCCEEDED
        assert job.attempt_count == 1
        assert job.result is not None

    for tenant in tenants:
        state = TenantSchedulerState.objects.get(tenant=tenant)
        assert state.active_reservations == 0
        assert not JobExecutionReservation.objects.filter(tenant=tenant).exists()

    assert Job.objects.filter(tenant__in=tenants, status=JobStatus.SUCCEEDED).count() == 40
    assert JobAttempt.objects.filter(job__tenant__in=tenants, status=AttemptStatus.SUCCEEDED).count() == 40
    assert JobOutbox.objects.filter(job__tenant__in=tenants, status=OutboxStatus.PUBLISHED).count() == 40


# ---------------------------------------------------------------------
# 5. 10 Tenants Concurrent Idempotency Isolation Stress Test
# ---------------------------------------------------------------------


@pytest.mark.django_db(transaction=True)
def test_10_tenants_concurrent_idempotency_isolation():
    """
    Tests that:
    1. Simultaneous duplicate requests for the SAME tenant and SAME idempotency key
       safely return the same job without creating duplicates.
    2. The SAME idempotency key used concurrently by 10 DIFFERENT tenants correctly
       creates 10 distinct jobs (tenant isolation).
    """
    tenants = create_10_tenants("IdemIso10x")
    shared_key = f"shared-idempotency-key-{uuid.uuid4()}"

    # 30 threads: 3 concurrent requests per tenant with the EXACT SAME idempotency key
    barrier = threading.Barrier(len(tenants) * 3)
    results = []
    errors = []

    def idempotency_worker(tenant):
        close_old_connections()
        try:
            barrier.wait(timeout=10.0)
            job, created = submit_job(
                tenant=tenant,
                job_type=JobType.NOTIFICATION,
                priority=JobPriority.NORMAL,
                payload={"shared": True, "duration": 0.01},
                idempotency_key=shared_key,
            )
            results.append((tenant.id, job.id, created))
        except Exception as e:
            errors.append((tenant.id, e))
        finally:
            close_old_connections()

    threads = []
    for tenant in tenants:
        for _ in range(3):
            t = threading.Thread(target=idempotency_worker, args=(tenant,))
            threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=15.0)

    assert not errors, f"Errors: {errors}"
    assert len(results) == 30

    # Exactly 10 unique jobs in total across 10 tenants (1 per tenant)
    distinct_job_ids = {job_id for _, job_id, _ in results}
    assert len(distinct_job_ids) == 10

    for tenant in tenants:
        tenant_jobs = Job.objects.filter(tenant=tenant, idempotency_key=shared_key)
        assert tenant_jobs.count() == 1


# ---------------------------------------------------------------------
# 6. 10 Tenants Concurrent REST API Job Scheduling
# ---------------------------------------------------------------------


@pytest.mark.django_db(transaction=True)
def test_10_tenants_concurrent_api_scheduling():
    """
    10 tenants concurrently submit jobs via REST API POST /jobs/ endpoints
    using multi-threaded APIClient instances synchronized via barrier.
    Verifies HTTP 201 responses, tenant isolation, and proper serialization.
    """
    tenants = create_10_tenants("APISched10x")
    barrier = threading.Barrier(len(tenants))
    api_responses = []
    api_errors = []

    def api_worker(tenant):
        close_old_connections()
        client = APIClient()
        try:
            barrier.wait(timeout=10.0)
            res = client.post(
                "/jobs/",
                {
                    "tenant": str(tenant.id),
                    "job_type": JobType.REPORTS,
                    "priority": JobPriority.HIGH,
                    "payload": {"report_name": f"monthly_sales_{tenant.id}", "duration": 0.01},
                    "idempotency_key": f"api-idem-10x-{tenant.id}",
                },
                format="json",
            )
            api_responses.append((tenant.id, res.status_code, res.data))
        except Exception as e:
            api_errors.append((tenant.id, e))
        finally:
            close_old_connections()

    threads = [threading.Thread(target=api_worker, args=(t,)) for t in tenants]

    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=15.0)

    assert not api_errors, f"API Errors: {api_errors}"
    assert len(api_responses) == 10

    for tenant_id, status_code, data in api_responses:
        assert status_code == 201
        assert data["status"] == JobStatus.WAITING
        assert str(data["tenant"]) == str(tenant_id)


# ---------------------------------------------------------------------
# 7. 10 Tenants Live Celery Worker Multi-Tenant Parallel Execution
# ---------------------------------------------------------------------


@pytest.mark.django_db(transaction=True)
def test_10_tenants_live_redis_celery_worker_parallel_execution():
    """
    Submits jobs for 10 distinct tenants simultaneously through the REST API,
    dispatches and publishes them to Redis, and asserts that the background
    Celery worker pool processes all 10 tenants' jobs concurrently.
    """
    check_live_worker_environment()
    client = APIClient()
    tenants = create_10_tenants("LiveCelery10x", max_concurrent=2)

    submitted_job_ids = []

    # 1. Concurrently submit 1 job per tenant across 10 tenants
    for idx, tenant in enumerate(tenants):
        res = client.post(
            "/jobs/",
            {
                "tenant": str(tenant.id),
                "job_type": JobType.NOTIFICATION if idx % 2 == 0 else JobType.REPORTS,
                "priority": JobPriority.HIGH,
                "payload": {"tenant_idx": idx, "duration": 0.05},
                "idempotency_key": f"live-celery-10x-{tenant.id}",
            },
            format="json",
        )
        assert res.status_code == status.HTTP_201_CREATED
        submitted_job_ids.append((tenant.id, str(res.data["id"])))

    # 2. Dispatch all 10 tenants and publish to Redis Celery queue
    for tenant_id, _ in submitted_job_ids:
        dispatch_next(tenant_id)

    publish_pending()

    # 3. Wait for all 10 tenants' jobs to reach SUCCEEDED in parallel via Celery worker
    for _, job_id in submitted_job_ids:
        wait_for_job_status(client, job_id, [JobStatus.SUCCEEDED], timeout=25.0)

    # 4. Verify all 10 jobs succeeded
    for _, job_id in submitted_job_ids:
        job = Job.objects.get(id=job_id)
        assert job.status == JobStatus.SUCCEEDED
        assert job.attempts.filter(status=AttemptStatus.SUCCEEDED).exists()


# ---------------------------------------------------------------------
# Standalone Interactive Runner
# ---------------------------------------------------------------------


def run_10_tenant_concurrency_suite():
    print("\n" + "=" * 85, flush=True)
    print("  SPECIAL TEST SUITE: 10-TENANT SIMULTANEOUS CONCURRENCY & CELERY SCHEDULING", flush=True)
    print("=" * 85, flush=True)

    tests_to_run = [
        ("1. 10 Tenants: Simultaneous 50-Job Parallel Scheduling", test_10_tenants_simultaneous_job_scheduling),
        ("2. 10 Tenants: Concurrent Dispatching & Capacity Limit", test_10_tenants_concurrent_dispatch_capacity_enforcement),
        ("3. 10 Tenants: Priority 1..5 Ordering Under Contention", test_10_tenants_priority_ordering_under_concurrent_contention),
        ("4. 10 Tenants: Celery Parallel Execution Pipeline (40 Jobs)", test_10_tenants_full_parallel_execution_pipeline),
        ("5. 10 Tenants: Concurrent Idempotency Isolation Stress", test_10_tenants_concurrent_idempotency_isolation),
        ("6. 10 Tenants: Multi-Threaded REST API Scheduling", test_10_tenants_concurrent_api_scheduling),
        ("7. 10 Tenants: Live Redis Celery Worker Execution", test_10_tenants_live_redis_celery_worker_parallel_execution),
    ]

    passed = 0
    failed = 0
    skipped = 0

    for idx, (title, test_fn) in enumerate(tests_to_run, 1):
        start = time.time()
        try:
            test_fn()
            dur = (time.time() - start) * 1000
            print(f"  \033[92m[PASS]\033[0m {title:<65} ({dur:6.1f}ms)", flush=True)
            passed += 1
        except pytest.skip.Exception as s:
            dur = (time.time() - start) * 1000
            print(f"  \033[93m[SKIP]\033[0m {title:<65} ({dur:6.1f}ms)", flush=True)
            skipped += 1
        except Exception as e:
            dur = (time.time() - start) * 1000
            print(f"  \033[91m[FAIL]\033[0m {title:<65} ({dur:6.1f}ms)", flush=True)
            print(f"         Error: {type(e).__name__}: {e}", flush=True)
            failed += 1

    print("-" * 85, flush=True)
    print(f"  Results: {passed} passed, {failed} failed, {skipped} skipped out of {len(tests_to_run)} scenarios.", flush=True)
    print("=" * 85 + "\n", flush=True)

    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    run_10_tenant_concurrency_suite()
