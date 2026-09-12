import os
import sys
import threading
import time
import uuid
from datetime import timedelta
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
    DeadLetterJob,
    DeadLetterReason,
    DeadLetterStatus,
    FailureType,
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
from jobs.services.execution import claim_job, complete_job
from jobs.services.failure import fail_attempt, finalize_failure
from jobs.services.outbox import publish_pending
from jobs.services.retry import promote_ready_retries
from tenants.models import Tenant

pytestmark = pytest.mark.django_db


# ---------------------------------------------------------------------
# Helper Utilities
# ---------------------------------------------------------------------


def sample_job_payload(tenant, retry_policy=None, **overrides):
    now = timezone.now()
    data = {
        "tenant": str(tenant.id),
        "job_type": JobType.DATA_PROCESSING,
        "priority": JobPriority.NORMAL,
        "payload": {"dataset": "users_2026", "batch_size": 100, "duration": 0.05},
        "available_at": now.isoformat(),
    }
    data.update(overrides)
    return data


def wait_for_job_status(
    api_client,
    job_id,
    expected_statuses,
    timeout=15.0,
    poll_interval=0.2,
):
    """
    Polls the GET /jobs/<job_id>/ endpoint until the job status matches
    one of the expected statuses or the timeout expires.
    """
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


def dispatch_and_publish_tenant(tenant_id):
    """
    Helper to dispatch the next waiting job for a tenant and publish the outbox event
    to Redis for Celery worker consumption.
    """
    dispatched = dispatch_next(tenant_id)
    publish_pending()
    return dispatched


def dispatch_and_publish_all():
    """
    Helper to dispatch across all tenants with ready work and publish all outbox events to Redis.
    """
    from jobs.tasks import dispatch_jobs

    dispatch_jobs()
    publish_pending()


# ---------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------


@pytest.fixture
def api_client():
    return APIClient()


@pytest.fixture
def tenant():
    t = Tenant.objects.create(
        name=f"Tenant Alpha {uuid.uuid4().hex[:6]}",
        max_concurrent_jobs=2,
    )
    TenantSchedulerState.objects.create(
        tenant=t,
    )
    return t


@pytest.fixture
def tenant_b():
    t = Tenant.objects.create(
        name=f"Tenant Beta {uuid.uuid4().hex[:6]}",
        max_concurrent_jobs=2,
    )
    TenantSchedulerState.objects.create(
        tenant=t,
    )
    return t


@pytest.fixture
def retry_policy():
    return None


# ---------------------------------------------------------------------
# Tenant API Tests
# ---------------------------------------------------------------------


def test_api_create_tenant(api_client):
    payload = {
        "name": f"Acme Corp {uuid.uuid4().hex[:6]}",
        "max_concurrent_jobs": 5,
    }
    response = api_client.post("/tenants/", payload, format="json")

    assert response.status_code == status.HTTP_201_CREATED
    data = response.data
    assert data["name"] == payload["name"]
    assert data["max_concurrent_jobs"] == 5
    assert data["is_active"] is True
    assert "id" in data
    assert Tenant.objects.filter(id=data["id"]).exists()


def test_api_list_tenants(api_client, tenant, tenant_b):
    response = api_client.get("/tenants/")

    assert response.status_code == status.HTTP_200_OK
    tenant_ids = [str(t["id"]) for t in response.data]
    assert str(tenant.id) in tenant_ids
    assert str(tenant_b.id) in tenant_ids


def test_api_retrieve_tenant(api_client, tenant):
    response = api_client.get(f"/tenants/{tenant.id}/")

    assert response.status_code == status.HTTP_200_OK
    assert str(response.data["id"]) == str(tenant.id)
    assert response.data["name"] == tenant.name
    assert response.data["max_concurrent_jobs"] == tenant.max_concurrent_jobs


def test_api_update_tenant(api_client, tenant):
    updated_name = f"Tenant Alpha Updated {uuid.uuid4().hex[:6]}"
    response = api_client.patch(
        f"/tenants/{tenant.id}/",
        {"max_concurrent_jobs": 10, "name": updated_name},
        format="json",
    )

    assert response.status_code == status.HTTP_200_OK
    tenant.refresh_from_db()
    assert tenant.max_concurrent_jobs == 10
    assert tenant.name == updated_name


def test_api_retrieve_tenant_not_found(api_client):
    random_uuid = uuid.uuid4()
    response = api_client.get(f"/tenants/{random_uuid}/")
    assert response.status_code == status.HTTP_404_NOT_FOUND


def test_api_create_tenant_validation_error(api_client):
    # Name is required
    response = api_client.post("/tenants/", {}, format="json")
    assert response.status_code == status.HTTP_400_BAD_REQUEST
    assert "name" in response.data


# ---------------------------------------------------------------------
# Job Submission API Tests (POST /jobs/)
# ---------------------------------------------------------------------


def test_api_submit_job_success(api_client, tenant, retry_policy):
    payload = sample_job_payload(
        tenant,
        job_type="notification",
        priority=JobPriority.HIGH,
        payload={"recipient": "user@example.com", "template": "welcome", "duration": 0.05},
    )

    response = api_client.post("/jobs/", payload, format="json")

    assert response.status_code == status.HTTP_201_CREATED
    data = response.data
    assert data["job_type"] == JobType.NOTIFICATION
    assert data["status"] == JobStatus.WAITING
    assert data["priority"] == JobPriority.HIGH
    assert "workload_class" not in data
    assert str(data["tenant"]) == str(tenant.id)
    assert data["idempotency_key"] is not None
    assert len(data["idempotency_key"]) > 0

    # Verify scheduler state refreshed
    scheduler_state = TenantSchedulerState.objects.get(tenant=tenant)
    assert scheduler_state.has_ready_work is True
    assert scheduler_state.highest_ready_priority == JobPriority.HIGH


def test_api_submit_job_idempotent_duplicate(api_client, tenant, retry_policy):
    idempotency_key = f"unique-order-sync-{uuid.uuid4()}"
    payload = sample_job_payload(
        tenant,
        retry_policy,
        idempotency_key=idempotency_key,
        payload={"order_id": 42, "duration": 0.05},
    )

    # First submission -> 201 Created
    response1 = api_client.post("/jobs/", payload, format="json")
    assert response1.status_code == status.HTTP_201_CREATED
    job_id_1 = str(response1.data["id"])

    # Second submission with same idempotency key -> 200 OK with original job
    response2 = api_client.post("/jobs/", payload, format="json")
    assert response2.status_code == status.HTTP_200_OK
    assert str(response2.data["id"]) == job_id_1

    # Verify only one job exists in database
    assert Job.objects.filter(tenant=tenant, idempotency_key=idempotency_key).count() == 1


def test_api_submit_job_idempotency_is_tenant_scoped(
    api_client, tenant, tenant_b, retry_policy
):
    idempotency_key = f"shared-request-uuid-{uuid.uuid4()}"
    payload_a = sample_job_payload(tenant, retry_policy, idempotency_key=idempotency_key)
    payload_b = sample_job_payload(tenant_b, retry_policy, idempotency_key=idempotency_key)

    response_a = api_client.post("/jobs/", payload_a, format="json")
    response_b = api_client.post("/jobs/", payload_b, format="json")

    assert response_a.status_code == status.HTTP_201_CREATED
    assert response_b.status_code == status.HTTP_201_CREATED
    assert str(response_a.data["id"]) != str(response_b.data["id"])
    assert str(response_a.data["tenant"]) == str(tenant.id)
    assert str(response_b.data["tenant"]) == str(tenant_b.id)


def test_api_submit_job_idempotency_header(api_client, tenant, retry_policy):
    idempotency_key = f"header-key-test-{uuid.uuid4()}"
    payload = sample_job_payload(tenant, retry_policy)

    response1 = api_client.post(
        "/jobs/",
        payload,
        HTTP_IDEMPOTENCY_KEY=idempotency_key,
        format="json",
    )
    assert response1.status_code == status.HTTP_201_CREATED
    assert response1.data["idempotency_key"] == idempotency_key

    response2 = api_client.post(
        "/jobs/",
        payload,
        HTTP_IDEMPOTENCY_KEY=idempotency_key,
        format="json",
    )
    assert response2.status_code == status.HTTP_200_OK
    assert response2.data["id"] == response1.data["id"]
    assert Job.objects.filter(tenant=tenant, idempotency_key=idempotency_key).count() == 1


def test_api_submit_job_validation_errors(api_client, tenant):
    # Missing required job_type
    bad_payload = {
        "tenant": str(tenant.id),
    }
    response = api_client.post("/jobs/", bad_payload, format="json")
    assert response.status_code == status.HTTP_400_BAD_REQUEST
    assert "job_type" in response.data

    # Invalid non-existent tenant UUID
    bad_payload_tenant = {
        "tenant": str(uuid.uuid4()),
        "job_type": JobType.REPORTS,
    }
    response_tenant = api_client.post("/jobs/", bad_payload_tenant, format="json")
    assert response_tenant.status_code == status.HTTP_400_BAD_REQUEST
    assert "tenant" in response_tenant.data

    # Invalid job_type choice
    bad_payload_job_type = {
        "tenant": str(tenant.id),
        "job_type": "invalid_job_type",
    }
    response_job_type = api_client.post("/jobs/", bad_payload_job_type, format="json")
    assert response_job_type.status_code == status.HTTP_400_BAD_REQUEST
    assert "job_type" in response_job_type.data


# ---------------------------------------------------------------------
# Job Retrieval and Listing API Tests
# ---------------------------------------------------------------------


def test_api_list_jobs(api_client, tenant, retry_policy):
    payload1 = sample_job_payload(tenant, retry_policy, job_type=JobType.REPORTS)
    payload2 = sample_job_payload(tenant, retry_policy, job_type=JobType.DATA_PROCESSING)

    res1 = api_client.post("/jobs/", payload1, format="json")
    res2 = api_client.post("/jobs/", payload2, format="json")

    response = api_client.get("/jobs/")
    assert response.status_code == status.HTTP_200_OK

    returned_ids = [str(j["id"]) for j in response.data]
    assert str(res1.data["id"]) in returned_ids
    assert str(res2.data["id"]) in returned_ids


def test_api_retrieve_job(api_client, tenant, retry_policy):
    payload = sample_job_payload(tenant, retry_policy, job_type=JobType.SYNCHRONIZATION)
    create_res = api_client.post("/jobs/", payload, format="json")
    job_id = str(create_res.data["id"])

    response = api_client.get(f"/jobs/{job_id}/")
    assert response.status_code == status.HTTP_200_OK
    assert str(response.data["id"]) == job_id
    assert response.data["job_type"] == JobType.SYNCHRONIZATION
    assert response.data["status"] == JobStatus.WAITING


def test_api_retrieve_job_not_found(api_client):
    random_uuid = uuid.uuid4()
    response = api_client.get(f"/jobs/{random_uuid}/")
    assert response.status_code == status.HTTP_404_NOT_FOUND


# ---------------------------------------------------------------------
# Job Cancellation API Tests (POST /jobs/<uuid:pk>/cancel/)
# ---------------------------------------------------------------------


def test_api_cancel_waiting_job(api_client, tenant, retry_policy):
    payload = sample_job_payload(tenant, retry_policy)
    create_res = api_client.post("/jobs/", payload, format="json")
    job_id = str(create_res.data["id"])

    response = api_client.post(f"/jobs/{job_id}/cancel/")

    assert response.status_code == status.HTTP_200_OK
    assert response.data["status"] == JobStatus.CANCELLED
    assert response.data["cancelled_at"] is not None

    # Verify in DB
    job = Job.objects.get(id=job_id)
    assert job.status == JobStatus.CANCELLED
    assert job.cancelled_at is not None


def test_api_cancel_retry_wait_job(api_client, tenant, retry_policy):
    job = Job.objects.create(
        tenant=tenant,
        job_type=JobType.REPORTS,
        status=JobStatus.RETRY_WAIT,
        available_at=timezone.now() + timedelta(minutes=5),
        ready_since=timezone.now(),
    )

    response = api_client.post(f"/jobs/{job.id}/cancel/")

    assert response.status_code == status.HTTP_200_OK
    assert response.data["status"] == JobStatus.CANCELLED
    job.refresh_from_db()
    assert job.status == JobStatus.CANCELLED


def test_api_cancel_queued_job_releases_capacity(api_client, tenant, retry_policy):
    job = Job.objects.create(
        tenant=tenant,
        job_type=JobType.REPORTS,
        status=JobStatus.WAITING,
        available_at=timezone.now(),
        ready_since=timezone.now(),
    )

    # Dispatch transitions job to QUEUED and creates reservation
    dispatched = dispatch_next(tenant.id)
    assert dispatched is not None and dispatched.id == job.id
    assert JobExecutionReservation.objects.filter(job=job).exists()

    response = api_client.post(f"/jobs/{job.id}/cancel/")

    assert response.status_code == status.HTTP_200_OK
    assert response.data["status"] == JobStatus.CANCELLED

    # Reservation must be deleted, releasing capacity
    assert not JobExecutionReservation.objects.filter(job=job).exists()


def test_api_cancel_executing_job(api_client, tenant, retry_policy):
    job = Job.objects.create(
        tenant=tenant,
        job_type=JobType.REPORTS,
        status=JobStatus.EXECUTING,
        available_at=timezone.now(),
        ready_since=timezone.now(),
        started_at=timezone.now(),
    )

    response = api_client.post(f"/jobs/{job.id}/cancel/")

    assert response.status_code == status.HTTP_200_OK
    assert response.data["status"] == JobStatus.CANCEL_REQUESTED
    job.refresh_from_db()
    assert job.status == JobStatus.CANCEL_REQUESTED


@pytest.mark.parametrize(
    "terminal_status",
    [JobStatus.SUCCEEDED, JobStatus.FAILED_FINAL, JobStatus.CANCELLED],
)
def test_api_cancel_terminal_job_is_noop(
    api_client, tenant, retry_policy, terminal_status
):
    job = Job.objects.create(
        tenant=tenant,
        job_type=JobType.REPORTS,
        status=terminal_status,
        available_at=timezone.now(),
        ready_since=timezone.now(),
        completed_at=timezone.now(),
    )

    response = api_client.post(f"/jobs/{job.id}/cancel/")

    assert response.status_code == status.HTTP_200_OK
    assert response.data["status"] == terminal_status
    job.refresh_from_db()
    assert job.status == terminal_status


# ---------------------------------------------------------------------
# Job Attempts API Tests (GET /jobs/<job_id>/attempts/)
# ---------------------------------------------------------------------


def test_api_job_attempts_empty_for_new_job(api_client, tenant, retry_policy):
    payload = sample_job_payload(tenant, retry_policy)
    create_res = api_client.post("/jobs/", payload, format="json")
    job_id = str(create_res.data["id"])

    response = api_client.get(f"/jobs/{job_id}/attempts/")

    assert response.status_code == status.HTTP_200_OK
    assert response.data == []


def test_api_job_attempts_lists_attempts(api_client, tenant, retry_policy):
    job = Job.objects.create(
        tenant=tenant,
        job_type=JobType.REPORTS,
        status=JobStatus.EXECUTING,
        available_at=timezone.now(),
        ready_since=timezone.now(),
    )

    JobAttempt.objects.create(
        job=job,
        attempt_number=1,
        status=AttemptStatus.FAILED,
        worker_id="worker-node-1",
        failure_type=FailureType.TRANSIENT,
        error_code="TIMEOUT",
        error_message="Connection timed out",
        started_at=timezone.now() - timedelta(minutes=5),
        finished_at=timezone.now() - timedelta(minutes=4),
    )

    JobAttempt.objects.create(
        job=job,
        attempt_number=2,
        status=AttemptStatus.RUNNING,
        worker_id="worker-node-2",
        started_at=timezone.now(),
    )

    response = api_client.get(f"/jobs/{job.id}/attempts/")

    assert response.status_code == status.HTTP_200_OK
    assert len(response.data) == 2

    att1_data = next(a for a in response.data if a["attempt_number"] == 1)
    assert att1_data["status"] == AttemptStatus.FAILED
    assert att1_data["worker_id"] == "worker-node-1"

    att2_data = next(a for a in response.data if a["attempt_number"] == 2)
    assert att2_data["status"] == AttemptStatus.RUNNING
    assert att2_data["worker_id"] == "worker-node-2"


def test_api_job_attempts_scoped_to_target_job(api_client, tenant, retry_policy):
    job_a = Job.objects.create(
        tenant=tenant,
        job_type=JobType.REPORTS,
        available_at=timezone.now(),
        ready_since=timezone.now(),
    )
    job_b = Job.objects.create(
        tenant=tenant,
        job_type=JobType.DATA_PROCESSING,
        available_at=timezone.now(),
        ready_since=timezone.now(),
    )

    JobAttempt.objects.create(
        job=job_a,
        attempt_number=1,
        status=AttemptStatus.SUCCEEDED,
        started_at=timezone.now(),
    )
    JobAttempt.objects.create(
        job=job_b,
        attempt_number=1,
        status=AttemptStatus.FAILED,
        started_at=timezone.now(),
    )

    response_a = api_client.get(f"/jobs/{job_a.id}/attempts/")
    assert response_a.status_code == status.HTTP_200_OK
    assert len(response_a.data) == 1
    assert str(response_a.data[0]["job"]) == str(job_a.id)

    response_b = api_client.get(f"/jobs/{job_b.id}/attempts/")
    assert response_b.status_code == status.HTTP_200_OK
    assert len(response_b.data) == 1
    assert str(response_b.data[0]["job"]) == str(job_b.id)


# ---------------------------------------------------------------------
# Dead Letter Queue API Tests
# ---------------------------------------------------------------------


def test_api_list_dead_letters(api_client, tenant, retry_policy):
    job = Job.objects.create(
        tenant=tenant,
        job_type=JobType.REPORTS,
        status=JobStatus.FAILED_FINAL,
        available_at=timezone.now(),
        ready_since=timezone.now(),
    )

    dlq = DeadLetterJob.objects.create(
        job=job,
        tenant=tenant,
        reason=DeadLetterReason.RETRIES_EXHAUSTED,
        status=DeadLetterStatus.OPEN,
        error_code="DB_CONNECTION_FAIL",
        error_message="All retry attempts exhausted",
        last_attempt_number=3,
    )

    response = api_client.get("/dead-letters/")

    assert response.status_code == status.HTTP_200_OK
    dlq_ids = [str(d["id"]) for d in response.data]
    assert str(dlq.id) in dlq_ids


def test_api_retrieve_dead_letter(api_client, tenant, retry_policy):
    job = Job.objects.create(
        tenant=tenant,
        job_type=JobType.REPORTS,
        status=JobStatus.FAILED_FINAL,
        available_at=timezone.now(),
        ready_since=timezone.now(),
    )

    dlq = DeadLetterJob.objects.create(
        job=job,
        tenant=tenant,
        reason=DeadLetterReason.POISON_JOB,
        status=DeadLetterStatus.OPEN,
        error_code="SYNTAX_ERROR",
        error_message="Invalid payload structure",
    )

    response = api_client.get(f"/dead-letters/{dlq.id}/")

    assert response.status_code == status.HTTP_200_OK
    assert str(response.data["id"]) == str(dlq.id)
    assert response.data["reason"] == DeadLetterReason.POISON_JOB
    assert response.data["status"] == DeadLetterStatus.OPEN
    assert response.data["error_code"] == "SYNTAX_ERROR"
    assert str(response.data["job"]) == str(job.id)


def test_api_replay_dead_letter(api_client, tenant, retry_policy):
    job = Job.objects.create(
        tenant=tenant,
        job_type=JobType.SYNCHRONIZATION,
        status=JobStatus.FAILED_FINAL,
        payload={"task": "sync_inventory", "duration": 0.05},
        priority=JobPriority.HIGH,
        available_at=timezone.now(),
        ready_since=timezone.now(),
    )

    dlq = DeadLetterJob.objects.create(
        job=job,
        tenant=tenant,
        reason=DeadLetterReason.RETRIES_EXHAUSTED,
        status=DeadLetterStatus.OPEN,
        error_code="TIMEOUT",
    )

    response = api_client.post(f"/dead-letters/{dlq.id}/replay/")

    assert response.status_code == status.HTTP_201_CREATED
    new_job_data = response.data

    # Replayed job has new ID and WAITING status
    assert str(new_job_data["id"]) != str(job.id)
    assert new_job_data["status"] == JobStatus.WAITING
    assert new_job_data["job_type"] == JobType.SYNCHRONIZATION
    assert new_job_data["priority"] == JobPriority.HIGH
    assert "workload_class" not in new_job_data
    assert str(new_job_data["tenant"]) == str(tenant.id)

    # Verify DLQ entry status updated
    dlq.refresh_from_db()
    assert dlq.status == DeadLetterStatus.REPLAYED
    assert str(dlq.replayed_job_id) == str(new_job_data["id"])
    assert dlq.replayed_at is not None

    # Verify the new job can be fetched via Jobs API
    job_get_res = api_client.get(f"/jobs/{new_job_data['id']}/")
    assert job_get_res.status_code == status.HTTP_200_OK
    assert str(job_get_res.data["id"]) == str(new_job_data["id"])


# ---------------------------------------------------------------------
# End-to-End Orchestration via REST API (Direct Orchestration)
# ---------------------------------------------------------------------


def test_api_e2e_successful_job_lifecycle(api_client, tenant, retry_policy):
    """
    Test full lifecycle:
    1. Submit job via REST API -> WAITING
    2. Dispatch job -> QUEUED
    3. Claim job -> EXECUTING
    4. Verify attempt created via GET /jobs/<id>/attempts/
    5. Complete job -> SUCCEEDED
    6. Verify final state via GET /jobs/<id>/
    """
    # 1. Submit job via REST API
    payload = sample_job_payload(
        tenant,
        retry_policy,
        job_type=JobType.REPORTS,
        payload={"order_id": 999, "duration": 0.05},
    )
    submit_res = api_client.post("/jobs/", payload, format="json")
    assert submit_res.status_code == status.HTTP_201_CREATED
    job_id = str(submit_res.data["id"])

    # 2. Dispatch
    dispatched = dispatch_next(tenant.id)
    assert dispatched is not None and dispatched.id == uuid.UUID(job_id)

    # Check job is now QUEUED via API
    get_res = api_client.get(f"/jobs/{job_id}/")
    assert get_res.data["status"] == JobStatus.QUEUED

    # 3. Claim job
    attempt = claim_job(job_id)
    assert attempt is not None

    # Check job is now EXECUTING via API
    get_res = api_client.get(f"/jobs/{job_id}/")
    assert get_res.data["status"] == JobStatus.EXECUTING

    # 4. Check attempt via API
    attempts_res = api_client.get(f"/jobs/{job_id}/attempts/")
    assert attempts_res.status_code == status.HTTP_200_OK
    assert len(attempts_res.data) == 1
    assert attempts_res.data[0]["status"] == AttemptStatus.RUNNING

    # 5. Complete job
    complete_job(attempt.id, result={"status": "all_good"})

    # 6. Verify final SUCCEEDED state via API
    final_res = api_client.get(f"/jobs/{job_id}/")
    assert final_res.status_code == status.HTTP_200_OK
    assert final_res.data["status"] == JobStatus.SUCCEEDED
    assert final_res.data["result"] == {"status": "all_good"}

    # Verify attempt finished
    final_attempts_res = api_client.get(f"/jobs/{job_id}/attempts/")
    assert final_attempts_res.data[0]["status"] == AttemptStatus.SUCCEEDED


def test_api_e2e_failure_and_dlq_replay_lifecycle(api_client, tenant, retry_policy):
    """
    Test full failure and DLQ flow:
    1. Submit job via REST API
    2. Dispatch and claim
    3. Finalize permanent failure -> routed to DLQ
    4. Inspect DLQ via GET /dead-letters/
    5. Replay via POST /dead-letters/<id>/replay/
    6. Verify new job via GET /jobs/<replayed_id>/
    """
    # 1. Submit
    payload = sample_job_payload(
        tenant,
        retry_policy,
        job_type=JobType.AI_PROCESSING,
        payload={"model": "gpt-custom", "duration": 0.05},
    )
    submit_res = api_client.post("/jobs/", payload, format="json")
    assert submit_res.status_code == status.HTTP_201_CREATED
    job_id = str(submit_res.data["id"])

    # 2. Dispatch and Claim
    dispatched = dispatch_next(tenant.id)
    assert dispatched is not None and dispatched.id == uuid.UUID(job_id)

    attempt = claim_job(job_id)
    assert attempt is not None

    # 3. Finalize permanent failure
    fail_attempt(
        attempt.id,
        exc="Critical payload corruption",
        failure_type=FailureType.PERMANENT,
    )

    # Verify job is FAILED_FINAL via API
    job_res = api_client.get(f"/jobs/{job_id}/")
    assert job_res.data["status"] == JobStatus.FAILED_FINAL

    # 4. Check DLQ via API
    dlq_list_res = api_client.get("/dead-letters/")
    assert dlq_list_res.status_code == status.HTTP_200_OK
    dlq_item = next(d for d in dlq_list_res.data if str(d["job"]) == job_id)
    assert dlq_item["reason"] == DeadLetterReason.PERMANENT_FAILURE
    assert dlq_item["error_message"] == "Critical payload corruption"
    assert dlq_item["status"] == DeadLetterStatus.OPEN

    # 5. Replay DLQ via API
    replay_res = api_client.post(f"/dead-letters/{dlq_item['id']}/replay/")
    assert replay_res.status_code == status.HTTP_201_CREATED
    new_job_id = str(replay_res.data["id"])

    # 6. Verify replayed job via API
    replayed_job_res = api_client.get(f"/jobs/{new_job_id}/")
    assert replayed_job_res.status_code == status.HTTP_200_OK
    assert replayed_job_res.data["status"] == JobStatus.WAITING
    assert replayed_job_res.data["job_type"] == JobType.AI_PROCESSING

    # DLQ status should now be REPLAYED
    updated_dlq_res = api_client.get(f"/dead-letters/{dlq_item['id']}/")
    assert updated_dlq_res.data["status"] == DeadLetterStatus.REPLAYED
    assert str(updated_dlq_res.data["replayed_job"]) == new_job_id


# ---------------------------------------------------------------------
# Concurrent API Requests
# ---------------------------------------------------------------------


@pytest.mark.django_db(transaction=True)
def test_api_concurrent_duplicate_submissions():
    """
    Simulate concurrent incoming API requests submitting the exact same
    idempotency_key for the same tenant. Exactly one job should be created,
    and all clients must receive the identical job ID.
    """
    tenant = Tenant.objects.create(
        name=f"Concurrent Tenant {uuid.uuid4().hex[:6]}",
        max_concurrent_jobs=2,
    )
    TenantSchedulerState.objects.create(
        tenant=tenant,
    )

    idempotency_key = f"concurrent-api-{uuid.uuid4()}"
    client_responses = []
    errors = []

    def submit_worker():
        try:
            close_old_connections()
            client = APIClient()
            payload = sample_job_payload(
                tenant,
                idempotency_key=idempotency_key,
                payload={"concurrent": True, "duration": 0.05},
            )
            res = client.post("/jobs/", payload, format="json")
            client_responses.append(res)
        except Exception as e:
            errors.append(e)
        finally:
            close_old_connections()

    threads = [threading.Thread(target=submit_worker) for _ in range(8)]

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, f"Encountered unexpected errors during concurrent requests: {errors}"
    assert len(client_responses) == 8

    # All responses should be HTTP 201 or 200
    for res in client_responses:
        assert res.status_code in {status.HTTP_200_OK, status.HTTP_201_CREATED}

    # All responses should return the exact same job ID
    job_ids = {str(res.data["id"]) for res in client_responses}
    assert len(job_ids) == 1, f"Expected exactly 1 unique job ID, got: {job_ids}"

    # Exactly one record in DB
    assert Job.objects.filter(tenant=tenant, idempotency_key=idempotency_key).count() == 1


# ---------------------------------------------------------------------
# Job Type Choices & Normalization API Tests
# ---------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw_input, expected_enum",
    [
        ("reports", JobType.REPORTS),
        ("REPORTS", JobType.REPORTS),
        ("data processing", JobType.DATA_PROCESSING),
        ("data_processing", JobType.DATA_PROCESSING),
        ("DATA_PROCESSING", JobType.DATA_PROCESSING),
        ("synchronization", JobType.SYNCHRONIZATION),
        ("SYNCHRONIZATION", JobType.SYNCHRONIZATION),
        ("AI processing", JobType.AI_PROCESSING),
        ("ai_processing", JobType.AI_PROCESSING),
        ("AI_PROCESSING", JobType.AI_PROCESSING),
        ("notification", JobType.NOTIFICATION),
        ("NOTIFICATION", JobType.NOTIFICATION),
    ],
)
def test_api_job_type_all_choices_accepted(
    api_client, tenant, retry_policy, raw_input, expected_enum
):
    payload = sample_job_payload(
        tenant,
        retry_policy,
        job_type=raw_input,
    )
    response = api_client.post("/jobs/", payload, format="json")
    assert response.status_code == status.HTTP_201_CREATED
    assert response.data["job_type"] == expected_enum
    assert "workload_class" not in response.data


def test_api_job_type_invalid_choice_rejected(api_client, tenant, retry_policy):
    payload = sample_job_payload(
        tenant,
        retry_policy,
        job_type="unsupported_job_type",
    )
    response = api_client.post("/jobs/", payload, format="json")
    assert response.status_code == status.HTTP_400_BAD_REQUEST
    assert "job_type" in response.data


def check_live_worker_environment():
    """
    Checks if test is running in a live database where background Celery worker is connected.
    If running in an isolated pytest test database (test_mt_jobs_db), skip external worker wait.
    """
    from django.db import connection

    db_name = connection.settings_dict.get("NAME", "")
    if db_name.startswith("test_"):
        pytest.skip(
            "Live Celery worker is connected to actual DB 'mt_jobs_db'. "
            "Run 'python jobs/test_api.py' to execute live worker tests against actual DB."
        )


# =====================================================================
# EXTENDED LIVE REDIS CELERY WORKER & PERSISTENT DATABASE TEST SUITE
# =====================================================================


@pytest.mark.django_db(transaction=True)
def test_worker_e2e_successful_job_execution(api_client, tenant):
    """
    Submits a job via the REST API, dispatches it to Redis, and asserts that
    the background Celery worker picks it up from Redis, executes the real
    simulated handler, and updates the database with SUCCEEDED status and
    proper execution attempt logs preserved in the database.
    """
    check_live_worker_environment()
    payload = sample_job_payload(
        tenant,
        job_type=JobType.REPORTS,
        priority=JobPriority.HIGH,
        payload={"report_id": f"rep_{uuid.uuid4().hex[:8]}", "duration": 0.05},
    )

    # 1. Submit via REST API
    create_res = api_client.post("/jobs/", payload, format="json")
    assert create_res.status_code == status.HTTP_201_CREATED
    job_id = str(create_res.data["id"])
    assert create_res.data["status"] == JobStatus.WAITING

    # 2. Dispatch to Redis worker queue
    dispatched = dispatch_and_publish_tenant(tenant.id)
    assert dispatched is not None
    assert str(dispatched.id) == job_id

    # 3. Wait for Celery worker to consume from Redis and complete execution
    final_job_data = wait_for_job_status(
        api_client,
        job_id,
        expected_statuses=[JobStatus.SUCCEEDED],
        timeout=15.0,
    )

    # 4. Verify Job completion state in API and DB
    assert final_job_data["status"] == JobStatus.SUCCEEDED
    assert final_job_data["result"] is not None
    assert final_job_data["result"].get("status") == "generated"
    assert final_job_data["completed_at"] is not None

    # 5. Verify Job Attempt records recorded by the real Celery worker
    attempts_res = api_client.get(f"/jobs/{job_id}/attempts/")
    assert attempts_res.status_code == status.HTTP_200_OK
    assert len(attempts_res.data) == 1

    attempt = attempts_res.data[0]
    assert attempt["attempt_number"] == 1
    assert attempt["status"] == AttemptStatus.SUCCEEDED
    assert attempt["worker_id"] is not None
    assert len(attempt["worker_id"]) > 0
    assert attempt["started_at"] is not None
    assert attempt["finished_at"] is not None

    # 6. Verify Outbox record is PUBLISHED
    outbox_entry = JobOutbox.objects.filter(job_id=job_id).first()
    assert outbox_entry is not None
    assert outbox_entry.status == OutboxStatus.PUBLISHED
    assert outbox_entry.published_at is not None

    # 7. Verify capacity released in tenant scheduler state
    state = TenantSchedulerState.objects.get(tenant=tenant)
    assert state.active_reservations == 0


@pytest.mark.django_db(transaction=True)
def test_worker_e2e_all_job_types_execution(api_client, tenant):
    """
    Verifies that the live Celery worker handles all supported job types
    (REPORTS, DATA_PROCESSING, SYNCHRONIZATION, AI_PROCESSING, NOTIFICATION)
    end-to-end via Redis dispatching.
    """
    check_live_worker_environment()
    job_configs = [
        (JobType.REPORTS, {"report_id": f"rep_all_{uuid.uuid4().hex[:6]}", "duration": 0.05}),
        (JobType.DATA_PROCESSING, {"dataset": "analytics_q3", "duration": 0.05}),
        (JobType.SYNCHRONIZATION, {"target_service": "crm_sync", "duration": 0.05}),
        (JobType.AI_PROCESSING, {"prompt_id": "embed_v2", "duration": 0.05}),
        (JobType.NOTIFICATION, {"channel": "slack", "duration": 0.05}),
    ]

    submitted_jobs = []

    for job_type, payload_data in job_configs:
        payload = sample_job_payload(
            tenant,
            job_type=job_type,
            payload=payload_data,
        )
        res = api_client.post("/jobs/", payload, format="json")
        assert res.status_code == status.HTTP_201_CREATED
        submitted_jobs.append((job_type, str(res.data["id"])))

    # Dispatch all waiting jobs in batches to Celery / Redis
    for _ in range(len(submitted_jobs)):
        dispatch_and_publish_tenant(tenant.id)
        time.sleep(0.1)

    # Wait for all jobs to reach SUCCEEDED status via worker execution
    for job_type, job_id in submitted_jobs:
        job_data = wait_for_job_status(
            api_client,
            job_id,
            expected_statuses=[JobStatus.SUCCEEDED],
            timeout=20.0,
        )
        assert job_data["status"] == JobStatus.SUCCEEDED
        assert job_data["result"] is not None

        # Confirm attempt exists in database
        assert JobAttempt.objects.filter(job_id=job_id, status=AttemptStatus.SUCCEEDED).exists()


@pytest.mark.django_db(transaction=True)
def test_worker_e2e_tenant_concurrency_limiting(api_client):
    """
    Verifies tenant concurrency throttling under live Celery worker execution:
    A tenant with max_concurrent_jobs=2 submits 4 jobs.
    Only 2 can be dispatched and executed simultaneously.
    As workers finish, subsequent jobs are dispatched and processed to completion.
    """
    check_live_worker_environment()
    tenant = Tenant.objects.create(
        name=f"Throttled Tenant {uuid.uuid4().hex[:6]}",
        max_concurrent_jobs=2,
    )
    TenantSchedulerState.objects.create(tenant=tenant)

    job_ids = []
    for i in range(4):
        payload = sample_job_payload(
            tenant,
            job_type=JobType.DATA_PROCESSING,
            payload={"batch_index": i, "duration": 0.1},
        )
        res = api_client.post("/jobs/", payload, format="json")
        assert res.status_code == status.HTTP_201_CREATED
        job_ids.append(str(res.data["id"]))

    # Initial dispatch: exactly 2 should be dispatched
    d1 = dispatch_next(tenant.id)
    d2 = dispatch_next(tenant.id)
    d3 = dispatch_next(tenant.id)

    assert d1 is not None
    assert d2 is not None
    assert d3 is None, "Tenant concurrency limit of 2 must prevent 3rd concurrent dispatch"

    publish_pending()

    # Wait for the first two jobs to complete in Celery worker
    wait_for_job_status(api_client, str(d1.id), [JobStatus.SUCCEEDED], timeout=15.0)
    wait_for_job_status(api_client, str(d2.id), [JobStatus.SUCCEEDED], timeout=15.0)

    # Now capacity is freed; dispatch remaining 2 jobs
    d4 = dispatch_next(tenant.id)
    d5 = dispatch_next(tenant.id)
    assert d4 is not None
    assert d5 is not None
    publish_pending()

    # Wait for remaining jobs to complete
    wait_for_job_status(api_client, str(d4.id), [JobStatus.SUCCEEDED], timeout=15.0)
    wait_for_job_status(api_client, str(d5.id), [JobStatus.SUCCEEDED], timeout=15.0)

    # All 4 jobs are SUCCEEDED
    for jid in job_ids:
        j = Job.objects.get(id=jid)
        assert j.status == JobStatus.SUCCEEDED


@pytest.mark.django_db(transaction=True)
def test_worker_e2e_temporary_failure_retry_flow(api_client, tenant):
    """
    Submits a job that fails with a retryable error on attempt 1,
    enters RETRY_WAIT, gets promoted by process_retries, and then
    succeeds on attempt 2 executed by the real Celery worker.
    """
    check_live_worker_environment()
    payload = sample_job_payload(
        tenant,
        job_type=JobType.DATA_PROCESSING,
        payload={"simulate_temporary_failure": True, "fail_until_attempt": 1, "duration": 0.05},
    )

    create_res = api_client.post("/jobs/", payload, format="json")
    assert create_res.status_code == status.HTTP_201_CREATED
    job_id = str(create_res.data["id"])

    # 1. Dispatch attempt 1 to Redis
    dispatch_and_publish_tenant(tenant.id)

    # 2. Wait for Celery worker to fail attempt 1 and place job into RETRY_WAIT
    job_data_att1 = wait_for_job_status(
        api_client,
        job_id,
        expected_statuses=[JobStatus.RETRY_WAIT],
        timeout=15.0,
    )
    assert job_data_att1["status"] == JobStatus.RETRY_WAIT
    assert job_data_att1["attempt_count"] == 1

    # Verify attempt 1 recorded as FAILED
    attempts_res = api_client.get(f"/jobs/{job_id}/attempts/")
    assert len(attempts_res.data) == 1
    assert attempts_res.data[0]["status"] == AttemptStatus.FAILED
    assert attempts_res.data[0]["failure_type"] == FailureType.TRANSIENT

    # 3. Simulate passage of retry delay by setting available_at to now
    job = Job.objects.get(id=job_id)
    job.available_at = timezone.now() - timedelta(seconds=1)
    job.save(update_fields=["available_at"])

    # 4. Promote ready retry to WAITING
    promoted = promote_ready_retries()
    assert promoted >= 1

    job.refresh_from_db()
    assert job.status == JobStatus.WAITING

    # 5. Dispatch attempt 2 to Redis worker
    dispatch_and_publish_tenant(tenant.id)

    # 6. Wait for worker to execute attempt 2 to SUCCEEDED
    final_job_data = wait_for_job_status(
        api_client,
        job_id,
        expected_statuses=[JobStatus.SUCCEEDED],
        timeout=15.0,
    )
    assert final_job_data["status"] == JobStatus.SUCCEEDED
    assert final_job_data["result"] == {"processed": True}
    assert final_job_data["attempt_count"] == 2

    # Verify both attempts preserved in DB
    final_attempts_res = api_client.get(f"/jobs/{job_id}/attempts/")
    assert len(final_attempts_res.data) == 2


@pytest.mark.django_db(transaction=True)
def test_worker_e2e_permanent_failure_and_dlq_escalation(api_client, tenant):
    """
    Submits a job that triggers PermanentJobError.
    The live Celery worker fails the attempt, marks the Job FAILED_FINAL,
    and creates a DeadLetterJob record in the database.
    """
    check_live_worker_environment()
    payload = sample_job_payload(
        tenant,
        job_type=JobType.REPORTS,
        payload={"simulate_permanent_failure": True, "duration": 0.05},
    )

    create_res = api_client.post("/jobs/", payload, format="json")
    assert create_res.status_code == status.HTTP_201_CREATED
    job_id = str(create_res.data["id"])

    # Dispatch to Redis worker
    dispatch_and_publish_tenant(tenant.id)

    # Wait for Celery worker to fail job permanently
    final_job_data = wait_for_job_status(
        api_client,
        job_id,
        expected_statuses=[JobStatus.FAILED_FINAL],
        timeout=15.0,
    )
    assert final_job_data["status"] == JobStatus.FAILED_FINAL
    assert final_job_data["last_error_message"] is not None

    # Verify DeadLetterJob record created in DLQ API & DB
    dlq_list_res = api_client.get("/dead-letters/")
    assert dlq_list_res.status_code == status.HTTP_200_OK
    dlq_item = next((d for d in dlq_list_res.data if str(d["job"]) == job_id), None)
    assert dlq_item is not None
    assert dlq_item["reason"] == DeadLetterReason.PERMANENT_FAILURE
    assert dlq_item["status"] == DeadLetterStatus.OPEN


@pytest.mark.django_db(transaction=True)
def test_worker_e2e_dlq_replay_and_worker_success(api_client, tenant):
    """
    Submits a job that permanently fails into DLQ, then replays it via
    the DLQ API endpoint. Dispatches the newly spawned job to Redis,
    and asserts the live Celery worker completes the replayed job.
    """
    check_live_worker_environment()
    # 1. Create a failed job in DLQ
    failed_job = Job.objects.create(
        tenant=tenant,
        job_type=JobType.REPORTS,
        status=JobStatus.FAILED_FINAL,
        payload={"report_id": f"rep_dlq_replay_{uuid.uuid4().hex[:6]}", "duration": 0.05},
        available_at=timezone.now(),
        ready_since=timezone.now(),
    )
    dlq = DeadLetterJob.objects.create(
        job=failed_job,
        tenant=tenant,
        reason=DeadLetterReason.PERMANENT_FAILURE,
        status=DeadLetterStatus.OPEN,
        error_message="Initial unrecoverable failure",
    )

    # 2. Replay via API
    replay_res = api_client.post(f"/dead-letters/{dlq.id}/replay/")
    assert replay_res.status_code == status.HTTP_201_CREATED
    replayed_job_id = str(replay_res.data["id"])
    assert replay_res.data["status"] == JobStatus.WAITING

    # 3. Dispatch replayed job to Redis worker
    dispatch_and_publish_tenant(tenant.id)

    # 4. Wait for Celery worker to complete replayed job
    replayed_data = wait_for_job_status(
        api_client,
        replayed_job_id,
        expected_statuses=[JobStatus.SUCCEEDED],
        timeout=15.0,
    )
    assert replayed_data["status"] == JobStatus.SUCCEEDED
    assert replayed_data["result"] is not None

    # 5. Verify DLQ status transitioned to REPLAYED
    dlq.refresh_from_db()
    assert dlq.status == DeadLetterStatus.REPLAYED
    assert str(dlq.replayed_job_id) == replayed_job_id


@pytest.mark.django_db(transaction=True)
def test_worker_e2e_overlap_key_mutual_exclusion(api_client, tenant):
    """
    Tests overlap key locking with live Celery worker execution:
    Two jobs submitted with the same overlap_key ("res_database_dump").
    First job runs in worker. While job 1 runs, job 2 is blocked from dispatch.
    Once job 1 completes in worker, job 2 is dispatched and completed by worker.
    """
    check_live_worker_environment()
    overlap_key = f"res_lock_{uuid.uuid4().hex[:6]}"

    payload1 = sample_job_payload(
        tenant,
        job_type=JobType.SYNCHRONIZATION,
        payload={"overlap_key": overlap_key, "duration": 0.1},
    )
    payload2 = sample_job_payload(
        tenant,
        job_type=JobType.SYNCHRONIZATION,
        payload={"overlap_key": overlap_key, "duration": 0.05},
    )

    res1 = api_client.post("/jobs/", payload1, format="json")
    res2 = api_client.post("/jobs/", payload2, format="json")

    job_id_1 = str(res1.data["id"])
    job_id_2 = str(res2.data["id"])

    # Dispatch first job -> gets overlap lock
    d1 = dispatch_next(tenant.id)
    assert d1 is not None and str(d1.id) == job_id_1
    publish_pending()

    # Second job should NOT dispatch because overlap lock is held by job 1
    d2 = dispatch_next(tenant.id)
    assert d2 is None, "Job 2 must be blocked by overlap key while Job 1 holds lock"

    # Wait for Celery worker to complete Job 1
    wait_for_job_status(api_client, job_id_1, [JobStatus.SUCCEEDED], timeout=15.0)

    # Overlap lock is released on completion; now Job 2 can be dispatched
    d2_after = dispatch_next(tenant.id)
    assert d2_after is not None and str(d2_after.id) == job_id_2
    publish_pending()

    # Wait for Celery worker to complete Job 2
    wait_for_job_status(api_client, job_id_2, [JobStatus.SUCCEEDED], timeout=15.0)

    # Both succeeded
    j1 = Job.objects.get(id=job_id_1)
    j2 = Job.objects.get(id=job_id_2)
    assert j1.status == JobStatus.SUCCEEDED
    assert j2.status == JobStatus.SUCCEEDED


@pytest.mark.django_db(transaction=True)
def test_worker_e2e_multi_tenant_fairness(api_client, tenant, tenant_b):
    """
    Verifies multi-tenant job execution through the live Redis Celery worker:
    Tenants Alpha and Beta both submit jobs. Both sets of jobs are dispatched
    and processed in parallel by the Celery worker pool.
    """
    check_live_worker_environment()
    alpha_jobs = []
    beta_jobs = []

    for i in range(2):
        res_a = api_client.post(
            "/jobs/",
            sample_job_payload(tenant, job_type=JobType.NOTIFICATION, payload={"idx": i, "duration": 0.05}),
            format="json",
        )
        res_b = api_client.post(
            "/jobs/",
            sample_job_payload(tenant_b, job_type=JobType.REPORTS, payload={"idx": i, "duration": 0.05}),
            format="json",
        )
        alpha_jobs.append(str(res_a.data["id"]))
        beta_jobs.append(str(res_b.data["id"]))

    # Dispatch both tenants across both batches
    for _ in range(2):
        dispatch_and_publish_tenant(tenant.id)
        dispatch_and_publish_tenant(tenant_b.id)

    # Wait for all jobs across both tenants to reach SUCCEEDED
    for jid in alpha_jobs + beta_jobs:
        wait_for_job_status(api_client, jid, [JobStatus.SUCCEEDED], timeout=20.0)

    for jid in alpha_jobs + beta_jobs:
        job = Job.objects.get(id=jid)
        assert job.status == JobStatus.SUCCEEDED
        assert job.attempts.filter(status=AttemptStatus.SUCCEEDED).exists()


# ---------------------------------------------------------------------
# Direct Test Runner (Executes against live database and Redis worker)
# ---------------------------------------------------------------------


def create_test_tenant(name_prefix="Test Tenant", max_concurrent_jobs=5):
    t = Tenant.objects.create(
        name=f"{name_prefix} [{uuid.uuid4().hex[:6]}]",
        max_concurrent_jobs=max_concurrent_jobs,
    )
    TenantSchedulerState.objects.create(tenant=t)
    return t


def run_live_e2e_suite():
    """
    Runs the comprehensive test suite directly against the real PostgreSQL
    database and live Redis Celery worker, printing detailed results and
    leaving all generated data preserved in the database.
    """
    print("\n" + "=" * 80, flush=True)
    print("  MULTI-TENANT JOB PLATFORM - LIVE DATABASE & REDIS WORKER TEST SUITE", flush=True)
    print("=" * 80, flush=True)

    client = APIClient()

    tests_to_run = [
        ("API: Create Tenant", lambda: test_api_create_tenant(client)),
        ("API: List Tenants", lambda: test_api_list_tenants(client, create_test_tenant("ListA"), create_test_tenant("ListB"))),
        ("API: Retrieve Tenant", lambda: test_api_retrieve_tenant(client, create_test_tenant("Ret"))),
        ("API: Update Tenant", lambda: test_api_update_tenant(client, create_test_tenant("Upd"))),
        ("API: Submit Job (Success)", lambda: test_api_submit_job_success(client, create_test_tenant("Sub"), None)),
        ("API: Idempotent Submission", lambda: test_api_submit_job_idempotent_duplicate(client, create_test_tenant("Idem"), None)),
        ("API: Idempotency Tenant Scoped", lambda: test_api_submit_job_idempotency_is_tenant_scoped(client, create_test_tenant("IdemA"), create_test_tenant("IdemB"), None)),
        ("API: Cancel Waiting Job", lambda: test_api_cancel_waiting_job(client, create_test_tenant("CanWait"), None)),
        ("API: Cancel Queued Job", lambda: test_api_cancel_queued_job_releases_capacity(client, create_test_tenant("CanQueue"), None)),
        ("API: Dead Letter Lifecycle", lambda: test_api_replay_dead_letter(client, create_test_tenant("DLQ"), None)),
        ("WORKER: E2E Report Job Execution", lambda: test_worker_e2e_successful_job_execution(client, create_test_tenant("WorkerRep"))),
        ("WORKER: E2E All Job Types (5 Types)", lambda: test_worker_e2e_all_job_types_execution(client, create_test_tenant("WorkerAll"))),
        ("WORKER: Concurrency Throttling", lambda: test_worker_e2e_tenant_concurrency_limiting(client)),
        ("WORKER: Retry Flow & Promotion", lambda: test_worker_e2e_temporary_failure_retry_flow(client, create_test_tenant("WorkerRetry"))),
        ("WORKER: Permanent Failure & DLQ", lambda: test_worker_e2e_permanent_failure_and_dlq_escalation(client, create_test_tenant("WorkerPerm"))),
        ("WORKER: DLQ Replay Worker Execution", lambda: test_worker_e2e_dlq_replay_and_worker_success(client, create_test_tenant("WorkerReplay"))),
        ("WORKER: Overlap Key Mutual Exclusion", lambda: test_worker_e2e_overlap_key_mutual_exclusion(client, create_test_tenant("WorkerOverlap"))),
        ("WORKER: Multi-Tenant Parallel Fairness", lambda: test_worker_e2e_multi_tenant_fairness(client, create_test_tenant("FairA"), create_test_tenant("FairB"))),
    ]

    passed = 0
    failed = 0

    for name, test_fn in tests_to_run:
        start_time = time.time()
        try:
            test_fn()
            duration = (time.time() - start_time) * 1000
            print(f"  \033[92m[PASS]\033[0m {name:<45} ({duration:6.1f}ms)", flush=True)
            passed += 1
        except Exception as e:
            duration = (time.time() - start_time) * 1000
            print(f"  \033[91m[FAIL]\033[0m {name:<45} ({duration:6.1f}ms)", flush=True)
            print(f"         Error: {type(e).__name__}: {e}", flush=True)
            failed += 1

    print("-" * 80, flush=True)
    print(f"  Summary: {passed} passed, {failed} failed out of {len(tests_to_run)} tests.", flush=True)
    print(f"  Database Data Preservation:", flush=True)
    print(f"    - Total Tenants in DB: {Tenant.objects.count()}", flush=True)
    print(f"    - Total Jobs in DB:    {Job.objects.count()}", flush=True)
    print(f"    - Total Attempts in DB:{JobAttempt.objects.count()}", flush=True)
    print(f"    - Total DLQ in DB:     {DeadLetterJob.objects.count()}", flush=True)
    print(f"    - Total Outbox in DB:  {JobOutbox.objects.count()}", flush=True)
    print("=" * 80 + "\n", flush=True)

    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    run_live_e2e_suite()
