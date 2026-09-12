import threading
import uuid
from datetime import timedelta

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
    JobPriority,
    JobStatus,
    JobType,
    TenantSchedulerState,
)
from jobs.services.dispatcher import dispatch_next
from jobs.services.execution import claim_job, complete_job
from jobs.services.failure import fail_attempt, finalize_failure
from tenants.models import Tenant

pytestmark = pytest.mark.django_db


# ---------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------


@pytest.fixture
def api_client():
    return APIClient()


@pytest.fixture
def tenant():
    t = Tenant.objects.create(
        name="Tenant Alpha",
        max_concurrent_jobs=2,
    )
    TenantSchedulerState.objects.create(
        tenant=t,
    )
    return t


@pytest.fixture
def tenant_b():
    t = Tenant.objects.create(
        name="Tenant Beta",
        max_concurrent_jobs=2,
    )
    TenantSchedulerState.objects.create(
        tenant=t,
    )
    return t


@pytest.fixture
def retry_policy():
    return None


def sample_job_payload(tenant, retry_policy=None, **overrides):
    now = timezone.now()
    data = {
        "tenant": str(tenant.id),
        "job_type": JobType.DATA_PROCESSING,
        "priority": JobPriority.NORMAL,
        "payload": {"dataset": "users_2026", "batch_size": 100},
        "available_at": now.isoformat(),
    }
    data.update(overrides)
    return data


# ---------------------------------------------------------------------
# Tenant API Tests
# ---------------------------------------------------------------------


def test_api_create_tenant(api_client):
    payload = {
        "name": "Acme Corp",
        "max_concurrent_jobs": 5,
    }
    response = api_client.post("/tenants/", payload, format="json")

    assert response.status_code == status.HTTP_201_CREATED
    data = response.data
    assert data["name"] == "Acme Corp"
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
    response = api_client.patch(
        f"/tenants/{tenant.id}/",
        {"max_concurrent_jobs": 10, "name": "Tenant Alpha Updated"},
        format="json",
    )

    assert response.status_code == status.HTTP_200_OK
    tenant.refresh_from_db()
    assert tenant.max_concurrent_jobs == 10
    assert tenant.name == "Tenant Alpha Updated"


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
        payload={"recipient": "user@example.com", "template": "welcome"},
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
    idempotency_key = "unique-order-sync-12345"
    payload = sample_job_payload(
        tenant,
        retry_policy,
        idempotency_key=idempotency_key,
        payload={"order_id": 42},
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
    idempotency_key = "shared-request-uuid-999"
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
    idempotency_key = "header-key-test-456"
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
        payload={"task": "sync_inventory"},
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
# End-to-End Orchestration via REST API
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
        payload={"order_id": 999},
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
        name="Concurrent Tenant",
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
                retry_policy,
                idempotency_key=idempotency_key,
                payload={"concurrent": True},
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

