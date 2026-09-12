"""
Jobs Celery Background Tasks

Purpose:
    Defines the Celery asynchronous task definitions that drive worker execution, periodic job dispatching,
    transactional outbox publishing, and retry promotions.

Use Case:
    - `execute_job`: Runs claimed jobs in worker worker processes with late acknowledgments.
    - `dispatch_jobs`: Scheduled loop that finds tenants with waiting work and triggers dispatch.
    - `publish_outbox`: Scheduled loop that drains pending outbox events to Redis.
    - `process_retries`: Scheduled loop that promotes retry-waiting jobs back to waiting.
"""

from celery import shared_task

from jobs.models import TenantSchedulerState
from jobs.services.dispatcher import dispatch_next
from jobs.services.execution import execute
from jobs.services.outbox import publish_pending
from jobs.services.retry import promote_ready_retries


@shared_task(
    acks_late=True,
    reject_on_worker_lost=True,
)
def execute_job(job_id):
    """
    Celery task that executes a single job.
    Configured with `acks_late=True` and `reject_on_worker_lost=True` to guarantee resilience if a worker dies.

    Args:
        job_id (str): UUID string of the Job to execute.
    """
    execute(job_id)


@shared_task
def process_retries():
    """
    Periodic task that checks for jobs whose retry backoff timers have elapsed and promotes them to WAITING.
    """
    return promote_ready_retries()


@shared_task
def publish_outbox():
    """
    Periodic task that publishes pending dispatch events from PostgreSQL to Celery.
    """
    publish_pending()


@shared_task
def dispatch_jobs():
    """
    Periodic scheduler task that finds tenants with ready work and dispatches their next highest-priority jobs.
    """
    tenant_ids = (
        TenantSchedulerState.objects
        .filter(has_ready_work=True)
        .values_list("tenant_id", flat=True)[:100]
    )

    for tenant_id in tenant_ids:
        dispatch_next(tenant_id)