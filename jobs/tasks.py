from celery import shared_task

from jobs.services.dispatcher import dispatch_next
from jobs.services.execution import execute
from jobs.services.outbox import publish_pending
from jobs.services.retry import promote_ready_retries
from jobs.models import TenantSchedulerState


@shared_task(
    acks_late=True,
    reject_on_worker_lost=True,
)
def execute_job(job_id):
    execute(job_id)


@shared_task
def process_retries():
    return promote_ready_retries()


@shared_task
def publish_outbox():
    publish_pending()


@shared_task
def dispatch_jobs():
    tenant_ids = (
        TenantSchedulerState.objects
        .filter(has_ready_work=True)
        .values_list("tenant_id", flat=True)[:100]
    )

    for tenant_id in tenant_ids:
        dispatch_next(tenant_id)