import random
import time

from jobs.domain.exceptions import (
    PermanentJobError,
    RetryableJobError,
)
from jobs.handlers.base import BaseJobHandler
from jobs.handlers.registry import register
from jobs.models.job import JobType


def _sleep(payload, default=(2, 5), context=None):
    if payload.get("simulate_permanent_failure"):
        raise PermanentJobError("Simulated permanent failure")
    if payload.get("simulate_temporary_failure"):
        fail_until = payload.get("fail_until_attempt", 1)
        if context and context.attempt and context.attempt.attempt_number <= fail_until:
            raise RetryableJobError("Simulated temporary failure")

    seconds = payload.get("duration")
    time.sleep(seconds or random.randint(*default))


@register(JobType.REPORTS)
@register("REPORT")
class ReportHandler(BaseJobHandler):
    def execute(self, payload, context):
        _sleep(payload, (3, 8), context=context)
        return {"report_id": payload.get("report_id"), "status": "generated"}


@register(JobType.DATA_PROCESSING)
class DataProcessingHandler(BaseJobHandler):
    def execute(self, payload, context):
        _sleep(payload, (5, 12), context=context)
        return {"processed": True}


@register(JobType.SYNCHRONIZATION)
@register("SYNC")
class SyncHandler(BaseJobHandler):
    def execute(self, payload, context):
        _sleep(payload, (4, 10), context=context)
        return {"synced": True}


@register(JobType.AI_PROCESSING)
@register("AI")
class AIHandler(BaseJobHandler):
    def execute(self, payload, context):
        _sleep(payload, (8, 15), context=context)
        return {"processed": True}


@register(JobType.NOTIFICATION)
class NotificationHandler(BaseJobHandler):
    def execute(self, payload, context):
        _sleep(payload, (1, 3), context=context)
        return {"sent": True}


@register("TEMPORARY_FAILURE")
class TemporaryFailureHandler(BaseJobHandler):
    def execute(self, payload, context):
        fail_until = payload.get("fail_until_attempt", 2)

        if context.attempt.attempt_number <= fail_until:
            raise RetryableJobError("Simulated temporary failure")

        return {"success_after_retry": True}


@register("PERMANENT_FAILURE")
class PermanentFailureHandler(BaseJobHandler):
    def execute(self, payload, context):
        raise PermanentJobError("Simulated permanent failure")


@register("OVERLAP")
class OverlapHandler(BaseJobHandler):
    def execute(self, payload, context):
        _sleep(payload, (5, 10))
        return {
            "resource": payload.get("overlap_key"),
            "completed": True,
        }