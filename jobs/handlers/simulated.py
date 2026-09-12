import random
import time

from jobs.domain.exceptions import (
    PermanentJobError,
    RetryableJobError,
)
from jobs.handlers.base import BaseJobHandler
from jobs.handlers.registry import register


def _sleep(payload, default=(2, 5)):
    seconds = payload.get("duration")
    time.sleep(seconds or random.randint(*default))


@register("REPORT")
class ReportHandler(BaseJobHandler):
    def execute(self, payload, context):
        _sleep(payload, (3, 8))
        return {"report_id": payload.get("report_id"), "status": "generated"}


@register("DATA_PROCESSING")
class DataProcessingHandler(BaseJobHandler):
    def execute(self, payload, context):
        _sleep(payload, (5, 12))
        return {"processed": True}


@register("SYNC")
class SyncHandler(BaseJobHandler):
    def execute(self, payload, context):
        _sleep(payload, (4, 10))
        return {"synced": True}


@register("AI")
class AIHandler(BaseJobHandler):
    def execute(self, payload, context):
        _sleep(payload, (8, 15))
        return {"processed": True}


@register("NOTIFICATION")
class NotificationHandler(BaseJobHandler):
    def execute(self, payload, context):
        _sleep(payload, (1, 3))
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