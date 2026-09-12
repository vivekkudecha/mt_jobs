"""
Simulated Job Handlers

Purpose:
    Provides concrete implementations for all supported job categories and test scenarios
    (Reports, Data Processing, Sync, AI, Notifications, and Failure Simulation).

Use Case:
    Simulates realistic asynchronous workloads with variable execution durations and programmable
    failure modes for local integration testing, API benchmarks, and resilience demonstrations.
"""

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
    """
    Helper function to simulate variable work duration and inject simulated failures.
    """
    # Check for forced simulated permanent failure
    if payload.get("simulate_permanent_failure"):
        raise PermanentJobError("Simulated permanent failure")

    # Check for forced simulated temporary failure up to a given attempt number
    if payload.get("simulate_temporary_failure"):
        fail_until = payload.get("fail_until_attempt", 1)
        if context and context.attempt and context.attempt.attempt_number <= fail_until:
            raise RetryableJobError("Simulated temporary failure")

    # Simulate realistic execution duration
    seconds = payload.get("duration")
    time.sleep(seconds or random.randint(*default))


@register(JobType.REPORTS)
@register("REPORT")
class ReportHandler(BaseJobHandler):
    """Simulates generating PDF/CSV analytical reports (3 to 8 seconds)."""

    def execute(self, payload, context):
        _sleep(payload, (3, 8), context=context)
        return {"report_id": payload.get("report_id"), "status": "generated"}


@register(JobType.DATA_PROCESSING)
class DataProcessingHandler(BaseJobHandler):
    """Simulates batch ETL data transformation jobs (5 to 12 seconds)."""

    def execute(self, payload, context):
        _sleep(payload, (5, 12), context=context)
        return {"processed": True}


@register(JobType.SYNCHRONIZATION)
@register("SYNC")
class SyncHandler(BaseJobHandler):
    """Simulates synchronizing records with external third-party APIs (4 to 10 seconds)."""

    def execute(self, payload, context):
        _sleep(payload, (4, 10), context=context)
        return {"synced": True}


@register(JobType.AI_PROCESSING)
@register("AI")
class AIHandler(BaseJobHandler):
    """Simulates running deep learning or LLM inference workflows (8 to 15 seconds)."""

    def execute(self, payload, context):
        _sleep(payload, (8, 15), context=context)
        return {"processed": True}


@register(JobType.NOTIFICATION)
class NotificationHandler(BaseJobHandler):
    """Simulates sending transactional emails, webhooks, or push notifications (1 to 3 seconds)."""

    def execute(self, payload, context):
        _sleep(payload, (1, 3), context=context)
        return {"sent": True}


@register("TEMPORARY_FAILURE")
class TemporaryFailureHandler(BaseJobHandler):
    """Test handler that fails with RetryableJobError until a specified attempt threshold."""

    def execute(self, payload, context):
        fail_until = payload.get("fail_until_attempt", 2)

        if context.attempt.attempt_number <= fail_until:
            raise RetryableJobError("Simulated temporary failure")

        return {"success_after_retry": True}


@register("PERMANENT_FAILURE")
class PermanentFailureHandler(BaseJobHandler):
    """Test handler that always raises PermanentJobError to verify Dead Letter Queue routing."""

    def execute(self, payload, context):
        raise PermanentJobError("Simulated permanent failure")


@register("OVERLAP")
class OverlapHandler(BaseJobHandler):
    """Test handler that holds a resource lock to test mutual exclusion."""

    def execute(self, payload, context):
        _sleep(payload, (5, 10))
        return {
            "resource": payload.get("overlap_key"),
            "completed": True,
        }