"""
Jobs DRF Serializers

Purpose:
    Serializes and deserializes data models to/from JSON for the REST API endpoints.
    Normalizes inputs (such as case-insensitive job type formatting).

Use Case:
    Used by views to validate incoming HTTP request payloads and format JSON API responses.
"""

from rest_framework import serializers

from jobs.models import (
    DeadLetterJob,
    Job,
    JobAttempt,
    JobExecutionReservation,
    JobOutbox,
    JobOverlapLock,
    TenantSchedulerState,
)


class JobSerializer(serializers.ModelSerializer):
    """
    Serializer for Job model instances with input normalization.
    """
    class Meta:
        model = Job
        fields = "__all__"
        read_only_fields = (
            "ready_since",
            "idempotency_key",
        )
        validators = []
        extra_kwargs = {
            "available_at": {"required": False},
        }

    def to_internal_value(self, data):
        """
        Normalizes job_type strings (e.g. 'reports' or 'data processing' -> 'REPORTS' or 'DATA_PROCESSING').
        """
        if isinstance(data, dict) and "job_type" in data and isinstance(data["job_type"], str):
            raw = data["job_type"].strip().upper().replace(" ", "_")
            data = data.copy()
            data["job_type"] = raw
        return super().to_internal_value(data)


class JobAttemptSerializer(serializers.ModelSerializer):
    """Serializer for JobAttempt execution history records."""
    class Meta:
        model = JobAttempt
        fields = "__all__"


class JobExecutionReservationSerializer(serializers.ModelSerializer):
    """Serializer for tenant execution reservation slots."""
    class Meta:
        model = JobExecutionReservation
        fields = "__all__"


class JobOverlapLockSerializer(serializers.ModelSerializer):
    """Serializer for resource overlap locks."""
    class Meta:
        model = JobOverlapLock
        fields = "__all__"


class JobOutboxSerializer(serializers.ModelSerializer):
    """Serializer for transactional outbox dispatch events."""
    class Meta:
        model = JobOutbox
        fields = "__all__"


class DeadLetterJobSerializer(serializers.ModelSerializer):
    """Serializer for DeadLetterJob records."""
    class Meta:
        model = DeadLetterJob
        fields = "__all__"


class TenantSchedulerStateSerializer(serializers.ModelSerializer):
    """Serializer for TenantSchedulerState summary records."""
    class Meta:
        model = TenantSchedulerState
        fields = "__all__"