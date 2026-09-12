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
        if isinstance(data, dict) and "job_type" in data and isinstance(data["job_type"], str):
            raw = data["job_type"].strip().upper().replace(" ", "_")
            data = data.copy()
            data["job_type"] = raw
        return super().to_internal_value(data)


class JobAttemptSerializer(serializers.ModelSerializer):
    class Meta:
        model = JobAttempt
        fields = "__all__"


class JobExecutionReservationSerializer(serializers.ModelSerializer):
    class Meta:
        model = JobExecutionReservation
        fields = "__all__"


class JobOverlapLockSerializer(serializers.ModelSerializer):
    class Meta:
        model = JobOverlapLock
        fields = "__all__"


class JobOutboxSerializer(serializers.ModelSerializer):
    class Meta:
        model = JobOutbox
        fields = "__all__"


class DeadLetterJobSerializer(serializers.ModelSerializer):
    class Meta:
        model = DeadLetterJob
        fields = "__all__"


class TenantSchedulerStateSerializer(serializers.ModelSerializer):
    class Meta:
        model = TenantSchedulerState
        fields = "__all__"