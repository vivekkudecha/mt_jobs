from rest_framework import serializers

from jobs.models import (
    DeadLetterJob,
    Job,
    JobAttempt,
    JobExecutionReservation,
    JobOutbox,
    JobOverlapLock,
    RetryPolicy,
    TenantSchedulerState,
)


class JobSerializer(serializers.ModelSerializer):
    class Meta:
        model = Job
        fields = "__all__"


class JobAttemptSerializer(serializers.ModelSerializer):
    class Meta:
        model = JobAttempt
        fields = "__all__"


class RetryPolicySerializer(serializers.ModelSerializer):
    class Meta:
        model = RetryPolicy
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