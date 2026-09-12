from rest_framework import generics
from rest_framework.response import Response
from rest_framework import status

from jobs.models import Job, JobAttempt, DeadLetterJob
from jobs.serializers import (
    JobSerializer,
    JobAttemptSerializer,
    DeadLetterJobSerializer,
)

from jobs.services.submission import submit_job
from jobs.services.cancellation import cancel_job
from jobs.services.scheduler_state import refresh_scheduler_state
from jobs.services.dead_letter import replay_dead_letter



class JobListCreateView(generics.ListCreateAPIView):
    queryset = Job.objects.all()
    serializer_class = JobSerializer

    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        job_data = serializer.validated_data.copy()
        idempotency_key = (
            request.headers.get("Idempotency-Key")
            or request.headers.get("X-Idempotency-Key")
            or request.data.get("idempotency_key")
        )
        if idempotency_key and "idempotency_key" not in job_data:
            job_data["idempotency_key"] = idempotency_key

        job, created = submit_job(**job_data)

        if created:
            refresh_scheduler_state(job.tenant_id)

        return Response(
            self.get_serializer(job).data,
            status=status.HTTP_201_CREATED if created else status.HTTP_200_OK,
        )

class JobCancelView(generics.GenericAPIView):
    queryset = Job.objects.all()
    serializer_class = JobSerializer

    def post(self, request, *args, **kwargs):
        job = cancel_job(self.kwargs["pk"])

        return Response(
            self.get_serializer(job).data,
            status=status.HTTP_200_OK,
        )

class JobDetailView(generics.RetrieveAPIView):
    queryset = Job.objects.all()
    serializer_class = JobSerializer


class JobAttemptListView(generics.ListAPIView):
    serializer_class = JobAttemptSerializer

    def get_queryset(self):
        return JobAttempt.objects.filter(
            job_id=self.kwargs["job_id"]
        )


class DeadLetterListView(generics.ListAPIView):
    queryset = DeadLetterJob.objects.all()
    serializer_class = DeadLetterJobSerializer


class DeadLetterReplayView(generics.GenericAPIView):
    serializer_class = JobSerializer

    def post(self, request, *args, **kwargs):
        job = replay_dead_letter(self.kwargs["pk"])

        return Response(
            self.get_serializer(job).data,
            status=status.HTTP_201_CREATED,
        )


class DeadLetterDetailView(generics.RetrieveAPIView):
    queryset = DeadLetterJob.objects.all()
    serializer_class = DeadLetterJobSerializer