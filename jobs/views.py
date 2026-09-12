"""
Jobs REST API Views

Purpose:
    Django REST Framework API views for job submission, status querying, execution attempts history,
    job cancellation, and dead-letter queue inspection/replaying.

Use Case:
    Exposes external HTTP endpoints for client apps, operators, and microservices to interact with the orchestrator.
"""

from rest_framework import generics, status
from rest_framework.response import Response

from jobs.models import DeadLetterJob, Job, JobAttempt
from jobs.serializers import (
    DeadLetterJobSerializer,
    JobAttemptSerializer,
    JobSerializer,
)
from jobs.services.cancellation import cancel_job
from jobs.services.dead_letter import replay_dead_letter
from jobs.services.scheduler_state import refresh_scheduler_state
from jobs.services.submission import submit_job


class JobListCreateView(generics.ListCreateAPIView):
    """
    GET /jobs/ - Lists all submitted jobs.
    POST /jobs/ - Submits a new job with idempotency key deduplication.
    """
    queryset = Job.objects.all()
    serializer_class = JobSerializer

    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        job_data = serializer.validated_data.copy()

        # Extract idempotency key from headers or request payload
        idempotency_key = (
            request.headers.get("Idempotency-Key")
            or request.headers.get("X-Idempotency-Key")
            or request.data.get("idempotency_key")
        )
        if idempotency_key and "idempotency_key" not in job_data:
            job_data["idempotency_key"] = idempotency_key

        # Submit job via submission service
        job, created = submit_job(**job_data)

        # Notify scheduler if a brand-new job was created
        if created:
            refresh_scheduler_state(job.tenant_id)

        return Response(
            self.get_serializer(job).data,
            status=status.HTTP_201_CREATED if created else status.HTTP_200_OK,
        )


class JobCancelView(generics.GenericAPIView):
    """
    POST /jobs/<uuid:pk>/cancel/ - Cancels an active or waiting job.
    """
    queryset = Job.objects.all()
    serializer_class = JobSerializer

    def post(self, request, *args, **kwargs):
        job = cancel_job(self.kwargs["pk"])

        return Response(
            self.get_serializer(job).data,
            status=status.HTTP_200_OK,
        )


class JobDetailView(generics.RetrieveAPIView):
    """
    GET /jobs/<uuid:pk>/ - Retrieves status, payload, and result details of a single job.
    """
    queryset = Job.objects.all()
    serializer_class = JobSerializer


class JobAttemptListView(generics.ListAPIView):
    """
    GET /jobs/<uuid:job_id>/attempts/ - Lists all historical execution attempts for a job.
    """
    serializer_class = JobAttemptSerializer

    def get_queryset(self):
        return JobAttempt.objects.filter(
            job_id=self.kwargs["job_id"]
        )


class DeadLetterListView(generics.ListAPIView):
    """
    GET /dead-letters/ - Lists all jobs in the Dead Letter Queue.
    """
    queryset = DeadLetterJob.objects.all()
    serializer_class = DeadLetterJobSerializer


class DeadLetterReplayView(generics.GenericAPIView):
    """
    POST /dead-letters/<uuid:pk>/replay/ - Replays a dead-lettered job by spawning a fresh clone.
    """
    serializer_class = JobSerializer

    def post(self, request, *args, **kwargs):
        job = replay_dead_letter(self.kwargs["pk"])

        return Response(
            self.get_serializer(job).data,
            status=status.HTTP_201_CREATED,
        )


class DeadLetterDetailView(generics.RetrieveAPIView):
    """
    GET /dead-letters/<uuid:pk>/ - Retrieves diagnostic details of a dead-letter queue entry.
    """
    queryset = DeadLetterJob.objects.all()
    serializer_class = DeadLetterJobSerializer