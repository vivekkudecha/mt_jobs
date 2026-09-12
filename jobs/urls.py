"""
Jobs URL Routing

Purpose:
    Maps HTTP REST API endpoint paths to their respective Django REST Framework views.

Endpoints:
    - POST /jobs/                      : Submit a new job (idempotent via header/payload)
    - GET  /jobs/                      : List all jobs
    - GET  /jobs/<uuid:pk>/            : Get job status and execution details
    - GET  /jobs/<uuid:job_id>/attempts/ : Get execution history attempts for a job
    - POST /jobs/<uuid:pk>/cancel/     : Cancel a job
    - GET  /dead-letters/              : List all dead-lettered jobs
    - GET  /dead-letters/<uuid:pk>/    : Get dead-letter job diagnostic details
    - POST /dead-letters/<uuid:pk>/replay/ : Replay a dead-lettered job
"""

from django.urls import path

from .views import (
    DeadLetterDetailView,
    DeadLetterListView,
    DeadLetterReplayView,
    JobAttemptListView,
    JobCancelView,
    JobDetailView,
    JobListCreateView,
)


urlpatterns = [
    # Job CRUD and Lifecycle
    path("jobs/", JobListCreateView.as_view()),
    path("jobs/<uuid:pk>/", JobDetailView.as_view()),
    path("jobs/<uuid:job_id>/attempts/", JobAttemptListView.as_view()),
    path("jobs/<uuid:pk>/cancel/", JobCancelView.as_view()),

    # Dead Letter Queue (DLQ) Management
    path("dead-letters/", DeadLetterListView.as_view()),
    path("dead-letters/<uuid:pk>/", DeadLetterDetailView.as_view()),
    path("dead-letters/<uuid:pk>/replay/", DeadLetterReplayView.as_view()),
]