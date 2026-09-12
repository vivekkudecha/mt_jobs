from jobs.views import DeadLetterReplayView
from django.urls import path

from .views import (
    JobListCreateView,
    JobCancelView,
    JobDetailView,
    JobAttemptListView,
    DeadLetterListView,
    DeadLetterDetailView,
)


urlpatterns = [
    path("jobs/", JobListCreateView.as_view()),
    path("jobs/<uuid:pk>/", JobDetailView.as_view()),
    path("jobs/<uuid:job_id>/attempts/", JobAttemptListView.as_view()),
    path("jobs/<uuid:pk>/cancel/", JobCancelView.as_view()),

    path("dead-letters/", DeadLetterListView.as_view()),
    path("dead-letters/<uuid:pk>/", DeadLetterDetailView.as_view()),
    path("dead-letters/<uuid:pk>/replay/", DeadLetterReplayView.as_view()),
]