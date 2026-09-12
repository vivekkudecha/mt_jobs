import os

from celery import Celery


# Celery runs outside Django's normal manage.py lifecycle,
# so make sure Django settings are available when a worker starts.
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")


# Single Celery application for the whole job-processing platform.
app = Celery("job_orchestrator")


# Read Celery configuration from Django settings.
#
# namespace="CELERY" means:
#
# broker_url       -> CELERY_BROKER_URL
# result_backend   -> CELERY_RESULT_BACKEND
# task_serializer  -> CELERY_TASK_SERIALIZER
#
# This keeps Celery configuration centralized in Django settings.
app.config_from_object(
    "django.conf:settings",
    namespace="CELERY",
)


# Automatically discovers:
#
# apps/jobs/tasks.py
# apps/tenants/tasks.py
# etc.
#
# from Django INSTALLED_APPS.
app.autodiscover_tasks()