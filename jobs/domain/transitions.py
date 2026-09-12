"""
Job State Transitions Map

Purpose:
    Defines the complete graph of permitted lifecycle state movements for any Job.

Use Case:
    Provides the exact lookup table used by `validate_transition` to ensure a job cannot
    skip critical steps or move backward into invalid states.
"""

from jobs.models import JobStatus


# Mapping from current JobStatus -> set of permitted destination JobStatuses
ALLOWED_TRANSITIONS = {
    # WAITING: Brand new job or retry promoted job ready for scheduling
    JobStatus.WAITING: {
        JobStatus.QUEUED,       # Picked up by the dispatcher and slot reserved
        JobStatus.CANCELLED,    # Cancelled by user before dispatch
    },

    # QUEUED: Slot is reserved, waiting for Celery worker to pick it up
    JobStatus.QUEUED: {
        JobStatus.EXECUTING,         # Worker has claimed the job
        JobStatus.CANCEL_REQUESTED,  # Cancellation requested while waiting for worker
        JobStatus.WAITING,           # Reconciliation recovered lost queued job
    },

    # EXECUTING: Currently running on a worker thread
    JobStatus.EXECUTING: {
        JobStatus.SUCCEEDED,         # Handler finished successfully
        JobStatus.RETRY_WAIT,        # Handler failed transiently; waiting for backoff delay
        JobStatus.FAILED_FINAL,      # Retries exhausted or permanent fatal error
        JobStatus.CANCEL_REQUESTED,  # User requested cancel while job is running
    },

    # RETRY_WAIT: Waiting for exponential backoff timer to expire
    JobStatus.RETRY_WAIT: {
        JobStatus.WAITING,      # Backoff delay elapsed; ready for redispatch
        JobStatus.CANCELLED,    # User cancelled during retry wait
    },

    # CANCEL_REQUESTED: Worker notified to stop execution
    JobStatus.CANCEL_REQUESTED: {
        JobStatus.CANCELLED,    # Worker acknowledged cancellation and stopped
        JobStatus.SUCCEEDED,    # Worker finished before cancel signal was processed
    },
}