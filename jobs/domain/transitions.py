from jobs.models import JobStatus


ALLOWED_TRANSITIONS = {
    JobStatus.WAITING: {
        JobStatus.QUEUED,
        JobStatus.CANCELLED,
    },
    JobStatus.QUEUED: {
        JobStatus.EXECUTING,
        JobStatus.CANCEL_REQUESTED,
        JobStatus.WAITING,
    },
    JobStatus.EXECUTING: {
        JobStatus.SUCCEEDED,
        JobStatus.RETRY_WAIT,
        JobStatus.FAILED_FINAL,
        JobStatus.CANCEL_REQUESTED,
    },
    JobStatus.RETRY_WAIT: {
        JobStatus.WAITING,
        JobStatus.CANCELLED,
    },
    JobStatus.CANCEL_REQUESTED: {
        JobStatus.CANCELLED,
        JobStatus.SUCCEEDED,
    },
}