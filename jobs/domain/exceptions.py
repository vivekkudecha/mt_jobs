"""
Job Domain Exceptions

Purpose:
    Defines specialized exceptions for job execution, state validation, and error classification.

Use Case:
    Job handlers throw these exceptions to instruct the execution engine whether to retry a job
    (`RetryableJobError`) or fail immediately and move to Dead Letter Queue (`PermanentJobError`).
"""


class JobError(Exception):
    """Base exception for all job execution related failures."""
    pass


class RetryableJobError(JobError):
    """
    Thrown when a job encounters a transient, recoverable failure.
    Examples: Network timeout, temporary third-party API outage, database lock contention.
    The system will retry this job following exponential backoff delays.
    """
    pass


class PermanentJobError(JobError):
    """
    Thrown when a job encounters an unrecoverable, permanent failure.
    Examples: Malformed JSON payload, invalid account credentials, business logic constraint violation.
    The system will NOT retry this job and will route it directly to the Dead Letter Queue.
    """
    pass


class InvalidJobTransition(JobError):
    """
    Thrown when a job attempts to transition between incompatible states
    (e.g., trying to execute a job that is already cancelled or succeeded).
    """
    pass