"""
Job Handler Registry

Purpose:
    Maintains a dynamic registry mapping string job types to singleton handler instances.

Use Case:
    Handlers register themselves using `@register(JobType.REPORTS)`. When a worker runs a job,
    it dynamically retrieves the handler with `get_handler(job.job_type)`.
"""

# Internal registry mapping: job_type_string -> handler_instance
_registry = {}


def register(job_type):
    """
    Class decorator that registers a handler for the given job_type.

    Example:
        @register(JobType.REPORTS)
        class ReportHandler(BaseJobHandler):
            ...
    """
    def decorator(cls):
        _registry[job_type] = cls()
        return cls

    return decorator


def get_handler(job_type):
    """
    Retrieves the registered handler instance for a given job type.

    Args:
        job_type (str): The string job type identifier.

    Returns:
        BaseJobHandler: The instantiated singleton handler.

    Raises:
        ValueError: If no handler is registered for the specified job_type.
    """
    handler = _registry.get(job_type)

    if not handler:
        raise ValueError(f"No handler registered for: {job_type}")

    return handler