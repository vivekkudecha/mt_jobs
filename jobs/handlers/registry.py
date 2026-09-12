_registry = {}


def register(job_type):
    def decorator(cls):
        _registry[job_type] = cls()
        return cls

    return decorator


def get_handler(job_type):
    handler = _registry.get(job_type)

    if not handler:
        raise ValueError(f"No handler registered for: {job_type}")

    return handler