class InvalidJobTransition(Exception):
    pass


class JobError(Exception):
    pass


class RetryableJobError(JobError):
    pass


class PermanentJobError(JobError):
    pass


class InvalidJobTransition(JobError):
    pass