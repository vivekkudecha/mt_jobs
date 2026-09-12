"""
State Machine Validator for Jobs

Purpose:
    Guarantees that jobs only transition through legal, predefined states.
    Prevents corrupting the job lifecycle (e.g. going from SUCCEEDED directly to EXECUTING).

Use Case:
    Whenever any service or worker wants to change the status of a job,
    it calls validate_transition(current, new) to ensure it is valid.
"""

from jobs.domain.exceptions import InvalidJobTransition
from jobs.domain.transitions import ALLOWED_TRANSITIONS


def validate_transition(current_status, new_status):
    """
    Validates whether moving from `current_status` to `new_status` is allowed.

    Args:
        current_status (str): The current JobStatus of the job.
        new_status (str): The desired target JobStatus.

    Raises:
        InvalidJobTransition: If the transition is not in the allowed mapping.
    """
    allowed = ALLOWED_TRANSITIONS.get(current_status, set())

    if new_status not in allowed:
        raise InvalidJobTransition(
            f"Invalid transition: {current_status} -> {new_status}"
        )