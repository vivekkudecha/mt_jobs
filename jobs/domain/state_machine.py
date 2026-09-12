from jobs.domain.exceptions import InvalidJobTransition
from jobs.domain.transitions import ALLOWED_TRANSITIONS


def validate_transition(current_status, new_status):
    allowed = ALLOWED_TRANSITIONS.get(current_status, set())

    if new_status not in allowed:
        raise InvalidJobTransition(
            f"Invalid transition: {current_status} -> {new_status}"
        )