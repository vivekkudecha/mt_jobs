"""
Base Job Handler Interface

Purpose:
    Abstract base class defining the standard execution interface for all job handlers.

Use Case:
    Any new custom job type must subclass `BaseJobHandler` and implement the `execute` method.
"""

from abc import ABC, abstractmethod


class BaseJobHandler(ABC):
    """
    Abstract interface for all job handlers.
    """

    @abstractmethod
    def execute(self, payload: dict, context) -> dict:
        """
        Executes the business logic for this job type.

        Args:
            payload (dict): The input payload JSON data from the Job instance.
            context (JobContext): Execution context providing access to job metadata and heartbeats.

        Returns:
            dict: The output result JSON to be stored in `job.result`.
        """
        raise NotImplementedError