"""
Job Execution Runtime Context

Purpose:
    Encapsulates runtime metadata about the current job and attempt, exposing helper methods like heartbeats.

Use Case:
    Passed as the `context` parameter to `handler.execute(payload, context)` so the handler can emit
    periodic heartbeats during long-running tasks.
"""

from jobs.services.heartbeat import heartbeat


class JobContext:
    """
    Runtime context provided to executing job handlers.
    """

    def __init__(self, job, attempt):
        self.job = job
        self.attempt = attempt

    def heartbeat(self):
        """
        Signals to the orchestrator that this attempt is still actively running,
        extending its execution lease.
        """
        return heartbeat(self.attempt.id)