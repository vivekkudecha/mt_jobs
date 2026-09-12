from jobs.services.heartbeat import heartbeat


class JobContext:
    def __init__(self, job, attempt):
        self.job = job
        self.attempt = attempt

    def heartbeat(self):
        return heartbeat(self.attempt.id)