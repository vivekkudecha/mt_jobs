from jobs.handlers import context
from abc import ABC, abstractmethod


class BaseJobHandler(ABC):
    @abstractmethod
    def execute(self, payload: context):
        raise NotImplementedError