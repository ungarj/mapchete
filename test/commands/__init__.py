from dataclasses import dataclass, field
from typing import List, Optional

from mapchete.protocols import ObserverProtocol
from mapchete.types import Progress


@dataclass
class TaskCounter(ObserverProtocol):
    tasks: int = field(default=0)
    messages: List = field(default_factory=list)

    def __init__(self):
        self.messages = []

    def update(
        self,
        *_,
        progress: Optional[Progress] = None,
        message: Optional[str] = None,
        **__,
    ):
        if progress:
            self.tasks = progress.current
        if message:
            self.messages.append(message)

    def text_in_messages(self, text: str) -> bool:
        for message in self.messages:
            if text in message:
                return True
        return False
