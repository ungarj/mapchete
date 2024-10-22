from typing import List, Optional

from mapchete.protocols import ObserverProtocol


class Observers:
    observers: List[ObserverProtocol]

    def __init__(self, observers: Optional[List[ObserverProtocol]] = None):
        self.observers = observers or []

    def notify(self, *args, **kwargs) -> None:
        for observer in self.observers:
            observer.update(*args, **kwargs)
