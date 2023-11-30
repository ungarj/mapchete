from typing import Protocol


class ObserverProtocol(Protocol):
    """Protocol used for custom observer classes hooked up into commands."""

    def update(self, *args, **kwargs) -> None:  # pragma: no cover
        ...
