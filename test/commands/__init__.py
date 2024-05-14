from mapchete.protocols import ObserverProtocol


class TaskCounter(ObserverProtocol):
    tasks = 0

    def update(self, *args, progress=None, **kwargs):
        if progress:
            self.tasks = progress.current
