from kodo.error import KodoError


class Interrupt(KodoError):
    pass


class LaunchFlow(Interrupt):
    def __init__(self, **inputs):
        self.inputs = inputs
        super().__init__()
