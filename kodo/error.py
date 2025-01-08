class KodoError(Exception):
    pass


class DuplicateFlowError(KodoError):
    pass


class SetupError(KodoError):
    pass
