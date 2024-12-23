from litestar import Controller


class BaseController(Controller):

    @property
    def qualname(self):
        return ":".join([
            self.__class__.__module__, self.__class__.__name__])


