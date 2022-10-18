# https://en.wikipedia.org/wiki/Mixin
# "This pattern is an example of enforcing the dependency inversion principle."
import logging
from typing import NamedTuple

logging.getLogger("Point2D").setLevel(logging.ERROR)
logging.getLogger("Point3D").setLevel(logging.WARNING)


class LoggerMixin:
    # It's a bit odd to call this a mixin, since mixins don't typically have an __init__
    def __init__(self, *args, **kwargs):
        print(f"Assigning a logger with name {self.__class__.__name__}")
        self.logger = logging.getLogger(self.__class__.__name__)


# Unfortunately, you can't write this, since "Multiple inheritance with NamedTuple is not supported."
# class Point2D(NamedTuple, LoggerMixin):
#    x: float
#    y: float
# So you must resort to ordinary classes, and all their boilerplate.


class Point2D(LoggerMixin):
    def __init__(self, x: float, y: float):
        super().__init__()
        self.x = x
        self.y = y

        # TypeError: object.__new__() takes exactly one argument(the type to instantiate)


class Point3D(LoggerMixin):
    def __init__(self, x: float, y: float, z: float):
        super().__init__()
        self.x = x
        self.y = y
        self.z = z


xy = Point2D(0, 4)
xyz = Point3D(1, 2, 3)

xy.logger.warning(
    "from xy"
)  # This won't come through, since the loglevel of this instance's logger is error.
xyz.logger.warning("from xyz")


# Note: you shouldn't assign a logger as an instance variable to a class. That's because these loggers are the same! So a class attribute would actually make some sense here. But even then, loggers are singletons,

abc = Point3D(0, 0, 0)
print(abc.logger is xyz.logger)
