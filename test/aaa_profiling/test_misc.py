from sqlalchemy import Enum
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import profiling
from sqlalchemy.util import classproperty


class EnumTest(fixtures.TestBase):
    __requires__ = ("cpython", "python_profiling_backend")

    def setup(self):
        class SomeEnum(object):
            # Implements PEP 435 in the minimal fashion needed by SQLAlchemy

            _members = {}

            @classproperty
            def __members__(cls):
                """simulate a very expensive ``__members__`` getter"""
                for i in range(10):
                    x = {}
                    x.update({k: v for k, v in cls._members.items()}.copy())
                return x.copy()

            def __init__(self, name, value):
                self.name = name
                self.value = value
                self._members[name] = self
                setattr(self.__class__, name, self)

        for i in range(400):
            SomeEnum("some%d" % i, i)

        self.SomeEnum = SomeEnum

    @profiling.function_call_count()
    def test_create_enum_from_pep_435_w_expensive_members(self):
        Enum(self.SomeEnum)
