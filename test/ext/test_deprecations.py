from sqlalchemy import testing
from sqlalchemy.orm import mapper
from . import test_mutable
from .test_mutable import Foo


class MutableIncludeNonPrimaryTest(test_mutable.MutableWithScalarJSONTest):
    @classmethod
    def setup_mappers(cls):
        foo = cls.tables.foo

        mapper(Foo, foo)
        with testing.expect_deprecated(
            "The mapper.non_primary parameter is deprecated"
        ):
            mapper(
                Foo, foo, non_primary=True, properties={"foo_bar": foo.c.data}
            )


class MutableAssocIncludeNonPrimaryTest(
    test_mutable.MutableAssociationScalarPickleTest
):
    @classmethod
    def setup_mappers(cls):
        foo = cls.tables.foo

        mapper(Foo, foo)
        with testing.expect_deprecated(
            "The mapper.non_primary parameter is deprecated"
        ):
            mapper(
                Foo, foo, non_primary=True, properties={"foo_bar": foo.c.data}
            )
