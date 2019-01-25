from sqlalchemy import testing
from sqlalchemy.orm import mapper
from .test_mutable import Foo
from .test_mutable import (
    MutableAssociationScalarPickleTest as _MutableAssociationScalarPickleTest,
)
from .test_mutable import (
    MutableWithScalarJSONTest as _MutableWithScalarJSONTest,
)


class MutableIncludeNonPrimaryTest(_MutableWithScalarJSONTest):
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


class MutableAssocIncludeNonPrimaryTest(_MutableAssociationScalarPickleTest):
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
