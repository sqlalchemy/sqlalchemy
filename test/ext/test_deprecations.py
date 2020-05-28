from sqlalchemy import testing
from sqlalchemy.ext.horizontal_shard import ShardedSession
from sqlalchemy.orm import mapper
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import mock
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


class HorizontalShardTest(fixtures.TestBase):
    def test_query_chooser(self):
        m1 = mock.Mock()

        with testing.expect_deprecated(
            "The ``query_choser`` parameter is deprecated; please use"
        ):
            s = ShardedSession(
                shard_chooser=m1.shard_chooser,
                id_chooser=m1.id_chooser,
                query_chooser=m1.query_chooser,
            )

        m2 = mock.Mock()
        s.execute_chooser(m2)

        eq_(m1.mock_calls, [mock.call.query_chooser(m2.statement)])
