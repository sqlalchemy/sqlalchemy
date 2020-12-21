from sqlalchemy import testing
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.ext.horizontal_shard import ShardedSession
from sqlalchemy.orm import mapper
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import mock
from . import test_mutable
from .test_mutable import Foo
from ..orm._fixtures import FixtureTest


class AutomapTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        FixtureTest.define_tables(metadata)

    def test_reflect_true(self):
        Base = automap_base(metadata=self.tables_test_metadata)
        engine_mock = mock.Mock()
        with mock.patch.object(Base.metadata, "reflect") as reflect_mock:
            with testing.expect_deprecated(
                "The AutomapBase.prepare.reflect parameter is deprecated",
                "The AutomapBase.prepare.engine parameter is deprecated",
            ):
                Base.prepare(
                    engine=engine_mock, reflect=True, schema="some_schema"
                )
            reflect_mock.assert_called_once_with(
                engine_mock,
                schema="some_schema",
                extend_existing=True,
                autoload_replace=False,
            )


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
