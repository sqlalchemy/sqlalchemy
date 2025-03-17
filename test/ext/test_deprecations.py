from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import testing
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.ext.horizontal_shard import ShardedSession
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import mock
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


class HorizontalShardTest(fixtures.TestBase):
    def test_query_chooser(self):
        m1 = mock.Mock()

        with testing.expect_deprecated(
            "The ``query_chooser`` parameter is deprecated; please use",
        ):
            s = ShardedSession(
                shard_chooser=m1.shard_chooser,
                identity_chooser=m1.identity_chooser,
                query_chooser=m1.query_chooser,
            )

        m2 = mock.Mock()
        s.execute_chooser(m2)

        eq_(m1.mock_calls, [mock.call.query_chooser(m2.statement)])

    def test_id_chooser(self, decl_base):
        class A(decl_base):
            __tablename__ = "a"
            id = Column(Integer, primary_key=True)

        m1 = mock.Mock()

        with testing.expect_deprecated(
            "The ``id_chooser`` parameter is deprecated; please use"
        ):
            s = ShardedSession(
                shard_chooser=m1.shard_chooser,
                id_chooser=m1.id_chooser,
                execute_chooser=m1.execute_chooser,
            )

        m2 = mock.Mock()
        s.identity_chooser(
            A.__mapper__,
            m2.primary_key,
            lazy_loaded_from=m2.lazy_loaded_from,
            execution_options=m2.execution_options,
            bind_arguments=m2.bind_arguments,
        )

        eq_(m1.mock_calls, [mock.call.id_chooser(mock.ANY, m2.primary_key)])
