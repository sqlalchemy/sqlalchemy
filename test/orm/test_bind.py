from sqlalchemy.test.testing import assert_raises, assert_raises_message
from sqlalchemy import MetaData, Integer
from sqlalchemy.test.schema import Table
from sqlalchemy.test.schema import Column
from sqlalchemy.orm import mapper, create_session
import sqlalchemy as sa
from sqlalchemy.test import testing
from test.orm import _base


class BindTest(_base.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table('test_table', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('data', Integer))

    @classmethod
    def setup_classes(cls):
        class Foo(_base.BasicEntity):
            pass

    @classmethod
    @testing.resolve_artifact_names
    def setup_mappers(cls):
        meta = MetaData()
        test_table.tometadata(meta)

        assert meta.tables['test_table'].bind is None
        mapper(Foo, meta.tables['test_table'])

    @testing.resolve_artifact_names
    def test_session_bind(self):
        engine = self.metadata.bind

        for bind in (engine, engine.connect()):
            try:
                sess = create_session(bind=bind)
                assert sess.bind is bind
                f = Foo()
                sess.add(f)
                sess.flush()
                assert sess.query(Foo).get(f.id) is f
            finally:
                if hasattr(bind, 'close'):
                    bind.close()

    @testing.resolve_artifact_names
    def test_session_unbound(self):
        sess = create_session()
        sess.add(Foo())
        assert_raises_message(
            sa.exc.UnboundExecutionError,
            ('Could not locate a bind configured on Mapper|Foo|test_table '
             'or this Session'),
            sess.flush)


