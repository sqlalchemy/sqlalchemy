from sqlalchemy.testing import assert_raises, assert_raises_message
from sqlalchemy import MetaData, Integer
from sqlalchemy.testing.schema import Table
from sqlalchemy.testing.schema import Column
from sqlalchemy.orm import mapper, create_session
import sqlalchemy as sa
from sqlalchemy import testing
from sqlalchemy.testing import fixtures


class BindTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table('test_table', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('data', Integer))

    @classmethod
    def setup_classes(cls):
        class Foo(cls.Basic):
            pass

    @classmethod
    def setup_mappers(cls):
        test_table, Foo = cls.tables.test_table, cls.classes.Foo

        meta = MetaData()
        test_table.tometadata(meta)

        assert meta.tables['test_table'].bind is None
        mapper(Foo, meta.tables['test_table'])

    def test_session_bind(self):
        Foo = self.classes.Foo

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

    def test_session_unbound(self):
        Foo = self.classes.Foo

        sess = create_session()
        sess.add(Foo())
        assert_raises_message(
            sa.exc.UnboundExecutionError,
            ('Could not locate a bind configured on Mapper|Foo|test_table '
             'or this Session'),
            sess.flush)


