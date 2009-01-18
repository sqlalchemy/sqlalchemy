import testenv; testenv.configure_for_tests()
from testlib.sa import MetaData, Table, Column, Integer
from testlib.sa.orm import mapper, create_session
from testlib import sa, testing
from orm import _base


class BindTest(_base.MappedTest):
    def define_tables(self, metadata):
        Table('test_table', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('data', Integer))

    def setup_classes(self):
        class Foo(_base.BasicEntity):
            pass

    @testing.resolve_artifact_names
    def setup_mappers(self):
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
        self.assertRaisesMessage(
            sa.exc.UnboundExecutionError,
            ('Could not locate a bind configured on Mapper|Foo|test_table '
             'or this Session'),
            sess.flush)


if __name__ == '__main__':
    testenv.main()
