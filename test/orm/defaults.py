import testenv; testenv.configure_for_tests()

from testlib import sa, testing
from testlib.sa import Table, Column, Integer, String, ForeignKey
from testlib.sa.orm import mapper, relation, create_session
from orm import _base
from testlib.testing import eq_


class TriggerDefaultsTest(_base.MappedTest):
    __requires__ = ('row_triggers',)

    def define_tables(self, metadata):
        dt = Table('dt', metadata,
                   Column('id', Integer, primary_key=True),
                   Column('col1', String(20)),
                   Column('col2', String(20),
                          server_default=sa.schema.FetchedValue()),
                   Column('col3', String(20),
                          sa.schema.FetchedValue(for_update=True)),
                   Column('col4', String(20),
                          sa.schema.FetchedValue(),
                          sa.schema.FetchedValue(for_update=True)))
        for ins in (
            sa.DDL("CREATE TRIGGER dt_ins AFTER INSERT ON dt "
                   "FOR EACH ROW BEGIN "
                   "UPDATE dt SET col2='ins', col4='ins' "
                   "WHERE dt.id = NEW.id; END",
                   on='sqlite'),
            ):
            if testing.against(ins.on):
                break
        else:
            ins = sa.DDL("CREATE TRIGGER dt_ins BEFORE INSERT ON dt "
                         "FOR EACH ROW BEGIN "
                         "SET NEW.col2='ins'; SET NEW.col4='ins'; END")
        ins.execute_at('after-create', dt)
        sa.DDL("DROP TRIGGER dt_ins").execute_at('before-drop', dt)


        for up in (
            sa.DDL("CREATE TRIGGER dt_up AFTER UPDATE ON dt "
                   "FOR EACH ROW BEGIN "
                   "UPDATE dt SET col3='up', col4='up' "
                   "WHERE dt.id = OLD.id; END",
                   on='sqlite'),
            ):
            if testing.against(up.on):
                break
        else:
            up = sa.DDL("CREATE TRIGGER dt_up BEFORE UPDATE ON dt "
                        "FOR EACH ROW BEGIN "
                        "SET NEW.col3='up'; SET NEW.col4='up'; END")
        up.execute_at('after-create', dt)
        sa.DDL("DROP TRIGGER dt_up").execute_at('before-drop', dt)


    def setup_classes(self):
        class Default(_base.BasicEntity):
            pass

    @testing.resolve_artifact_names
    def setup_mappers(self):
        mapper(Default, dt)

    @testing.resolve_artifact_names
    def test_insert(self):

        d1 = Default(id=1)

        eq_(d1.col1, None)
        eq_(d1.col2, None)
        eq_(d1.col3, None)
        eq_(d1.col4, None)

        session = create_session()
        session.add(d1)
        session.flush()

        eq_(d1.col1, None)
        eq_(d1.col2, 'ins')
        eq_(d1.col3, None)
        # don't care which trigger fired
        assert d1.col4 in ('ins', 'up')

    @testing.resolve_artifact_names
    def test_update(self):
        d1 = Default(id=1)

        session = create_session()
        session.add(d1)
        session.flush()
        d1.col1 = 'set'
        session.flush()

        eq_(d1.col1, 'set')
        eq_(d1.col2, 'ins')
        eq_(d1.col3, 'up')
        eq_(d1.col4, 'up')

class ExcludedDefaultsTest(_base.MappedTest):
    def define_tables(self, metadata):
        dt = Table('dt', metadata,
                   Column('id', Integer, primary_key=True),
                   Column('col1', String(20), default="hello"),
        )
        
    @testing.resolve_artifact_names
    def test_exclude(self):
        class Foo(_base.ComparableEntity):
            pass
        mapper(Foo, dt, exclude_properties=('col1',))
    
        f1 = Foo()
        sess = create_session()
        sess.add(f1)
        sess.flush()
        eq_(dt.select().execute().fetchall(), [(1, "hello")])
    
if __name__ == "__main__":
    testenv.main()
