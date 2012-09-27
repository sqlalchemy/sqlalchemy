import sqlalchemy as sa
from sqlalchemy import Integer, String, ForeignKey, event
from sqlalchemy import testing
from sqlalchemy.testing.schema import Table, Column
from sqlalchemy.orm import mapper, relationship, create_session
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import eq_


class TriggerDefaultsTest(fixtures.MappedTest):
    __requires__ = ('row_triggers',)

    @classmethod
    def define_tables(cls, metadata):
        dt = Table('dt', metadata,
                   Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
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
            sa.DDL("CREATE TRIGGER dt_ins ON dt AFTER INSERT AS "
                   "UPDATE dt SET col2='ins', col4='ins' "
                   "WHERE dt.id IN (SELECT id FROM inserted);",
                   on='mssql'),
            sa.DDL("CREATE TRIGGER dt_ins BEFORE INSERT "
                     "ON dt "
                     "FOR EACH ROW "
                     "BEGIN "
                     ":NEW.col2 := 'ins'; :NEW.col4 := 'ins'; END;",
                     on='oracle'),
            sa.DDL("CREATE TRIGGER dt_ins BEFORE INSERT ON dt "
                         "FOR EACH ROW BEGIN "
                         "SET NEW.col2='ins'; SET NEW.col4='ins'; END",
                         on=lambda ddl, event, target, bind, **kw:
                                bind.engine.name not in ('oracle', 'mssql', 'sqlite')
                ),
            ):
            event.listen(dt, 'after_create', ins)

        event.listen(dt, 'before_drop', sa.DDL("DROP TRIGGER dt_ins"))

        for up in (
            sa.DDL("CREATE TRIGGER dt_up AFTER UPDATE ON dt "
                   "FOR EACH ROW BEGIN "
                   "UPDATE dt SET col3='up', col4='up' "
                   "WHERE dt.id = OLD.id; END",
                   on='sqlite'),
            sa.DDL("CREATE TRIGGER dt_up ON dt AFTER UPDATE AS "
                   "UPDATE dt SET col3='up', col4='up' "
                   "WHERE dt.id IN (SELECT id FROM deleted);",
                   on='mssql'),
            sa.DDL("CREATE TRIGGER dt_up BEFORE UPDATE ON dt "
                  "FOR EACH ROW BEGIN "
                  ":NEW.col3 := 'up'; :NEW.col4 := 'up'; END;",
                  on='oracle'),
            sa.DDL("CREATE TRIGGER dt_up BEFORE UPDATE ON dt "
                        "FOR EACH ROW BEGIN "
                        "SET NEW.col3='up'; SET NEW.col4='up'; END",
                        on=lambda ddl, event, target, bind, **kw:
                                bind.engine.name not in ('oracle', 'mssql', 'sqlite')
                    ),
            ):
            event.listen(dt, 'after_create', up)

        event.listen(dt, 'before_drop', sa.DDL("DROP TRIGGER dt_up"))


    @classmethod
    def setup_classes(cls):
        class Default(cls.Comparable):
            pass

    @classmethod
    def setup_mappers(cls):
        Default, dt = cls.classes.Default, cls.tables.dt

        mapper(Default, dt)

    def test_insert(self):
        Default = self.classes.Default


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

    def test_update(self):
        Default = self.classes.Default

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

class ExcludedDefaultsTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        dt = Table('dt', metadata,
                   Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
                   Column('col1', String(20), default="hello"),
        )

    def test_exclude(self):
        dt = self.tables.dt

        class Foo(fixtures.BasicEntity):
            pass
        mapper(Foo, dt, exclude_properties=('col1',))

        f1 = Foo()
        sess = create_session()
        sess.add(f1)
        sess.flush()
        eq_(dt.select().execute().fetchall(), [(1, "hello")])

