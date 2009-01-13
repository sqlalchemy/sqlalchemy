import testenv; testenv.configure_for_tests()
from sqlalchemy import *
from sqlalchemy import exc, schema
from testlib import *
from testlib import config, engines
from sqlalchemy.engine import ddl
from testlib.testing import eq_
from testlib.assertsql import AllOf, RegexSQL, ExactSQL, CompiledSQL

class ConstraintTest(TestBase, AssertsExecutionResults, AssertsCompiledSQL):

    def setUp(self):
        global metadata
        metadata = MetaData(testing.db)

    def tearDown(self):
        metadata.drop_all()

    def test_constraint(self):
        employees = Table('employees', metadata,
            Column('id', Integer),
            Column('soc', String(40)),
            Column('name', String(30)),
            PrimaryKeyConstraint('id', 'soc')
            )
        elements = Table('elements', metadata,
            Column('id', Integer),
            Column('stuff', String(30)),
            Column('emp_id', Integer),
            Column('emp_soc', String(40)),
            PrimaryKeyConstraint('id', name='elements_primkey'),
            ForeignKeyConstraint(['emp_id', 'emp_soc'], ['employees.id', 'employees.soc'])
            )
        metadata.create_all()

    def test_double_fk_usage_raises(self):
        f = ForeignKey('b.id')
        
        self.assertRaises(exc.InvalidRequestError, Table, "a", metadata,
            Column('x', Integer, f),
            Column('y', Integer, f)
        )
        
    def test_circular_constraint(self):
        a = Table("a", metadata,
            Column('id', Integer, primary_key=True),
            Column('bid', Integer),
            ForeignKeyConstraint(["bid"], ["b.id"], name="afk")
            )
        b = Table("b", metadata,
            Column('id', Integer, primary_key=True),
            Column("aid", Integer),
            ForeignKeyConstraint(["aid"], ["a.id"], use_alter=True, name="bfk")
            )
        metadata.create_all()

    def test_circular_constraint_2(self):
        a = Table("a", metadata,
            Column('id', Integer, primary_key=True),
            Column('bid', Integer, ForeignKey("b.id")),
            )
        b = Table("b", metadata,
            Column('id', Integer, primary_key=True),
            Column("aid", Integer, ForeignKey("a.id", use_alter=True, name="bfk")),
            )
        metadata.create_all()

    @testing.fails_on('mysql', 'FIXME: unknown')
    def test_check_constraint(self):
        foo = Table('foo', metadata,
            Column('id', Integer, primary_key=True),
            Column('x', Integer),
            Column('y', Integer),
            CheckConstraint('x>y'))
        bar = Table('bar', metadata,
            Column('id', Integer, primary_key=True),
            Column('x', Integer, CheckConstraint('x>7')),
            Column('z', Integer)
            )

        metadata.create_all()
        foo.insert().execute(id=1,x=9,y=5)
        self.assertRaises(exc.SQLError, foo.insert().execute, id=2,x=5,y=9)
        bar.insert().execute(id=1,x=10)
        self.assertRaises(exc.SQLError, bar.insert().execute, id=2,x=5)

    def test_unique_constraint(self):
        foo = Table('foo', metadata,
            Column('id', Integer, primary_key=True),
            Column('value', String(30), unique=True))
        bar = Table('bar', metadata,
            Column('id', Integer, primary_key=True),
            Column('value', String(30)),
            Column('value2', String(30)),
            UniqueConstraint('value', 'value2', name='uix1')
            )
        metadata.create_all()
        foo.insert().execute(id=1, value='value1')
        foo.insert().execute(id=2, value='value2')
        bar.insert().execute(id=1, value='a', value2='a')
        bar.insert().execute(id=2, value='a', value2='b')
        self.assertRaises(exc.SQLError, foo.insert().execute, id=3, value='value1')
        self.assertRaises(exc.SQLError, bar.insert().execute, id=3, value='a', value2='b')

    def test_index_create(self):
        employees = Table('employees', metadata,
                          Column('id', Integer, primary_key=True),
                          Column('first_name', String(30)),
                          Column('last_name', String(30)),
                          Column('email_address', String(30)))
        employees.create()

        i = Index('employee_name_index',
                  employees.c.last_name, employees.c.first_name)
        i.create()
        assert i in employees.indexes

        i2 = Index('employee_email_index',
                   employees.c.email_address, unique=True)
        i2.create()
        assert i2 in employees.indexes

    def test_index_create_camelcase(self):
        """test that mixed-case index identifiers are legal"""
        employees = Table('companyEmployees', metadata,
                          Column('id', Integer, primary_key=True),
                          Column('firstName', String(30)),
                          Column('lastName', String(30)),
                          Column('emailAddress', String(30)))

        employees.create()

        i = Index('employeeNameIndex',
                  employees.c.lastName, employees.c.firstName)
        i.create()

        i = Index('employeeEmailIndex',
                  employees.c.emailAddress, unique=True)
        i.create()

        # Check that the table is useable. This is mostly for pg,
        # which can be somewhat sticky with mixed-case identifiers
        employees.insert().execute(firstName='Joe', lastName='Smith', id=0)
        ss = employees.select().execute().fetchall()
        assert ss[0].firstName == 'Joe'
        assert ss[0].lastName == 'Smith'

    def test_index_create_inline(self):
        """Test indexes defined with tables"""

        events = Table('events', metadata,
                       Column('id', Integer, primary_key=True),
                       Column('name', String(30), index=True, unique=True),
                       Column('location', String(30), index=True),
                       Column('sport', String(30)),
                       Column('announcer', String(30)),
                       Column('winner', String(30)))

        Index('sport_announcer', events.c.sport, events.c.announcer, unique=True)
        Index('idx_winners', events.c.winner)

        eq_(
            set([ ix.name for ix in events.indexes ]),
            set(['ix_events_name', 'ix_events_location', 'sport_announcer', 'idx_winners'])
        )

        self.assert_sql_execution(
            testing.db,
            lambda: events.create(testing.db),
            RegexSQL("^CREATE TABLE events"),
            AllOf(
                ExactSQL('CREATE UNIQUE INDEX ix_events_name ON events (name)'),
                ExactSQL('CREATE INDEX ix_events_location ON events (location)'),
                ExactSQL('CREATE UNIQUE INDEX sport_announcer ON events (sport, announcer)'),
                ExactSQL('CREATE INDEX idx_winners ON events (winner)')
            )
        )

        # verify that the table is functional
        events.insert().execute(id=1, name='hockey finals', location='rink',
                                sport='hockey', announcer='some canadian',
                                winner='sweden')
        ss = events.select().execute().fetchall()

    def test_too_long_idx_name(self):
        dialect = testing.db.dialect.__class__()
        dialect.max_identifier_length = 20

        t1 = Table("sometable", MetaData(), Column("foo", Integer))
        self.assert_compile(
            schema.CreateIndex(Index("this_name_is_too_long_for_what_were_doing", t1.c.foo)),
            "CREATE INDEX this_name_is_t_1 ON sometable (foo)",
            dialect=dialect
        )
        
        self.assert_compile(
            schema.CreateIndex(Index("this_other_name_is_too_long_for_what_were_doing", t1.c.foo)),
            "CREATE INDEX this_other_nam_1 ON sometable (foo)",
            dialect=dialect
        )

    
class ConstraintCompilationTest(TestBase):

    def _test_deferrable(self, constraint_factory):
        t = Table('tbl', MetaData(),
                  Column('a', Integer),
                  Column('b', Integer),
                  constraint_factory(deferrable=True))
                  
        sql = str(schema.CreateTable(t).compile(bind=testing.db))
        assert 'DEFERRABLE' in sql, sql
        assert 'NOT DEFERRABLE' not in sql, sql
        
        t = Table('tbl', MetaData(),
                  Column('a', Integer),
                  Column('b', Integer),
                  constraint_factory(deferrable=False))

        sql = str(schema.CreateTable(t).compile(bind=testing.db))
        assert 'NOT DEFERRABLE' in sql


        t = Table('tbl', MetaData(),
                  Column('a', Integer),
                  Column('b', Integer),
                  constraint_factory(deferrable=True, initially='IMMEDIATE'))
        sql = str(schema.CreateTable(t).compile(bind=testing.db))
        assert 'NOT DEFERRABLE' not in sql
        assert 'INITIALLY IMMEDIATE' in sql

        t = Table('tbl', MetaData(),
                  Column('a', Integer),
                  Column('b', Integer),
                  constraint_factory(deferrable=True, initially='DEFERRED'))
        sql = str(schema.CreateTable(t).compile(bind=testing.db))

        assert 'NOT DEFERRABLE' not in sql
        assert 'INITIALLY DEFERRED' in sql

    def test_deferrable_pk(self):
        factory = lambda **kw: PrimaryKeyConstraint('a', **kw)
        self._test_deferrable(factory)

    def test_deferrable_table_fk(self):
        factory = lambda **kw: ForeignKeyConstraint(['b'], ['tbl.a'], **kw)
        self._test_deferrable(factory)

    def test_deferrable_column_fk(self):
        t = Table('tbl', MetaData(),
                  Column('a', Integer),
                  Column('b', Integer,
                         ForeignKey('tbl.a', deferrable=True,
                                    initially='DEFERRED')))

        sql = str(schema.CreateTable(t).compile(bind=testing.db))
        assert 'DEFERRABLE' in sql
        assert 'INITIALLY DEFERRED' in sql

    def test_deferrable_unique(self):
        factory = lambda **kw: UniqueConstraint('b', **kw)
        self._test_deferrable(factory)

    def test_deferrable_table_check(self):
        factory = lambda **kw: CheckConstraint('a < b', **kw)
        self._test_deferrable(factory)

    def test_deferrable_column_check(self):
        t = Table('tbl', MetaData(),
                  Column('a', Integer),
                  Column('b', Integer,
                         CheckConstraint('a < b',
                                         deferrable=True,
                                         initially='DEFERRED')))
        sql = str(schema.CreateTable(t).compile(bind=testing.db))
        assert 'DEFERRABLE' in sql
        assert 'INITIALLY DEFERRED' in sql


if __name__ == "__main__":
    testenv.main()
