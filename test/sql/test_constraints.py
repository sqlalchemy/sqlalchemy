from sqlalchemy.test.testing import eq_, assert_raises, assert_raises_message
from sqlalchemy import *
from sqlalchemy import exc
from sqlalchemy.test import *
from sqlalchemy.test import config, engines

class ConstraintTest(TestBase, AssertsExecutionResults):

    def setup(self):
        global metadata
        metadata = MetaData(testing.db)

    def teardown(self):
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
        
        assert_raises(exc.InvalidRequestError, Table, "a", metadata,
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
        try:
            foo.insert().execute(id=2,x=5,y=9)
            assert False
        except exc.SQLError:
            assert True

        bar.insert().execute(id=1,x=10)
        try:
            bar.insert().execute(id=2,x=5)
            assert False
        except exc.SQLError:
            assert True

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
        try:
            foo.insert().execute(id=3, value='value1')
            assert False
        except exc.SQLError:
            assert True
        try:
            bar.insert().execute(id=3, value='a', value2='b')
            assert False
        except exc.SQLError:
            assert True

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

        index_names = [ ix.name for ix in events.indexes ]
        assert 'ix_events_name' in index_names
        assert 'ix_events_location' in index_names
        assert 'sport_announcer' in index_names
        assert 'idx_winners' in index_names
        assert len(index_names) == 4

        capt = []
        connection = testing.db.connect()
        # TODO: hacky, put a real connection proxy in
        ex = connection._Connection__execute_context
        def proxy(context):
            capt.append(context.statement)
            capt.append(repr(context.parameters))
            ex(context)
        connection._Connection__execute_context = proxy
        schemagen = testing.db.dialect.schemagenerator(testing.db.dialect, connection)
        schemagen.traverse(events)

        assert capt[0].strip().startswith('CREATE TABLE events')

        s = set([capt[x].strip() for x in [2,4,6,8]])

        assert s == set([
            'CREATE UNIQUE INDEX ix_events_name ON events (name)',
            'CREATE INDEX ix_events_location ON events (location)',
            'CREATE UNIQUE INDEX sport_announcer ON events (sport, announcer)',
            'CREATE INDEX idx_winners ON events (winner)'
            ])

        # verify that the table is functional
        events.insert().execute(id=1, name='hockey finals', location='rink',
                                sport='hockey', announcer='some canadian',
                                winner='sweden')
        ss = events.select().execute().fetchall()

    def test_too_long_idx_name(self):
        dialect = testing.db.dialect.__class__()
        dialect.max_identifier_length = 20

        schemagen = dialect.schemagenerator(dialect, None)
        schemagen.execute = lambda : None

        t1 = Table("sometable", MetaData(), Column("foo", Integer))
        schemagen.visit_index(Index("this_name_is_too_long_for_what_were_doing", t1.c.foo))
        eq_(schemagen.buffer.getvalue(), "CREATE INDEX this_name_is_t_1 ON sometable (foo)")
        schemagen.buffer.truncate(0)
        schemagen.visit_index(Index("this_other_name_is_too_long_for_what_were_doing", t1.c.foo))
        eq_(schemagen.buffer.getvalue(), "CREATE INDEX this_other_nam_2 ON sometable (foo)")

        schemadrop = dialect.schemadropper(dialect, None)
        schemadrop.execute = lambda: None
        assert_raises(exc.IdentifierError, schemadrop.visit_index, Index("this_name_is_too_long_for_what_were_doing", t1.c.foo))

    
class ConstraintCompilationTest(TestBase, AssertsExecutionResults):
    class accum(object):
        def __init__(self):
            self.statements = []
        def __call__(self, sql, *a, **kw):
            self.statements.append(sql)
        def __contains__(self, substring):
            for s in self.statements:
                if substring in s:
                    return True
            return False
        def __str__(self):
            return '\n'.join([repr(x) for x in self.statements])
        def clear(self):
            del self.statements[:]

    def setup(self):
        self.sql = self.accum()
        opts = config.db_opts.copy()
        opts['strategy'] = 'mock'
        opts['executor'] = self.sql
        self.engine = engines.testing_engine(options=opts)


    def _test_deferrable(self, constraint_factory):
        meta = MetaData(self.engine)
        t = Table('tbl', meta,
                  Column('a', Integer),
                  Column('b', Integer),
                  constraint_factory(deferrable=True))
        t.create()
        assert 'DEFERRABLE' in self.sql, self.sql
        assert 'NOT DEFERRABLE' not in self.sql, self.sql
        self.sql.clear()
        meta.clear()

        t = Table('tbl', meta,
                  Column('a', Integer),
                  Column('b', Integer),
                  constraint_factory(deferrable=False))
        t.create()
        assert 'NOT DEFERRABLE' in self.sql
        self.sql.clear()
        meta.clear()

        t = Table('tbl', meta,
                  Column('a', Integer),
                  Column('b', Integer),
                  constraint_factory(deferrable=True, initially='IMMEDIATE'))
        t.create()
        assert 'NOT DEFERRABLE' not in self.sql
        assert 'INITIALLY IMMEDIATE' in self.sql
        self.sql.clear()
        meta.clear()

        t = Table('tbl', meta,
                  Column('a', Integer),
                  Column('b', Integer),
                  constraint_factory(deferrable=True, initially='DEFERRED'))
        t.create()

        assert 'NOT DEFERRABLE' not in self.sql
        assert 'INITIALLY DEFERRED' in self.sql, self.sql

    def test_deferrable_pk(self):
        factory = lambda **kw: PrimaryKeyConstraint('a', **kw)
        self._test_deferrable(factory)

    def test_deferrable_table_fk(self):
        factory = lambda **kw: ForeignKeyConstraint(['b'], ['tbl.a'], **kw)
        self._test_deferrable(factory)

    def test_deferrable_column_fk(self):
        meta = MetaData(self.engine)
        t = Table('tbl', meta,
                  Column('a', Integer),
                  Column('b', Integer,
                         ForeignKey('tbl.a', deferrable=True,
                                    initially='DEFERRED')))
        t.create()
        assert 'DEFERRABLE' in self.sql, self.sql
        assert 'INITIALLY DEFERRED' in self.sql, self.sql

    def test_deferrable_unique(self):
        factory = lambda **kw: UniqueConstraint('b', **kw)
        self._test_deferrable(factory)

    def test_deferrable_table_check(self):
        factory = lambda **kw: CheckConstraint('a < b', **kw)
        self._test_deferrable(factory)

    def test_deferrable_column_check(self):
        meta = MetaData(self.engine)
        t = Table('tbl', meta,
                  Column('a', Integer),
                  Column('b', Integer,
                         CheckConstraint('a < b',
                                         deferrable=True,
                                         initially='DEFERRED')))
        t.create()
        assert 'DEFERRABLE' in self.sql, self.sql
        assert 'INITIALLY DEFERRED' in self.sql, self.sql


