from test.lib.testing import assert_raises, assert_raises_message
from sqlalchemy import *
from sqlalchemy import exc, schema
from test.lib import *
from test.lib import config, engines
from sqlalchemy.engine import ddl
from test.lib.testing import eq_
from test.lib.assertsql import AllOf, RegexSQL, ExactSQL, CompiledSQL
from sqlalchemy.dialects.postgresql import base as postgresql

class ConstraintTest(fixtures.TestBase, AssertsExecutionResults, AssertsCompiledSQL):

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

        Column('x', Integer, f)
        assert_raises(exc.InvalidRequestError, Column, "y", Integer, f)

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
        assert_raises(exc.DBAPIError, foo.insert().execute, id=2,x=5,y=9)
        bar.insert().execute(id=1,x=10)
        assert_raises(exc.DBAPIError, bar.insert().execute, id=2,x=5)

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
        assert_raises(exc.DBAPIError, foo.insert().execute, id=3, value='value1')
        assert_raises(exc.DBAPIError, bar.insert().execute, id=3, value='a', value2='b')

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

        for max_ident, max_index in [(22, None), (256, 22)]:
            dialect.max_identifier_length = max_ident
            dialect.max_index_name_length = max_index

            for tname, cname, exp in [
                ('sometable', 'this_name_is_too_long', 'ix_sometable_t_09aa'),
                ('sometable', 'this_name_alsois_long', 'ix_sometable_t_3cf1'),
            ]:

                t1 = Table(tname, MetaData(), 
                            Column(cname, Integer, index=True),
                        )
                ix1 = list(t1.indexes)[0]

                self.assert_compile(
                    schema.CreateIndex(ix1),
                    "CREATE INDEX %s "
                    "ON %s (%s)" % (exp, tname, cname),
                    dialect=dialect
                )

        dialect.max_identifier_length = 22
        dialect.max_index_name_length = None

        t1 = Table('t', MetaData(), Column('c', Integer))
        assert_raises(
            exc.IdentifierError,
            schema.CreateIndex(Index(
                        "this_other_name_is_too_long_for_what_were_doing", 
                        t1.c.c)).compile,
            dialect=dialect
        )

    def test_index_declartion_inline(self):
        t1 = Table('t1', metadata, 
            Column('x', Integer),
            Column('y', Integer),
            Index('foo', 'x', 'y')
        )
        self.assert_compile(
            schema.CreateIndex(list(t1.indexes)[0]), 
            "CREATE INDEX foo ON t1 (x, y)"
        )

    def test_index_asserts_cols_standalone(self):
        t1 = Table('t1', metadata, 
            Column('x', Integer)
        )
        t2 = Table('t2', metadata,
            Column('y', Integer)
        )
        assert_raises_message(
            exc.ArgumentError,
            "Column 't2.y' is not part of table 't1'.",
            Index,
            "bar", t1.c.x, t2.c.y
        )

    def test_index_asserts_cols_inline(self):
        t1 = Table('t1', metadata, 
            Column('x', Integer)
        )
        assert_raises_message(
            exc.ArgumentError,
            "Index 'bar' is against table 't1', and "
            "cannot be associated with table 't2'.",
            Table, 't2', metadata,
                Column('y', Integer),
                Index('bar', t1.c.x)
        )

class ConstraintCompilationTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = 'default'

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

    def test_column_level_ck_name(self):
        t = Table('tbl', MetaData(),
            Column('a', Integer, CheckConstraint("a > 5", name="ck_a_greater_five"))
        )
        self.assert_compile(
            schema.CreateTable(t),
            "CREATE TABLE tbl (a INTEGER CONSTRAINT "
            "ck_a_greater_five CHECK (a > 5))"
        )
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

        self.assert_compile(
            schema.CreateTable(t),
            "CREATE TABLE tbl (a INTEGER, b INTEGER, "
            "FOREIGN KEY(b) REFERENCES tbl "
            "(a) DEFERRABLE INITIALLY DEFERRED)",
        )

    def test_deferrable_unique(self):
        factory = lambda **kw: UniqueConstraint('b', **kw)
        self._test_deferrable(factory)

    def test_deferrable_table_check(self):
        factory = lambda **kw: CheckConstraint('a < b', **kw)
        self._test_deferrable(factory)

    def test_multiple(self):
        m = MetaData()
        foo = Table("foo", m, 
            Column('id', Integer, primary_key=True),
            Column('bar', Integer, primary_key=True)
        )
        tb = Table("some_table", m,
        Column('id', Integer, primary_key=True),
        Column('foo_id', Integer, ForeignKey('foo.id')),
        Column('foo_bar', Integer, ForeignKey('foo.bar')),
        )
        self.assert_compile(
            schema.CreateTable(tb),
            "CREATE TABLE some_table ("
                "id INTEGER NOT NULL, "
                "foo_id INTEGER, "
                "foo_bar INTEGER, "
                "PRIMARY KEY (id), "
                "FOREIGN KEY(foo_id) REFERENCES foo (id), "
                "FOREIGN KEY(foo_bar) REFERENCES foo (bar))"
        )

    def test_deferrable_column_check(self):
        t = Table('tbl', MetaData(),
                  Column('a', Integer),
                  Column('b', Integer,
                         CheckConstraint('a < b',
                                         deferrable=True,
                                         initially='DEFERRED')))

        self.assert_compile(
            schema.CreateTable(t),
            "CREATE TABLE tbl (a INTEGER, b INTEGER CHECK (a < b) DEFERRABLE INITIALLY DEFERRED)"
        )

    def test_use_alter(self):
        m = MetaData()
        t = Table('t', m,
                  Column('a', Integer),
        )

        t2 = Table('t2', m,
                Column('a', Integer, ForeignKey('t.a', use_alter=True, name='fk_ta')),
                Column('b', Integer, ForeignKey('t.a', name='fk_tb')), # to ensure create ordering ...
        )

        e = engines.mock_engine(dialect_name='postgresql')
        m.create_all(e)
        m.drop_all(e)

        e.assert_sql([
            'CREATE TABLE t (a INTEGER)', 
            'CREATE TABLE t2 (a INTEGER, b INTEGER, CONSTRAINT fk_tb FOREIGN KEY(b) REFERENCES t (a))', 
            'ALTER TABLE t2 ADD CONSTRAINT fk_ta FOREIGN KEY(a) REFERENCES t (a)', 
            'ALTER TABLE t2 DROP CONSTRAINT fk_ta', 
            'DROP TABLE t2', 
            'DROP TABLE t'
        ])


    def test_add_drop_constraint(self):
        m = MetaData()

        t = Table('tbl', m,
                  Column('a', Integer),
                  Column('b', Integer)
        )

        t2 = Table('t2', m,
                Column('a', Integer),
                Column('b', Integer)
        )

        constraint = CheckConstraint('a < b',name="my_test_constraint",
                                        deferrable=True,initially='DEFERRED', table=t)


        # before we create an AddConstraint,
        # the CONSTRAINT comes out inline
        self.assert_compile(
            schema.CreateTable(t),
            "CREATE TABLE tbl ("
            "a INTEGER, "
            "b INTEGER, "
            "CONSTRAINT my_test_constraint CHECK (a < b) DEFERRABLE INITIALLY DEFERRED"
            ")"
        )

        self.assert_compile(
            schema.AddConstraint(constraint),
            "ALTER TABLE tbl ADD CONSTRAINT my_test_constraint "
                    "CHECK (a < b) DEFERRABLE INITIALLY DEFERRED"
        )

        # once we make an AddConstraint,
        # inline compilation of the CONSTRAINT
        # is disabled
        self.assert_compile(
            schema.CreateTable(t),
            "CREATE TABLE tbl ("
            "a INTEGER, "
            "b INTEGER"
            ")"
        )

        self.assert_compile(
            schema.DropConstraint(constraint),
            "ALTER TABLE tbl DROP CONSTRAINT my_test_constraint"
        )

        self.assert_compile(
            schema.DropConstraint(constraint, cascade=True),
            "ALTER TABLE tbl DROP CONSTRAINT my_test_constraint CASCADE"
        )

        constraint = ForeignKeyConstraint(["b"], ["t2.a"])
        t.append_constraint(constraint)
        self.assert_compile(
            schema.AddConstraint(constraint),
            "ALTER TABLE tbl ADD FOREIGN KEY(b) REFERENCES t2 (a)"
        )

        constraint = ForeignKeyConstraint([t.c.a], [t2.c.b])
        t.append_constraint(constraint)
        self.assert_compile(
            schema.AddConstraint(constraint),
            "ALTER TABLE tbl ADD FOREIGN KEY(a) REFERENCES t2 (b)"
        )

        constraint = UniqueConstraint("a", "b", name="uq_cst")
        t2.append_constraint(constraint)
        self.assert_compile(
            schema.AddConstraint(constraint),
            "ALTER TABLE t2 ADD CONSTRAINT uq_cst UNIQUE (a, b)"
        )

        constraint = UniqueConstraint(t2.c.a, t2.c.b, name="uq_cs2")
        self.assert_compile(
            schema.AddConstraint(constraint),
            "ALTER TABLE t2 ADD CONSTRAINT uq_cs2 UNIQUE (a, b)"
        )

        assert t.c.a.primary_key is False
        constraint = PrimaryKeyConstraint(t.c.a)
        assert t.c.a.primary_key is True
        self.assert_compile(
            schema.AddConstraint(constraint),
            "ALTER TABLE tbl ADD PRIMARY KEY (a)"
        )


