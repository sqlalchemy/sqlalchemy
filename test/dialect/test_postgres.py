from sqlalchemy.test.testing import eq_, assert_raises, assert_raises_message
import datetime
from sqlalchemy import *
from sqlalchemy.orm import *
from sqlalchemy import exc
from sqlalchemy.databases import postgres
from sqlalchemy.engine.strategies import MockEngineStrategy
from sqlalchemy.test import *
from sqlalchemy.sql import table, column


class SequenceTest(TestBase, AssertsCompiledSQL):
    def test_basic(self):
        seq = Sequence("my_seq_no_schema")
        dialect = postgres.PGDialect()
        assert dialect.identifier_preparer.format_sequence(seq) == "my_seq_no_schema"

        seq = Sequence("my_seq", schema="some_schema")
        assert dialect.identifier_preparer.format_sequence(seq) == "some_schema.my_seq"

        seq = Sequence("My_Seq", schema="Some_Schema")
        assert dialect.identifier_preparer.format_sequence(seq) == '"Some_Schema"."My_Seq"'

class CompileTest(TestBase, AssertsCompiledSQL):
    __dialect__ = postgres.dialect()

    def test_update_returning(self):
        dialect = postgres.dialect()
        table1 = table('mytable',
            column('myid', Integer),
            column('name', String(128)),
            column('description', String(128)),
        )

        u = update(table1, values=dict(name='foo'), postgres_returning=[table1.c.myid, table1.c.name])
        self.assert_compile(u, "UPDATE mytable SET name=%(name)s RETURNING mytable.myid, mytable.name", dialect=dialect)

        u = update(table1, values=dict(name='foo'), postgres_returning=[table1])
        self.assert_compile(u, "UPDATE mytable SET name=%(name)s "\
            "RETURNING mytable.myid, mytable.name, mytable.description", dialect=dialect)

        u = update(table1, values=dict(name='foo'), postgres_returning=[func.length(table1.c.name)])
        self.assert_compile(u, "UPDATE mytable SET name=%(name)s RETURNING length(mytable.name)", dialect=dialect)

    def test_insert_returning(self):
        dialect = postgres.dialect()
        table1 = table('mytable',
            column('myid', Integer),
            column('name', String(128)),
            column('description', String(128)),
        )

        i = insert(table1, values=dict(name='foo'), postgres_returning=[table1.c.myid, table1.c.name])
        self.assert_compile(i, "INSERT INTO mytable (name) VALUES (%(name)s) RETURNING mytable.myid, mytable.name", dialect=dialect)

        i = insert(table1, values=dict(name='foo'), postgres_returning=[table1])
        self.assert_compile(i, "INSERT INTO mytable (name) VALUES (%(name)s) "\
            "RETURNING mytable.myid, mytable.name, mytable.description", dialect=dialect)

        i = insert(table1, values=dict(name='foo'), postgres_returning=[func.length(table1.c.name)])
        self.assert_compile(i, "INSERT INTO mytable (name) VALUES (%(name)s) RETURNING length(mytable.name)", dialect=dialect)

    def test_extract(self):

        t = table('t', column('col1', DateTime), column('col2', Date), column('col3', Time),
                    column('col4', postgres.PGDateTime), column('col5', postgres.PGDate),
                    column('col6', postgres.PGTime), column('col7', postgres.PGInterval)
        )

        for field in 'year', 'month', 'day', 'epoch', 'hour':
            for expr, compiled_expr in [
                ( t.c.col1, "t.col1 :: timestamp" ),
                ( t.c.col2, "t.col2 :: date" ),
                ( t.c.col3, "t.col3 :: time" ),
                (func.current_timestamp() - datetime.timedelta(days=5),
                    "(CURRENT_TIMESTAMP - %(current_timestamp_1)s) :: timestamp"
                ),
                (func.current_timestamp() + func.current_timestamp(), 
                    "CURRENT_TIMESTAMP + CURRENT_TIMESTAMP" # invalid, no cast.
                ),
                (text("foo.date + foo.time"), 
                    "foo.date + foo.time" # plain text.  no cast.
                ),
                (func.current_timestamp() + datetime.timedelta(days=5), 
                    "(CURRENT_TIMESTAMP + %(current_timestamp_1)s) :: timestamp"
                ),
                (t.c.col2 + t.c.col3,
                    "(t.col2 + t.col3) :: timestamp"
                ),
                (t.c.col5 + t.c.col6,
                    "(t.col5 + t.col6) :: timestamp"
                ),
                # addition is commutative
                (t.c.col2 + datetime.timedelta(days=5),
                    "(t.col2 + %(col2_1)s) :: timestamp"
                ),
                (datetime.timedelta(days=5) + t.c.col2,
                    "(%(col2_1)s + t.col2) :: timestamp"
                ),
                (t.c.col4 + t.c.col7,
                    "(t.col4 + t.col7) :: timestamp"
                ),
                # subtraction is not
                (t.c.col1 - datetime.timedelta(seconds=30),
                    "(t.col1 - %(col1_1)s) :: timestamp"
                ),
                (datetime.timedelta(seconds=30) - t.c.col1,
                    "%(col1_1)s - t.col1" # invalid - no cast.
                ),
                (func.coalesce(t.c.col1, func.current_timestamp()),
                    "coalesce(t.col1, CURRENT_TIMESTAMP) :: timestamp"
                ),
                (t.c.col3 + datetime.timedelta(seconds=30),
                    "(t.col3 + %(col3_1)s) :: time"
                ),
                (t.c.col6 + datetime.timedelta(seconds=30),
                    "(t.col6 + %(col6_1)s) :: time"
                ),
                (func.current_timestamp() - func.coalesce(t.c.col1, func.current_timestamp()),
                    "(CURRENT_TIMESTAMP - coalesce(t.col1, CURRENT_TIMESTAMP)) :: interval",
                ),
                (3 * func.foobar(type_=Interval),
                    "(%(foobar_1)s * foobar()) :: interval"
                ),
                (literal(datetime.timedelta(seconds=10)) - literal(datetime.timedelta(seconds=10)),
                    "(%(param_1)s - %(param_2)s) :: interval"
                ),
                (t.c.col3 + "some string", # dont crack up on entirely unsupported types
                    "t.col3 + %(col3_1)s"
                )
            ]:
                self.assert_compile(
                    select([extract(field, expr)]).select_from(t),
                    "SELECT EXTRACT(%s FROM %s) AS anon_1 FROM t" % (
                        field, 
                        compiled_expr
                    )
                )

class ReturningTest(TestBase, AssertsExecutionResults):
    __only_on__ = 'postgres'

    @testing.exclude('postgres', '<', (8, 2), '8.3+ feature')
    def test_update_returning(self):
        meta = MetaData(testing.db)
        table = Table('tables', meta,
            Column('id', Integer, primary_key=True),
            Column('persons', Integer),
            Column('full', Boolean)
        )
        table.create()
        try:
            table.insert().execute([{'persons': 5, 'full': False}, {'persons': 3, 'full': False}])

            result = table.update(table.c.persons > 4, dict(full=True), postgres_returning=[table.c.id]).execute()
            eq_(result.fetchall(), [(1,)])

            result2 = select([table.c.id, table.c.full]).order_by(table.c.id).execute()
            eq_(result2.fetchall(), [(1,True),(2,False)])
        finally:
            table.drop()

    @testing.exclude('postgres', '<', (8, 2), '8.3+ feature')
    def test_insert_returning(self):
        meta = MetaData(testing.db)
        table = Table('tables', meta,
            Column('id', Integer, primary_key=True),
            Column('persons', Integer),
            Column('full', Boolean)
        )
        table.create()
        try:
            result = table.insert(postgres_returning=[table.c.id]).execute({'persons': 1, 'full': False})

            eq_(result.fetchall(), [(1,)])

            @testing.fails_on('postgres', 'Known limitation of psycopg2')
            def test_executemany():
                # return value is documented as failing with psycopg2/executemany
                result2 = table.insert(postgres_returning=[table]).execute(
                     [{'persons': 2, 'full': False}, {'persons': 3, 'full': True}])
                eq_(result2.fetchall(), [(2, 2, False), (3,3,True)])
            
            test_executemany()
            
            result3 = table.insert(postgres_returning=[(table.c.id*2).label('double_id')]).execute({'persons': 4, 'full': False})
            eq_([dict(row) for row in result3], [{'double_id':8}])

            result4 = testing.db.execute('insert into tables (id, persons, "full") values (5, 10, true) returning persons')
            eq_([dict(row) for row in result4], [{'persons': 10}])
        finally:
            table.drop()


class InsertTest(TestBase, AssertsExecutionResults):
    __only_on__ = 'postgres'

    @classmethod
    def setup_class(cls):
        global metadata
        metadata = MetaData(testing.db)

    def teardown(self):
        metadata.drop_all()
        metadata.tables.clear()

    def test_compiled_insert(self):
        table = Table('testtable', metadata,
            Column('id', Integer, primary_key=True),
            Column('data', String(30)))

        metadata.create_all()

        ins = table.insert(values={'data':bindparam('x')}).compile()
        ins.execute({'x':"five"}, {'x':"seven"})
        assert table.select().execute().fetchall() == [(1, 'five'), (2, 'seven')]

    def test_sequence_insert(self):
        table = Table('testtable', metadata,
            Column('id', Integer, Sequence('my_seq'), primary_key=True),
            Column('data', String(30)))
        metadata.create_all()
        self._assert_data_with_sequence(table, "my_seq")

    def test_opt_sequence_insert(self):
        table = Table('testtable', metadata,
            Column('id', Integer, Sequence('my_seq', optional=True), primary_key=True),
            Column('data', String(30)))
        metadata.create_all()
        self._assert_data_autoincrement(table)

    def test_autoincrement_insert(self):
        table = Table('testtable', metadata,
            Column('id', Integer, primary_key=True),
            Column('data', String(30)))
        metadata.create_all()
        self._assert_data_autoincrement(table)

    def test_noautoincrement_insert(self):
        table = Table('testtable', metadata,
            Column('id', Integer, primary_key=True, autoincrement=False),
            Column('data', String(30)))
        metadata.create_all()
        self._assert_data_noautoincrement(table)

    def _assert_data_autoincrement(self, table):
        def go():
            # execute with explicit id
            r = table.insert().execute({'id':30, 'data':'d1'})
            assert r.last_inserted_ids() == [30]

            # execute with prefetch id
            r = table.insert().execute({'data':'d2'})
            assert r.last_inserted_ids() == [1]

            # executemany with explicit ids
            table.insert().execute({'id':31, 'data':'d3'}, {'id':32, 'data':'d4'})

            # executemany, uses SERIAL
            table.insert().execute({'data':'d5'}, {'data':'d6'})

            # single execute, explicit id, inline
            table.insert(inline=True).execute({'id':33, 'data':'d7'})

            # single execute, inline, uses SERIAL
            table.insert(inline=True).execute({'data':'d8'})

        # note that the test framework doesnt capture the "preexecute" of a seqeuence
        # or default.  we just see it in the bind params.

        self.assert_sql(testing.db, go, [], with_sequences=[
            (
                "INSERT INTO testtable (id, data) VALUES (:id, :data)",
                {'id':30, 'data':'d1'}
            ),
            (
                "INSERT INTO testtable (id, data) VALUES (:id, :data)",
                {'id':1, 'data':'d2'}
            ),
            (
                "INSERT INTO testtable (id, data) VALUES (:id, :data)",
                [{'id':31, 'data':'d3'}, {'id':32, 'data':'d4'}]
            ),
            (
                "INSERT INTO testtable (data) VALUES (:data)",
                [{'data':'d5'}, {'data':'d6'}]
            ),
            (
                "INSERT INTO testtable (id, data) VALUES (:id, :data)",
                [{'id':33, 'data':'d7'}]
            ),
            (
                "INSERT INTO testtable (data) VALUES (:data)",
                [{'data':'d8'}]
            ),
        ])

        assert table.select().execute().fetchall() == [
            (30, 'd1'),
            (1, 'd2'),
            (31, 'd3'),
            (32, 'd4'),
            (2, 'd5'),
            (3, 'd6'),
            (33, 'd7'),
            (4, 'd8'),
        ]
        table.delete().execute()

        # test the same series of events using a reflected
        # version of the table
        m2 = MetaData(testing.db)
        table = Table(table.name, m2, autoload=True)

        def go():
            table.insert().execute({'id':30, 'data':'d1'})
            r = table.insert().execute({'data':'d2'})
            assert r.last_inserted_ids() == [5]
            table.insert().execute({'id':31, 'data':'d3'}, {'id':32, 'data':'d4'})
            table.insert().execute({'data':'d5'}, {'data':'d6'})
            table.insert(inline=True).execute({'id':33, 'data':'d7'})
            table.insert(inline=True).execute({'data':'d8'})

        self.assert_sql(testing.db, go, [], with_sequences=[
            (
                "INSERT INTO testtable (id, data) VALUES (:id, :data)",
                {'id':30, 'data':'d1'}
            ),
            (
                "INSERT INTO testtable (id, data) VALUES (:id, :data)",
                {'id':5, 'data':'d2'}
            ),
            (
                "INSERT INTO testtable (id, data) VALUES (:id, :data)",
                [{'id':31, 'data':'d3'}, {'id':32, 'data':'d4'}]
            ),
            (
                "INSERT INTO testtable (data) VALUES (:data)",
                [{'data':'d5'}, {'data':'d6'}]
            ),
            (
                "INSERT INTO testtable (id, data) VALUES (:id, :data)",
                [{'id':33, 'data':'d7'}]
            ),
            (
                "INSERT INTO testtable (data) VALUES (:data)",
                [{'data':'d8'}]
            ),
        ])

        assert table.select().execute().fetchall() == [
            (30, 'd1'),
            (5, 'd2'),
            (31, 'd3'),
            (32, 'd4'),
            (6, 'd5'),
            (7, 'd6'),
            (33, 'd7'),
            (8, 'd8'),
        ]
        table.delete().execute()

    def _assert_data_with_sequence(self, table, seqname):
        def go():
            table.insert().execute({'id':30, 'data':'d1'})
            table.insert().execute({'data':'d2'})
            table.insert().execute({'id':31, 'data':'d3'}, {'id':32, 'data':'d4'})
            table.insert().execute({'data':'d5'}, {'data':'d6'})
            table.insert(inline=True).execute({'id':33, 'data':'d7'})
            table.insert(inline=True).execute({'data':'d8'})

        self.assert_sql(testing.db, go, [], with_sequences=[
            (
                "INSERT INTO testtable (id, data) VALUES (:id, :data)",
                {'id':30, 'data':'d1'}
            ),
            (
                "INSERT INTO testtable (id, data) VALUES (:id, :data)",
                {'id':1, 'data':'d2'}
            ),
            (
                "INSERT INTO testtable (id, data) VALUES (:id, :data)",
                [{'id':31, 'data':'d3'}, {'id':32, 'data':'d4'}]
            ),
            (
                "INSERT INTO testtable (id, data) VALUES (nextval('%s'), :data)" % seqname,
                [{'data':'d5'}, {'data':'d6'}]
            ),
            (
                "INSERT INTO testtable (id, data) VALUES (:id, :data)",
                [{'id':33, 'data':'d7'}]
            ),
            (
                "INSERT INTO testtable (id, data) VALUES (nextval('%s'), :data)" % seqname,
                [{'data':'d8'}]
            ),
        ])

        assert table.select().execute().fetchall() == [
            (30, 'd1'),
            (1, 'd2'),
            (31, 'd3'),
            (32, 'd4'),
            (2, 'd5'),
            (3, 'd6'),
            (33, 'd7'),
            (4, 'd8'),
        ]

        # cant test reflection here since the Sequence must be
        # explicitly specified

    def _assert_data_noautoincrement(self, table):
        table.insert().execute({'id':30, 'data':'d1'})
        try:
            table.insert().execute({'data':'d2'})
            assert False
        except exc.IntegrityError, e:
            assert "violates not-null constraint" in str(e)
        try:
            table.insert().execute({'data':'d2'}, {'data':'d3'})
            assert False
        except exc.IntegrityError, e:
            assert "violates not-null constraint" in str(e)

        table.insert().execute({'id':31, 'data':'d2'}, {'id':32, 'data':'d3'})
        table.insert(inline=True).execute({'id':33, 'data':'d4'})

        assert table.select().execute().fetchall() == [
            (30, 'd1'),
            (31, 'd2'),
            (32, 'd3'),
            (33, 'd4'),
        ]
        table.delete().execute()

        # test the same series of events using a reflected
        # version of the table
        m2 = MetaData(testing.db)
        table = Table(table.name, m2, autoload=True)
        table.insert().execute({'id':30, 'data':'d1'})
        try:
            table.insert().execute({'data':'d2'})
            assert False
        except exc.IntegrityError, e:
            assert "violates not-null constraint" in str(e)
        try:
            table.insert().execute({'data':'d2'}, {'data':'d3'})
            assert False
        except exc.IntegrityError, e:
            assert "violates not-null constraint" in str(e)

        table.insert().execute({'id':31, 'data':'d2'}, {'id':32, 'data':'d3'})
        table.insert(inline=True).execute({'id':33, 'data':'d4'})

        assert table.select().execute().fetchall() == [
            (30, 'd1'),
            (31, 'd2'),
            (32, 'd3'),
            (33, 'd4'),
        ]

class DomainReflectionTest(TestBase, AssertsExecutionResults):
    "Test PostgreSQL domains"

    __only_on__ = 'postgres'

    @classmethod
    def setup_class(cls):
        con = testing.db.connect()
        for ddl in ('CREATE DOMAIN testdomain INTEGER NOT NULL DEFAULT 42',
                    'CREATE DOMAIN alt_schema.testdomain INTEGER DEFAULT 0'):
            try:
                con.execute(ddl)
            except exc.SQLError, e:
                if not "already exists" in str(e):
                    raise e
        con.execute('CREATE TABLE testtable (question integer, answer testdomain)')
        con.execute('CREATE TABLE alt_schema.testtable(question integer, answer alt_schema.testdomain, anything integer)')
        con.execute('CREATE TABLE crosschema (question integer, answer alt_schema.testdomain)')

    @classmethod
    def teardown_class(cls):
        con = testing.db.connect()
        con.execute('DROP TABLE testtable')
        con.execute('DROP TABLE alt_schema.testtable')
        con.execute('DROP TABLE crosschema')
        con.execute('DROP DOMAIN testdomain')
        con.execute('DROP DOMAIN alt_schema.testdomain')

    def test_table_is_reflected(self):
        metadata = MetaData(testing.db)
        table = Table('testtable', metadata, autoload=True)
        eq_(set(table.columns.keys()), set(['question', 'answer']), "Columns of reflected table didn't equal expected columns")
        eq_(table.c.answer.type.__class__, postgres.PGInteger)

    def test_domain_is_reflected(self):
        metadata = MetaData(testing.db)
        table = Table('testtable', metadata, autoload=True)
        eq_(str(table.columns.answer.server_default.arg), '42', "Reflected default value didn't equal expected value")
        assert not table.columns.answer.nullable, "Expected reflected column to not be nullable."

    def test_table_is_reflected_alt_schema(self):
        metadata = MetaData(testing.db)
        table = Table('testtable', metadata, autoload=True, schema='alt_schema')
        eq_(set(table.columns.keys()), set(['question', 'answer', 'anything']), "Columns of reflected table didn't equal expected columns")
        eq_(table.c.anything.type.__class__, postgres.PGInteger)

    def test_schema_domain_is_reflected(self):
        metadata = MetaData(testing.db)
        table = Table('testtable', metadata, autoload=True, schema='alt_schema')
        eq_(str(table.columns.answer.server_default.arg), '0', "Reflected default value didn't equal expected value")
        assert table.columns.answer.nullable, "Expected reflected column to be nullable."

    def test_crosschema_domain_is_reflected(self):
        metadata = MetaData(testing.db)
        table = Table('crosschema', metadata, autoload=True)
        eq_(str(table.columns.answer.server_default.arg), '0', "Reflected default value didn't equal expected value")
        assert table.columns.answer.nullable, "Expected reflected column to be nullable."

    def test_unknown_types(self):
        from sqlalchemy.databases import postgres

        ischema_names = postgres.ischema_names
        postgres.ischema_names = {}
        try:
            m2 = MetaData(testing.db)
            assert_raises(exc.SAWarning, Table, "testtable", m2, autoload=True)

            @testing.emits_warning('Did not recognize type')
            def warns():
                m3 = MetaData(testing.db)
                t3 = Table("testtable", m3, autoload=True)
                assert t3.c.answer.type.__class__ == sa.types.NullType

        finally:
            postgres.ischema_names = ischema_names


class MiscTest(TestBase, AssertsExecutionResults):
    __only_on__ = 'postgres'

    def test_date_reflection(self):
        m1 = MetaData(testing.db)
        t1 = Table('pgdate', m1,
            Column('date1', DateTime(timezone=True)),
            Column('date2', DateTime(timezone=False))
            )
        m1.create_all()
        try:
            m2 = MetaData(testing.db)
            t2 = Table('pgdate', m2, autoload=True)
            assert t2.c.date1.type.timezone is True
            assert t2.c.date2.type.timezone is False
        finally:
            m1.drop_all()

    def test_pg_weirdchar_reflection(self):
        meta1 = MetaData(testing.db)
        subject = Table("subject", meta1,
                        Column("id$", Integer, primary_key=True),
                        )

        referer = Table("referer", meta1,
                        Column("id", Integer, primary_key=True),
                        Column("ref", Integer, ForeignKey('subject.id$')),
                        )
        meta1.create_all()
        try:
            meta2 = MetaData(testing.db)
            subject = Table("subject", meta2, autoload=True)
            referer = Table("referer", meta2, autoload=True)
            print str(subject.join(referer).onclause)
            self.assert_((subject.c['id$']==referer.c.ref).compare(subject.join(referer).onclause))
        finally:
            meta1.drop_all()
    
    def test_extract(self):
        for field, exp in (
                    ('year', 2009),
                    ('month', 11),
                    ('day', 10),
            ):
            r = testing.db.execute(
                select([extract(field, datetime.datetime(2009, 11, 15, 12, 15, 35) - datetime.timedelta(days =5))])
            ).scalar()
            eq_(r, exp)
            
    def test_checksfor_sequence(self):
        meta1 = MetaData(testing.db)
        t = Table('mytable', meta1,
            Column('col1', Integer, Sequence('fooseq')))
        try:
            testing.db.execute("CREATE SEQUENCE fooseq")
            t.create(checkfirst=True)
        finally:
            t.drop(checkfirst=True)

    def test_distinct_on(self):
        t = Table('mytable', MetaData(testing.db),
                  Column('id', Integer, primary_key=True),
                  Column('a', String(8)))
        eq_(
            str(t.select(distinct=t.c.a)),
            'SELECT DISTINCT ON (mytable.a) mytable.id, mytable.a \n'
            'FROM mytable')
        eq_(
            str(t.select(distinct=['id','a'])),
            'SELECT DISTINCT ON (id, a) mytable.id, mytable.a \n'
            'FROM mytable')
        eq_(
            str(t.select(distinct=[t.c.id, t.c.a])),
            'SELECT DISTINCT ON (mytable.id, mytable.a) mytable.id, mytable.a \n'
            'FROM mytable')

    def test_schema_reflection(self):
        """note: this test requires that the 'alt_schema' schema be separate and accessible by the test user"""

        meta1 = MetaData(testing.db)
        users = Table('users', meta1,
            Column('user_id', Integer, primary_key = True),
            Column('user_name', String(30), nullable = False),
            schema="alt_schema"
            )

        addresses = Table('email_addresses', meta1,
            Column('address_id', Integer, primary_key = True),
            Column('remote_user_id', Integer, ForeignKey(users.c.user_id)),
            Column('email_address', String(20)),
            schema="alt_schema"
        )
        meta1.create_all()
        try:
            meta2 = MetaData(testing.db)
            addresses = Table('email_addresses', meta2, autoload=True, schema="alt_schema")
            users = Table('users', meta2, mustexist=True, schema="alt_schema")

            print users
            print addresses
            j = join(users, addresses)
            print str(j.onclause)
            self.assert_((users.c.user_id==addresses.c.remote_user_id).compare(j.onclause))
        finally:
            meta1.drop_all()

    def test_schema_reflection_2(self):
        meta1 = MetaData(testing.db)
        subject = Table("subject", meta1,
                        Column("id", Integer, primary_key=True),
                        )

        referer = Table("referer", meta1,
                        Column("id", Integer, primary_key=True),
                        Column("ref", Integer, ForeignKey('subject.id')),
                        schema="alt_schema")
        meta1.create_all()
        try:
            meta2 = MetaData(testing.db)
            subject = Table("subject", meta2, autoload=True)
            referer = Table("referer", meta2, schema="alt_schema", autoload=True)
            print str(subject.join(referer).onclause)
            self.assert_((subject.c.id==referer.c.ref).compare(subject.join(referer).onclause))
        finally:
            meta1.drop_all()

    def test_schema_reflection_3(self):
        meta1 = MetaData(testing.db)
        subject = Table("subject", meta1,
                        Column("id", Integer, primary_key=True),
                        schema='alt_schema_2'
                        )

        referer = Table("referer", meta1,
                        Column("id", Integer, primary_key=True),
                        Column("ref", Integer, ForeignKey('alt_schema_2.subject.id')),
                        schema="alt_schema")

        meta1.create_all()
        try:
            meta2 = MetaData(testing.db)
            subject = Table("subject", meta2, autoload=True, schema="alt_schema_2")
            referer = Table("referer", meta2, schema="alt_schema", autoload=True)
            print str(subject.join(referer).onclause)
            self.assert_((subject.c.id==referer.c.ref).compare(subject.join(referer).onclause))
        finally:
            meta1.drop_all()

    def test_schema_roundtrips(self):
        meta = MetaData(testing.db)
        users = Table('users', meta,
            Column('id', Integer, primary_key=True),
            Column('name', String(50)), schema='alt_schema')
        users.create()
        try:
            users.insert().execute(id=1, name='name1')
            users.insert().execute(id=2, name='name2')
            users.insert().execute(id=3, name='name3')
            users.insert().execute(id=4, name='name4')

            eq_(users.select().where(users.c.name=='name2').execute().fetchall(), [(2, 'name2')])
            eq_(users.select(use_labels=True).where(users.c.name=='name2').execute().fetchall(), [(2, 'name2')])

            users.delete().where(users.c.id==3).execute()
            eq_(users.select().where(users.c.name=='name3').execute().fetchall(), [])

            users.update().where(users.c.name=='name4').execute(name='newname')
            eq_(users.select(use_labels=True).where(users.c.id==4).execute().fetchall(), [(4, 'newname')])

        finally:
            users.drop()

    def test_preexecute_passivedefault(self):
        """test that when we get a primary key column back
        from reflecting a table which has a default value on it, we pre-execute
        that DefaultClause upon insert."""

        try:
            meta = MetaData(testing.db)
            testing.db.execute("""
             CREATE TABLE speedy_users
             (
                 speedy_user_id   SERIAL     PRIMARY KEY,

                 user_name        VARCHAR    NOT NULL,
                 user_password    VARCHAR    NOT NULL
             );
            """, None)

            t = Table("speedy_users", meta, autoload=True)
            r = t.insert().execute(user_name='user', user_password='lala')
            assert r.last_inserted_ids() == [1]
            l = t.select().execute().fetchall()
            assert l == [(1, 'user', 'lala')]
        finally:
            testing.db.execute("drop table speedy_users", None)

    @testing.emits_warning()
    def test_index_reflection(self):
        """ Reflecting partial & expression-based indexes should warn """
        import warnings
        def capture_warnings(*args, **kw):
            capture_warnings._orig_showwarning(*args, **kw)
            capture_warnings.warnings.append(args)
        capture_warnings._orig_showwarning = warnings.warn
        capture_warnings.warnings = []

        m1 = MetaData(testing.db)
        t1 = Table('party', m1,
            Column('id', String(10), nullable=False),
            Column('name', String(20), index=True), 
            Column('aname', String(20))
            )
        m1.create_all()
        
        testing.db.execute("""
          create index idx1 on party ((id || name))
        """, None) 
        testing.db.execute("""
          create unique index idx2 on party (id) where name = 'test'
        """, None)
        
        testing.db.execute("""
            create index idx3 on party using btree
                (lower(name::text), lower(aname::text))
        """)
        
        try:
            m2 = MetaData(testing.db)

            warnings.warn = capture_warnings
            t2 = Table('party', m2, autoload=True)
      
            wrn = capture_warnings.warnings
            assert str(wrn[0][0]) == (
              "Skipped unsupported reflection of expression-based index idx1")
            assert str(wrn[1][0]) == (
              "Predicate of partial index idx2 ignored during reflection")
            assert len(t2.indexes) == 2
            # Make sure indexes are in the order we expect them in
            tmp = [(idx.name, idx) for idx in t2.indexes]
            tmp.sort()
            
            r1, r2 = [idx[1] for idx in tmp]

            assert r1.name == 'idx2'
            assert r1.unique == True
            assert r2.unique == False
            assert [t2.c.id] == r1.columns
            assert [t2.c.name] == r2.columns
        finally:
            warnings.warn = capture_warnings._orig_showwarning
            m1.drop_all()

    def test_create_partial_index(self):
        tbl = Table('testtbl', MetaData(), Column('data',Integer))
        idx = Index('test_idx1', tbl.c.data, postgres_where=and_(tbl.c.data > 5, tbl.c.data < 10))

        executed_sql = []
        mock_strategy = MockEngineStrategy()
        mock_conn = mock_strategy.create('postgres://', executed_sql.append)

        idx.create(mock_conn)

        assert executed_sql == ['CREATE INDEX test_idx1 ON testtbl (data) WHERE testtbl.data > 5 AND testtbl.data < 10']

class TimezoneTest(TestBase, AssertsExecutionResults):
    """Test timezone-aware datetimes.

    psycopg will return a datetime with a tzinfo attached to it, if postgres
    returns it.  python then will not let you compare a datetime with a tzinfo
    to a datetime that doesnt have one.  this test illustrates two ways to
    have datetime types with and without timezone info.
    """

    __only_on__ = 'postgres'

    @classmethod
    def setup_class(cls):
        global tztable, notztable, metadata
        metadata = MetaData(testing.db)

        # current_timestamp() in postgres is assumed to return TIMESTAMP WITH TIMEZONE
        tztable = Table('tztable', metadata,
            Column("id", Integer, primary_key=True),
            Column("date", DateTime(timezone=True), onupdate=func.current_timestamp()),
            Column("name", String(20)),
        )
        notztable = Table('notztable', metadata,
            Column("id", Integer, primary_key=True),
            Column("date", DateTime(timezone=False), onupdate=cast(func.current_timestamp(), DateTime(timezone=False))),
            Column("name", String(20)),
        )
        metadata.create_all()
    @classmethod
    def teardown_class(cls):
        metadata.drop_all()

    def test_with_timezone(self):
        # get a date with a tzinfo
        somedate = testing.db.connect().scalar(func.current_timestamp().select())
        tztable.insert().execute(id=1, name='row1', date=somedate)
        c = tztable.update(tztable.c.id==1).execute(name='newname')
        print tztable.select(tztable.c.id==1).execute().fetchone()

    def test_without_timezone(self):
        # get a date without a tzinfo
        somedate = datetime.datetime(2005, 10,20, 11, 52, 00)
        notztable.insert().execute(id=1, name='row1', date=somedate)
        c = notztable.update(notztable.c.id==1).execute(name='newname')
        print notztable.select(tztable.c.id==1).execute().fetchone()

class ArrayTest(TestBase, AssertsExecutionResults):
    __only_on__ = 'postgres'

    @classmethod
    def setup_class(cls):
        global metadata, arrtable
        metadata = MetaData(testing.db)

        arrtable = Table('arrtable', metadata,
            Column('id', Integer, primary_key=True),
            Column('intarr', postgres.PGArray(Integer)),
            Column('strarr', postgres.PGArray(String(convert_unicode=True)), nullable=False)
        )
        metadata.create_all()
    @classmethod
    def teardown_class(cls):
        metadata.drop_all()

    def test_reflect_array_column(self):
        metadata2 = MetaData(testing.db)
        tbl = Table('arrtable', metadata2, autoload=True)
        assert isinstance(tbl.c.intarr.type, postgres.PGArray)
        assert isinstance(tbl.c.strarr.type, postgres.PGArray)
        assert isinstance(tbl.c.intarr.type.item_type, Integer)
        assert isinstance(tbl.c.strarr.type.item_type, String)

    def test_insert_array(self):
        arrtable.insert().execute(intarr=[1,2,3], strarr=['abc', 'def'])
        results = arrtable.select().execute().fetchall()
        eq_(len(results), 1)
        eq_(results[0]['intarr'], [1,2,3])
        eq_(results[0]['strarr'], ['abc','def'])
        arrtable.delete().execute()

    def test_array_where(self):
        arrtable.insert().execute(intarr=[1,2,3], strarr=['abc', 'def'])
        arrtable.insert().execute(intarr=[4,5,6], strarr='ABC')
        results = arrtable.select().where(arrtable.c.intarr == [1,2,3]).execute().fetchall()
        eq_(len(results), 1)
        eq_(results[0]['intarr'], [1,2,3])
        arrtable.delete().execute()

    def test_array_concat(self):
        arrtable.insert().execute(intarr=[1,2,3], strarr=['abc', 'def'])
        results = select([arrtable.c.intarr + [4,5,6]]).execute().fetchall()
        eq_(len(results), 1)
        eq_(results[0][0], [1,2,3,4,5,6])
        arrtable.delete().execute()

    def test_array_subtype_resultprocessor(self):
        arrtable.insert().execute(intarr=[4,5,6], strarr=[[u'm\xe4\xe4'], [u'm\xf6\xf6']])
        arrtable.insert().execute(intarr=[1,2,3], strarr=[u'm\xe4\xe4', u'm\xf6\xf6'])
        results = arrtable.select(order_by=[arrtable.c.intarr]).execute().fetchall()
        eq_(len(results), 2)
        eq_(results[0]['strarr'], [u'm\xe4\xe4', u'm\xf6\xf6'])
        eq_(results[1]['strarr'], [[u'm\xe4\xe4'], [u'm\xf6\xf6']])
        arrtable.delete().execute()

    def test_array_mutability(self):
        class Foo(object): pass
        footable = Table('foo', metadata,
            Column('id', Integer, primary_key=True),
            Column('intarr', postgres.PGArray(Integer), nullable=True)
        )
        mapper(Foo, footable)
        metadata.create_all()
        sess = create_session()

        foo = Foo()
        foo.id = 1
        foo.intarr = [1,2,3]
        sess.add(foo)
        sess.flush()
        sess.expunge_all()
        foo = sess.query(Foo).get(1)
        eq_(foo.intarr, [1,2,3])

        foo.intarr.append(4)
        sess.flush()
        sess.expunge_all()
        foo = sess.query(Foo).get(1)
        eq_(foo.intarr, [1,2,3,4])

        foo.intarr = []
        sess.flush()
        sess.expunge_all()
        eq_(foo.intarr, [])

        foo.intarr = None
        sess.flush()
        sess.expunge_all()
        eq_(foo.intarr, None)

        # Errors in r4217:
        foo = Foo()
        foo.id = 2
        sess.add(foo)
        sess.flush()

class TimeStampTest(TestBase, AssertsExecutionResults):
    __only_on__ = 'postgres'
    
    @testing.uses_deprecated()
    def test_timestamp(self):
        engine = testing.db
        connection = engine.connect()
        s = select([func.TIMESTAMP("12/25/07").label("ts")])
        result = connection.execute(s).fetchone()
        eq_(result[0], datetime.datetime(2007, 12, 25, 0, 0))

class ServerSideCursorsTest(TestBase, AssertsExecutionResults):
    __only_on__ = 'postgres'

    @classmethod
    def setup_class(cls):
        global ss_engine
        ss_engine = engines.testing_engine(options={'server_side_cursors':True})

    @classmethod
    def teardown_class(cls):
        ss_engine.dispose()

    def test_uses_ss(self):
        result = ss_engine.execute("select 1")
        assert result.cursor.name
        
        result = ss_engine.execute(text("select 1"))
        assert result.cursor.name

        result = ss_engine.execute(select([1]))
        assert result.cursor.name
        
    def test_roundtrip(self):
        test_table = Table('test_table', MetaData(ss_engine),
            Column('id', Integer, primary_key=True),
            Column('data', String(50))
        )
        test_table.create(checkfirst=True)
        try:
            test_table.insert().execute(data='data1')

            nextid = ss_engine.execute(Sequence('test_table_id_seq'))
            test_table.insert().execute(id=nextid, data='data2')

            eq_(test_table.select().execute().fetchall(), [(1, 'data1'), (2, 'data2')])

            test_table.update().where(test_table.c.id==2).values(data=test_table.c.data + ' updated').execute()
            eq_(test_table.select().execute().fetchall(), [(1, 'data1'), (2, 'data2 updated')])
            test_table.delete().execute()
            eq_(test_table.count().scalar(), 0)
        finally:
            test_table.drop(checkfirst=True)

class SpecialTypesTest(TestBase, ComparesTables):
    """test DDL and reflection of PG-specific types """
    
    __only_on__ = 'postgres'
    __excluded_on__ = (('postgres', '<', (8, 3, 0)),)
    
    @classmethod
    def setup_class(cls):
        global metadata, table
        metadata = MetaData(testing.db)

        # create these types so that we can issue
        # special SQL92 INTERVAL syntax
        class y2m(postgres.PGInterval):
            def get_col_spec(self):
                return "INTERVAL YEAR TO MONTH"

        class d2s(postgres.PGInterval):
            def get_col_spec(self):
                return "INTERVAL DAY TO SECOND"
        
        table = Table('sometable', metadata,
            Column('id', postgres.PGUuid, primary_key=True),
            Column('flag', postgres.PGBit),
            Column('addr', postgres.PGInet),
            Column('addr2', postgres.PGMacAddr),
            Column('addr3', postgres.PGCidr),
            Column('doubleprec', postgres.PGDoublePrecision),
            Column('plain_interval', postgres.PGInterval),
            Column('year_interval', y2m()),
            Column('month_interval', d2s()),
        )
        
        metadata.create_all()

        # cheat so that the "strict type check"
        # works
        table.c.year_interval.type = postgres.PGInterval()
        table.c.month_interval.type = postgres.PGInterval()
    
    @classmethod
    def teardown_class(cls):
        metadata.drop_all()
    
    def test_reflection(self):
        m = MetaData(testing.db)
        t = Table('sometable', m, autoload=True)
        
        self.assert_tables_equal(table, t, strict_types=True)

class MatchTest(TestBase, AssertsCompiledSQL):
    __only_on__ = 'postgres'
    __excluded_on__ = (('postgres', '<', (8, 3, 0)),)

    @classmethod
    def setup_class(cls):
        global metadata, cattable, matchtable
        metadata = MetaData(testing.db)

        cattable = Table('cattable', metadata,
            Column('id', Integer, primary_key=True),
            Column('description', String(50)),
        )
        matchtable = Table('matchtable', metadata,
            Column('id', Integer, primary_key=True),
            Column('title', String(200)),
            Column('category_id', Integer, ForeignKey('cattable.id')),
        )
        metadata.create_all()

        cattable.insert().execute([
            {'id': 1, 'description': 'Python'},
            {'id': 2, 'description': 'Ruby'},
        ])
        matchtable.insert().execute([
            {'id': 1, 'title': 'Agile Web Development with Rails', 'category_id': 2},
            {'id': 2, 'title': 'Dive Into Python', 'category_id': 1},
            {'id': 3, 'title': 'Programming Matz''s Ruby', 'category_id': 2},
            {'id': 4, 'title': 'The Definitive Guide to Django', 'category_id': 1},
            {'id': 5, 'title': 'Python in a Nutshell', 'category_id': 1}
        ])

    @classmethod
    def teardown_class(cls):
        metadata.drop_all()

    def test_expression(self):
        self.assert_compile(matchtable.c.title.match('somstr'), "matchtable.title @@ to_tsquery(%(title_1)s)")

    def test_simple_match(self):
        results = matchtable.select().where(matchtable.c.title.match('python')).order_by(matchtable.c.id).execute().fetchall()
        eq_([2, 5], [r.id for r in results])

    def test_simple_match_with_apostrophe(self):
        results = matchtable.select().where(matchtable.c.title.match("Matz''s")).execute().fetchall()
        eq_([3], [r.id for r in results])

    def test_simple_derivative_match(self):
        results = matchtable.select().where(matchtable.c.title.match('nutshells')).execute().fetchall()
        eq_([5], [r.id for r in results])

    def test_or_match(self):
        results1 = matchtable.select().where(or_(matchtable.c.title.match('nutshells'), 
                                                 matchtable.c.title.match('rubies'))
                                            ).order_by(matchtable.c.id).execute().fetchall()
        eq_([3, 5], [r.id for r in results1])
        results2 = matchtable.select().where(matchtable.c.title.match('nutshells | rubies'), 
                                            ).order_by(matchtable.c.id).execute().fetchall()
        eq_([3, 5], [r.id for r in results2])
        

    def test_and_match(self):
        results1 = matchtable.select().where(and_(matchtable.c.title.match('python'), 
                                                  matchtable.c.title.match('nutshells'))
                                            ).execute().fetchall()
        eq_([5], [r.id for r in results1])
        results2 = matchtable.select().where(matchtable.c.title.match('python & nutshells'), 
                                            ).execute().fetchall()
        eq_([5], [r.id for r in results2])

    def test_match_across_joins(self):
        results = matchtable.select().where(and_(cattable.c.id==matchtable.c.category_id, 
                                            or_(cattable.c.description.match('Ruby'), 
                                                matchtable.c.title.match('nutshells')))
                                           ).order_by(matchtable.c.id).execute().fetchall()
        eq_([1, 3, 5], [r.id for r in results])


