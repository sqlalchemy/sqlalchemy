import testenv; testenv.configure_for_tests()
import datetime
from sqlalchemy import *
from sqlalchemy.orm import *
from sqlalchemy import exc
from sqlalchemy.databases import postgres
from sqlalchemy.engine.strategies import MockEngineStrategy
from testlib import *
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
            self.assertEqual(result.fetchall(), [(1,)])

            result2 = select([table.c.id, table.c.full]).order_by(table.c.id).execute()
            self.assertEqual(result2.fetchall(), [(1,True),(2,False)])
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

            self.assertEqual(result.fetchall(), [(1,)])

            # Multiple inserts only return the last row
            result2 = table.insert(postgres_returning=[table]).execute(
                 [{'persons': 2, 'full': False}, {'persons': 3, 'full': True}])

            self.assertEqual(result2.fetchall(), [(3,3,True)])

            result3 = table.insert(postgres_returning=[(table.c.id*2).label('double_id')]).execute({'persons': 4, 'full': False})
            self.assertEqual([dict(row) for row in result3], [{'double_id':8}])

            result4 = testing.db.execute('insert into tables (id, persons, "full") values (5, 10, true) returning persons')
            self.assertEqual([dict(row) for row in result4], [{'persons': 10}])
        finally:
            table.drop()


class InsertTest(TestBase, AssertsExecutionResults):
    __only_on__ = 'postgres'

    def setUpAll(self):
        global metadata
        metadata = MetaData(testing.db)

    def tearDown(self):
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

    def setUpAll(self):
        con = testing.db.connect()
        try:
            con.execute('CREATE DOMAIN testdomain INTEGER NOT NULL DEFAULT 42')
            con.execute('CREATE DOMAIN alt_schema.testdomain INTEGER DEFAULT 0')
        except exc.SQLError, e:
            if not "already exists" in str(e):
                raise e
        con.execute('CREATE TABLE testtable (question integer, answer testdomain)')
        con.execute('CREATE TABLE alt_schema.testtable(question integer, answer alt_schema.testdomain, anything integer)')
        con.execute('CREATE TABLE crosschema (question integer, answer alt_schema.testdomain)')

    def tearDownAll(self):
        con = testing.db.connect()
        con.execute('DROP TABLE testtable')
        con.execute('DROP TABLE alt_schema.testtable')
        con.execute('DROP TABLE crosschema')
        con.execute('DROP DOMAIN testdomain')
        con.execute('DROP DOMAIN alt_schema.testdomain')

    def test_table_is_reflected(self):
        metadata = MetaData(testing.db)
        table = Table('testtable', metadata, autoload=True)
        self.assertEquals(set(table.columns.keys()), set(['question', 'answer']), "Columns of reflected table didn't equal expected columns")
        self.assertEquals(table.c.answer.type.__class__, postgres.PGInteger)

    def test_domain_is_reflected(self):
        metadata = MetaData(testing.db)
        table = Table('testtable', metadata, autoload=True)
        self.assertEquals(str(table.columns.answer.server_default.arg), '42', "Reflected default value didn't equal expected value")
        self.assertFalse(table.columns.answer.nullable, "Expected reflected column to not be nullable.")

    def test_table_is_reflected_alt_schema(self):
        metadata = MetaData(testing.db)
        table = Table('testtable', metadata, autoload=True, schema='alt_schema')
        self.assertEquals(set(table.columns.keys()), set(['question', 'answer', 'anything']), "Columns of reflected table didn't equal expected columns")
        self.assertEquals(table.c.anything.type.__class__, postgres.PGInteger)

    def test_schema_domain_is_reflected(self):
        metadata = MetaData(testing.db)
        table = Table('testtable', metadata, autoload=True, schema='alt_schema')
        self.assertEquals(str(table.columns.answer.server_default.arg), '0', "Reflected default value didn't equal expected value")
        self.assertTrue(table.columns.answer.nullable, "Expected reflected column to be nullable.")

    def test_crosschema_domain_is_reflected(self):
        metadata = MetaData(testing.db)
        table = Table('crosschema', metadata, autoload=True)
        self.assertEquals(str(table.columns.answer.server_default.arg), '0', "Reflected default value didn't equal expected value")
        self.assertTrue(table.columns.answer.nullable, "Expected reflected column to be nullable.")

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
        self.assertEquals(
            str(t.select(distinct=t.c.a)),
            'SELECT DISTINCT ON (mytable.a) mytable.id, mytable.a \n'
            'FROM mytable')
        self.assertEquals(
            str(t.select(distinct=['id','a'])),
            'SELECT DISTINCT ON (id, a) mytable.id, mytable.a \n'
            'FROM mytable')
        self.assertEquals(
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

            self.assertEquals(users.select().where(users.c.name=='name2').execute().fetchall(), [(2, 'name2')])
            self.assertEquals(users.select(use_labels=True).where(users.c.name=='name2').execute().fetchall(), [(2, 'name2')])

            users.delete().where(users.c.id==3).execute()
            self.assertEquals(users.select().where(users.c.name=='name3').execute().fetchall(), [])

            users.update().where(users.c.name=='name4').execute(name='newname')
            self.assertEquals(users.select(use_labels=True).where(users.c.id==4).execute().fetchall(), [(4, 'newname')])

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

    def setUpAll(self):
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
    def tearDownAll(self):
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

    def setUpAll(self):
        global metadata, arrtable
        metadata = MetaData(testing.db)

        arrtable = Table('arrtable', metadata,
            Column('id', Integer, primary_key=True),
            Column('intarr', postgres.PGArray(Integer)),
            Column('strarr', postgres.PGArray(String(convert_unicode=True)), nullable=False)
        )
        metadata.create_all()
    def tearDownAll(self):
        metadata.drop_all()

    def test_reflect_array_column(self):
        metadata2 = MetaData(testing.db)
        tbl = Table('arrtable', metadata2, autoload=True)
        self.assertTrue(isinstance(tbl.c.intarr.type, postgres.PGArray))
        self.assertTrue(isinstance(tbl.c.strarr.type, postgres.PGArray))
        self.assertTrue(isinstance(tbl.c.intarr.type.item_type, Integer))
        self.assertTrue(isinstance(tbl.c.strarr.type.item_type, String))

    def test_insert_array(self):
        arrtable.insert().execute(intarr=[1,2,3], strarr=['abc', 'def'])
        results = arrtable.select().execute().fetchall()
        self.assertEquals(len(results), 1)
        self.assertEquals(results[0]['intarr'], [1,2,3])
        self.assertEquals(results[0]['strarr'], ['abc','def'])
        arrtable.delete().execute()

    def test_array_where(self):
        arrtable.insert().execute(intarr=[1,2,3], strarr=['abc', 'def'])
        arrtable.insert().execute(intarr=[4,5,6], strarr='ABC')
        results = arrtable.select().where(arrtable.c.intarr == [1,2,3]).execute().fetchall()
        self.assertEquals(len(results), 1)
        self.assertEquals(results[0]['intarr'], [1,2,3])
        arrtable.delete().execute()

    def test_array_concat(self):
        arrtable.insert().execute(intarr=[1,2,3], strarr=['abc', 'def'])
        results = select([arrtable.c.intarr + [4,5,6]]).execute().fetchall()
        self.assertEquals(len(results), 1)
        self.assertEquals(results[0][0], [1,2,3,4,5,6])
        arrtable.delete().execute()

    def test_array_subtype_resultprocessor(self):
        arrtable.insert().execute(intarr=[4,5,6], strarr=[[u'm\xe4\xe4'], [u'm\xf6\xf6']])
        arrtable.insert().execute(intarr=[1,2,3], strarr=[u'm\xe4\xe4', u'm\xf6\xf6'])
        results = arrtable.select(order_by=[arrtable.c.intarr]).execute().fetchall()
        self.assertEquals(len(results), 2)
        self.assertEquals(results[0]['strarr'], [u'm\xe4\xe4', u'm\xf6\xf6'])
        self.assertEquals(results[1]['strarr'], [[u'm\xe4\xe4'], [u'm\xf6\xf6']])
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
        sess.save(foo)
        sess.flush()
        sess.clear()
        foo = sess.query(Foo).get(1)
        self.assertEquals(foo.intarr, [1,2,3])

        foo.intarr.append(4)
        sess.flush()
        sess.clear()
        foo = sess.query(Foo).get(1)
        self.assertEquals(foo.intarr, [1,2,3,4])

        foo.intarr = []
        sess.flush()
        sess.clear()
        self.assertEquals(foo.intarr, [])

        foo.intarr = None
        sess.flush()
        sess.clear()
        self.assertEquals(foo.intarr, None)

        # Errors in r4217:
        foo = Foo()
        foo.id = 2
        sess.save(foo)
        sess.flush()

class TimeStampTest(TestBase, AssertsExecutionResults):
    __only_on__ = 'postgres'
    def test_timestamp(self):
        engine = testing.db
        connection = engine.connect()
        s = select([func.TIMESTAMP("12/25/07").label("ts")])
        result = connection.execute(s).fetchone()
        self.assertEqual(result[0], datetime.datetime(2007, 12, 25, 0, 0))

class ServerSideCursorsTest(TestBase, AssertsExecutionResults):
    __only_on__ = 'postgres'

    def setUpAll(self):
        global ss_engine
        ss_engine = engines.testing_engine(options={'server_side_cursors':True})

    def tearDownAll(self):
        ss_engine.dispose()

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

            self.assertEquals(test_table.select().execute().fetchall(), [(1, 'data1'), (2, 'data2')])

            test_table.update().where(test_table.c.id==2).values(data=test_table.c.data + ' updated').execute()
            self.assertEquals(test_table.select().execute().fetchall(), [(1, 'data1'), (2, 'data2 updated')])
            test_table.delete().execute()
            self.assertEquals(test_table.count().scalar(), 0)
        finally:
            test_table.drop(checkfirst=True)


if __name__ == "__main__":
    testenv.main()
