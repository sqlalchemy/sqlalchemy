import testbase
import datetime
from sqlalchemy import *
from sqlalchemy import exceptions
from sqlalchemy.databases import postgres
from testlib import *

class InsertTest(AssertMixin):
    @testing.supported('postgres')
    def setUpAll(self):
        global metadata
        metadata = MetaData(testbase.db)
        
    @testing.supported('postgres')
    def tearDown(self):
        metadata.drop_all()
        metadata.tables.clear()
        
    @testing.supported('postgres')
    def test_compiled_insert(self):
        table = Table('testtable', metadata, 
            Column('id', Integer, primary_key=True),
            Column('data', String(30)))
            
        metadata.create_all()

        ins = table.insert(values={'data':bindparam('x')}).compile()
        ins.execute({'x':"five"}, {'x':"seven"})
        assert table.select().execute().fetchall() == [(1, 'five'), (2, 'seven')]
        
    @testing.supported('postgres')
    def test_sequence_insert(self):
        table = Table('testtable', metadata, 
            Column('id', Integer, Sequence('my_seq'), primary_key=True),
            Column('data', String(30)))
        metadata.create_all()
        self._assert_data_with_sequence(table, "my_seq")

    @testing.supported('postgres')
    def test_opt_sequence_insert(self):
        table = Table('testtable', metadata, 
            Column('id', Integer, Sequence('my_seq', optional=True), primary_key=True),
            Column('data', String(30)))
        metadata.create_all()
        self._assert_data_autoincrement(table)

    @testing.supported('postgres')
    def test_autoincrement_insert(self):
        table = Table('testtable', metadata, 
            Column('id', Integer, primary_key=True),
            Column('data', String(30)))
        metadata.create_all()
        self._assert_data_autoincrement(table)

    @testing.supported('postgres')
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
    
        self.assert_sql(testbase.db, go, [], with_sequences=[
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
        m2 = MetaData(testbase.db)
        table = Table(table.name, m2, autoload=True)

        def go():
            table.insert().execute({'id':30, 'data':'d1'})
            r = table.insert().execute({'data':'d2'})
            assert r.last_inserted_ids() == [5]
            table.insert().execute({'id':31, 'data':'d3'}, {'id':32, 'data':'d4'})
            table.insert().execute({'data':'d5'}, {'data':'d6'})
            table.insert(inline=True).execute({'id':33, 'data':'d7'})
            table.insert(inline=True).execute({'data':'d8'})
    
        self.assert_sql(testbase.db, go, [], with_sequences=[
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

        self.assert_sql(testbase.db, go, [], with_sequences=[
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
        except exceptions.IntegrityError, e:
            assert "violates not-null constraint" in str(e)
        try:
            table.insert().execute({'data':'d2'}, {'data':'d3'})
            assert False
        except exceptions.IntegrityError, e:
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
        m2 = MetaData(testbase.db)
        table = Table(table.name, m2, autoload=True)
        table.insert().execute({'id':30, 'data':'d1'})
        try:
            table.insert().execute({'data':'d2'})
            assert False
        except exceptions.IntegrityError, e:
            assert "violates not-null constraint" in str(e)
        try:
            table.insert().execute({'data':'d2'}, {'data':'d3'})
            assert False
        except exceptions.IntegrityError, e:
            assert "violates not-null constraint" in str(e)
            
        table.insert().execute({'id':31, 'data':'d2'}, {'id':32, 'data':'d3'})
        table.insert(inline=True).execute({'id':33, 'data':'d4'})
    
        assert table.select().execute().fetchall() == [
            (30, 'd1'),
            (31, 'd2'),
            (32, 'd3'),
            (33, 'd4'),
        ]
    
class DomainReflectionTest(AssertMixin):
    "Test PostgreSQL domains"

    @testing.supported('postgres')
    def setUpAll(self):
        con = testbase.db.connect()
        try:
            con.execute('CREATE DOMAIN testdomain INTEGER NOT NULL DEFAULT 42')
            con.execute('CREATE DOMAIN alt_schema.testdomain INTEGER DEFAULT 0')
        except exceptions.SQLError, e:
            if not "already exists" in str(e):
                raise e
        con.execute('CREATE TABLE testtable (question integer, answer testdomain)')
        con.execute('CREATE TABLE alt_schema.testtable(question integer, answer alt_schema.testdomain, anything integer)')
        con.execute('CREATE TABLE crosschema (question integer, answer alt_schema.testdomain)')

    @testing.supported('postgres')
    def tearDownAll(self):
        con = testbase.db.connect()
        con.execute('DROP TABLE testtable')
        con.execute('DROP TABLE alt_schema.testtable')
        con.execute('DROP TABLE crosschema')
        con.execute('DROP DOMAIN testdomain')
        con.execute('DROP DOMAIN alt_schema.testdomain')

    @testing.supported('postgres')
    def test_table_is_reflected(self):
        metadata = MetaData(testbase.db)
        table = Table('testtable', metadata, autoload=True)
        self.assertEquals(set(table.columns.keys()), set(['question', 'answer']), "Columns of reflected table didn't equal expected columns")
        self.assertEquals(table.c.answer.type.__class__, postgres.PGInteger)
        
    @testing.supported('postgres')
    def test_domain_is_reflected(self):
        metadata = MetaData(testbase.db)
        table = Table('testtable', metadata, autoload=True)
        self.assertEquals(str(table.columns.answer.default.arg), '42', "Reflected default value didn't equal expected value")
        self.assertFalse(table.columns.answer.nullable, "Expected reflected column to not be nullable.")

    @testing.supported('postgres')
    def test_table_is_reflected_alt_schema(self):
        metadata = MetaData(testbase.db)
        table = Table('testtable', metadata, autoload=True, schema='alt_schema')
        self.assertEquals(set(table.columns.keys()), set(['question', 'answer', 'anything']), "Columns of reflected table didn't equal expected columns")
        self.assertEquals(table.c.anything.type.__class__, postgres.PGInteger)

    @testing.supported('postgres')
    def test_schema_domain_is_reflected(self):
        metadata = MetaData(testbase.db)
        table = Table('testtable', metadata, autoload=True, schema='alt_schema')
        self.assertEquals(str(table.columns.answer.default.arg), '0', "Reflected default value didn't equal expected value")
        self.assertTrue(table.columns.answer.nullable, "Expected reflected column to be nullable.")

    @testing.supported('postgres')
    def test_crosschema_domain_is_reflected(self):
        metadata = MetaData(testbase.db)
        table = Table('crosschema', metadata, autoload=True)
        self.assertEquals(str(table.columns.answer.default.arg), '0', "Reflected default value didn't equal expected value")
        self.assertTrue(table.columns.answer.nullable, "Expected reflected column to be nullable.")

class MiscTest(AssertMixin):
    @testing.supported('postgres')
    def test_date_reflection(self):
        m1 = MetaData(testbase.db)
        t1 = Table('pgdate', m1, 
            Column('date1', DateTime(timezone=True)),
            Column('date2', DateTime(timezone=False))
            )
        m1.create_all()
        try:
            m2 = MetaData(testbase.db)
            t2 = Table('pgdate', m2, autoload=True)
            assert t2.c.date1.type.timezone is True
            assert t2.c.date2.type.timezone is False
        finally:
            m1.drop_all()

    @testing.supported('postgres')
    def test_pg_weirdchar_reflection(self):
        meta1 = MetaData(testbase.db)
        subject = Table("subject", meta1,
                        Column("id$", Integer, primary_key=True),
                        )

        referer = Table("referer", meta1,
                        Column("id", Integer, primary_key=True),
                        Column("ref", Integer, ForeignKey('subject.id$')),
                        )
        meta1.create_all()
        try:
            meta2 = MetaData(testbase.db)
            subject = Table("subject", meta2, autoload=True)
            referer = Table("referer", meta2, autoload=True)
            print str(subject.join(referer).onclause)
            self.assert_((subject.c['id$']==referer.c.ref).compare(subject.join(referer).onclause))
        finally:
            meta1.drop_all()
        
    @testing.supported('postgres')
    def test_checksfor_sequence(self):
        meta1 = MetaData(testbase.db)
        t = Table('mytable', meta1, 
            Column('col1', Integer, Sequence('fooseq')))
        try:
            testbase.db.execute("CREATE SEQUENCE fooseq")
            t.create(checkfirst=True)
        finally:
            t.drop(checkfirst=True)

    @testing.supported('postgres')
    def test_distinct_on(self):
        t = Table('mytable', MetaData(testbase.db),
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

    @testing.supported('postgres')
    def test_schema_reflection(self):
        """note: this test requires that the 'alt_schema' schema be separate and accessible by the test user"""

        meta1 = MetaData(testbase.db)
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
            meta2 = MetaData(testbase.db)
            addresses = Table('email_addresses', meta2, autoload=True, schema="alt_schema")
            users = Table('users', meta2, mustexist=True, schema="alt_schema")

            print users
            print addresses
            j = join(users, addresses)
            print str(j.onclause)
            self.assert_((users.c.user_id==addresses.c.remote_user_id).compare(j.onclause))
        finally:
            meta1.drop_all()

    @testing.supported('postgres')
    def test_schema_reflection_2(self):
        meta1 = MetaData(testbase.db)
        subject = Table("subject", meta1,
                        Column("id", Integer, primary_key=True),
                        )

        referer = Table("referer", meta1,
                        Column("id", Integer, primary_key=True),
                        Column("ref", Integer, ForeignKey('subject.id')),
                        schema="alt_schema")
        meta1.create_all()
        try:
            meta2 = MetaData(testbase.db)
            subject = Table("subject", meta2, autoload=True)
            referer = Table("referer", meta2, schema="alt_schema", autoload=True)
            print str(subject.join(referer).onclause)
            self.assert_((subject.c.id==referer.c.ref).compare(subject.join(referer).onclause))
        finally:
            meta1.drop_all()
            
    @testing.supported('postgres')
    def test_schema_reflection_3(self):
        meta1 = MetaData(testbase.db)
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
            meta2 = MetaData(testbase.db)
            subject = Table("subject", meta2, autoload=True, schema="alt_schema_2")
            referer = Table("referer", meta2, schema="alt_schema", autoload=True)
            print str(subject.join(referer).onclause)
            self.assert_((subject.c.id==referer.c.ref).compare(subject.join(referer).onclause))
        finally:
            meta1.drop_all()
        
    @testing.supported('postgres')
    def test_preexecute_passivedefault(self):
        """test that when we get a primary key column back 
        from reflecting a table which has a default value on it, we pre-execute
        that PassiveDefault upon insert."""
        
        try:
            meta = MetaData(testbase.db)
            testbase.db.execute("""
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
            testbase.db.execute("drop table speedy_users", None)

class TimezoneTest(AssertMixin):
    """test timezone-aware datetimes.  psycopg will return a datetime with a tzinfo attached to it,
    if postgres returns it.  python then will not let you compare a datetime with a tzinfo to a datetime
    that doesnt have one.  this test illustrates two ways to have datetime types with and without timezone
    info. """
    @testing.supported('postgres')
    def setUpAll(self):
        global tztable, notztable, metadata
        metadata = MetaData(testbase.db)

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
    @testing.supported('postgres')
    def tearDownAll(self):
        metadata.drop_all()

    @testing.supported('postgres')
    def test_with_timezone(self):
        # get a date with a tzinfo
        somedate = testbase.db.connect().scalar(func.current_timestamp().select())
        tztable.insert().execute(id=1, name='row1', date=somedate)
        c = tztable.update(tztable.c.id==1).execute(name='newname')
        print tztable.select(tztable.c.id==1).execute().fetchone()

    @testing.supported('postgres')
    def test_without_timezone(self):
        # get a date without a tzinfo
        somedate = datetime.datetime(2005, 10,20, 11, 52, 00)
        notztable.insert().execute(id=1, name='row1', date=somedate)
        c = notztable.update(notztable.c.id==1).execute(name='newname')
        print notztable.select(tztable.c.id==1).execute().fetchone()

class ArrayTest(AssertMixin):
    @testing.supported('postgres')
    def setUpAll(self):
        global metadata, arrtable
        metadata = MetaData(testbase.db)
        
        arrtable = Table('arrtable', metadata,
            Column('id', Integer, primary_key=True),
            Column('intarr', postgres.PGArray(Integer)),
            Column('strarr', postgres.PGArray(String), nullable=False)
        )
        metadata.create_all()
    @testing.supported('postgres')
    def tearDownAll(self):
        metadata.drop_all()
    
    @testing.supported('postgres')
    def test_reflect_array_column(self):
        metadata2 = MetaData(testbase.db)
        tbl = Table('arrtable', metadata2, autoload=True)
        self.assertTrue(isinstance(tbl.c.intarr.type, postgres.PGArray))
        self.assertTrue(isinstance(tbl.c.strarr.type, postgres.PGArray))
        self.assertTrue(isinstance(tbl.c.intarr.type.item_type, Integer))
        self.assertTrue(isinstance(tbl.c.strarr.type.item_type, String))
        
    @testing.supported('postgres')
    def test_insert_array(self):
        arrtable.insert().execute(intarr=[1,2,3], strarr=['abc', 'def'])
        results = arrtable.select().execute().fetchall()
        self.assertEquals(len(results), 1)
        self.assertEquals(results[0]['intarr'], [1,2,3])
        self.assertEquals(results[0]['strarr'], ['abc','def'])
        arrtable.delete().execute()

    @testing.supported('postgres')
    def test_array_where(self):
        arrtable.insert().execute(intarr=[1,2,3], strarr=['abc', 'def'])
        arrtable.insert().execute(intarr=[4,5,6], strarr='ABC')
        results = arrtable.select().where(arrtable.c.intarr == [1,2,3]).execute().fetchall()
        self.assertEquals(len(results), 1)
        self.assertEquals(results[0]['intarr'], [1,2,3])
        arrtable.delete().execute()
    
    @testing.supported('postgres')
    def test_array_concat(self):
        arrtable.insert().execute(intarr=[1,2,3], strarr=['abc', 'def'])
        results = select([arrtable.c.intarr + [4,5,6]]).execute().fetchall()
        self.assertEquals(len(results), 1)
        self.assertEquals(results[0][0], [1,2,3,4,5,6])
        arrtable.delete().execute()

if __name__ == "__main__":
    testbase.main()
