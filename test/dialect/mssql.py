import testenv; testenv.configure_for_tests()
import re
from sqlalchemy import *
from sqlalchemy.orm import *
from sqlalchemy import exceptions
from sqlalchemy.sql import table, column
from sqlalchemy.databases import mssql
from testlib import *


class CompileTest(TestBase, AssertsCompiledSQL):
    __dialect__ = mssql.MSSQLDialect()

    def test_insert(self):
        t = table('sometable', column('somecolumn'))
        self.assert_compile(t.insert(), "INSERT INTO sometable (somecolumn) VALUES (:somecolumn)")

    def test_update(self):
        t = table('sometable', column('somecolumn'))
        self.assert_compile(t.update(t.c.somecolumn==7), "UPDATE sometable SET somecolumn=:somecolumn WHERE sometable.somecolumn = :sometable_somecolumn_1", dict(somecolumn=10))

    def test_count(self):
        t = table('sometable', column('somecolumn'))
        self.assert_compile(t.count(), "SELECT count(sometable.somecolumn) AS tbl_row_count FROM sometable")

    def test_noorderby_insubquery(self):
        """test that the ms-sql dialect removes ORDER BY clauses from subqueries"""

        table1 = table('mytable',
            column('myid', Integer),
            column('name', String),
            column('description', String),
        )

        q = select([table1.c.myid], order_by=[table1.c.myid]).alias('foo')
        crit = q.c.myid == table1.c.myid
        self.assert_compile(select(['*'], crit), """SELECT * FROM (SELECT mytable.myid AS myid FROM mytable) AS foo, mytable WHERE foo.myid = mytable.myid""")

    def test_aliases_schemas(self):
        metadata = MetaData()
        table1 = table('mytable',
            column('myid', Integer),
            column('name', String),
            column('description', String),
        )

        table4 = Table(
            'remotetable', metadata,
            Column('rem_id', Integer, primary_key=True),
            Column('datatype_id', Integer),
            Column('value', String(20)),
            schema = 'remote_owner'
        )

        self.assert_compile(table4.select(), "SELECT remotetable_1.rem_id, remotetable_1.datatype_id, remotetable_1.value FROM remote_owner.remotetable AS remotetable_1")

        self.assert_compile(table1.join(table4, table1.c.myid==table4.c.rem_id).select(), "SELECT mytable.myid, mytable.name, mytable.description, remotetable_1.rem_id, remotetable_1.datatype_id, remotetable_1.value FROM mytable JOIN remote_owner.remotetable AS remotetable_1 ON remotetable_1.rem_id = mytable.myid")

    def test_delete_schema(self):
        metadata = MetaData()
        tbl = Table('test', metadata, Column('id', Integer, primary_key=True), schema='paj')
        self.assert_compile(tbl.delete(tbl.c.id == 1), "DELETE FROM paj.test WHERE paj.test.id = :test_id_1")

    def test_union(self):
        t1 = table('t1',
            column('col1'),
            column('col2'),
            column('col3'),
            column('col4')
            )
        t2 = table('t2',
            column('col1'),
            column('col2'),
            column('col3'),
            column('col4'))

        (s1, s2) = (
                    select([t1.c.col3.label('col3'), t1.c.col4.label('col4')], t1.c.col2.in_(["t1col2r1", "t1col2r2"])),
            select([t2.c.col3.label('col3'), t2.c.col4.label('col4')], t2.c.col2.in_(["t2col2r2", "t2col2r3"]))
        )
        u = union(s1, s2, order_by=['col3', 'col4'])
        self.assert_compile(u, "SELECT t1.col3 AS col3, t1.col4 AS col4 FROM t1 WHERE t1.col2 IN (:t1_col2_1, :t1_col2_2) "\
        "UNION SELECT t2.col3 AS col3, t2.col4 AS col4 FROM t2 WHERE t2.col2 IN (:t2_col2_1, :t2_col2_2) ORDER BY col3, col4")

        self.assert_compile(u.alias('bar').select(), "SELECT bar.col3, bar.col4 FROM (SELECT t1.col3 AS col3, t1.col4 AS col4 FROM t1 WHERE "\
        "t1.col2 IN (:t1_col2_1, :t1_col2_2) UNION SELECT t2.col3 AS col3, t2.col4 AS col4 FROM t2 WHERE t2.col2 IN (:t2_col2_1, :t2_col2_2)) AS bar")

    def test_function(self):
        self.assert_compile(func.foo(1, 2), "foo(:foo_1, :foo_2)")
        self.assert_compile(func.current_time(), "CURRENT_TIME")
        self.assert_compile(func.foo(), "foo()")

        m = MetaData()
        t = Table('sometable', m, Column('col1', Integer), Column('col2', Integer))
        self.assert_compile(select([func.max(t.c.col1)]), "SELECT max(sometable.col1) AS max_1 FROM sometable")

class ReflectionTest(TestBase):
    __only_on__ = 'mssql'

    def testidentity(self):
        meta = MetaData(testing.db)
        table = Table(
            'identity_test', meta,
            Column('col1', Integer, Sequence('fred', 2, 3), primary_key=True)
        )
        table.create()

        meta2 = MetaData(testing.db)
        try:
            table2 = Table('identity_test', meta2, autoload=True)
            assert table2.c['col1'].sequence.start == 2
            assert table2.c['col1'].sequence.increment == 3
        finally:
            table.drop()


class QueryTest(TestBase):
    __only_on__ = 'mssql'

    def test_fetchid_trigger(self):
        meta = MetaData(testing.db)
        t1 = Table('t1', meta,
                Column('id', Integer, Sequence('fred', 100, 1), primary_key=True),
                Column('descr', String(200)))
        t2 = Table('t2', meta,
                Column('id', Integer, Sequence('fred', 200, 1), primary_key=True),
                Column('descr', String(200)))
        meta.create_all()
        con = testing.db.connect()
        con.execute("""create trigger paj on t1 for insert as
            insert into t2 (descr) select descr from inserted""")

        try:
            tr = con.begin()
            r = con.execute(t2.insert(), descr='hello')
            self.assert_(r.last_inserted_ids() == [200])
            r = con.execute(t1.insert(), descr='hello')
            self.assert_(r.last_inserted_ids() == [100])

        finally:
            tr.commit()
            con.execute("""drop trigger paj""")
            meta.drop_all()

    def test_insertid_schema(self):
        meta = MetaData(testing.db)
        con = testing.db.connect()
        con.execute('create schema paj')
        tbl = Table('test', meta, Column('id', Integer, primary_key=True), schema='paj')
        tbl.create()
        try:
            tbl.insert().execute({'id':1})
        finally:
            tbl.drop()
            con.execute('drop schema paj')

    def test_delete_schema(self):
        meta = MetaData(testing.db)
        con = testing.db.connect()
        con.execute('create schema paj')
        tbl = Table('test', meta, Column('id', Integer, primary_key=True), schema='paj')
        tbl.create()
        try:
            tbl.insert().execute({'id':1})
            tbl.delete(tbl.c.id == 1).execute()
        finally:
            tbl.drop()
            con.execute('drop schema paj')

    def test_insertid_reserved(self):
        meta = MetaData(testing.db)
        table = Table(
            'select', meta,
            Column('col', Integer, primary_key=True)
        )
        table.create()

        meta2 = MetaData(testing.db)
        try:
            table.insert().execute(col=7)
        finally:
            table.drop()

    def test_select_limit_nooffset(self):
        metadata = MetaData(testing.db)

        users = Table('query_users', metadata,
            Column('user_id', INT, primary_key = True),
            Column('user_name', VARCHAR(20)),
        )
        addresses = Table('query_addresses', metadata,
            Column('address_id', Integer, primary_key=True),
            Column('user_id', Integer, ForeignKey('query_users.user_id')),
            Column('address', String(30)))
        metadata.create_all()

        try:
            try:
                r = users.select(limit=3, offset=2,
                                 order_by=[users.c.user_id]).execute().fetchall()
                assert False # InvalidRequestError should have been raised
            except exceptions.InvalidRequestError:
                pass
        finally:
            metadata.drop_all()

class Foo(object):
    def __init__(self, **kw):
        for k in kw:
            setattr(self, k, kw[k])

class GenerativeQueryTest(TestBase):
    __only_on__ = 'mssql'

    def setUpAll(self):
        global foo, metadata
        metadata = MetaData(testing.db)
        foo = Table('foo', metadata,
                    Column('id', Integer, Sequence('foo_id_seq'),
                           primary_key=True),
                    Column('bar', Integer),
                    Column('range', Integer))

        mapper(Foo, foo)
        metadata.create_all()

        sess = create_session(bind=testing.db)
        for i in range(100):
            sess.save(Foo(bar=i, range=i%10))
        sess.flush()

    def tearDownAll(self):
        metadata.drop_all()
        clear_mappers()

    def test_slice_mssql(self):
        sess = create_session(bind=testing.db)
        query = sess.query(Foo)
        orig = query.all()
        assert list(query[:10]) == orig[:10]
        assert list(query[:10]) == orig[:10]


if __name__ == "__main__":
    testenv.main()
