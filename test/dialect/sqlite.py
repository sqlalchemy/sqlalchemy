"""SQLite-specific tests."""

import testenv; testenv.configure_for_tests()
import datetime
from sqlalchemy import *
from sqlalchemy import exc
from sqlalchemy.databases import sqlite
from testlib import *


class TestTypes(TestBase, AssertsExecutionResults):
    __only_on__ = 'sqlite'

    def test_string_dates_raise(self):
        self.assertRaises(TypeError, testing.db.execute, select([1]).where(bindparam("date", type_=Date)), date=str(datetime.date(2007, 10, 30)))
    
    def test_time_microseconds(self):
        dt = datetime.datetime(2008, 6, 27, 12, 0, 0, 125)  # 125 usec
        self.assertEquals(str(dt), '2008-06-27 12:00:00.000125')
        sldt = sqlite.SLDateTime()
        bp = sldt.bind_processor(None)
        self.assertEquals(bp(dt), '2008-06-27 12:00:00.000125')
        
        rp = sldt.result_processor(None)
        self.assertEquals(rp(bp(dt)), dt)
        
        sldt.__legacy_microseconds__ = True
        bp = sldt.bind_processor(None)
        self.assertEquals(bp(dt), '2008-06-27 12:00:00.125')
        self.assertEquals(rp(bp(dt)), dt)

    def test_no_convert_unicode(self):
        """test no utf-8 encoding occurs"""
        
        dialect = sqlite.dialect()
        for t in (
                String(convert_unicode=True),
                CHAR(convert_unicode=True),
                Unicode(),
                UnicodeText(),
                String(assert_unicode=True, convert_unicode=True),
                CHAR(assert_unicode=True, convert_unicode=True),
                Unicode(assert_unicode=True),
                UnicodeText(assert_unicode=True)
            ):

            bindproc = t.dialect_impl(dialect).bind_processor(dialect)
            assert not bindproc or isinstance(bindproc(u"some string"), unicode)
        
    @testing.uses_deprecated('Using String type with no length')
    def test_type_reflection(self):
        # (ask_for, roundtripped_as_if_different)
        specs = [( String(), sqlite.SLString(), ),
                 ( String(1), sqlite.SLString(1), ),
                 ( String(3), sqlite.SLString(3), ),
                 ( Text(), sqlite.SLText(), ),
                 ( Unicode(), sqlite.SLString(), ),
                 ( Unicode(1), sqlite.SLString(1), ),
                 ( Unicode(3), sqlite.SLString(3), ),
                 ( UnicodeText(), sqlite.SLText(), ),
                 ( CLOB, sqlite.SLText(), ),
                 ( sqlite.SLChar(1), ),
                 ( CHAR(3), sqlite.SLChar(3), ),
                 ( NCHAR(2), sqlite.SLChar(2), ),
                 ( SmallInteger(), sqlite.SLSmallInteger(), ),
                 ( sqlite.SLSmallInteger(), ),
                 ( Binary(3), sqlite.SLBinary(), ),
                 ( Binary(), sqlite.SLBinary() ),
                 ( sqlite.SLBinary(3), sqlite.SLBinary(), ),
                 ( NUMERIC, sqlite.SLNumeric(), ),
                 ( NUMERIC(10,2), sqlite.SLNumeric(10,2), ),
                 ( Numeric, sqlite.SLNumeric(), ),
                 ( Numeric(10, 2), sqlite.SLNumeric(10, 2), ),
                 ( DECIMAL, sqlite.SLNumeric(), ),
                 ( DECIMAL(10, 2), sqlite.SLNumeric(10, 2), ),
                 ( Float, sqlite.SLNumeric(), ),
                 ( sqlite.SLNumeric(), ),
                 ( INT, sqlite.SLInteger(), ),
                 ( Integer, sqlite.SLInteger(), ),
                 ( sqlite.SLInteger(), ),
                 ( TIMESTAMP, sqlite.SLDateTime(), ),
                 ( DATETIME, sqlite.SLDateTime(), ),
                 ( DateTime, sqlite.SLDateTime(), ),
                 ( sqlite.SLDateTime(), ),
                 ( DATE, sqlite.SLDate(), ),
                 ( Date, sqlite.SLDate(), ),
                 ( sqlite.SLDate(), ),
                 ( TIME, sqlite.SLTime(), ),
                 ( Time, sqlite.SLTime(), ),
                 ( sqlite.SLTime(), ),
                 ( BOOLEAN, sqlite.SLBoolean(), ),
                 ( Boolean, sqlite.SLBoolean(), ),
                 ( sqlite.SLBoolean(), ),
                 ]
        columns = [Column('c%i' % (i + 1), t[0]) for i, t in enumerate(specs)]

        db = testing.db
        m = MetaData(db)
        t_table = Table('types', m, *columns)
        try:
            m.create_all()

            m2 = MetaData(db)
            rt = Table('types', m2, autoload=True)
            try:
                db.execute('CREATE VIEW types_v AS SELECT * from types')
                rv = Table('types_v', m2, autoload=True)

                expected = [len(c) > 1 and c[1] or c[0] for c in specs]
                for table in rt, rv:
                    for i, reflected in enumerate(table.c):
                        assert isinstance(reflected.type, type(expected[i])), type(expected[i])
            finally:
                db.execute('DROP VIEW types_v')
        finally:
            m.drop_all()


class TestDefaults(TestBase, AssertsExecutionResults):
    __only_on__ = 'sqlite'

    @testing.exclude('sqlite', '<', (3, 3, 8), 
        "sqlite3 changesets 3353 and 3440 modified behavior of default displayed in pragma table_info()")
    def test_default_reflection(self):
        # (ask_for, roundtripped_as_if_different)
        specs = [( String(3), '"foo"' ),
                 ( NUMERIC(10,2), '100.50' ),
                 ( Integer, '5' ),
                 ( Boolean, 'False' ),
                 ]
        columns = [Column('c%i' % (i + 1), t[0], server_default=text(t[1])) for i, t in enumerate(specs)]

        db = testing.db
        m = MetaData(db)
        t_table = Table('t_defaults', m, *columns)

        try:
            m.create_all()

            m2 = MetaData(db)
            rt = Table('t_defaults', m2, autoload=True)
            expected = [c[1] for c in specs]
            for i, reflected in enumerate(rt.c):
                self.assertEquals(reflected.server_default.arg.text, expected[i])
        finally:
            m.drop_all()

    @testing.exclude('sqlite', '<', (3, 3, 8), 
        "sqlite3 changesets 3353 and 3440 modified behavior of default displayed in pragma table_info()")
    def test_default_reflection_2(self):
        db = testing.db
        m = MetaData(db)

        expected = ["'my_default'", '0']
        table = """CREATE TABLE r_defaults (
            data VARCHAR(40) DEFAULT 'my_default',
            val INTEGER NOT NULL DEFAULT 0
        )"""

        try:
            db.execute(table)

            rt = Table('r_defaults', m, autoload=True)
            for i, reflected in enumerate(rt.c):
                self.assertEquals(reflected.server_default.arg.text, expected[i])
        finally:
            db.execute("DROP TABLE r_defaults")


class DialectTest(TestBase, AssertsExecutionResults):
    __only_on__ = 'sqlite'

    def test_extra_reserved_words(self):
        """Tests reserved words in identifiers.

        'true', 'false', and 'column' are undocumented reserved words
        when used as column identifiers (as of 3.5.1).  Covering them here
        to ensure they remain in place if the dialect's reserved_words set
        is updated in the future.
        """

        meta = MetaData(testing.db)
        t = Table('reserved', meta,
                  Column('safe', Integer),
                  Column('true', Integer),
                  Column('false', Integer),
                  Column('column', Integer))

        try:
            meta.create_all()
            t.insert().execute(safe=1)
            list(t.select().execute())
        finally:
            meta.drop_all()

    def test_quoted_identifiers(self):
        """Tests autoload of tables created with quoted column names."""

        # This is quirky in sqlite.
        testing.db.execute("""CREATE TABLE "django_content_type" (
            "id" integer NOT NULL PRIMARY KEY,
            "django_stuff" text NULL
        )
        """)
        testing.db.execute("""
        CREATE TABLE "django_admin_log" (
            "id" integer NOT NULL PRIMARY KEY,
            "action_time" datetime NOT NULL,
            "content_type_id" integer NULL REFERENCES "django_content_type" ("id"),
            "object_id" text NULL,
            "change_message" text NOT NULL
        )
        """)
        try:
            meta = MetaData(testing.db)
            table1 = Table("django_admin_log", meta, autoload=True)
            table2 = Table("django_content_type", meta, autoload=True)
            j = table1.join(table2)
            assert j.onclause == table1.c.content_type_id==table2.c.id
        finally:
            testing.db.execute("drop table django_admin_log")
            testing.db.execute("drop table django_content_type")


    def test_attached_as_schema(self):
        cx = testing.db.connect()
        try:
            cx.execute('ATTACH DATABASE ":memory:" AS  alt_schema')
            dialect = cx.dialect
            assert dialect.table_names(cx, 'alt_schema') == []

            meta = MetaData(cx)
            Table('created', meta, Column('id', Integer),
                  schema='alt_schema')
            alt_master = Table('sqlite_master', meta, autoload=True,
                               schema='alt_schema')
            meta.create_all(cx)

            self.assertEquals(dialect.table_names(cx, 'alt_schema'),
                              ['created'])
            assert len(alt_master.c) > 0

            meta.clear()
            reflected = Table('created', meta, autoload=True,
                              schema='alt_schema')
            assert len(reflected.c) == 1

            cx.execute(reflected.insert(), dict(id=1))
            r = cx.execute(reflected.select()).fetchall()
            assert list(r) == [(1,)]

            cx.execute(reflected.update(), dict(id=2))
            r = cx.execute(reflected.select()).fetchall()
            assert list(r) == [(2,)]

            cx.execute(reflected.delete(reflected.c.id==2))
            r = cx.execute(reflected.select()).fetchall()
            assert list(r) == []

            # note that sqlite_master is cleared, above
            meta.drop_all()

            assert dialect.table_names(cx, 'alt_schema') == []
        finally:
            cx.execute('DETACH DATABASE alt_schema')

    @testing.exclude('sqlite', '<', (2, 6), 'no database support')
    def test_temp_table_reflection(self):
        cx = testing.db.connect()
        try:
            cx.execute('CREATE TEMPORARY TABLE tempy (id INT)')

            assert 'tempy' in cx.dialect.table_names(cx, None)

            meta = MetaData(cx)
            tempy = Table('tempy', meta, autoload=True)
            assert len(tempy.c) == 1
            meta.drop_all()
        except:
            try:
                cx.execute('DROP TABLE tempy')
            except exc.DBAPIError:
                pass
            raise

class InsertTest(TestBase, AssertsExecutionResults):
    """Tests inserts and autoincrement."""

    __only_on__ = 'sqlite'

    # empty insert (i.e. INSERT INTO table DEFAULT VALUES)
    # fails on 3.3.7 and before
    def _test_empty_insert(self, table, expect=1):
        try:
            table.create()
            for wanted in (expect, expect * 2):

                table.insert().execute()

                rows = table.select().execute().fetchall()
                self.assertEquals(len(rows), wanted)
        finally:
            table.drop()

    @testing.exclude('sqlite', '<', (3, 3, 8), 'no database support')
    def test_empty_insert_pk1(self):
        self._test_empty_insert(
            Table('a', MetaData(testing.db),
                  Column('id', Integer, primary_key=True)))

    @testing.exclude('sqlite', '<', (3, 3, 8), 'no database support')
    def test_empty_insert_pk2(self):
        self.assertRaises(
            exc.DBAPIError,
            self._test_empty_insert,
            Table('b', MetaData(testing.db),
                  Column('x', Integer, primary_key=True),
                  Column('y', Integer, primary_key=True)))

    @testing.exclude('sqlite', '<', (3, 3, 8), 'no database support')
    def test_empty_insert_pk3(self):
        self.assertRaises(
            exc.DBAPIError,
            self._test_empty_insert,
            Table('c', MetaData(testing.db),
                  Column('x', Integer, primary_key=True),
                  Column('y', Integer, DefaultClause('123'),
                         primary_key=True)))

    @testing.exclude('sqlite', '<', (3, 3, 8), 'no database support')
    def test_empty_insert_pk4(self):
        self._test_empty_insert(
            Table('d', MetaData(testing.db),
                  Column('x', Integer, primary_key=True),
                  Column('y', Integer, DefaultClause('123'))))

    @testing.exclude('sqlite', '<', (3, 3, 8), 'no database support')
    def test_empty_insert_nopk1(self):
        self._test_empty_insert(
            Table('e', MetaData(testing.db),
                  Column('id', Integer)))

    @testing.exclude('sqlite', '<', (3, 3, 8), 'no database support')
    def test_empty_insert_nopk2(self):
        self._test_empty_insert(
            Table('f', MetaData(testing.db),
                  Column('x', Integer),
                  Column('y', Integer)))

    def test_inserts_with_spaces(self):
        tbl = Table('tbl', MetaData('sqlite:///'),
                  Column('with space', Integer),
                  Column('without', Integer))
        tbl.create()
        try:
            tbl.insert().execute({'without':123})
            assert list(tbl.select().execute()) == [(None, 123)]

            tbl.insert().execute({'with space':456})
            assert list(tbl.select().execute()) == [(None, 123), (456, None)]

        finally:
            tbl.drop()

def full_text_search_missing():
    """Test if full text search is not implemented and return False if 
    it is and True otherwise."""

    try:
        testing.db.execute("CREATE VIRTUAL TABLE t using FTS3;")
        testing.db.execute("DROP TABLE t;")
        return False
    except:
        return True

class MatchTest(TestBase, AssertsCompiledSQL):
    __only_on__ = 'sqlite'
    __skip_if__ = (full_text_search_missing, )

    def setUpAll(self):
        global metadata, cattable, matchtable
        metadata = MetaData(testing.db)
        
        testing.db.execute("""
        CREATE VIRTUAL TABLE cattable using FTS3 (
            id INTEGER NOT NULL, 
            description VARCHAR(50), 
            PRIMARY KEY (id)
        )
        """)
        cattable = Table('cattable', metadata, autoload=True)
        
        testing.db.execute("""
        CREATE VIRTUAL TABLE matchtable using FTS3 (
            id INTEGER NOT NULL, 
            title VARCHAR(200),
            category_id INTEGER NOT NULL, 
            PRIMARY KEY (id)
        )
        """)
        matchtable = Table('matchtable', metadata, autoload=True)
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

    def tearDownAll(self):
        metadata.drop_all()

    def test_expression(self):
        self.assert_compile(matchtable.c.title.match('somstr'), "matchtable.title MATCH ?")

    def test_simple_match(self):
        results = matchtable.select().where(matchtable.c.title.match('python')).order_by(matchtable.c.id).execute().fetchall()
        self.assertEquals([2, 5], [r.id for r in results])

    def test_simple_prefix_match(self):
        results = matchtable.select().where(matchtable.c.title.match('nut*')).execute().fetchall()
        self.assertEquals([5], [r.id for r in results])

    def test_or_match(self):
        results2 = matchtable.select().where(matchtable.c.title.match('nutshell OR ruby'), 
                                            ).order_by(matchtable.c.id).execute().fetchall()
        self.assertEquals([3, 5], [r.id for r in results2])
        

    def test_and_match(self):
        results2 = matchtable.select().where(matchtable.c.title.match('python nutshell'), 
                                            ).execute().fetchall()
        self.assertEquals([5], [r.id for r in results2])

    def test_match_across_joins(self):
        results = matchtable.select().where(and_(cattable.c.id==matchtable.c.category_id, 
                                            cattable.c.description.match('Ruby'))
                                           ).order_by(matchtable.c.id).execute().fetchall()
        self.assertEquals([1, 3], [r.id for r in results])


if __name__ == "__main__":
    testenv.main()
