"""SQLite-specific tests."""

import testenv; testenv.configure_for_tests()
import datetime
from sqlalchemy import *
from sqlalchemy import exc
from sqlalchemy.databases import sqlite
from testlib import *


class TestTypes(TestBase, AssertsExecutionResults):
    __only_on__ = 'sqlite'

    def test_date(self):
        meta = MetaData(testing.db)
        t = Table('testdate', meta,
                  Column('id', Integer, primary_key=True),
                  Column('adate', Date),
                  Column('adatetime', DateTime))
        meta.create_all()
        try:
            d1 = datetime.date(2007, 10, 30)
            d2 = datetime.datetime(2007, 10, 30)

            t.insert().execute(adate=str(d1), adatetime=str(d2))

            self.assert_(t.select().execute().fetchall()[0] ==
                         (1, datetime.date(2007, 10, 30),
                          datetime.datetime(2007, 10, 30)))

        finally:
            meta.drop_all()

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
                        print reflected.type, type(expected[i])
                        assert isinstance(reflected.type, type(expected[i])), type(expected[i])
            finally:
                db.execute('DROP VIEW types_v')
        finally:
            m.drop_all()

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
    # fails as recently as sqlite 3.3.6.  passes on 3.4.1.  this syntax
    # is nowhere to be found in the sqlite3 documentation or changelog, so can't
    # determine what versions in between it's legal for.
    def _test_empty_insert(self, table, expect=1):
        try:
            table.create()
            for wanted in (expect, expect * 2):

                table.insert().execute()

                rows = table.select().execute().fetchall()
                print rows
                self.assertEquals(len(rows), wanted)
        finally:
            table.drop()

    @testing.exclude('sqlite', '<', (3, 4), 'no database support')
    def test_empty_insert_pk1(self):
        self._test_empty_insert(
            Table('a', MetaData(testing.db),
                  Column('id', Integer, primary_key=True)))

    @testing.exclude('sqlite', '<', (3, 4), 'no database support')
    def test_empty_insert_pk2(self):
        self.assertRaises(
            exc.DBAPIError,
            self._test_empty_insert,
            Table('b', MetaData(testing.db),
                  Column('x', Integer, primary_key=True),
                  Column('y', Integer, primary_key=True)))

    @testing.exclude('sqlite', '<', (3, 4), 'no database support')
    def test_empty_insert_pk3(self):
        self.assertRaises(
            exc.DBAPIError,
            self._test_empty_insert,
            Table('c', MetaData(testing.db),
                  Column('x', Integer, primary_key=True),
                  Column('y', Integer, DefaultClause('123'),
                         primary_key=True)))

    @testing.exclude('sqlite', '<', (3, 4), 'no database support')
    def test_empty_insert_pk4(self):
        self._test_empty_insert(
            Table('d', MetaData(testing.db),
                  Column('x', Integer, primary_key=True),
                  Column('y', Integer, DefaultClause('123'))))

    @testing.exclude('sqlite', '<', (3, 4), 'no database support')
    def test_empty_insert_nopk1(self):
        self._test_empty_insert(
            Table('e', MetaData(testing.db),
                  Column('id', Integer)))

    @testing.exclude('sqlite', '<', (3, 4), 'no database support')
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


if __name__ == "__main__":
    testenv.main()
