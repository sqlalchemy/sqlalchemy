"""SQLite-specific tests."""

import testbase
import datetime
from sqlalchemy import *
from sqlalchemy import exceptions
from sqlalchemy.databases import sqlite
from testlib import *


class TestTypes(AssertMixin):
    @testing.supported('sqlite')
    def test_date(self):
        meta = MetaData(testbase.db)
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


class DialectTest(AssertMixin):
    @testing.supported('sqlite')
    def test_extra_reserved_words(self):
        """Tests reserved words in identifiers.

        'true', 'false', and 'column' are undocumented reserved words
        when used as column identifiers (as of 3.5.1).  Covering them here
        to ensure they remain in place if the dialect's reserved_words set
        is updated in the future.
        """

        meta = MetaData(testbase.db)
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

    @testing.supported('sqlite')
    def test_quoted_identifiers(self):
        """Tests autoload of tables created with quoted column names."""

        # This is quirky in sqlite.
        testbase.db.execute("""CREATE TABLE "django_content_type" (
            "id" integer NOT NULL PRIMARY KEY,
            "django_stuff" text NULL
        )
        """)
        testbase.db.execute("""
        CREATE TABLE "django_admin_log" (
            "id" integer NOT NULL PRIMARY KEY,
            "action_time" datetime NOT NULL,
            "content_type_id" integer NULL REFERENCES "django_content_type" ("id"),
            "object_id" text NULL,
            "change_message" text NOT NULL
        )
        """)
        try:
            meta = MetaData(testbase.db)
            table1 = Table("django_admin_log", meta, autoload=True)
            table2 = Table("django_content_type", meta, autoload=True)
            j = table1.join(table2)
            assert j.onclause == table1.c.content_type_id==table2.c.id
        finally:
            testbase.db.execute("drop table django_admin_log")
            testbase.db.execute("drop table django_content_type")


class InsertTest(AssertMixin):
    """Tests inserts and autoincrement."""

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

    @testing.supported('sqlite')
    @testing.exclude('sqlite', '<', (3, 4))
    def test_empty_insert_pk1(self):
        self._test_empty_insert(
            Table('a', MetaData(testbase.db),
                  Column('id', Integer, primary_key=True)))

    @testing.supported('sqlite')
    @testing.exclude('sqlite', '<', (3, 4))
    def test_empty_insert_pk2(self):
        self.assertRaises(
            exceptions.DBAPIError,
            self._test_empty_insert,
            Table('b', MetaData(testbase.db),
                  Column('x', Integer, primary_key=True),
                  Column('y', Integer, primary_key=True)))

    @testing.supported('sqlite')
    @testing.exclude('sqlite', '<', (3, 4))
    def test_empty_insert_pk3(self):
        self.assertRaises(
            exceptions.DBAPIError,
            self._test_empty_insert,
            Table('c', MetaData(testbase.db),
                  Column('x', Integer, primary_key=True),
                  Column('y', Integer, PassiveDefault('123'),
                         primary_key=True)))

    @testing.supported('sqlite')
    @testing.exclude('sqlite', '<', (3, 4))
    def test_empty_insert_pk4(self):
        self._test_empty_insert(
            Table('d', MetaData(testbase.db),
                  Column('x', Integer, primary_key=True),
                  Column('y', Integer, PassiveDefault('123'))))

    @testing.supported('sqlite')
    @testing.exclude('sqlite', '<', (3, 4))
    def test_empty_insert_nopk1(self):
        self._test_empty_insert(
            Table('e', MetaData(testbase.db),
                  Column('id', Integer)))
    
    @testing.supported('sqlite')
    @testing.exclude('sqlite', '<', (3, 4))
    def test_empty_insert_nopk2(self):
        self._test_empty_insert(
            Table('f', MetaData(testbase.db),
                  Column('x', Integer),
                  Column('y', Integer)))

    @testing.supported('sqlite')
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
    testbase.main()
