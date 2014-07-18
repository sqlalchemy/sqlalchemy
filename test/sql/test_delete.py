#! coding:utf-8

from sqlalchemy import Column, Integer, String, Table, delete, select, and_, \
    or_
from sqlalchemy.dialects import mysql
from sqlalchemy.testing import AssertsCompiledSQL, fixtures


class _DeleteTestBase(object):

    @classmethod
    def define_tables(cls, metadata):
        Table('mytable', metadata,
              Column('myid', Integer),
              Column('name', String(30)),
              Column('description', String(50)))
        Table('myothertable', metadata,
              Column('otherid', Integer),
              Column('othername', String(30)))


class DeleteTest(_DeleteTestBase, fixtures.TablesTest, AssertsCompiledSQL):
    __dialect__ = 'default'

    def test_delete(self):
        table1 = self.tables.mytable

        self.assert_compile(
            delete(table1, table1.c.myid == 7),
            'DELETE FROM mytable WHERE mytable.myid = :myid_1')

        self.assert_compile(
            table1.delete().where(table1.c.myid == 7),
            'DELETE FROM mytable WHERE mytable.myid = :myid_1')

        self.assert_compile(
            table1.delete().
            where(table1.c.myid == 7).
            where(table1.c.name == 'somename'),
            'DELETE FROM mytable '
            'WHERE mytable.myid = :myid_1 '
            'AND mytable.name = :name_1')

    def test_where_empty(self):
        table1 = self.tables.mytable

        self.assert_compile(
            table1.delete().where(and_()),
            "DELETE FROM mytable"
        )
        self.assert_compile(
            table1.delete().where(or_()),
            "DELETE FROM mytable"
        )

    def test_prefix_with(self):
        table1 = self.tables.mytable

        stmt = table1.delete().\
            prefix_with('A', 'B', dialect='mysql').\
            prefix_with('C', 'D')

        self.assert_compile(stmt,
                            'DELETE C D FROM mytable')

        self.assert_compile(stmt,
                            'DELETE A B C D FROM mytable',
                            dialect=mysql.dialect())

    def test_alias(self):
        table1 = self.tables.mytable

        talias1 = table1.alias('t1')
        stmt = delete(talias1).where(talias1.c.myid == 7)

        self.assert_compile(
            stmt,
            'DELETE FROM mytable AS t1 WHERE t1.myid = :myid_1')

    def test_correlated(self):
        table1, table2 = self.tables.mytable, self.tables.myothertable

        # test a non-correlated WHERE clause
        s = select([table2.c.othername], table2.c.otherid == 7)
        self.assert_compile(delete(table1, table1.c.name == s),
                            'DELETE FROM mytable '
                            'WHERE mytable.name = ('
                            'SELECT myothertable.othername '
                            'FROM myothertable '
                            'WHERE myothertable.otherid = :otherid_1'
                            ')')

        # test one that is actually correlated...
        s = select([table2.c.othername], table2.c.otherid == table1.c.myid)
        self.assert_compile(table1.delete(table1.c.name == s),
                            'DELETE FROM mytable '
                            'WHERE mytable.name = ('
                            'SELECT myothertable.othername '
                            'FROM myothertable '
                            'WHERE myothertable.otherid = mytable.myid'
                            ')')
