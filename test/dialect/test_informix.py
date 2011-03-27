from sqlalchemy import *
from sqlalchemy.databases import informix
from test.lib import *


class CompileTest(fixtures.TestBase, AssertsCompiledSQL):

    __dialect__ = informix.InformixDialect()

    def test_statements(self):
        meta = MetaData()
        t1 = Table('t1', meta, Column('col1', Integer,
                   primary_key=True), Column('col2', String(50)))
        t2 = Table('t2', meta, Column('col1', Integer,
                   primary_key=True), Column('col2', String(50)),
                   Column('col3', Integer, ForeignKey('t1.col1')))
        self.assert_compile(t1.select(),
                            'SELECT t1.col1, t1.col2 FROM t1')
        self.assert_compile(select([t1, t2]).select_from(t1.join(t2)),
                            'SELECT t1.col1, t1.col2, t2.col1, '
                            't2.col2, t2.col3 FROM t1 JOIN t2 ON '
                            't1.col1 = t2.col3')
        self.assert_compile(t1.update().values({t1.c.col1: t1.c.col1
                            + 1}), 'UPDATE t1 SET col1=(t1.col1 + ?)')

