"""tests that various From objects properly export their columns, as well as useable primary keys
and foreign keys.  Full relational algebra depends on every selectable unit behaving
nicely with others.."""

import testbase
import unittest, sys, datetime


db = testbase.db

from sqlalchemy import *


table = Table('table1', db, 
    Column('col1', Integer, primary_key=True),
    Column('col2', String(20)),
    Column('col3', Integer),
    Column('colx', Integer),
    
)

table2 = Table('table2', db,
    Column('col1', Integer, primary_key=True),
    Column('col2', Integer, ForeignKey('table1.col1')),
    Column('col3', String(20)),
    Column('coly', Integer),
)

class SelectableTest(testbase.AssertMixin):
    def testtablealias(self):
        a = table.alias('a')
        
        j = join(a, table2)
        
        criterion = a.c.col1 == table2.c.col2
        print
        print str(j)
        self.assert_(criterion.compare(j.onclause))

    def testunion(self):
        # tests that we can correspond a column in a Select statement with a certain Table, against
        # a column in a Union where one of its underlying Selects matches to that same Table
        u = select([table.c.col1, table.c.col2, table.c.col3, table.c.colx, null().label('coly')]).union(
                select([table2.c.col1, table2.c.col2, table2.c.col3, null().label('colx'), table2.c.coly])
            )
        s1 = table.select(use_labels=True)
        s2 = table2.select(use_labels=True)
        print ["%d %s" % (id(c),c.key) for c in u.c]
        c = u.corresponding_column(s1.c.table1_col2)
        print "%d %s" % (id(c), c.key)
        assert u.corresponding_column(s1.c.table1_col2) is u.c.col2
        assert u.corresponding_column(s2.c.table2_col2) is u.c.col2

    def testaliasunion(self):
        # same as testunion, except its an alias of the union
        u = select([table.c.col1, table.c.col2, table.c.col3, table.c.colx, null().label('coly')]).union(
                select([table2.c.col1, table2.c.col2, table2.c.col3, null().label('colx'), table2.c.coly])
            ).alias('analias')
        s1 = table.select(use_labels=True)
        s2 = table2.select(use_labels=True)
        assert u.corresponding_column(s1.c.table1_col2) is u.c.col2
        assert u.corresponding_column(s2.c.table2_col2) is u.c.col2
        assert u.corresponding_column(s2.c.table2_coly) is u.c.coly
        assert s2.corresponding_column(u.c.coly) is s2.c.table2_coly

    def testselectunion(self):
        # like testaliasunion, but off a Select off the union.
        u = select([table.c.col1, table.c.col2, table.c.col3, table.c.colx, null().label('coly')]).union(
                select([table2.c.col1, table2.c.col2, table2.c.col3, null().label('colx'), table2.c.coly])
            ).alias('analias')
        s = select([u])
        s1 = table.select(use_labels=True)
        s2 = table2.select(use_labels=True)
        assert s.corresponding_column(s1.c.table1_col2) is s.c.col2
        assert s.corresponding_column(s2.c.table2_col2) is s.c.col2

    def testunionagainstjoin(self):
        # same as testunion, except its an alias of the union
        u = select([table.c.col1, table.c.col2, table.c.col3, table.c.colx, null().label('coly')]).union(
                select([table2.c.col1, table2.c.col2, table2.c.col3, null().label('colx'), table2.c.coly])
            ).alias('analias')
        j1 = table.join(table2)
        assert u.corresponding_column(j1.c.table1_colx) is u.c.colx
        assert j1.corresponding_column(u.c.colx) is j1.c.table1_colx
        
    def testjoin(self):
        a = join(table, table2)
        print str(a.select(use_labels=True))
        b = table2.alias('b')
        j = join(a, b)
        print str(j)
        criterion = a.c.table1_col1 == b.c.col2
        self.assert_(criterion.compare(j.onclause))

    def testselectalias(self):
        a = table.select().alias('a')
        print str(a.select())
        j = join(a, table2)
        
        criterion = a.c.col1 == table2.c.col2
        print
        print str(j)
        self.assert_(criterion.compare(j.onclause))

    def testselectlabels(self):
        a = table.select(use_labels=True)
        print str(a.select())
        j = join(a, table2)
        
        criterion = a.c.table1_col1 == table2.c.col2
        print
        print str(j)
        self.assert_(criterion.compare(j.onclause))

    def testcolumnlabels(self):
        a = select([table.c.col1.label('acol1'), table.c.col2.label('acol2'), table.c.col3.label('acol3')])
        print str(a)
        print [c for c in a.columns]
        print str(a.select())
        j = join(a, table2)
        criterion = a.c.acol1 == table2.c.col2
        print str(j)
        self.assert_(criterion.compare(j.onclause))
        
    def testselectaliaslabels(self):
        a = table2.select(use_labels=True).alias('a')
        print str(a.select())
        j = join(a, table)
        
        criterion =  table.c.col1 == a.c.table2_col2
        print str(criterion)
        print str(j.onclause)
        self.assert_(criterion.compare(j.onclause))
        
if __name__ == "__main__":
    testbase.main()
    