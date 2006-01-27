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
    redefine=True
)

table2 = Table('table2', db,
    Column('col1', Integer, primary_key=True),
    Column('col2', Integer, ForeignKey('table1.col1')),
    Column('col3', String(20)),
    redefine=True
)

class SelectableTest(testbase.AssertMixin):
    def testtablealias(self):
        a = table.alias('a')
        
        j = join(a, table2)
        
        criterion = a.c.col1 == table2.c.col2
        print
        print str(j)
        self.assert_(criterion.compare(j.onclause))

    def testjoin(self):
        a = join(table, table2)
        print str(a.select(use_labels=True))
        # TODO - figure out what we're trying to do here
        return
        b = table2.alias('b')
        j = join(a, b)
        print str(j)
        return
        criterion = a.c.col1 == b.c.col2
        print
        print str(j)
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
    