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
    def testdistance(self):
        s = select([table.c.col1.label('c2'), table.c.col1, table.c.col1.label('c1')])

        # didnt do this yet...col.label().make_proxy() has same "distance" as col.make_proxy() so far
        #assert s.corresponding_column(table.c.col1) is s.c.col1
        assert s.corresponding_column(s.c.col1) is s.c.col1
        assert s.corresponding_column(s.c.c1) is s.c.c1
        
    def testjoinagainstself(self):
        jj = select([table.c.col1.label('bar_col1')])
        jjj = join(table, jj, table.c.col1==jj.c.bar_col1)
        
        # test column directly agaisnt itself
        assert jjj.corresponding_column(jjj.c.table1_col1) is jjj.c.table1_col1

        assert jjj.corresponding_column(jj.c.bar_col1) is jjj.c.bar_col1
        
        # test alias of the join, targets the column with the least 
        # "distance" between the requested column and the returned column
        # (i.e. there is less indirection between j2.c.table1_col1 and table.c.col1, than
        # there is from j2.c.bar_col1 to table.c.col1)
        j2 = jjj.alias('foo')
        assert j2.corresponding_column(table.c.col1) is j2.c.table1_col1
        

    def testjoinagainstjoin(self):
        j  = outerjoin(table, table2, table.c.col1==table2.c.col2)
        jj = select([ table.c.col1.label('bar_col1')],from_obj=[j]).alias('foo')
        jjj = join(table, jj, table.c.col1==jj.c.bar_col1)
        assert jjj.corresponding_column(jjj.c.table1_col1) is jjj.c.table1_col1

        j2 = jjj.alias('foo')
        print j2.corresponding_column(jjj.c.table1_col1)
        assert j2.corresponding_column(jjj.c.table1_col1) is j2.c.table1_col1
        
        assert jjj.corresponding_column(jj.c.bar_col1) is jj.c.bar_col1
        
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
        print id(u.corresponding_column(s1.c.table1_col2).table)
        print id(u.c.col2.table)
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
        print criterion
        print j.onclause
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

class PrimaryKeyTest(testbase.AssertMixin):
    def test_join_pk_collapse_implicit(self):
        """test that redundant columns in a join get 'collapsed' into a minimal primary key, 
        which is the root column along a chain of foreign key relationships."""
        
        meta = MetaData()
        a = Table('a', meta, Column('id', Integer, primary_key=True))
        b = Table('b', meta, Column('id', Integer, ForeignKey('a.id'), primary_key=True))
        c = Table('c', meta, Column('id', Integer, ForeignKey('b.id'), primary_key=True))
        d = Table('d', meta, Column('id', Integer, ForeignKey('c.id'), primary_key=True))

        assert c.c.id.references(b.c.id)
        assert not d.c.id.references(a.c.id)
        
        assert list(a.join(b).primary_key) == [a.c.id]
        assert list(b.join(c).primary_key) == [b.c.id]
        assert list(a.join(b).join(c).primary_key) == [a.c.id]
        assert list(b.join(c).join(d).primary_key) == [b.c.id]
        assert list(d.join(c).join(b).primary_key) == [b.c.id]
        assert list(a.join(b).join(c).join(d).primary_key) == [a.c.id]

    def test_join_pk_collapse_explicit(self):
        """test that redundant columns in a join get 'collapsed' into a minimal primary key, 
        which is the root column along a chain of explicit join conditions."""

        meta = MetaData()
        a = Table('a', meta, Column('id', Integer, primary_key=True), Column('x', Integer))
        b = Table('b', meta, Column('id', Integer, ForeignKey('a.id'), primary_key=True), Column('x', Integer))
        c = Table('c', meta, Column('id', Integer, ForeignKey('b.id'), primary_key=True), Column('x', Integer))
        d = Table('d', meta, Column('id', Integer, ForeignKey('c.id'), primary_key=True), Column('x', Integer))

        print list(a.join(b, a.c.x==b.c.id).primary_key)
        assert list(a.join(b, a.c.x==b.c.id).primary_key) == [b.c.id]
        assert list(b.join(c, b.c.x==c.c.id).primary_key) == [b.c.id]
        assert list(a.join(b).join(c, c.c.id==b.c.x).primary_key) == [a.c.id]
        assert list(b.join(c, c.c.x==b.c.id).join(d).primary_key) == [c.c.id]
        assert list(b.join(c, c.c.id==b.c.x).join(d).primary_key) == [b.c.id]
        assert list(d.join(b, d.c.id==b.c.id).join(c, b.c.id==c.c.x).primary_key) == [c.c.id]
        assert list(a.join(b).join(c, c.c.id==b.c.x).join(d).primary_key) == [a.c.id]
        
        assert list(a.join(b, and_(a.c.id==b.c.id, a.c.x==b.c.id)).primary_key) == [a.c.id]
    
    def test_init_doesnt_blowitaway(self):
        meta = MetaData()
        a = Table('a', meta, Column('id', Integer, primary_key=True), Column('x', Integer))
        b = Table('b', meta, Column('id', Integer, ForeignKey('a.id'), primary_key=True), Column('x', Integer))

        j = a.join(b)
        assert list(j.primary_key) == [a.c.id]
        
        j.foreign_keys
        assert list(j.primary_key) == [a.c.id]

        
        
if __name__ == "__main__":
    testbase.main()
    