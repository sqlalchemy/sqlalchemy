from testbase import PersistTest
import testbase
from sqlalchemy import *

class QuoteTest(PersistTest):
    def setUpAll(self):
        # TODO: figure out which databases/which identifiers allow special characters to be used,
        # such as:  spaces, quote characters, punctuation characters, set up tests for those as
        # well.
        global table1, table2, table3
        metadata = BoundMetaData(testbase.db)
        table1 = Table('WorstCase1', metadata,
            Column('lowercase', Integer, primary_key=True),
            Column('UPPERCASE', Integer),
            Column('MixedCase', Integer),
            Column('ASC', Integer, key='a123'))
        table2 = Table('WorstCase2', metadata,
            Column('desc', Integer, primary_key=True, key='d123'),
            Column('Union', Integer, key='u123'),
            Column('MixedCase', Integer))
        table1.create()
        table2.create()
    
    def tearDown(self):
        table1.delete().execute()
        table2.delete().execute()
        
    def tearDownAll(self):
        table1.drop()
        table2.drop()
        
    def testbasic(self):
        table1.insert().execute({'lowercase':1,'UPPERCASE':2,'MixedCase':3,'a123':4},
                {'lowercase':2,'UPPERCASE':2,'MixedCase':3,'a123':4},
                {'lowercase':4,'UPPERCASE':3,'MixedCase':2,'a123':1})
        table2.insert().execute({'d123':1,'u123':2,'MixedCase':3},
                {'d123':2,'u123':2,'MixedCase':3},
                {'d123':4,'u123':3,'MixedCase':2})
        
        res1 = select([table1.c.lowercase, table1.c.UPPERCASE, table1.c.MixedCase, table1.c.a123]).execute().fetchall()
        print res1
        assert(res1==[(1,2,3,4),(2,2,3,4),(4,3,2,1)])
        
        res2 = select([table2.c.d123, table2.c.u123, table2.c.MixedCase]).execute().fetchall()
        print res2
        assert(res2==[(1,2,3),(2,2,3),(4,3,2)])
        
    def testreflect(self):
        meta2 = BoundMetaData(testbase.db)
        t2 = Table('WorstCase2', meta2, autoload=True, quote=True)
        assert t2.c.has_key('MixedCase')
    
    def testlabels(self):
        table1.insert().execute({'lowercase':1,'UPPERCASE':2,'MixedCase':3,'a123':4},
                {'lowercase':2,'UPPERCASE':2,'MixedCase':3,'a123':4},
                {'lowercase':4,'UPPERCASE':3,'MixedCase':2,'a123':1})
        table2.insert().execute({'d123':1,'u123':2,'MixedCase':3},
                {'d123':2,'u123':2,'MixedCase':3},
                {'d123':4,'u123':3,'MixedCase':2})
        
        res1 = select([table1.c.lowercase, table1.c.UPPERCASE, table1.c.MixedCase, table1.c.a123], use_labels=True).execute().fetchall()
        print res1
        assert(res1==[(1,2,3,4),(2,2,3,4),(4,3,2,1)])
        
        res2 = select([table2.c.d123, table2.c.u123, table2.c.MixedCase], use_labels=True).execute().fetchall()
        print res2
        assert(res2==[(1,2,3),(2,2,3),(4,3,2)])
    
    def testcascade(self):
        lcmetadata = MetaData(case_sensitive=False)
        t1 = Table('SomeTable', lcmetadata, 
            Column('UcCol', Integer),
            Column('normalcol', String))
        t2 = Table('othertable', lcmetadata,
            Column('UcCol', Integer),
            Column('normalcol', String, ForeignKey('SomeTable.normalcol')))
        assert lcmetadata.case_sensitive is False
        assert t1.c.UcCol.case_sensitive is False
        assert t2.c.normalcol.case_sensitive is False
    
    def testlabels(self):
        """test the quoting of labels.
        
        if labels arent quoted, a query in postgres in particular will fail since it produces:
        
        SELECT LaLa.lowercase, LaLa."UPPERCASE", LaLa."MixedCase", LaLa."ASC" 
        FROM (SELECT DISTINCT "WorstCase1".lowercase AS lowercase, "WorstCase1"."UPPERCASE" AS UPPERCASE, "WorstCase1"."MixedCase" AS MixedCase, "WorstCase1"."ASC" AS ASC \nFROM "WorstCase1") AS LaLa
        
        where the "UPPERCASE" column of "LaLa" doesnt exist.
        """
        x = table1.select(distinct=True).alias("LaLa").select().scalar()

    def testlabels2(self):
        metadata = MetaData()
        table = Table("ImATable", metadata, 
            Column("col1", Integer))
        x = select([table.c.col1.label("ImATable_col1")]).alias("SomeAlias")
        assert str(select([x.c.ImATable_col1])) == '''SELECT "SomeAlias"."ImATable_col1" \nFROM (SELECT "ImATable".col1 AS "ImATable_col1" \nFROM "ImATable") AS "SomeAlias"'''

        # note that 'foo' and 'FooCol' are literals already quoted
        x = select([sql.literal_column("'foo'").label("somelabel")], from_obj=[table]).alias("AnAlias")
        x = x.select()
        assert str(x) == '''SELECT "AnAlias".somelabel \nFROM (SELECT 'foo' AS somelabel \nFROM "ImATable") AS "AnAlias"'''
        
        x = select([sql.literal_column("'FooCol'").label("SomeLabel")], from_obj=[table])
        x = x.select()
        assert str(x) == '''SELECT "SomeLabel" \nFROM (SELECT 'FooCol' AS "SomeLabel" \nFROM "ImATable")'''
        
    def testlabelsnocase(self):
        metadata = MetaData()
        table1 = Table('SomeCase1', metadata,
            Column('lowercase', Integer, primary_key=True),
            Column('UPPERCASE', Integer),
            Column('MixedCase', Integer))
        table2 = Table('SomeCase2', metadata,
            Column('id', Integer, primary_key=True, key='d123'),
            Column('col2', Integer, key='u123'),
            Column('MixedCase', Integer))
        
        # first test case sensitive tables migrating via tometadata
        meta = BoundMetaData(testbase.db, case_sensitive=False)
        lc_table1 = table1.tometadata(meta)
        lc_table2 = table2.tometadata(meta)
        assert lc_table1.case_sensitive is False
        assert lc_table1.c.UPPERCASE.case_sensitive is False
        s = lc_table1.select()
        assert hasattr(s.c.UPPERCASE, "case_sensitive")
        assert s.c.UPPERCASE.case_sensitive is False
        
        # now, the aliases etc. should be case-insensitive.  PG will screw up if this doesnt work.
        # also, if this test is run in the context of the other tests, we also test that the dialect properly
        # caches identifiers with "case_sensitive" and "not case_sensitive" separately.
        meta.create_all()
        try:
            x = lc_table1.select(distinct=True).alias("lala").select().scalar()
        finally:
            meta.drop_all()
        
if __name__ == "__main__":
    testbase.main()
