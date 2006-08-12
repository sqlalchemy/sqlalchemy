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
            Column('MixedCase', Integer, quote=True),
            Column('ASC', Integer, quote=True),
            quote=True)
        table2 = Table('WorstCase2', metadata,
            Column('desc', Integer, quote=True, primary_key=True),
            Column('Union', Integer, quote=True),
            Column('MixedCase', Integer, quote=True),
            quote=True)
        table1.create()
        table2.create()
    
    def tearDown(self):
        table1.delete().execute()
        table2.delete().execute()
        
    def tearDownAll(self):
        table1.drop()
        table2.drop()
        
    def testbasic(self):
        table1.insert().execute({'lowercase':1,'UPPERCASE':2,'MixedCase':3,'ASC':4},
                {'lowercase':2,'UPPERCASE':2,'MixedCase':3,'ASC':4},
                {'lowercase':4,'UPPERCASE':3,'MixedCase':2,'ASC':1})
        table2.insert().execute({'desc':1,'Union':2,'MixedCase':3},
                {'desc':2,'Union':2,'MixedCase':3},
                {'desc':4,'Union':3,'MixedCase':2})
        
        res1 = select([table1.c.lowercase, table1.c.UPPERCASE, table1.c.MixedCase, table1.c.ASC]).execute().fetchall()
        print res1
        assert(res1==[(1,2,3,4),(2,2,3,4),(4,3,2,1)])
        
        res2 = select([table2.c.desc, table2.c.Union, table2.c.MixedCase]).execute().fetchall()
        print res2
        assert(res2==[(1,2,3),(2,2,3),(4,3,2)])
        
    def testreflect(self):
        meta2 = BoundMetaData(testbase.db)
        t2 = Table('WorstCase2', meta2, autoload=True, quote=True)
        assert t2.c.has_key('MixedCase')
    
    def testlabels(self):
        table1.insert().execute({'lowercase':1,'UPPERCASE':2,'MixedCase':3,'ASC':4},
                {'lowercase':2,'UPPERCASE':2,'MixedCase':3,'ASC':4},
                {'lowercase':4,'UPPERCASE':3,'MixedCase':2,'ASC':1})
        table2.insert().execute({'desc':1,'Union':2,'MixedCase':3},
                {'desc':2,'Union':2,'MixedCase':3},
                {'desc':4,'Union':3,'MixedCase':2})
        
        res1 = select([table1.c.lowercase, table1.c.UPPERCASE, table1.c.MixedCase, table1.c.ASC], use_labels=True).execute().fetchall()
        print res1
        assert(res1==[(1,2,3,4),(2,2,3,4),(4,3,2,1)])
        
        res2 = select([table2.c.desc, table2.c.Union, table2.c.MixedCase], use_labels=True).execute().fetchall()
        print res2
        assert(res2==[(1,2,3),(2,2,3),(4,3,2)])
        
if __name__ == "__main__":
    testbase.main()
