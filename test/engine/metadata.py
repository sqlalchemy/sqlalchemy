import testbase
from sqlalchemy import *

class MetaDataTest(testbase.PersistTest):
    def test_global_metadata(self):
         t1 = Table('table1', Column('col1', Integer, primary_key=True),
             Column('col2', String(20)))
         t2 = Table('table2', Column('col1', Integer, primary_key=True),
             Column('col2', String(20)))

         assert t1.c.col1
         global_connect(testbase.db)
         default_metadata.create_all()
         try:
             assert t1.count().scalar() == 0
         finally:
             default_metadata.drop_all()
             default_metadata.clear()

    def test_metadata_connect(self):
        metadata = MetaData()
        t1 = Table('table1', metadata, Column('col1', Integer, primary_key=True),
            Column('col2', String(20)))
        metadata.engine = testbase.db
        metadata.create_all()
        try:
            assert t1.count().scalar() == 0
        finally:
            metadata.drop_all()
    
if __name__ == '__main__':
    testbase.main()