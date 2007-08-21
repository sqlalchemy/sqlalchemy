import testbase
from sqlalchemy import *
from sqlalchemy import exceptions
from testlib import *

class MetaDataTest(PersistTest):
    def test_metadata_connect(self):
        metadata = MetaData()
        t1 = Table('table1', metadata, Column('col1', Integer, primary_key=True),
            Column('col2', String(20)))
        metadata.bind = testbase.db
        metadata.create_all()
        try:
            assert t1.count().scalar() == 0
        finally:
            metadata.drop_all()


    def test_dupe_tables(self):
        metadata = MetaData()
        t1 = Table('table1', metadata, Column('col1', Integer, primary_key=True),
            Column('col2', String(20)))

        metadata.bind = testbase.db
        metadata.create_all()
        try:
            try:
                t1 = Table('table1', metadata, autoload=True)
                t2 = Table('table1', metadata, Column('col1', Integer, primary_key=True),
                    Column('col2', String(20)))
                assert False
            except exceptions.ArgumentError, e:
                assert str(e) == "Table 'table1' is already defined for this MetaData instance."
        finally:
            metadata.drop_all()
            
if __name__ == '__main__':
    testbase.main()
