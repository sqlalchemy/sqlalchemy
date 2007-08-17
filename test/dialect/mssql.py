from testbase import AssertMixin
import testbase
from sqlalchemy import *
from sqlalchemy.databases import mssql
import datetime

db = testbase.db

class TestTypes(AssertMixin):

    @testbase.supported('mssql')
    def test_types(self):
        tbl = Table('test', testbase.metadata,
            Column('a', mssql.MSMoney),
            Column('b', mssql.MSSmallMoney),
            Column('c', mssql.MSBigInteger),
            Column('d', mssql.MSVariant),
            Column('e', mssql.MSUniqueIdentifier))
        tbl.create()
        
        try:
            m = BoundMetaData(db)
            Table('test', m, autoload=True)
            
        finally:
            tbl.drop()

if __name__ == "__main__":
    testbase.main()
