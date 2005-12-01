from testbase import PersistTest
import testbase
import unittest, sys, datetime

import sqlalchemy.databases.sqlite as sqllite

from sqlalchemy import *

class ColumnsTest(PersistTest):

    defaultExpectedResults = { 'int_column': 'int_column INTEGER',
                               'varchar_column': 'varchar_column VARCHAR(20)',
                               'numeric_column': 'numeric_column NUMERIC(12, 3)',
                               'float_column': 'float_column NUMERIC(25, 2)'
                             }
    
    def setUp(self):
        pass

    def _buildTestTable(self):
        testTable = Table('testColumns', self.db,
            Column('int_column', INT),
            Column('varchar_column', VARCHAR(20)),
            Column('numeric_column', Numeric(12,3)),
            Column('float_column', FLOAT(25)),
        )
        return testTable

    def _doTest(self, expectedResults):
        testTable = self._buildTestTable()
        for aCol in testTable.c:
            self.assertEquals(expectedResults[aCol.name], self.db.schemagenerator(None).get_column_specification(aCol))
        
    def testSqliteColumns(self):
        self.db = create_engine('sqlite', {'filename':':memory:'})
        self._doTest(self.defaultExpectedResults)

    def testPostgresColumns(self):
        self.db = engine.create_engine('postgres', {'database':'test', 'host':'127.0.0.1', 'user':'scott', 'password':'tiger'}, echo=False)
        expectedResults = self.defaultExpectedResults.copy()
        expectedResults['float_column'] = 'float_column FLOAT(25)'
        self._doTest(expectedResults)

    def testMySqlColumns(self):
        self.db = engine.create_engine('mysql', {'db':'test', 'host':'127.0.0.1', 'user':'scott', 'passwd':'tiger'}, echo=False)
        expectedResults = self.defaultExpectedResults.copy()
        expectedResults['float_column'] = 'float_column FLOAT(25)'
        self._doTest(expectedResults)
    
if __name__ == "__main__":
    unittest.main()        
