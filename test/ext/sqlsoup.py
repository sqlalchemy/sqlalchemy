import testbase

import sqlalchemy.ext.sqlsoup as sqlsoup

class SqlSoupTest(testbase.AssertMixin):
    def tearDown(self):
        pass
    def tearDownAll(self):
        pass
    def setUpAll(self):
        pass
    def setUp(self):
        pass
    def testall(self):
        import doctest
        doctest.testmod(m=sqlsoup,verbose=True)
        
if __name__ == "__main__":
    testbase.main()        
