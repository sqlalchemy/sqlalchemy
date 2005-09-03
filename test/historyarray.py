from testbase import PersistTest
import sqlalchemy.util as util
import unittest, sys, os

class HistoryArrayTest(PersistTest):
    def testadd(self):
        a = util.HistoryArraySet()
        a.append('hi')
        self.assert_(a == ['hi'])
        self.assert_(a.added_items() == ['hi'])
    
    def testremove(self):
        a = util.HistoryArraySet()
        a.append('hi')
        a.clear_history()
        self.assert_(a == ['hi'])
        self.assert_(a.added_items() == [])
        a.remove('hi')
        self.assert_(a == [])
        self.assert_(a.deleted_items() == ['hi'])
        
    def testremoveadded(self):
        a = util.HistoryArraySet()
        a.append('hi')
        a.remove('hi')
        self.assert_(a.added_items() == [])
        self.assert_(a.deleted_items() == [])
        self.assert_(a == [])

    def testaddedremoved(self):
        a = util.HistoryArraySet()
        a.append('hi')
        a.clear_history()
        a.remove('hi')
        self.assert_(a.deleted_items() == ['hi'])
        a.append('hi')
        self.assert_(a.added_items() == [])
        self.assert_(a.deleted_items() == [])
        self.assert_(a == ['hi'])
    
    def testarray(self):
        a = util.HistoryArraySet()
        a.append('hi')
        a.append('there')
        self.assert_(a[0] == 'hi' and a[1] == 'there')
        del a[1]
        self.assert_(a == ['hi'])
        a.append('hi')
        a.append('there')
        a[3:4] = ['yo', 'hi']
        self.assert_(a == ['hi', 'there', 'yo'])    
if __name__ == "__main__":
    unittest.main()