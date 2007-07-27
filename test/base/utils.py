import testbase
from sqlalchemy import util, column, sql, exceptions
from testlib import *


class OrderedDictTest(PersistTest):
    def test_odict(self):
        o = util.OrderedDict()
        o['a'] = 1
        o['b'] = 2
        o['snack'] = 'attack'
        o['c'] = 3

        self.assert_(o.keys() == ['a', 'b', 'snack', 'c'])
        self.assert_(o.values() == [1, 2, 'attack', 3])
    
        o.pop('snack')

        self.assert_(o.keys() == ['a', 'b', 'c'])
        self.assert_(o.values() == [1, 2, 3])

        o2 = util.OrderedDict(d=4)
        o2['e'] = 5

        self.assert_(o2.keys() == ['d', 'e'])
        self.assert_(o2.values() == [4, 5])

        o.update(o2)
        self.assert_(o.keys() == ['a', 'b', 'c', 'd', 'e'])
        self.assert_(o.values() == [1, 2, 3, 4, 5])

        o.setdefault('c', 'zzz')
        o.setdefault('f', 6)
        self.assert_(o.keys() == ['a', 'b', 'c', 'd', 'e', 'f'])
        self.assert_(o.values() == [1, 2, 3, 4, 5, 6])

class ColumnCollectionTest(PersistTest):
    def test_in(self):
        cc = sql.ColumnCollection()
        cc.add(column('col1'))
        cc.add(column('col2'))
        cc.add(column('col3'))
        assert 'col1' in cc
        assert 'col2' in cc

        try:
            cc['col1'] in cc
            assert False
        except exceptions.ArgumentError, e:
            assert str(e) == "__contains__ requires a string argument"
            
    def test_compare(self):
        cc1 = sql.ColumnCollection()
        cc2 = sql.ColumnCollection()
        cc3 = sql.ColumnCollection()
        c1 = column('col1')
        c2 = c1.label('col2')
        c3 = column('col3')
        cc1.add(c1)
        cc2.add(c2)
        cc3.add(c3)
        assert (cc1==cc2).compare(c1 == c2)
        assert not (cc1==cc3).compare(c2 == c3)
        
        
if __name__ == "__main__":
    testbase.main()
