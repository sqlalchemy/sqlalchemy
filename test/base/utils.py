import testbase
from sqlalchemy import util

class OrderedDictTest(testbase.PersistTest):
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

if __name__ == "__main__":
    testbase.main()
