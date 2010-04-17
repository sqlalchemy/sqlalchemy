from sqlalchemy.util import jython, function_named

import gc
import time
import random

if jython:
    def gc_collect(*args):
        """aggressive gc.collect for tests."""
        gc.collect()
        time.sleep(0.1)
        gc.collect()
        gc.collect()
        return 0
        
    # "lazy" gc, for VM's that don't GC on refcount == 0
    lazy_gc = gc_collect

else:
    # assume CPython - straight gc.collect, lazy_gc() is a pass
    gc_collect = gc.collect
    def lazy_gc():
        pass



def picklers():
    picklers = set()
    # Py2K
    try:
        import cPickle
        picklers.add(cPickle)
    except ImportError:
        pass
    # end Py2K
    import pickle
    picklers.add(pickle)
    
    # yes, this thing needs this much testing
    for pickle in picklers:
        for protocol in -1, 0, 1, 2:
            yield pickle.loads, lambda d:pickle.dumps(d, protocol)
    
    
def round_decimal(value, prec):
    if isinstance(value, float):
        return round(value, prec)
    
    import decimal

    # can also use shift() here but that is 2.6 only
    return (value * decimal.Decimal("1" + "0" * prec)).to_integral(decimal.ROUND_FLOOR) / \
                        pow(10, prec)
    
class RandomSet(set):
    def __iter__(self):
        l = list(set.__iter__(self))
        random.shuffle(l)
        return iter(l)
    
    def pop(self):
        index = random.randint(0, len(self) - 1)
        item = list(set.__iter__(self))[index]
        self.remove(item)
        return item
        
    def union(self, other):
        return RandomSet(set.union(self, other))
    
    def difference(self, other):
        return RandomSet(set.difference(self, other))
        
    def intersection(self, other):
        return RandomSet(set.intersection(self, other))
        
    def copy(self):
        return RandomSet(self)
        