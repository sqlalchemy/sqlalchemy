from sqlalchemy.util import jython, function_named

import gc
import time

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
    