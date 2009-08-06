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


