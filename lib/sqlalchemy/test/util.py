from sqlalchemy.util import jython, function_named

import gc

if jython:
    def gc_collect(*args):
        gc.collect()
        time.sleep(0.1)
        gc.collect()
        gc.collect()
        return 0
else:
    gc_collect = gc.collect




