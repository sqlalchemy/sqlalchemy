"""Cross platform garbage collection utility"""
import gc
import sys
import time

if sys.platform.startswith('java'):
    def collect(*args):
        gc.collect()
        time.sleep(0.1)
        gc.collect()
        gc.collect()
        return 0
else:
    collect = gc.collect
