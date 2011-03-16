from sqlalchemy.util import jython, pypy, defaultdict, decorator
from sqlalchemy.util.compat import decimal

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
elif pypy:
    def gc_collect(*args):
        gc.collect()
        gc.collect()
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

def conforms_partial_ordering(tuples, sorted_elements):
    """True if the given sorting conforms to the given partial ordering."""

    deps = defaultdict(set)
    for parent, child in tuples:
        deps[parent].add(child)
    for i, node in enumerate(sorted_elements):
        for n in sorted_elements[i:]:
            if node in deps[n]:
                return False
    else:
        return True

def all_partial_orderings(tuples, elements):
    edges = defaultdict(set)
    for parent, child in tuples:
        edges[child].add(parent)

    def _all_orderings(elements):

        if len(elements) == 1:
            yield list(elements)
        else:
            for elem in elements:
                subset = set(elements).difference([elem])
                if not subset.intersection(edges[elem]):
                    for sub_ordering in _all_orderings(subset):
                        yield [elem] + sub_ordering

    return iter(_all_orderings(elements))


def function_named(fn, name):
    """Return a function with a given __name__.

    Will assign to __name__ and return the original function if possible on
    the Python implementation, otherwise a new function will be constructed.

    This function should be phased out as much as possible
    in favor of @decorator.   Tests that "generate" many named tests
    should be modernized.

    """
    try:
        fn.__name__ = name
    except TypeError:
        fn = types.FunctionType(fn.func_code, fn.func_globals, name,
                          fn.func_defaults, fn.func_closure)
    return fn

