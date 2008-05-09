import new

__all__ = 'set', 'frozenset', 'sorted', '_function_named', 'deque', 'reversed'

try:
    set = set
except NameError:
    import sets

    # keep this in sync with sqlalchemy.util.Set
    # can't just import it in testlib because of coverage, load order, etc.
    class set(sets.Set):
        def _binary_sanity_check(self, other):
            pass

        def issubset(self, iterable):
            other = type(self)(iterable)
            return sets.Set.issubset(self, other)
        def __le__(self, other):
            sets.Set._binary_sanity_check(self, other)
            return sets.Set.__le__(self, other)
        def issuperset(self, iterable):
            other = type(self)(iterable)
            return sets.Set.issuperset(self, other)
        def __ge__(self, other):
            sets.Set._binary_sanity_check(self, other)
            return sets.Set.__ge__(self, other)

        # lt and gt still require a BaseSet
        def __lt__(self, other):
            sets.Set._binary_sanity_check(self, other)
            return sets.Set.__lt__(self, other)
        def __gt__(self, other):
            sets.Set._binary_sanity_check(self, other)
            return sets.Set.__gt__(self, other)

        def __ior__(self, other):
            if not isinstance(other, sets.BaseSet):
                return NotImplemented
            return sets.Set.__ior__(self, other)
        def __iand__(self, other):
            if not isinstance(other, sets.BaseSet):
                return NotImplemented
            return sets.Set.__iand__(self, other)
        def __ixor__(self, other):
            if not isinstance(other, sets.BaseSet):
                return NotImplemented
            return sets.Set.__ixor__(self, other)
        def __isub__(self, other):
            if not isinstance(other, sets.BaseSet):
                return NotImplemented
            return sets.Set.__isub__(self, other)

try:
    frozenset = frozenset
except NameError:
    import sets
    from sets import ImmutableSet as frozenset

try:
    sorted = sorted
except NameError:
    def sorted(iterable, cmp=None):
        l = list(iterable)
        if cmp:
            l.sort(cmp)
        else:
            l.sort()
        return l

try:
    reversed = reversed
except NameError:
    def reversed(seq):
        i = len(seq) - 1
        while  i >= 0:
            yield seq[i]
            i -= 1
        raise StopIteration()

try:
    from collections import deque
except ImportError:
    class deque(list):
        def appendleft(self, x):
            self.insert(0, x)
        def popleft(self):
            return self.pop(0)
        def extendleft(self, iterable):
            for x in reversed(list(iterable)):
                self.insert(0, x)

def _function_named(fn, newname):
    try:
        fn.__name__ = newname
    except:
        fn = new.function(fn.func_code, fn.func_globals, newname,
                          fn.func_defaults, fn.func_closure)
    return fn

