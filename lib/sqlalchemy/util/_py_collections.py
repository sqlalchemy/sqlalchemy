from itertools import filterfalse


class ImmutableContainer:
    def _immutable(self, *arg, **kw):
        raise TypeError("%s object is immutable" % self.__class__.__name__)

    __delitem__ = __setitem__ = __setattr__ = _immutable


class immutabledict(ImmutableContainer, dict):

    clear = pop = popitem = setdefault = update = ImmutableContainer._immutable

    def __new__(cls, *args):
        new = dict.__new__(cls)
        dict.__init__(new, *args)
        return new

    def __init__(self, *args):
        pass

    def __reduce__(self):
        return immutabledict, (dict(self),)

    def union(self, __d=None):
        if not __d:
            return self

        new = dict.__new__(self.__class__)
        dict.__init__(new, self)
        dict.update(new, __d)
        return new

    def _union_w_kw(self, __d=None, **kw):
        # not sure if C version works correctly w/ this yet
        if not __d and not kw:
            return self

        new = dict.__new__(self.__class__)
        dict.__init__(new, self)
        if __d:
            dict.update(new, __d)
        dict.update(new, kw)
        return new

    def merge_with(self, *dicts):
        new = None
        for d in dicts:
            if d:
                if new is None:
                    new = dict.__new__(self.__class__)
                    dict.__init__(new, self)
                dict.update(new, d)
        if new is None:
            return self

        return new

    def __repr__(self):
        return "immutabledict(%s)" % dict.__repr__(self)


class OrderedSet(set):
    def __init__(self, d=None):
        set.__init__(self)
        if d is not None:
            self._list = unique_list(d)
            set.update(self, self._list)
        else:
            self._list = []

    def add(self, element):
        if element not in self:
            self._list.append(element)
        set.add(self, element)

    def remove(self, element):
        set.remove(self, element)
        self._list.remove(element)

    def insert(self, pos, element):
        if element not in self:
            self._list.insert(pos, element)
        set.add(self, element)

    def discard(self, element):
        if element in self:
            self._list.remove(element)
            set.remove(self, element)

    def clear(self):
        set.clear(self)
        self._list = []

    def __getitem__(self, key):
        return self._list[key]

    def __iter__(self):
        return iter(self._list)

    def __add__(self, other):
        return self.union(other)

    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self._list)

    __str__ = __repr__

    def update(self, iterable):
        for e in iterable:
            if e not in self:
                self._list.append(e)
                set.add(self, e)
        return self

    __ior__ = update

    def union(self, other):
        result = self.__class__(self)
        result.update(other)
        return result

    __or__ = union

    def intersection(self, other):
        other = other if isinstance(other, set) else set(other)
        return self.__class__(a for a in self if a in other)

    __and__ = intersection

    def symmetric_difference(self, other):
        other_set = other if isinstance(other, set) else set(other)
        result = self.__class__(a for a in self if a not in other_set)
        result.update(a for a in other if a not in self)
        return result

    __xor__ = symmetric_difference

    def difference(self, other):
        other = other if isinstance(other, set) else set(other)
        return self.__class__(a for a in self if a not in other)

    __sub__ = difference

    def intersection_update(self, other):
        other = other if isinstance(other, set) else set(other)
        set.intersection_update(self, other)
        self._list = [a for a in self._list if a in other]
        return self

    __iand__ = intersection_update

    def symmetric_difference_update(self, other):
        set.symmetric_difference_update(self, other)
        self._list = [a for a in self._list if a in self]
        self._list += [a for a in other if a in self]
        return self

    __ixor__ = symmetric_difference_update

    def difference_update(self, other):
        set.difference_update(self, other)
        self._list = [a for a in self._list if a in self]
        return self

    __isub__ = difference_update


class IdentitySet:
    """A set that considers only object id() for uniqueness.

    This strategy has edge cases for builtin types- it's possible to have
    two 'foo' strings in one of these sets, for example.  Use sparingly.

    """

    def __init__(self, iterable=None):
        self._members = dict()
        if iterable:
            self.update(iterable)

    def add(self, value):
        self._members[id(value)] = value

    def __contains__(self, value):
        return id(value) in self._members

    def remove(self, value):
        del self._members[id(value)]

    def discard(self, value):
        try:
            self.remove(value)
        except KeyError:
            pass

    def pop(self):
        try:
            pair = self._members.popitem()
            return pair[1]
        except KeyError:
            raise KeyError("pop from an empty set")

    def clear(self):
        self._members.clear()

    def __cmp__(self, other):
        raise TypeError("cannot compare sets using cmp()")

    def __eq__(self, other):
        if isinstance(other, IdentitySet):
            return self._members == other._members
        else:
            return False

    def __ne__(self, other):
        if isinstance(other, IdentitySet):
            return self._members != other._members
        else:
            return True

    def issubset(self, iterable):
        if isinstance(iterable, self.__class__):
            other = iterable
        else:
            other = self.__class__(iterable)

        if len(self) > len(other):
            return False
        for m in filterfalse(
            other._members.__contains__, iter(self._members.keys())
        ):
            return False
        return True

    def __le__(self, other):
        if not isinstance(other, IdentitySet):
            return NotImplemented
        return self.issubset(other)

    def __lt__(self, other):
        if not isinstance(other, IdentitySet):
            return NotImplemented
        return len(self) < len(other) and self.issubset(other)

    def issuperset(self, iterable):
        if isinstance(iterable, self.__class__):
            other = iterable
        else:
            other = self.__class__(iterable)

        if len(self) < len(other):
            return False

        for m in filterfalse(
            self._members.__contains__, iter(other._members.keys())
        ):
            return False
        return True

    def __ge__(self, other):
        if not isinstance(other, IdentitySet):
            return NotImplemented
        return self.issuperset(other)

    def __gt__(self, other):
        if not isinstance(other, IdentitySet):
            return NotImplemented
        return len(self) > len(other) and self.issuperset(other)

    def union(self, iterable):
        result = self.__class__()
        members = self._members
        result._members.update(members)
        result._members.update((id(obj), obj) for obj in iterable)
        return result

    def __or__(self, other):
        if not isinstance(other, IdentitySet):
            return NotImplemented
        return self.union(other)

    def update(self, iterable):
        self._members.update((id(obj), obj) for obj in iterable)

    def __ior__(self, other):
        if not isinstance(other, IdentitySet):
            return NotImplemented
        self.update(other)
        return self

    def difference(self, iterable):
        result = self.__new__(self.__class__)
        if isinstance(iterable, self.__class__):
            other = iterable._members
        else:
            other = {id(obj) for obj in iterable}
        result._members = {
            k: v for k, v in self._members.items() if k not in other
        }
        return result

    def __sub__(self, other):
        if not isinstance(other, IdentitySet):
            return NotImplemented
        return self.difference(other)

    def difference_update(self, iterable):
        self._members = self.difference(iterable)._members

    def __isub__(self, other):
        if not isinstance(other, IdentitySet):
            return NotImplemented
        self.difference_update(other)
        return self

    def intersection(self, iterable):
        result = self.__new__(self.__class__)
        if isinstance(iterable, self.__class__):
            other = iterable._members
        else:
            other = {id(obj) for obj in iterable}
        result._members = {
            k: v for k, v in self._members.items() if k in other
        }
        return result

    def __and__(self, other):
        if not isinstance(other, IdentitySet):
            return NotImplemented
        return self.intersection(other)

    def intersection_update(self, iterable):
        self._members = self.intersection(iterable)._members

    def __iand__(self, other):
        if not isinstance(other, IdentitySet):
            return NotImplemented
        self.intersection_update(other)
        return self

    def symmetric_difference(self, iterable):
        result = self.__new__(self.__class__)
        if isinstance(iterable, self.__class__):
            other = iterable._members
        else:
            other = {id(obj): obj for obj in iterable}
        result._members = {
            k: v for k, v in self._members.items() if k not in other
        }
        result._members.update(
            (k, v) for k, v in other.items() if k not in self._members
        )
        return result

    def __xor__(self, other):
        if not isinstance(other, IdentitySet):
            return NotImplemented
        return self.symmetric_difference(other)

    def symmetric_difference_update(self, iterable):
        self._members = self.symmetric_difference(iterable)._members

    def __ixor__(self, other):
        if not isinstance(other, IdentitySet):
            return NotImplemented
        self.symmetric_difference(other)
        return self

    def copy(self):
        result = self.__new__(self.__class__)
        result._members = self._members.copy()
        return result

    __copy__ = copy

    def __len__(self):
        return len(self._members)

    def __iter__(self):
        return iter(self._members.values())

    def __hash__(self):
        raise TypeError("set objects are unhashable")

    def __repr__(self):
        return "%s(%r)" % (type(self).__name__, list(self._members.values()))


def unique_list(seq, hashfunc=None):
    seen = set()
    seen_add = seen.add
    if not hashfunc:
        return [x for x in seq if x not in seen and not seen_add(x)]
    else:
        return [
            x
            for x in seq
            if hashfunc(x) not in seen and not seen_add(hashfunc(x))
        ]
