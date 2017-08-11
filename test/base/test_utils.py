import copy
import sys

from sqlalchemy import util, sql, exc, testing
from sqlalchemy.testing import assert_raises, assert_raises_message, fixtures
from sqlalchemy.testing import eq_, is_, ne_, fails_if, mock, expect_warnings
from sqlalchemy.testing.util import picklers, gc_collect
from sqlalchemy.util import classproperty, WeakSequence, get_callable_argspec
from sqlalchemy.sql import column
from sqlalchemy.util import langhelpers, compat
import inspect


class _KeyedTupleTest(object):

    def _fixture(self, values, labels):
        raise NotImplementedError()

    def test_empty(self):
        keyed_tuple = self._fixture([], [])
        eq_(str(keyed_tuple), '()')
        eq_(len(keyed_tuple), 0)

        eq_(list(keyed_tuple.keys()), [])
        eq_(keyed_tuple._fields, ())
        eq_(keyed_tuple._asdict(), {})

    def test_values_but_no_labels(self):
        keyed_tuple = self._fixture([1, 2], [])
        eq_(str(keyed_tuple), '(1, 2)')
        eq_(len(keyed_tuple), 2)

        eq_(list(keyed_tuple.keys()), [])
        eq_(keyed_tuple._fields, ())
        eq_(keyed_tuple._asdict(), {})

        eq_(keyed_tuple[0], 1)
        eq_(keyed_tuple[1], 2)

    def test_basic_creation(self):
        keyed_tuple = self._fixture([1, 2], ['a', 'b'])
        eq_(str(keyed_tuple), '(1, 2)')
        eq_(list(keyed_tuple.keys()), ['a', 'b'])
        eq_(keyed_tuple._fields, ('a', 'b'))
        eq_(keyed_tuple._asdict(), {'a': 1, 'b': 2})

    def test_basic_index_access(self):
        keyed_tuple = self._fixture([1, 2], ['a', 'b'])
        eq_(keyed_tuple[0], 1)
        eq_(keyed_tuple[1], 2)

        def should_raise():
            keyed_tuple[2]
        assert_raises(IndexError, should_raise)

    def test_basic_attribute_access(self):
        keyed_tuple = self._fixture([1, 2], ['a', 'b'])
        eq_(keyed_tuple.a, 1)
        eq_(keyed_tuple.b, 2)

        def should_raise():
            keyed_tuple.c
        assert_raises(AttributeError, should_raise)

    def test_none_label(self):
        keyed_tuple = self._fixture([1, 2, 3], ['a', None, 'b'])
        eq_(str(keyed_tuple), '(1, 2, 3)')

        eq_(list(keyed_tuple.keys()), ['a', 'b'])
        eq_(keyed_tuple._fields, ('a', 'b'))
        eq_(keyed_tuple._asdict(), {'a': 1, 'b': 3})

        # attribute access: can't get at value 2
        eq_(keyed_tuple.a, 1)
        eq_(keyed_tuple.b, 3)

        # index access: can get at value 2
        eq_(keyed_tuple[0], 1)
        eq_(keyed_tuple[1], 2)
        eq_(keyed_tuple[2], 3)

    def test_duplicate_labels(self):
        keyed_tuple = self._fixture([1, 2, 3], ['a', 'b', 'b'])
        eq_(str(keyed_tuple), '(1, 2, 3)')

        eq_(list(keyed_tuple.keys()), ['a', 'b', 'b'])
        eq_(keyed_tuple._fields, ('a', 'b', 'b'))
        eq_(keyed_tuple._asdict(), {'a': 1, 'b': 3})

        # attribute access: can't get at value 2
        eq_(keyed_tuple.a, 1)
        eq_(keyed_tuple.b, 3)

        # index access: can get at value 2
        eq_(keyed_tuple[0], 1)
        eq_(keyed_tuple[1], 2)
        eq_(keyed_tuple[2], 3)

    def test_immutable(self):
        keyed_tuple = self._fixture([1, 2], ['a', 'b'])
        eq_(str(keyed_tuple), '(1, 2)')

        eq_(keyed_tuple.a, 1)

        assert_raises(AttributeError, setattr, keyed_tuple, "a", 5)

        def should_raise():
            keyed_tuple[0] = 100
        assert_raises(TypeError, should_raise)

    def test_serialize(self):

        keyed_tuple = self._fixture([1, 2, 3], ['a', None, 'b'])

        for loads, dumps in picklers():
            kt = loads(dumps(keyed_tuple))

            eq_(str(kt), '(1, 2, 3)')

            eq_(list(kt.keys()), ['a', 'b'])
            eq_(kt._fields, ('a', 'b'))
            eq_(kt._asdict(), {'a': 1, 'b': 3})


class KeyedTupleTest(_KeyedTupleTest, fixtures.TestBase):
    def _fixture(self, values, labels):
        return util.KeyedTuple(values, labels)


class LWKeyedTupleTest(_KeyedTupleTest, fixtures.TestBase):
    def _fixture(self, values, labels):
        return util.lightweight_named_tuple('n', labels)(values)


class WeakSequenceTest(fixtures.TestBase):
    @testing.requires.predictable_gc
    def test_cleanout_elements(self):
        class Foo(object):
            pass
        f1, f2, f3 = Foo(), Foo(), Foo()
        w = WeakSequence([f1, f2, f3])
        eq_(len(w), 3)
        eq_(len(w._storage), 3)
        del f2
        gc_collect()
        eq_(len(w), 2)
        eq_(len(w._storage), 2)

    @testing.requires.predictable_gc
    def test_cleanout_appended(self):
        class Foo(object):
            pass
        f1, f2, f3 = Foo(), Foo(), Foo()
        w = WeakSequence()
        w.append(f1)
        w.append(f2)
        w.append(f3)
        eq_(len(w), 3)
        eq_(len(w._storage), 3)
        del f2
        gc_collect()
        eq_(len(w), 2)
        eq_(len(w._storage), 2)


class OrderedDictTest(fixtures.TestBase):

    def test_odict(self):
        o = util.OrderedDict()
        o['a'] = 1
        o['b'] = 2
        o['snack'] = 'attack'
        o['c'] = 3

        eq_(list(o.keys()), ['a', 'b', 'snack', 'c'])
        eq_(list(o.values()), [1, 2, 'attack', 3])

        o.pop('snack')
        eq_(list(o.keys()), ['a', 'b', 'c'])
        eq_(list(o.values()), [1, 2, 3])

        try:
            o.pop('eep')
            assert False
        except KeyError:
            pass

        eq_(o.pop('eep', 'woot'), 'woot')

        try:
            o.pop('whiff', 'bang', 'pow')
            assert False
        except TypeError:
            pass

        eq_(list(o.keys()), ['a', 'b', 'c'])
        eq_(list(o.values()), [1, 2, 3])

        o2 = util.OrderedDict(d=4)
        o2['e'] = 5

        eq_(list(o2.keys()), ['d', 'e'])
        eq_(list(o2.values()), [4, 5])

        o.update(o2)
        eq_(list(o.keys()), ['a', 'b', 'c', 'd', 'e'])
        eq_(list(o.values()), [1, 2, 3, 4, 5])

        o.setdefault('c', 'zzz')
        o.setdefault('f', 6)
        eq_(list(o.keys()), ['a', 'b', 'c', 'd', 'e', 'f'])
        eq_(list(o.values()), [1, 2, 3, 4, 5, 6])

    def test_odict_constructor(self):
        o = util.OrderedDict([('name', 'jbe'),
                              ('fullname', 'jonathan'), ('password', '')])
        eq_(list(o.keys()), ['name', 'fullname', 'password'])

    def test_odict_copy(self):
        o = util.OrderedDict()
        o["zzz"] = 1
        o["aaa"] = 2
        eq_(list(o.keys()), ['zzz', 'aaa'])

        o2 = o.copy()
        eq_(list(o2.keys()), list(o.keys()))

        o3 = copy.copy(o)
        eq_(list(o3.keys()), list(o.keys()))


class OrderedSetTest(fixtures.TestBase):

    def test_mutators_against_iter(self):
        # testing a set modified against an iterator
        o = util.OrderedSet([3, 2, 4, 5])

        eq_(o.difference(iter([3, 4])), util.OrderedSet([2, 5]))
        eq_(o.intersection(iter([3, 4, 6])), util.OrderedSet([3, 4]))
        eq_(o.union(iter([3, 4, 6])), util.OrderedSet([2, 3, 4, 5, 6]))


class FrozenDictTest(fixtures.TestBase):

    def test_serialize(self):
        d = util.immutabledict({1: 2, 3: 4})
        for loads, dumps in picklers():
            print(loads(dumps(d)))


class MemoizedAttrTest(fixtures.TestBase):

    def test_memoized_property(self):
        val = [20]

        class Foo(object):
            @util.memoized_property
            def bar(self):
                v = val[0]
                val[0] += 1
                return v

        ne_(Foo.bar, None)
        f1 = Foo()
        assert 'bar' not in f1.__dict__
        eq_(f1.bar, 20)
        eq_(f1.bar, 20)
        eq_(val[0], 21)
        eq_(f1.__dict__['bar'], 20)

    def test_memoized_instancemethod(self):
        val = [20]

        class Foo(object):
            @util.memoized_instancemethod
            def bar(self):
                v = val[0]
                val[0] += 1
                return v

        assert inspect.ismethod(Foo().bar)
        ne_(Foo.bar, None)
        f1 = Foo()
        assert 'bar' not in f1.__dict__
        eq_(f1.bar(), 20)
        eq_(f1.bar(), 20)
        eq_(val[0], 21)

    def test_memoized_slots(self):
        canary = mock.Mock()

        class Foob(util.MemoizedSlots):
            __slots__ = ('foo_bar', 'gogo')

            def _memoized_method_gogo(self):
                canary.method()
                return "gogo"

            def _memoized_attr_foo_bar(self):
                canary.attr()
                return "foobar"

        f1 = Foob()
        assert_raises(AttributeError, setattr, f1, "bar", "bat")

        eq_(f1.foo_bar, "foobar")

        eq_(f1.foo_bar, "foobar")

        eq_(f1.gogo(), "gogo")

        eq_(f1.gogo(), "gogo")

        eq_(canary.mock_calls, [mock.call.attr(), mock.call.method()])


class WrapCallableTest(fixtures.TestBase):
    def test_wrapping_update_wrapper_fn(self):
        def my_fancy_default():
            """run the fancy default"""
            return 10

        c = util.wrap_callable(lambda: my_fancy_default, my_fancy_default)

        eq_(c.__name__, "my_fancy_default")
        eq_(c.__doc__, "run the fancy default")

    def test_wrapping_update_wrapper_fn_nodocstring(self):
        def my_fancy_default():
            return 10

        c = util.wrap_callable(lambda: my_fancy_default, my_fancy_default)
        eq_(c.__name__, "my_fancy_default")
        eq_(c.__doc__, None)

    def test_wrapping_update_wrapper_cls(self):
        class MyFancyDefault(object):
            """a fancy default"""

            def __call__(self):
                """run the fancy default"""
                return 10

        def_ = MyFancyDefault()
        c = util.wrap_callable(lambda: def_(), def_)

        eq_(c.__name__, "MyFancyDefault")
        eq_(c.__doc__, "run the fancy default")

    def test_wrapping_update_wrapper_cls_noclsdocstring(self):
        class MyFancyDefault(object):

            def __call__(self):
                """run the fancy default"""
                return 10

        def_ = MyFancyDefault()
        c = util.wrap_callable(lambda: def_(), def_)
        eq_(c.__name__, "MyFancyDefault")
        eq_(c.__doc__, "run the fancy default")

    def test_wrapping_update_wrapper_cls_nomethdocstring(self):
        class MyFancyDefault(object):
            """a fancy default"""

            def __call__(self):
                return 10

        def_ = MyFancyDefault()
        c = util.wrap_callable(lambda: def_(), def_)
        eq_(c.__name__, "MyFancyDefault")
        eq_(c.__doc__, "a fancy default")

    def test_wrapping_update_wrapper_cls_noclsdocstring_nomethdocstring(self):
        class MyFancyDefault(object):

            def __call__(self):
                return 10

        def_ = MyFancyDefault()
        c = util.wrap_callable(lambda: def_(), def_)
        eq_(c.__name__, "MyFancyDefault")
        eq_(c.__doc__, None)

    def test_wrapping_update_wrapper_functools_parial(self):
        def my_default(x):
            return x

        import functools
        my_functools_default = functools.partial(my_default, 5)

        c = util.wrap_callable(
            lambda: my_functools_default(), my_functools_default)
        eq_(c.__name__, "partial")
        eq_(c.__doc__, my_functools_default.__call__.__doc__)
        eq_(c(), 5)


class ToListTest(fixtures.TestBase):
    def test_from_string(self):
        eq_(
            util.to_list("xyz"),
            ["xyz"]
        )

    def test_from_set(self):
        spec = util.to_list(set([1, 2, 3]))
        assert isinstance(spec, list)
        eq_(
            sorted(spec),
            [1, 2, 3]
        )

    def test_from_dict(self):
        spec = util.to_list({1: "a", 2: "b", 3: "c"})
        assert isinstance(spec, list)
        eq_(
            sorted(spec),
            [1, 2, 3]
        )

    def test_from_tuple(self):
        eq_(
            util.to_list((1, 2, 3)),
            [1, 2, 3]
        )

    def test_from_bytes(self):

        eq_(
            util.to_list(compat.b('abc')),
            [compat.b('abc')]
        )

        eq_(
            util.to_list([
                compat.b('abc'), compat.b('def')]),
            [compat.b('abc'), compat.b('def')]
        )


class ColumnCollectionTest(testing.AssertsCompiledSQL, fixtures.TestBase):

    def test_in(self):
        cc = sql.ColumnCollection()
        cc.add(sql.column('col1'))
        cc.add(sql.column('col2'))
        cc.add(sql.column('col3'))
        assert 'col1' in cc
        assert 'col2' in cc

        try:
            cc['col1'] in cc
            assert False
        except exc.ArgumentError as e:
            eq_(str(e), "__contains__ requires a string argument")

    def test_compare(self):
        cc1 = sql.ColumnCollection()
        cc2 = sql.ColumnCollection()
        cc3 = sql.ColumnCollection()
        c1 = sql.column('col1')
        c2 = c1.label('col2')
        c3 = sql.column('col3')
        cc1.add(c1)
        cc2.add(c2)
        cc3.add(c3)
        assert (cc1 == cc2).compare(c1 == c2)
        assert not (cc1 == cc3).compare(c2 == c3)

    @testing.emits_warning("Column ")
    def test_dupes_add(self):
        cc = sql.ColumnCollection()

        c1, c2a, c3, c2b = (column('c1'),
                            column('c2'),
                            column('c3'),
                            column('c2'))

        cc.add(c1)
        cc.add(c2a)
        cc.add(c3)
        cc.add(c2b)

        eq_(cc._all_columns, [c1, c2a, c3, c2b])

        # for iter, c2a is replaced by c2b, ordering
        # is maintained in that way.  ideally, iter would be
        # the same as the "_all_columns" collection.
        eq_(list(cc), [c1, c2b, c3])

        assert cc.contains_column(c2a)
        assert cc.contains_column(c2b)

        ci = cc.as_immutable()
        eq_(ci._all_columns, [c1, c2a, c3, c2b])
        eq_(list(ci), [c1, c2b, c3])

    def test_identical_dupe_add(self):
        cc = sql.ColumnCollection()

        c1, c2, c3 = (column('c1'),
                      column('c2'),
                      column('c3'))

        cc.add(c1)
        cc.add(c2)
        cc.add(c3)
        cc.add(c2)

        eq_(cc._all_columns, [c1, c2, c3])

        self.assert_compile(
            cc == [c1, c2, c3],
            "c1 = c1 AND c2 = c2 AND c3 = c3"
        )

        # for iter, c2a is replaced by c2b, ordering
        # is maintained in that way.  ideally, iter would be
        # the same as the "_all_columns" collection.
        eq_(list(cc), [c1, c2, c3])

        assert cc.contains_column(c2)

        ci = cc.as_immutable()
        eq_(ci._all_columns, [c1, c2, c3])
        eq_(list(ci), [c1, c2, c3])

        self.assert_compile(
            ci == [c1, c2, c3],
            "c1 = c1 AND c2 = c2 AND c3 = c3"
        )

    def test_replace(self):
        cc = sql.ColumnCollection()

        c1, c2a, c3, c2b = (column('c1'),
                            column('c2'),
                            column('c3'),
                            column('c2'))

        cc.add(c1)
        cc.add(c2a)
        cc.add(c3)

        cc.replace(c2b)

        eq_(cc._all_columns, [c1, c2b, c3])
        eq_(list(cc), [c1, c2b, c3])

        assert not cc.contains_column(c2a)
        assert cc.contains_column(c2b)

        ci = cc.as_immutable()
        eq_(ci._all_columns, [c1, c2b, c3])
        eq_(list(ci), [c1, c2b, c3])

    def test_replace_key_matches(self):
        cc = sql.ColumnCollection()

        c1, c2a, c3, c2b = (column('c1'),
                            column('c2'),
                            column('c3'),
                            column('X'))
        c2b.key = 'c2'

        cc.add(c1)
        cc.add(c2a)
        cc.add(c3)

        cc.replace(c2b)

        assert not cc.contains_column(c2a)
        assert cc.contains_column(c2b)

        eq_(cc._all_columns, [c1, c2b, c3])
        eq_(list(cc), [c1, c2b, c3])

        ci = cc.as_immutable()
        eq_(ci._all_columns, [c1, c2b, c3])
        eq_(list(ci), [c1, c2b, c3])

    def test_replace_name_matches(self):
        cc = sql.ColumnCollection()

        c1, c2a, c3, c2b = (column('c1'),
                            column('c2'),
                            column('c3'),
                            column('c2'))
        c2b.key = 'X'

        cc.add(c1)
        cc.add(c2a)
        cc.add(c3)

        cc.replace(c2b)

        assert not cc.contains_column(c2a)
        assert cc.contains_column(c2b)

        eq_(cc._all_columns, [c1, c2b, c3])
        eq_(list(cc), [c1, c3, c2b])

        ci = cc.as_immutable()
        eq_(ci._all_columns, [c1, c2b, c3])
        eq_(list(ci), [c1, c3, c2b])

    def test_replace_no_match(self):
        cc = sql.ColumnCollection()

        c1, c2, c3, c4 = column('c1'), column('c2'), column('c3'), column('c4')
        c4.key = 'X'

        cc.add(c1)
        cc.add(c2)
        cc.add(c3)

        cc.replace(c4)

        assert cc.contains_column(c2)
        assert cc.contains_column(c4)

        eq_(cc._all_columns, [c1, c2, c3, c4])
        eq_(list(cc), [c1, c2, c3, c4])

        ci = cc.as_immutable()
        eq_(ci._all_columns, [c1, c2, c3, c4])
        eq_(list(ci), [c1, c2, c3, c4])

    def test_dupes_extend(self):
        cc = sql.ColumnCollection()

        c1, c2a, c3, c2b = (column('c1'),
                            column('c2'),
                            column('c3'),
                            column('c2'))

        cc.add(c1)
        cc.add(c2a)

        cc.extend([c3, c2b])

        eq_(cc._all_columns, [c1, c2a, c3, c2b])

        # for iter, c2a is replaced by c2b, ordering
        # is maintained in that way.  ideally, iter would be
        # the same as the "_all_columns" collection.
        eq_(list(cc), [c1, c2b, c3])

        assert cc.contains_column(c2a)
        assert cc.contains_column(c2b)

        ci = cc.as_immutable()
        eq_(ci._all_columns, [c1, c2a, c3, c2b])
        eq_(list(ci), [c1, c2b, c3])

    def test_dupes_update(self):
        cc = sql.ColumnCollection()

        c1, c2a, c3, c2b = (column('c1'),
                            column('c2'),
                            column('c3'),
                            column('c2'))

        cc.add(c1)
        cc.add(c2a)

        cc.update([(c3.key, c3), (c2b.key, c2b)])

        eq_(cc._all_columns, [c1, c2a, c3, c2b])

        assert cc.contains_column(c2a)
        assert cc.contains_column(c2b)

        # for iter, c2a is replaced by c2b, ordering
        # is maintained in that way.  ideally, iter would be
        # the same as the "_all_columns" collection.
        eq_(list(cc), [c1, c2b, c3])

    def test_extend_existing(self):
        cc = sql.ColumnCollection()

        c1, c2, c3, c4, c5 = (column('c1'),
                              column('c2'),
                              column('c3'),
                              column('c4'),
                              column('c5'))

        cc.extend([c1, c2])
        eq_(cc._all_columns, [c1, c2])

        cc.extend([c3])
        eq_(cc._all_columns, [c1, c2, c3])
        cc.extend([c4, c2, c5])

        eq_(cc._all_columns, [c1, c2, c3, c4, c5])

    def test_update_existing(self):
        cc = sql.ColumnCollection()

        c1, c2, c3, c4, c5 = (column('c1'),
                              column('c2'),
                              column('c3'),
                              column('c4'),
                              column('c5'))

        cc.update([('c1', c1), ('c2', c2)])
        eq_(cc._all_columns, [c1, c2])

        cc.update([('c3', c3)])
        eq_(cc._all_columns, [c1, c2, c3])
        cc.update([('c4', c4), ('c2', c2), ('c5', c5)])

        eq_(cc._all_columns, [c1, c2, c3, c4, c5])


class LRUTest(fixtures.TestBase):

    def test_lru(self):
        class item(object):
            def __init__(self, id):
                self.id = id

            def __str__(self):
                return "item id %d" % self.id

        lru = util.LRUCache(10, threshold=.2)

        for id in range(1, 20):
            lru[id] = item(id)

        # first couple of items should be gone
        assert 1 not in lru
        assert 2 not in lru

        # next batch over the threshold of 10 should be present
        for id_ in range(11, 20):
            assert id_ in lru

        lru[12]
        lru[15]
        lru[23] = item(23)
        lru[24] = item(24)
        lru[25] = item(25)
        lru[26] = item(26)
        lru[27] = item(27)

        assert 11 not in lru
        assert 13 not in lru

        for id_ in (25, 24, 23, 14, 12, 19, 18, 17, 16, 15):
            assert id_ in lru

        i1 = lru[25]
        i2 = item(25)
        lru[25] = i2
        assert 25 in lru
        assert lru[25] is i2


class ImmutableSubclass(str):
    pass


class FlattenIteratorTest(fixtures.TestBase):

    def test_flatten(self):
        assert list(util.flatten_iterator([[1, 2, 3], [4, 5, 6], 7,
                    8])) == [1, 2, 3, 4, 5, 6, 7, 8]

    def test_str_with_iter(self):
        """ensure that a str object with an __iter__ method (like in
        PyPy) is not interpreted as an iterable.

        """

        class IterString(str):
            def __iter__(self):
                return iter(self + '')

        iter_list = [IterString('asdf'), [IterString('x'), IterString('y')]]

        assert list(util.flatten_iterator(iter_list)) == ['asdf', 'x', 'y']


class HashOverride(object):

    def __init__(self, value=None):
        self.value = value

    def __hash__(self):
        return hash(self.value)


class EqOverride(object):

    def __init__(self, value=None):
        self.value = value
    __hash__ = object.__hash__

    def __eq__(self, other):
        if isinstance(other, EqOverride):
            return self.value == other.value
        else:
            return False

    def __ne__(self, other):
        if isinstance(other, EqOverride):
            return self.value != other.value
        else:
            return True


class HashEqOverride(object):

    def __init__(self, value=None):
        self.value = value

    def __hash__(self):
        return hash(self.value)

    def __eq__(self, other):
        if isinstance(other, EqOverride):
            return self.value == other.value
        else:
            return False

    def __ne__(self, other):
        if isinstance(other, EqOverride):
            return self.value != other.value
        else:
            return True


class IdentitySetTest(fixtures.TestBase):

    def assert_eq(self, identityset, expected_iterable):
        expected = sorted([id(o) for o in expected_iterable])
        found = sorted([id(o) for o in identityset])
        eq_(found, expected)

    def test_init(self):
        ids = util.IdentitySet([1, 2, 3, 2, 1])
        self.assert_eq(ids, [1, 2, 3])

        ids = util.IdentitySet(ids)
        self.assert_eq(ids, [1, 2, 3])

        ids = util.IdentitySet()
        self.assert_eq(ids, [])

        ids = util.IdentitySet([])
        self.assert_eq(ids, [])

        ids = util.IdentitySet(ids)
        self.assert_eq(ids, [])

    def test_add(self):
        for type_ in (object, ImmutableSubclass):
            data = [type_(), type_()]
            ids = util.IdentitySet()
            for i in list(range(2)) + list(range(2)):
                ids.add(data[i])
            self.assert_eq(ids, data)

        for type_ in (EqOverride, HashOverride, HashEqOverride):
            data = [type_(1), type_(1), type_(2)]
            ids = util.IdentitySet()
            for i in list(range(3)) + list(range(3)):
                ids.add(data[i])
            self.assert_eq(ids, data)

    def test_dunder_sub2(self):
        IdentitySet = util.IdentitySet
        o1, o2, o3 = object(), object(), object()
        ids1 = IdentitySet([o1])
        ids2 = IdentitySet([o1, o2, o3])
        eq_(
            ids2 - ids1,
            IdentitySet([o2, o3])
        )

        ids2 -= ids1
        eq_(ids2, IdentitySet([o2, o3]))

    def test_dunder_eq(self):
        _, _, twin1, twin2, unique1, unique2 = self._create_sets()

        # basic set math
        eq_(twin1 == twin2, True)
        eq_(unique1 == unique2, False)

        # not an IdentitySet
        not_an_identity_set = object()
        eq_(unique1 == not_an_identity_set, False)

    def test_dunder_ne(self):
        _, _, twin1, twin2, unique1, unique2 = self._create_sets()

        # basic set math
        eq_(twin1 != twin2, False)
        eq_(unique1 != unique2, True)

        # not an IdentitySet
        not_an_identity_set = object()
        eq_(unique1 != not_an_identity_set, True)

    def test_dunder_le(self):
        super_, sub_, twin1, twin2, unique1, unique2 = self._create_sets()

        # basic set math
        eq_(sub_ <= super_, True)
        eq_(super_ <= sub_, False)

        # the same sets
        eq_(twin1 <= twin2, True)
        eq_(twin2 <= twin1, True)

        # totally different sets
        eq_(unique1 <= unique2, False)
        eq_(unique2 <= unique1, False)

        # not an IdentitySet
        def should_raise():
            not_an_identity_set = object()
            return unique1 <= not_an_identity_set
        self._assert_unorderable_types(should_raise)

    def test_dunder_lt(self):
        super_, sub_, twin1, twin2, unique1, unique2 = self._create_sets()

        # basic set math
        eq_(sub_ < super_, True)
        eq_(super_ < sub_, False)

        # the same sets
        eq_(twin1 < twin2, False)
        eq_(twin2 < twin1, False)

        # totally different sets
        eq_(unique1 < unique2, False)
        eq_(unique2 < unique1, False)

        # not an IdentitySet
        def should_raise():
            not_an_identity_set = object()
            return unique1 < not_an_identity_set
        self._assert_unorderable_types(should_raise)

    def test_dunder_ge(self):
        super_, sub_, twin1, twin2, unique1, unique2 = self._create_sets()

        # basic set math
        eq_(sub_ >= super_, False)
        eq_(super_ >= sub_, True)

        # the same sets
        eq_(twin1 >= twin2, True)
        eq_(twin2 >= twin1, True)

        # totally different sets
        eq_(unique1 >= unique2, False)
        eq_(unique2 >= unique1, False)

        # not an IdentitySet
        def should_raise():
            not_an_identity_set = object()
            return unique1 >= not_an_identity_set
        self._assert_unorderable_types(should_raise)

    def test_dunder_gt(self):
        super_, sub_, twin1, twin2, unique1, unique2 = self._create_sets()

        # basic set math
        eq_(sub_ > super_, False)
        eq_(super_ > sub_, True)

        # the same sets
        eq_(twin1 > twin2, False)
        eq_(twin2 > twin1, False)

        # totally different sets
        eq_(unique1 > unique2, False)
        eq_(unique2 > unique1, False)

        # not an IdentitySet
        def should_raise():
            not_an_identity_set = object()
            return unique1 > not_an_identity_set
        self._assert_unorderable_types(should_raise)

    def test_issubset(self):
        super_, sub_, twin1, twin2, unique1, unique2 = self._create_sets()

        # basic set math
        eq_(sub_.issubset(super_), True)
        eq_(super_.issubset(sub_), False)

        # the same sets
        eq_(twin1.issubset(twin2), True)
        eq_(twin2.issubset(twin1), True)

        # totally different sets
        eq_(unique1.issubset(unique2), False)
        eq_(unique2.issubset(unique1), False)

        # not an IdentitySet
        not_an_identity_set = object()
        assert_raises(TypeError, unique1.issubset, not_an_identity_set)

    def test_issuperset(self):
        super_, sub_, twin1, twin2, unique1, unique2 = self._create_sets()

        # basic set math
        eq_(sub_.issuperset(super_), False)
        eq_(super_.issuperset(sub_), True)

        # the same sets
        eq_(twin1.issuperset(twin2), True)
        eq_(twin2.issuperset(twin1), True)

        # totally different sets
        eq_(unique1.issuperset(unique2), False)
        eq_(unique2.issuperset(unique1), False)

        # not an IdentitySet
        not_an_identity_set = object()
        assert_raises(TypeError, unique1.issuperset, not_an_identity_set)

    def test_union(self):
        super_, sub_, twin1, twin2, _, _ = self._create_sets()

        # basic set math
        eq_(sub_.union(super_), super_)
        eq_(super_.union(sub_), super_)

        # the same sets
        eq_(twin1.union(twin2), twin1)
        eq_(twin2.union(twin1), twin1)

        # empty sets
        empty = util.IdentitySet([])
        eq_(empty.union(empty), empty)

        # totally different sets
        unique1 = util.IdentitySet([1])
        unique2 = util.IdentitySet([2])
        eq_(unique1.union(unique2), util.IdentitySet([1, 2]))

        # not an IdentitySet
        not_an_identity_set = object()
        assert_raises(TypeError, unique1.union, not_an_identity_set)

    def test_dunder_or(self):
        super_, sub_, twin1, twin2, _, _ = self._create_sets()

        # basic set math
        eq_(sub_ | super_, super_)
        eq_(super_ | sub_, super_)

        # the same sets
        eq_(twin1 | twin2, twin1)
        eq_(twin2 | twin1, twin1)

        # empty sets
        empty = util.IdentitySet([])
        eq_(empty | empty, empty)

        # totally different sets
        unique1 = util.IdentitySet([1])
        unique2 = util.IdentitySet([2])
        eq_(unique1 | unique2, util.IdentitySet([1, 2]))

        # not an IdentitySet
        def should_raise():
            not_an_identity_set = object()
            return unique1 | not_an_identity_set
        assert_raises(TypeError, should_raise)

    def test_update(self):
        pass  # TODO

    def test_dunder_ior(self):
        super_, sub_, _, _, _, _ = self._create_sets()

        # basic set math
        sub_ |= super_
        eq_(sub_, super_)
        super_ |= sub_
        eq_(super_, super_)

        # totally different sets
        unique1 = util.IdentitySet([1])
        unique2 = util.IdentitySet([2])
        unique1 |= unique2
        eq_(unique1, util.IdentitySet([1, 2]))
        eq_(unique2, util.IdentitySet([2]))

        # not an IdentitySet
        def should_raise():
            unique = util.IdentitySet([1])
            not_an_identity_set = object()
            unique |= not_an_identity_set
        assert_raises(TypeError, should_raise)

    def test_difference(self):
        _, _, twin1, twin2, _, _ = self._create_sets()

        # basic set math
        set1 = util.IdentitySet([1, 2, 3])
        set2 = util.IdentitySet([2, 3, 4])
        eq_(set1.difference(set2), util.IdentitySet([1]))
        eq_(set2.difference(set1), util.IdentitySet([4]))

        # empty sets
        empty = util.IdentitySet([])
        eq_(empty.difference(empty), empty)

        # the same sets
        eq_(twin1.difference(twin2), empty)
        eq_(twin2.difference(twin1), empty)

        # totally different sets
        unique1 = util.IdentitySet([1])
        unique2 = util.IdentitySet([2])
        eq_(unique1.difference(unique2), util.IdentitySet([1]))
        eq_(unique2.difference(unique1), util.IdentitySet([2]))

        # not an IdentitySet
        not_an_identity_set = object()
        assert_raises(TypeError, unique1.difference, not_an_identity_set)

    def test_dunder_sub(self):
        _, _, twin1, twin2, _, _ = self._create_sets()

        # basic set math
        set1 = util.IdentitySet([1, 2, 3])
        set2 = util.IdentitySet([2, 3, 4])
        eq_(set1 - set2, util.IdentitySet([1]))
        eq_(set2 - set1, util.IdentitySet([4]))

        # empty sets
        empty = util.IdentitySet([])
        eq_(empty - empty, empty)

        # the same sets
        eq_(twin1 - twin2, empty)
        eq_(twin2 - twin1, empty)

        # totally different sets
        unique1 = util.IdentitySet([1])
        unique2 = util.IdentitySet([2])
        eq_(unique1 - unique2, util.IdentitySet([1]))
        eq_(unique2 - unique1, util.IdentitySet([2]))

        # not an IdentitySet
        def should_raise():
            not_an_identity_set = object()
            unique1 - not_an_identity_set
        assert_raises(TypeError, should_raise)

    def test_difference_update(self):
        pass  # TODO

    def test_dunder_isub(self):
        pass  # TODO

    def test_intersection(self):
        super_, sub_, twin1, twin2, unique1, unique2 = self._create_sets()

        # basic set math
        eq_(sub_.intersection(super_), sub_)
        eq_(super_.intersection(sub_), sub_)

        # the same sets
        eq_(twin1.intersection(twin2), twin1)
        eq_(twin2.intersection(twin1), twin1)

        # empty sets
        empty = util.IdentitySet([])
        eq_(empty.intersection(empty), empty)

        # totally different sets
        eq_(unique1.intersection(unique2), empty)

        # not an IdentitySet
        not_an_identity_set = object()
        assert_raises(TypeError, unique1.intersection, not_an_identity_set)

    def test_dunder_and(self):
        super_, sub_, twin1, twin2, unique1, unique2 = self._create_sets()

        # basic set math
        eq_(sub_ & super_, sub_)
        eq_(super_ & sub_, sub_)

        # the same sets
        eq_(twin1 & twin2, twin1)
        eq_(twin2 & twin1, twin1)

        # empty sets
        empty = util.IdentitySet([])
        eq_(empty & empty, empty)

        # totally different sets
        eq_(unique1 & unique2, empty)

        # not an IdentitySet
        def should_raise():
            not_an_identity_set = object()
            return unique1 & not_an_identity_set
        assert_raises(TypeError, should_raise)

    def test_intersection_update(self):
        pass  # TODO

    def test_dunder_iand(self):
        pass  # TODO

    def test_symmetric_difference(self):
        _, _, twin1, twin2, _, _ = self._create_sets()

        # basic set math
        set1 = util.IdentitySet([1, 2, 3])
        set2 = util.IdentitySet([2, 3, 4])
        eq_(set1.symmetric_difference(set2), util.IdentitySet([1, 4]))
        eq_(set2.symmetric_difference(set1), util.IdentitySet([1, 4]))

        # empty sets
        empty = util.IdentitySet([])
        eq_(empty.symmetric_difference(empty), empty)

        # the same sets
        eq_(twin1.symmetric_difference(twin2), empty)
        eq_(twin2.symmetric_difference(twin1), empty)

        # totally different sets
        unique1 = util.IdentitySet([1])
        unique2 = util.IdentitySet([2])
        eq_(unique1.symmetric_difference(unique2), util.IdentitySet([1, 2]))
        eq_(unique2.symmetric_difference(unique1), util.IdentitySet([1, 2]))

        # not an IdentitySet
        not_an_identity_set = object()
        assert_raises(
            TypeError, unique1.symmetric_difference, not_an_identity_set)

    def test_dunder_xor(self):
        _, _, twin1, twin2, _, _ = self._create_sets()

        # basic set math
        set1 = util.IdentitySet([1, 2, 3])
        set2 = util.IdentitySet([2, 3, 4])
        eq_(set1 ^ set2, util.IdentitySet([1, 4]))
        eq_(set2 ^ set1, util.IdentitySet([1, 4]))

        # empty sets
        empty = util.IdentitySet([])
        eq_(empty ^ empty, empty)

        # the same sets
        eq_(twin1 ^ twin2, empty)
        eq_(twin2 ^ twin1, empty)

        # totally different sets
        unique1 = util.IdentitySet([1])
        unique2 = util.IdentitySet([2])
        eq_(unique1 ^ unique2, util.IdentitySet([1, 2]))
        eq_(unique2 ^ unique1, util.IdentitySet([1, 2]))

        # not an IdentitySet
        def should_raise():
            not_an_identity_set = object()
            return unique1 ^ not_an_identity_set
        assert_raises(TypeError, should_raise)

    def test_symmetric_difference_update(self):
        pass  # TODO

    def _create_sets(self):
        o1, o2, o3, o4, o5 = object(), object(), object(), object(), object()
        super_ = util.IdentitySet([o1, o2, o3])
        sub_ = util.IdentitySet([o2])
        twin1 = util.IdentitySet([o3])
        twin2 = util.IdentitySet([o3])
        unique1 = util.IdentitySet([o4])
        unique2 = util.IdentitySet([o5])
        return super_, sub_, twin1, twin2, unique1, unique2

    def _assert_unorderable_types(self, callable_):
        if util.py36:
            assert_raises_message(
                TypeError, 'not supported between instances of', callable_)
        elif util.py3k:
            assert_raises_message(
                TypeError, 'unorderable types', callable_)
        else:
            assert_raises_message(
                TypeError, 'cannot compare sets using cmp()', callable_)

    def test_basic_sanity(self):
        IdentitySet = util.IdentitySet

        o1, o2, o3 = object(), object(), object()
        ids = IdentitySet([o1])
        ids.discard(o1)
        ids.discard(o1)
        ids.add(o1)
        ids.remove(o1)
        assert_raises(KeyError, ids.remove, o1)

        eq_(ids.copy(), ids)

        # explicit __eq__ and __ne__ tests
        assert ids != None  # noqa
        assert not(ids == None)  # noqa

        ne_(ids, IdentitySet([o1, o2, o3]))
        ids.clear()
        assert o1 not in ids
        ids.add(o2)
        assert o2 in ids
        eq_(ids.pop(), o2)
        ids.add(o1)
        eq_(len(ids), 1)

        isuper = IdentitySet([o1, o2])
        assert ids < isuper
        assert ids.issubset(isuper)
        assert isuper.issuperset(ids)
        assert isuper > ids

        eq_(ids.union(isuper), isuper)
        eq_(ids | isuper, isuper)
        eq_(isuper - ids, IdentitySet([o2]))
        eq_(isuper.difference(ids), IdentitySet([o2]))
        eq_(ids.intersection(isuper), IdentitySet([o1]))
        eq_(ids & isuper, IdentitySet([o1]))
        eq_(ids.symmetric_difference(isuper), IdentitySet([o2]))
        eq_(ids ^ isuper, IdentitySet([o2]))

        ids.update(isuper)
        ids |= isuper
        ids.difference_update(isuper)
        ids -= isuper
        ids.intersection_update(isuper)
        ids &= isuper
        ids.symmetric_difference_update(isuper)
        ids ^= isuper

        ids.update('foobar')
        try:
            ids |= 'foobar'
            assert False
        except TypeError:
            assert True

        try:
            s = set([o1, o2])
            s |= ids
            assert False
        except TypeError:
            assert True

        assert_raises(TypeError, util.cmp, ids)
        assert_raises(TypeError, hash, ids)


class OrderedIdentitySetTest(fixtures.TestBase):

    def assert_eq(self, identityset, expected_iterable):
        expected = [id(o) for o in expected_iterable]
        found = [id(o) for o in identityset]
        eq_(found, expected)

    def test_add(self):
        elem = object
        s = util.OrderedIdentitySet()
        s.add(elem())
        s.add(elem())

    def test_intersection(self):
        elem = object
        eq_ = self.assert_eq

        a, b, c, d, e, f, g = \
            elem(), elem(), elem(), elem(), elem(), elem(), elem()

        s1 = util.OrderedIdentitySet([a, b, c])
        s2 = util.OrderedIdentitySet([d, e, f])
        s3 = util.OrderedIdentitySet([a, d, f, g])
        eq_(s1.intersection(s2), [])
        eq_(s1.intersection(s3), [a])
        eq_(s1.union(s2).intersection(s3), [a, d, f])


class DictlikeIteritemsTest(fixtures.TestBase):
    baseline = set([('a', 1), ('b', 2), ('c', 3)])

    def _ok(self, instance):
        iterator = util.dictlike_iteritems(instance)
        eq_(set(iterator), self.baseline)

    def _notok(self, instance):
        assert_raises(TypeError,
                      util.dictlike_iteritems,
                      instance)

    def test_dict(self):
        d = dict(a=1, b=2, c=3)
        self._ok(d)

    def test_subdict(self):
        class subdict(dict):
            pass
        d = subdict(a=1, b=2, c=3)
        self._ok(d)

    if util.py2k:
        def test_UserDict(self):
            import UserDict
            d = UserDict.UserDict(a=1, b=2, c=3)
            self._ok(d)

    def test_object(self):
        self._notok(object())

    if util.py2k:
        def test_duck_1(self):
            class duck1(object):
                def iteritems(duck):
                    return iter(self.baseline)
            self._ok(duck1())

    def test_duck_2(self):
        class duck2(object):
            def items(duck):
                return list(self.baseline)
        self._ok(duck2())

    if util.py2k:
        def test_duck_3(self):
            class duck3(object):
                def iterkeys(duck):
                    return iter(['a', 'b', 'c'])

                def __getitem__(duck, key):
                    return dict(a=1, b=2, c=3).get(key)
            self._ok(duck3())

    def test_duck_4(self):
        class duck4(object):
            def iterkeys(duck):
                return iter(['a', 'b', 'c'])
        self._notok(duck4())

    def test_duck_5(self):
        class duck5(object):
            def keys(duck):
                return ['a', 'b', 'c']

            def get(duck, key):
                return dict(a=1, b=2, c=3).get(key)
        self._ok(duck5())

    def test_duck_6(self):
        class duck6(object):
            def keys(duck):
                return ['a', 'b', 'c']
        self._notok(duck6())


class DuckTypeCollectionTest(fixtures.TestBase):

    def test_sets(self):
        class SetLike(object):
            def add(self):
                pass

        class ForcedSet(list):
            __emulates__ = set

        for type_ in (set,
                      SetLike,
                      ForcedSet):
            eq_(util.duck_type_collection(type_), set)
            instance = type_()
            eq_(util.duck_type_collection(instance), set)

        for type_ in (frozenset, ):
            is_(util.duck_type_collection(type_), None)
            instance = type_()
            is_(util.duck_type_collection(instance), None)


class PublicFactoryTest(fixtures.TestBase):

    def _fixture(self):
        class Thingy(object):
            def __init__(self, value):
                "make a thingy"
                self.value = value

            @classmethod
            def foobar(cls, x, y):
                "do the foobar"
                return Thingy(x + y)

        return Thingy

    def test_classmethod(self):
        Thingy = self._fixture()
        foob = langhelpers.public_factory(
            Thingy.foobar, ".sql.elements.foob")
        eq_(foob(3, 4).value, 7)
        eq_(foob(x=3, y=4).value, 7)
        eq_(foob.__doc__, "do the foobar")
        eq_(foob.__module__, "sqlalchemy.sql.elements")
        assert Thingy.foobar.__doc__.startswith("This function is mirrored;")

    def test_constructor(self):
        Thingy = self._fixture()
        foob = langhelpers.public_factory(
            Thingy, ".sql.elements.foob")
        eq_(foob(7).value, 7)
        eq_(foob(value=7).value, 7)
        eq_(foob.__doc__, "make a thingy")
        eq_(foob.__module__, "sqlalchemy.sql.elements")
        assert Thingy.__init__.__doc__.startswith(
            "Construct a new :class:`.Thingy` object.")


class ArgInspectionTest(fixtures.TestBase):

    def test_get_cls_kwargs(self):

        class A(object):
            def __init__(self, a):
                pass

        class A1(A):
            def __init__(self, a1):
                pass

        class A11(A1):
            def __init__(self, a11, **kw):
                pass

        class B(object):
            def __init__(self, b, **kw):
                pass

        class B1(B):
            def __init__(self, b1, **kw):
                pass

        class B2(B):
            def __init__(self, b2):
                pass

        class AB(A, B):
            def __init__(self, ab):
                pass

        class BA(B, A):
            def __init__(self, ba, **kwargs):
                pass

        class BA1(BA):
            pass

        class CAB(A, B):
            pass

        class CBA(B, A):
            pass

        class CB1A1(B1, A1):
            pass

        class CAB1(A, B1):
            pass

        class CB1A(B1, A):
            pass

        class CB2A(B2, A):
            pass

        class D(object):
            pass

        class BA2(B, A):
            pass

        class A11B1(A11, B1):
            pass

        def test(cls, *expected):
            eq_(set(util.get_cls_kwargs(cls)), set(expected))

        test(A, 'a')
        test(A1, 'a1')
        test(A11, 'a11', 'a1')
        test(B, 'b')
        test(B1, 'b1', 'b')
        test(AB, 'ab')
        test(BA, 'ba', 'b', 'a')
        test(BA1, 'ba', 'b', 'a')
        test(CAB, 'a')
        test(CBA, 'b', 'a')
        test(CAB1, 'a')
        test(CB1A, 'b1', 'b', 'a')
        test(CB2A, 'b2')
        test(CB1A1, "a1", "b1", "b")
        test(D)
        test(BA2, "a", "b")
        test(A11B1, "a1", "a11", "b", "b1")

    def test_get_func_kwargs(self):

        def f1():
            pass

        def f2(foo):
            pass

        def f3(*foo):
            pass

        def f4(**foo):
            pass

        def test(fn, *expected):
            eq_(set(util.get_func_kwargs(fn)), set(expected))

        test(f1)
        test(f2, 'foo')
        test(f3)
        test(f4)

    def test_callable_argspec_fn(self):
        def foo(x, y, **kw):
            pass
        eq_(
            get_callable_argspec(foo),
            (['x', 'y'], None, 'kw', None)
        )

    def test_callable_argspec_fn_no_self(self):
        def foo(x, y, **kw):
            pass
        eq_(
            get_callable_argspec(foo, no_self=True),
            (['x', 'y'], None, 'kw', None)
        )

    def test_callable_argspec_fn_no_self_but_self(self):
        def foo(self, x, y, **kw):
            pass
        eq_(
            get_callable_argspec(foo, no_self=True),
            (['self', 'x', 'y'], None, 'kw', None)
        )

    @fails_if(lambda: util.pypy,  "pypy returns plain *arg, **kw")
    def test_callable_argspec_py_builtin(self):
        import datetime
        assert_raises(
            TypeError,
            get_callable_argspec, datetime.datetime.now
        )

    @fails_if(lambda: util.pypy,  "pypy returns plain *arg, **kw")
    def test_callable_argspec_obj_init(self):
        assert_raises(
            TypeError,
            get_callable_argspec, object
        )

    def test_callable_argspec_method(self):
        class Foo(object):
            def foo(self, x, y, **kw):
                pass
        eq_(
            get_callable_argspec(Foo.foo),
            (['self', 'x', 'y'], None, 'kw', None)
        )

    def test_callable_argspec_instance_method_no_self(self):
        class Foo(object):
            def foo(self, x, y, **kw):
                pass
        eq_(
            get_callable_argspec(Foo().foo, no_self=True),
            (['x', 'y'], None, 'kw', None)
        )

    def test_callable_argspec_unbound_method_no_self(self):
        class Foo(object):
            def foo(self, x, y, **kw):
                pass
        eq_(
            get_callable_argspec(Foo.foo, no_self=True),
            (['self', 'x', 'y'], None, 'kw', None)
        )

    def test_callable_argspec_init(self):
        class Foo(object):
            def __init__(self, x, y):
                pass

        eq_(
            get_callable_argspec(Foo),
            (['self', 'x', 'y'], None, None, None)
        )

    def test_callable_argspec_init_no_self(self):
        class Foo(object):
            def __init__(self, x, y):
                pass

        eq_(
            get_callable_argspec(Foo, no_self=True),
            (['x', 'y'], None, None, None)
        )

    def test_callable_argspec_call(self):
        class Foo(object):
            def __call__(self, x, y):
                pass
        eq_(
            get_callable_argspec(Foo()),
            (['self', 'x', 'y'], None, None, None)
        )

    def test_callable_argspec_call_no_self(self):
        class Foo(object):
            def __call__(self, x, y):
                pass
        eq_(
            get_callable_argspec(Foo(), no_self=True),
            (['x', 'y'], None, None, None)
        )

    @fails_if(lambda: util.pypy,  "pypy returns plain *arg, **kw")
    def test_callable_argspec_partial(self):
        from functools import partial

        def foo(x, y, z, **kw):
            pass
        bar = partial(foo, 5)

        assert_raises(
            TypeError,
            get_callable_argspec, bar
        )


class SymbolTest(fixtures.TestBase):

    def test_basic(self):
        sym1 = util.symbol('foo')
        assert sym1.name == 'foo'
        sym2 = util.symbol('foo')

        assert sym1 is sym2
        assert sym1 == sym2

        sym3 = util.symbol('bar')
        assert sym1 is not sym3
        assert sym1 != sym3

    def test_pickle(self):
        sym1 = util.symbol('foo')
        sym2 = util.symbol('foo')

        assert sym1 is sym2

        # default
        s = util.pickle.dumps(sym1)
        sym3 = util.pickle.loads(s)

        for protocol in 0, 1, 2:
            print(protocol)
            serial = util.pickle.dumps(sym1)
            rt = util.pickle.loads(serial)
            assert rt is sym1
            assert rt is sym2

    def test_bitflags(self):
        sym1 = util.symbol('sym1', canonical=1)
        sym2 = util.symbol('sym2', canonical=2)

        assert sym1 & sym1
        assert not sym1 & sym2
        assert not sym1 & sym1 & sym2

    def test_composites(self):
        sym1 = util.symbol('sym1', canonical=1)
        sym2 = util.symbol('sym2', canonical=2)
        sym3 = util.symbol('sym3', canonical=4)
        sym4 = util.symbol('sym4', canonical=8)

        assert sym1 & (sym2 | sym1 | sym4)
        assert not sym1 & (sym2 | sym3)

        assert not (sym1 | sym2) & (sym3 | sym4)
        assert (sym1 | sym2) & (sym2 | sym4)


class TestFormatArgspec(fixtures.TestBase):

    def test_specs(self):
        def test(fn, wanted, grouped=None):
            if grouped is None:
                parsed = util.format_argspec_plus(fn)
            else:
                parsed = util.format_argspec_plus(fn, grouped=grouped)
            eq_(parsed, wanted)

        test(lambda: None,
             {'args': '()', 'self_arg': None,
              'apply_kw': '()', 'apply_pos': '()'})

        test(lambda: None,
             {'args': '', 'self_arg': None,
              'apply_kw': '', 'apply_pos': ''},
             grouped=False)

        test(lambda self: None,
             {'args': '(self)', 'self_arg': 'self',
              'apply_kw': '(self)', 'apply_pos': '(self)'})

        test(lambda self: None,
             {'args': 'self', 'self_arg': 'self',
              'apply_kw': 'self', 'apply_pos': 'self'},
             grouped=False)

        test(lambda *a: None,
             {'args': '(*a)', 'self_arg': 'a[0]',
              'apply_kw': '(*a)', 'apply_pos': '(*a)'})

        test(lambda **kw: None,
             {'args': '(**kw)', 'self_arg': None,
              'apply_kw': '(**kw)', 'apply_pos': '(**kw)'})

        test(lambda *a, **kw: None,
             {'args': '(*a, **kw)', 'self_arg': 'a[0]',
              'apply_kw': '(*a, **kw)', 'apply_pos': '(*a, **kw)'})

        test(lambda a, *b: None,
             {'args': '(a, *b)', 'self_arg': 'a',
              'apply_kw': '(a, *b)', 'apply_pos': '(a, *b)'})

        test(lambda a, **b: None,
             {'args': '(a, **b)', 'self_arg': 'a',
              'apply_kw': '(a, **b)', 'apply_pos': '(a, **b)'})

        test(lambda a, *b, **c: None,
             {'args': '(a, *b, **c)', 'self_arg': 'a',
              'apply_kw': '(a, *b, **c)', 'apply_pos': '(a, *b, **c)'})

        test(lambda a, b=1, **c: None,
             {'args': '(a, b=1, **c)', 'self_arg': 'a',
              'apply_kw': '(a, b=b, **c)', 'apply_pos': '(a, b, **c)'})

        test(lambda a=1, b=2: None,
             {'args': '(a=1, b=2)', 'self_arg': 'a',
              'apply_kw': '(a=a, b=b)', 'apply_pos': '(a, b)'})

        test(lambda a=1, b=2: None,
             {'args': 'a=1, b=2', 'self_arg': 'a',
              'apply_kw': 'a=a, b=b', 'apply_pos': 'a, b'},
             grouped=False)

    @testing.fails_if(lambda: util.pypy,
                      "pypy doesn't report Obj.__init__ as object.__init__")
    def test_init_grouped(self):
        object_spec = {
            'args': '(self)', 'self_arg': 'self',
            'apply_pos': '(self)', 'apply_kw': '(self)'}
        wrapper_spec = {
            'args': '(self, *args, **kwargs)', 'self_arg': 'self',
            'apply_pos': '(self, *args, **kwargs)',
            'apply_kw': '(self, *args, **kwargs)'}
        custom_spec = {
            'args': '(slef, a=123)', 'self_arg': 'slef',  # yes, slef
            'apply_pos': '(slef, a)', 'apply_kw': '(slef, a=a)'}

        self._test_init(None, object_spec, wrapper_spec, custom_spec)
        self._test_init(True, object_spec, wrapper_spec, custom_spec)

    @testing.fails_if(lambda: util.pypy,
                      "pypy doesn't report Obj.__init__ as object.__init__")
    def test_init_bare(self):
        object_spec = {
            'args': 'self', 'self_arg': 'self',
            'apply_pos': 'self', 'apply_kw': 'self'}
        wrapper_spec = {
            'args': 'self, *args, **kwargs', 'self_arg': 'self',
            'apply_pos': 'self, *args, **kwargs',
            'apply_kw': 'self, *args, **kwargs'}
        custom_spec = {
            'args': 'slef, a=123', 'self_arg': 'slef',  # yes, slef
            'apply_pos': 'slef, a', 'apply_kw': 'slef, a=a'}

        self._test_init(False, object_spec, wrapper_spec, custom_spec)

    def _test_init(self, grouped, object_spec, wrapper_spec, custom_spec):
        def test(fn, wanted):
            if grouped is None:
                parsed = util.format_argspec_init(fn)
            else:
                parsed = util.format_argspec_init(fn, grouped=grouped)
            eq_(parsed, wanted)

        class Obj(object):
            pass

        test(Obj.__init__, object_spec)

        class Obj(object):
            def __init__(self):
                pass

        test(Obj.__init__, object_spec)

        class Obj(object):
            def __init__(slef, a=123):
                pass

        test(Obj.__init__, custom_spec)

        class Obj(list):
            pass

        test(Obj.__init__, wrapper_spec)

        class Obj(list):
            def __init__(self, *args, **kwargs):
                pass

        test(Obj.__init__, wrapper_spec)

        class Obj(list):
            def __init__(self):
                pass

        test(Obj.__init__, object_spec)

        class Obj(list):
            def __init__(slef, a=123):
                pass

        test(Obj.__init__, custom_spec)


class GenericReprTest(fixtures.TestBase):

    def test_all_positional(self):
        class Foo(object):
            def __init__(self, a, b, c):
                self.a = a
                self.b = b
                self.c = c
        eq_(
            util.generic_repr(Foo(1, 2, 3)),
            "Foo(1, 2, 3)"
        )

    def test_positional_plus_kw(self):
        class Foo(object):
            def __init__(self, a, b, c=5, d=4):
                self.a = a
                self.b = b
                self.c = c
                self.d = d
        eq_(
            util.generic_repr(Foo(1, 2, 3, 6)),
            "Foo(1, 2, c=3, d=6)"
        )

    def test_kw_defaults(self):
        class Foo(object):
            def __init__(self, a=1, b=2, c=3, d=4):
                self.a = a
                self.b = b
                self.c = c
                self.d = d
        eq_(
            util.generic_repr(Foo(1, 5, 3, 7)),
            "Foo(b=5, d=7)"
        )

    def test_multi_kw(self):
        class Foo(object):
            def __init__(self, a, b, c=3, d=4):
                self.a = a
                self.b = b
                self.c = c
                self.d = d

        class Bar(Foo):
            def __init__(self, e, f, g=5, **kw):
                self.e = e
                self.f = f
                self.g = g
                super(Bar, self).__init__(**kw)

        eq_(
            util.generic_repr(
                Bar('e', 'f', g=7, a=6, b=5, d=9),
                to_inspect=[Bar, Foo]
            ),
            "Bar('e', 'f', g=7, a=6, b=5, d=9)"
        )

        eq_(
            util.generic_repr(
                Bar('e', 'f', a=6, b=5),
                to_inspect=[Bar, Foo]
            ),
            "Bar('e', 'f', a=6, b=5)"
        )

    def test_multi_kw_repeated(self):
        class Foo(object):
            def __init__(self, a=1, b=2):
                self.a = a
                self.b = b

        class Bar(Foo):
            def __init__(self, b=3, c=4, **kw):
                self.c = c
                super(Bar, self).__init__(b=b, **kw)

        eq_(
            util.generic_repr(
                Bar(a='a', b='b', c='c'),
                to_inspect=[Bar, Foo]
            ),
            "Bar(b='b', c='c', a='a')"
        )

    def test_discard_vargs(self):
        class Foo(object):
            def __init__(self, a, b, *args):
                self.a = a
                self.b = b
                self.c, self.d = args[0:2]
        eq_(
            util.generic_repr(Foo(1, 2, 3, 4)),
            "Foo(1, 2)"
        )

    def test_discard_vargs_kwargs(self):
        class Foo(object):
            def __init__(self, a, b, *args, **kw):
                self.a = a
                self.b = b
                self.c, self.d = args[0:2]
        eq_(
            util.generic_repr(Foo(1, 2, 3, 4, x=7, y=4)),
            "Foo(1, 2)"
        )

    def test_significant_vargs(self):
        class Foo(object):
            def __init__(self, a, b, *args):
                self.a = a
                self.b = b
                self.args = args
        eq_(
            util.generic_repr(Foo(1, 2, 3, 4)),
            "Foo(1, 2, 3, 4)"
        )

    def test_no_args(self):
        class Foo(object):
            def __init__(self):
                pass
        eq_(
            util.generic_repr(Foo()),
            "Foo()"
        )

    def test_no_init(self):
        class Foo(object):
            pass
        eq_(
            util.generic_repr(Foo()),
            "Foo()"
        )


class AsInterfaceTest(fixtures.TestBase):

    class Something(object):

        def _ignoreme(self):
            pass

        def foo(self):
            pass

        def bar(self):
            pass

    class Partial(object):

        def bar(self):
            pass

    class Object(object):
        pass

    def test_instance(self):
        obj = object()
        assert_raises(TypeError, util.as_interface, obj,
                      cls=self.Something)

        assert_raises(TypeError, util.as_interface, obj,
                      methods=('foo'))

        assert_raises(TypeError, util.as_interface, obj,
                      cls=self.Something, required=('foo'))

        obj = self.Something()
        eq_(obj, util.as_interface(obj, cls=self.Something))
        eq_(obj, util.as_interface(obj, methods=('foo',)))
        eq_(
            obj, util.as_interface(obj, cls=self.Something,
                                   required=('outofband',)))
        partial = self.Partial()

        slotted = self.Object()
        slotted.bar = lambda self: 123

        for obj in partial, slotted:
            eq_(obj, util.as_interface(obj, cls=self.Something))
            assert_raises(TypeError, util.as_interface, obj,
                          methods=('foo'))
            eq_(obj, util.as_interface(obj, methods=('bar',)))
            eq_(obj, util.as_interface(obj, cls=self.Something,
                                       required=('bar',)))
            assert_raises(TypeError, util.as_interface, obj,
                          cls=self.Something, required=('foo',))

            assert_raises(TypeError, util.as_interface, obj,
                          cls=self.Something, required=self.Something)

    def test_dict(self):
        obj = {}
        assert_raises(TypeError, util.as_interface, obj,
                      cls=self.Something)
        assert_raises(TypeError, util.as_interface, obj, methods='foo')
        assert_raises(TypeError, util.as_interface, obj,
                      cls=self.Something, required='foo')

        def assertAdapted(obj, *methods):
            assert isinstance(obj, type)
            found = set([m for m in dir(obj) if not m.startswith('_')])
            for method in methods:
                assert method in found
                found.remove(method)
            assert not found

        def fn(self): return 123
        obj = {'foo': fn, 'bar': fn}
        res = util.as_interface(obj, cls=self.Something)
        assertAdapted(res, 'foo', 'bar')
        res = util.as_interface(obj, cls=self.Something,
                                required=self.Something)
        assertAdapted(res, 'foo', 'bar')
        res = util.as_interface(obj, cls=self.Something, required=('foo',))
        assertAdapted(res, 'foo', 'bar')
        res = util.as_interface(obj, methods=('foo', 'bar'))
        assertAdapted(res, 'foo', 'bar')
        res = util.as_interface(obj, methods=('foo', 'bar', 'baz'))
        assertAdapted(res, 'foo', 'bar')
        res = util.as_interface(obj, methods=('foo', 'bar'), required=('foo',))
        assertAdapted(res, 'foo', 'bar')
        assert_raises(TypeError, util.as_interface, obj, methods=('foo',))
        assert_raises(TypeError, util.as_interface, obj,
                      methods=('foo', 'bar', 'baz'), required=('baz', ))
        obj = {'foo': 123}
        assert_raises(TypeError, util.as_interface, obj, cls=self.Something)


class TestClassHierarchy(fixtures.TestBase):

    def test_object(self):
        eq_(set(util.class_hierarchy(object)), set((object,)))

    def test_single(self):
        class A(object):
            pass

        class B(object):
            pass

        eq_(set(util.class_hierarchy(A)), set((A, object)))
        eq_(set(util.class_hierarchy(B)), set((B, object)))

        class C(A, B):
            pass

        eq_(set(util.class_hierarchy(A)), set((A, B, C, object)))
        eq_(set(util.class_hierarchy(B)), set((A, B, C, object)))

    if util.py2k:
        def test_oldstyle_mixin(self):
            class A(object):
                pass

            class Mixin:
                pass

            class B(A, Mixin):
                pass

            eq_(set(util.class_hierarchy(B)), set((A, B, object)))
            eq_(set(util.class_hierarchy(Mixin)), set())
            eq_(set(util.class_hierarchy(A)), set((A, B, object)))


class ReraiseTest(fixtures.TestBase):
    @testing.requires.python3
    def test_raise_from_cause_same_cause(self):
        class MyException(Exception):
            pass

        def go():
            try:
                raise MyException("exc one")
            except Exception as err:
                util.raise_from_cause(err)

        try:
            go()
            assert False
        except MyException as err:
            is_(err.__cause__, None)

    def test_reraise_disallow_same_cause(self):
        class MyException(Exception):
            pass

        def go():
            try:
                raise MyException("exc one")
            except Exception as err:
                type_, value, tb = sys.exc_info()
                util.reraise(type_, err, tb, value)

        assert_raises_message(
            AssertionError,
            "Same cause emitted",
            go
        )

    def test_raise_from_cause(self):
        class MyException(Exception):
            pass

        class MyOtherException(Exception):
            pass

        me = MyException("exc on")

        def go():
            try:
                raise me
            except Exception:
                util.raise_from_cause(MyOtherException("exc two"))

        try:
            go()
            assert False
        except MyOtherException as moe:
            if testing.requires.python3.enabled:
                is_(moe.__cause__, me)

    @testing.requires.python2
    def test_safe_reraise_py2k_warning(self):
        class MyException(Exception):
            pass

        class MyOtherException(Exception):
            pass

        m1 = MyException("exc one")
        m2 = MyOtherException("exc two")

        def go2():
            raise m2

        def go():
            try:
                raise m1
            except Exception:
                with util.safe_reraise():
                    go2()

        with expect_warnings(
            "An exception has occurred during handling of a previous "
            "exception.  The previous exception "
            "is:.*MyException.*exc one"
        ):
            try:
                go()
                assert False
            except MyOtherException:
                pass


class TestClassProperty(fixtures.TestBase):

    def test_simple(self):
        class A(object):
            something = {'foo': 1}

        class B(A):

            @classproperty
            def something(cls):
                d = dict(super(B, cls).something)
                d.update({'bazz': 2})
                return d

        eq_(B.something, {'foo': 1, 'bazz': 2})


class TestProperties(fixtures.TestBase):

    def test_pickle(self):
        data = {'hello': 'bla'}
        props = util.Properties(data)

        for loader, dumper in picklers():
            s = dumper(props)
            p = loader(s)

            eq_(props._data, p._data)
            eq_(props.keys(), p.keys())

    def test_pickle_immuatbleprops(self):
        data = {'hello': 'bla'}
        props = util.Properties(data).as_immutable()

        for loader, dumper in picklers():
            s = dumper(props)
            p = loader(s)

            eq_(props._data, p._data)
            eq_(props.keys(), p.keys())

    def test_pickle_orderedprops(self):
        data = {'hello': 'bla'}
        props = util.OrderedProperties()
        props.update(data)

        for loader, dumper in picklers():
            s = dumper(props)
            p = loader(s)

            eq_(props._data, p._data)
            eq_(props.keys(), p.keys())
