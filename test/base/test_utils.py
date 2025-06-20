import copy
from decimal import Decimal
import inspect
from pathlib import Path
import pickle
import sys

from sqlalchemy import exc
from sqlalchemy import sql
from sqlalchemy import testing
from sqlalchemy import util
from sqlalchemy.sql import column
from sqlalchemy.sql.base import DedupeColumnCollection
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import combinations
from sqlalchemy.testing import eq_
from sqlalchemy.testing import expect_raises_message
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import in_
from sqlalchemy.testing import is_
from sqlalchemy.testing import is_false
from sqlalchemy.testing import is_instance_of
from sqlalchemy.testing import is_none
from sqlalchemy.testing import is_true
from sqlalchemy.testing import mock
from sqlalchemy.testing import ne_
from sqlalchemy.testing import not_in
from sqlalchemy.testing.util import gc_collect
from sqlalchemy.testing.util import picklers
from sqlalchemy.util import classproperty
from sqlalchemy.util import compat
from sqlalchemy.util import FastIntFlag
from sqlalchemy.util import get_callable_argspec
from sqlalchemy.util import is_non_string_iterable
from sqlalchemy.util import langhelpers
from sqlalchemy.util import preloaded
from sqlalchemy.util import WeakSequence
from sqlalchemy.util._collections import merge_lists_w_ordering
from sqlalchemy.util._has_cython import _all_cython_modules


class WeakSequenceTest(fixtures.TestBase):
    @testing.requires.predictable_gc
    def test_cleanout_elements(self):
        class Foo:
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
        class Foo:
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


class MergeListsWOrderingTest(fixtures.TestBase):
    @testing.combinations(
        (
            ["__tablename__", "id", "x", "created_at"],
            ["id", "name", "data", "y", "created_at"],
            ["__tablename__", "id", "name", "data", "y", "x", "created_at"],
        ),
        (["a", "b", "c", "d", "e", "f"], [], ["a", "b", "c", "d", "e", "f"]),
        ([], ["a", "b", "c", "d", "e", "f"], ["a", "b", "c", "d", "e", "f"]),
        ([], [], []),
        (["a", "b", "c"], ["a", "b", "c"], ["a", "b", "c"]),
        (
            ["a", "b", "c"],
            ["a", "b", "c", "d", "e"],
            ["a", "b", "c", "d", "e"],
        ),
        (["a", "b", "c", "d"], ["c", "d", "e"], ["a", "b", "c", "d", "e"]),
        (
            ["a", "c", "e", "g"],
            ["b", "d", "f", "g"],
            ["a", "c", "e", "b", "d", "f", "g"],  # no overlaps until "g"
        ),
        (
            ["a", "b", "e", "f", "g"],
            ["b", "c", "d", "e"],
            ["a", "b", "c", "d", "e", "f", "g"],
        ),
        (
            ["a", "b", "c", "e", "f"],
            ["c", "d", "f", "g"],
            ["a", "b", "c", "d", "e", "f", "g"],
        ),
        (
            ["c", "d", "f", "g"],
            ["a", "b", "c", "e", "f"],
            ["a", "b", "c", "e", "d", "f", "g"],
        ),
        argnames="a,b,expected",
    )
    def test_merge_lists(self, a, b, expected):
        eq_(merge_lists_w_ordering(a, b), expected)


class OrderedDictTest(fixtures.TestBase):
    def test_odict(self):
        o = util.OrderedDict()
        o["a"] = 1
        o["b"] = 2
        o["snack"] = "attack"
        o["c"] = 3

        eq_(list(o.keys()), ["a", "b", "snack", "c"])
        eq_(list(o.values()), [1, 2, "attack", 3])

        o.pop("snack")
        eq_(list(o.keys()), ["a", "b", "c"])
        eq_(list(o.values()), [1, 2, 3])

        try:
            o.pop("eep")
            assert False
        except KeyError:
            pass

        eq_(o.pop("eep", "woot"), "woot")

        try:
            o.pop("whiff", "bang", "pow")
            assert False
        except TypeError:
            pass

        eq_(list(o.keys()), ["a", "b", "c"])
        eq_(list(o.values()), [1, 2, 3])

        o2 = util.OrderedDict(d=4)
        o2["e"] = 5

        eq_(list(o2.keys()), ["d", "e"])
        eq_(list(o2.values()), [4, 5])

        o.update(o2)
        eq_(list(o.keys()), ["a", "b", "c", "d", "e"])
        eq_(list(o.values()), [1, 2, 3, 4, 5])

        o.setdefault("c", "zzz")
        o.setdefault("f", 6)
        eq_(list(o.keys()), ["a", "b", "c", "d", "e", "f"])
        eq_(list(o.values()), [1, 2, 3, 4, 5, 6])

    def test_odict_constructor(self):
        o = util.OrderedDict(
            [("name", "jbe"), ("fullname", "jonathan"), ("password", "")]
        )
        eq_(list(o.keys()), ["name", "fullname", "password"])

    def test_odict_copy(self):
        o = util.OrderedDict()
        o["zzz"] = 1
        o["aaa"] = 2
        eq_(list(o.keys()), ["zzz", "aaa"])

        o2 = o.copy()
        eq_(list(o2.keys()), list(o.keys()))

        o3 = copy.copy(o)
        eq_(list(o3.keys()), list(o.keys()))

    def test_no_sort_legacy_dictionary(self):
        d1 = {"c": 1, "b": 2, "a": 3}
        util.sort_dictionary(d1)
        eq_(list(d1), ["a", "b", "c"])

    def test_sort_dictionary(self):
        o = util.OrderedDict()

        o["za"] = 1
        o["az"] = 2
        o["cc"] = 3

        eq_(
            list(o),
            ["za", "az", "cc"],
        )

        util.sort_dictionary(o)
        eq_(list(o), ["az", "cc", "za"])

        util.sort_dictionary(o, lambda key: key[1])
        eq_(list(o), ["za", "cc", "az"])


class OrderedSetTest(fixtures.TestBase):
    def test_mutators_against_iter(self):
        # testing a set modified against an iterator
        o = util.OrderedSet([3, 2, 4, 5])

        eq_(o.difference(iter([3, 4])), util.OrderedSet([2, 5]))
        eq_(o.intersection(iter([3, 4, 6])), util.OrderedSet([3, 4]))
        eq_(o.union(iter([3, 4, 6])), util.OrderedSet([3, 2, 4, 5, 6]))
        eq_(
            o.symmetric_difference(iter([3, 4, 6])), util.OrderedSet([2, 5, 6])
        )

    def test_mutators_against_iter_update(self):
        # testing a set modified against an iterator
        o = util.OrderedSet([3, 2, 4, 5])
        o.difference_update(iter([3, 4]))
        eq_(list(o), [2, 5])

        o = util.OrderedSet([3, 2, 4, 5])
        o.intersection_update(iter([3, 4]))
        eq_(list(o), [3, 4])

        o = util.OrderedSet([3, 2, 4, 5])
        o.update(iter([3, 4, 6]))
        eq_(list(o), [3, 2, 4, 5, 6])

        o = util.OrderedSet([3, 2, 4, 5])
        o.symmetric_difference_update(iter([3, 4, 6]))
        eq_(list(o), [2, 5, 6])

    def test_len(self):
        eq_(len(util.OrderedSet([1, 2, 3])), 3)

    def test_eq_no_insert_order(self):
        eq_(util.OrderedSet([3, 2, 4, 5]), util.OrderedSet([2, 3, 4, 5]))

        ne_(util.OrderedSet([3, 2, 4, 5]), util.OrderedSet([3, 2, 4, 5, 6]))

    def test_eq_non_ordered_set(self):
        eq_(util.OrderedSet([3, 2, 4, 5]), {2, 3, 4, 5})

        ne_(util.OrderedSet([3, 2, 4, 5]), {3, 2, 4, 5, 6})

    def test_repr(self):
        o = util.OrderedSet([])
        eq_(str(o), "OrderedSet([])")
        o = util.OrderedSet([3, 2, 4, 5])
        eq_(str(o), "OrderedSet([3, 2, 4, 5])")

    def test_modify(self):
        o = util.OrderedSet([3, 9, 11])
        is_none(o.add(42))
        in_(42, o)
        in_(3, o)

        is_none(o.remove(9))
        not_in(9, o)
        in_(3, o)

        is_none(o.discard(11))
        in_(3, o)

        o.add(99)
        is_none(o.insert(1, 13))
        eq_(list(o), [3, 13, 42, 99])
        eq_(o[2], 42)

        val = o.pop()
        eq_(val, 99)
        not_in(99, o)
        eq_(list(o), [3, 13, 42])

        is_none(o.clear())
        not_in(3, o)
        is_false(bool(o))

    def test_empty_pop(self):
        with expect_raises_message(KeyError, "pop from an empty set"):
            util.OrderedSet().pop()

    @testing.combinations(
        lambda o: o + util.OrderedSet([11, 22]),
        lambda o: o | util.OrderedSet([11, 22]),
        lambda o: o.union(util.OrderedSet([11, 22])),
        lambda o: o.union([11, 2], [22, 8]),
    )
    def test_op(self, fn):
        o = util.OrderedSet(range(10))
        x = fn(o)
        is_instance_of(x, util.OrderedSet)
        in_(9, x)
        in_(11, x)
        not_in(11, o)

    def test_update(self):
        o = util.OrderedSet(range(10))
        is_none(o.update([22, 2], [33, 11]))
        in_(11, o)
        in_(22, o)

    def test_set_ops(self):
        o1, o2 = util.OrderedSet([1, 3, 5, 7]), {2, 3, 4, 5}
        eq_(o1 & o2, {3, 5})
        eq_(o1.intersection(o2), {3, 5})
        o3 = o1.copy()
        o3 &= o2
        eq_(o3, {3, 5})
        o3 = o1.copy()
        is_none(o3.intersection_update(o2))
        eq_(o3, {3, 5})

        eq_(o1 | o2, {1, 2, 3, 4, 5, 7})
        eq_(o1.union(o2), {1, 2, 3, 4, 5, 7})
        o3 = o1.copy()
        o3 |= o2
        eq_(o3, {1, 2, 3, 4, 5, 7})
        o3 = o1.copy()
        is_none(o3.update(o2))
        eq_(o3, {1, 2, 3, 4, 5, 7})

        eq_(o1 - o2, {1, 7})
        eq_(o1.difference(o2), {1, 7})
        o3 = o1.copy()
        o3 -= o2
        eq_(o3, {1, 7})
        o3 = o1.copy()
        is_none(o3.difference_update(o2))
        eq_(o3, {1, 7})

        eq_(o1 ^ o2, {1, 2, 4, 7})
        eq_(o1.symmetric_difference(o2), {1, 2, 4, 7})
        o3 = o1.copy()
        o3 ^= o2
        eq_(o3, {1, 2, 4, 7})
        o3 = o1.copy()
        is_none(o3.symmetric_difference_update(o2))
        eq_(o3, {1, 2, 4, 7})

    def test_copy(self):
        o = util.OrderedSet([3, 2, 4, 5])
        cp = o.copy()
        is_instance_of(cp, util.OrderedSet)
        eq_(o, cp)
        o.add(42)
        is_false(42 in cp)

    def test_pickle(self):
        o = util.OrderedSet([2, 4, 9, 42])
        for loads, dumps in picklers():
            l = loads(dumps(o))
            is_instance_of(l, util.OrderedSet)
            eq_(list(l), [2, 4, 9, 42])


class ImmutableDictTest(fixtures.TestBase):
    def test_union_no_change(self):
        d = util.immutabledict({1: 2, 3: 4})

        d2 = d.union({})

        is_(d2, d)

    def test_merge_with_no_change(self):
        d = util.immutabledict({1: 2, 3: 4})

        d2 = d.merge_with({}, None)

        eq_(d2, {1: 2, 3: 4})
        is_(d2, d)

    def test_merge_with_dicts(self):
        d = util.immutabledict({1: 2, 3: 4})

        d2 = d.merge_with({3: 5, 7: 12}, {9: 18, 15: 25})

        eq_(d, {1: 2, 3: 4})
        eq_(d2, {1: 2, 3: 5, 7: 12, 9: 18, 15: 25})
        assert isinstance(d2, util.immutabledict)

        d3 = d.merge_with({17: 42})

        eq_(d3, {1: 2, 3: 4, 17: 42})

    def test_merge_with_tuples(self):
        d = util.immutabledict({1: 2, 3: 4})

        d2 = d.merge_with([(3, 5), (7, 12)], [(9, 18), (15, 25)])

        eq_(d, {1: 2, 3: 4})
        eq_(d2, {1: 2, 3: 5, 7: 12, 9: 18, 15: 25})

    def test_union_dictionary(self):
        d = util.immutabledict({1: 2, 3: 4})

        d2 = d.union({3: 5, 7: 12})
        assert isinstance(d2, util.immutabledict)

        eq_(d, {1: 2, 3: 4})
        eq_(d2, {1: 2, 3: 5, 7: 12})

    def _dont_test_union_kw(self):
        d = util.immutabledict({"a": "b", "c": "d"})

        d2 = d.union(e="f", g="h")
        assert isinstance(d2, util.immutabledict)

        eq_(d, {"a": "b", "c": "d"})
        eq_(d2, {"a": "b", "c": "d", "e": "f", "g": "h"})

    def test_union_tuples(self):
        d = util.immutabledict({1: 2, 3: 4})

        d2 = d.union([(3, 5), (7, 12)])

        eq_(d, {1: 2, 3: 4})
        eq_(d2, {1: 2, 3: 5, 7: 12})

    def test_keys(self):
        d = util.immutabledict({1: 2, 3: 4})

        eq_(set(d.keys()), {1, 3})

    def test_values(self):
        d = util.immutabledict({1: 2, 3: 4})

        eq_(set(d.values()), {2, 4})

    def test_items(self):
        d = util.immutabledict({1: 2, 3: 4})

        eq_(set(d.items()), {(1, 2), (3, 4)})

    def test_contains(self):
        d = util.immutabledict({1: 2, 3: 4})

        assert 1 in d
        assert "foo" not in d

    def test_rich_compare(self):
        d = util.immutabledict({1: 2, 3: 4})
        d2 = util.immutabledict({1: 2, 3: 4})
        d3 = util.immutabledict({5: 12})
        d4 = {5: 12}

        eq_(d, d2)
        ne_(d, d3)
        ne_(d, d4)
        eq_(d3, d4)

    def test_serialize(self):
        d = util.immutabledict({1: 2, 3: 4})
        for loads, dumps in picklers():
            d2 = loads(dumps(d))

            eq_(d2, {1: 2, 3: 4})

            assert isinstance(d2, util.immutabledict)

    def test_repr(self):
        # this is used by the stub generator in alembic
        i = util.immutabledict()
        eq_(str(i), "immutabledict({})")
        i2 = util.immutabledict({"a": 42, 42: "a"})
        eq_(str(i2), "immutabledict({'a': 42, 42: 'a'})")

    def test_pep584(self):
        i = util.immutabledict({"a": 2})
        with expect_raises_message(TypeError, "object is immutable"):
            i |= {"b": 42}
        eq_(i, {"a": 2})

        i2 = i | {"x": 3}
        eq_(i, {"a": 2})
        eq_(i2, {"a": 2, "x": 3})
        is_true(isinstance(i2, util.immutabledict))

        i2 = {"x": 3} | i2
        eq_(i, {"a": 2})
        eq_(i2, {"a": 2, "x": 3})
        is_true(isinstance(i2, util.immutabledict))


class ImmutableTest(fixtures.TestBase):
    @combinations(util.immutabledict({1: 2, 3: 4}), util.FacadeDict({2: 3}))
    def test_immutable(self, d):
        calls = (
            lambda: d.__delitem__(1),
            lambda: d.__setitem__(2, 3),
            lambda: d.__setattr__(2, 3),
            d.clear,
            lambda: d.setdefault(1, 3),
            lambda: d.update({2: 4}),
        )
        if hasattr(d, "pop"):
            calls += (lambda: d.pop(2), d.popitem)
        for m in calls:
            with expect_raises_message(TypeError, "object is immutable"):
                m()

    def test_readonly_properties(self):
        d = util.ReadOnlyProperties({3: 4})
        calls = (
            lambda: d.__delitem__(1),
            lambda: d.__setitem__(2, 3),
            lambda: d.__setattr__(2, 3),
        )
        for m in calls:
            with expect_raises_message(TypeError, "object is immutable"):
                m()


class MemoizedAttrTest(fixtures.TestBase):
    def test_memoized_property(self):
        val = [20]

        class Foo:
            @util.memoized_property
            def bar(self):
                v = val[0]
                val[0] += 1
                return v

        ne_(Foo.bar, None)
        f1 = Foo()
        assert "bar" not in f1.__dict__
        eq_(f1.bar, 20)
        eq_(f1.bar, 20)
        eq_(val[0], 21)
        eq_(f1.__dict__["bar"], 20)

    def test_memoized_instancemethod(self):
        val = [20]

        class Foo:
            @util.memoized_instancemethod
            def bar(self):
                v = val[0]
                val[0] += 1
                return v

        assert inspect.ismethod(Foo().bar)
        ne_(Foo.bar, None)
        f1 = Foo()
        assert "bar" not in f1.__dict__
        eq_(f1.bar(), 20)
        eq_(f1.bar(), 20)
        eq_(val[0], 21)

    def test_memoized_slots(self):
        canary = mock.Mock()

        class Foob(util.MemoizedSlots):
            __slots__ = ("foo_bar", "gogo")

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
        class MyFancyDefault:
            """a fancy default"""

            def __call__(self):
                """run the fancy default"""
                return 10

        def_ = MyFancyDefault()
        c = util.wrap_callable(lambda: def_(), def_)

        eq_(c.__name__, "MyFancyDefault")
        eq_(c.__doc__, "run the fancy default")

    def test_wrapping_update_wrapper_cls_noclsdocstring(self):
        class MyFancyDefault:
            def __call__(self):
                """run the fancy default"""
                return 10

        def_ = MyFancyDefault()
        c = util.wrap_callable(lambda: def_(), def_)
        eq_(c.__name__, "MyFancyDefault")
        eq_(c.__doc__, "run the fancy default")

    def test_wrapping_update_wrapper_cls_nomethdocstring(self):
        class MyFancyDefault:
            """a fancy default"""

            def __call__(self):
                return 10

        def_ = MyFancyDefault()
        c = util.wrap_callable(lambda: def_(), def_)
        eq_(c.__name__, "MyFancyDefault")
        eq_(c.__doc__, "a fancy default")

    def test_wrapping_update_wrapper_cls_noclsdocstring_nomethdocstring(self):
        class MyFancyDefault:
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
            lambda: my_functools_default(), my_functools_default
        )
        eq_(c.__name__, "partial")
        if not compat.pypy:  # pypy fails this check
            eq_(c.__doc__, my_functools_default.__call__.__doc__)
        eq_(c(), 5)


class ToListTest(fixtures.TestBase):
    def test_from_string(self):
        eq_(util.to_list("xyz"), ["xyz"])

    def test_from_set(self):
        spec = util.to_list({1, 2, 3})
        assert isinstance(spec, list)
        eq_(sorted(spec), [1, 2, 3])

    def test_from_dict(self):
        spec = util.to_list({1: "a", 2: "b", 3: "c"})
        assert isinstance(spec, list)
        eq_(sorted(spec), [1, 2, 3])

    def test_from_tuple(self):
        eq_(util.to_list((1, 2, 3)), [1, 2, 3])

    def test_from_bytes(self):
        eq_(util.to_list(compat.b("abc")), [compat.b("abc")])

        eq_(
            util.to_list([compat.b("abc"), compat.b("def")]),
            [compat.b("abc"), compat.b("def")],
        )


class ColumnCollectionCommon(testing.AssertsCompiledSQL):
    def _assert_collection_integrity(self, coll):
        eq_(coll._colset, {c for k, c, _ in coll._collection})
        d = {}
        for k, col, _ in coll._collection:
            d.setdefault(k, (k, col))
        d.update(
            {idx: (k, col) for idx, (k, col, _) in enumerate(coll._collection)}
        )
        eq_(coll._index, d)

        if not coll._proxy_index:
            coll._init_proxy_index()

        all_metrics = {
            metrics for mm in coll._proxy_index.values() for metrics in mm
        }
        eq_(
            all_metrics,
            {m for (_, _, m) in coll._collection},
        )

        for mm in all_metrics:
            for eps_col in mm.get_expanded_proxy_set():
                assert mm in coll._proxy_index[eps_col]
                for mm_ in coll._proxy_index[eps_col]:
                    assert eps_col in mm_.get_expanded_proxy_set()

    def test_keys(self):
        c1, c2, c3 = sql.column("c1"), sql.column("c2"), sql.column("c3")
        c2.key = "foo"
        cc = self._column_collection(
            columns=[("c1", c1), ("foo", c2), ("c3", c3)]
        )
        keys = cc.keys()
        eq_(keys, ["c1", "foo", "c3"])
        ne_(id(keys), id(cc.keys()))

        ci = cc.as_readonly()
        eq_(ci.keys(), ["c1", "foo", "c3"])

    def test_values(self):
        c1, c2, c3 = sql.column("c1"), sql.column("c2"), sql.column("c3")
        c2.key = "foo"
        cc = self._column_collection(
            columns=[("c1", c1), ("foo", c2), ("c3", c3)]
        )
        val = cc.values()
        eq_(val, [c1, c2, c3])
        ne_(id(val), id(cc.values()))

        ci = cc.as_readonly()
        eq_(ci.values(), [c1, c2, c3])

    def test_items(self):
        c1, c2, c3 = sql.column("c1"), sql.column("c2"), sql.column("c3")
        c2.key = "foo"
        cc = self._column_collection(
            columns=[("c1", c1), ("foo", c2), ("c3", c3)]
        )
        items = cc.items()
        eq_(items, [("c1", c1), ("foo", c2), ("c3", c3)])
        ne_(id(items), id(cc.items()))

        ci = cc.as_readonly()
        eq_(ci.items(), [("c1", c1), ("foo", c2), ("c3", c3)])

    def test_getitem_tuple_str(self):
        c1, c2, c3 = sql.column("c1"), sql.column("c2"), sql.column("c3")
        c2.key = "foo"
        cc = self._column_collection(
            columns=[("c1", c1), ("foo", c2), ("c3", c3)]
        )
        sub_cc = cc["c3", "foo"]
        is_(sub_cc.c3, c3)
        eq_(list(sub_cc), [c3, c2])

    def test_getitem_tuple_int(self):
        c1, c2, c3 = sql.column("c1"), sql.column("c2"), sql.column("c3")
        c2.key = "foo"
        cc = self._column_collection(
            columns=[("c1", c1), ("foo", c2), ("c3", c3)]
        )

        sub_cc = cc[2, 1]
        is_(sub_cc.c3, c3)
        eq_(list(sub_cc), [c3, c2])

    def test_key_index_error(self):
        cc = self._column_collection(
            columns=[
                ("col1", sql.column("col1")),
                ("col2", sql.column("col2")),
            ]
        )
        assert_raises(KeyError, lambda: cc["foo"])
        assert_raises(KeyError, lambda: cc[object()])
        assert_raises(IndexError, lambda: cc[5])

    def test_contains_column(self):
        c1, c2, c3 = sql.column("c1"), sql.column("c2"), sql.column("c3")
        cc = self._column_collection(columns=[("c1", c1), ("c2", c2)])

        is_true(cc.contains_column(c1))
        is_false(cc.contains_column(c3))

    def test_contains_column_not_column(self):
        c1, c2, c3 = sql.column("c1"), sql.column("c2"), sql.column("c3")
        cc = self._column_collection(columns=[("c1", c1), ("c2", c2)])

        is_false(cc.contains_column(c3 == 2))

        with testing.expect_raises_message(
            exc.ArgumentError,
            "contains_column cannot be used with string arguments",
        ):
            cc.contains_column("c1")
        with testing.expect_raises_message(
            exc.ArgumentError,
            "contains_column cannot be used with string arguments",
        ):
            cc.contains_column("foo")

    def test_in(self):
        col1 = sql.column("col1")
        cc = self._column_collection(
            columns=[
                ("col1", col1),
                ("col2", sql.column("col2")),
                ("col3", sql.column("col3")),
            ]
        )
        assert "col1" in cc
        assert "col2" in cc

        assert_raises_message(
            exc.ArgumentError,
            "__contains__ requires a string argument",
            lambda: col1 in cc,
        )

    def test_compare(self):
        c1 = sql.column("col1")
        c2 = c1.label("col2")
        c3 = sql.column("col3")

        is_true(
            self._column_collection(
                [("col1", c1), ("col2", c2), ("col3", c3)]
            ).compare(
                self._column_collection(
                    [("col1", c1), ("col2", c2), ("col3", c3)]
                )
            )
        )
        is_false(
            self._column_collection(
                [("col1", c1), ("col2", c2), ("col3", c3)]
            ).compare(self._column_collection([("col1", c1), ("col2", c2)]))
        )

    def test_str(self):
        c1 = sql.column("col1")
        c2 = c1.label("col2")
        c3 = sql.column("col3")
        cc = self._column_collection(
            [("col1", c1), ("col2", c2), ("col3", c3)]
        )

        eq_(str(cc), "%s(%s, %s, %s)" % (type(cc).__name__, c1, c2, c3))
        eq_(repr(cc), object.__repr__(cc))


class ColumnCollectionTest(ColumnCollectionCommon, fixtures.TestBase):
    def _column_collection(self, columns=None):
        return sql.ColumnCollection(columns=columns)

    def test_separate_key_all_cols(self):
        c1, c2 = sql.column("col1"), sql.column("col2")
        cc = self._column_collection([("kcol1", c1), ("kcol2", c2)])
        eq_(cc._all_columns, [c1, c2])

    def test_separate_key_get(self):
        c1, c2 = sql.column("col1"), sql.column("col2")
        cc = self._column_collection([("kcol1", c1), ("kcol2", c2)])
        is_(cc.kcol1, c1)
        is_(cc.kcol2, c2)

    def test_separate_key_in(self):
        cc = self._column_collection(
            columns=[
                ("kcol1", sql.column("col1")),
                ("kcol2", sql.column("col2")),
                ("kcol3", sql.column("col3")),
            ]
        )
        assert "col1" not in cc
        assert "kcol2" in cc

    def test_dupes_add(self):
        c1, c2a, c3, c2b = (
            column("c1"),
            column("c2"),
            column("c3"),
            column("c2"),
        )

        cc = sql.ColumnCollection()

        cc.add(c1)
        cc.add(c2a, "c2")
        cc.add(c3)
        cc.add(c2b)

        eq_(cc._all_columns, [c1, c2a, c3, c2b])

        eq_(list(cc), [c1, c2a, c3, c2b])
        eq_(cc.keys(), ["c1", "c2", "c3", "c2"])

        assert cc.contains_column(c2a)
        assert cc.contains_column(c2b)

        # this is deterministic
        is_(cc["c2"], c2a)

        self._assert_collection_integrity(cc)

        ci = cc.as_readonly()
        eq_(ci._all_columns, [c1, c2a, c3, c2b])
        eq_(list(ci), [c1, c2a, c3, c2b])
        eq_(ci.keys(), ["c1", "c2", "c3", "c2"])

    def test_dupes_construct(self):
        c1, c2a, c3, c2b = (
            column("c1"),
            column("c2"),
            column("c3"),
            column("c2"),
        )

        cc = sql.ColumnCollection(
            columns=[("c1", c1), ("c2", c2a), ("c3", c3), ("c2", c2b)]
        )

        eq_(cc._all_columns, [c1, c2a, c3, c2b])

        eq_(list(cc), [c1, c2a, c3, c2b])
        eq_(cc.keys(), ["c1", "c2", "c3", "c2"])

        assert cc.contains_column(c2a)
        assert cc.contains_column(c2b)

        # this is deterministic
        is_(cc["c2"], c2a)

        self._assert_collection_integrity(cc)

        ci = cc.as_readonly()
        eq_(ci._all_columns, [c1, c2a, c3, c2b])
        eq_(list(ci), [c1, c2a, c3, c2b])
        eq_(ci.keys(), ["c1", "c2", "c3", "c2"])

    def test_identical_dupe_construct(self):
        c1, c2, c3 = (column("c1"), column("c2"), column("c3"))

        cc = sql.ColumnCollection(
            columns=[("c1", c1), ("c2", c2), ("c3", c3), ("c2", c2)]
        )

        eq_(cc._all_columns, [c1, c2, c3, c2])

        # for iter, c2a is replaced by c2b, ordering
        # is maintained in that way.  ideally, iter would be
        # the same as the "_all_columns" collection.
        eq_(list(cc), [c1, c2, c3, c2])

        assert cc.contains_column(c2)
        self._assert_collection_integrity(cc)

        ci = cc.as_readonly()
        eq_(ci._all_columns, [c1, c2, c3, c2])
        eq_(list(ci), [c1, c2, c3, c2])


class DedupeColumnCollectionTest(ColumnCollectionCommon, fixtures.TestBase):
    def _column_collection(self, columns=None):
        return DedupeColumnCollection(columns=columns)

    def test_separate_key_cols(self):
        c1, c2 = sql.column("col1"), sql.column("col2")
        assert_raises_message(
            exc.ArgumentError,
            "DedupeColumnCollection requires columns be under "
            "the same key as their .key",
            self._column_collection,
            [("kcol1", c1), ("kcol2", c2)],
        )

        cc = self._column_collection()
        assert_raises_message(
            exc.ArgumentError,
            "DedupeColumnCollection requires columns be under "
            "the same key as their .key",
            cc.add,
            c1,
            "kcol1",
        )

    def test_pickle_w_mutation(self):
        c1, c2, c3 = sql.column("c1"), sql.column("c2"), sql.column("c3")

        c2.key = "foo"

        cc = self._column_collection(columns=[("c1", c1), ("foo", c2)])
        ci = cc.as_readonly()

        d = {"cc": cc, "ci": ci}

        for loads, dumps in picklers():
            dp = loads(dumps(d))

            cp = dp["cc"]
            cpi = dp["ci"]
            self._assert_collection_integrity(cp)
            self._assert_collection_integrity(cpi)

            assert cp._colset is cpi._colset
            assert cp._index is cpi._index
            assert cp._collection is cpi._collection

            cp.add(c3)

            eq_(cp.keys(), ["c1", "foo", "c3"])
            eq_(cpi.keys(), ["c1", "foo", "c3"])

            assert cp.contains_column(c3)
            assert cpi.contains_column(c3)

    def test_keys_after_replace(self):
        c1, c2, c3 = sql.column("c1"), sql.column("c2"), sql.column("c3")
        c2.key = "foo"
        cc = self._column_collection(
            columns=[("c1", c1), ("foo", c2), ("c3", c3)]
        )
        eq_(cc.keys(), ["c1", "foo", "c3"])

        c4 = sql.column("c3")
        cc.replace(c4)
        eq_(cc.keys(), ["c1", "foo", "c3"])
        self._assert_collection_integrity(cc)

    def test_dupes_add_dedupe(self):
        cc = DedupeColumnCollection()

        c1, c2a, c3, c2b = (
            column("c1"),
            column("c2"),
            column("c3"),
            column("c2"),
        )

        cc.add(c1)
        cc.add(c2a)
        cc.add(c3)
        cc.add(c2b)

        eq_(cc._all_columns, [c1, c2b, c3])

        eq_(list(cc), [c1, c2b, c3])

        assert not cc.contains_column(c2a)
        assert cc.contains_column(c2b)
        self._assert_collection_integrity(cc)

    def test_dupes_construct_dedupe(self):
        c1, c2a, c3, c2b = (
            column("c1"),
            column("c2"),
            column("c3"),
            column("c2"),
        )

        cc = DedupeColumnCollection(
            columns=[("c1", c1), ("c2", c2a), ("c3", c3), ("c2", c2b)]
        )

        eq_(cc._all_columns, [c1, c2b, c3])

        eq_(list(cc), [c1, c2b, c3])

        assert not cc.contains_column(c2a)
        assert cc.contains_column(c2b)
        self._assert_collection_integrity(cc)

    def test_identical_dupe_add_dedupes(self):
        cc = DedupeColumnCollection()

        c1, c2, c3 = (column("c1"), column("c2"), column("c3"))

        cc.add(c1)
        cc.add(c2)
        cc.add(c3)
        cc.add(c2)

        eq_(cc._all_columns, [c1, c2, c3])

        # for iter, c2a is replaced by c2b, ordering
        # is maintained in that way.  ideally, iter would be
        # the same as the "_all_columns" collection.
        eq_(list(cc), [c1, c2, c3])

        assert cc.contains_column(c2)
        self._assert_collection_integrity(cc)

        ci = cc.as_readonly()
        eq_(ci._all_columns, [c1, c2, c3])
        eq_(list(ci), [c1, c2, c3])

    def test_identical_dupe_construct_dedupes(self):
        c1, c2, c3 = (column("c1"), column("c2"), column("c3"))

        cc = DedupeColumnCollection(
            columns=[("c1", c1), ("c2", c2), ("c3", c3), ("c2", c2)]
        )

        eq_(cc._all_columns, [c1, c2, c3])

        # for iter, c2a is replaced by c2b, ordering
        # is maintained in that way.  ideally, iter would be
        # the same as the "_all_columns" collection.
        eq_(list(cc), [c1, c2, c3])

        assert cc.contains_column(c2)
        self._assert_collection_integrity(cc)

        ci = cc.as_readonly()
        eq_(ci._all_columns, [c1, c2, c3])
        eq_(list(ci), [c1, c2, c3])

    def test_replace(self):
        cc = DedupeColumnCollection()
        ci = cc.as_readonly()

        c1, c2a, c3, c2b = (
            column("c1"),
            column("c2"),
            column("c3"),
            column("c2"),
        )

        cc.add(c1)
        cc.add(c2a)
        cc.add(c3)

        cc.replace(c2b)

        eq_(cc._all_columns, [c1, c2b, c3])
        eq_(list(cc), [c1, c2b, c3])
        is_(cc[1], c2b)

        assert not cc.contains_column(c2a)
        assert cc.contains_column(c2b)
        self._assert_collection_integrity(cc)

        eq_(ci._all_columns, [c1, c2b, c3])
        eq_(list(ci), [c1, c2b, c3])
        is_(ci[1], c2b)

    def test_replace_key_matches_name_of_another(self):
        cc = DedupeColumnCollection()
        ci = cc.as_readonly()

        c1, c2a, c3, c2b = (
            column("c1"),
            column("c2"),
            column("c3"),
            column("c4"),
        )
        c2b.key = "c2"

        cc.add(c1)
        cc.add(c2a)
        cc.add(c3)

        cc.replace(c2b)

        eq_(cc._all_columns, [c1, c2b, c3])
        eq_(list(cc), [c1, c2b, c3])
        is_(cc[1], c2b)
        self._assert_collection_integrity(cc)

        assert not cc.contains_column(c2a)
        assert cc.contains_column(c2b)

        eq_(ci._all_columns, [c1, c2b, c3])
        eq_(list(ci), [c1, c2b, c3])
        is_(ci[1], c2b)

    def test_replace_key_matches(self):
        cc = DedupeColumnCollection()
        ci = cc.as_readonly()

        c1, c2a, c3, c2b = (
            column("c1"),
            column("c2"),
            column("c3"),
            column("X"),
        )
        c2b.key = "c2"

        cc.add(c1)
        cc.add(c2a)
        cc.add(c3)

        cc.replace(c2b)

        assert not cc.contains_column(c2a)
        assert cc.contains_column(c2b)
        is_(cc[1], c2b)
        assert_raises(IndexError, lambda: cc[3])
        self._assert_collection_integrity(cc)

        eq_(cc._all_columns, [c1, c2b, c3])
        eq_(list(cc), [c1, c2b, c3])

        eq_(ci._all_columns, [c1, c2b, c3])
        eq_(list(ci), [c1, c2b, c3])
        is_(ci[1], c2b)
        assert_raises(IndexError, lambda: ci[3])

    def test_replace_name_matches(self):
        cc = DedupeColumnCollection()
        ci = cc.as_readonly()

        c1, c2a, c3, c2b = (
            column("c1"),
            column("c2"),
            column("c3"),
            column("c2"),
        )
        c2b.key = "X"

        cc.add(c1)
        cc.add(c2a)
        cc.add(c3)

        cc.replace(c2b)

        assert not cc.contains_column(c2a)
        assert cc.contains_column(c2b)

        eq_(cc._all_columns, [c1, c2b, c3])
        eq_(list(cc), [c1, c2b, c3])
        eq_(len(cc), 3)
        is_(cc[1], c2b)
        self._assert_collection_integrity(cc)

        eq_(ci._all_columns, [c1, c2b, c3])
        eq_(list(ci), [c1, c2b, c3])
        eq_(len(ci), 3)
        is_(ci[1], c2b)

    def test_replace_no_match(self):
        cc = DedupeColumnCollection()
        ci = cc.as_readonly()

        c1, c2, c3, c4 = column("c1"), column("c2"), column("c3"), column("c4")
        c4.key = "X"

        cc.add(c1)
        cc.add(c2)
        cc.add(c3)

        cc.replace(c4)

        assert cc.contains_column(c2)
        assert cc.contains_column(c4)

        eq_(cc._all_columns, [c1, c2, c3, c4])
        eq_(list(cc), [c1, c2, c3, c4])
        is_(cc[3], c4)
        self._assert_collection_integrity(cc)

        eq_(ci._all_columns, [c1, c2, c3, c4])
        eq_(list(ci), [c1, c2, c3, c4])
        is_(ci[3], c4)

    def test_replace_switch_key_name(self):
        c1 = column("id")
        c2 = column("street")
        c3 = column("user_id")

        cc = DedupeColumnCollection(
            columns=[("id", c1), ("street", c2), ("user_id", c3)]
        )

        # for replace col with different key than name, it necessarily
        # removes two columns

        c4 = column("id")
        c4.key = "street"

        cc.replace(c4)

        eq_(list(cc), [c4, c3])
        self._assert_collection_integrity(cc)

    def test_remove(self):
        c1, c2, c3 = column("c1"), column("c2"), column("c3")

        cc = DedupeColumnCollection(
            columns=[("c1", c1), ("c2", c2), ("c3", c3)]
        )
        ci = cc.as_readonly()

        eq_(cc._all_columns, [c1, c2, c3])
        eq_(list(cc), [c1, c2, c3])
        assert cc.contains_column(c2)
        assert "c2" in cc

        eq_(ci._all_columns, [c1, c2, c3])
        eq_(list(ci), [c1, c2, c3])
        assert ci.contains_column(c2)
        assert "c2" in ci

        cc.remove(c2)

        eq_(cc._all_columns, [c1, c3])
        eq_(list(cc), [c1, c3])
        is_(cc[0], c1)
        is_(cc[1], c3)
        assert not cc.contains_column(c2)
        assert "c2" not in cc
        self._assert_collection_integrity(cc)

        eq_(ci._all_columns, [c1, c3])
        eq_(list(ci), [c1, c3])
        is_(ci[0], c1)
        is_(ci[1], c3)
        assert not ci.contains_column(c2)
        assert "c2" not in ci

        assert_raises(IndexError, lambda: ci[2])

    def test_remove_doesnt_change_iteration(self):
        c1, c2, c3, c4, c5 = (
            column("c1"),
            column("c2"),
            column("c3"),
            column("c4"),
            column("c5"),
        )

        cc = DedupeColumnCollection(
            columns=[
                ("c1", c1),
                ("c2", c2),
                ("c3", c3),
                ("c4", c4),
                ("c5", c5),
            ]
        )

        for col in cc:
            if col.name not in ["c1", "c2"]:
                cc.remove(col)

        eq_(cc.keys(), ["c1", "c2"])
        eq_([c.name for c in cc], ["c1", "c2"])
        self._assert_collection_integrity(cc)

    def test_dupes_extend(self):
        cc = DedupeColumnCollection()
        ci = cc.as_readonly()

        c1, c2a, c3, c2b = (
            column("c1"),
            column("c2"),
            column("c3"),
            column("c2"),
        )

        cc.add(c1)
        cc.add(c2a)

        cc.extend([c3, c2b])  # this should remove c2a

        eq_(cc._all_columns, [c1, c2b, c3])
        eq_(list(cc), [c1, c2b, c3])
        is_(cc[1], c2b)
        is_(cc[2], c3)
        assert_raises(IndexError, lambda: cc[3])
        self._assert_collection_integrity(cc)

        assert not cc.contains_column(c2a)
        assert cc.contains_column(c2b)

        eq_(ci._all_columns, [c1, c2b, c3])
        eq_(list(ci), [c1, c2b, c3])
        is_(ci[1], c2b)
        is_(ci[2], c3)
        assert_raises(IndexError, lambda: ci[3])

        assert not ci.contains_column(c2a)
        assert ci.contains_column(c2b)

    def test_extend_existing_maintains_ordering(self):
        cc = DedupeColumnCollection()

        c1, c2, c3, c4, c5 = (
            column("c1"),
            column("c2"),
            column("c3"),
            column("c4"),
            column("c5"),
        )

        cc.extend([c1, c2])
        eq_(cc._all_columns, [c1, c2])
        self._assert_collection_integrity(cc)

        cc.extend([c3])
        eq_(cc._all_columns, [c1, c2, c3])
        self._assert_collection_integrity(cc)

        cc.extend([c4, c2, c5])

        eq_(cc._all_columns, [c1, c2, c3, c4, c5])
        self._assert_collection_integrity(cc)


class LRUTest(fixtures.TestBase):
    def test_lru(self):
        class item:
            def __init__(self, id_):
                self.id = id_

            def __str__(self):
                return "item id %d" % self.id

        lru = util.LRUCache(10, threshold=0.2)

        for id_ in range(1, 20):
            lru[id_] = item(id_)

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

        lru[25]
        i2 = item(25)
        lru[25] = i2
        assert 25 in lru
        assert lru[25] is i2


class ImmutableSubclass(str):
    pass


class FlattenIteratorTest(fixtures.TestBase):
    def test_flatten(self):
        assert list(util.flatten_iterator([[1, 2, 3], [4, 5, 6], 7, 8])) == [
            1,
            2,
            3,
            4,
            5,
            6,
            7,
            8,
        ]

    def test_str_with_iter(self):
        """ensure that a str object with an __iter__ method (like in
        PyPy) is not interpreted as an iterable.

        """

        class IterString(str):
            def __iter__(self):
                return iter(self + "")

        iter_list = [IterString("asdf"), [IterString("x"), IterString("y")]]

        assert list(util.flatten_iterator(iter_list)) == ["asdf", "x", "y"]


class HashOverride:
    def __init__(self, value=None):
        self.value = value

    def __hash__(self):
        return hash(self.value)


class NoHash:
    def __init__(self, value=None):
        self.value = value

    __hash__ = None


class EqOverride:
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


class HashEqOverride:
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


class MiscTest(fixtures.TestBase):
    @testing.combinations(
        (["one", "two", "three"], True),
        (("one", "two", "three"), True),
        ((), True),
        ("four", False),
        (252, False),
        (Decimal("252"), False),
        (b"four", False),
        (iter("four"), True),
        (b"", False),
        ("", False),
        (None, False),
        ({"dict": "value"}, True),
        ({}, True),
        ({"set", "two"}, True),
        (set(), True),
        (util.immutabledict(), True),
        (util.immutabledict({"key": "value"}), True),
    )
    def test_non_string_iterable_check(self, fixture, expected):
        is_(is_non_string_iterable(fixture), expected)


class IdentitySetTest(fixtures.TestBase):
    obj_type = object

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

        for type_ in (NoHash, EqOverride, HashOverride, HashEqOverride):
            data = [type_(1), type_(1), type_(2)]
            ids = util.IdentitySet()
            for i in list(range(3)) + list(range(3)):
                ids.add(data[i])
            self.assert_eq(ids, data)

    def test_dunder_sub2(self):
        IdentitySet = util.IdentitySet
        o1, o2, o3 = self.obj_type(), self.obj_type(), self.obj_type()
        ids1 = IdentitySet([o1])
        ids2 = IdentitySet([o1, o2, o3])
        eq_(ids2 - ids1, IdentitySet([o2, o3]))

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
            TypeError, unique1.symmetric_difference, not_an_identity_set
        )

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
        o1, o2, o3, o4, o5 = (
            self.obj_type(),
            self.obj_type(),
            self.obj_type(),
            self.obj_type(),
            self.obj_type(),
        )
        super_ = util.IdentitySet([o1, o2, o3])
        sub_ = util.IdentitySet([o2])
        twin1 = util.IdentitySet([o3])
        twin2 = util.IdentitySet([o3])
        unique1 = util.IdentitySet([o4])
        unique2 = util.IdentitySet([o5])
        return super_, sub_, twin1, twin2, unique1, unique2

    def _assert_unorderable_types(self, callable_):
        assert_raises_message(
            TypeError, "not supported between instances of", callable_
        )

    def test_basic_sanity(self):
        IdentitySet = util.IdentitySet

        o1, o2, o3 = self.obj_type(), self.obj_type(), self.obj_type()
        ids = IdentitySet([o1])
        ids.discard(o1)
        ids.discard(o1)
        ids.add(o1)
        ids.remove(o1)
        assert_raises(KeyError, ids.remove, o1)

        eq_(ids.copy(), ids)

        # explicit __eq__ and __ne__ tests
        assert ids != None  # noqa
        assert not (ids == None)  # noqa

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

        ids.update("foobar")
        try:
            ids |= "foobar"
            assert False
        except TypeError:
            assert True

        try:
            s = {o1, o2}
            s |= ids
            assert False
        except TypeError:
            assert True

        assert_raises(TypeError, util.cmp, ids)
        assert_raises(TypeError, hash, ids)

    def test_repr(self):
        i = util.IdentitySet([])
        eq_(str(i), "IdentitySet([])")
        i = util.IdentitySet([1, 2, 3])
        eq_(str(i), "IdentitySet([1, 2, 3])")


class NoHashIdentitySetTest(IdentitySetTest):
    obj_type = NoHash


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

        a, b, c, d, e, f, g = (
            elem(),
            elem(),
            elem(),
            elem(),
            elem(),
            elem(),
            elem(),
        )

        s1 = util.OrderedIdentitySet([a, b, c])
        s2 = util.OrderedIdentitySet([d, e, f])
        s3 = util.OrderedIdentitySet([a, d, f, g])
        eq_(s1.intersection(s2), [])
        eq_(s1.intersection(s3), [a])
        eq_(s1.union(s2).intersection(s3), [a, d, f])


class DictlikeIteritemsTest(fixtures.TestBase):
    baseline = {("a", 1), ("b", 2), ("c", 3)}

    def _ok(self, instance):
        iterator = util.dictlike_iteritems(instance)
        eq_(set(iterator), self.baseline)

    def _notok(self, instance):
        assert_raises(TypeError, util.dictlike_iteritems, instance)

    def test_dict(self):
        d = dict(a=1, b=2, c=3)
        self._ok(d)

    def test_subdict(self):
        class subdict(dict):
            pass

        d = subdict(a=1, b=2, c=3)
        self._ok(d)

    def test_object(self):
        self._notok(object())

    def test_duck_2(self):
        class duck2:
            def items(duck):
                return list(self.baseline)

        self._ok(duck2())

    def test_duck_4(self):
        class duck4:
            def iterkeys(duck):
                return iter(["a", "b", "c"])

        self._notok(duck4())

    def test_duck_5(self):
        class duck5:
            def keys(duck):
                return ["a", "b", "c"]

            def get(duck, key):
                return dict(a=1, b=2, c=3).get(key)

        self._ok(duck5())

    def test_duck_6(self):
        class duck6:
            def keys(duck):
                return ["a", "b", "c"]

        self._notok(duck6())


class DuckTypeCollectionTest(fixtures.TestBase):
    def test_sets(self):
        class SetLike:
            def add(self):
                pass

        class ForcedSet(list):
            __emulates__ = set

        for type_ in (set, SetLike, ForcedSet):
            eq_(util.duck_type_collection(type_), set)
            instance = type_()
            eq_(util.duck_type_collection(instance), set)

        for type_ in (frozenset,):
            is_(util.duck_type_collection(type_), None)
            instance = type_()
            is_(util.duck_type_collection(instance), None)


class ArgInspectionTest(fixtures.TestBase):
    def test_get_cls_kwargs(self):
        class A:
            def __init__(self, a):
                pass

        class A1(A):
            def __init__(self, a1):
                pass

        class A11(A1):
            def __init__(self, a11, **kw):
                pass

        class B:
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

        class D:
            pass

        class BA2(B, A):
            pass

        class A11B1(A11, B1):
            pass

        def test(cls, *expected):
            eq_(set(util.get_cls_kwargs(cls)), set(expected))

        test(A, "a")
        test(A1, "a1")
        test(A11, "a11", "a1")
        test(B, "b")
        test(B1, "b1", "b")
        test(AB, "ab")
        test(BA, "ba", "b", "a")
        test(BA1, "ba", "b", "a")
        test(CAB, "a")
        test(CBA, "b", "a")
        test(CAB1, "a")
        test(CB1A, "b1", "b", "a")
        test(CB2A, "b2")
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
        test(f2, "foo")
        test(f3)
        test(f4)

    def test_callable_argspec_fn(self):
        def foo(x, y, **kw):
            pass

        eq_(
            get_callable_argspec(foo),
            compat.FullArgSpec(["x", "y"], None, "kw", None, [], None, {}),
        )

    def test_callable_argspec_fn_no_self(self):
        def foo(x, y, **kw):
            pass

        eq_(
            get_callable_argspec(foo, no_self=True),
            compat.FullArgSpec(["x", "y"], None, "kw", None, [], None, {}),
        )

    def test_callable_argspec_fn_no_self_but_self(self):
        def foo(self, x, y, **kw):
            pass

        eq_(
            get_callable_argspec(foo, no_self=True),
            compat.FullArgSpec(
                ["self", "x", "y"], None, "kw", None, [], None, {}
            ),
        )

    @testing.requires.cpython
    def test_callable_argspec_py_builtin(self):
        import datetime

        assert_raises(TypeError, get_callable_argspec, datetime.datetime.now)

    @testing.requires.cpython
    def test_callable_argspec_obj_init(self):
        assert_raises(TypeError, get_callable_argspec, object)

    def test_callable_argspec_method(self):
        class Foo:
            def foo(self, x, y, **kw):
                pass

        eq_(
            get_callable_argspec(Foo.foo),
            compat.FullArgSpec(
                ["self", "x", "y"], None, "kw", None, [], None, {}
            ),
        )

    def test_callable_argspec_instance_method_no_self(self):
        class Foo:
            def foo(self, x, y, **kw):
                pass

        eq_(
            get_callable_argspec(Foo().foo, no_self=True),
            compat.FullArgSpec(["x", "y"], None, "kw", None, [], None, {}),
        )

    def test_callable_argspec_unbound_method_no_self(self):
        class Foo:
            def foo(self, x, y, **kw):
                pass

        eq_(
            get_callable_argspec(Foo.foo, no_self=True),
            compat.FullArgSpec(
                ["self", "x", "y"], None, "kw", None, [], None, {}
            ),
        )

    def test_callable_argspec_init(self):
        class Foo:
            def __init__(self, x, y):
                pass

        eq_(
            get_callable_argspec(Foo),
            compat.FullArgSpec(
                ["self", "x", "y"], None, None, None, [], None, {}
            ),
        )

    def test_callable_argspec_init_no_self(self):
        class Foo:
            def __init__(self, x, y):
                pass

        eq_(
            get_callable_argspec(Foo, no_self=True),
            compat.FullArgSpec(["x", "y"], None, None, None, [], None, {}),
        )

    def test_callable_argspec_call(self):
        class Foo:
            def __call__(self, x, y):
                pass

        eq_(
            get_callable_argspec(Foo()),
            compat.FullArgSpec(
                ["self", "x", "y"], None, None, None, [], None, {}
            ),
        )

    def test_callable_argspec_call_no_self(self):
        class Foo:
            def __call__(self, x, y):
                pass

        eq_(
            get_callable_argspec(Foo(), no_self=True),
            compat.FullArgSpec(["x", "y"], None, None, None, [], None, {}),
        )

    @testing.requires.cpython
    def test_callable_argspec_partial(self):
        from functools import partial

        def foo(x, y, z, **kw):
            pass

        bar = partial(foo, 5)

        assert_raises(TypeError, get_callable_argspec, bar)

    def test_getargspec_6_tuple(self):
        def foo(x, y, z, **kw):
            pass

        spec = compat.inspect_getfullargspec(foo)

        eq_(
            spec,
            compat.FullArgSpec(
                args=["x", "y", "z"],
                varargs=None,
                varkw="kw",
                defaults=None,
                kwonlyargs=[],
                kwonlydefaults=None,
                annotations={},
            ),
        )


class SymbolTest(fixtures.TestBase):
    def test_basic(self):
        sym1 = util.symbol("foo")
        assert sym1.name == "foo"
        sym2 = util.symbol("foo")

        assert sym1 is sym2
        assert sym1 == sym2

        sym3 = util.symbol("bar")
        assert sym1 is not sym3
        assert sym1 != sym3

    def test_fast_int_flag(self):
        class Enum(FastIntFlag):
            fi_sym1 = 1
            fi_sym2 = 2

            fi_sym3 = 3

        assert Enum.fi_sym1 is not Enum.fi_sym3
        assert Enum.fi_sym1 != Enum.fi_sym3

        assert Enum.fi_sym1.name == "fi_sym1"

        # modified for #8783
        eq_(
            list(Enum.__members__.values()),
            [Enum.fi_sym1, Enum.fi_sym2, Enum.fi_sym3],
        )

    def test_fast_int_flag_still_global(self):
        """FastIntFlag still causes elements to be global symbols.

        This is to support pickling.  There are likely other ways to
        achieve this, however this is what we have for now.

        """

        class Enum1(FastIntFlag):
            fi_sym1 = 1
            fi_sym2 = 2

        class Enum2(FastIntFlag):
            fi_sym1 = 1
            fi_sym2 = 2

        # they are global
        assert Enum1.fi_sym1 is Enum2.fi_sym1

    def test_fast_int_flag_dont_allow_conflicts(self):
        """FastIntFlag still causes elements to be global symbols.

        While we do this and haven't yet changed it, make sure conflicting
        int values for the same name don't come in.

        """

        class Enum1(FastIntFlag):
            fi_sym1 = 1
            fi_sym2 = 2

        with expect_raises_message(
            TypeError,
            "Can't replace canonical symbol for 'fi_sym1' "
            "with new int value 2",
        ):

            class Enum2(FastIntFlag):
                fi_sym1 = 2
                fi_sym2 = 3

    @testing.combinations("native", "ours", argnames="native")
    def test_compare_to_native_py_intflag(self, native):
        """monitor IntFlag behavior in upstream Python for #8783"""

        if native == "native":
            from enum import IntFlag
        else:
            from sqlalchemy.util import FastIntFlag as IntFlag

        class Enum(IntFlag):
            fi_sym1 = 1
            fi_sym2 = 2
            fi_sym4 = 4

            fi_sym1plus2 = 3

            # not an alias because there's no 16
            fi_sym17 = 17

        sym1, sym2, sym4, sym1plus2, sym17 = Enum.__members__.values()
        eq_(
            [sym1, sym2, sym4, sym1plus2, sym17],
            [
                Enum.fi_sym1,
                Enum.fi_sym2,
                Enum.fi_sym4,
                Enum.fi_sym1plus2,
                Enum.fi_sym17,
            ],
        )

    def test_pickle(self):
        sym1 = util.symbol("foo")
        sym2 = util.symbol("foo")

        assert sym1 is sym2

        # default
        s = pickle.dumps(sym1)
        pickle.loads(s)

        for _, dumper in picklers():
            serial = dumper(sym1)
            rt = pickle.loads(serial)
            assert rt is sym1
            assert rt is sym2

    def test_bitflags(self):
        sym1 = util.symbol("sym1", canonical=1)
        sym2 = util.symbol("sym2", canonical=2)

        assert sym1 & sym1
        assert not sym1 & sym2
        assert not sym1 & sym1 & sym2

    def test_composites(self):
        sym1 = util.symbol("sym1", canonical=1)
        sym2 = util.symbol("sym2", canonical=2)
        sym3 = util.symbol("sym3", canonical=4)
        sym4 = util.symbol("sym4", canonical=8)

        assert sym1 & (sym2 | sym1 | sym4)
        assert not sym1 & (sym2 | sym3)

        assert not (sym1 | sym2) & (sym3 | sym4)
        assert (sym1 | sym2) & (sym2 | sym4)

    def test_fast_int_flag_no_more_iter(self):
        """test #8783"""

        class MyEnum(FastIntFlag):
            sym1 = 1
            sym2 = 2
            sym3 = 4
            sym4 = 8

        with expect_raises_message(
            NotImplementedError, "iter not implemented to ensure compatibility"
        ):
            list(MyEnum)

    def test_parser(self):
        class MyEnum(FastIntFlag):
            sym1 = 1
            sym2 = 2
            sym3 = 4
            sym4 = 8

        sym1, sym2, sym3, sym4 = tuple(MyEnum.__members__.values())
        lookup_one = {sym1: [], sym2: [True], sym3: [False], sym4: [None]}
        lookup_two = {sym1: [], sym2: [True], sym3: [False]}
        lookup_three = {sym1: [], sym2: ["symbol2"], sym3: []}

        is_(
            langhelpers.parse_user_argument_for_enum(
                "sym2", lookup_one, "some_name", resolve_symbol_names=True
            ),
            sym2,
        )

        assert_raises_message(
            exc.ArgumentError,
            "Invalid value for 'some_name': 'sym2'",
            langhelpers.parse_user_argument_for_enum,
            "sym2",
            lookup_one,
            "some_name",
        )
        is_(
            langhelpers.parse_user_argument_for_enum(
                True, lookup_one, "some_name", resolve_symbol_names=False
            ),
            sym2,
        )

        is_(
            langhelpers.parse_user_argument_for_enum(
                sym2, lookup_one, "some_name"
            ),
            sym2,
        )

        is_(
            langhelpers.parse_user_argument_for_enum(
                None, lookup_one, "some_name"
            ),
            sym4,
        )

        is_(
            langhelpers.parse_user_argument_for_enum(
                None, lookup_two, "some_name"
            ),
            None,
        )

        is_(
            langhelpers.parse_user_argument_for_enum(
                "symbol2", lookup_three, "some_name"
            ),
            sym2,
        )

        assert_raises_message(
            exc.ArgumentError,
            "Invalid value for 'some_name': 'foo'",
            langhelpers.parse_user_argument_for_enum,
            "foo",
            lookup_three,
            "some_name",
        )


class _Py3KFixtures:
    def _kw_only_fixture(self, a, *, b, c):
        pass

    def _kw_plus_posn_fixture(self, a, *args, b, c):
        pass

    def _kw_opt_fixture(self, a, *, b, c="c"):
        pass

    def _ret_annotation_fixture(self, a, b) -> int:
        return 1


py3k_fixtures = _Py3KFixtures()


class TestFormatArgspec(_Py3KFixtures, fixtures.TestBase):
    @testing.combinations(
        (
            lambda: None,
            {
                "grouped_args": "()",
                "self_arg": None,
                "apply_kw": "()",
                "apply_pos": "()",
                "apply_pos_proxied": "()",
                "apply_kw_proxied": "()",
            },
            True,
        ),
        (
            lambda: None,
            {
                "grouped_args": "()",
                "self_arg": None,
                "apply_kw": "",
                "apply_pos": "",
                "apply_pos_proxied": "",
                "apply_kw_proxied": "",
            },
            False,
        ),
        (
            lambda self: None,
            {
                "grouped_args": "(self)",
                "self_arg": "self",
                "apply_kw": "(self)",
                "apply_pos": "(self)",
                "apply_pos_proxied": "()",
                "apply_kw_proxied": "()",
            },
            True,
        ),
        (
            lambda self: None,
            {
                "grouped_args": "(self)",
                "self_arg": "self",
                "apply_kw": "self",
                "apply_pos": "self",
                "apply_pos_proxied": "",
                "apply_kw_proxied": "",
            },
            False,
        ),
        (
            lambda *a: None,
            {
                "grouped_args": "(*a)",
                "self_arg": "a[0]",
                "apply_kw": "(*a)",
                "apply_pos": "(*a)",
                "apply_pos_proxied": "(*a)",
                "apply_kw_proxied": "(*a)",
            },
            True,
        ),
        (
            lambda **kw: None,
            {
                "grouped_args": "(**kw)",
                "self_arg": None,
                "apply_kw": "(**kw)",
                "apply_pos": "(**kw)",
                "apply_pos_proxied": "(**kw)",
                "apply_kw_proxied": "(**kw)",
            },
            True,
        ),
        (
            lambda *a, **kw: None,
            {
                "grouped_args": "(*a, **kw)",
                "self_arg": "a[0]",
                "apply_kw": "(*a, **kw)",
                "apply_pos": "(*a, **kw)",
                "apply_pos_proxied": "(*a, **kw)",
                "apply_kw_proxied": "(*a, **kw)",
            },
            True,
        ),
        (
            lambda a, *b: None,
            {
                "grouped_args": "(a, *b)",
                "self_arg": "a",
                "apply_kw": "(a, *b)",
                "apply_pos": "(a, *b)",
                "apply_pos_proxied": "(*b)",
                "apply_kw_proxied": "(*b)",
            },
            True,
        ),
        (
            lambda a, **b: None,
            {
                "grouped_args": "(a, **b)",
                "self_arg": "a",
                "apply_kw": "(a, **b)",
                "apply_pos": "(a, **b)",
                "apply_pos_proxied": "(**b)",
                "apply_kw_proxied": "(**b)",
            },
            True,
        ),
        (
            lambda a, *b, **c: None,
            {
                "grouped_args": "(a, *b, **c)",
                "self_arg": "a",
                "apply_kw": "(a, *b, **c)",
                "apply_pos": "(a, *b, **c)",
                "apply_pos_proxied": "(*b, **c)",
                "apply_kw_proxied": "(*b, **c)",
            },
            True,
        ),
        (
            lambda a, b=1, **c: None,
            {
                "grouped_args": "(a, b=1, **c)",
                "self_arg": "a",
                "apply_kw": "(a, b=b, **c)",
                "apply_pos": "(a, b, **c)",
                "apply_pos_proxied": "(b, **c)",
                "apply_kw_proxied": "(b=b, **c)",
            },
            True,
        ),
        (
            lambda a=1, b=2: None,
            {
                "grouped_args": "(a=1, b=2)",
                "self_arg": "a",
                "apply_kw": "(a=a, b=b)",
                "apply_pos": "(a, b)",
                "apply_pos_proxied": "(b)",
                "apply_kw_proxied": "(b=b)",
            },
            True,
        ),
        (
            lambda a=1, b=2: None,
            {
                "grouped_args": "(a=1, b=2)",
                "self_arg": "a",
                "apply_kw": "a=a, b=b",
                "apply_pos": "a, b",
                "apply_pos_proxied": "b",
                "apply_kw_proxied": "b=b",
            },
            False,
        ),
        (
            py3k_fixtures._ret_annotation_fixture,
            {
                "grouped_args": "(self, a, b) -> 'int'",
                "self_arg": "self",
                "apply_pos": "self, a, b",
                "apply_kw": "self, a, b",
                "apply_pos_proxied": "a, b",
                "apply_kw_proxied": "a, b",
            },
            False,
        ),
        (
            py3k_fixtures._kw_only_fixture,
            {
                "grouped_args": "(self, a, *, b, c)",
                "self_arg": "self",
                "apply_pos": "self, a, *, b, c",
                "apply_kw": "self, a, b=b, c=c",
                "apply_pos_proxied": "a, *, b, c",
                "apply_kw_proxied": "a, b=b, c=c",
            },
            False,
        ),
        (
            py3k_fixtures._kw_plus_posn_fixture,
            {
                "grouped_args": "(self, a, *args, b, c)",
                "self_arg": "self",
                "apply_pos": "self, a, *args, b, c",
                "apply_kw": "self, a, b=b, c=c, *args",
                "apply_pos_proxied": "a, *args, b, c",
                "apply_kw_proxied": "a, b=b, c=c, *args",
            },
            False,
        ),
        (
            py3k_fixtures._kw_opt_fixture,
            {
                "grouped_args": "(self, a, *, b, c='c')",
                "self_arg": "self",
                "apply_pos": "self, a, *, b, c",
                "apply_kw": "self, a, b=b, c=c",
                "apply_pos_proxied": "a, *, b, c",
                "apply_kw_proxied": "a, b=b, c=c",
            },
            False,
        ),
        argnames="fn,wanted,grouped",
    )
    def test_specs(self, fn, wanted, grouped):
        # test direct function
        if grouped is None:
            parsed = util.format_argspec_plus(fn)
        else:
            parsed = util.format_argspec_plus(fn, grouped=grouped)
        eq_(parsed, wanted)

        # test sending fullargspec
        spec = compat.inspect_getfullargspec(fn)
        if grouped is None:
            parsed = util.format_argspec_plus(spec)
        else:
            parsed = util.format_argspec_plus(spec, grouped=grouped)
        eq_(parsed, wanted)

    @testing.requires.cpython
    def test_init_grouped(self):
        object_spec = {
            "grouped_args": "(self)",
            "self_arg": "self",
            "apply_pos": "(self)",
            "apply_kw": "(self)",
            "apply_pos_proxied": "()",
            "apply_kw_proxied": "()",
        }
        wrapper_spec = {
            "grouped_args": "(self, *args, **kwargs)",
            "self_arg": "self",
            "apply_pos": "(self, *args, **kwargs)",
            "apply_kw": "(self, *args, **kwargs)",
            "apply_pos_proxied": "(*args, **kwargs)",
            "apply_kw_proxied": "(*args, **kwargs)",
        }
        custom_spec = {
            "grouped_args": "(slef, a=123)",
            "self_arg": "slef",  # yes, slef
            "apply_pos": "(slef, a)",
            "apply_pos_proxied": "(a)",
            "apply_kw_proxied": "(a=a)",
            "apply_kw": "(slef, a=a)",
        }

        self._test_init(None, object_spec, wrapper_spec, custom_spec)
        self._test_init(True, object_spec, wrapper_spec, custom_spec)

    @testing.requires.cpython
    def test_init_bare(self):
        object_spec = {
            "grouped_args": "(self)",
            "self_arg": "self",
            "apply_pos": "self",
            "apply_kw": "self",
            "apply_pos_proxied": "",
            "apply_kw_proxied": "",
        }
        wrapper_spec = {
            "grouped_args": "(self, *args, **kwargs)",
            "self_arg": "self",
            "apply_pos": "self, *args, **kwargs",
            "apply_kw": "self, *args, **kwargs",
            "apply_pos_proxied": "*args, **kwargs",
            "apply_kw_proxied": "*args, **kwargs",
        }
        custom_spec = {
            "grouped_args": "(slef, a=123)",
            "self_arg": "slef",  # yes, slef
            "apply_pos": "slef, a",
            "apply_kw": "slef, a=a",
            "apply_pos_proxied": "a",
            "apply_kw_proxied": "a=a",
        }

        self._test_init(False, object_spec, wrapper_spec, custom_spec)

    def _test_init(self, grouped, object_spec, wrapper_spec, custom_spec):
        def test(fn, wanted):
            if grouped is None:
                parsed = util.format_argspec_init(fn)
            else:
                parsed = util.format_argspec_init(fn, grouped=grouped)
            eq_(parsed, wanted)

        class Obj:
            pass

        test(Obj.__init__, object_spec)

        class Obj:
            def __init__(self):
                pass

        test(Obj.__init__, object_spec)

        class Obj:
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
        class Foo:
            def __init__(self, a, b, c):
                self.a = a
                self.b = b
                self.c = c

        eq_(util.generic_repr(Foo(1, 2, 3)), "Foo(1, 2, 3)")

    def test_positional_plus_kw(self):
        class Foo:
            def __init__(self, a, b, c=5, d=4):
                self.a = a
                self.b = b
                self.c = c
                self.d = d

        eq_(util.generic_repr(Foo(1, 2, 3, 6)), "Foo(1, 2, c=3, d=6)")

    def test_kw_defaults(self):
        class Foo:
            def __init__(self, a=1, b=2, c=3, d=4):
                self.a = a
                self.b = b
                self.c = c
                self.d = d

        eq_(util.generic_repr(Foo(1, 5, 3, 7)), "Foo(b=5, d=7)")

    def test_multi_kw(self):
        class Foo:
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
                super().__init__(**kw)

        eq_(
            util.generic_repr(
                Bar("e", "f", g=7, a=6, b=5, d=9), to_inspect=[Bar, Foo]
            ),
            "Bar('e', 'f', g=7, a=6, b=5, d=9)",
        )

        eq_(
            util.generic_repr(Bar("e", "f", a=6, b=5), to_inspect=[Bar, Foo]),
            "Bar('e', 'f', a=6, b=5)",
        )

    def test_multi_kw_repeated(self):
        class Foo:
            def __init__(self, a=1, b=2):
                self.a = a
                self.b = b

        class Bar(Foo):
            def __init__(self, b=3, c=4, **kw):
                self.c = c
                super().__init__(b=b, **kw)

        eq_(
            util.generic_repr(Bar(a="a", b="b", c="c"), to_inspect=[Bar, Foo]),
            "Bar(b='b', c='c', a='a')",
        )

    def test_discard_vargs(self):
        class Foo:
            def __init__(self, a, b, *args):
                self.a = a
                self.b = b
                self.c, self.d = args[0:2]

        eq_(util.generic_repr(Foo(1, 2, 3, 4)), "Foo(1, 2)")

    def test_discard_vargs_kwargs(self):
        class Foo:
            def __init__(self, a, b, *args, **kw):
                self.a = a
                self.b = b
                self.c, self.d = args[0:2]

        eq_(util.generic_repr(Foo(1, 2, 3, 4, x=7, y=4)), "Foo(1, 2)")

    def test_significant_vargs(self):
        class Foo:
            def __init__(self, a, b, *args):
                self.a = a
                self.b = b
                self.args = args

        eq_(util.generic_repr(Foo(1, 2, 3, 4)), "Foo(1, 2, 3, 4)")

    def test_no_args(self):
        class Foo:
            def __init__(self):
                pass

        eq_(util.generic_repr(Foo()), "Foo()")

    def test_no_init(self):
        class Foo:
            pass

        eq_(util.generic_repr(Foo()), "Foo()")


class AsInterfaceTest(fixtures.TestBase):
    class Something:
        def _ignoreme(self):
            pass

        def foo(self):
            pass

        def bar(self):
            pass

    class Partial:
        def bar(self):
            pass

    class Object:
        pass

    def test_no_cls_no_methods(self):
        obj = object()
        assert_raises(TypeError, util.as_interface, obj)

    def test_instance(self):
        obj = object()
        assert_raises(TypeError, util.as_interface, obj, cls=self.Something)

        assert_raises(TypeError, util.as_interface, obj, methods=("foo"))

        assert_raises(
            TypeError,
            util.as_interface,
            obj,
            cls=self.Something,
            required=("foo"),
        )

        obj = self.Something()
        eq_(obj, util.as_interface(obj, cls=self.Something))
        eq_(obj, util.as_interface(obj, methods=("foo",)))
        eq_(
            obj,
            util.as_interface(
                obj, cls=self.Something, required=("outofband",)
            ),
        )
        partial = self.Partial()

        slotted = self.Object()
        slotted.bar = lambda self: 123

        for obj in partial, slotted:
            eq_(obj, util.as_interface(obj, cls=self.Something))
            assert_raises(TypeError, util.as_interface, obj, methods=("foo"))
            eq_(obj, util.as_interface(obj, methods=("bar",)))
            eq_(
                obj,
                util.as_interface(obj, cls=self.Something, required=("bar",)),
            )
            assert_raises(
                TypeError,
                util.as_interface,
                obj,
                cls=self.Something,
                required=("foo",),
            )

            assert_raises(
                TypeError,
                util.as_interface,
                obj,
                cls=self.Something,
                required=self.Something,
            )

    def test_dict(self):
        obj = {}
        assert_raises(TypeError, util.as_interface, obj, cls=self.Something)
        assert_raises(TypeError, util.as_interface, obj, methods="foo")
        assert_raises(
            TypeError,
            util.as_interface,
            obj,
            cls=self.Something,
            required="foo",
        )

        def assertAdapted(obj, *methods):
            assert isinstance(obj, type)
            found = {m for m in dir(obj) if not m.startswith("_")}
            for method in methods:
                assert method in found
                found.remove(method)
            assert not found

        def fn(self):
            return 123

        obj = {"foo": fn, "bar": fn}
        res = util.as_interface(obj, cls=self.Something)
        assertAdapted(res, "foo", "bar")
        res = util.as_interface(
            obj, cls=self.Something, required=self.Something
        )
        assertAdapted(res, "foo", "bar")
        res = util.as_interface(obj, cls=self.Something, required=("foo",))
        assertAdapted(res, "foo", "bar")
        res = util.as_interface(obj, methods=("foo", "bar"))
        assertAdapted(res, "foo", "bar")
        res = util.as_interface(obj, methods=("foo", "bar", "baz"))
        assertAdapted(res, "foo", "bar")
        res = util.as_interface(obj, methods=("foo", "bar"), required=("foo",))
        assertAdapted(res, "foo", "bar")
        assert_raises(TypeError, util.as_interface, obj, methods=("foo",))
        assert_raises(
            TypeError,
            util.as_interface,
            obj,
            methods=("foo", "bar", "baz"),
            required=("baz",),
        )
        obj = {"foo": 123}
        assert_raises(TypeError, util.as_interface, obj, cls=self.Something)


class TestClassHierarchy(fixtures.TestBase):
    def test_object(self):
        eq_(set(util.class_hierarchy(object)), {object})

    def test_single(self):
        class A:
            pass

        class B:
            pass

        eq_(set(util.class_hierarchy(A)), {A, object})
        eq_(set(util.class_hierarchy(B)), {B, object})

        class C(A, B):
            pass

        eq_(set(util.class_hierarchy(A)), {A, B, C, object})
        eq_(set(util.class_hierarchy(B)), {A, B, C, object})


class TestClassProperty(fixtures.TestBase):
    def test_simple(self):
        class A:
            something = {"foo": 1}

        class B(A):
            @classproperty
            def something(cls):
                d = dict(super().something)
                d.update({"bazz": 2})
                return d

        eq_(B.something, {"foo": 1, "bazz": 2})


class TestProperties(fixtures.TestBase):
    def test_pickle(self):
        data = {"hello": "bla"}
        props = util.Properties(data)

        for loader, dumper in picklers():
            s = dumper(props)
            p = loader(s)

            eq_(props._data, p._data)
            eq_(props.keys(), p.keys())

    def test_keys_in_dir(self):
        data = {"hello": "bla"}
        props = util.Properties(data)
        in_("hello", dir(props))

    def test_pickle_immuatbleprops(self):
        data = {"hello": "bla"}
        props = util.Properties(data).as_readonly()

        for loader, dumper in picklers():
            s = dumper(props)
            p = loader(s)

            eq_(props._data, p._data)
            eq_(props.keys(), p.keys())

    def test_pickle_orderedprops(self):
        data = {"hello": "bla"}
        props = util.OrderedProperties()
        props.update(data)

        for loader, dumper in picklers():
            s = dumper(props)
            p = loader(s)

            eq_(props._data, p._data)
            eq_(props.keys(), p.keys())


class QuotedTokenParserTest(fixtures.TestBase):
    def _test(self, string, expected):
        eq_(langhelpers.quoted_token_parser(string), expected)

    def test_single(self):
        self._test("name", ["name"])

    def test_dotted(self):
        self._test("schema.name", ["schema", "name"])

    def test_dotted_quoted_left(self):
        self._test('"Schema".name', ["Schema", "name"])

    def test_dotted_quoted_left_w_quote_left_edge(self):
        self._test('"""Schema".name', ['"Schema', "name"])

    def test_dotted_quoted_left_w_quote_right_edge(self):
        self._test('"Schema""".name', ['Schema"', "name"])

    def test_dotted_quoted_left_w_quote_middle(self):
        self._test('"Sch""ema".name', ['Sch"ema', "name"])

    def test_dotted_quoted_right(self):
        self._test('schema."SomeName"', ["schema", "SomeName"])

    def test_dotted_quoted_right_w_quote_left_edge(self):
        self._test('schema."""name"', ["schema", '"name'])

    def test_dotted_quoted_right_w_quote_right_edge(self):
        self._test('schema."name"""', ["schema", 'name"'])

    def test_dotted_quoted_right_w_quote_middle(self):
        self._test('schema."na""me"', ["schema", 'na"me'])

    def test_quoted_single_w_quote_left_edge(self):
        self._test('"""name"', ['"name'])

    def test_quoted_single_w_quote_right_edge(self):
        self._test('"name"""', ['name"'])

    def test_quoted_single_w_quote_middle(self):
        self._test('"na""me"', ['na"me'])

    def test_dotted_quoted_left_w_dot_left_edge(self):
        self._test('".Schema".name', [".Schema", "name"])

    def test_dotted_quoted_left_w_dot_right_edge(self):
        self._test('"Schema.".name', ["Schema.", "name"])

    def test_dotted_quoted_left_w_dot_middle(self):
        self._test('"Sch.ema".name', ["Sch.ema", "name"])

    def test_dotted_quoted_right_w_dot_left_edge(self):
        self._test('schema.".name"', ["schema", ".name"])

    def test_dotted_quoted_right_w_dot_right_edge(self):
        self._test('schema."name."', ["schema", "name."])

    def test_dotted_quoted_right_w_dot_middle(self):
        self._test('schema."na.me"', ["schema", "na.me"])

    def test_quoted_single_w_dot_left_edge(self):
        self._test('".name"', [".name"])

    def test_quoted_single_w_dot_right_edge(self):
        self._test('"name."', ["name."])

    def test_quoted_single_w_dot_middle(self):
        self._test('"na.me"', ["na.me"])


class BackslashReplaceTest(fixtures.TestBase):
    def test_ascii_to_utf8(self):
        eq_(
            compat.decode_backslashreplace(util.b("hello world"), "utf-8"),
            "hello world",
        )

    def test_utf8_to_utf8(self):
        eq_(
            compat.decode_backslashreplace(
                "some message méil".encode(), "utf-8"
            ),
            "some message méil",
        )

    def test_latin1_to_utf8(self):
        eq_(
            compat.decode_backslashreplace(
                "some message méil".encode("latin-1"), "utf-8"
            ),
            "some message m\\xe9il",
        )

        eq_(
            compat.decode_backslashreplace(
                "some message méil".encode("latin-1"), "latin-1"
            ),
            "some message méil",
        )

    def test_cp1251_to_utf8(self):
        message = "some message П".encode("cp1251")
        eq_(message, b"some message \xcf")
        eq_(
            compat.decode_backslashreplace(message, "utf-8"),
            "some message \\xcf",
        )

        eq_(
            compat.decode_backslashreplace(message, "cp1251"),
            "some message П",
        )


class TestModuleRegistry(fixtures.TestBase):
    def test_modules_are_loaded(self):
        to_restore = []
        for m in ("xml.dom", "wsgiref.simple_server"):
            to_restore.append((m, sys.modules.pop(m, None)))
        try:
            mr = preloaded._ModuleRegistry()

            ret = mr.preload_module(
                "xml.dom", "wsgiref.simple_server", "sqlalchemy.sql.util"
            )
            o = object()
            is_(ret(o), o)

            is_false(hasattr(mr, "xml_dom"))
            mr.import_prefix("xml")
            is_true("xml.dom" in sys.modules)
            is_(sys.modules["xml.dom"], mr.xml_dom)

            is_true("wsgiref.simple_server" not in sys.modules)
            mr.import_prefix("wsgiref")
            is_true("wsgiref.simple_server" in sys.modules)
            is_(sys.modules["wsgiref.simple_server"], mr.wsgiref_simple_server)

            mr.import_prefix("sqlalchemy")
            is_(sys.modules["sqlalchemy.sql.util"], mr.sql_util)
        finally:
            for name, mod in to_restore:
                if mod is not None:
                    sys.modules[name] = mod


class MethodOveriddenTest(fixtures.TestBase):
    def test_subclass_overrides_cls_given(self):
        class Foo:
            def bar(self):
                pass

        class Bar(Foo):
            def bar(self):
                pass

        is_true(util.method_is_overridden(Bar, Foo.bar))

    def test_subclass_overrides(self):
        class Foo:
            def bar(self):
                pass

        class Bar(Foo):
            def bar(self):
                pass

        is_true(util.method_is_overridden(Bar(), Foo.bar))

    def test_subclass_overrides_skiplevel(self):
        class Foo:
            def bar(self):
                pass

        class Bar(Foo):
            pass

        class Bat(Bar):
            def bar(self):
                pass

        is_true(util.method_is_overridden(Bat(), Foo.bar))

    def test_subclass_overrides_twolevels(self):
        class Foo:
            def bar(self):
                pass

        class Bar(Foo):
            def bar(self):
                pass

        class Bat(Bar):
            pass

        is_true(util.method_is_overridden(Bat(), Foo.bar))

    def test_subclass_doesnt_override_cls_given(self):
        class Foo:
            def bar(self):
                pass

        class Bar(Foo):
            pass

        is_false(util.method_is_overridden(Bar, Foo.bar))

    def test_subclass_doesnt_override(self):
        class Foo:
            def bar(self):
                pass

        class Bar(Foo):
            pass

        is_false(util.method_is_overridden(Bar(), Foo.bar))

    def test_subclass_overrides_multi_mro(self):
        class Base:
            pass

        class Foo:
            pass

        class Bat(Base):
            def bar(self):
                pass

        class HoHo(Foo, Bat):
            def bar(self):
                pass

        is_true(util.method_is_overridden(HoHo(), Bat.bar))


class CyExtensionTest(fixtures.TestBase):
    __requires__ = ("cextensions",)

    def test_all_cyext_imported(self):
        ext = _all_cython_modules()
        lib_folder = (Path(__file__).parent / ".." / ".." / "lib").resolve()
        sa_folder = lib_folder / "sqlalchemy"
        cython_files = [f.resolve() for f in sa_folder.glob("**/*_cy.py")]
        eq_(len(ext), len(cython_files))
        names = {
            ".".join(f.relative_to(lib_folder).parts).replace(".py", "")
            for f in cython_files
        }
        eq_({m.__name__ for m in ext}, set(names))

    @testing.combinations(*_all_cython_modules())
    def test_load_uncompiled_module(self, module):
        is_true(module._is_compiled())
        py_module = langhelpers.load_uncompiled_module(module)
        is_false(py_module._is_compiled())
        eq_(py_module.__name__, module.__name__)
        eq_(py_module.__package__, module.__package__)

    def test_setup_defines_all_files(self):
        try:
            import setuptools  # noqa: F401
        except ImportError:
            testing.skip_test("setuptools is required")
        with (
            mock.patch("setuptools.setup", mock.MagicMock()),
            mock.patch.dict(
                "os.environ",
                {"DISABLE_SQLALCHEMY_CEXT": "", "REQUIRE_SQLALCHEMY_CEXT": ""},
            ),
        ):
            import setup

            setup_modules = {f"sqlalchemy.{m}" for m in setup.CYTHON_MODULES}
            expected = {e.__name__ for e in _all_cython_modules()}
            print(expected)
            print(setup_modules)
            eq_(setup_modules, expected)
