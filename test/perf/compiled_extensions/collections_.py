from secrets import token_urlsafe
from textwrap import wrap

from sqlalchemy.util.langhelpers import load_uncompiled_module
from .base import Case
from .base import test_case


class ImmutableDict(Case):
    @staticmethod
    def python():
        from sqlalchemy.util import _immutabledict_cy

        py_immutabledict = load_uncompiled_module(_immutabledict_cy)
        assert not py_immutabledict._is_compiled()
        return py_immutabledict.immutabledict

    @staticmethod
    def cython():
        from sqlalchemy.util import _immutabledict_cy

        assert _immutabledict_cy._is_compiled()
        return _immutabledict_cy.immutabledict

    IMPLEMENTATIONS = {
        "python": python.__func__,
        "cython": cython.__func__,
    }

    def init_objects(self):
        self.small = {"a": 5, "b": 4}
        self.large = {f"k{i}": f"v{i}" for i in range(50)}
        self.empty = self.impl()
        self.d1 = self.impl({"x": 5, "y": 4})
        self.d2 = self.impl({f"key{i}": f"value{i}" for i in range(50)})

    @classmethod
    def update_results(cls, results):
        cls._divide_results(results, "c", "python", "c / py")
        cls._divide_results(results, "cython", "python", "cy / py")
        cls._divide_results(results, "cython", "c", "cy / c")

    @test_case
    def init_empty(self):
        self.impl()

    @test_case
    def init_kw(self):
        self.impl(a=1, b=2)

    @test_case
    def init(self):
        self.impl(self.small)

    @test_case
    def init_large(self):
        self.impl(self.large)

    @test_case
    def len(self):
        len(self.d1) + len(self.d2)

    @test_case
    def getitem(self):
        self.d1["x"]
        self.d2["key42"]

    @test_case
    def union(self):
        self.d1.union(self.small)
        self.d1.union(self.small.items())

    @test_case
    def union_large(self):
        self.d2.union(self.large)

    @test_case
    def union_imm(self):
        self.empty.union(self.d1)
        self.d1.union(self.d2)
        self.d1.union(self.empty)

    @test_case
    def merge_with(self):
        self.d1.merge_with(self.small)
        self.d1.merge_with(self.small.items())

    @test_case
    def merge_with_large(self):
        self.d2.merge_with(self.large)

    @test_case
    def merge_with_imm(self):
        self.d1.merge_with(self.d2)
        self.empty.merge_with(self.d1)
        self.empty.merge_with(self.d1, self.d2)

    @test_case
    def merge_with_only_one(self):
        self.d1.merge_with(self.empty, None, self.empty)
        self.empty.merge_with(self.empty, self.d1, self.empty)

    @test_case
    def merge_with_many(self):
        self.d1.merge_with(self.d2, self.small, None, self.small, self.large)

    @test_case
    def get(self):
        self.d1.get("x")
        self.d2.get("key42")

    @test_case
    def get_miss(self):
        self.d1.get("xxx")
        self.d2.get("xxx")

    @test_case
    def keys(self):
        self.d1.keys()
        self.d2.keys()

    @test_case
    def items(self):
        self.d1.items()
        self.d2.items()

    @test_case
    def values(self):
        self.d1.values()
        self.d2.values()

    @test_case
    def iter(self):
        list(self.d1)
        list(self.d2)

    @test_case
    def in_case(self):
        "x" in self.d1
        "key42" in self.d1

    @test_case
    def in_miss(self):
        "xx" in self.d1
        "xx" in self.d1

    @test_case
    def eq(self):
        self.d1 == self.d1
        self.d2 == self.d2

    @test_case
    def eq_dict(self):
        self.d1 == dict(self.d1)
        self.d2 == dict(self.d2)

    @test_case
    def eq_other(self):
        self.d1 == self.d2
        self.d1 == "foo"

    @test_case
    def ne(self):
        self.d1 != self.d1
        self.d2 != self.d2

    @test_case
    def ne_dict(self):
        self.d1 != dict(self.d1)
        self.d2 != dict(self.d2)

    @test_case
    def ne_other(self):
        self.d1 != self.d2
        self.d1 != "foo"


class IdentitySet(Case):
    @staticmethod
    def set_fn():
        return set

    @staticmethod
    def python():
        from sqlalchemy.util import _collections_cy

        py_coll = load_uncompiled_module(_collections_cy)
        assert not py_coll._is_compiled()
        return py_coll.IdentitySet

    @staticmethod
    def cython():
        from sqlalchemy.util import _collections_cy

        assert _collections_cy._is_compiled()
        return _collections_cy.IdentitySet

    IMPLEMENTATIONS = {
        "set": set_fn.__func__,
        "python": python.__func__,
        "cython": cython.__func__,
    }
    NUMBER = 10

    def init_objects(self):
        self.val1 = list(range(10))
        self.val2 = list(wrap(token_urlsafe(4 * 2048), 4))
        self.imp_1 = self.impl(self.val1)
        self.imp_2 = self.impl(self.val2)

    @classmethod
    def update_results(cls, results):
        cls._divide_results(results, "python", "set", "py / set")
        cls._divide_results(results, "cython", "python", "cy / py")
        cls._divide_results(results, "cython", "set", "cy / set")

    @test_case(number=2_500_000)
    def init_empty(self):
        self.impl()

    @test_case(number=2_500)
    def init(self):
        self.impl(self.val1)
        self.impl(self.val2)

    @test_case(number=5_000)
    def init_from_impl(self):
        self.impl(self.imp_2)

    @test_case(number=100)
    def add(self):
        ii = self.impl()
        x = 25_000
        for i in range(x):
            ii.add(str(i % (x / 2)))

    @test_case
    def contains(self):
        ii = self.impl(self.val2)
        for _ in range(1_000):
            for x in self.val1 + self.val2:
                x in ii

    @test_case(number=200)
    def remove(self):
        v = [str(i) for i in range(7500)]
        ii = self.impl(v)
        for x in v[:5000]:
            ii.remove(x)

    @test_case(number=200)
    def discard(self):
        v = [str(i) for i in range(7500)]
        ii = self.impl(v)
        for x in v[:5000]:
            ii.discard(x)

    @test_case
    def pop(self):
        for x in range(50_000):
            ii = self.impl(self.val1)
            for x in self.val1:
                ii.pop()

    @test_case
    def clear(self):
        i, v = self.impl, self.val1
        for _ in range(125_000):
            ii = i(v)
            ii.clear()

    @test_case(number=2_500_000)
    def eq(self):
        self.imp_1 == self.imp_1
        self.imp_1 == self.imp_2
        self.imp_1 == self.val2

    @test_case(number=2_500_000)
    def ne(self):
        self.imp_1 != self.imp_1
        self.imp_1 != self.imp_2
        self.imp_1 != self.val2

    @test_case(number=20_000)
    def issubset(self):
        self.imp_1.issubset(self.imp_1)
        self.imp_1.issubset(self.imp_2)
        self.imp_1.issubset(self.val1)
        self.imp_1.issubset(self.val2)

    @test_case(number=50_000)
    def le(self):
        self.imp_1 <= self.imp_1
        self.imp_1 <= self.imp_2
        self.imp_2 <= self.imp_1
        self.imp_2 <= self.imp_2

    @test_case(number=2_500_000)
    def lt(self):
        self.imp_1 < self.imp_1
        self.imp_1 < self.imp_2
        self.imp_2 < self.imp_1
        self.imp_2 < self.imp_2

    @test_case(number=20_000)
    def issuperset(self):
        self.imp_1.issuperset(self.imp_1)
        self.imp_1.issuperset(self.imp_2)
        self.imp_1.issubset(self.val1)
        self.imp_1.issubset(self.val2)

    @test_case(number=50_000)
    def ge(self):
        self.imp_1 >= self.imp_1
        self.imp_1 >= self.imp_2
        self.imp_2 >= self.imp_1
        self.imp_2 >= self.imp_2

    @test_case(number=2_500_000)
    def gt(self):
        self.imp_1 > self.imp_1
        self.imp_2 > self.imp_2
        self.imp_2 > self.imp_1
        self.imp_2 > self.imp_2

    @test_case(number=10_000)
    def union(self):
        self.imp_1.union(self.imp_2)

    @test_case(number=10_000)
    def or_test(self):
        self.imp_1 | self.imp_2

    @test_case
    def update(self):
        ii = self.impl(self.val1)
        for _ in range(1_000):
            ii.update(self.imp_2)

    @test_case
    def ior(self):
        ii = self.impl(self.val1)
        for _ in range(1_000):
            ii |= self.imp_2

    @test_case
    def difference(self):
        for _ in range(2_500):
            self.imp_1.difference(self.imp_2)
            self.imp_1.difference(self.val2)

    @test_case(number=250_000)
    def sub(self):
        self.imp_1 - self.imp_2

    @test_case
    def difference_update(self):
        ii = self.impl(self.val1)
        for _ in range(2_500):
            ii.difference_update(self.imp_2)
            ii.difference_update(self.val2)

    @test_case
    def isub(self):
        ii = self.impl(self.val1)
        for _ in range(250_000):
            ii -= self.imp_2

    @test_case(number=20_000)
    def intersection(self):
        self.imp_1.intersection(self.imp_2)
        self.imp_1.intersection(self.val2)

    @test_case(number=250_000)
    def and_test(self):
        self.imp_1 & self.imp_2

    @test_case
    def intersection_up(self):
        ii = self.impl(self.val1)
        for _ in range(2_500):
            ii.intersection_update(self.imp_2)
            ii.intersection_update(self.val2)

    @test_case
    def iand(self):
        ii = self.impl(self.val1)
        for _ in range(250_000):
            ii &= self.imp_2

    @test_case(number=2_500)
    def symmetric_diff(self):
        self.imp_1.symmetric_difference(self.imp_2)
        self.imp_1.symmetric_difference(self.val2)

    @test_case(number=2_500)
    def xor(self):
        self.imp_1 ^ self.imp_2

    @test_case
    def symmetric_diff_up(self):
        ii = self.impl(self.val1)
        for _ in range(125):
            ii.symmetric_difference_update(self.imp_2)
            ii.symmetric_difference_update(self.val2)

    @test_case
    def ixor(self):
        ii = self.impl(self.val1)
        for _ in range(250):
            ii ^= self.imp_2

    @test_case(number=25_000)
    def copy(self):
        self.imp_1.copy()
        self.imp_2.copy()

    @test_case(number=2_500_000)
    def len(self):
        len(self.imp_1)
        len(self.imp_2)

    @test_case(number=25_000)
    def iter(self):
        list(self.imp_1)
        list(self.imp_2)

    @test_case(number=10_000)
    def repr(self):
        str(self.imp_1)
        str(self.imp_2)


class OrderedSet(IdentitySet):
    @staticmethod
    def set_fn():
        return set

    @staticmethod
    def python():
        from sqlalchemy.util import _collections_cy

        py_coll = load_uncompiled_module(_collections_cy)
        assert not py_coll._is_compiled()
        return py_coll.OrderedSet

    @staticmethod
    def cython():
        from sqlalchemy.util import _collections_cy

        assert _collections_cy._is_compiled()
        return _collections_cy.OrderedSet

    @staticmethod
    def ordered_lib():
        from orderedset import OrderedSet

        return OrderedSet

    IMPLEMENTATIONS = {
        "set": set_fn.__func__,
        "python": python.__func__,
        "cython": cython.__func__,
        "ordsetlib": ordered_lib.__func__,
    }

    @classmethod
    def update_results(cls, results):
        super().update_results(results)
        cls._divide_results(results, "ordsetlib", "set", "ordlib/set")
        cls._divide_results(results, "cython", "ordsetlib", "cy / ordlib")

    @test_case
    def add_op(self):
        ii = self.impl(self.val1)
        v2 = self.impl(self.val2)
        for _ in range(500):
            ii + v2

    @test_case
    def getitem(self):
        ii = self.impl(self.val1)
        for _ in range(250_000):
            for i in range(len(self.val1)):
                ii[i]

    @test_case
    def insert(self):
        for _ in range(5):
            ii = self.impl(self.val1)
            for i in range(5_000):
                ii.insert(i // 2, i)
                ii.insert(-i % 2, i)


class UniqueList(Case):
    @staticmethod
    def python():
        from sqlalchemy.util import _collections_cy

        py_coll = load_uncompiled_module(_collections_cy)
        assert not py_coll._is_compiled()
        return py_coll.unique_list

    @staticmethod
    def cython():
        from sqlalchemy.util import _collections_cy

        assert _collections_cy._is_compiled()
        return _collections_cy.unique_list

    IMPLEMENTATIONS = {
        "python": python.__func__,
        "cython": cython.__func__,
    }

    @classmethod
    def update_results(cls, results):
        cls._divide_results(results, "cython", "python", "cy / py")

    def init_objects(self):
        self.int_small = list(range(10))
        self.int_vlarge = list(range(25_000)) * 2
        d = wrap(token_urlsafe(100 * 2048), 4)
        assert len(d) > 50_000
        self.vlarge = d[:50_000]
        self.large = d[:500]
        self.small = d[:15]

    @test_case
    def small_str(self):
        self.impl(self.small)

    @test_case(number=50_000)
    def large_str(self):
        self.impl(self.large)

    @test_case(number=250)
    def vlarge_str(self):
        self.impl(self.vlarge)

    @test_case
    def small_range(self):
        self.impl(range(10))

    @test_case
    def small_int(self):
        self.impl(self.int_small)

    @test_case(number=25_000)
    def large_int(self):
        self.impl([1, 1, 1, 2, 3] * 100)
        self.impl(range(1000))

    @test_case(number=250)
    def vlarge_int(self):
        self.impl(self.int_vlarge)
