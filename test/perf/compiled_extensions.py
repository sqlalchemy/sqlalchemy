from collections import defaultdict
from decimal import Decimal
import re
from secrets import token_urlsafe
from textwrap import wrap
from timeit import timeit
from types import MappingProxyType

from sqlalchemy import bindparam
from sqlalchemy import column
from sqlalchemy.util.langhelpers import load_uncompiled_module


def test_case(fn=None, *, number=None):
    def wrap(fn):
        fn.__test_case__ = True
        if number is not None:
            fn.__number__ = number
        return fn

    if fn is None:
        return wrap
    else:
        return wrap(fn)


class Case:
    """Base test case. Mark test cases with ``test_case``"""

    IMPLEMENTATIONS = {}
    "Keys are the impl name, values are callable to load it"
    NUMBER = 1_000_000

    _CASES = []

    def __init__(self, impl):
        self.impl = impl
        self.init_objects()

    def __init_subclass__(cls):
        if not cls.__name__.startswith("_"):
            Case._CASES.append(cls)

    def init_objects(self):
        pass

    @classmethod
    def _load(cls, fn):
        try:
            return fn()
        except Exception as e:
            print(f"Error loading {fn}: {e!r}")

    @classmethod
    def import_object(cls):
        impl = []
        for name, fn in cls.IMPLEMENTATIONS.items():
            obj = cls._load(fn)
            if obj:
                impl.append((name, obj))
        return impl

    @classmethod
    def _divide_results(cls, results, num, div, name):
        "utility method to create ratios of two implementation"
        if div in results and num in results:
            results[name] = {
                m: results[num][m] / results[div][m] for m in results[div]
            }

    @classmethod
    def update_results(cls, results):
        pass

    @classmethod
    def run_case(cls, factor, filter_):
        objects = cls.import_object()
        number = max(1, int(cls.NUMBER * factor))

        stack = [c for c in cls.mro() if c not in {object, Case}]
        methods = []
        while stack:
            curr = stack.pop(0)
            # dict keeps the definition order, dir is instead sorted
            methods += [
                m
                for m, fn in curr.__dict__.items()
                if hasattr(fn, "__test_case__")
            ]

        if filter_:
            methods = [m for m in methods if re.search(filter_, m)]

        results = defaultdict(dict)
        for name, impl in objects:
            print(f"Running {name:<10} ", end="", flush=True)
            impl_case = cls(impl)
            fails = []
            for m in methods:
                call = getattr(impl_case, m)
                try:
                    t_num = number
                    fn_num = getattr(call, "__number__", None)
                    if fn_num is not None:
                        t_num = max(1, int(fn_num * factor))
                    value = timeit(call, number=t_num)
                    print(".", end="", flush=True)
                except Exception as e:
                    fails.append(f"{name}::{m} error: {e}")
                    print("x", end="", flush=True)
                    value = float("nan")

                results[name][m] = value
            print(" Done")
            for f in fails:
                print("\t", f)

        cls.update_results(results)
        return results


class ImmutableDict(Case):
    @staticmethod
    def python():
        from sqlalchemy.util import _immutabledict_cy

        py_immutabledict = load_uncompiled_module(_immutabledict_cy)
        assert not py_immutabledict._is_compiled()
        return py_immutabledict.immutabledict

    @staticmethod
    def c():
        from sqlalchemy.cimmutabledict import immutabledict

        return immutabledict

    @staticmethod
    def cython():
        from sqlalchemy.util import _immutabledict_cy

        assert _immutabledict_cy._is_compiled()
        return _immutabledict_cy.immutabledict

    IMPLEMENTATIONS = {
        "python": python.__func__,
        "c": c.__func__,
        "cython": cython.__func__,
    }

    def init_objects(self):
        self.small = {"a": 5, "b": 4}
        self.large = {f"k{i}": f"v{i}" for i in range(50)}
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
    def merge_with(self):
        self.d1.merge_with(self.small)
        self.d1.merge_with(self.small.items())

    @test_case
    def merge_with_large(self):
        self.d2.merge_with(self.large)

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


class Processors(Case):
    @staticmethod
    def python():
        from sqlalchemy.engine import _processors_cy

        py_processors = load_uncompiled_module(_processors_cy)
        assert not py_processors._is_compiled()
        return py_processors

    @staticmethod
    def c():
        from sqlalchemy import cprocessors as mod

        mod.to_decimal_processor_factory = (
            lambda t, s: mod.DecimalResultProcessor(t, "%%.%df" % s).process
        )

        return mod

    @staticmethod
    def cython():
        from sqlalchemy.engine import _processors_cy

        assert _processors_cy._is_compiled()
        return _processors_cy

    IMPLEMENTATIONS = {
        "python": python.__func__,
        "c": c.__func__,
        "cython": cython.__func__,
    }
    NUMBER = 500_000

    def init_objects(self):
        self.to_dec = self.impl.to_decimal_processor_factory(Decimal, 3)

    @classmethod
    def update_results(cls, results):
        cls._divide_results(results, "c", "python", "c / py")
        cls._divide_results(results, "cython", "python", "cy / py")
        cls._divide_results(results, "cython", "c", "cy / c")

    @test_case
    def int_to_boolean(self):
        self.impl.int_to_boolean(None)
        self.impl.int_to_boolean(10)
        self.impl.int_to_boolean(1)
        self.impl.int_to_boolean(-10)
        self.impl.int_to_boolean(0)

    @test_case
    def to_str(self):
        self.impl.to_str(None)
        self.impl.to_str(123)
        self.impl.to_str(True)
        self.impl.to_str(self)
        self.impl.to_str("self")

    @test_case
    def to_float(self):
        self.impl.to_float(None)
        self.impl.to_float(123)
        self.impl.to_float(True)
        self.impl.to_float(42)
        self.impl.to_float(0)
        self.impl.to_float(42.0)
        self.impl.to_float("nan")
        self.impl.to_float("42")
        self.impl.to_float("42.0")

    @test_case
    def str_to_datetime(self):
        self.impl.str_to_datetime(None)
        self.impl.str_to_datetime("2020-01-01 20:10:34")
        self.impl.str_to_datetime("2030-11-21 01:04:34.123456")

    @test_case
    def str_to_time(self):
        self.impl.str_to_time(None)
        self.impl.str_to_time("20:10:34")
        self.impl.str_to_time("01:04:34.123456")

    @test_case
    def str_to_date(self):
        self.impl.str_to_date(None)
        self.impl.str_to_date("2020-01-01")

    @test_case
    def to_decimal_call(self):
        assert self.to_dec(None) is None
        self.to_dec(123.44)
        self.to_dec(99)
        self.to_dec(1 / 3)

    @test_case
    def to_decimal_pf_make(self):
        self.impl.to_decimal_processor_factory(Decimal, 3)
        self.impl.to_decimal_processor_factory(Decimal, 7)


class DistillParam(Case):
    NUMBER = 2_000_000

    @staticmethod
    def python():
        from sqlalchemy.engine import _util_cy

        py_util = load_uncompiled_module(_util_cy)
        assert not py_util._is_compiled()
        return py_util

    @staticmethod
    def cython():
        from sqlalchemy.engine import _util_cy

        assert _util_cy._is_compiled()
        return _util_cy

    IMPLEMENTATIONS = {
        "python": python.__func__,
        "cython": cython.__func__,
    }

    def init_objects(self):
        self.tup_tup = tuple(tuple(range(10)) for _ in range(100))
        self.list_tup = list(self.tup_tup)
        self.dict = {f"c{i}": i for i in range(100)}
        self.mapping = MappingProxyType(self.dict)
        self.tup_dic = (self.dict, self.dict)
        self.list_dic = [self.dict, self.dict]

    @classmethod
    def update_results(cls, results):
        cls._divide_results(results, "c", "python", "c / py")
        cls._divide_results(results, "cython", "python", "cy / py")
        cls._divide_results(results, "cython", "c", "cy / c")

    @test_case
    def none_20(self):
        self.impl._distill_params_20(None)

    @test_case
    def empty_sequence_20(self):
        self.impl._distill_params_20(())
        self.impl._distill_params_20([])

    @test_case
    def list_20(self):
        self.impl._distill_params_20(self.list_tup)

    @test_case
    def tuple_20(self):
        self.impl._distill_params_20(self.tup_tup)

    @test_case
    def list_dict_20(self):
        self.impl._distill_params_20(self.list_tup)

    @test_case
    def tuple_dict_20(self):
        self.impl._distill_params_20(self.dict)

    @test_case
    def mapping_20(self):
        self.impl._distill_params_20(self.mapping)

    @test_case
    def raw_none(self):
        self.impl._distill_raw_params(None)

    @test_case
    def raw_empty_sequence(self):
        self.impl._distill_raw_params(())
        self.impl._distill_raw_params([])

    @test_case
    def raw_list(self):
        self.impl._distill_raw_params(self.list_tup)

    @test_case
    def raw_tuple(self):
        self.impl._distill_raw_params(self.tup_tup)

    @test_case
    def raw_list_dict(self):
        self.impl._distill_raw_params(self.list_tup)

    @test_case
    def raw_tuple_dict(self):
        self.impl._distill_raw_params(self.dict)

    @test_case
    def raw_mapping(self):
        self.impl._distill_raw_params(self.mapping)


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


class TupleGetter(Case):
    NUMBER = 2_000_000

    @staticmethod
    def python():
        from sqlalchemy.engine import _util_cy

        py_util = load_uncompiled_module(_util_cy)
        assert not py_util._is_compiled()
        return py_util.tuplegetter

    @staticmethod
    def c():
        from sqlalchemy import cresultproxy

        return cresultproxy.tuplegetter

    @staticmethod
    def cython():
        from sqlalchemy.engine import _util_cy

        assert _util_cy._is_compiled()
        return _util_cy.tuplegetter

    IMPLEMENTATIONS = {
        "python": python.__func__,
        "c": c.__func__,
        "cython": cython.__func__,
    }

    def init_objects(self):
        self.impl_tg = self.impl

        self.tuple = tuple(range(1000))
        self.tg_inst = self.impl_tg(42)
        self.tg_inst_m = self.impl_tg(42, 420, 99, 9, 1)
        self.tg_inst_seq = self.impl_tg(*range(70, 75))

    @classmethod
    def update_results(cls, results):
        cls._divide_results(results, "c", "python", "c / py")
        cls._divide_results(results, "cython", "python", "cy / py")
        cls._divide_results(results, "cython", "c", "cy / c")

    @test_case
    def tuplegetter_one(self):
        self.tg_inst(self.tuple)

    @test_case
    def tuplegetter_many(self):
        self.tg_inst_m(self.tuple)

    @test_case
    def tuplegetter_seq(self):
        self.tg_inst_seq(self.tuple)

    @test_case
    def tuplegetter_new_one(self):
        self.impl_tg(42)(self.tuple)

    @test_case
    def tuplegetter_new_many(self):
        self.impl_tg(42, 420, 99, 9, 1)(self.tuple)

    @test_case
    def tuplegetter_new_seq(self):
        self.impl_tg(40, 41, 42, 43, 44)(self.tuple)


class BaseRow(Case):
    @staticmethod
    def python():
        from sqlalchemy.engine import _row_cy

        py_res = load_uncompiled_module(_row_cy)
        assert not py_res._is_compiled()
        return py_res.BaseRow

    @staticmethod
    def c():
        from sqlalchemy.cresultproxy import BaseRow

        return BaseRow

    @staticmethod
    def cython():
        from sqlalchemy.engine import _row_cy

        assert _row_cy._is_compiled()
        return _row_cy.BaseRow

    IMPLEMENTATIONS = {
        "python": python.__func__,
        # "c": c.__func__,
        "cython": cython.__func__,
    }

    def init_objects(self):
        from sqlalchemy.engine.result import SimpleResultMetaData
        from string import ascii_letters

        self.parent = SimpleResultMetaData(("a", "b", "c"))
        self.row_args = (
            self.parent,
            self.parent._processors,
            self.parent._key_to_index,
            (1, 2, 3),
        )
        self.parent_long = SimpleResultMetaData(tuple(ascii_letters))
        self.row_long_args = (
            self.parent_long,
            self.parent_long._processors,
            self.parent_long._key_to_index,
            tuple(range(len(ascii_letters))),
        )
        self.row = self.impl(*self.row_args)
        self.row_long = self.impl(*self.row_long_args)
        assert isinstance(self.row, self.impl), type(self.row)

        class Row(self.impl):
            pass

        self.Row = Row
        self.row_sub = Row(*self.row_args)

        self.row_state = self.row.__getstate__()
        self.row_long_state = self.row_long.__getstate__()

        assert len(ascii_letters) == 52
        _proc = [None, int, float, None, str] * 10
        _proc += [int, float]
        self.parent_proc = SimpleResultMetaData(
            tuple(ascii_letters),
            _processors=_proc,
        )
        self.row_proc_args = (
            self.parent_proc,
            self.parent_proc._processors,
            self.parent_proc._key_to_index,
            tuple(range(len(ascii_letters))),
        )

        self.parent_proc_none = SimpleResultMetaData(
            tuple(ascii_letters), _processors=[None] * 52
        )
        self.row_proc_none_args = (
            self.parent_proc_none,
            # NOTE: usually the code calls _effective_processors that returns
            # None for this case of all None.
            self.parent_proc_none._processors,
            self.parent_proc_none._key_to_index,
            tuple(range(len(ascii_letters))),
        )

    @classmethod
    def update_results(cls, results):
        cls._divide_results(results, "c", "python", "c / py")
        cls._divide_results(results, "cython", "python", "cy / py")
        cls._divide_results(results, "cython", "c", "cy / c")

    @test_case
    def base_row_new(self):
        self.impl(*self.row_args)
        self.impl(*self.row_long_args)

    @test_case
    def row_new(self):
        self.Row(*self.row_args)
        self.Row(*self.row_long_args)

    @test_case
    def base_row_new_proc(self):
        self.impl(*self.row_proc_args)

    @test_case
    def row_new_proc(self):
        self.Row(*self.row_proc_args)

    @test_case
    def brow_new_proc_none(self):
        self.impl(*self.row_proc_none_args)

    @test_case
    def row_new_proc_none(self):
        self.Row(*self.row_proc_none_args)

    @test_case
    def row_dumps(self):
        self.row.__getstate__()
        self.row_long.__getstate__()

    @test_case
    def row_loads(self):
        self.impl.__new__(self.impl).__setstate__(self.row_state)
        self.impl.__new__(self.impl).__setstate__(self.row_long_state)

    @test_case
    def row_values_impl(self):
        self.row._values_impl()
        self.row_long._values_impl()

    @test_case
    def row_iter(self):
        list(self.row)
        list(self.row_long)

    @test_case
    def row_len(self):
        len(self.row)
        len(self.row_long)

    @test_case
    def row_hash(self):
        hash(self.row)
        hash(self.row_long)

    @test_case
    def getitem(self):
        self.row[0]
        self.row[1]
        self.row[-1]
        self.row_long[0]
        self.row_long[1]
        self.row_long[-1]

    @test_case
    def getitem_slice(self):
        self.row[0:1]
        self.row[1:-1]
        self.row_long[0:1]
        self.row_long[1:-1]

    @test_case
    def get_by_key(self):
        self.row._get_by_key_impl_mapping("a")
        self.row._get_by_key_impl_mapping("b")
        self.row_long._get_by_key_impl_mapping("s")
        self.row_long._get_by_key_impl_mapping("a")

    @test_case
    def getattr(self):
        self.row.a
        self.row.b
        self.row_long.x
        self.row_long.y

    @test_case(number=25_000)
    def get_by_key_recreate(self):
        self.init_objects()
        row = self.row
        for _ in range(25):
            row._get_by_key_impl_mapping("a")
        l_row = self.row_long
        for _ in range(25):
            l_row._get_by_key_impl_mapping("f")
            l_row._get_by_key_impl_mapping("o")
            l_row._get_by_key_impl_mapping("r")
            l_row._get_by_key_impl_mapping("t")
            l_row._get_by_key_impl_mapping("y")
            l_row._get_by_key_impl_mapping("t")
            l_row._get_by_key_impl_mapping("w")
            l_row._get_by_key_impl_mapping("o")

    @test_case(number=10_000)
    def getattr_recreate(self):
        self.init_objects()
        row = self.row
        for _ in range(25):
            row.a
        l_row = self.row_long
        for _ in range(25):
            l_row.f
            l_row.o
            l_row.r
            l_row.t
            l_row.y
            l_row.t
            l_row.w
            l_row.o


class AnonMap(Case):
    @staticmethod
    def python():
        from sqlalchemy.sql import _util_cy

        py_util = load_uncompiled_module(_util_cy)
        assert not py_util._is_compiled()
        return py_util.anon_map

    @staticmethod
    def cython():
        from sqlalchemy.sql import _util_cy

        assert _util_cy._is_compiled()
        return _util_cy.anon_map

    IMPLEMENTATIONS = {"python": python.__func__, "cython": cython.__func__}

    NUMBER = 1000000

    def init_objects(self):
        self.object_1 = column("x")
        self.object_2 = bindparam("y")

        self.impl_w_non_present = self.impl()
        self.impl_w_present = iwp = self.impl()
        iwp.get_anon(self.object_1)
        iwp.get_anon(self.object_2)

    @classmethod
    def update_results(cls, results):
        cls._divide_results(results, "cython", "python", "cy / py")

    @test_case
    def test_make(self):
        self.impl()

    @test_case
    def test_get_anon_np(self):
        self.impl_w_non_present.get_anon(self.object_1)

    @test_case
    def test_get_anon_p(self):
        self.impl_w_present.get_anon(self.object_1)

    @test_case
    def test_has_key_np(self):
        id(self.object_1) in self.impl_w_non_present

    @test_case
    def test_has_key_p(self):
        id(self.object_1) in self.impl_w_present


class PrefixAnonMap(Case):
    @staticmethod
    def python():
        from sqlalchemy.sql import _util_cy

        py_util = load_uncompiled_module(_util_cy)
        assert not py_util._is_compiled()
        return py_util.prefix_anon_map

    @staticmethod
    def cython():
        from sqlalchemy.sql import _util_cy

        assert _util_cy._is_compiled()
        return _util_cy.prefix_anon_map

    IMPLEMENTATIONS = {"python": python.__func__, "cython": cython.__func__}

    NUMBER = 1000000

    def init_objects(self):
        from sqlalchemy.sql.elements import _anonymous_label

        self.name = _anonymous_label.safe_construct(58243, "some_column_name")

        self.impl_w_non_present = self.impl()
        self.impl_w_present = iwp = self.impl()
        self.name.apply_map(iwp)

    @classmethod
    def update_results(cls, results):
        cls._divide_results(results, "cython", "python", "cy / py")

    @test_case
    def test_make(self):
        self.impl()

    @test_case
    def test_apply_np(self):
        self.name.apply_map(self.impl_w_non_present)

    @test_case
    def test_apply_p(self):
        self.name.apply_map(self.impl_w_present)


def tabulate(results, inverse):
    dim = 11
    header = "{:<20}|" + (" {:<%s} |" % dim) * len(results)
    num_format = "{:<%s.9f}" % dim
    row = "{:<20}|" + " {} |" * len(results)
    names = list(results)
    print(header.format("", *names))

    for meth in inverse:
        strings = [
            num_format.format(inverse[meth][name])[:dim] for name in names
        ]
        print(row.format(meth, *strings))


def main():
    import argparse

    cases = Case._CASES

    parser = argparse.ArgumentParser(
        description="Compare implementation between them"
    )
    parser.add_argument(
        "case",
        help="Case to run",
        nargs="+",
        choices=["all"] + [c.__name__ for c in cases],
    )
    parser.add_argument("--filter", help="filter the test for this regexp")
    parser.add_argument(
        "--factor", help="scale number passed to timeit", type=float, default=1
    )
    parser.add_argument("--csv", help="save to csv", action="store_true")

    args = parser.parse_args()

    if "all" in args.case:
        to_run = cases
    else:
        to_run = [c for c in cases if c.__name__ in args.case]

    for case in to_run:
        print("Running case", case.__name__)
        result = case.run_case(args.factor, args.filter)

        inverse = defaultdict(dict)
        for name in result:
            for meth in result[name]:
                inverse[meth][name] = result[name][meth]

        tabulate(result, inverse)

        if args.csv:
            import csv

            file_name = f"{case.__name__}.csv"
            with open(file_name, "w", newline="") as f:
                w = csv.DictWriter(f, ["", *result])
                w.writeheader()
                for n in inverse:
                    w.writerow({"": n, **inverse[n]})
            print("Wrote file", file_name)


if __name__ == "__main__":
    main()
