from decimal import Decimal
from types import MappingProxyType

from sqlalchemy import bindparam
from sqlalchemy import column
from sqlalchemy.util.langhelpers import load_uncompiled_module
from .base import Case
from .base import test_case


class Processors(Case):
    @staticmethod
    def python():
        from sqlalchemy.engine import _processors_cy

        py_processors = load_uncompiled_module(_processors_cy)
        assert not py_processors._is_compiled()
        return py_processors

    @staticmethod
    def cython():
        from sqlalchemy.engine import _processors_cy

        assert _processors_cy._is_compiled()
        return _processors_cy

    IMPLEMENTATIONS = {
        "python": python.__func__,
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
    def list_dict_20(self):
        self.impl._distill_params_20(self.list_dic)

    @test_case
    def tuple_dict_20(self):
        self.impl._distill_params_20(self.tup_dic)

    @test_case
    def mapping_20(self):
        self.impl._distill_params_20(self.mapping)

    @test_case
    def dict_20(self):
        self.impl._distill_params_20(self.dict)

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
        self.impl._distill_raw_params(self.list_dic)

    @test_case
    def raw_tuple_dict(self):
        self.impl._distill_raw_params(self.tup_dic)

    @test_case
    def raw_mapping(self):
        self.impl._distill_raw_params(self.mapping)

    @test_case
    def raw_dict(self):
        self.impl._distill_raw_params(self.mapping)


class AnonMap(Case):
    NUMBER = 5_000_000

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
        self.impl_w_non_present.clear()

    @test_case
    def test_get_anon_p(self):
        self.impl_w_present.get_anon(self.object_1)

    @test_case
    def test_get_item_np(self):
        self.impl_w_non_present[self.object_1]
        self.impl_w_non_present.clear()

    @test_case
    def test_get_item_p(self):
        self.impl_w_present[self.object_1]

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
