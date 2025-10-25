from sqlalchemy.util.langhelpers import load_uncompiled_module
from .base import Case
from .base import test_case


class TupleGetter(Case):
    NUMBER = 2_000_000

    @staticmethod
    def python():
        from sqlalchemy.engine import _util_cy

        py_util = load_uncompiled_module(_util_cy)
        assert not py_util._is_compiled()
        return py_util.tuplegetter

    @staticmethod
    def cython():
        from sqlalchemy.engine import _util_cy

        assert _util_cy._is_compiled()
        return _util_cy.tuplegetter

    IMPLEMENTATIONS = {
        "python": python.__func__,
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
    def cython():
        from sqlalchemy.engine import _row_cy

        assert _row_cy._is_compiled()
        return _row_cy.BaseRow

    IMPLEMENTATIONS = {
        "python": python.__func__,
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

        class RowMap(self.impl):
            __getitem__ = self.impl._get_by_key_impl_mapping

        self.Row = Row
        self.row_map = RowMap(*self.row_args)
        self.row_long_map = RowMap(*self.row_long_args)

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
        self.row_map["a"]
        self.row_map["b"]
        self.row_long_map["s"]
        self.row_long_map["a"]

    @test_case
    def get_by_key2(self):
        # NOTE: this is not representative of real usage
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

    @test_case(number=15_000)
    def get_by_key_recreate(self):
        self.init_objects()
        row = self.row_map
        for _ in range(25):
            row["a"]
        l_row = self.row_long_map
        for _ in range(25):
            l_row["f"]
            l_row["o"]
            l_row["r"]
            l_row["t"]
            l_row["y"]
            l_row["t"]
            l_row["w"]
            l_row["o"]

    @test_case(number=15_000)
    def get_by_key_recreate2(self):
        # NOTE: this is not representative of real usage
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

    @test_case
    def contains(self):
        1 in self.row
        -1 in self.row
        1 in self.row_long
        -1 in self.row_long
