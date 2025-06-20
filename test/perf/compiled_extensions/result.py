from __future__ import annotations

from dataclasses import dataclass
from itertools import product
from operator import itemgetter
from typing import Callable
from typing import Optional

import sqlalchemy as sa
from sqlalchemy.dialects import sqlite
from sqlalchemy.engine import cursor
from sqlalchemy.engine import result
from sqlalchemy.engine.default import DefaultExecutionContext
from .base import Case
from .base import test_case


class _CommonResult(Case):
    @classmethod
    def init_class(cls):
        # 3-col
        cls.def3_plain = Definition(list("abc"))
        cls.def3_1proc = Definition(list("abc"), [None, str, None])
        cls.def3_tf = Definition(list("abc"), tuplefilter=itemgetter(1, 2))
        cls.def3_1proc_tf = Definition(
            list("abc"), [None, str, None], itemgetter(1, 2)
        )
        cls.data3_100 = [(i, i + i, i - 1) for i in range(100)]
        cls.data3_1000 = [(i, i + i, i - 1) for i in range(1000)]
        cls.data3_10000 = [(i, i + i, i - 1) for i in range(10000)]

        cls.make_test_cases("row3col", "def3_", "data3_")

        # 21-col
        cols = [f"c_{i}" for i in range(21)]
        cls.def21_plain = Definition(cols)
        cls.def21_7proc = Definition(cols, [None, str, None] * 7)
        cls.def21_tf = Definition(
            cols, tuplefilter=itemgetter(1, 2, 9, 17, 18)
        )
        cls.def21_7proc_tf = Definition(
            cols, [None, str, None] * 7, itemgetter(1, 2, 9, 17, 18)
        )
        cls.data21_100 = [(i, i + i, i - 1) * 7 for i in range(100)]
        cls.data21_1000 = [(i, i + i, i - 1) * 7 for i in range(1000)]
        cls.data21_10000 = [(i, i + i, i - 1) * 7 for i in range(10000)]

        cls.make_test_cases("row21col", "def21_", "data21_")

    @classmethod
    def make_test_cases(cls, prefix: str, def_prefix: str, data_prefix: str):
        all_defs = [
            (k, v) for k, v in vars(cls).items() if k.startswith(def_prefix)
        ]
        all_data = [
            (k, v) for k, v in vars(cls).items() if k.startswith(data_prefix)
        ]
        assert all_defs and all_data

        def make_case(name, definition, data, number):
            init_args = cls.get_init_args_callable(definition, data)

            def go_all(self):
                result = self.impl(*init_args())
                result.all()

            setattr(cls, name + "_all", test_case(go_all, number=number))

            def go_all_uq(self):
                result = self.impl(*init_args()).unique()
                result.all()

            setattr(cls, name + "_all_uq", test_case(go_all_uq, number=number))

            def go_iter(self):
                result = self.impl(*init_args())
                for _ in result:
                    pass

            setattr(cls, name + "_iter", test_case(go_iter, number=number))

            def go_iter_uq(self):
                result = self.impl(*init_args()).unique()
                for _ in result:
                    pass

            setattr(
                cls, name + "_iter_uq", test_case(go_iter_uq, number=number)
            )

            def go_many(self):
                result = self.impl(*init_args())
                while result.fetchmany(10):
                    pass

            setattr(cls, name + "_many", test_case(go_many, number=number))

            def go_many_uq(self):
                result = self.impl(*init_args()).unique()
                while result.fetchmany(10):
                    pass

            setattr(
                cls, name + "_many_uq", test_case(go_many_uq, number=number)
            )

            def go_one(self):
                result = self.impl(*init_args())
                while result.fetchone() is not None:
                    pass

            setattr(cls, name + "_one", test_case(go_one, number=number))

            def go_one_uq(self):
                result = self.impl(*init_args()).unique()
                while result.fetchone() is not None:
                    pass

            setattr(cls, name + "_one_uq", test_case(go_one_uq, number=number))

            def go_scalar_all(self):
                result = self.impl(*init_args())
                result.scalars().all()

            setattr(
                cls, name + "_sc_all", test_case(go_scalar_all, number=number)
            )

            def go_scalar_iter(self):
                result = self.impl(*init_args())
                rs = result.scalars()
                for _ in rs:
                    pass

            setattr(
                cls,
                name + "_sc_iter",
                test_case(go_scalar_iter, number=number),
            )

            def go_scalar_many(self):
                result = self.impl(*init_args())
                rs = result.scalars()
                while rs.fetchmany(10):
                    pass

            setattr(
                cls,
                name + "_sc_many",
                test_case(go_scalar_many, number=number),
            )

        for (def_name, definition), (data_name, data) in product(
            all_defs, all_data
        ):
            name = (
                f"{prefix}_{def_name.removeprefix(def_prefix)}_"
                f"{data_name.removeprefix(data_prefix)}"
            )
            number = 500 if data_name.endswith("10000") else None
            make_case(name, definition, data, number)

    @classmethod
    def get_init_args_callable(
        cls, definition: Definition, data: list
    ) -> Callable:
        raise NotImplementedError


class IteratorResult(_CommonResult):
    NUMBER = 1_000

    impl: result.IteratorResult

    @staticmethod
    def default():
        return cursor.IteratorResult

    IMPLEMENTATIONS = {"default": default.__func__}

    @classmethod
    def get_init_args_callable(
        cls, definition: Definition, data: list
    ) -> Callable:
        meta = result.SimpleResultMetaData(
            definition.columns,
            _processors=definition.processors,
            _tuplefilter=definition.tuplefilter,
        )
        return lambda: (meta, iter(data))


class CursorResult(_CommonResult):
    NUMBER = 1_000

    impl: cursor.CursorResult

    @staticmethod
    def default():
        return cursor.CursorResult

    IMPLEMENTATIONS = {"default": default.__func__}

    @classmethod
    def get_init_args_callable(
        cls, definition: Definition, data: list
    ) -> Callable:
        if definition.processors:
            proc_dict = {
                c: p for c, p in zip(definition.columns, definition.processors)
            }
        else:
            proc_dict = None

        class MockExecutionContext(DefaultExecutionContext):
            def create_cursor(self):
                return _MockCursor(data, self.compiled)

            def get_result_processor(self, type_, colname, coltype):
                return None if proc_dict is None else proc_dict[colname]

            def args_for_new_cursor_result(self):
                self.cursor = self.create_cursor()
                return (
                    self,
                    self.cursor_fetch_strategy,
                    context.cursor.description,
                )

        dialect = sqlite.dialect()
        stmt = sa.select(
            *(sa.column(c) for c in definition.columns)
        ).select_from(sa.table("t"))
        compiled = stmt._compile_w_cache(
            dialect, compiled_cache=None, column_keys=[]
        )[0]

        context = MockExecutionContext._init_compiled(
            dialect=dialect,
            connection=_MockConnection(dialect),
            dbapi_connection=None,
            execution_options={},
            compiled=compiled,
            parameters=[],
            invoked_statement=stmt,
            extracted_parameters=None,
        )
        _ = context._setup_result_proxy()
        assert compiled._cached_metadata

        return context.args_for_new_cursor_result


class _MockCursor:
    def __init__(self, rows: list[tuple], compiled):
        self._rows = list(rows)
        if compiled._result_columns is None:
            self.description = None
        else:
            self.description = [
                (rc.keyname, 42, None, None, None, True)
                for rc in compiled._result_columns
            ]

    def close(self):
        pass

    def fetchone(self):
        if self._rows:
            return self._rows.pop(0)
        else:
            return None

    def fetchmany(self, size=None):
        if size is None:
            return self.fetchall()
        else:
            ret = self._rows[:size]
            self._rows[:size] = []
            return ret

    def fetchall(self):
        ret = self._rows
        self._rows = []
        return ret


class _MockConnection:
    _echo = False

    def __init__(self, dialect):
        self.dialect = dialect

    def _safe_close_cursor(self, cursor):
        cursor.close()

    def _handle_dbapi_exception(self, e, *args, **kw):
        raise e


@dataclass
class Definition:
    columns: list[str]
    processors: Optional[list[Optional[Callable]]] = None
    tuplefilter: Optional[Callable] = None
