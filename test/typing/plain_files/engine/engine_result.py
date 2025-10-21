from typing import Any
from typing import assert_type
from typing import Sequence

from sqlalchemy import column
from sqlalchemy.engine import Result
from sqlalchemy.engine import Row
from sqlalchemy.engine import RowMapping
from sqlalchemy.engine.result import FrozenResult
from sqlalchemy.engine.result import MappingResult
from sqlalchemy.engine.result import ScalarResult


def row_one(row: Row[int, str, bool]) -> None:
    assert_type(row[0], int)
    assert_type(row[1], str)
    assert_type(row[2], bool)

    # EXPECTED_MYPY: Tuple index out of range
    row[3]
    # EXPECTED_MYPY: No overload variant of "__getitem__" of "tuple" matches argument type "str"  # noqa: E501
    row["a"]

    assert_type(row._mapping, RowMapping)
    rm = row._mapping
    assert_type(rm["foo"], Any)
    assert_type(rm[column("bar")], Any)

    # EXPECTED_MYPY_RE: Invalid index type "int" for "RowMapping"; expected type "(str \| SQLCoreOperations\[Any\]|Union\[str, SQLCoreOperations\[Any\]\])"  # noqa: E501
    rm[3]


def result_one(res: Result[int, str]) -> None:
    assert_type(res.one(), Row[int, str])
    assert_type(res.one_or_none(), Row[int, str] | None)
    assert_type(res.fetchone(), Row[int, str] | None)
    assert_type(res.first(), Row[int, str] | None)
    assert_type(res.all(), Sequence[Row[int, str]])
    assert_type(res.fetchmany(), Sequence[Row[int, str]])
    assert_type(res.fetchall(), Sequence[Row[int, str]])
    assert_type(next(res), Row[int, str])
    for rf in res:
        assert_type(rf, Row[int, str])
    for rp in res.partitions():
        assert_type(rp, Sequence[Row[int, str]])

    res_s = assert_type(res.scalars(), ScalarResult[int])
    res_s = assert_type(res.scalars(0), ScalarResult[int])
    assert_type(res_s.one(), int)
    assert_type(res.scalars(1), ScalarResult[Any])
    assert_type(res.mappings(), MappingResult)
    assert_type(res.freeze(), FrozenResult[int, str])

    assert_type(res.scalar_one(), int)
    assert_type(res.scalar_one_or_none(), int | None)
    assert_type(res.scalar(), int | None)
