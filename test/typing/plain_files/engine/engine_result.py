from typing import Tuple

from sqlalchemy import column
from sqlalchemy.engine import Result
from sqlalchemy.engine import Row


def row_one(row: Row[Tuple[int, str, bool]]) -> None:
    # EXPECTED_TYPE: Any
    reveal_type(row[0])
    # EXPECTED_TYPE: Any
    reveal_type(row[1])
    # EXPECTED_TYPE: Any
    reveal_type(row[2])

    # EXPECTED_MYPY: No overload variant of "__getitem__" of "Row" matches argument type "str"  # noqa: E501
    row["a"]

    # EXPECTED_TYPE: RowMapping
    reveal_type(row._mapping)
    rm = row._mapping
    # EXPECTED_TYPE: Any
    reveal_type(rm["foo"])
    # EXPECTED_TYPE: Any
    reveal_type(rm[column("bar")])

    # EXPECTED_MYPY_RE: Invalid index type "int" for "RowMapping"; expected type "(str \| SQLCoreOperations\[Any\]|Union\[str, SQLCoreOperations\[Any\]\])"  # noqa: E501
    rm[3]


def result_one(
    res: Result[Tuple[int, str]], r_single: Result[Tuple[float]]
) -> None:
    # EXPECTED_TYPE: Row[Tuple[int, str]]
    reveal_type(res.one())
    # EXPECTED_TYPE: Row[Tuple[int, str]] | None
    reveal_type(res.one_or_none())
    # EXPECTED_TYPE: Row[Tuple[int, str]] | None
    reveal_type(res.fetchone())
    # EXPECTED_TYPE: Row[Tuple[int, str]] | None
    reveal_type(res.first())
    # EXPECTED_TYPE: Sequence[Row[Tuple[int, str]]]
    reveal_type(res.all())
    # EXPECTED_TYPE: Sequence[Row[Tuple[int, str]]]
    reveal_type(res.fetchmany())
    # EXPECTED_TYPE: Sequence[Row[Tuple[int, str]]]
    reveal_type(res.fetchall())
    # EXPECTED_TYPE: Row[Tuple[int, str]]
    reveal_type(next(res))
    for rf in res:
        # EXPECTED_TYPE: Row[Tuple[int, str]]
        reveal_type(rf)
    for rp in res.partitions():
        # EXPECTED_TYPE: Sequence[Row[Tuple[int, str]]]
        reveal_type(rp)

    # EXPECTED_TYPE: ScalarResult[Any]
    res_s = reveal_type(res.scalars())
    # EXPECTED_TYPE: ScalarResult[Any]
    res_s = reveal_type(res.scalars(0))
    # EXPECTED_TYPE: Any
    reveal_type(res_s.one())
    # EXPECTED_TYPE: ScalarResult[Any]
    reveal_type(res.scalars(1))
    # EXPECTED_TYPE: MappingResult
    reveal_type(res.mappings())
    # EXPECTED_TYPE: FrozenResult[Tuple[int, str]]
    reveal_type(res.freeze())

    # EXPECTED_TYPE: Any
    reveal_type(res.scalar_one())
    # EXPECTED_TYPE: Any | None
    reveal_type(res.scalar_one_or_none())
    # EXPECTED_TYPE: Any
    reveal_type(res.scalar())

    # EXPECTED_TYPE: ScalarResult[float]
    res_s2 = reveal_type(r_single.scalars())
    # EXPECTED_TYPE: ScalarResult[float]
    res_s2 = reveal_type(r_single.scalars(0))
    # EXPECTED_TYPE: float
    reveal_type(res_s2.one())
    # EXPECTED_TYPE: ScalarResult[Any]
    reveal_type(r_single.scalars(1))
    # EXPECTED_TYPE: MappingResult
    reveal_type(r_single.mappings())

    # EXPECTED_TYPE: float
    reveal_type(r_single.scalar_one())
    # EXPECTED_TYPE: float | None
    reveal_type(r_single.scalar_one_or_none())
    # EXPECTED_TYPE: float | None
    reveal_type(r_single.scalar())
