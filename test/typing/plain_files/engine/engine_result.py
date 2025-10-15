from sqlalchemy import column
from sqlalchemy.engine import Result
from sqlalchemy.engine import Row


def row_one(row: Row[int, str, bool]) -> None:
    # EXPECTED_TYPE: int
    reveal_type(row[0])
    # EXPECTED_TYPE: str
    reveal_type(row[1])
    # EXPECTED_TYPE: bool
    reveal_type(row[2])

    # EXPECTED_MYPY: Tuple index out of range
    row[3]
    # EXPECTED_MYPY: No overload variant of "__getitem__" of "tuple" matches argument type "str"  # noqa: E501
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


def result_one(res: Result[int, str]) -> None:
    # EXPECTED_TYPE: tuple[int, str, fallback=Row[int, str]]
    reveal_type(res.one())
    # EXPECTED_TYPE: tuple[int, str, fallback=Row[int, str]] | None
    reveal_type(res.one_or_none())
    # EXPECTED_TYPE: tuple[int, str, fallback=Row[int, str]] | None
    reveal_type(res.fetchone())
    # EXPECTED_TYPE: tuple[int, str, fallback=Row[int, str]] | None
    reveal_type(res.first())
    # EXPECTED_TYPE: Sequence[tuple[int, str, fallback=Row[int, str]]]
    reveal_type(res.all())
    # EXPECTED_TYPE: Sequence[tuple[int, str, fallback=Row[int, str]]]
    reveal_type(res.fetchmany())
    # EXPECTED_TYPE: Sequence[tuple[int, str, fallback=Row[int, str]]]
    reveal_type(res.fetchall())
    # EXPECTED_TYPE: tuple[int, str, fallback=Row[int, str]]
    reveal_type(next(res))
    for rf in res:
        # EXPECTED_TYPE: tuple[int, str, fallback=Row[int, str]]
        reveal_type(rf)
    for rp in res.partitions():
        # EXPECTED_TYPE: Sequence[tuple[int, str, fallback=Row[int, str]]]
        reveal_type(rp)

    # EXPECTED_TYPE: ScalarResult[int]
    res_s = reveal_type(res.scalars())
    # EXPECTED_TYPE: ScalarResult[int]
    res_s = reveal_type(res.scalars(0))
    # EXPECTED_TYPE: int
    reveal_type(res_s.one())
    # EXPECTED_TYPE: ScalarResult[Any]
    reveal_type(res.scalars(1))
    # EXPECTED_TYPE: MappingResult
    reveal_type(res.mappings())
    # EXPECTED_TYPE: FrozenResult[int, str]
    reveal_type(res.freeze())

    # EXPECTED_TYPE: int
    reveal_type(res.scalar_one())
    # EXPECTED_TYPE: int | None
    reveal_type(res.scalar_one_or_none())
    # EXPECTED_TYPE: int | None
    reveal_type(res.scalar())
