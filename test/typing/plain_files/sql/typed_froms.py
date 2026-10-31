from typing import Any
from typing import assert_type

from sqlalchemy import BigInteger
from sqlalchemy import Column
from sqlalchemy import column
from sqlalchemy import ColumnClause
from sqlalchemy import CTE
from sqlalchemy import Integer
from sqlalchemy import Join
from sqlalchemy import MetaData
from sqlalchemy import Named
from sqlalchemy import Select
from sqlalchemy import select
from sqlalchemy import SmallInteger
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import table
from sqlalchemy import TableClause
from sqlalchemy import TypedColumns
from sqlalchemy.sql.base import ReadOnlyColumnCollection
from sqlalchemy.sql.selectable import NamedFromClause
from sqlalchemy.util.typing import Never
from sqlalchemy.util.typing import Unpack

meta = MetaData()


class with_name(TypedColumns):
    name: Column[str]


class user_col(with_name):
    id = Column(Integer, primary_key=True)
    age: Column[int]
    middle_name: Column[str | None]


user = Table("user", meta, user_col)

assert_type(with_name.name, Never)
assert_type(user, Table[user_col])
assert_type(user.c.id, Column[int])
assert_type(user.c.name, Column[str])
assert_type(user.c.age, Column[int])
assert_type(user.c.middle_name, Column[str | None])

assert_type(select(user.c.age, user.c.name), Select[int, str])


class with_age(TypedColumns):
    age: Named[int]


class person_col(with_age, with_name):
    id: Named[int] = Column(primary_key=True)


person = Table("person", meta, person_col)


def select_name(table: Table[with_name]) -> Select[str]:
    # it's covariant
    assert_type(table.c.name, Column[str])
    return select(table.c.name)


def default_generic(table: Table) -> None:
    assert_type(table.c.name, Column[Any])


def generic_any(table: Table[Any]) -> None:
    # TODO: would be nice to have this also be Column[Any]
    assert_type(table.c.name, Any)


select_name(person)
select_name(user)

assert_type(person, Table[person_col])
assert_type(person.c.id, Column[int])
assert_type(person.c.name, Column[str])
assert_type(person.c.age, Column[int])
assert_type(with_age.age, Never)

assert_type(person.select(), Select[*tuple[Any, ...]])
assert_type(select(person), Select[*tuple[Any, ...]])


class address_cols(TypedColumns):
    user: Named[str]
    address: Named[str]


address = Table("address", meta, address_cols, Column("extra", Integer))
assert_type(address, Table[address_cols])
assert_type(address.c.extra, Column[Any])
assert_type(address.c["user"], Column[Any])
assert_type("user" in address.c, bool)
assert_type(address.c.keys(), list[str])

plain = Table("a", meta, Column("x", Integer), Column("y", String))
assert_type(plain, Table[ReadOnlyColumnCollection[str, Column[Any]]])
assert_type(plain.c.x, Column[Any])
assert_type(plain.c.y, Column[Any])


class plain_cols(TypedColumns):
    x: Named[int | None]
    y: Named[str | None]


plain_now_typed = plain.with_cols(plain_cols)
assert_type(plain_now_typed, Table[plain_cols])
assert_type(plain_now_typed.c.x, Column[int | None])
assert_type(plain_now_typed.c.y, Column[str | None])

aa = address.alias()
assert_type(address.c.user, Column[str])
join = address.join(plain)

# a join defines a new namespace of cols that is table-prefixed, so
# this part can't be automated
assert_type(join, Join)


# but we can cast
class address_join_cols(TypedColumns):
    address_user: Named[str]
    address_address: Named[str]


join_typed = join.with_cols(address_join_cols)
assert_type(join_typed, Join[address_join_cols])
assert_type(join_typed.c.address_user, Column[str])
assert_type(join_typed.c.address_x, Column[Any])

my_select = select(address.c.user, address.c.address)
my_cte = my_select.cte().with_cols(address_cols)

assert_type(my_cte, CTE[address_cols])
assert_type(my_cte.c.address, Column[str])
my_sq = my_select.subquery().with_cols(address_cols)

assert_type(my_sq, NamedFromClause[address_cols])
assert_type(my_sq.c.address, Column[str])

alias = person.alias()
assert_type(alias, NamedFromClause[person_col])
assert_type(alias.with_cols(address_cols), NamedFromClause[address_cols])


class with_name_clause(TypedColumns):
    name: ColumnClause[str]


lower_table = table("t", column("name", String)).with_cols(with_name_clause)
assert_type(lower_table, TableClause[with_name_clause])
assert_type(lower_table.c.name, ColumnClause[str])
lower_table2 = lower_table.with_cols(with_name)
assert_type(lower_table2.c.name, Column[str])


def test_row_pos() -> None:
    # no row pos specified, behaves like a normal table
    assert_type(select(address), Select[Unpack[tuple[Any, ...]]])

    class user_cols(TypedColumns):
        id: Named[int]
        name: Named[str]
        age: Named[int]

        __row_pos__: tuple[int, str, int]

    user = Table("user", meta, user_cols)

    class item_cols(TypedColumns):
        name: Named[str]
        weight: Named[float]

        __row_pos__: tuple[str, float]

    item = Table("item", meta, item_cols)

    assert_type(select(user), Select[int, str, int])
    # NOTE: mypy seems not to understand multiple unpacks...
    # https://github.com/python/mypy/issues/20188
    # assert_type(select(item, user), Select[str, float, int, str, int])
    assert_type(select(item, user), Select[str, float])
    assert_type(select(item, user, item), Select[Unpack[tuple[Any, ...]]])
    # col after
    assert_type(select(user, person.c.name), Select[int, str, int, str])
    assert_type(
        select(user, person.c.name, person.c.id),
        Select[int, str, int, str, int],
    )
    assert_type(
        select(user, person.c.name, person.c.id, person.c.name),
        Select[int, str, int, str, int, str],
    )
    # col before
    assert_type(select(person.c.name, user), Select[str, int, str, int])
    assert_type(
        select(person.c.id, person.c.name, person.c.name, user),
        Select[int, str, str, int, str, int],
    )

    # select method
    assert_type(user.select(), Select[int, str, int])
    assert_type(user.alias().select(), Select[int, str, int])
    join = user.join(item).with_cols(item_cols)
    assert_type(join, Join[item_cols])
    # NOTE: mypy does not understand annotations on self
    # https://github.com/python/mypy/issues/14243
    # assert_type(join.select(), Select[str, float])
    assert_type(join.select(), Select[Unpack[tuple[Any, ...]]])


def test_column_assigned_to_wider_annotation() -> None:
    """test #13654"""

    class widened_cols(TypedColumns):
        a: Named[int | None] = Column(SmallInteger)
        b: Named[str | None] = Column(String(42))
        c: Named[int | None] = Column("c", Integer, nullable=True)
        d: Named[str | None] = Column(type_=String(42))
        e: Column[int | None] = Column(Integer)
        f: Named[str] = Column(String(42))
        g: Named[int] = Column(primary_key=True)
        # nullability comes from the annotation, not from Column's
        # nullable default
        h: Named[int | None] = Column(BigInteger)
        i: Named[int] = Column(BigInteger)
        j: Named[int | None] = Column()
        k: Named[int] = Column()
        m: Named[str | None] = Column(String(42), nullable=True)
        n: Named[str] = Column(primary_key=True)

    widened = Table("widened", meta, widened_cols)

    assert_type(widened.c.a, Column[int | None])
    assert_type(widened.c.b, Column[str | None])
    assert_type(widened.c.e, Column[int | None])
    assert_type(
        select(widened.c.h, widened.c.i, widened.c.j, widened.c.k),
        Select[int | None, int, int | None, int],
    )
    assert_type(select(widened.c.m, widened.c.n), Select[str | None, str])
    assert_type(
        select(widened.c.a, widened.c.b, widened.c.f, widened.c.g),
        Select[int | None, str | None, str, int],
    )

    x: Column[int | None] = Column("x", Integer)
    assert_type(select(x), Select[int | None])


def test_column_type_not_affected_by_nullable(flag: bool) -> None:
    """a non-annotated Column infers the python type of the SQL type;
    nullable and primary_key don't change it"""

    assert_type(Column(Integer), Column[int])
    assert_type(Column("x", Integer), Column[int])
    assert_type(Column("y", Integer, nullable=True), Column[int])
    assert_type(Column("z", Integer, nullable=False), Column[int])
    assert_type(
        Column("c", Integer, nullable=True, primary_key=True), Column[int]
    )
    assert_type(Column("c", Integer, primary_key=True), Column[int])
    assert_type(Column("q", Integer, nullable=flag), Column[int])
    assert_type(Column("q", Integer, primary_key=flag), Column[int])


def test_column_explicit_nullable_independent_of_annotation() -> None:
    """an explicit nullable may differ from the annotation, e.g. a column
    that's NULL-able for schema reasons but is not expected to contain
    NULL values.  the annotation determines the python type, the explicit
    nullable determines the schema; neither is a typing error"""

    class explicit_nullable_cols(TypedColumns):
        a: Named[str] = Column(String(42), nullable=True)
        b: Named[str | None] = Column(String(42), nullable=False)

    explicit_nullable = Table(
        "explicit_nullable", meta, explicit_nullable_cols
    )
    assert_type(
        select(explicit_nullable.c.a, explicit_nullable.c.b),
        Select[str, str | None],
    )


def test_select_typed_columns() -> None:
    assert_type(select(user.c.id, user.c.name), Select[int, str])
    assert_type(select(user.c.id, user.c.middle_name), Select[int, str | None])
    assert_type(
        select(plain_now_typed.c.x, plain_now_typed.c.y),
        Select[int | None, str | None],
    )

    class plain_cols_w_types(TypedColumns):
        x: Named[int | None] = Column(SmallInteger)
        y: Named[str | None] = Column(String(42))

    plain_w_types = plain.with_cols(plain_cols_w_types)
    assert_type(plain_w_types.c.x, Column[int | None])
    assert_type(
        select(plain_w_types.c.x, plain_w_types.c.y),
        Select[int | None, str | None],
    )


def test_column_incompatible_annotation() -> None:
    class bad_cols(TypedColumns):
        # EXPECTED_MYPY: Argument 1 to "Column" has incompatible type "Integer"
        # EXPECTED_MYPY: Following member(s) of "Integer" have conflicts:
        # EXPECTED_MYPY_RE: \s+_sa_typing_python_type: expected "str", got "int"  # noqa: E501
        a: Named[str] = Column(Integer())
        # EXPECTED_MYPY: Argument 1 to "Column" has incompatible type "String"
        # EXPECTED_MYPY: Following member(s) of "String" have conflicts:
        # EXPECTED_MYPY_RE: \s+_sa_typing_python_type: expected .+, got "str"
        b: Named[int | None] = Column(String(30))
