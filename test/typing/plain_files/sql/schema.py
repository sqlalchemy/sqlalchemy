from typing import Union

from sqlalchemy import CheckConstraint
from sqlalchemy import Column
from sqlalchemy import Constraint
from sqlalchemy import ForeignKey
from sqlalchemy import ForeignKeyConstraint
from sqlalchemy import ForeignKeyTarget
from sqlalchemy import Index
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import Table

MetaData(
    naming_convention={
        "ix": "ix_%(column_0_label)s",
        "uq": "uq_%(table_name)s_%(column_0_name)s",
        "ck": "ck_%(table_name)s_%(constraint_name)s",
        "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
        "pk": "pk_%(table_name)s",
    }
)


MetaData(naming_convention={"uq": "uq_%(table_name)s_%(column_0_N_name)s"})


def fk_guid(constraint: Union[Constraint, Index], table: Table) -> str:
    return "foo"


MetaData(
    naming_convention={
        "fk_guid": fk_guid,
        "ix": "ix_%(column_0_label)s",
        "fk": "fk_%(fk_guid)s",
        "foo": lambda c, t: t.name + str(c.name),
    }
)

NAMING_CONVENTIONS_ONLY_CALLABLE = {
    "fk_guid": fk_guid,
    "foo": lambda c, t: t.name + str(c.name),
}

MetaData(naming_convention=NAMING_CONVENTIONS_ONLY_CALLABLE)

NAMING_CONVENTIONS_TYPES_FOR_KEYS_ONLY = {
    CheckConstraint: "%(table_name)s_%(constraint_name)s_ck",
    Index: "%(column_0_label)s_ix",
}

MetaData(naming_convention=NAMING_CONVENTIONS_TYPES_FOR_KEYS_ONLY)

NAMING_CONVENTIONS_TYPES_AND_STR_FOR_KEYS = {
    CheckConstraint: "%(table_name)s_%(constraint_name)s_ck",
    Index: "%(column_0_label)s_ix",
    "custom": "custom",
    "fk": "fk_name",
}

MetaData(naming_convention=NAMING_CONVENTIONS_TYPES_AND_STR_FOR_KEYS)


NAMING_CONVENTIONS_STR = {
    "ix": "%(column_0_label)s_ix",
    "uq": "%(table_name)s_%(column_0_name)s_uq",
    "ck": "%(table_name)s_%(constraint_name)s_ck",
    "fk": "%(table_name)s_%(column_0_name)s_%(referred_table_name)s_fk",
    "pk": "%(table_name)s_pk",
}

MetaData(naming_convention=NAMING_CONVENTIONS_STR)


def index_only(index: Index, table: Table) -> str:
    return "index_only"


def constraint_only(constraint: Constraint, table: Table) -> str:
    return "constraint_only"


# constraint-only callable or index-only callable
MetaData(
    naming_convention={
        "ix": index_only,
        "uq": constraint_only,
    }
)


# ForeignKey target given as name tokens, in each accepted form
ForeignKey("my_table.my_col")
ForeignKey(("my.tbl", "my.col"))
ForeignKey((None, "my.tbl", "my.col"))
ForeignKey(("some.schema", "my.tbl", "my.col"))
ForeignKey(ForeignKeyTarget("some.schema", "my.tbl", "my.col"))
ForeignKey(Column("my_col", Integer))

# EXPECTED_MYPY: Argument 1 to "ForeignKey" has incompatible type
ForeignKey((None, "my.tbl", 3))

ForeignKeyConstraint(
    ["a", "b"],
    ["my_table.a", ("my.tbl", "b")],
)


def fk_target_tokens(fk: ForeignKey) -> str:
    tokens: ForeignKeyTarget = fk.target_tokens

    # EXPECTED_TYPE: Optional[str]
    reveal_type(tokens.schema)

    # EXPECTED_TYPE: str
    reveal_type(tokens.table_name)

    # EXPECTED_TYPE: Optional[str]
    reveal_type(tokens.column_name)

    return tokens.table_name
