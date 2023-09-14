from typing import Union

from sqlalchemy import CheckConstraint
from sqlalchemy import Constraint
from sqlalchemy import Index
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
