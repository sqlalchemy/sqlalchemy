# schema.py
# Copyright (C) 2005-2015 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Compatibility namespace for sqlalchemy.sql.schema and related.

"""

from .sql.base import (
    SchemaVisitor
    )


from .sql.schema import (
    CheckConstraint,
    Column,
    ColumnDefault,
    Constraint,
    DefaultClause,
    DefaultGenerator,
    FetchedValue,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    MetaData,
    PassiveDefault,
    PrimaryKeyConstraint,
    SchemaItem,
    Sequence,
    Table,
    ThreadLocalMetaData,
    UniqueConstraint,
    _get_table_key,
    ColumnCollectionConstraint,
    ColumnCollectionMixin
    )


from .sql.naming import conv


from .sql.ddl import (
    DDL,
    CreateTable,
    DropTable,
    CreateSequence,
    DropSequence,
    CreateIndex,
    DropIndex,
    CreateSchema,
    DropSchema,
    _DropView,
    CreateColumn,
    AddConstraint,
    DropConstraint,
    DDLBase,
    DDLElement,
    _CreateDropBase,
    _DDLCompiles,
    sort_tables,
    sort_tables_and_constraints
)
