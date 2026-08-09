# sql/test_naming.py
# Copyright (C) 2005-2026 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php

from sqlalchemy import CheckConstraint
from sqlalchemy import Column
from sqlalchemy import ForeignKeyConstraint
from sqlalchemy import Index
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import PrimaryKeyConstraint
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import UniqueConstraint
from sqlalchemy.schema import conv
from sqlalchemy.schema import f as schema_f
from sqlalchemy.sql.naming import f
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures


class BatchOp:
    """Mock representing schema batch operation supporting batch_op.f()."""

    def __init__(self, table):
        self.table = table

    def f(self, name):
        """Mark constraint name as pre-formatted for naming convention."""
        return f(name)

    def create_index(self, name, columns, **kw):
        idx = Index(self.f(name), *[self.table.c[c] if isinstance(c, str) else c for c in columns], **kw)
        return idx

    def create_unique_constraint(self, name, columns, **kw):
        uq = UniqueConstraint(*columns, name=self.f(name), **kw)
        self.table.append_constraint(uq)
        return uq

    def create_foreign_key(self, name, referent, local_cols, remote_cols, **kw):
        fk = ForeignKeyConstraint(
            local_cols,
            [f"{referent}.{c}" for c in remote_cols],
            name=self.f(name),
            **kw
        )
        self.table.append_constraint(fk)
        return fk

    def create_check_constraint(self, name, condition, **kw):
        ck = CheckConstraint(condition, name=self.f(name), **kw)
        self.table.append_constraint(ck)
        return ck


class BatchOpNamingConventionTest(fixtures.TestBase):
    """Test naming convention handling and batch_op.f() support."""

    def test_f_function_behavior(self):
        result = f("my_constraint")
        eq_(isinstance(result, conv), True)
        eq_(isinstance(result, str), True)
        eq_(str(result), "my_constraint")

        # Test idempotency
        eq_(f(result), result)

        # Test None
        eq_(f(None), None)

        # Test schema import parity
        eq_(schema_f("test_name"), f("test_name"))

    def test_batch_op_f_check_constraint(self):
        m = MetaData(
            naming_convention={"ck": "ck_%(table_name)s_%(constraint_name)s"}
        )
        t = Table(
            "user",
            m,
            Column("id", Integer, primary_key=True),
            Column("age", Integer),
        )
        batch_op = BatchOp(t)

        ck1 = batch_op.create_check_constraint(
            batch_op.f("ck_user_age_positive"), "age > 0"
        )
        eq_(ck1.name, "ck_user_age_positive")
        eq_(isinstance(ck1.name, conv), True)

    def test_batch_op_f_unique_constraint(self):
        m = MetaData(
            naming_convention={"uq": "uq_%(table_name)s_%(column_0_name)s"}
        )
        t = Table(
            "user",
            m,
            Column("id", Integer, primary_key=True),
            Column("email", String(255)),
        )
        batch_op = BatchOp(t)

        uq1 = batch_op.create_unique_constraint(
            batch_op.f("uq_custom_user_email"), ["email"]
        )
        eq_(uq1.name, "uq_custom_user_email")
        eq_(isinstance(uq1.name, conv), True)

    def test_batch_op_f_foreign_key_constraint(self):
        m = MetaData(
            naming_convention={
                "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s"
            }
        )
        t = Table(
            "address",
            m,
            Column("id", Integer, primary_key=True),
            Column("user_id", Integer),
        )
        batch_op = BatchOp(t)

        fk1 = batch_op.create_foreign_key(
            batch_op.f("fk_address_user"), "user", ["user_id"], ["id"]
        )
        eq_(fk1.name, "fk_address_user")
        eq_(isinstance(fk1.name, conv), True)

    def test_batch_op_f_index(self):
        m = MetaData(
            naming_convention={"ix": "ix_%(table_name)s_%(column_0_name)s"}
        )
        t = Table(
            "user",
            m,
            Column("id", Integer, primary_key=True),
            Column("username", String(50)),
        )
        batch_op = BatchOp(t)

        idx = batch_op.create_index(
            batch_op.f("ix_custom_username_idx"), ["username"]
        )
        eq_(idx.name, "ix_custom_username_idx")
        eq_(isinstance(idx.name, conv), True)

    def test_unformatted_vs_f_formatted(self):
        m = MetaData(
            naming_convention={"ck": "ck_%(table_name)s_%(constraint_name)s"}
        )
        t = Table(
            "product",
            m,
            Column("id", Integer, primary_key=True),
            Column("price", Integer),
        )
        batch_op = BatchOp(t)

        # Plain string gets naming convention applied
        ck_plain = CheckConstraint("price > 0", name="price_pos")
        t.append_constraint(ck_plain)
        eq_(ck_plain.name, "ck_product_price_pos")

        # batch_op.f() prevents re-formatting
        ck_f = batch_op.create_check_constraint(
            batch_op.f("ck_product_price_pos"), "price > 0"
        )
        eq_(ck_f.name, "ck_product_price_pos")
