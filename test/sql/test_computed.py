# coding: utf-8
from sqlalchemy import Column, Computed, Integer, MetaData, Table
from sqlalchemy.exc import ArgumentError
from sqlalchemy.schema import CreateTable
from sqlalchemy.testing import (
    AssertsCompiledSQL,
    combinations,
    fixtures,
    assert_raises_message,
)


class DDLComputedTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = "default"

    @combinations(
        ("no_persisted", "", Ellipsis),
        ("persisted_none", "", None),
        ("persisted_true", " STORED", True),
        ("persisted_false", " VIRTUAL", False),
        id_="iaa",
    )
    def test_column_computed(self, text, persisted):
        m = MetaData()
        kwargs = {"persisted": persisted} if persisted != Ellipsis else {}
        t = Table(
            "t",
            m,
            Column("x", Integer),
            Column("y", Integer, Computed("x + 2", **kwargs)),
        )
        self.assert_compile(
            CreateTable(t),
            "CREATE TABLE t (x INTEGER, y INTEGER GENERATED "
            "ALWAYS AS (x + 2)%s)" % text,
        )

    def test_server_default_onupdate(self):
        text = (
            "A generated column cannot specify a server_default or a "
            "server_onupdate argument"
        )

        def fn(**kwargs):
            m = MetaData()
            Table(
                "t",
                m,
                Column("x", Integer),
                Column("y", Integer, Computed("x + 2"), **kwargs),
            )

        assert_raises_message(ArgumentError, text, fn, server_default="42")
        assert_raises_message(ArgumentError, text, fn, server_onupdate="42")
