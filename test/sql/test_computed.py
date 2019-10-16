# coding: utf-8
import pytest
from sqlalchemy import Column, Computed, Integer, MetaData, Table
from sqlalchemy.exc import ArgumentError
from sqlalchemy.schema import CreateTable
from sqlalchemy.testing import AssertsCompiledSQL, fixtures


class DDLComputedTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = "default"

    def _test_column_computed(self, *args):
        m = MetaData()
        kwargs = {"persisted": args[1]} if len(args) == 2 else {}
        t = Table(
            "t",
            m,
            Column("x", Integer),
            Column("y", Integer, Computed("x + 2", **kwargs)),
        )
        self.assert_compile(
            CreateTable(t),
            "CREATE TABLE t (x INTEGER, y INTEGER GENERATED "
            "ALWAYS AS (x + 2)%s)" % args[0],
        )

    def test_column_computed_no_persisted(self):
        self._test_column_computed("")

    def test_column_computed_persisted_none(self):
        self._test_column_computed("", None)

    def test_column_computed_persisted_true(self):
        self._test_column_computed(" STORED", True)

    def test_column_computed_persisted_false(self):
        self._test_column_computed(" VIRTUAL", False)

    def test_server_default_onupdate(self):
        text = (
            "A generated column cannot specify a server_default or a "
            "server_onupdate argument"
        )
        with pytest.raises(ArgumentError, match=text):
            m = MetaData()
            Table(
                "t",
                m,
                Column("x", Integer),
                Column("y", Integer, Computed("x + 2"), server_default="42"),
            )

        with pytest.raises(ArgumentError, match=text):
            m = MetaData()
            Table(
                "t",
                m,
                Column("x", Integer),
                Column("y", Integer, Computed("x + 2"), server_onupdate="42"),
            )
