"""Tests for temporal table structure, including system and
application versioning"""
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import schema
from sqlalchemy import SystemTimePeriod
from sqlalchemy import Table
from sqlalchemy.sql.sqltypes import TIMESTAMP
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import fixtures


class PeriodTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = "default"
    pass


class ApplicationVersioningTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = "default"
    pass


class SystemVersioningTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = "default"

    # @testing.requires.system_versioned_tables_support
    def test_create_table_versioning_no_columns(self):
        m = MetaData()
        t1 = Table("t1", m, Column("x", Integer), SystemTimePeriod())
        self.assert_compile(
            schema.CreateTable(t1),
            "CREATE TABLE t1 (x INTEGER) WITH SYSTEM VERSIONING",
        )

    # @testing.requires.system_versioned_tables_support
    def test_create_table_versioning_columns_specified(self):
        m = MetaData()
        t1 = Table(
            "t1",
            m,
            Column("x", Integer),
            Column("start_timestamp", TIMESTAMP(6)),
            Column("end_timestamp", TIMESTAMP(6)),
            SystemTimePeriod("start_timestamp", "end_timestamp"),
        )
        self.assert_compile(
            schema.CreateTable(t1),
            "CREATE TABLE t1 ("
            "x INTEGER, "
            "start_timestamp TIMESTAMP GENERATED ALWAYS AS ROW START, "
            "end_timestamp TIMESTAMP GENERATED ALWAYS AS ROW END, "
            "PERIOD FOR SYSTEM_TIME (start_timestamp, end_timestamp)"
            ") WITH SYSTEM VERSIONING",
        )

    # # @testing.requires.system_versioned_tables_support
    # def test_missing_system_versioning_column(self):
    #     m = MetaData()
    #     t1 = Table(
    #         "t1",
    #         m,
    #         Column("x", Integer),
    #         Column("start_timestamp", TIMESTAMP(6),
    #           system_versioning="start"),
    #         Column("end_timestamp ", TIMESTAMP(6)),
    #         system_versioning=True,
    #     )

    #     assert_raises_message(
    #         exc.CompileError,
    #         "Unable to compile system versioning period. "
    #         'Did you set both "start" and "end" columns?',
    #         m.create_all,
    #         testing.db,
    #     )

    # @testing.requires.system_versioned_tables_support
    # def test_too_many_system_versioning_columns(self):
    #     m = MetaData()
    #     t1 = Table(
    #         "t1",
    #         m,
    #         Column("x", Integer),
    #         Column("start_timestamp", TIMESTAMP(6),
    #           system_versioning="start"),
    #         Column("end_timestamp ", TIMESTAMP(6),
    #           system_versioning="start"),
    #         system_versioning=True,
    #     )

    #     assert_raises_message(
    #         exc.CompileError,
    #         ".*too many system versioning.*",
    #         m.create_all,
    #         testing.db,
    #     )
