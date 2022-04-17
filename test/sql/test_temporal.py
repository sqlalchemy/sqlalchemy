"""Tests for temporal table structure, including system and
application versioning"""
from sqlalchemy import Column
from sqlalchemy import ForeignKeyConstraint
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import Period
from sqlalchemy import PrimaryKeyConstraint
from sqlalchemy import schema
from sqlalchemy import SystemTimePeriod
from sqlalchemy import Table
from sqlalchemy.sql.sqltypes import DATE
from sqlalchemy.sql.sqltypes import TIMESTAMP
from sqlalchemy.sql.sqltypes import VARCHAR
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import fixtures


class PeriodTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = "default"

    def test_period(self):
        m = MetaData()
        t1 = Table(
            "t1",
            m,
            Column("start", TIMESTAMP(6)),
            Column("end", TIMESTAMP(6)),
            Period("test_period", "start", "end"),
        )
        self.assert_compile(
            schema.CreateTable(t1),
            "CREATE TABLE t1 ("
            "start_timestamp TIMESTAMP, "
            "end_timestamp TIMESTAMP, "
            "PERIOD FOR test_period (start_timestamp, end_timestamp))",
        )


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

    def test_column_with_system_versioning(self):
        m = MetaData()
        t1 = Table(
            "t1",
            m,
            Column("x", Integer, system_versioning=True),
            Column("y", Integer),
        )
        self.assert_compile(
            schema.CreateTable(t1),
            "CREATE TABLE t1 ("
            "x INTEGER WITH SYSTEM VERSIONING,"
            "y INTEGER);",
        )

    def test_column_without_system_versioning(self):
        m = MetaData()
        t1 = Table(
            "t1",
            m,
            Column("x", Integer),
            Column("y", Integer, system_versioning=False),
            SystemTimePeriod(),
        )
        self.assert_compile(
            schema.CreateTable(t1),
            "CREATE TABLE t1 ("
            "x INTEGER,"
            "y INTEGER WITHOUT SYSTEM VERSIONING"
            ") WITH SYSTEM VERSIONING;",
        )


class BitemporalTest(fixtures.TestBase, AssertsCompiledSQL):
    """Test creating tables with both SV and AV"""

    def test_bitemporal_table(self):
        """Test creating the example table in the document
        "Temporal features in SQL:2011" """

        m = MetaData()
        t1 = Table(
            "Emp",
            m,
            Column("ENo", Integer),
            Column("ESart", DATE),
            Column("EEnd", DATE),
            Column("EDept", Integer),
            Period("SYSTEM_TIME", "Sys_start", "Sys_end"),
            Column("Sys_start", TIMESTAMP(12)),
            Column("Sys_end", TIMESTAMP(12)),
            Column("EName", VARCHAR(30)),
            SystemTimePeriod("Sys_start", "Sys_end"),
            PrimaryKeyConstraint("ENo", "Eperiod", without_overlaps=True),
            ForeignKeyConstraint(
                ("EDept", "PERIOD EPeriod"),
                ("Dept.DNo", "PERIOD Dept.DPeriod"),
            ),
        )
        self.assert_compile(
            schema.CreateTable(t1),
            "CREATE TABLE Emp("
            "ENo INTEGER,"
            "ESart DATE,"
            "EEnd DATE,"
            "EDept INTEGER,"
            "PERIOD FOR EPeriod (EStart, EEnd),"
            "Sys_start TIMESTAMP(12) GENERATED ALWAYS AS ROW START,"
            "Sys_end TIMESTAMP(12) GENERATED ALWAYS AS ROW END,"
            "EName VARCHAR(30),"
            "PERIOD FOR SYSTEM_TIME(Sys_start, Sys_end),"
            "PRIMARY KEY (ENo, EPeriod WITHOUT OVERLAPS),"
            "FOREIGN KEY (Edept, PERIOD EPeriod)"
            "REFERENCES Dept (DNo, PERIOD DPeriod)"
            ") WITH SYSTEM VERSIONING",
        )
