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
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_


class PeriodTest(fixtures.TestBase, AssertsCompiledSQL):
    """Test the basic period construct"""

    __dialect__ = "default"

    def test_period(self):
        m = MetaData()
        t = Table(
            "t",
            m,
            Column("start", TIMESTAMP),
            Column("end", TIMESTAMP),
            Period("test_period", "start", "end"),
        )
        self.assert_compile(
            schema.CreateTable(t),
            "CREATE TABLE t ("
            "start_timestamp TIMESTAMP, "
            "end_timestamp TIMESTAMP, "
            "PERIOD FOR test_period (start_timestamp, end_timestamp))",
        )

    def test_to_metadata(self):
        period = Period("test_period", "start", "end")
        m = MetaData()
        t = Table(
            "t",
            m,
            Column("start", TIMESTAMP),
            Column("end", TIMESTAMP),
            period,
        )
        is_(period, t.periods.test_period)
        eq_(period.name, "test_period")
        eq_(period.start, "start")
        eq_(period.end, "end")


class ApplicationVersioningTest(fixtures.TestBase, AssertsCompiledSQL):
    """Application versioning does not currently have anything separate
    from the Period construct."""

    __dialect__ = "default"


class SystemVersioningTest(fixtures.TestBase, AssertsCompiledSQL):
    """Test possible constructs related to system versioning.

    Tests come from MariaDB's implementation examples
    https://mariadb.com/kb/en/system-versioned-tables/
    And from "Temporal features in SQL:2011"
    https://cs.ulb.ac.be/public/_media/teaching/infoh415/
    tempfeaturessql2011.pdf"""

    __dialect__ = "default"

    # @testing.requires.system_versioned_tables_support
    def test_create_table_versioning_no_columns(self):
        m = MetaData()
        t = Table("t", m, Column("x", Integer), SystemTimePeriod())
        self.assert_compile(
            schema.CreateTable(t),
            "CREATE TABLE t (x INTEGER) WITH SYSTEM VERSIONING",
        )

    # @testing.requires.system_versioned_tables_support
    def test_create_table_versioning_columns_specified(self):
        m = MetaData()
        t = Table(
            "t",
            m,
            Column("x", Integer),
            Column("start_timestamp", TIMESTAMP),
            Column("end_timestamp", TIMESTAMP),
            SystemTimePeriod("start_timestamp", "end_timestamp"),
        )
        self.assert_compile(
            schema.CreateTable(t),
            "CREATE TABLE t ("
            "x INTEGER, "
            "start_timestamp TIMESTAMP GENERATED ALWAYS AS ROW START, "
            "end_timestamp TIMESTAMP GENERATED ALWAYS AS ROW END, "
            "PERIOD FOR SYSTEM_TIME (start_timestamp, end_timestamp)"
            ") WITH SYSTEM VERSIONING",
        )

    def test_column_with_system_versioning(self):
        m = MetaData()
        t = Table(
            "t",
            m,
            Column("x", Integer, system_versioning=True),
            Column("y", Integer),
        )
        self.assert_compile(
            schema.CreateTable(t),
            "CREATE TABLE t ("
            "x INTEGER WITH SYSTEM VERSIONING,"
            "y INTEGER);",
        )

    def test_column_without_system_versioning(self):
        m = MetaData()
        t = Table(
            "t",
            m,
            Column("x", Integer),
            Column("y", Integer, system_versioning=False),
            SystemTimePeriod(),
        )
        self.assert_compile(
            schema.CreateTable(t),
            "CREATE TABLE t ("
            "x INTEGER,"
            "y INTEGER WITHOUT SYSTEM VERSIONING"
            ") WITH SYSTEM VERSIONING;",
        )

    def test_bitemporal_table(self):
        """Test creating the example table in the document
        "Temporal features in SQL:2011", with both SV and AV"""

        m = MetaData()
        t = Table(
            "Emp",
            m,
            Column("ENo", Integer),
            Column("ESart", DATE),
            Column("EEnd", DATE),
            Column("EDept", Integer),
            Period("SYSTEM_TIME", "Sys_start", "Sys_end"),
            Column("Sys_start", TIMESTAMP),
            Column("Sys_end", TIMESTAMP),
            Column("EName", VARCHAR(30)),
            SystemTimePeriod("Sys_start", "Sys_end"),
            PrimaryKeyConstraint("ENo", "Eperiod", without_overlaps=True),
            ForeignKeyConstraint(
                ("EDept", "PERIOD EPeriod"),
                ("Dept.DNo", "PERIOD Dept.DPeriod"),
            ),
        )
        self.assert_compile(
            schema.CreateTable(t),
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

    def test_to_metadata(self):
        sysperiod = SystemTimePeriod()
        m = MetaData()
        t = Table("t", m, Column("x", Integer), sysperiod)
        is_(sysperiod, t.periods.system_time)

        sysperiod1 = SystemTimePeriod("st")
        m1 = MetaData()
        t1 = Table(
            "t1",
            m1,
            Column("start", TIMESTAMP),
            Column("end", TIMESTAMP),
            sysperiod1,
        )

        is_(sysperiod1, t1.periods.system_time)
        eq_(sysperiod1.name, "SYSTEM_TIME")
        eq_(sysperiod1.start, "start")
        eq_(sysperiod1.end, "end")
