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
            Column("start_ts", TIMESTAMP),
            Column("end_ts", TIMESTAMP),
            Period("test_period", "start_ts", "end_ts"),
        )
        self.assert_compile(
            schema.CreateTable(t),
            "CREATE TABLE t ("
            "start_ts TIMESTAMP, "
            "end_ts TIMESTAMP, "
            "PERIOD FOR test_period (start_ts, end_ts))",
        )

    def test_pks_constraint(self):
        """Test setting a primary key on a PERIOD via a constraint"""
        m = MetaData()
        t = Table(
            "t",
            m,
            Column("id", Integer, nullable=False),
            Column("start_ts", TIMESTAMP),
            Column("end_ts", TIMESTAMP),
            Period("test_period", "start_ts", "end_ts"),
            PrimaryKeyConstraint("id", "test_period"),
        )
        self.assert_compile(
            schema.CreateTable(t),
            "CREATE TABLE t ("
            "id INTEGER NOT NULL, "
            "start_ts TIMESTAMP, "
            "end_ts TIMESTAMP, "
            "PERIOD FOR test_period (start_ts, end_ts), "
            "PRIMARY KEY (id, test_period))",
        )

    def test_pks_col_arg(self):
        """Test setting a primary key on a PERIOD via column/period args"""
        m = MetaData()
        t = Table(
            "t",
            m,
            Column("id", Integer, primary_key=True),
            Column("start_ts", TIMESTAMP),
            Column("end_ts", TIMESTAMP),
            Period("test_period", "start_ts", "end_ts", primary_key=True),
        )
        self.assert_compile(
            schema.CreateTable(t),
            "CREATE TABLE t ("
            "id INTEGER NOT NULL, "
            "start_ts TIMESTAMP, "
            "end_ts TIMESTAMP, "
            "PERIOD FOR test_period (start_ts, end_ts), "
            "PRIMARY KEY (id, test_period))",
        )

    def test_pk_without_overlaps(self):
        """Test the WITHOUT OVERLAPS clause on a primary key"""
        m = MetaData()
        t = Table(
            "t",
            m,
            Column("id", Integer),
            Column("start_ts", TIMESTAMP),
            Column("end_ts", TIMESTAMP),
            Period("test_period", "start_ts", "end_ts"),
            PrimaryKeyConstraint("id", "test_period", without_overlaps=True),
        )
        self.assert_compile(
            schema.CreateTable(t),
            "CREATE TABLE t ("
            "id INTEGER NOT NULL, "
            "start_ts TIMESTAMP, "
            "end_ts TIMESTAMP, "
            "PERIOD FOR test_period (start_ts, end_ts), "
            "PRIMARY KEY (id, test_period WITHOUT OVERLAPS))",
        )

    def test_to_metadata(self):
        period = Period("test_period", "start_ts", "end_ts")
        start = Column("start_ts", TIMESTAMP)
        end = Column("end_ts", TIMESTAMP)
        m = MetaData()
        t = Table(
            "t",
            m,
            start,
            end,
            period,
        )
        is_(t._system_versioning_period, None)
        is_(period, t.periods.test_period)
        eq_(period.name, "test_period")
        eq_(period.start, start)
        eq_(period.end, end)

        # Verify specifying period with objects
        start1 = Column("start_ts", TIMESTAMP)
        end1 = Column("end_ts", TIMESTAMP)
        period1 = Period("test_period", start1, end1)
        m1 = MetaData()
        t1 = Table(
            "t1",
            m1,
            start1,
            end1,
            period1,
        )
        eq_(period1.start, start1)
        eq_(period1.end, end1)


class ApplicationVersioningTest(fixtures.TestBase, AssertsCompiledSQL):
    """Application versioning does not currently have anything separate
    from the Period construct."""

    pass


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
            "x INTEGER WITH SYSTEM VERSIONING, "
            "y INTEGER)",
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
            "x INTEGER, "
            "y INTEGER WITHOUT SYSTEM VERSIONING"
            ") WITH SYSTEM VERSIONING",
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
            Period("EPeriod", "ESart", "EEnd"),
            Column("Sys_start", TIMESTAMP),
            Column("Sys_end", TIMESTAMP),
            Column("EName", VARCHAR(30)),
            SystemTimePeriod("Sys_start", "Sys_end"),
            PrimaryKeyConstraint("ENo", "EPeriod", without_overlaps=True),
            ForeignKeyConstraint(
                ("EDept", "EPeriod"),
                ("Dept.DNo", "Dept.DPeriod"),
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
        # Test system period with no given columns
        sysperiod = SystemTimePeriod()
        m = MetaData()
        t = Table("t", m, Column("x", Integer), sysperiod)
        is_(t._system_versioning_period, sysperiod)

        # Test period with given columns
        start = Column("start_ts", TIMESTAMP)
        end = Column("end_ts", TIMESTAMP)
        sysperiod1 = SystemTimePeriod(start, end)
        m1 = MetaData()
        t1 = Table(
            "t1",
            m1,
            start,
            end,
            sysperiod1,
        )

        is_(t1._system_versioning_period, sysperiod1)
        eq_(sysperiod1.name, "SYSTEM_TIME")
        eq_(sysperiod1.start, start)
        eq_(sysperiod1.end, end)
