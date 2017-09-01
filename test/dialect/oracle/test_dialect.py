# coding: utf-8


from sqlalchemy.testing import eq_
from sqlalchemy import types as sqltypes, exc, schema
from sqlalchemy.sql import table, column
from sqlalchemy.testing import (fixtures,
                                AssertsExecutionResults,
                                AssertsCompiledSQL)
from sqlalchemy import testing
from sqlalchemy import Integer, Text, LargeBinary, Unicode, UniqueConstraint,\
    Index, MetaData, select, inspect, ForeignKey, String, func, \
    TypeDecorator, bindparam, Numeric, TIMESTAMP, CHAR, text, \
    literal_column, VARCHAR, create_engine, Date, NVARCHAR, \
    ForeignKeyConstraint, Sequence, Float, DateTime, cast, UnicodeText, \
    union, except_, type_coerce, or_, outerjoin, DATE, NCHAR, outparam, \
    PrimaryKeyConstraint, FLOAT
from sqlalchemy.util import u, b
from sqlalchemy import util
from sqlalchemy.testing import assert_raises, assert_raises_message
from sqlalchemy.testing.engines import testing_engine
from sqlalchemy.dialects.oracle import cx_oracle, base as oracle
from sqlalchemy.engine import default
import decimal
from sqlalchemy.engine import url
from sqlalchemy.testing.schema import Table, Column
import datetime
import os
from sqlalchemy import sql
from sqlalchemy.testing.mock import Mock


class DialectTest(fixtures.TestBase):
    def test_cx_oracle_version_parse(self):
        dialect = cx_oracle.OracleDialect_cx_oracle()

        eq_(
            dialect._parse_cx_oracle_ver("5.2"),
            (5, 2)
        )

        eq_(
            dialect._parse_cx_oracle_ver("5.0.1"),
            (5, 0, 1)
        )

        eq_(
            dialect._parse_cx_oracle_ver("6.0b1"),
            (6, 0)
        )


class OutParamTest(fixtures.TestBase, AssertsExecutionResults):
    __only_on__ = 'oracle+cx_oracle'
    __backend__ = True

    @classmethod
    def setup_class(cls):
        testing.db.execute("""
        create or replace procedure foo(x_in IN number, x_out OUT number,
        y_out OUT number, z_out OUT varchar) IS
        retval number;
        begin
            retval := 6;
            x_out := 10;
            y_out := x_in * 15;
            z_out := NULL;
        end;
        """)

    def test_out_params(self):
        result = testing.db.execute(text('begin foo(:x_in, :x_out, :y_out, '
                                         ':z_out); end;',
                                    bindparams=[bindparam('x_in', Float),
                                                outparam('x_out', Integer),
                                                outparam('y_out', Float),
                                                outparam('z_out', String)]),
                                    x_in=5)
        eq_(result.out_parameters,
            {'x_out': 10, 'y_out': 75, 'z_out': None})
        assert isinstance(result.out_parameters['x_out'], int)

    @classmethod
    def teardown_class(cls):
        testing.db.execute("DROP PROCEDURE foo")


class QuotedBindRoundTripTest(fixtures.TestBase):

    __only_on__ = 'oracle'
    __backend__ = True

    @testing.provide_metadata
    def test_table_round_trip(self):
        oracle.RESERVED_WORDS.remove('UNION')

        metadata = self.metadata
        table = Table("t1", metadata,
                      Column("option", Integer),
                      Column("plain", Integer, quote=True),
                      # test that quote works for a reserved word
                      # that the dialect isn't aware of when quote
                      # is set
                      Column("union", Integer, quote=True))
        metadata.create_all()

        table.insert().execute(
            {"option": 1, "plain": 1, "union": 1}
        )
        eq_(
            testing.db.execute(table.select()).first(),
            (1, 1, 1)
        )
        table.update().values(option=2, plain=2, union=2).execute()
        eq_(
            testing.db.execute(table.select()).first(),
            (2, 2, 2)
        )

    def test_numeric_bind_round_trip(self):
        eq_(
            testing.db.scalar(
                select([
                    literal_column("2", type_=Integer()) +
                    bindparam("2_1", value=2)])
            ),
            4
        )

    @testing.provide_metadata
    def test_numeric_bind_in_crud(self):
        t = Table(
            "asfd", self.metadata,
            Column("100K", Integer)
        )
        t.create()

        testing.db.execute(t.insert(), {"100K": 10})
        eq_(
            testing.db.scalar(t.select()), 10
        )


class CompatFlagsTest(fixtures.TestBase, AssertsCompiledSQL):

    def _dialect(self, server_version, **kw):
        def server_version_info(conn):
            return server_version

        dialect = oracle.dialect(
            dbapi=Mock(version="0.0.0", paramstyle="named"),
            **kw)
        dialect._get_server_version_info = server_version_info
        dialect._check_unicode_returns = Mock()
        dialect._check_unicode_description = Mock()
        dialect._get_default_schema_name = Mock()
        dialect._detect_decimal_char = Mock()
        return dialect

    def test_ora8_flags(self):
        dialect = self._dialect((8, 2, 5))

        # before connect, assume modern DB
        assert dialect._supports_char_length
        assert dialect._supports_nchar
        assert dialect.use_ansi

        dialect.initialize(Mock())
        assert not dialect.implicit_returning
        assert not dialect._supports_char_length
        assert not dialect._supports_nchar
        assert not dialect.use_ansi
        self.assert_compile(String(50), "VARCHAR2(50)", dialect=dialect)
        self.assert_compile(Unicode(50), "VARCHAR2(50)", dialect=dialect)
        self.assert_compile(UnicodeText(), "CLOB", dialect=dialect)

        dialect = self._dialect((8, 2, 5), implicit_returning=True)
        dialect.initialize(testing.db.connect())
        assert dialect.implicit_returning

    def test_default_flags(self):
        """test with no initialization or server version info"""

        dialect = self._dialect(None)

        assert dialect._supports_char_length
        assert dialect._supports_nchar
        assert dialect.use_ansi
        self.assert_compile(String(50), "VARCHAR2(50 CHAR)", dialect=dialect)
        self.assert_compile(Unicode(50), "NVARCHAR2(50)", dialect=dialect)
        self.assert_compile(UnicodeText(), "NCLOB", dialect=dialect)

    def test_ora10_flags(self):
        dialect = self._dialect((10, 2, 5))

        dialect.initialize(Mock())
        assert dialect._supports_char_length
        assert dialect._supports_nchar
        assert dialect.use_ansi
        self.assert_compile(String(50), "VARCHAR2(50 CHAR)", dialect=dialect)
        self.assert_compile(Unicode(50), "NVARCHAR2(50)", dialect=dialect)
        self.assert_compile(UnicodeText(), "NCLOB", dialect=dialect)


class ExecuteTest(fixtures.TestBase):

    __only_on__ = 'oracle'
    __backend__ = True

    def test_basic(self):
        eq_(testing.db.execute('/*+ this is a comment */ SELECT 1 FROM '
            'DUAL').fetchall(), [(1, )])

    def test_sequences_are_integers(self):
        seq = Sequence('foo_seq')
        seq.create(testing.db)
        try:
            val = testing.db.execute(seq)
            eq_(val, 1)
            assert type(val) is int
        finally:
            seq.drop(testing.db)

    @testing.provide_metadata
    def test_limit_offset_for_update(self):
        metadata = self.metadata
        # oracle can't actually do the ROWNUM thing with FOR UPDATE
        # very well.

        t = Table('t1',
                  metadata,
                  Column('id', Integer, primary_key=True),
                  Column('data', Integer))
        metadata.create_all()

        t.insert().execute(
            {'id': 1, 'data': 1},
            {'id': 2, 'data': 7},
            {'id': 3, 'data': 12},
            {'id': 4, 'data': 15},
            {'id': 5, 'data': 32},
        )

        # here, we can't use ORDER BY.
        eq_(
            t.select(for_update=True).limit(2).execute().fetchall(),
            [(1, 1),
             (2, 7)]
        )

        # here, its impossible.  But we'd prefer it to raise ORA-02014
        # instead of issuing a syntax error.
        assert_raises_message(
            exc.DatabaseError,
            "ORA-02014",
            t.select(for_update=True).limit(2).offset(3).execute
        )


class UnicodeSchemaTest(fixtures.TestBase):
    __only_on__ = 'oracle'
    __backend__ = True

    @testing.provide_metadata
    def test_quoted_column_non_unicode(self):
        metadata = self.metadata
        table = Table("atable", metadata,
                      Column("_underscorecolumn",
                             Unicode(255),
                             primary_key=True))
        metadata.create_all()

        table.insert().execute(
            {'_underscorecolumn': u('’é')},
        )
        result = testing.db.execute(
            table.select().where(table.c._underscorecolumn == u('’é'))
        ).scalar()
        eq_(result, u('’é'))

    @testing.provide_metadata
    def test_quoted_column_unicode(self):
        metadata = self.metadata
        table = Table("atable", metadata,
                      Column(u("méil"), Unicode(255), primary_key=True))
        metadata.create_all()

        table.insert().execute(
            {u('méil'): u('’é')},
        )
        result = testing.db.execute(
            table.select().where(table.c[u('méil')] == u('’é'))
        ).scalar()
        eq_(result, u('’é'))


class ServiceNameTest(fixtures.TestBase):
    __only_on__ = 'oracle+cx_oracle'
    __backend__ = True

    def test_cx_oracle_service_name(self):
        url_string = 'oracle+cx_oracle://scott:tiger@host/?service_name=hr'
        eng = create_engine(url_string, _initialize=False)
        cargs, cparams = eng.dialect.create_connect_args(eng.url)

        assert 'SERVICE_NAME=hr' in cparams['dsn']
        assert 'SID=hr' not in cparams['dsn']

    def test_cx_oracle_service_name_bad(self):
        url_string = 'oracle+cx_oracle://scott:tiger@host/hr1?service_name=hr2'
        assert_raises(
            exc.InvalidRequestError,
            create_engine, url_string,
            _initialize=False
        )

