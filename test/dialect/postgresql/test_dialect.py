# coding: utf-8

from sqlalchemy.testing.assertions import (
    eq_, assert_raises, assert_raises_message, AssertsExecutionResults,
    AssertsCompiledSQL)
from sqlalchemy.testing import engines, fixtures
from sqlalchemy import testing
import datetime
from sqlalchemy import (
    Table, Column, select, MetaData, text, Integer, String, Sequence, Numeric,
    DateTime, BigInteger, func, extract, SmallInteger, TypeDecorator, literal,
    cast, bindparam)
from sqlalchemy import exc, schema
from sqlalchemy.dialects.postgresql import base as postgresql
import logging
import logging.handlers
from sqlalchemy.testing.mock import Mock
from sqlalchemy.engine import engine_from_config
from sqlalchemy.engine import url
from sqlalchemy.testing import is_
from sqlalchemy.testing import expect_deprecated
from ...engine import test_execute
from sqlalchemy import dialects


class DialectTest(fixtures.TestBase):
    """python-side dialect tests.  """

    def test_version_parsing(self):

        def mock_conn(res):
            return Mock(
                execute=Mock(return_value=Mock(scalar=Mock(return_value=res))))

        dialect = postgresql.dialect()
        for string, version in [
                (
                    'PostgreSQL 8.3.8 on i686-redhat-linux-gnu, compiled by '
                    'GCC gcc (GCC) 4.1.2 20070925 (Red Hat 4.1.2-33)',
                    (8, 3, 8)),
                (
                    'PostgreSQL 8.5devel on x86_64-unknown-linux-gnu, '
                    'compiled by GCC gcc (GCC) 4.4.2, 64-bit', (8, 5)),
                (
                    'EnterpriseDB 9.1.2.2 on x86_64-unknown-linux-gnu, '
                    'compiled by gcc (GCC) 4.1.2 20080704 (Red Hat 4.1.2-50), '
                    '64-bit', (9, 1, 2)),
                (
                    '[PostgreSQL 9.2.4 ] VMware vFabric Postgres 9.2.4.0 '
                    'release build 1080137', (9, 2, 4)),
                (
                    'PostgreSQL 10devel on x86_64-pc-linux-gnu'
                    'compiled by gcc (GCC) 6.3.1 20170306, 64-bit', (10,)),
                (
                    'PostgreSQL 10beta1 on x86_64-pc-linux-gnu, '
                    'compiled by gcc (GCC) 4.8.5 20150623 '
                    '(Red Hat 4.8.5-11), 64-bit', (10,))
        ]:
            eq_(dialect._get_server_version_info(mock_conn(string)),
                version)

    def test_deprecated_dialect_name_still_loads(self):
        dialects.registry.clear()
        with expect_deprecated(
                "The 'postgres' dialect name "
                "has been renamed to 'postgresql'"):
            dialect = url.URL("postgres").get_dialect()
        is_(dialect, postgresql.dialect)

    @testing.requires.psycopg2_compatibility
    def test_pg_dialect_use_native_unicode_from_config(self):
        config = {
            'sqlalchemy.url': testing.db.url,
            'sqlalchemy.use_native_unicode': "false"}

        e = engine_from_config(config, _initialize=False)
        eq_(e.dialect.use_native_unicode, False)

        config = {
            'sqlalchemy.url': testing.db.url,
            'sqlalchemy.use_native_unicode': "true"}

        e = engine_from_config(config, _initialize=False)
        eq_(e.dialect.use_native_unicode, True)


class BatchInsertsTest(fixtures.TablesTest):
    __only_on__ = 'postgresql+psycopg2'
    __backend__ = True

    run_create_tables = "each"

    @classmethod
    def define_tables(cls, metadata):
        Table(
            'data', metadata,
            Column('id', Integer, primary_key=True),
            Column('x', String),
            Column('y', String),
            Column('z', Integer, server_default="5")
        )

    def setup(self):
        super(BatchInsertsTest, self).setup()
        self.engine = engines.testing_engine(options={"use_batch_mode": True})

    def teardown(self):
        self.engine.dispose()
        super(BatchInsertsTest, self).teardown()

    def test_insert(self):
        with self.engine.connect() as conn:
            conn.execute(
                self.tables.data.insert(),
                [
                    {"x": "x1", "y": "y1"},
                    {"x": "x2", "y": "y2"},
                    {"x": "x3", "y": "y3"}
                ]
            )

            eq_(
                conn.execute(select([self.tables.data])).fetchall(),
                [
                    (1, "x1", "y1", 5),
                    (2, "x2", "y2", 5),
                    (3, "x3", "y3", 5)
                ]
            )

    def test_not_sane_rowcount(self):
        self.engine.connect().close()
        assert not self.engine.dialect.supports_sane_multi_rowcount

    def test_update(self):
        with self.engine.connect() as conn:
            conn.execute(
                self.tables.data.insert(),
                [
                    {"x": "x1", "y": "y1"},
                    {"x": "x2", "y": "y2"},
                    {"x": "x3", "y": "y3"}
                ]
            )

            conn.execute(
                self.tables.data.update().
                where(self.tables.data.c.x == bindparam('xval')).
                values(y=bindparam('yval')),
                [
                    {"xval": "x1", "yval": "y5"},
                    {"xval": "x3", "yval": "y6"}
                ]
            )
            eq_(
                conn.execute(
                    select([self.tables.data]).
                    order_by(self.tables.data.c.id)).
                fetchall(),
                [
                    (1, "x1", "y5", 5),
                    (2, "x2", "y2", 5),
                    (3, "x3", "y6", 5)
                ]
            )


class MiscBackendTest(
        fixtures.TestBase, AssertsExecutionResults, AssertsCompiledSQL):

    __only_on__ = 'postgresql'
    __backend__ = True

    @testing.provide_metadata
    def test_date_reflection(self):
        metadata = self.metadata
        Table(
            'pgdate', metadata, Column('date1', DateTime(timezone=True)),
            Column('date2', DateTime(timezone=False)))
        metadata.create_all()
        m2 = MetaData(testing.db)
        t2 = Table('pgdate', m2, autoload=True)
        assert t2.c.date1.type.timezone is True
        assert t2.c.date2.type.timezone is False

    @testing.requires.psycopg2_compatibility
    def test_psycopg2_version(self):
        v = testing.db.dialect.psycopg2_version
        assert testing.db.dialect.dbapi.__version__.\
            startswith(".".join(str(x) for x in v))

    @testing.requires.psycopg2_compatibility
    def test_psycopg2_non_standard_err(self):
        # under pypy the name here is psycopg2cffi
        psycopg2 = testing.db.dialect.dbapi
        TransactionRollbackError = __import__(
            "%s.extensions" % psycopg2.__name__
        ).extensions.TransactionRollbackError

        exception = exc.DBAPIError.instance(
            "some statement", {}, TransactionRollbackError("foo"),
            psycopg2.Error)
        assert isinstance(exception, exc.OperationalError)

    # currently not passing with pg 9.3 that does not seem to generate
    # any notices here, would rather find a way to mock this
    @testing.requires.no_coverage
    @testing.requires.psycopg2_compatibility
    def _test_notice_logging(self):
        log = logging.getLogger('sqlalchemy.dialects.postgresql')
        buf = logging.handlers.BufferingHandler(100)
        lev = log.level
        log.addHandler(buf)
        log.setLevel(logging.INFO)
        try:
            conn = testing.db.connect()
            trans = conn.begin()
            try:
                conn.execute('create table foo (id serial primary key)')
            finally:
                trans.rollback()
        finally:
            log.removeHandler(buf)
            log.setLevel(lev)
        msgs = ' '.join(b.msg for b in buf.buffer)
        assert 'will create implicit sequence' in msgs
        assert 'will create implicit index' in msgs

    @testing.requires.psycopg2_or_pg8000_compatibility
    @engines.close_open_connections
    def test_client_encoding(self):
        c = testing.db.connect()
        current_encoding = c.execute("show client_encoding").fetchone()[0]
        c.close()

        # attempt to use an encoding that's not
        # already set
        if current_encoding == 'UTF8':
            test_encoding = 'LATIN1'
        else:
            test_encoding = 'UTF8'

        e = engines.testing_engine(options={'client_encoding': test_encoding})
        c = e.connect()
        new_encoding = c.execute("show client_encoding").fetchone()[0]
        eq_(new_encoding, test_encoding)

    @testing.requires.psycopg2_or_pg8000_compatibility
    @engines.close_open_connections
    def test_autocommit_isolation_level(self):
        c = testing.db.connect().execution_options(
            isolation_level='AUTOCOMMIT')
        # If we're really in autocommit mode then we'll get an error saying
        # that the prepared transaction doesn't exist. Otherwise, we'd
        # get an error saying that the command can't be run within a
        # transaction.
        assert_raises_message(
            exc.ProgrammingError,
            'prepared transaction with identifier "gilberte" does not exist',
            c.execute, "commit prepared 'gilberte'")

    @testing.fails_on('+zxjdbc',
                      "Can't infer the SQL type to use for an instance "
                      "of org.python.core.PyObjectDerived.")
    def test_extract(self):
        fivedaysago = datetime.datetime.now() \
            - datetime.timedelta(days=5)
        for field, exp in ('year', fivedaysago.year), \
                ('month', fivedaysago.month), ('day', fivedaysago.day):
            r = testing.db.execute(
                select([
                    extract(field, func.now() + datetime.timedelta(days=-5))])
            ).scalar()
            eq_(r, exp)

    @testing.provide_metadata
    def test_checksfor_sequence(self):
        meta1 = self.metadata
        seq = Sequence('fooseq')
        t = Table(
            'mytable', meta1,
            Column('col1', Integer, seq)
        )
        seq.drop()
        testing.db.execute('CREATE SEQUENCE fooseq')
        t.create(checkfirst=True)

    @testing.provide_metadata
    def test_schema_roundtrips(self):
        meta = self.metadata
        users = Table(
            'users', meta, Column(
                'id', Integer, primary_key=True), Column(
                'name', String(50)), schema='test_schema')
        users.create()
        users.insert().execute(id=1, name='name1')
        users.insert().execute(id=2, name='name2')
        users.insert().execute(id=3, name='name3')
        users.insert().execute(id=4, name='name4')
        eq_(users.select().where(users.c.name == 'name2')
            .execute().fetchall(), [(2, 'name2')])
        eq_(users.select(use_labels=True).where(
            users.c.name == 'name2').execute().fetchall(), [(2, 'name2')])
        users.delete().where(users.c.id == 3).execute()
        eq_(users.select().where(users.c.name == 'name3')
            .execute().fetchall(), [])
        users.update().where(users.c.name == 'name4'
                             ).execute(name='newname')
        eq_(users.select(use_labels=True).where(
            users.c.id == 4).execute().fetchall(), [(4, 'newname')])

    def test_quoted_name_bindparam_ok(self):
        from sqlalchemy.sql.elements import quoted_name

        with testing.db.connect() as conn:
            eq_(
                conn.scalar(
                    select(
                        [cast(
                            literal(quoted_name("some_name", False)), String)]
                    )
                ),
                "some_name"
            )

    def test_preexecute_passivedefault(self):
        """test that when we get a primary key column back from
        reflecting a table which has a default value on it, we pre-
        execute that DefaultClause upon insert."""

        try:
            meta = MetaData(testing.db)
            testing.db.execute("""
             CREATE TABLE speedy_users
             (
                 speedy_user_id   SERIAL     PRIMARY KEY,

                 user_name        VARCHAR    NOT NULL,
                 user_password    VARCHAR    NOT NULL
             );
            """)
            t = Table('speedy_users', meta, autoload=True)
            r = t.insert().execute(user_name='user',
                                   user_password='lala')
            assert r.inserted_primary_key == [1]
            result = t.select().execute().fetchall()
            assert result == [(1, 'user', 'lala')]
        finally:
            testing.db.execute('drop table speedy_users')

    @testing.fails_on('+zxjdbc', 'psycopg2/pg8000 specific assertion')
    @testing.requires.psycopg2_or_pg8000_compatibility
    def test_numeric_raise(self):
        stmt = text(
            "select cast('hi' as char) as hi", typemap={'hi': Numeric})
        assert_raises(exc.InvalidRequestError, testing.db.execute, stmt)

    @testing.only_if(
        "postgresql >= 8.2", "requires standard_conforming_strings")
    def test_serial_integer(self):

        class BITD(TypeDecorator):
            impl = Integer

            def load_dialect_impl(self, dialect):
                if dialect.name == 'postgresql':
                    return BigInteger()
                else:
                    return Integer()

        for version, type_, expected in [
            (None, Integer, 'SERIAL'),
            (None, BigInteger, 'BIGSERIAL'),
            ((9, 1), SmallInteger, 'SMALLINT'),
            ((9, 2), SmallInteger, 'SMALLSERIAL'),
            (None, postgresql.INTEGER, 'SERIAL'),
            (None, postgresql.BIGINT, 'BIGSERIAL'),
            (
                None, Integer().with_variant(BigInteger(), 'postgresql'),
                'BIGSERIAL'),
            (
                None, Integer().with_variant(postgresql.BIGINT, 'postgresql'),
                'BIGSERIAL'),
            (
                (9, 2), Integer().with_variant(SmallInteger, 'postgresql'),
                'SMALLSERIAL'),
            (None, BITD(), 'BIGSERIAL')
        ]:
            m = MetaData()

            t = Table('t', m, Column('c', type_, primary_key=True))

            if version:
                dialect = postgresql.dialect()
                dialect._get_server_version_info = Mock(return_value=version)
                dialect.initialize(testing.db.connect())
            else:
                dialect = testing.db.dialect

            ddl_compiler = dialect.ddl_compiler(dialect, schema.CreateTable(t))
            eq_(
                ddl_compiler.get_column_specification(t.c.c),
                "c %s NOT NULL" % expected
            )


class AutocommitTextTest(test_execute.AutocommitTextTest):
    __only_on__ = 'postgresql'

    def test_grant(self):
        self._test_keyword("GRANT USAGE ON SCHEMA fooschema TO foorole")

    def test_import_foreign_schema(self):
        self._test_keyword("IMPORT FOREIGN SCHEMA foob")

    def test_refresh_view(self):
        self._test_keyword("REFRESH MATERIALIZED VIEW fooview")

    def test_revoke(self):
        self._test_keyword("REVOKE USAGE ON SCHEMA fooschema FROM foorole")

    def test_truncate(self):
        self._test_keyword("TRUNCATE footable")
