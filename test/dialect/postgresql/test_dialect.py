# coding: utf-8

from sqlalchemy.testing.assertions import eq_, assert_raises, \
                assert_raises_message, AssertsExecutionResults, \
                AssertsCompiledSQL
from sqlalchemy.testing import engines, fixtures
from sqlalchemy import testing
import datetime
from sqlalchemy import Table, Column, select, MetaData, text, Integer, \
            String, Sequence, ForeignKey, join, Numeric, \
            PrimaryKeyConstraint, DateTime, tuple_, Float, BigInteger, \
            func, literal_column, literal, bindparam, cast, extract, \
            SmallInteger, Enum, REAL, update, insert, Index, delete, \
            and_, Date, TypeDecorator, Time, Unicode, Interval, or_, Text
from sqlalchemy import exc, schema
from sqlalchemy.dialects.postgresql import base as postgresql
import logging
import logging.handlers
from sqlalchemy.testing.mock import Mock

class MiscTest(fixtures.TestBase, AssertsExecutionResults, AssertsCompiledSQL):

    __only_on__ = 'postgresql'

    @testing.provide_metadata
    def test_date_reflection(self):
        metadata = self.metadata
        t1 = Table('pgdate', metadata, Column('date1',
                   DateTime(timezone=True)), Column('date2',
                   DateTime(timezone=False)))
        metadata.create_all()
        m2 = MetaData(testing.db)
        t2 = Table('pgdate', m2, autoload=True)
        assert t2.c.date1.type.timezone is True
        assert t2.c.date2.type.timezone is False

    @testing.fails_on('+zxjdbc',
                      'The JDBC driver handles the version parsing')
    def test_version_parsing(self):

        def mock_conn(res):
            return Mock(
                    execute=Mock(
                            return_value=Mock(scalar=Mock(return_value=res))
                        )
                    )

        for string, version in \
            [('PostgreSQL 8.3.8 on i686-redhat-linux-gnu, compiled by '
             'GCC gcc (GCC) 4.1.2 20070925 (Red Hat 4.1.2-33)', (8, 3,
             8)),
             ('PostgreSQL 8.5devel on x86_64-unknown-linux-gnu, '
             'compiled by GCC gcc (GCC) 4.4.2, 64-bit', (8, 5)),
             ('EnterpriseDB 9.1.2.2 on x86_64-unknown-linux-gnu, '
             'compiled by gcc (GCC) 4.1.2 20080704 (Red Hat 4.1.2-50), '
             '64-bit', (9, 1, 2))]:
            eq_(testing.db.dialect._get_server_version_info(mock_conn(string)),
                version)

    @testing.only_on('postgresql+psycopg2', 'psycopg2-specific feature')
    def test_psycopg2_version(self):
        v = testing.db.dialect.psycopg2_version
        assert testing.db.dialect.dbapi.__version__.\
                    startswith(".".join(str(x) for x in v))

    @testing.only_on('postgresql+psycopg2', 'psycopg2-specific feature')
    def test_notice_logging(self):
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

    @testing.only_on('postgresql+psycopg2', 'psycopg2-specific feature')
    @engines.close_open_connections
    def test_client_encoding(self):
        c = testing.db.connect()
        current_encoding = c.connection.connection.encoding
        c.close()

        # attempt to use an encoding that's not
        # already set
        if current_encoding == 'UTF8':
            test_encoding = 'LATIN1'
        else:
            test_encoding = 'UTF8'

        e = engines.testing_engine(
                        options={'client_encoding':test_encoding}
                    )
        c = e.connect()
        eq_(c.connection.connection.encoding, test_encoding)

    @testing.only_on('postgresql+psycopg2', 'psycopg2-specific feature')
    @engines.close_open_connections
    def test_autocommit_isolation_level(self):
        extensions = __import__('psycopg2.extensions').extensions

        c = testing.db.connect()
        c = c.execution_options(isolation_level='AUTOCOMMIT')
        eq_(c.connection.connection.isolation_level,
            extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    @testing.fails_on('+zxjdbc',
                      "Can't infer the SQL type to use for an instance "
                      "of org.python.core.PyObjectDerived.")
    @testing.fails_on('+pg8000', "Can't determine correct type.")
    def test_extract(self):
        fivedaysago = datetime.datetime.now() \
            - datetime.timedelta(days=5)
        for field, exp in ('year', fivedaysago.year), ('month',
                fivedaysago.month), ('day', fivedaysago.day):
            r = testing.db.execute(select([extract(field, func.now()
                                   + datetime.timedelta(days=-5))])).scalar()
            eq_(r, exp)

    def test_checksfor_sequence(self):
        meta1 = MetaData(testing.db)
        seq = Sequence('fooseq')
        t = Table('mytable', meta1, Column('col1', Integer,
                  seq))
        seq.drop()
        try:
            testing.db.execute('CREATE SEQUENCE fooseq')
            t.create(checkfirst=True)
        finally:
            t.drop(checkfirst=True)

    def test_schema_roundtrips(self):
        meta = MetaData(testing.db)
        users = Table('users', meta, Column('id', Integer,
                      primary_key=True), Column('name', String(50)),
                      schema='test_schema')
        users.create()
        try:
            users.insert().execute(id=1, name='name1')
            users.insert().execute(id=2, name='name2')
            users.insert().execute(id=3, name='name3')
            users.insert().execute(id=4, name='name4')
            eq_(users.select().where(users.c.name == 'name2'
                ).execute().fetchall(), [(2, 'name2')])
            eq_(users.select(use_labels=True).where(users.c.name
                == 'name2').execute().fetchall(), [(2, 'name2')])
            users.delete().where(users.c.id == 3).execute()
            eq_(users.select().where(users.c.name == 'name3'
                ).execute().fetchall(), [])
            users.update().where(users.c.name == 'name4'
                                 ).execute(name='newname')
            eq_(users.select(use_labels=True).where(users.c.id
                == 4).execute().fetchall(), [(4, 'newname')])
        finally:
            users.drop()

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
            l = t.select().execute().fetchall()
            assert l == [(1, 'user', 'lala')]
        finally:
            testing.db.execute('drop table speedy_users')


    @testing.fails_on('+zxjdbc', 'psycopg2/pg8000 specific assertion')
    @testing.fails_on('pypostgresql',
                      'psycopg2/pg8000 specific assertion')
    def test_numeric_raise(self):
        stmt = text("select cast('hi' as char) as hi", typemap={'hi'
                    : Numeric})
        assert_raises(exc.InvalidRequestError, testing.db.execute, stmt)

    def test_serial_integer(self):
        for type_, expected in [
            (Integer, 'SERIAL'),
            (BigInteger, 'BIGSERIAL'),
            (SmallInteger, 'SMALLINT'),
            (postgresql.INTEGER, 'SERIAL'),
            (postgresql.BIGINT, 'BIGSERIAL'),
        ]:
            m = MetaData()

            t = Table('t', m, Column('c', type_, primary_key=True))
            ddl_compiler = testing.db.dialect.ddl_compiler(testing.db.dialect, schema.CreateTable(t))
            eq_(
                ddl_compiler.get_column_specification(t.c.c),
                "c %s NOT NULL" % expected
            )
