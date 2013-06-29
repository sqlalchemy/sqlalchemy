# coding: utf-8
from __future__ import with_statement
from sqlalchemy.testing.assertions import eq_, assert_raises, \
                assert_raises_message, is_, AssertsExecutionResults, \
                AssertsCompiledSQL, ComparesTables
from sqlalchemy.testing import engines, fixtures
from sqlalchemy import testing
from sqlalchemy import Table, Column, select, MetaData, text, Integer, \
            String, Sequence, ForeignKey, join, Numeric, \
            PrimaryKeyConstraint, DateTime, tuple_, Float, BigInteger, \
            func, literal_column, literal, bindparam, cast, extract, \
            SmallInteger, Enum, REAL, update, insert, Index, delete, \
            and_, Date, TypeDecorator, Time, Unicode, Interval, or_, Text
from sqlalchemy import exc
from sqlalchemy.dialects.postgresql import base as postgresql
import logging

class DomainReflectionTest(fixtures.TestBase, AssertsExecutionResults):

    """Test PostgreSQL domains"""

    __only_on__ = 'postgresql'

    @classmethod
    def setup_class(cls):
        con = testing.db.connect()
        for ddl in \
            'CREATE DOMAIN testdomain INTEGER NOT NULL DEFAULT 42', \
            'CREATE DOMAIN test_schema.testdomain INTEGER DEFAULT 0', \
            "CREATE TYPE testtype AS ENUM ('test')", \
            'CREATE DOMAIN enumdomain AS testtype'\
            :
            try:
                con.execute(ddl)
            except exc.DBAPIError, e:
                if not 'already exists' in str(e):
                    raise e
        con.execute('CREATE TABLE testtable (question integer, answer '
                    'testdomain)')
        con.execute('CREATE TABLE test_schema.testtable(question '
                    'integer, answer test_schema.testdomain, anything '
                    'integer)')
        con.execute('CREATE TABLE crosschema (question integer, answer '
                    'test_schema.testdomain)')

        con.execute('CREATE TABLE enum_test (id integer, data enumdomain)')

    @classmethod
    def teardown_class(cls):
        con = testing.db.connect()
        con.execute('DROP TABLE testtable')
        con.execute('DROP TABLE test_schema.testtable')
        con.execute('DROP TABLE crosschema')
        con.execute('DROP DOMAIN testdomain')
        con.execute('DROP DOMAIN test_schema.testdomain')
        con.execute("DROP TABLE enum_test")
        con.execute("DROP DOMAIN enumdomain")
        con.execute("DROP TYPE testtype")

    def test_table_is_reflected(self):
        metadata = MetaData(testing.db)
        table = Table('testtable', metadata, autoload=True)
        eq_(set(table.columns.keys()), set(['question', 'answer']),
            "Columns of reflected table didn't equal expected columns")
        assert isinstance(table.c.answer.type, Integer)

    def test_domain_is_reflected(self):
        metadata = MetaData(testing.db)
        table = Table('testtable', metadata, autoload=True)
        eq_(str(table.columns.answer.server_default.arg), '42',
            "Reflected default value didn't equal expected value")
        assert not table.columns.answer.nullable, \
            'Expected reflected column to not be nullable.'

    def test_enum_domain_is_reflected(self):
        metadata = MetaData(testing.db)
        table = Table('enum_test', metadata, autoload=True)
        eq_(
            table.c.data.type.enums,
            ('test', )
        )

    def test_table_is_reflected_test_schema(self):
        metadata = MetaData(testing.db)
        table = Table('testtable', metadata, autoload=True,
                      schema='test_schema')
        eq_(set(table.columns.keys()), set(['question', 'answer',
            'anything']),
            "Columns of reflected table didn't equal expected columns")
        assert isinstance(table.c.anything.type, Integer)

    def test_schema_domain_is_reflected(self):
        metadata = MetaData(testing.db)
        table = Table('testtable', metadata, autoload=True,
                      schema='test_schema')
        eq_(str(table.columns.answer.server_default.arg), '0',
            "Reflected default value didn't equal expected value")
        assert table.columns.answer.nullable, \
            'Expected reflected column to be nullable.'

    def test_crosschema_domain_is_reflected(self):
        metadata = MetaData(testing.db)
        table = Table('crosschema', metadata, autoload=True)
        eq_(str(table.columns.answer.server_default.arg), '0',
            "Reflected default value didn't equal expected value")
        assert table.columns.answer.nullable, \
            'Expected reflected column to be nullable.'

    def test_unknown_types(self):
        from sqlalchemy.databases import postgresql
        ischema_names = postgresql.PGDialect.ischema_names
        postgresql.PGDialect.ischema_names = {}
        try:
            m2 = MetaData(testing.db)
            assert_raises(exc.SAWarning, Table, 'testtable', m2,
                          autoload=True)

            @testing.emits_warning('Did not recognize type')
            def warns():
                m3 = MetaData(testing.db)
                t3 = Table('testtable', m3, autoload=True)
                assert t3.c.answer.type.__class__ == sa.types.NullType
        finally:
            postgresql.PGDialect.ischema_names = ischema_names


class ReflectionTest(fixtures.TestBase):
    __only_on__ = 'postgresql'

    @testing.fails_if(('postgresql', '<', (8, 4)),
            "newer query is bypassed due to unsupported SQL functions")
    @testing.provide_metadata
    def test_reflected_primary_key_order(self):
        meta1 = self.metadata
        subject = Table('subject', meta1,
                        Column('p1', Integer, primary_key=True),
                        Column('p2', Integer, primary_key=True),
                        PrimaryKeyConstraint('p2', 'p1')
                        )
        meta1.create_all()
        meta2 = MetaData(testing.db)
        subject = Table('subject', meta2, autoload=True)
        eq_(subject.primary_key.columns.keys(), ['p2', 'p1'])

    @testing.provide_metadata
    def test_pg_weirdchar_reflection(self):
        meta1 = self.metadata
        subject = Table('subject', meta1, Column('id$', Integer,
                        primary_key=True))
        referer = Table('referer', meta1, Column('id', Integer,
                        primary_key=True), Column('ref', Integer,
                        ForeignKey('subject.id$')))
        meta1.create_all()
        meta2 = MetaData(testing.db)
        subject = Table('subject', meta2, autoload=True)
        referer = Table('referer', meta2, autoload=True)
        self.assert_((subject.c['id$']
                     == referer.c.ref).compare(
                        subject.join(referer).onclause))

    @testing.provide_metadata
    def test_renamed_sequence_reflection(self):
        metadata = self.metadata
        t = Table('t', metadata, Column('id', Integer, primary_key=True))
        metadata.create_all()
        m2 = MetaData(testing.db)
        t2 = Table('t', m2, autoload=True, implicit_returning=False)
        eq_(t2.c.id.server_default.arg.text,
            "nextval('t_id_seq'::regclass)")
        r = t2.insert().execute()
        eq_(r.inserted_primary_key, [1])
        testing.db.connect().execution_options(autocommit=True).\
                execute('alter table t_id_seq rename to foobar_id_seq'
                )
        m3 = MetaData(testing.db)
        t3 = Table('t', m3, autoload=True, implicit_returning=False)
        eq_(t3.c.id.server_default.arg.text,
            "nextval('foobar_id_seq'::regclass)")
        r = t3.insert().execute()
        eq_(r.inserted_primary_key, [2])

    @testing.provide_metadata
    def test_renamed_pk_reflection(self):
        metadata = self.metadata
        t = Table('t', metadata, Column('id', Integer, primary_key=True))
        metadata.create_all()
        testing.db.connect().execution_options(autocommit=True).\
            execute('alter table t rename id to t_id')
        m2 = MetaData(testing.db)
        t2 = Table('t', m2, autoload=True)
        eq_([c.name for c in t2.primary_key], ['t_id'])

    @testing.provide_metadata
    def test_schema_reflection(self):
        """note: this test requires that the 'test_schema' schema be
        separate and accessible by the test user"""

        meta1 = self.metadata

        users = Table('users', meta1, Column('user_id', Integer,
                      primary_key=True), Column('user_name',
                      String(30), nullable=False), schema='test_schema')
        addresses = Table(
            'email_addresses',
            meta1,
            Column('address_id', Integer, primary_key=True),
            Column('remote_user_id', Integer,
                   ForeignKey(users.c.user_id)),
            Column('email_address', String(20)),
            schema='test_schema',
            )
        meta1.create_all()
        meta2 = MetaData(testing.db)
        addresses = Table('email_addresses', meta2, autoload=True,
                          schema='test_schema')
        users = Table('users', meta2, mustexist=True,
                      schema='test_schema')
        j = join(users, addresses)
        self.assert_((users.c.user_id
                     == addresses.c.remote_user_id).compare(j.onclause))

    @testing.provide_metadata
    def test_schema_reflection_2(self):
        meta1 = self.metadata
        subject = Table('subject', meta1, Column('id', Integer,
                        primary_key=True))
        referer = Table('referer', meta1, Column('id', Integer,
                        primary_key=True), Column('ref', Integer,
                        ForeignKey('subject.id')), schema='test_schema')
        meta1.create_all()
        meta2 = MetaData(testing.db)
        subject = Table('subject', meta2, autoload=True)
        referer = Table('referer', meta2, schema='test_schema',
                        autoload=True)
        self.assert_((subject.c.id
                     == referer.c.ref).compare(
                        subject.join(referer).onclause))

    @testing.provide_metadata
    def test_schema_reflection_3(self):
        meta1 = self.metadata
        subject = Table('subject', meta1, Column('id', Integer,
                        primary_key=True), schema='test_schema_2')
        referer = Table('referer', meta1, Column('id', Integer,
                        primary_key=True), Column('ref', Integer,
                        ForeignKey('test_schema_2.subject.id')),
                        schema='test_schema')
        meta1.create_all()
        meta2 = MetaData(testing.db)
        subject = Table('subject', meta2, autoload=True,
                        schema='test_schema_2')
        referer = Table('referer', meta2, schema='test_schema',
                        autoload=True)
        self.assert_((subject.c.id
                     == referer.c.ref).compare(
                        subject.join(referer).onclause))

    @testing.provide_metadata
    def test_uppercase_lowercase_table(self):
        metadata = self.metadata

        a_table = Table('a', metadata, Column('x', Integer))
        A_table = Table('A', metadata, Column('x', Integer))

        a_table.create()
        assert testing.db.has_table("a")
        assert not testing.db.has_table("A")
        A_table.create(checkfirst=True)
        assert testing.db.has_table("A")

    def test_uppercase_lowercase_sequence(self):

        a_seq = Sequence('a')
        A_seq = Sequence('A')

        a_seq.create(testing.db)
        assert testing.db.dialect.has_sequence(testing.db, "a")
        assert not testing.db.dialect.has_sequence(testing.db, "A")
        A_seq.create(testing.db, checkfirst=True)
        assert testing.db.dialect.has_sequence(testing.db, "A")

        a_seq.drop(testing.db)
        A_seq.drop(testing.db)

    def test_schema_reflection_multi_search_path(self):
        """test the 'set the same schema' rule when
        multiple schemas/search paths are in effect."""

        db = engines.testing_engine()
        conn = db.connect()
        trans = conn.begin()
        try:
            conn.execute("set search_path to test_schema_2, "
                                "test_schema, public")
            conn.dialect.default_schema_name = "test_schema_2"

            conn.execute("""
            CREATE TABLE test_schema.some_table (
                id SERIAL not null primary key
            )
            """)

            conn.execute("""
            CREATE TABLE test_schema_2.some_other_table (
                id SERIAL not null primary key,
                sid INTEGER REFERENCES test_schema.some_table(id)
            )
            """)

            m1 = MetaData()

            t2_schema = Table('some_other_table',
                                m1,
                                schema="test_schema_2",
                                autoload=True,
                                autoload_with=conn)
            t1_schema = Table('some_table',
                                m1,
                                schema="test_schema",
                                autoload=True,
                                autoload_with=conn)

            t2_no_schema = Table('some_other_table',
                                m1,
                                autoload=True,
                                autoload_with=conn)

            t1_no_schema = Table('some_table',
                                m1,
                                autoload=True,
                                autoload_with=conn)

            # OK, this because, "test_schema" is
            # in the search path, and might as well be
            # the default too.  why would we assign
            # a "schema" to the Table ?
            assert t2_schema.c.sid.references(
                                t1_no_schema.c.id)

            assert t2_no_schema.c.sid.references(
                                t1_no_schema.c.id)

        finally:
            trans.rollback()
            conn.close()
            db.dispose()

    @testing.provide_metadata
    def test_index_reflection(self):
        """ Reflecting partial & expression-based indexes should warn
        """

        metadata = self.metadata

        t1 = Table('party', metadata, Column('id', String(10),
                   nullable=False), Column('name', String(20),
                   index=True), Column('aname', String(20)))
        metadata.create_all()
        testing.db.execute("""
          create index idx1 on party ((id || name))
        """)
        testing.db.execute("""
          create unique index idx2 on party (id) where name = 'test'
        """)
        testing.db.execute("""
            create index idx3 on party using btree
                (lower(name::text), lower(aname::text))
        """)

        def go():
            m2 = MetaData(testing.db)
            t2 = Table('party', m2, autoload=True)
            assert len(t2.indexes) == 2

            # Make sure indexes are in the order we expect them in

            tmp = [(idx.name, idx) for idx in t2.indexes]
            tmp.sort()
            r1, r2 = [idx[1] for idx in tmp]
            assert r1.name == 'idx2'
            assert r1.unique == True
            assert r2.unique == False
            assert [t2.c.id] == r1.columns
            assert [t2.c.name] == r2.columns

        testing.assert_warnings(go,
            [
                'Skipped unsupported reflection of '
                'expression-based index idx1',
                'Predicate of partial index idx2 ignored during '
                'reflection',
                'Skipped unsupported reflection of '
                'expression-based index idx3'
            ])

    @testing.provide_metadata
    def test_index_reflection_modified(self):
        """reflect indexes when a column name has changed - PG 9
        does not update the name of the column in the index def.
        [ticket:2141]

        """

        metadata = self.metadata

        t1 = Table('t', metadata,
            Column('id', Integer, primary_key=True),
            Column('x', Integer)
        )
        metadata.create_all()
        conn = testing.db.connect().execution_options(autocommit=True)
        conn.execute("CREATE INDEX idx1 ON t (x)")
        conn.execute("ALTER TABLE t RENAME COLUMN x to y")

        ind = testing.db.dialect.get_indexes(conn, "t", None)
        eq_(ind, [{'unique': False, 'column_names': ['y'], 'name': 'idx1'}])
        conn.close()

class CustomTypeReflectionTest(fixtures.TestBase):

    class CustomType(object):
        def __init__(self, arg1=None, arg2=None):
            self.arg1 = arg1
            self.arg2 = arg2

    ischema_names = None

    def setup(self):
        ischema_names = postgresql.PGDialect.ischema_names
        postgresql.PGDialect.ischema_names = ischema_names.copy()
        self.ischema_names = ischema_names

    def teardown(self):
        postgresql.PGDialect.ischema_names = self.ischema_names
        self.ischema_names = None

    def _assert_reflected(self, dialect):
        for sch, args in [
            ('my_custom_type', (None, None)),
            ('my_custom_type()', (None, None)),
            ('my_custom_type(ARG1)', ('ARG1', None)),
            ('my_custom_type(ARG1, ARG2)', ('ARG1', 'ARG2')),
        ]:
            column_info = dialect._get_column_info(
                'colname', sch, None, False,
                {}, {}, 'public')
            assert isinstance(column_info['type'], self.CustomType)
            eq_(column_info['type'].arg1, args[0])
            eq_(column_info['type'].arg2, args[1])

    def test_clslevel(self):
        postgresql.PGDialect.ischema_names['my_custom_type'] = self.CustomType
        dialect = postgresql.PGDialect()
        self._assert_reflected(dialect)

    def test_instancelevel(self):
        dialect = postgresql.PGDialect()
        dialect.ischema_names = dialect.ischema_names.copy()
        dialect.ischema_names['my_custom_type'] = self.CustomType
        self._assert_reflected(dialect)
