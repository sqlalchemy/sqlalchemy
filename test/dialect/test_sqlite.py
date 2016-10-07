#!coding: utf-8

"""SQLite-specific tests."""
import os
import datetime

from sqlalchemy.testing import eq_, assert_raises, \
    assert_raises_message, is_, expect_warnings
from sqlalchemy import Table, select, bindparam, Column,\
    MetaData, func, extract, ForeignKey, text, DefaultClause, and_, \
    create_engine, UniqueConstraint, Index, PrimaryKeyConstraint
from sqlalchemy.types import Integer, String, Boolean, DateTime, Date, Time
from sqlalchemy import types as sqltypes
from sqlalchemy import event, inspect
from sqlalchemy.util import u, ue
from sqlalchemy import exc, sql, schema, pool, util
from sqlalchemy.dialects.sqlite import base as sqlite, \
    pysqlite as pysqlite_dialect
from sqlalchemy.engine.url import make_url
from sqlalchemy.testing import fixtures, AssertsCompiledSQL, \
    AssertsExecutionResults, engines
from sqlalchemy import testing
from sqlalchemy.schema import CreateTable, FetchedValue
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.testing import mock


class TestTypes(fixtures.TestBase, AssertsExecutionResults):

    __only_on__ = 'sqlite'

    def test_boolean(self):
        """Test that the boolean only treats 1 as True

        """

        meta = MetaData(testing.db)
        t = Table(
            'bool_table', meta,
            Column('id', Integer, primary_key=True),
            Column('boo', Boolean(create_constraint=False)))
        try:
            meta.create_all()
            testing.db.execute("INSERT INTO bool_table (id, boo) "
                               "VALUES (1, 'false');")
            testing.db.execute("INSERT INTO bool_table (id, boo) "
                               "VALUES (2, 'true');")
            testing.db.execute("INSERT INTO bool_table (id, boo) "
                               "VALUES (3, '1');")
            testing.db.execute("INSERT INTO bool_table (id, boo) "
                               "VALUES (4, '0');")
            testing.db.execute('INSERT INTO bool_table (id, boo) '
                               'VALUES (5, 1);')
            testing.db.execute('INSERT INTO bool_table (id, boo) '
                               'VALUES (6, 0);')
            eq_(t.select(t.c.boo).order_by(t.c.id).execute().fetchall(),
                [(3, True), (5, True)])
        finally:
            meta.drop_all()

    def test_string_dates_passed_raise(self):
        assert_raises(exc.StatementError, testing.db.execute,
                      select([1]).where(bindparam('date', type_=Date)),
                      date=str(datetime.date(2007, 10, 30)))

    def test_cant_parse_datetime_message(self):
        for (typ, disp) in [
            (Time, "time"),
            (DateTime, "datetime"),
            (Date, "date")
        ]:
            assert_raises_message(
                ValueError,
                "Couldn't parse %s string." % disp,
                lambda: testing.db.execute(
                    text("select 'ASDF' as value", typemap={"value": typ})
                ).scalar()
            )

    def test_native_datetime(self):
        dbapi = testing.db.dialect.dbapi
        connect_args = {
            'detect_types': dbapi.PARSE_DECLTYPES | dbapi.PARSE_COLNAMES}
        engine = engines.testing_engine(
            options={'connect_args': connect_args, 'native_datetime': True})
        t = Table(
            'datetest', MetaData(),
            Column('id', Integer, primary_key=True),
            Column('d1', Date), Column('d2', sqltypes.TIMESTAMP))
        t.create(engine)
        try:
            engine.execute(t.insert(), {
                'd1': datetime.date(2010, 5, 10),
                'd2': datetime.datetime(2010, 5, 10, 12, 15, 25)
            })
            row = engine.execute(t.select()).first()
            eq_(
                row,
                (1, datetime.date(2010, 5, 10),
                    datetime.datetime(2010, 5, 10, 12, 15, 25)))
            r = engine.execute(func.current_date()).scalar()
            assert isinstance(r, util.string_types)
        finally:
            t.drop(engine)
            engine.dispose()

    @testing.provide_metadata
    def test_custom_datetime(self):
        sqlite_date = sqlite.DATETIME(
            # 2004-05-21T00:00:00
            storage_format="%(year)04d-%(month)02d-%(day)02d"
            "T%(hour)02d:%(minute)02d:%(second)02d",
            regexp=r"(\d+)-(\d+)-(\d+)T(\d+):(\d+):(\d+)",
        )
        t = Table('t', self.metadata, Column('d', sqlite_date))
        self.metadata.create_all(testing.db)
        testing.db.execute(
            t.insert().
            values(d=datetime.datetime(2010, 10, 15, 12, 37, 0)))
        testing.db.execute("insert into t (d) values ('2004-05-21T00:00:00')")
        eq_(
            testing.db.execute("select * from t order by d").fetchall(),
            [('2004-05-21T00:00:00',), ('2010-10-15T12:37:00',)]
        )
        eq_(
            testing.db.execute(select([t.c.d]).order_by(t.c.d)).fetchall(),
            [
                (datetime.datetime(2004, 5, 21, 0, 0),),
                (datetime.datetime(2010, 10, 15, 12, 37),)]
        )

    @testing.provide_metadata
    def test_custom_datetime_text_affinity(self):
        sqlite_date = sqlite.DATETIME(
            storage_format="%(year)04d%(month)02d%(day)02d"
            "%(hour)02d%(minute)02d%(second)02d",
            regexp=r"(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})",
        )
        t = Table('t', self.metadata, Column('d', sqlite_date))
        self.metadata.create_all(testing.db)
        testing.db.execute(
            t.insert().
            values(d=datetime.datetime(2010, 10, 15, 12, 37, 0)))
        testing.db.execute("insert into t (d) values ('20040521000000')")
        eq_(
            testing.db.execute("select * from t order by d").fetchall(),
            [('20040521000000',), ('20101015123700',)]
        )
        eq_(
            testing.db.execute(select([t.c.d]).order_by(t.c.d)).fetchall(),
            [
                (datetime.datetime(2004, 5, 21, 0, 0),),
                (datetime.datetime(2010, 10, 15, 12, 37),)]
        )

    @testing.provide_metadata
    def test_custom_date_text_affinity(self):
        sqlite_date = sqlite.DATE(
            storage_format="%(year)04d%(month)02d%(day)02d",
            regexp=r"(\d{4})(\d{2})(\d{2})",
        )
        t = Table('t', self.metadata, Column('d', sqlite_date))
        self.metadata.create_all(testing.db)
        testing.db.execute(
            t.insert().
            values(d=datetime.date(2010, 10, 15)))
        testing.db.execute("insert into t (d) values ('20040521')")
        eq_(
            testing.db.execute("select * from t order by d").fetchall(),
            [('20040521',), ('20101015',)]
        )
        eq_(
            testing.db.execute(select([t.c.d]).order_by(t.c.d)).fetchall(),
            [
                (datetime.date(2004, 5, 21),),
                (datetime.date(2010, 10, 15),)]
        )

    @testing.provide_metadata
    def test_custom_date(self):
        sqlite_date = sqlite.DATE(
            # 2004-05-21T00:00:00
            storage_format="%(year)04d|%(month)02d|%(day)02d",
            regexp=r"(\d+)\|(\d+)\|(\d+)",
        )
        t = Table('t', self.metadata, Column('d', sqlite_date))
        self.metadata.create_all(testing.db)
        testing.db.execute(
            t.insert().
            values(d=datetime.date(2010, 10, 15)))
        testing.db.execute("insert into t (d) values ('2004|05|21')")
        eq_(
            testing.db.execute("select * from t order by d").fetchall(),
            [('2004|05|21',), ('2010|10|15',)]
        )
        eq_(
            testing.db.execute(select([t.c.d]).order_by(t.c.d)).fetchall(),
            [
                (datetime.date(2004, 5, 21),),
                (datetime.date(2010, 10, 15),)]
        )

    def test_no_convert_unicode(self):
        """test no utf-8 encoding occurs"""

        dialect = sqlite.dialect()
        for t in (
            String(convert_unicode=True),
            sqltypes.CHAR(convert_unicode=True),
            sqltypes.Unicode(),
            sqltypes.UnicodeText(),
            String(convert_unicode=True),
            sqltypes.CHAR(convert_unicode=True),
            sqltypes.Unicode(),
            sqltypes.UnicodeText(),
        ):
            bindproc = t.dialect_impl(dialect).bind_processor(dialect)
            assert not bindproc or \
                isinstance(bindproc(util.u('some string')), util.text_type)


class DateTimeTest(fixtures.TestBase, AssertsCompiledSQL):

    def test_time_microseconds(self):
        dt = datetime.datetime(2008, 6, 27, 12, 0, 0, 125, )
        eq_(str(dt), '2008-06-27 12:00:00.000125')
        sldt = sqlite.DATETIME()
        bp = sldt.bind_processor(None)
        eq_(bp(dt), '2008-06-27 12:00:00.000125')
        rp = sldt.result_processor(None, None)
        eq_(rp(bp(dt)), dt)

    def test_truncate_microseconds(self):
        dt = datetime.datetime(2008, 6, 27, 12, 0, 0, 125)
        dt_out = datetime.datetime(2008, 6, 27, 12, 0, 0)
        eq_(str(dt), '2008-06-27 12:00:00.000125')
        sldt = sqlite.DATETIME(truncate_microseconds=True)
        bp = sldt.bind_processor(None)
        eq_(bp(dt), '2008-06-27 12:00:00')
        rp = sldt.result_processor(None, None)
        eq_(rp(bp(dt)), dt_out)

    def test_custom_format_compact(self):
        dt = datetime.datetime(2008, 6, 27, 12, 0, 0, 125)
        eq_(str(dt), '2008-06-27 12:00:00.000125')
        sldt = sqlite.DATETIME(
            storage_format=(
                "%(year)04d%(month)02d%(day)02d"
                "%(hour)02d%(minute)02d%(second)02d%(microsecond)06d"
            ),
            regexp="(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})(\d{6})",
        )
        bp = sldt.bind_processor(None)
        eq_(bp(dt), '20080627120000000125')
        rp = sldt.result_processor(None, None)
        eq_(rp(bp(dt)), dt)


class DateTest(fixtures.TestBase, AssertsCompiledSQL):

    def test_default(self):
        dt = datetime.date(2008, 6, 27)
        eq_(str(dt), '2008-06-27')
        sldt = sqlite.DATE()
        bp = sldt.bind_processor(None)
        eq_(bp(dt), '2008-06-27')
        rp = sldt.result_processor(None, None)
        eq_(rp(bp(dt)), dt)

    def test_custom_format(self):
        dt = datetime.date(2008, 6, 27)
        eq_(str(dt), '2008-06-27')
        sldt = sqlite.DATE(
            storage_format="%(month)02d/%(day)02d/%(year)04d",
            regexp="(?P<month>\d+)/(?P<day>\d+)/(?P<year>\d+)",
        )
        bp = sldt.bind_processor(None)
        eq_(bp(dt), '06/27/2008')
        rp = sldt.result_processor(None, None)
        eq_(rp(bp(dt)), dt)


class TimeTest(fixtures.TestBase, AssertsCompiledSQL):

    def test_default(self):
        dt = datetime.date(2008, 6, 27)
        eq_(str(dt), '2008-06-27')
        sldt = sqlite.DATE()
        bp = sldt.bind_processor(None)
        eq_(bp(dt), '2008-06-27')
        rp = sldt.result_processor(None, None)
        eq_(rp(bp(dt)), dt)

    def test_truncate_microseconds(self):
        dt = datetime.time(12, 0, 0, 125)
        dt_out = datetime.time(12, 0, 0)
        eq_(str(dt), '12:00:00.000125')
        sldt = sqlite.TIME(truncate_microseconds=True)
        bp = sldt.bind_processor(None)
        eq_(bp(dt), '12:00:00')
        rp = sldt.result_processor(None, None)
        eq_(rp(bp(dt)), dt_out)

    def test_custom_format(self):
        dt = datetime.date(2008, 6, 27)
        eq_(str(dt), '2008-06-27')
        sldt = sqlite.DATE(
            storage_format="%(year)04d%(month)02d%(day)02d",
            regexp="(\d{4})(\d{2})(\d{2})",
        )
        bp = sldt.bind_processor(None)
        eq_(bp(dt), '20080627')
        rp = sldt.result_processor(None, None)
        eq_(rp(bp(dt)), dt)


class DefaultsTest(fixtures.TestBase, AssertsCompiledSQL):

    __only_on__ = 'sqlite'

    @testing.exclude('sqlite', '<', (3, 3, 8),
                     'sqlite3 changesets 3353 and 3440 modified '
                     'behavior of default displayed in pragma '
                     'table_info()')
    def test_default_reflection(self):

        # (ask_for, roundtripped_as_if_different)

        specs = [(String(3), '"foo"'), (sqltypes.NUMERIC(10, 2), '100.50'),
                 (Integer, '5'), (Boolean, 'False')]
        columns = [Column('c%i' % (i + 1), t[0],
                   server_default=text(t[1])) for (i, t) in
                   enumerate(specs)]
        db = testing.db
        m = MetaData(db)
        Table('t_defaults', m, *columns)
        try:
            m.create_all()
            m2 = MetaData(db)
            rt = Table('t_defaults', m2, autoload=True)
            expected = [c[1] for c in specs]
            for i, reflected in enumerate(rt.c):
                eq_(str(reflected.server_default.arg), expected[i])
        finally:
            m.drop_all()

    @testing.exclude('sqlite', '<', (3, 3, 8),
                     'sqlite3 changesets 3353 and 3440 modified '
                     'behavior of default displayed in pragma '
                     'table_info()')
    def test_default_reflection_2(self):

        db = testing.db
        m = MetaData(db)
        expected = ["'my_default'", '0']
        table = \
            """CREATE TABLE r_defaults (
            data VARCHAR(40) DEFAULT 'my_default',
            val INTEGER NOT NULL DEFAULT 0
            )"""
        try:
            db.execute(table)
            rt = Table('r_defaults', m, autoload=True)
            for i, reflected in enumerate(rt.c):
                eq_(str(reflected.server_default.arg), expected[i])
        finally:
            db.execute('DROP TABLE r_defaults')

    def test_default_reflection_3(self):
        db = testing.db
        table = \
            """CREATE TABLE r_defaults (
            data VARCHAR(40) DEFAULT 'my_default',
            val INTEGER NOT NULL DEFAULT 0
            )"""
        try:
            db.execute(table)
            m1 = MetaData(db)
            t1 = Table('r_defaults', m1, autoload=True)
            db.execute("DROP TABLE r_defaults")
            t1.create()
            m2 = MetaData(db)
            t2 = Table('r_defaults', m2, autoload=True)
            self.assert_compile(
                CreateTable(t2),
                "CREATE TABLE r_defaults (data VARCHAR(40) "
                "DEFAULT 'my_default', val INTEGER DEFAULT 0 "
                "NOT NULL)"
            )
        finally:
            db.execute("DROP TABLE r_defaults")

    @testing.provide_metadata
    def test_boolean_default(self):
        t = Table(
            "t", self.metadata,
            Column("x", Boolean, server_default=sql.false()))
        t.create(testing.db)
        testing.db.execute(t.insert())
        testing.db.execute(t.insert().values(x=True))
        eq_(
            testing.db.execute(t.select().order_by(t.c.x)).fetchall(),
            [(False,), (True,)]
        )

    def test_old_style_default(self):
        """test non-quoted integer value on older sqlite pragma"""

        dialect = sqlite.dialect()
        info = dialect._get_column_info("foo", "INTEGER", False, 3, False)
        eq_(info['default'], '3')


class DialectTest(fixtures.TestBase, AssertsExecutionResults):

    __only_on__ = 'sqlite'

    def test_extra_reserved_words(self):
        """Tests reserved words in identifiers.

        'true', 'false', and 'column' are undocumented reserved words
        when used as column identifiers (as of 3.5.1).  Covering them
        here to ensure they remain in place if the dialect's
        reserved_words set is updated in the future. """

        meta = MetaData(testing.db)
        t = Table(
            'reserved',
            meta,
            Column('safe', Integer),
            Column('true', Integer),
            Column('false', Integer),
            Column('column', Integer),
        )
        try:
            meta.create_all()
            t.insert().execute(safe=1)
            list(t.select().execute())
        finally:
            meta.drop_all()

    @testing.provide_metadata
    def test_quoted_identifiers_functional_one(self):
        """Tests autoload of tables created with quoted column names."""

        metadata = self.metadata
        testing.db.execute("""CREATE TABLE "django_content_type" (
            "id" integer NOT NULL PRIMARY KEY,
            "django_stuff" text NULL
        )
        """)
        testing.db.execute("""
        CREATE TABLE "django_admin_log" (
            "id" integer NOT NULL PRIMARY KEY,
            "action_time" datetime NOT NULL,
            "content_type_id" integer NULL
                    REFERENCES "django_content_type" ("id"),
            "object_id" text NULL,
            "change_message" text NOT NULL
        )
        """)
        table1 = Table('django_admin_log', metadata, autoload=True)
        table2 = Table('django_content_type', metadata, autoload=True)
        j = table1.join(table2)
        assert j.onclause.compare(
            table1.c.content_type_id == table2.c.id)

    @testing.provide_metadata
    def test_quoted_identifiers_functional_two(self):
        """"test the edgiest of edge cases, quoted table/col names
        that start and end with quotes.

        SQLite claims to have fixed this in
        http://www.sqlite.org/src/info/600482d161, however
        it still fails if the FK points to a table name that actually
        has quotes as part of its name.

        """

        metadata = self.metadata
        testing.db.execute(r'''CREATE TABLE """a""" (
            """id""" integer NOT NULL PRIMARY KEY
        )
        ''')

        # unfortunately, still can't do this; sqlite quadruples
        # up the quotes on the table name here for pragma foreign_key_list
        # testing.db.execute(r'''
        # CREATE TABLE """b""" (
        #    """id""" integer NOT NULL PRIMARY KEY,
        #    """aid""" integer NULL
        #           REFERENCES """a""" ("""id""")
        #)
        #''')

        table1 = Table(r'"a"', metadata, autoload=True)
        assert '"id"' in table1.c

        #table2 = Table(r'"b"', metadata, autoload=True)
        #j = table1.join(table2)
        # assert j.onclause.compare(table1.c['"id"']
        #        == table2.c['"aid"'])

    @testing.provide_metadata
    def test_description_encoding(self):
        # amazingly, pysqlite seems to still deliver cursor.description
        # as encoded bytes in py2k

        t = Table(
            'x', self.metadata,
            Column(u('méil'), Integer, primary_key=True),
            Column(ue('\u6e2c\u8a66'), Integer),
        )
        self.metadata.create_all(testing.db)

        result = testing.db.execute(t.select())
        assert u('méil') in result.keys()
        assert ue('\u6e2c\u8a66') in result.keys()

    def test_file_path_is_absolute(self):
        d = pysqlite_dialect.dialect()
        eq_(
            d.create_connect_args(make_url('sqlite:///foo.db')),
            ([os.path.abspath('foo.db')], {})
        )

    def test_pool_class(self):
        e = create_engine('sqlite+pysqlite://')
        assert e.pool.__class__ is pool.SingletonThreadPool

        e = create_engine('sqlite+pysqlite:///:memory:')
        assert e.pool.__class__ is pool.SingletonThreadPool

        e = create_engine('sqlite+pysqlite:///foo.db')
        assert e.pool.__class__ is pool.NullPool


class AttachedDBTest(fixtures.TestBase):
    __only_on__ = 'sqlite'

    def _fixture(self):
        meta = self.metadata
        self.conn = testing.db.connect()
        ct = Table(
            'created', meta,
            Column('id', Integer),
            Column('name', String),
            schema='test_schema')

        meta.create_all(self.conn)
        return ct

    def setup(self):
        self.conn = testing.db.connect()
        self.metadata = MetaData()

    def teardown(self):
        self.metadata.drop_all(self.conn)
        self.conn.close()

    def test_no_tables(self):
        insp = inspect(self.conn)
        eq_(insp.get_table_names("test_schema"), [])

    def test_table_names_present(self):
        self._fixture()
        insp = inspect(self.conn)
        eq_(insp.get_table_names("test_schema"), ["created"])

    def test_table_names_system(self):
        self._fixture()
        insp = inspect(self.conn)
        eq_(insp.get_table_names("test_schema"), ["created"])

    def test_schema_names(self):
        self._fixture()
        insp = inspect(self.conn)
        eq_(insp.get_schema_names(), ["main", "test_schema"])

        # implicitly creates a "temp" schema
        self.conn.execute("select * from sqlite_temp_master")

        # we're not including it
        insp = inspect(self.conn)
        eq_(insp.get_schema_names(), ["main", "test_schema"])

    def test_reflect_system_table(self):
        meta = MetaData(self.conn)
        alt_master = Table(
            'sqlite_master', meta, autoload=True,
            autoload_with=self.conn,
            schema='test_schema')
        assert len(alt_master.c) > 0

    def test_reflect_user_table(self):
        self._fixture()

        m2 = MetaData()
        c2 = Table('created', m2, autoload=True, autoload_with=self.conn)
        eq_(len(c2.c), 2)

    def test_crud(self):
        ct = self._fixture()

        self.conn.execute(ct.insert(), {'id': 1, 'name': 'foo'})
        eq_(
            self.conn.execute(ct.select()).fetchall(),
            [(1, 'foo')]
        )

        self.conn.execute(ct.update(), {'id': 2, 'name': 'bar'})
        eq_(
            self.conn.execute(ct.select()).fetchall(),
            [(2, 'bar')]
        )
        self.conn.execute(ct.delete())
        eq_(
            self.conn.execute(ct.select()).fetchall(),
            []
        )

    def test_col_targeting(self):
        ct = self._fixture()

        self.conn.execute(ct.insert(), {'id': 1, 'name': 'foo'})
        row = self.conn.execute(ct.select()).first()
        eq_(row['id'], 1)
        eq_(row['name'], 'foo')

    def test_col_targeting_union(self):
        ct = self._fixture()

        self.conn.execute(ct.insert(), {'id': 1, 'name': 'foo'})
        row = self.conn.execute(ct.select().union(ct.select())).first()
        eq_(row['id'], 1)
        eq_(row['name'], 'foo')


class SQLTest(fixtures.TestBase, AssertsCompiledSQL):

    """Tests SQLite-dialect specific compilation."""

    __dialect__ = sqlite.dialect()

    def test_extract(self):
        t = sql.table('t', sql.column('col1'))
        mapping = {
            'month': '%m',
            'day': '%d',
            'year': '%Y',
            'second': '%S',
            'hour': '%H',
            'doy': '%j',
            'minute': '%M',
            'epoch': '%s',
            'dow': '%w',
            'week': '%W',
        }
        for field, subst in mapping.items():
            self.assert_compile(select([extract(field, t.c.col1)]),
                                "SELECT CAST(STRFTIME('%s', t.col1) AS "
                                "INTEGER) AS anon_1 FROM t" % subst)

    def test_true_false(self):
        self.assert_compile(
            sql.false(), "0"
        )
        self.assert_compile(
            sql.true(),
            "1"
        )

    def test_is_distinct_from(self):
        self.assert_compile(
            sql.column('x').is_distinct_from(None),
            "x IS NOT NULL"
        )

        self.assert_compile(
            sql.column('x').isnot_distinct_from(False),
            "x IS 0"
        )

    def test_localtime(self):
        self.assert_compile(
            func.localtimestamp(),
            'DATETIME(CURRENT_TIMESTAMP, "localtime")'
        )

    def test_constraints_with_schemas(self):
        metadata = MetaData()
        Table(
            't1', metadata,
            Column('id', Integer, primary_key=True),
            schema='master')
        t2 = Table(
            't2', metadata,
            Column('id', Integer, primary_key=True),
            Column('t1_id', Integer, ForeignKey('master.t1.id')),
            schema='master'
        )
        t3 = Table(
            't3', metadata,
            Column('id', Integer, primary_key=True),
            Column('t1_id', Integer, ForeignKey('master.t1.id')),
            schema='alternate'
        )
        t4 = Table(
            't4', metadata,
            Column('id', Integer, primary_key=True),
            Column('t1_id', Integer, ForeignKey('master.t1.id')),
        )

        # schema->schema, generate REFERENCES with no schema name
        self.assert_compile(
            schema.CreateTable(t2),
            "CREATE TABLE master.t2 ("
            "id INTEGER NOT NULL, "
            "t1_id INTEGER, "
            "PRIMARY KEY (id), "
            "FOREIGN KEY(t1_id) REFERENCES t1 (id)"
            ")"
        )

        # schema->different schema, don't generate REFERENCES
        self.assert_compile(
            schema.CreateTable(t3),
            "CREATE TABLE alternate.t3 ("
            "id INTEGER NOT NULL, "
            "t1_id INTEGER, "
            "PRIMARY KEY (id)"
            ")"
        )

        # same for local schema
        self.assert_compile(
            schema.CreateTable(t4),
            "CREATE TABLE t4 ("
            "id INTEGER NOT NULL, "
            "t1_id INTEGER, "
            "PRIMARY KEY (id)"
            ")"
        )

    def test_create_partial_index(self):
        m = MetaData()
        tbl = Table('testtbl', m, Column('data', Integer))
        idx = Index('test_idx1', tbl.c.data,
                    sqlite_where=and_(tbl.c.data > 5, tbl.c.data < 10))

        # test quoting and all that

        idx2 = Index('test_idx2', tbl.c.data,
                     sqlite_where=and_(tbl.c.data > 'a', tbl.c.data
                                           < "b's"))
        self.assert_compile(schema.CreateIndex(idx),
                            'CREATE INDEX test_idx1 ON testtbl (data) '
                            'WHERE data > 5 AND data < 10',
                            dialect=sqlite.dialect())
        self.assert_compile(schema.CreateIndex(idx2),
                            "CREATE INDEX test_idx2 ON testtbl (data) "
                            "WHERE data > 'a' AND data < 'b''s'",
                            dialect=sqlite.dialect())

    def test_no_autoinc_on_composite_pk(self):
        m = MetaData()
        t = Table(
            't', m,
            Column('x', Integer, primary_key=True, autoincrement=True),
            Column('y', Integer, primary_key=True))
        assert_raises_message(
            exc.CompileError,
            "SQLite does not support autoincrement for composite",
            CreateTable(t).compile, dialect=sqlite.dialect()
        )


class InsertTest(fixtures.TestBase, AssertsExecutionResults):

    """Tests inserts and autoincrement."""

    __only_on__ = 'sqlite'

    # empty insert (i.e. INSERT INTO table DEFAULT VALUES) fails on
    # 3.3.7 and before

    def _test_empty_insert(self, table, expect=1):
        try:
            table.create()
            for wanted in expect, expect * 2:
                table.insert().execute()
                rows = table.select().execute().fetchall()
                eq_(len(rows), wanted)
        finally:
            table.drop()

    @testing.exclude('sqlite', '<', (3, 3, 8), 'no database support')
    def test_empty_insert_pk1(self):
        self._test_empty_insert(
            Table(
                'a', MetaData(testing.db),
                Column('id', Integer, primary_key=True)))

    @testing.exclude('sqlite', '<', (3, 3, 8), 'no database support')
    def test_empty_insert_pk2(self):
        # now warns due to [ticket:3216]

        with expect_warnings(
            "Column 'b.x' is marked as a member of the "
            "primary key for table 'b'",
            "Column 'b.y' is marked as a member of the "
            "primary key for table 'b'",
        ):
            assert_raises(
                exc.IntegrityError, self._test_empty_insert,
                Table(
                    'b', MetaData(testing.db),
                    Column('x', Integer, primary_key=True),
                    Column('y', Integer, primary_key=True)))

    @testing.exclude('sqlite', '<', (3, 3, 8), 'no database support')
    def test_empty_insert_pk2_fv(self):
        assert_raises(
            exc.DBAPIError, self._test_empty_insert,
            Table(
                'b', MetaData(testing.db),
                Column('x', Integer, primary_key=True,
                       server_default=FetchedValue()),
                Column('y', Integer, primary_key=True,
                       server_default=FetchedValue())))

    @testing.exclude('sqlite', '<', (3, 3, 8), 'no database support')
    def test_empty_insert_pk3(self):
        # now warns due to [ticket:3216]
        with expect_warnings(
            "Column 'c.x' is marked as a member of the primary key for table"
        ):
            assert_raises(
                exc.IntegrityError,
                self._test_empty_insert,
                Table(
                    'c', MetaData(testing.db),
                    Column('x', Integer, primary_key=True),
                    Column('y', Integer,
                           DefaultClause('123'), primary_key=True))
            )

    @testing.exclude('sqlite', '<', (3, 3, 8), 'no database support')
    def test_empty_insert_pk3_fv(self):
        assert_raises(
            exc.DBAPIError, self._test_empty_insert,
            Table(
                'c', MetaData(testing.db),
                Column('x', Integer, primary_key=True,
                       server_default=FetchedValue()),
                Column('y', Integer, DefaultClause('123'), primary_key=True)))

    @testing.exclude('sqlite', '<', (3, 3, 8), 'no database support')
    def test_empty_insert_pk4(self):
        self._test_empty_insert(
            Table(
                'd', MetaData(testing.db),
                Column('x', Integer, primary_key=True),
                Column('y', Integer, DefaultClause('123'))
            ))

    @testing.exclude('sqlite', '<', (3, 3, 8), 'no database support')
    def test_empty_insert_nopk1(self):
        self._test_empty_insert(Table('e', MetaData(testing.db),
                                Column('id', Integer)))

    @testing.exclude('sqlite', '<', (3, 3, 8), 'no database support')
    def test_empty_insert_nopk2(self):
        self._test_empty_insert(
            Table(
                'f', MetaData(testing.db),
                Column('x', Integer), Column('y', Integer)))

    def test_inserts_with_spaces(self):
        tbl = Table('tbl', MetaData('sqlite:///'), Column('with space',
                    Integer), Column('without', Integer))
        tbl.create()
        try:
            tbl.insert().execute({'without': 123})
            assert list(tbl.select().execute()) == [(None, 123)]
            tbl.insert().execute({'with space': 456})
            assert list(tbl.select().execute()) == [
                (None, 123), (456, None)]
        finally:
            tbl.drop()


def full_text_search_missing():
    """Test if full text search is not implemented and return False if
    it is and True otherwise."""

    try:
        testing.db.execute('CREATE VIRTUAL TABLE t using FTS3;')
        testing.db.execute('DROP TABLE t;')
        return False
    except:
        return True

metadata = cattable = matchtable = None


class MatchTest(fixtures.TestBase, AssertsCompiledSQL):

    __only_on__ = 'sqlite'
    __skip_if__ = full_text_search_missing,

    @classmethod
    def setup_class(cls):
        global metadata, cattable, matchtable
        metadata = MetaData(testing.db)
        testing.db.execute("""
        CREATE VIRTUAL TABLE cattable using FTS3 (
            id INTEGER NOT NULL,
            description VARCHAR(50),
            PRIMARY KEY (id)
        )
        """)
        cattable = Table('cattable', metadata, autoload=True)
        testing.db.execute("""
        CREATE VIRTUAL TABLE matchtable using FTS3 (
            id INTEGER NOT NULL,
            title VARCHAR(200),
            category_id INTEGER NOT NULL,
            PRIMARY KEY (id)
        )
        """)
        matchtable = Table('matchtable', metadata, autoload=True)
        metadata.create_all()
        cattable.insert().execute(
            [{'id': 1, 'description': 'Python'},
             {'id': 2, 'description': 'Ruby'}])
        matchtable.insert().execute(
            [
                {'id': 1, 'title': 'Agile Web Development with Rails',
                 'category_id': 2},
                {'id': 2, 'title': 'Dive Into Python', 'category_id': 1},
                {'id': 3, 'title': "Programming Matz's Ruby",
                 'category_id': 2},
                {'id': 4, 'title': 'The Definitive Guide to Django',
                 'category_id': 1},
                {'id': 5, 'title': 'Python in a Nutshell', 'category_id': 1}
            ])

    @classmethod
    def teardown_class(cls):
        metadata.drop_all()

    def test_expression(self):
        self.assert_compile(matchtable.c.title.match('somstr'),
                            'matchtable.title MATCH ?', dialect=sqlite.dialect())

    def test_simple_match(self):
        results = \
            matchtable.select().where(
                matchtable.c.title.match('python')).\
            order_by(matchtable.c.id).execute().fetchall()
        eq_([2, 5], [r.id for r in results])

    def test_simple_prefix_match(self):
        results = \
            matchtable.select().where(
                matchtable.c.title.match('nut*')).execute().fetchall()
        eq_([5], [r.id for r in results])

    def test_or_match(self):
        results2 = \
            matchtable.select().where(
                matchtable.c.title.match('nutshell OR ruby')).\
            order_by(matchtable.c.id).execute().fetchall()
        eq_([3, 5], [r.id for r in results2])

    def test_and_match(self):
        results2 = \
            matchtable.select().where(
                matchtable.c.title.match('python nutshell')
            ).execute().fetchall()
        eq_([5], [r.id for r in results2])

    def test_match_across_joins(self):
        results = matchtable.select().where(
            and_(
                cattable.c.id == matchtable.c.category_id,
                cattable.c.description.match('Ruby')
            )
        ).order_by(matchtable.c.id).execute().fetchall()
        eq_([1, 3], [r.id for r in results])


class AutoIncrementTest(fixtures.TestBase, AssertsCompiledSQL):

    def test_sqlite_autoincrement(self):
        table = Table('autoinctable', MetaData(), Column('id', Integer,
                      primary_key=True), Column('x', Integer,
                      default=None), sqlite_autoincrement=True)
        self.assert_compile(
            schema.CreateTable(table),
            'CREATE TABLE autoinctable (id INTEGER NOT '
            'NULL PRIMARY KEY AUTOINCREMENT, x INTEGER)',
            dialect=sqlite.dialect())

    def test_sqlite_autoincrement_constraint(self):
        table = Table(
            'autoinctable',
            MetaData(),
            Column('id', Integer, primary_key=True),
            Column('x', Integer, default=None),
            UniqueConstraint('x'),
            sqlite_autoincrement=True,
        )
        self.assert_compile(schema.CreateTable(table),
                            'CREATE TABLE autoinctable (id INTEGER NOT '
                            'NULL PRIMARY KEY AUTOINCREMENT, x '
                            'INTEGER, UNIQUE (x))',
                            dialect=sqlite.dialect())

    def test_sqlite_no_autoincrement(self):
        table = Table('noautoinctable', MetaData(), Column('id',
                      Integer, primary_key=True), Column('x', Integer,
                      default=None))
        self.assert_compile(schema.CreateTable(table),
                            'CREATE TABLE noautoinctable (id INTEGER '
                            'NOT NULL, x INTEGER, PRIMARY KEY (id))',
                            dialect=sqlite.dialect())

    def test_sqlite_autoincrement_int_affinity(self):
        class MyInteger(sqltypes.TypeDecorator):
            impl = Integer
        table = Table(
            'autoinctable',
            MetaData(),
            Column('id', MyInteger, primary_key=True),
            sqlite_autoincrement=True,
        )
        self.assert_compile(schema.CreateTable(table),
                            'CREATE TABLE autoinctable (id INTEGER NOT '
                            'NULL PRIMARY KEY AUTOINCREMENT)',
                            dialect=sqlite.dialect())


class ReflectHeadlessFKsTest(fixtures.TestBase):
    __only_on__ = 'sqlite'

    def setup(self):
        testing.db.execute("CREATE TABLE a (id INTEGER PRIMARY KEY)")
        # this syntax actually works on other DBs perhaps we'd want to add
        # tests to test_reflection
        testing.db.execute(
            "CREATE TABLE b (id INTEGER PRIMARY KEY REFERENCES a)")

    def teardown(self):
        testing.db.execute("drop table b")
        testing.db.execute("drop table a")

    def test_reflect_tables_fk_no_colref(self):
        meta = MetaData()
        a = Table('a', meta, autoload=True, autoload_with=testing.db)
        b = Table('b', meta, autoload=True, autoload_with=testing.db)

        assert b.c.id.references(a.c.id)


class ConstraintReflectionTest(fixtures.TestBase):
    __only_on__ = 'sqlite'

    @classmethod
    def setup_class(cls):
        with testing.db.begin() as conn:

            conn.execute("CREATE TABLE a1 (id INTEGER PRIMARY KEY)")
            conn.execute("CREATE TABLE a2 (id INTEGER PRIMARY KEY)")
            conn.execute(
                "CREATE TABLE b (id INTEGER PRIMARY KEY, "
                "FOREIGN KEY(id) REFERENCES a1(id),"
                "FOREIGN KEY(id) REFERENCES a2(id)"
                ")")
            conn.execute(
                "CREATE TABLE c (id INTEGER, "
                "CONSTRAINT bar PRIMARY KEY(id),"
                "CONSTRAINT foo1 FOREIGN KEY(id) REFERENCES a1(id),"
                "CONSTRAINT foo2 FOREIGN KEY(id) REFERENCES a2(id)"
                ")")
            conn.execute(
                # the lower casing + inline is intentional here
                "CREATE TABLE d (id INTEGER, x INTEGER unique)")
            conn.execute(
                # the lower casing + inline is intentional here
                'CREATE TABLE d1 '
                '(id INTEGER, "some ( STUPID n,ame" INTEGER unique)')
            conn.execute(
                # the lower casing + inline is intentional here
                'CREATE TABLE d2 ( "some STUPID n,ame" INTEGER unique)')
            conn.execute(
                # the lower casing + inline is intentional here
                'CREATE TABLE d3 ( "some STUPID n,ame" INTEGER NULL unique)')

            conn.execute(
                # lower casing + inline is intentional
                "CREATE TABLE e (id INTEGER, x INTEGER references a2(id))")
            conn.execute(
                'CREATE TABLE e1 (id INTEGER, "some ( STUPID n,ame" INTEGER '
                'references a2   ("some ( STUPID n,ame"))')
            conn.execute(
                'CREATE TABLE e2 (id INTEGER, '
                '"some ( STUPID n,ame" INTEGER NOT NULL  '
                'references a2   ("some ( STUPID n,ame"))')

            conn.execute(
                "CREATE TABLE f (x INTEGER, CONSTRAINT foo_fx UNIQUE(x))"
            )
            conn.execute(
                "CREATE TEMPORARY TABLE g "
                "(x INTEGER, CONSTRAINT foo_gx UNIQUE(x))"
            )
            conn.execute(
                # intentional broken casing
                "CREATE TABLE h (x INTEGER, COnstraINT foo_hx unIQUE(x))"
            )
            conn.execute(
                "CREATE TABLE i (x INTEGER, y INTEGER, PRIMARY KEY(x, y))"
            )
            conn.execute(
                "CREATE TABLE j (id INTEGER, q INTEGER, p INTEGER, "
                "PRIMARY KEY(id), FOreiGN KEY(q,p) REFERENCes  i(x,y))"
            )
            conn.execute(
                "CREATE TABLE k (id INTEGER, q INTEGER, p INTEGER, "
                "PRIMARY KEY(id), "
                "conSTRAINT my_fk FOreiGN KEY (  q  , p  )   "
                "REFERENCes   i    (  x ,   y ))"
            )

            meta = MetaData()
            Table(
                'l', meta, Column('bar', String, index=True),
                schema='main')

            Table(
                'm', meta,
                Column('id', Integer, primary_key=True),
                Column('x', String(30)),
                UniqueConstraint('x')
            )

            Table(
                'n', meta,
                Column('id', Integer, primary_key=True),
                Column('x', String(30)),
                UniqueConstraint('x'),
                prefixes=['TEMPORARY']
            )

            Table(
                'p', meta,
                Column('id', Integer),
                PrimaryKeyConstraint('id', name='pk_name'),
            )

            Table(
                'q', meta,
                Column('id', Integer),
                PrimaryKeyConstraint('id'),
            )

            meta.create_all(conn)

            # will contain an "autoindex"
            conn.execute("create table o (foo varchar(20) primary key)")
            conn.execute(
                "CREATE TABLE onud_test (id INTEGER PRIMARY KEY, "
                "c1 INTEGER, c2 INTEGER, c3 INTEGER, c4 INTEGER, "
                "CONSTRAINT fk1 FOREIGN KEY (c1) REFERENCES a1(id) "
                "ON DELETE SET NULL, "
                "CONSTRAINT fk2 FOREIGN KEY (c2) REFERENCES a1(id) "
                "ON UPDATE CASCADE, "
                "CONSTRAINT fk3 FOREIGN KEY (c3) REFERENCES a2(id) "
                "ON DELETE CASCADE ON UPDATE SET NULL,"
                "CONSTRAINT fk4 FOREIGN KEY (c4) REFERENCES a2(id) "
                "ON UPDATE NO ACTION)"
            )

            conn.execute(
                "CREATE TABLE cp ("
                "q INTEGER check (q > 1 AND q < 6),\n"
                "CONSTRAINT cq CHECK (q == 1 OR (q > 2 AND q < 5))\n"
                ")"
            )

    @classmethod
    def teardown_class(cls):
        with testing.db.begin() as conn:
            for name in [
                "m", "main.l", "k", "j", "i", "h", "g", "f", "e", "e1",
                    "d", "d1", "d2", "c", "b", "a1", "a2"]:
                try:
                    conn.execute("drop table %s" % name)
                except:
                    pass

    def test_legacy_quoted_identifiers_unit(self):
        dialect = sqlite.dialect()
        dialect._broken_fk_pragma_quotes = True

        for row in [
            (0, None, 'target', 'tid', 'id', None),
            (0, None, '"target"', 'tid', 'id', None),
            (0, None, '[target]', 'tid', 'id', None),
            (0, None, "'target'", 'tid', 'id', None),
            (0, None, '`target`', 'tid', 'id', None),
        ]:
            def _get_table_pragma(*arg, **kw):
                return [row]

            def _get_table_sql(*arg, **kw):
                return "CREATE TABLE foo "\
                    "(tid INTEGER, "\
                    "FOREIGN KEY(tid) REFERENCES %s (id))" % row[2]
            with mock.patch.object(
                    dialect, "_get_table_pragma", _get_table_pragma):
                with mock.patch.object(
                        dialect, '_get_table_sql', _get_table_sql):

                    fkeys = dialect.get_foreign_keys(None, 'foo')
                    eq_(
                        fkeys,
                        [{
                            'referred_table': 'target',
                            'referred_columns': ['id'],
                            'referred_schema': None,
                            'name': None,
                            'constrained_columns': ['tid'],
                            'options': {}
                        }])

    def test_foreign_key_name_is_none(self):
        # and not "0"
        inspector = Inspector(testing.db)
        fks = inspector.get_foreign_keys('b')
        eq_(
            fks,
            [
                {'referred_table': 'a1', 'referred_columns': ['id'],
                 'referred_schema': None, 'name': None,
                 'constrained_columns': ['id'],
                 'options': {}},
                {'referred_table': 'a2', 'referred_columns': ['id'],
                 'referred_schema': None, 'name': None,
                 'constrained_columns': ['id'],
                 'options': {}},
            ]
        )

    def test_foreign_key_name_is_not_none(self):
        inspector = Inspector(testing.db)
        fks = inspector.get_foreign_keys('c')
        eq_(
            fks,
            [
                {
                    'referred_table': 'a1', 'referred_columns': ['id'],
                    'referred_schema': None, 'name': 'foo1',
                    'constrained_columns': ['id'],
                    'options': {}},
                {
                    'referred_table': 'a2', 'referred_columns': ['id'],
                    'referred_schema': None, 'name': 'foo2',
                    'constrained_columns': ['id'],
                    'options': {}},
            ]
        )

    def test_unnamed_inline_foreign_key(self):
        inspector = Inspector(testing.db)
        fks = inspector.get_foreign_keys('e')
        eq_(
            fks,
            [{
                'referred_table': 'a2', 'referred_columns': ['id'],
                'referred_schema': None,
                'name': None, 'constrained_columns': ['x'],
                'options': {}
            }]
        )

    def test_unnamed_inline_foreign_key_quoted(self):
        inspector = Inspector(testing.db)
        fks = inspector.get_foreign_keys('e1')
        eq_(
            fks,
            [{
                'referred_table': 'a2',
                'referred_columns': ['some ( STUPID n,ame'],
                'referred_schema': None,
                'options': {},
                'name': None, 'constrained_columns': ['some ( STUPID n,ame']
            }]
        )
        fks = inspector.get_foreign_keys('e2')
        eq_(
            fks,
            [{
                'referred_table': 'a2',
                'referred_columns': ['some ( STUPID n,ame'],
                'referred_schema': None,
                'options': {},
                'name': None, 'constrained_columns': ['some ( STUPID n,ame']
            }]
        )

    def test_foreign_key_composite_broken_casing(self):
        inspector = Inspector(testing.db)
        fks = inspector.get_foreign_keys('j')
        eq_(
            fks,
            [{
                'referred_table': 'i',
                'referred_columns': ['x', 'y'],
                'referred_schema': None, 'name': None,
                'constrained_columns': ['q', 'p'],
                'options': {}}]
        )
        fks = inspector.get_foreign_keys('k')
        eq_(
            fks,
            [
                {'referred_table': 'i', 'referred_columns': ['x', 'y'],
                 'referred_schema': None, 'name': 'my_fk',
                 'constrained_columns': ['q', 'p'],
                 'options': {}}]
        )

    def test_foreign_key_ondelete_onupdate(self):
        inspector = Inspector(testing.db)
        fks = inspector.get_foreign_keys('onud_test')
        eq_(
            fks,
            [
                {
                    'referred_table': 'a1', 'referred_columns': ['id'],
                    'referred_schema': None, 'name': 'fk1',
                    'constrained_columns': ['c1'],
                    'options': {'ondelete': 'SET NULL'}
                },
                {
                    'referred_table': 'a1', 'referred_columns': ['id'],
                    'referred_schema': None, 'name': 'fk2',
                    'constrained_columns': ['c2'],
                    'options': {'onupdate': 'CASCADE'}
                },
                {
                    'referred_table': 'a2', 'referred_columns': ['id'],
                    'referred_schema': None, 'name': 'fk3',
                    'constrained_columns': ['c3'],
                    'options': {'ondelete': 'CASCADE', 'onupdate': 'SET NULL'}
                },
                {
                    'referred_table': 'a2', 'referred_columns': ['id'],
                    'referred_schema': None, 'name': 'fk4',
                    'constrained_columns': ['c4'],
                    'options': {'onupdate': 'NO ACTION'}
                },
            ]
        )

    def test_foreign_key_options_unnamed_inline(self):
        with testing.db.connect() as conn:
            conn.execute(
                "create table foo (id integer, "
                "foreign key (id) references bar (id) on update cascade)")

            insp = inspect(conn)
            eq_(
                insp.get_foreign_keys('foo'),
                [{
                    'name': None,
                    'referred_columns': ['id'],
                    'referred_table': 'bar',
                    'constrained_columns': ['id'],
                    'referred_schema': None,
                    'options': {'onupdate': 'CASCADE'}}]
            )

    def test_dont_reflect_autoindex(self):
        inspector = Inspector(testing.db)
        eq_(inspector.get_indexes('o'), [])
        eq_(
            inspector.get_indexes('o', include_auto_indexes=True),
            [{
                'unique': 1,
                'name': 'sqlite_autoindex_o_1',
                'column_names': ['foo']}])

    def test_create_index_with_schema(self):
        """Test creation of index with explicit schema"""

        inspector = Inspector(testing.db)
        eq_(
            inspector.get_indexes('l', schema='main'),
            [{'unique': 0, 'name': u'ix_main_l_bar',
              'column_names': [u'bar']}])

    def test_unique_constraint_named(self):
        inspector = Inspector(testing.db)
        eq_(
            inspector.get_unique_constraints("f"),
            [{'column_names': ['x'], 'name': 'foo_fx'}]
        )

    def test_unique_constraint_named_broken_casing(self):
        inspector = Inspector(testing.db)
        eq_(
            inspector.get_unique_constraints("h"),
            [{'column_names': ['x'], 'name': 'foo_hx'}]
        )

    def test_unique_constraint_named_broken_temp(self):
        inspector = Inspector(testing.db)
        eq_(
            inspector.get_unique_constraints("g"),
            [{'column_names': ['x'], 'name': 'foo_gx'}]
        )

    def test_unique_constraint_unnamed_inline(self):
        inspector = Inspector(testing.db)
        eq_(
            inspector.get_unique_constraints("d"),
            [{'column_names': ['x'], 'name': None}]
        )

    def test_unique_constraint_unnamed_inline_quoted(self):
        inspector = Inspector(testing.db)
        eq_(
            inspector.get_unique_constraints("d1"),
            [{'column_names': ['some ( STUPID n,ame'], 'name': None}]
        )
        eq_(
            inspector.get_unique_constraints("d2"),
            [{'column_names': ['some STUPID n,ame'], 'name': None}]
        )
        eq_(
            inspector.get_unique_constraints("d3"),
            [{'column_names': ['some STUPID n,ame'], 'name': None}]
        )

    def test_unique_constraint_unnamed_normal(self):
        inspector = Inspector(testing.db)
        eq_(
            inspector.get_unique_constraints("m"),
            [{'column_names': ['x'], 'name': None}]
        )

    def test_unique_constraint_unnamed_normal_temporary(self):
        inspector = Inspector(testing.db)
        eq_(
            inspector.get_unique_constraints("n"),
            [{'column_names': ['x'], 'name': None}]
        )

    def test_primary_key_constraint_named(self):
        inspector = Inspector(testing.db)
        eq_(
            inspector.get_pk_constraint("p"),
            {'constrained_columns': ['id'], 'name': 'pk_name'}
        )

    def test_primary_key_constraint_unnamed(self):
        inspector = Inspector(testing.db)
        eq_(
            inspector.get_pk_constraint("q"),
            {'constrained_columns': ['id'], 'name': None}
        )

    def test_primary_key_constraint_no_pk(self):
        inspector = Inspector(testing.db)
        eq_(
            inspector.get_pk_constraint("d"),
            {'constrained_columns': [], 'name': None}
        )

    def test_check_constraint(self):
        inspector = Inspector(testing.db)
        eq_(
            inspector.get_check_constraints("cp"),
            [{'sqltext': 'q > 1 AND q < 6', 'name': None},
             {'sqltext': 'q == 1 OR (q > 2 AND q < 5)', 'name': 'cq'}]
        )


class SavepointTest(fixtures.TablesTest):

    """test that savepoints work when we use the correct event setup"""
    __only_on__ = 'sqlite'

    @classmethod
    def define_tables(cls, metadata):
        Table(
            'users', metadata,
            Column('user_id', Integer, primary_key=True),
            Column('user_name', String)
        )

    @classmethod
    def setup_bind(cls):
        engine = engines.testing_engine(options={"use_reaper": False})

        @event.listens_for(engine, "connect")
        def do_connect(dbapi_connection, connection_record):
            # disable pysqlite's emitting of the BEGIN statement entirely.
            # also stops it from emitting COMMIT before any DDL.
            dbapi_connection.isolation_level = None

        @event.listens_for(engine, "begin")
        def do_begin(conn):
            # emit our own BEGIN
            conn.execute("BEGIN")

        return engine

    def test_nested_subtransaction_rollback(self):
        users = self.tables.users
        connection = self.bind.connect()
        transaction = connection.begin()
        connection.execute(users.insert(), user_id=1, user_name='user1')
        trans2 = connection.begin_nested()
        connection.execute(users.insert(), user_id=2, user_name='user2')
        trans2.rollback()
        connection.execute(users.insert(), user_id=3, user_name='user3')
        transaction.commit()
        eq_(connection.execute(select([users.c.user_id]).
            order_by(users.c.user_id)).fetchall(),
            [(1, ), (3, )])
        connection.close()

    def test_nested_subtransaction_commit(self):
        users = self.tables.users
        connection = self.bind.connect()
        transaction = connection.begin()
        connection.execute(users.insert(), user_id=1, user_name='user1')
        trans2 = connection.begin_nested()
        connection.execute(users.insert(), user_id=2, user_name='user2')
        trans2.commit()
        connection.execute(users.insert(), user_id=3, user_name='user3')
        transaction.commit()
        eq_(connection.execute(select([users.c.user_id]).
            order_by(users.c.user_id)).fetchall(),
            [(1, ), (2, ), (3, )])
        connection.close()

    def test_rollback_to_subtransaction(self):
        users = self.tables.users
        connection = self.bind.connect()
        transaction = connection.begin()
        connection.execute(users.insert(), user_id=1, user_name='user1')
        connection.begin_nested()
        connection.execute(users.insert(), user_id=2, user_name='user2')
        trans3 = connection.begin()
        connection.execute(users.insert(), user_id=3, user_name='user3')
        trans3.rollback()
        connection.execute(users.insert(), user_id=4, user_name='user4')
        transaction.commit()
        eq_(connection.execute(select([users.c.user_id]).
            order_by(users.c.user_id)).fetchall(),
            [(1, ), (4, )])
        connection.close()


class TypeReflectionTest(fixtures.TestBase):

    __only_on__ = 'sqlite'

    def _fixed_lookup_fixture(self):
        return [
            (sqltypes.String(), sqltypes.VARCHAR()),
            (sqltypes.String(1), sqltypes.VARCHAR(1)),
            (sqltypes.String(3), sqltypes.VARCHAR(3)),
            (sqltypes.Text(), sqltypes.TEXT()),
            (sqltypes.Unicode(), sqltypes.VARCHAR()),
            (sqltypes.Unicode(1), sqltypes.VARCHAR(1)),
            (sqltypes.UnicodeText(), sqltypes.TEXT()),
            (sqltypes.CHAR(3), sqltypes.CHAR(3)),
            (sqltypes.NUMERIC, sqltypes.NUMERIC()),
            (sqltypes.NUMERIC(10, 2), sqltypes.NUMERIC(10, 2)),
            (sqltypes.Numeric, sqltypes.NUMERIC()),
            (sqltypes.Numeric(10, 2), sqltypes.NUMERIC(10, 2)),
            (sqltypes.DECIMAL, sqltypes.DECIMAL()),
            (sqltypes.DECIMAL(10, 2), sqltypes.DECIMAL(10, 2)),
            (sqltypes.INTEGER, sqltypes.INTEGER()),
            (sqltypes.BIGINT, sqltypes.BIGINT()),
            (sqltypes.Float, sqltypes.FLOAT()),
            (sqltypes.TIMESTAMP, sqltypes.TIMESTAMP()),
            (sqltypes.DATETIME, sqltypes.DATETIME()),
            (sqltypes.DateTime, sqltypes.DATETIME()),
            (sqltypes.DateTime(), sqltypes.DATETIME()),
            (sqltypes.DATE, sqltypes.DATE()),
            (sqltypes.Date, sqltypes.DATE()),
            (sqltypes.TIME, sqltypes.TIME()),
            (sqltypes.Time, sqltypes.TIME()),
            (sqltypes.BOOLEAN, sqltypes.BOOLEAN()),
            (sqltypes.Boolean, sqltypes.BOOLEAN()),
            (sqlite.DATE(
                storage_format="%(year)04d%(month)02d%(day)02d",
            ), sqltypes.DATE()),
            (sqlite.TIME(
                storage_format="%(hour)02d%(minute)02d%(second)02d",
            ), sqltypes.TIME()),
            (sqlite.DATETIME(
                storage_format="%(year)04d%(month)02d%(day)02d"
                "%(hour)02d%(minute)02d%(second)02d",
            ), sqltypes.DATETIME()),
        ]

    def _unsupported_args_fixture(self):
        return [
            ("INTEGER(5)", sqltypes.INTEGER(),),
            ("DATETIME(6, 12)", sqltypes.DATETIME())
        ]

    def _type_affinity_fixture(self):
        return [
            ("LONGTEXT", sqltypes.TEXT()),
            ("TINYINT", sqltypes.INTEGER()),
            ("MEDIUMINT", sqltypes.INTEGER()),
            ("INT2", sqltypes.INTEGER()),
            ("UNSIGNED BIG INT", sqltypes.INTEGER()),
            ("INT8", sqltypes.INTEGER()),
            ("CHARACTER(20)", sqltypes.TEXT()),
            ("CLOB", sqltypes.TEXT()),
            ("CLOBBER", sqltypes.TEXT()),
            ("VARYING CHARACTER(70)", sqltypes.TEXT()),
            ("NATIVE CHARACTER(70)", sqltypes.TEXT()),
            ("BLOB", sqltypes.BLOB()),
            ("BLOBBER", sqltypes.NullType()),
            ("DOUBLE PRECISION", sqltypes.REAL()),
            ("FLOATY", sqltypes.REAL()),
            ("NOTHING WE KNOW", sqltypes.NUMERIC()),
        ]

    def _fixture_as_string(self, fixture):
        for from_, to_ in fixture:
            if isinstance(from_, sqltypes.TypeEngine):
                from_ = str(from_.compile())
            elif isinstance(from_, type):
                from_ = str(from_().compile())
            yield from_, to_

    def _test_lookup_direct(self, fixture, warnings=False):
        dialect = sqlite.dialect()
        for from_, to_ in self._fixture_as_string(fixture):
            if warnings:
                def go():
                    return dialect._resolve_type_affinity(from_)
                final_type = testing.assert_warnings(
                    go, ["Could not instantiate"], regex=True)
            else:
                final_type = dialect._resolve_type_affinity(from_)
            expected_type = type(to_)
            is_(type(final_type), expected_type)

    def _test_round_trip(self, fixture, warnings=False):
        from sqlalchemy import inspect
        conn = testing.db.connect()
        for from_, to_ in self._fixture_as_string(fixture):
            inspector = inspect(conn)
            conn.execute("CREATE TABLE foo (data %s)" % from_)
            try:
                if warnings:
                    def go():
                        return inspector.get_columns("foo")[0]
                    col_info = testing.assert_warnings(
                        go, ["Could not instantiate"], regex=True)
                else:
                    col_info = inspector.get_columns("foo")[0]
                expected_type = type(to_)
                is_(type(col_info['type']), expected_type)

                # test args
                for attr in ("scale", "precision", "length"):
                    if getattr(to_, attr, None) is not None:
                        eq_(
                            getattr(col_info['type'], attr),
                            getattr(to_, attr, None)
                        )
            finally:
                conn.execute("DROP TABLE foo")

    def test_lookup_direct_lookup(self):
        self._test_lookup_direct(self._fixed_lookup_fixture())

    def test_lookup_direct_unsupported_args(self):
        self._test_lookup_direct(
            self._unsupported_args_fixture(), warnings=True)

    def test_lookup_direct_type_affinity(self):
        self._test_lookup_direct(self._type_affinity_fixture())

    def test_round_trip_direct_lookup(self):
        self._test_round_trip(self._fixed_lookup_fixture())

    def test_round_trip_direct_unsupported_args(self):
        self._test_round_trip(
            self._unsupported_args_fixture(), warnings=True)

    def test_round_trip_direct_type_affinity(self):
        self._test_round_trip(self._type_affinity_fixture())
