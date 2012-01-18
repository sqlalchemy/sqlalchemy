"""SQLite-specific tests."""

from test.lib.testing import eq_, assert_raises, \
    assert_raises_message
import datetime
from sqlalchemy import *
from sqlalchemy import exc, sql, schema, pool, types as sqltypes
from sqlalchemy.dialects.sqlite import base as sqlite, \
    pysqlite as pysqlite_dialect
from sqlalchemy.engine.url import make_url
from test.lib import *
import os
from sqlalchemy.schema import CreateTable

class TestTypes(fixtures.TestBase, AssertsExecutionResults):

    __only_on__ = 'sqlite'

    def test_boolean(self):
        """Test that the boolean only treats 1 as True

        """

        meta = MetaData(testing.db)
        t = Table('bool_table', meta, Column('id', Integer,
                  primary_key=True), Column('boo',
                  Boolean(create_constraint=False)))
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
                    text("select 'ASDF' as value", typemap={"value":typ})
                ).scalar()
            )

    def test_time_microseconds(self):
        dt = datetime.datetime(2008, 6, 27, 12, 0, 0, 125, )
        eq_(str(dt), '2008-06-27 12:00:00.000125')
        sldt = sqlite.DATETIME()
        bp = sldt.bind_processor(None)
        eq_(bp(dt), '2008-06-27 12:00:00.000125')
        rp = sldt.result_processor(None, None)
        eq_(rp(bp(dt)), dt)

    def test_native_datetime(self):
        dbapi = testing.db.dialect.dbapi
        connect_args = {'detect_types': dbapi.PARSE_DECLTYPES \
                        | dbapi.PARSE_COLNAMES}
        engine = engines.testing_engine(options={'connect_args'
                : connect_args, 'native_datetime': True})
        t = Table('datetest', MetaData(), Column('id', Integer,
                  primary_key=True), Column('d1', Date), Column('d2',
                  TIMESTAMP))
        t.create(engine)
        try:
            engine.execute(t.insert(), {'d1': datetime.date(2010, 5,
                           10), 
                          'd2': datetime.datetime( 2010, 5, 10, 12, 15, 25,
                          )})
            row = engine.execute(t.select()).first()
            eq_(row, (1, datetime.date(2010, 5, 10), 
            datetime.datetime( 2010, 5, 10, 12, 15, 25, )))
            r = engine.execute(func.current_date()).scalar()
            assert isinstance(r, basestring)
        finally:
            t.drop(engine)
            engine.dispose()

    def test_no_convert_unicode(self):
        """test no utf-8 encoding occurs"""

        dialect = sqlite.dialect()
        for t in (
            String(convert_unicode=True),
            CHAR(convert_unicode=True),
            Unicode(),
            UnicodeText(),
            String(convert_unicode=True),
            CHAR(convert_unicode=True),
            Unicode(),
            UnicodeText(),
            ):
            bindproc = t.dialect_impl(dialect).bind_processor(dialect)
            assert not bindproc or isinstance(bindproc(u'some string'),
                    unicode)

    @testing.provide_metadata
    def test_type_reflection(self):
        metadata = self.metadata

        # (ask_for, roundtripped_as_if_different)

        specs = [
            (String(), String()),
            (String(1), String(1)),
            (String(3), String(3)),
            (Text(), Text()),
            (Unicode(), String()),
            (Unicode(1), String(1)),
            (Unicode(3), String(3)),
            (UnicodeText(), Text()),
            (CHAR(1), ),
            (CHAR(3), CHAR(3)),
            (NUMERIC, NUMERIC()),
            (NUMERIC(10, 2), NUMERIC(10, 2)),
            (Numeric, NUMERIC()),
            (Numeric(10, 2), NUMERIC(10, 2)),
            (DECIMAL, DECIMAL()),
            (DECIMAL(10, 2), DECIMAL(10, 2)),
            (Float, Float()),
            (NUMERIC(), ),
            (TIMESTAMP, TIMESTAMP()),
            (DATETIME, DATETIME()),
            (DateTime, DateTime()),
            (DateTime(), ),
            (DATE, DATE()),
            (Date, Date()),
            (TIME, TIME()),
            (Time, Time()),
            (BOOLEAN, BOOLEAN()),
            (Boolean, Boolean()),
            ]
        columns = [Column('c%i' % (i + 1), t[0]) for (i, t) in
                   enumerate(specs)]
        db = testing.db
        t_table = Table('types', metadata, *columns)
        metadata.create_all()
        m2 = MetaData(db)
        rt = Table('types', m2, autoload=True)
        try:
            db.execute('CREATE VIEW types_v AS SELECT * from types')
            rv = Table('types_v', m2, autoload=True)
            expected = [len(c) > 1 and c[1] or c[0] for c in specs]
            for table in rt, rv:
                for i, reflected in enumerate(table.c):
                    assert isinstance(reflected.type,
                            type(expected[i])), '%d: %r' % (i,
                            type(expected[i]))
        finally:
            db.execute('DROP VIEW types_v')

    @testing.emits_warning('Did not recognize')
    @testing.provide_metadata
    def test_unknown_reflection(self):
        metadata = self.metadata
        t = Table('t', metadata,
            Column('x', sqltypes.BINARY(16)),
            Column('y', sqltypes.BINARY())
        )
        t.create()
        t2 = Table('t', MetaData(), autoload=True, autoload_with=testing.db)
        assert isinstance(t2.c.x.type, sqltypes.NullType)
        assert isinstance(t2.c.y.type, sqltypes.NullType)


class DefaultsTest(fixtures.TestBase, AssertsCompiledSQL):

    __only_on__ = 'sqlite'

    @testing.exclude('sqlite', '<', (3, 3, 8),
                     'sqlite3 changesets 3353 and 3440 modified '
                     'behavior of default displayed in pragma '
                     'table_info()')
    def test_default_reflection(self):

        # (ask_for, roundtripped_as_if_different)

        specs = [(String(3), '"foo"'), (NUMERIC(10, 2), '100.50'),
                 (Integer, '5'), (Boolean, 'False')]
        columns = [Column('c%i' % (i + 1), t[0],
                   server_default=text(t[1])) for (i, t) in
                   enumerate(specs)]
        db = testing.db
        m = MetaData(db)
        t_table = Table('t_defaults', m, *columns)
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
        t= Table("t", self.metadata, 
                Column("x", Boolean, server_default=sql.false()))
        t.create(testing.db)
        testing.db.execute(t.insert())
        testing.db.execute(t.insert().values(x=True))
        eq_(
            testing.db.execute(t.select().order_by(t.c.x)).fetchall(),
            [(False,), (True,)]
        )


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

    def test_quoted_identifiers(self):
        """Tests autoload of tables created with quoted column names."""

        # This is quirky in sqlite.

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
        try:
            meta = MetaData(testing.db)
            table1 = Table('django_admin_log', meta, autoload=True)
            table2 = Table('django_content_type', meta, autoload=True)
            j = table1.join(table2)
            assert j.onclause.compare(table1.c.content_type_id
                    == table2.c.id)
        finally:
            testing.db.execute('drop table django_admin_log')
            testing.db.execute('drop table django_content_type')

    def test_attached_as_schema(self):
        cx = testing.db.connect()
        try:
            cx.execute('ATTACH DATABASE ":memory:" AS  test_schema')
            dialect = cx.dialect
            assert dialect.get_table_names(cx, 'test_schema') == []
            meta = MetaData(cx)
            Table('created', meta, Column('id', Integer),
                  schema='test_schema')
            alt_master = Table('sqlite_master', meta, autoload=True,
                               schema='test_schema')
            meta.create_all(cx)
            eq_(dialect.get_table_names(cx, 'test_schema'), ['created'])
            assert len(alt_master.c) > 0
            meta.clear()
            reflected = Table('created', meta, autoload=True,
                              schema='test_schema')
            assert len(reflected.c) == 1
            cx.execute(reflected.insert(), dict(id=1))
            r = cx.execute(reflected.select()).fetchall()
            assert list(r) == [(1, )]
            cx.execute(reflected.update(), dict(id=2))
            r = cx.execute(reflected.select()).fetchall()
            assert list(r) == [(2, )]
            cx.execute(reflected.delete(reflected.c.id == 2))
            r = cx.execute(reflected.select()).fetchall()
            assert list(r) == []

            # note that sqlite_master is cleared, above

            meta.drop_all()
            assert dialect.get_table_names(cx, 'test_schema') == []
        finally:
            cx.execute('DETACH DATABASE test_schema')

    @testing.exclude('sqlite', '<', (2, 6), 'no database support')
    def test_temp_table_reflection(self):
        cx = testing.db.connect()
        try:
            cx.execute('CREATE TEMPORARY TABLE tempy (id INT)')
            assert 'tempy' in cx.dialect.get_table_names(cx, None)
            meta = MetaData(cx)
            tempy = Table('tempy', meta, autoload=True)
            assert len(tempy.c) == 1
            meta.drop_all()
        except:
            try:
                cx.execute('DROP TABLE tempy')
            except exc.DBAPIError:
                pass
            raise

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


    def test_dont_reflect_autoindex(self):
        meta = MetaData(testing.db)
        t = Table('foo', meta, Column('bar', String, primary_key=True))
        meta.create_all()
        from sqlalchemy.engine.reflection import Inspector
        try:
            inspector = Inspector(testing.db)
            eq_(inspector.get_indexes('foo'), [])
            eq_(inspector.get_indexes('foo',
                include_auto_indexes=True), [{'unique': 1, 'name'
                : u'sqlite_autoindex_foo_1', 'column_names': [u'bar']}])
        finally:
            meta.drop_all()

    def test_create_index_with_schema(self):
        """Test creation of index with explicit schema"""

        meta = MetaData(testing.db)
        t = Table('foo', meta, Column('bar', String, index=True),
                  schema='main')
        try:
            meta.create_all()
        finally:
            meta.drop_all()


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

    def test_constraints_with_schemas(self):
        metadata = MetaData()
        t1 = Table('t1', metadata, 
                        Column('id', Integer, primary_key=True),
                        schema='master')
        t2 = Table('t2', metadata, 
                        Column('id', Integer, primary_key=True),
                        Column('t1_id', Integer, ForeignKey('master.t1.id')),
                        schema='master'
                    )
        t3 = Table('t3', metadata, 
                        Column('id', Integer, primary_key=True),
                        Column('t1_id', Integer, ForeignKey('master.t1.id')),
                        schema='alternate'
                    )
        t4 = Table('t4', metadata, 
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
        self._test_empty_insert(Table('a', MetaData(testing.db),
                                Column('id', Integer,
                                primary_key=True)))

    @testing.exclude('sqlite', '<', (3, 3, 8), 'no database support')
    def test_empty_insert_pk2(self):
        assert_raises(exc.DBAPIError, self._test_empty_insert, Table('b'
                      , MetaData(testing.db), Column('x', Integer,
                      primary_key=True), Column('y', Integer,
                      primary_key=True)))

    @testing.exclude('sqlite', '<', (3, 3, 8), 'no database support')
    def test_empty_insert_pk3(self):
        assert_raises(exc.DBAPIError, self._test_empty_insert, Table('c'
                      , MetaData(testing.db), Column('x', Integer,
                      primary_key=True), Column('y', Integer,
                      DefaultClause('123'), primary_key=True)))

    @testing.exclude('sqlite', '<', (3, 3, 8), 'no database support')
    def test_empty_insert_pk4(self):
        self._test_empty_insert(Table('d', MetaData(testing.db),
                                Column('x', Integer, primary_key=True),
                                Column('y', Integer, DefaultClause('123'
                                ))))

    @testing.exclude('sqlite', '<', (3, 3, 8), 'no database support')
    def test_empty_insert_nopk1(self):
        self._test_empty_insert(Table('e', MetaData(testing.db),
                                Column('id', Integer)))

    @testing.exclude('sqlite', '<', (3, 3, 8), 'no database support')
    def test_empty_insert_nopk2(self):
        self._test_empty_insert(Table('f', MetaData(testing.db),
                                Column('x', Integer), Column('y',
                                Integer)))

    def test_inserts_with_spaces(self):
        tbl = Table('tbl', MetaData('sqlite:///'), Column('with space',
                    Integer), Column('without', Integer))
        tbl.create()
        try:
            tbl.insert().execute({'without': 123})
            assert list(tbl.select().execute()) == [(None, 123)]
            tbl.insert().execute({'with space': 456})
            assert list(tbl.select().execute()) == [(None, 123), (456,
                    None)]
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
        cattable.insert().execute([{'id': 1, 'description': 'Python'},
                                  {'id': 2, 'description': 'Ruby'}])
        matchtable.insert().execute([{'id': 1, 'title'
                                    : 'Agile Web Development with Rails'
                                    , 'category_id': 2}, {'id': 2,
                                    'title': 'Dive Into Python',
                                    'category_id': 1}, {'id': 3, 'title'
                                    : "Programming Matz's Ruby",
                                    'category_id': 2}, {'id': 4, 'title'
                                    : 'The Definitive Guide to Django',
                                    'category_id': 1}, {'id': 5, 'title'
                                    : 'Python in a Nutshell',
                                    'category_id': 1}])

    @classmethod
    def teardown_class(cls):
        metadata.drop_all()

    def test_expression(self):
        self.assert_compile(matchtable.c.title.match('somstr'),
                            'matchtable.title MATCH ?', dialect=sqlite.dialect())

    def test_simple_match(self):
        results = \
            matchtable.select().where(matchtable.c.title.match('python'
                )).order_by(matchtable.c.id).execute().fetchall()
        eq_([2, 5], [r.id for r in results])

    def test_simple_prefix_match(self):
        results = \
            matchtable.select().where(matchtable.c.title.match('nut*'
                )).execute().fetchall()
        eq_([5], [r.id for r in results])

    def test_or_match(self):
        results2 = \
            matchtable.select().where(
                matchtable.c.title.match('nutshell OR ruby'
                )).order_by(matchtable.c.id).execute().fetchall()
        eq_([3, 5], [r.id for r in results2])

    def test_and_match(self):
        results2 = \
            matchtable.select().where(
                matchtable.c.title.match('python nutshell'
                )).execute().fetchall()
        eq_([5], [r.id for r in results2])

    def test_match_across_joins(self):
        results = matchtable.select().where(and_(cattable.c.id
                == matchtable.c.category_id,
                cattable.c.description.match('Ruby'
                ))).order_by(matchtable.c.id).execute().fetchall()
        eq_([1, 3], [r.id for r in results])


class AutoIncrementTest(fixtures.TestBase, AssertsCompiledSQL):

    def test_sqlite_autoincrement(self):
        table = Table('autoinctable', MetaData(), Column('id', Integer,
                      primary_key=True), Column('x', Integer,
                      default=None), sqlite_autoincrement=True)
        self.assert_compile(schema.CreateTable(table),
                            'CREATE TABLE autoinctable (id INTEGER NOT '
                            'NULL PRIMARY KEY AUTOINCREMENT, x INTEGER)'
                            , dialect=sqlite.dialect())

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
        class MyInteger(TypeDecorator):
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
        testing.db.execute("CREATE TABLE b (id INTEGER PRIMARY KEY REFERENCES a)")

    def teardown(self):
        testing.db.execute("drop table b")
        testing.db.execute("drop table a")

    def test_reflect_tables_fk_no_colref(self):
        meta = MetaData()
        a = Table('a', meta, autoload=True, autoload_with=testing.db)
        b = Table('b', meta, autoload=True, autoload_with=testing.db)

        assert b.c.id.references(a.c.id)

class ReflectFKConstraintTest(fixtures.TestBase):
    __only_on__ = 'sqlite'

    def setup(self):
        testing.db.execute("CREATE TABLE a1 (id INTEGER PRIMARY KEY)")
        testing.db.execute("CREATE TABLE a2 (id INTEGER PRIMARY KEY)")
        testing.db.execute("CREATE TABLE b (id INTEGER PRIMARY KEY, "
                            "FOREIGN KEY(id) REFERENCES a1(id),"
                            "FOREIGN KEY(id) REFERENCES a2(id)"
                            ")")
        testing.db.execute("CREATE TABLE c (id INTEGER, "
                            "CONSTRAINT bar PRIMARY KEY(id),"
                            "CONSTRAINT foo1 FOREIGN KEY(id) REFERENCES a1(id),"
                            "CONSTRAINT foo2 FOREIGN KEY(id) REFERENCES a2(id)"
                            ")")

    def teardown(self):
        testing.db.execute("drop table c")
        testing.db.execute("drop table b")
        testing.db.execute("drop table a1")
        testing.db.execute("drop table a2")

    def test_name_is_none(self):
        # and not "0"
        meta = MetaData()
        b = Table('b', meta, autoload=True, autoload_with=testing.db)
        eq_(
            [con.name for con in b.constraints],
            [None, None, None]
        )

    def test_name_not_none(self):
        # we don't have names for PK constraints,
        # it appears we get back None in the pragma for 
        # FKs also (also it doesn't even appear to be documented on sqlite's docs
        # at http://www.sqlite.org/pragma.html#pragma_foreign_key_list
        # how did we ever know that's the "name" field ??)

        meta = MetaData()
        c = Table('c', meta, autoload=True, autoload_with=testing.db)
        eq_(
            set([con.name for con in c.constraints]),
            set([None, None])
        )
