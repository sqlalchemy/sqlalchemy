"""SQLite-specific tests."""

from sqlalchemy.test.testing import eq_, assert_raises, \
    assert_raises_message
import datetime
from sqlalchemy import *
from sqlalchemy import exc, sql, schema
from sqlalchemy.dialects.sqlite import base as sqlite, \
    pysqlite as pysqlite_dialect
from sqlalchemy.test import *


class TestTypes(TestBase, AssertsExecutionResults):

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

    def test_string_dates_raise(self):
        assert_raises(TypeError, testing.db.execute,
                      select([1]).where(bindparam('date', type_=Date)),
                      date=str(datetime.date(2007, 10, 30)))

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

    def test_type_reflection(self):

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
        m = MetaData(db)
        t_table = Table('types', m, *columns)
        m.create_all()
        try:
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
        finally:
            m.drop_all()


class TestDefaults(TestBase, AssertsExecutionResults):

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
        expected = ['my_default', '0']
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


class DialectTest(TestBase, AssertsExecutionResults):

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

    def test_set_isolation_level(self):
        """Test setting the read uncommitted/serializable levels"""

        eng = create_engine(testing.db.url)
        eq_(eng.execute('PRAGMA read_uncommitted').scalar(), 0)
        eng = create_engine(testing.db.url,
                            isolation_level='READ UNCOMMITTED')
        eq_(eng.execute('PRAGMA read_uncommitted').scalar(), 1)
        eng = create_engine(testing.db.url,
                            isolation_level='SERIALIZABLE')
        eq_(eng.execute('PRAGMA read_uncommitted').scalar(), 0)
        assert_raises(exc.ArgumentError, create_engine, testing.db.url,
                      isolation_level='FOO')

    def test_create_index_with_schema(self):
        """Test creation of index with explicit schema"""

        meta = MetaData(testing.db)
        t = Table('foo', meta, Column('bar', String, index=True),
                  schema='main')
        try:
            meta.create_all()
        finally:
            meta.drop_all()


class SQLTest(TestBase, AssertsCompiledSQL):

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


class InsertTest(TestBase, AssertsExecutionResults):

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


class MatchTest(TestBase, AssertsCompiledSQL):

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
                            'matchtable.title MATCH ?')

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


class TestAutoIncrement(TestBase, AssertsCompiledSQL):

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
