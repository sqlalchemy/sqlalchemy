# coding: utf-8

from test.lib.testing import eq_, assert_raises, assert_raises_message

# Py2K
import sets
# end Py2K

from sqlalchemy import *
from sqlalchemy import sql, exc, schema, types as sqltypes, event
from sqlalchemy.dialects.mysql import base as mysql
from sqlalchemy.engine.url import make_url

from test.lib.testing import eq_
from test.lib import *
from test.lib.engines import utf8_engine
import datetime

class CompileTest(fixtures.TestBase, AssertsCompiledSQL):

    __dialect__ = mysql.dialect()

    def test_reserved_words(self):
        table = Table("mysql_table", MetaData(),
            Column("col1", Integer),
            Column("master_ssl_verify_server_cert", Integer))
        x = select([table.c.col1, table.c.master_ssl_verify_server_cert])

        self.assert_compile(x, 
            '''SELECT mysql_table.col1, mysql_table.`master_ssl_verify_server_cert` FROM mysql_table''')

    def test_create_index_with_length(self):
        m = MetaData()
        tbl = Table('testtbl', m, Column('data', String(255)))
        idx = Index('test_idx1', tbl.c.data,
                    mysql_length=10)
        idx2 = Index('test_idx2', tbl.c.data,
                    mysql_length=5)

        self.assert_compile(schema.CreateIndex(idx),
                            'CREATE INDEX test_idx1 ON testtbl (data(10))',
                            dialect=mysql.dialect())
        self.assert_compile(schema.CreateIndex(idx2),
                            "CREATE INDEX test_idx2 ON testtbl (data(5))",
                            dialect=mysql.dialect())

class DialectTest(fixtures.TestBase):
    __only_on__ = 'mysql'

    @testing.only_on(['mysql+mysqldb', 'mysql+oursql'], 
                    'requires particular SSL arguments')
    def test_ssl_arguments(self):
        dialect = testing.db.dialect
        kwarg = dialect.create_connect_args(
            make_url("mysql://scott:tiger@localhost:3306/test"
                "?ssl_ca=/ca.pem&ssl_cert=/cert.pem&ssl_key=/key.pem")
        )[1]
        # args that differ among mysqldb and oursql
        for k in ('use_unicode', 'found_rows', 'client_flag'):
            kwarg.pop(k, None)
        eq_(
            kwarg, 
            {
                'passwd': 'tiger', 'db': 'test', 
                'ssl': {'ca': '/ca.pem', 'cert': '/cert.pem', 
                        'key': '/key.pem'}, 
                'host': 'localhost', 'user': 'scott', 
                'port': 3306
            }
        )

class TypesTest(fixtures.TestBase, AssertsExecutionResults, AssertsCompiledSQL):
    "Test MySQL column types"

    __only_on__ = 'mysql'
    __dialect__ = mysql.dialect()

    @testing.uses_deprecated('Manually quoting ENUM value literals')
    def test_basic(self):
        meta1 = MetaData(testing.db)
        table = Table(
            'mysql_types', meta1,
            Column('id', Integer, primary_key=True),
            Column('num1', mysql.MSInteger(unsigned=True)),
            Column('text1', mysql.MSLongText),
            Column('text2', mysql.MSLongText()),
            Column('num2', mysql.MSBigInteger),
            Column('num3', mysql.MSBigInteger()),
            Column('num4', mysql.MSDouble),
            Column('num5', mysql.MSDouble()),
            Column('num6', mysql.MSMediumInteger),
            Column('enum1', mysql.ENUM("'black'", "'white'")),
            Column('enum2', mysql.ENUM("dog", "cat")),
            )
        try:
            table.drop(checkfirst=True)
            table.create()
            meta2 = MetaData(testing.db)
            t2 = Table('mysql_types', meta2, autoload=True)
            assert isinstance(t2.c.num1.type, mysql.MSInteger)
            assert t2.c.num1.type.unsigned
            assert isinstance(t2.c.text1.type, mysql.MSLongText)
            assert isinstance(t2.c.text2.type, mysql.MSLongText)
            assert isinstance(t2.c.num2.type, mysql.MSBigInteger)
            assert isinstance(t2.c.num3.type, mysql.MSBigInteger)
            assert isinstance(t2.c.num4.type, mysql.MSDouble)
            assert isinstance(t2.c.num5.type, mysql.MSDouble)
            assert isinstance(t2.c.num6.type, mysql.MSMediumInteger)
            assert isinstance(t2.c.enum1.type, mysql.ENUM)
            assert isinstance(t2.c.enum2.type, mysql.ENUM)
            t2.drop()
            t2.create()
        finally:
            meta1.drop_all()

    def test_numeric(self):
        "Exercise type specification and options for numeric types."

        columns = [
            # column type, args, kwargs, expected ddl
            # e.g. Column(Integer(10, unsigned=True)) == 
            # 'INTEGER(10) UNSIGNED'
            (mysql.MSNumeric, [], {},
             'NUMERIC'),
            (mysql.MSNumeric, [None], {},
             'NUMERIC'),
            (mysql.MSNumeric, [12], {},
             'NUMERIC(12)'),
            (mysql.MSNumeric, [12, 4], {'unsigned':True},
             'NUMERIC(12, 4) UNSIGNED'),
            (mysql.MSNumeric, [12, 4], {'zerofill':True},
             'NUMERIC(12, 4) ZEROFILL'),
            (mysql.MSNumeric, [12, 4], {'zerofill':True, 'unsigned':True},
             'NUMERIC(12, 4) UNSIGNED ZEROFILL'),

            (mysql.MSDecimal, [], {},
             'DECIMAL'),
            (mysql.MSDecimal, [None], {},
             'DECIMAL'),
            (mysql.MSDecimal, [12], {},
             'DECIMAL(12)'),
            (mysql.MSDecimal, [12, None], {},
             'DECIMAL(12)'),
            (mysql.MSDecimal, [12, 4], {'unsigned':True},
             'DECIMAL(12, 4) UNSIGNED'),
            (mysql.MSDecimal, [12, 4], {'zerofill':True},
             'DECIMAL(12, 4) ZEROFILL'),
            (mysql.MSDecimal, [12, 4], {'zerofill':True, 'unsigned':True},
             'DECIMAL(12, 4) UNSIGNED ZEROFILL'),

            (mysql.MSDouble, [None, None], {},
             'DOUBLE'),
            (mysql.MSDouble, [12, 4], {'unsigned':True},
             'DOUBLE(12, 4) UNSIGNED'),
            (mysql.MSDouble, [12, 4], {'zerofill':True},
             'DOUBLE(12, 4) ZEROFILL'),
            (mysql.MSDouble, [12, 4], {'zerofill':True, 'unsigned':True},
             'DOUBLE(12, 4) UNSIGNED ZEROFILL'),

            (mysql.MSReal, [None, None], {},
             'REAL'),
            (mysql.MSReal, [12, 4], {'unsigned':True},
             'REAL(12, 4) UNSIGNED'),
            (mysql.MSReal, [12, 4], {'zerofill':True},
             'REAL(12, 4) ZEROFILL'),
            (mysql.MSReal, [12, 4], {'zerofill':True, 'unsigned':True},
             'REAL(12, 4) UNSIGNED ZEROFILL'),

            (mysql.MSFloat, [], {},
             'FLOAT'),
            (mysql.MSFloat, [None], {},
             'FLOAT'),
            (mysql.MSFloat, [12], {},
             'FLOAT(12)'),
            (mysql.MSFloat, [12, 4], {},
             'FLOAT(12, 4)'),
            (mysql.MSFloat, [12, 4], {'unsigned':True},
             'FLOAT(12, 4) UNSIGNED'),
            (mysql.MSFloat, [12, 4], {'zerofill':True},
             'FLOAT(12, 4) ZEROFILL'),
            (mysql.MSFloat, [12, 4], {'zerofill':True, 'unsigned':True},
             'FLOAT(12, 4) UNSIGNED ZEROFILL'),

            (mysql.MSInteger, [], {},
             'INTEGER'),
            (mysql.MSInteger, [4], {},
             'INTEGER(4)'),
            (mysql.MSInteger, [4], {'unsigned':True},
             'INTEGER(4) UNSIGNED'),
            (mysql.MSInteger, [4], {'zerofill':True},
             'INTEGER(4) ZEROFILL'),
            (mysql.MSInteger, [4], {'zerofill':True, 'unsigned':True},
             'INTEGER(4) UNSIGNED ZEROFILL'),

            (mysql.MSBigInteger, [], {},
             'BIGINT'),
            (mysql.MSBigInteger, [4], {},
             'BIGINT(4)'),
            (mysql.MSBigInteger, [4], {'unsigned':True},
             'BIGINT(4) UNSIGNED'),
            (mysql.MSBigInteger, [4], {'zerofill':True},
             'BIGINT(4) ZEROFILL'),
            (mysql.MSBigInteger, [4], {'zerofill':True, 'unsigned':True},
             'BIGINT(4) UNSIGNED ZEROFILL'),

             (mysql.MSMediumInteger, [], {},
              'MEDIUMINT'),
             (mysql.MSMediumInteger, [4], {},
              'MEDIUMINT(4)'),
             (mysql.MSMediumInteger, [4], {'unsigned':True},
              'MEDIUMINT(4) UNSIGNED'),
             (mysql.MSMediumInteger, [4], {'zerofill':True},
              'MEDIUMINT(4) ZEROFILL'),
             (mysql.MSMediumInteger, [4], {'zerofill':True, 'unsigned':True},
              'MEDIUMINT(4) UNSIGNED ZEROFILL'),

            (mysql.MSTinyInteger, [], {},
             'TINYINT'),
            (mysql.MSTinyInteger, [1], {},
             'TINYINT(1)'),
            (mysql.MSTinyInteger, [1], {'unsigned':True},
             'TINYINT(1) UNSIGNED'),
            (mysql.MSTinyInteger, [1], {'zerofill':True},
             'TINYINT(1) ZEROFILL'),
            (mysql.MSTinyInteger, [1], {'zerofill':True, 'unsigned':True},
             'TINYINT(1) UNSIGNED ZEROFILL'),

            (mysql.MSSmallInteger, [], {},
             'SMALLINT'),
            (mysql.MSSmallInteger, [4], {},
             'SMALLINT(4)'),
            (mysql.MSSmallInteger, [4], {'unsigned':True},
             'SMALLINT(4) UNSIGNED'),
            (mysql.MSSmallInteger, [4], {'zerofill':True},
             'SMALLINT(4) ZEROFILL'),
            (mysql.MSSmallInteger, [4], {'zerofill':True, 'unsigned':True},
             'SMALLINT(4) UNSIGNED ZEROFILL'),
           ]

        table_args = ['test_mysql_numeric', MetaData(testing.db)]
        for index, spec in enumerate(columns):
            type_, args, kw, res = spec
            table_args.append(Column('c%s' % index, type_(*args, **kw)))

        numeric_table = Table(*table_args)
        gen = testing.db.dialect.ddl_compiler(testing.db.dialect, None)

        for col in numeric_table.c:
            index = int(col.name[1:])
            eq_(gen.get_column_specification(col),
                           "%s %s" % (col.name, columns[index][3]))
            self.assert_(repr(col))

        try:
            numeric_table.create(checkfirst=True)
            assert True
        except:
            raise
        numeric_table.drop()

    @testing.exclude('mysql', '<', (4, 1, 1), 'no charset support')
    def test_charset(self):
        """Exercise CHARACTER SET and COLLATE-ish options on string types."""

        columns = [
            (mysql.MSChar, [1], {},
             'CHAR(1)'),
             (mysql.NCHAR, [1], {},
              'NATIONAL CHAR(1)'),
            (mysql.MSChar, [1], {'binary':True},
             'CHAR(1) BINARY'),
            (mysql.MSChar, [1], {'ascii':True},
             'CHAR(1) ASCII'),
            (mysql.MSChar, [1], {'unicode':True},
             'CHAR(1) UNICODE'),
            (mysql.MSChar, [1], {'ascii':True, 'binary':True},
             'CHAR(1) ASCII BINARY'),
            (mysql.MSChar, [1], {'unicode':True, 'binary':True},
             'CHAR(1) UNICODE BINARY'),
            (mysql.MSChar, [1], {'charset':'utf8'},
             'CHAR(1) CHARACTER SET utf8'),
            (mysql.MSChar, [1], {'charset':'utf8', 'binary':True},
             'CHAR(1) CHARACTER SET utf8 BINARY'),
            (mysql.MSChar, [1], {'charset':'utf8', 'unicode':True},
             'CHAR(1) CHARACTER SET utf8'),
            (mysql.MSChar, [1], {'charset':'utf8', 'ascii':True},
             'CHAR(1) CHARACTER SET utf8'),
            (mysql.MSChar, [1], {'collation': 'utf8_bin'},
             'CHAR(1) COLLATE utf8_bin'),
            (mysql.MSChar, [1], {'charset': 'utf8', 'collation': 'utf8_bin'},
             'CHAR(1) CHARACTER SET utf8 COLLATE utf8_bin'),
            (mysql.MSChar, [1], {'charset': 'utf8', 'binary': True},
             'CHAR(1) CHARACTER SET utf8 BINARY'),
            (mysql.MSChar, [1], {'charset': 'utf8', 'collation': 'utf8_bin',
                              'binary': True},
             'CHAR(1) CHARACTER SET utf8 COLLATE utf8_bin'),
            (mysql.MSChar, [1], {'national':True},
             'NATIONAL CHAR(1)'),
            (mysql.MSChar, [1], {'national':True, 'charset':'utf8'},
             'NATIONAL CHAR(1)'),
            (mysql.MSChar, [1], {'national':True, 'charset':'utf8',
                                'binary':True},
             'NATIONAL CHAR(1) BINARY'),
            (mysql.MSChar, [1], {'national':True, 'binary':True,
                                'unicode':True},
             'NATIONAL CHAR(1) BINARY'),
            (mysql.MSChar, [1], {'national':True, 'collation':'utf8_bin'},
             'NATIONAL CHAR(1) COLLATE utf8_bin'),

            (mysql.MSString, [1], {'charset':'utf8', 'collation':'utf8_bin'},
             'VARCHAR(1) CHARACTER SET utf8 COLLATE utf8_bin'),
            (mysql.MSString, [1], {'national':True, 'collation':'utf8_bin'},
             'NATIONAL VARCHAR(1) COLLATE utf8_bin'),

            (mysql.MSTinyText, [], {'charset':'utf8', 'collation':'utf8_bin'},
             'TINYTEXT CHARACTER SET utf8 COLLATE utf8_bin'),

            (mysql.MSMediumText, [], {'charset':'utf8', 'binary':True},
             'MEDIUMTEXT CHARACTER SET utf8 BINARY'),

            (mysql.MSLongText, [], {'ascii':True},
             'LONGTEXT ASCII'),

            (mysql.ENUM, ["foo", "bar"], {'unicode':True},
             '''ENUM('foo','bar') UNICODE''')
           ]

        table_args = ['test_mysql_charset', MetaData(testing.db)]
        for index, spec in enumerate(columns):
            type_, args, kw, res = spec
            table_args.append(Column('c%s' % index, type_(*args, **kw)))

        charset_table = Table(*table_args)
        gen = testing.db.dialect.ddl_compiler(testing.db.dialect, None)

        for col in charset_table.c:
            index = int(col.name[1:])
            eq_(gen.get_column_specification(col),
                           "%s %s" % (col.name, columns[index][3]))
            self.assert_(repr(col))

        try:
            charset_table.create(checkfirst=True)
            assert True
        except:
            raise
        charset_table.drop()

    @testing.exclude('mysql', '<', (5, 0, 5), 'a 5.0+ feature')
    @testing.provide_metadata
    def test_charset_collate_table(self):
        t = Table('foo', self.metadata,
            Column('id', Integer),
            mysql_default_charset='utf8',
            mysql_collate='utf8_unicode_ci'
        )
        t.create()
        m2 = MetaData(testing.db)
        t2 = Table('foo', m2, autoload=True)
        eq_(t2.kwargs['mysql_collate'], 'utf8_unicode_ci')
        eq_(t2.kwargs['mysql_default charset'], 'utf8')

    @testing.exclude('mysql', '<', (5, 0, 5), 'a 5.0+ feature')
    @testing.fails_on('mysql+oursql', 'some round trips fail, oursql bug ?')
    def test_bit_50(self):
        """Exercise BIT types on 5.0+ (not valid for all engine types)"""

        meta = MetaData(testing.db)
        bit_table = Table('mysql_bits', meta,
                          Column('b1', mysql.MSBit),
                          Column('b2', mysql.MSBit()),
                          Column('b3', mysql.MSBit(), nullable=False),
                          Column('b4', mysql.MSBit(1)),
                          Column('b5', mysql.MSBit(8)),
                          Column('b6', mysql.MSBit(32)),
                          Column('b7', mysql.MSBit(63)),
                          Column('b8', mysql.MSBit(64)))

        eq_(colspec(bit_table.c.b1), 'b1 BIT')
        eq_(colspec(bit_table.c.b2), 'b2 BIT')
        eq_(colspec(bit_table.c.b3), 'b3 BIT NOT NULL')
        eq_(colspec(bit_table.c.b4), 'b4 BIT(1)')
        eq_(colspec(bit_table.c.b5), 'b5 BIT(8)')
        eq_(colspec(bit_table.c.b6), 'b6 BIT(32)')
        eq_(colspec(bit_table.c.b7), 'b7 BIT(63)')
        eq_(colspec(bit_table.c.b8), 'b8 BIT(64)')

        for col in bit_table.c:
            self.assert_(repr(col))
        try:
            meta.create_all()

            meta2 = MetaData(testing.db)
            reflected = Table('mysql_bits', meta2, autoload=True)

            for table in bit_table, reflected:

                def roundtrip(store, expected=None):
                    expected = expected or store
                    table.insert(store).execute()
                    row = table.select().execute().first()
                    try:
                        self.assert_(list(row) == expected)
                    except:
                        print "Storing %s" % store
                        print "Expected %s" % expected
                        print "Found %s" % list(row)
                        raise
                    table.delete().execute().close()

                roundtrip([0] * 8)
                roundtrip([None, None, 0, None, None, None, None, None])
                roundtrip([1] * 8)
                roundtrip([sql.text("b'1'")] * 8, [1] * 8)

                i = 255
                roundtrip([0, 0, 0, 0, i, i, i, i])
                i = 2**32 - 1
                roundtrip([0, 0, 0, 0, 0, i, i, i])
                i = 2**63 - 1
                roundtrip([0, 0, 0, 0, 0, 0, i, i])
                i = 2**64 - 1
                roundtrip([0, 0, 0, 0, 0, 0, 0, i])
        finally:
            meta.drop_all()

    def test_boolean(self):
        """Test BOOL/TINYINT(1) compatibility and reflection."""

        meta = MetaData(testing.db)
        bool_table = Table(
            'mysql_bool',
            meta,
            Column('b1', BOOLEAN),
            Column('b2', Boolean),
            Column('b3', mysql.MSTinyInteger(1)),
            Column('b4', mysql.MSTinyInteger(1, unsigned=True)),
            Column('b5', mysql.MSTinyInteger),
            )
        eq_(colspec(bool_table.c.b1), 'b1 BOOL')
        eq_(colspec(bool_table.c.b2), 'b2 BOOL')
        eq_(colspec(bool_table.c.b3), 'b3 TINYINT(1)')
        eq_(colspec(bool_table.c.b4), 'b4 TINYINT(1) UNSIGNED')
        eq_(colspec(bool_table.c.b5), 'b5 TINYINT')
        for col in bool_table.c:
            self.assert_(repr(col))
        try:
            meta.create_all()
            table = bool_table

            def roundtrip(store, expected=None):
                expected = expected or store
                table.insert(store).execute()
                row = table.select().execute().first()
                try:
                    self.assert_(list(row) == expected)
                    for i, val in enumerate(expected):
                        if isinstance(val, bool):
                            self.assert_(val is row[i])
                except:
                    print 'Storing %s' % store
                    print 'Expected %s' % expected
                    print 'Found %s' % list(row)
                    raise
                table.delete().execute().close()

            roundtrip([None, None, None, None, None])
            roundtrip([True, True, 1, 1, 1])
            roundtrip([False, False, 0, 0, 0])
            roundtrip([True, True, True, True, True], [True, True, 1,
                      1, 1])
            roundtrip([False, False, 0, 0, 0], [False, False, 0, 0, 0])
            meta2 = MetaData(testing.db)
            table = Table('mysql_bool', meta2, autoload=True)
            eq_(colspec(table.c.b3), 'b3 TINYINT(1)')
            eq_(colspec(table.c.b4), 'b4 TINYINT(1) UNSIGNED')
            meta2 = MetaData(testing.db)
            table = Table(
                'mysql_bool',
                meta2,
                Column('b1', BOOLEAN),
                Column('b2', Boolean),
                Column('b3', BOOLEAN),
                Column('b4', BOOLEAN),
                autoload=True,
                )
            eq_(colspec(table.c.b3), 'b3 BOOL')
            eq_(colspec(table.c.b4), 'b4 BOOL')
            roundtrip([None, None, None, None, None])
            roundtrip([True, True, 1, 1, 1], [True, True, True, True,
                      1])
            roundtrip([False, False, 0, 0, 0], [False, False, False,
                      False, 0])
            roundtrip([True, True, True, True, True], [True, True,
                      True, True, 1])
            roundtrip([False, False, 0, 0, 0], [False, False, False,
                      False, 0])
        finally:
            meta.drop_all()

    @testing.exclude('mysql', '<', (4, 1, 0), '4.1+ syntax')
    def test_timestamp(self):
        """Exercise funky TIMESTAMP default syntax."""

        meta = MetaData(testing.db)

        try:
            columns = [
                ([TIMESTAMP],
                 'TIMESTAMP NULL'),
                ([mysql.MSTimeStamp],
                 'TIMESTAMP NULL'),
                ([mysql.MSTimeStamp,
                  DefaultClause(sql.text('CURRENT_TIMESTAMP'))],
                 "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"),
                ([mysql.MSTimeStamp,
                  DefaultClause(sql.text("'1999-09-09 09:09:09'"))],
                 "TIMESTAMP DEFAULT '1999-09-09 09:09:09'"),
                ([mysql.MSTimeStamp,
                  DefaultClause(sql.text("'1999-09-09 09:09:09' "
                                          "ON UPDATE CURRENT_TIMESTAMP"))],
                 "TIMESTAMP DEFAULT '1999-09-09 09:09:09' "
                 "ON UPDATE CURRENT_TIMESTAMP"),
                ([mysql.MSTimeStamp,
                  DefaultClause(sql.text("CURRENT_TIMESTAMP "
                                          "ON UPDATE CURRENT_TIMESTAMP"))],
                 "TIMESTAMP DEFAULT CURRENT_TIMESTAMP "
                 "ON UPDATE CURRENT_TIMESTAMP"),
                ]
            for idx, (spec, expected) in enumerate(columns):
                t = Table('mysql_ts%s' % idx, meta,
                          Column('id', Integer, primary_key=True),
                          Column('t', *spec))
                eq_(colspec(t.c.t), "t %s" % expected)
                self.assert_(repr(t.c.t))
                t.create()
                r = Table('mysql_ts%s' % idx, MetaData(testing.db),
                          autoload=True)
                if len(spec) > 1:
                    self.assert_(r.c.t is not None)
        finally:
            meta.drop_all()

    def test_timestamp_nullable(self):
        meta = MetaData(testing.db)
        ts_table = Table('mysql_timestamp', meta,
                            Column('t1', TIMESTAMP),
                            Column('t2', TIMESTAMP, nullable=False),
                    )
        meta.create_all()
        try:
            # there's a slight assumption here that this test can
            # complete within the scope of a single second.
            # if needed, can break out the eq_() just to check for
            # timestamps that are within a few seconds of "now" 
            # using timedelta.

            now = testing.db.execute("select now()").scalar()

            # TIMESTAMP without NULL inserts current time when passed
            # NULL.  when not passed, generates 0000-00-00 quite
            # annoyingly.
            ts_table.insert().execute({'t1':now, 't2':None})
            ts_table.insert().execute({'t1':None, 't2':None})

            eq_(
                ts_table.select().execute().fetchall(),
                [(now, now), (None, now)]
            )
        finally:
            meta.drop_all()

    def test_year(self):
        """Exercise YEAR."""

        meta = MetaData(testing.db)
        year_table = Table('mysql_year', meta,
                           Column('y1', mysql.MSYear),
                           Column('y2', mysql.MSYear),
                           Column('y3', mysql.MSYear),
                           Column('y4', mysql.MSYear(2)),
                           Column('y5', mysql.MSYear(4)))

        for col in year_table.c:
            self.assert_(repr(col))
        try:
            year_table.create()
            reflected = Table('mysql_year', MetaData(testing.db),
                              autoload=True)

            for table in year_table, reflected:
                table.insert(['1950', '50', None, 50, 1950]).execute()
                row = table.select().execute().first()
                eq_(list(row), [1950, 2050, None, 50, 1950])
                table.delete().execute()
                self.assert_(colspec(table.c.y1).startswith('y1 YEAR'))
                eq_(colspec(table.c.y4), 'y4 YEAR(2)')
                eq_(colspec(table.c.y5), 'y5 YEAR(4)')
        finally:
            meta.drop_all()


    def test_set(self):
        """Exercise the SET type."""

        meta = MetaData(testing.db)
        set_table = Table('mysql_set', meta, Column('s1',
                          mysql.MSSet("'dq'", "'sq'")), Column('s2',
                          mysql.MSSet("'a'")), Column('s3',
                          mysql.MSSet("'5'", "'7'", "'9'")))
        eq_(colspec(set_table.c.s1), "s1 SET('dq','sq')")
        eq_(colspec(set_table.c.s2), "s2 SET('a')")
        eq_(colspec(set_table.c.s3), "s3 SET('5','7','9')")
        for col in set_table.c:
            self.assert_(repr(col))
        try:
            set_table.create()
            reflected = Table('mysql_set', MetaData(testing.db),
                              autoload=True)
            for table in set_table, reflected:

                def roundtrip(store, expected=None):
                    expected = expected or store
                    table.insert(store).execute()
                    row = table.select().execute().first()
                    try:
                        self.assert_(list(row) == expected)
                    except:
                        print 'Storing %s' % store
                        print 'Expected %s' % expected
                        print 'Found %s' % list(row)
                        raise
                    table.delete().execute()

                roundtrip([None, None, None], [None] * 3)
                roundtrip(['', '', ''], [set([''])] * 3)
                roundtrip([set(['dq']), set(['a']), set(['5'])])
                roundtrip(['dq', 'a', '5'], [set(['dq']), set(['a']),
                          set(['5'])])
                roundtrip([1, 1, 1], [set(['dq']), set(['a']), set(['5'
                          ])])
                roundtrip([set(['dq', 'sq']), None, set(['9', '5', '7'
                          ])])
            set_table.insert().execute({'s3': set(['5'])}, {'s3'
                    : set(['5', '7'])}, {'s3': set(['5', '7', '9'])},
                    {'s3': set(['7', '9'])})
            rows = select([set_table.c.s3], set_table.c.s3.in_([set(['5'
                          ]), set(['5', '7']), set(['7', '5'
                          ])])).execute().fetchall()
            found = set([frozenset(row[0]) for row in rows])
            eq_(found, set([frozenset(['5']), frozenset(['5', '7'])]))
        finally:
            meta.drop_all()

    @testing.uses_deprecated('Manually quoting ENUM value literals')
    def test_enum(self):
        """Exercise the ENUM type."""

        db = testing.db
        enum_table = Table('mysql_enum', MetaData(testing.db),
            Column('e1', mysql.ENUM("'a'", "'b'")),
            Column('e2', mysql.ENUM("'a'", "'b'"),
                   nullable=False),
            Column('e2generic', Enum("a", "b"),
                  nullable=False),
            Column('e3', mysql.ENUM("'a'", "'b'", strict=True)),
            Column('e4', mysql.ENUM("'a'", "'b'", strict=True),
                   nullable=False),
            Column('e5', mysql.ENUM("a", "b")),
            Column('e5generic', Enum("a", "b")),
            Column('e6', mysql.ENUM("'a'", "b")),
            )

        eq_(colspec(enum_table.c.e1),
                       "e1 ENUM('a','b')")
        eq_(colspec(enum_table.c.e2),
                       "e2 ENUM('a','b') NOT NULL")
        eq_(colspec(enum_table.c.e2generic),
                      "e2generic ENUM('a','b') NOT NULL")
        eq_(colspec(enum_table.c.e3),
                       "e3 ENUM('a','b')")
        eq_(colspec(enum_table.c.e4),
                       "e4 ENUM('a','b') NOT NULL")
        eq_(colspec(enum_table.c.e5),
                       "e5 ENUM('a','b')")
        eq_(colspec(enum_table.c.e5generic),
                      "e5generic ENUM('a','b')")
        eq_(colspec(enum_table.c.e6),
                       "e6 ENUM('''a''','b')")
        enum_table.drop(checkfirst=True)
        enum_table.create()

        assert_raises(exc.DBAPIError, enum_table.insert().execute, 
                        e1=None, e2=None, e3=None, e4=None)

        assert_raises(exc.StatementError, enum_table.insert().execute,
                                        e1='c', e2='c', e2generic='c', e3='c',
                                        e4='c', e5='c', e5generic='c', e6='c')

        enum_table.insert().execute()
        enum_table.insert().execute(e1='a', e2='a', e2generic='a', e3='a',
                                    e4='a', e5='a', e5generic='a', e6="'a'")
        enum_table.insert().execute(e1='b', e2='b', e2generic='b', e3='b',
                                    e4='b', e5='b', e5generic='b', e6='b')

        res = enum_table.select().execute().fetchall()

        expected = [(None, 'a', 'a', None, 'a', None, None, None), 
                    ('a', 'a', 'a', 'a', 'a', 'a', 'a', "'a'"), 
                    ('b', 'b', 'b', 'b', 'b', 'b', 'b', 'b')]

        # This is known to fail with MySQLDB 1.2.2 beta versions
        # which return these as sets.Set(['a']), sets.Set(['b'])
        # (even on Pythons with __builtin__.set)
        if (testing.against('mysql+mysqldb') and
            testing.db.dialect.dbapi.version_info < (1, 2, 2, 'beta', 3) and
            testing.db.dialect.dbapi.version_info >= (1, 2, 2)):
            # these mysqldb seem to always uses 'sets', even on later pythons
            import sets
            def convert(value):
                if value is None:
                    return value
                if value == '':
                    return sets.Set([])
                else:
                    return sets.Set([value])

            e = []
            for row in expected:
                e.append(tuple([convert(c) for c in row]))
            expected = e

        eq_(res, expected)
        enum_table.drop()

    def test_unicode_enum(self):
        unicode_engine = utf8_engine()
        metadata = MetaData(unicode_engine)
        t1 = Table('table', metadata,
            Column('id', Integer, primary_key=True),
            Column('value', Enum(u'réveillé', u'drôle', u'S’il')),
            Column('value2', mysql.ENUM(u'réveillé', u'drôle', u'S’il'))
        )
        metadata.create_all()
        try:
            t1.insert().execute(value=u'drôle', value2=u'drôle')
            t1.insert().execute(value=u'réveillé', value2=u'réveillé')
            t1.insert().execute(value=u'S’il', value2=u'S’il')
            eq_(t1.select().order_by(t1.c.id).execute().fetchall(), 
                [(1, u'drôle', u'drôle'), (2, u'réveillé', u'réveillé'), 
                            (3, u'S’il', u'S’il')]
            )

            # test reflection of the enum labels

            m2 = MetaData(testing.db)
            t2 = Table('table', m2, autoload=True)

            # TODO: what's wrong with the last element ?  is there
            # latin-1 stuff forcing its way in ?

            assert t2.c.value.type.enums[0:2] == \
                    (u'réveillé', u'drôle') #, u'S’il') # eh ?
            assert t2.c.value2.type.enums[0:2] == \
                    (u'réveillé', u'drôle') #, u'S’il') # eh ? 
        finally:
            metadata.drop_all()

    def test_enum_compile(self):
        e1 = Enum('x', 'y', 'z', name='somename')
        t1 = Table('sometable', MetaData(), Column('somecolumn', e1))
        self.assert_compile(schema.CreateTable(t1),
                            "CREATE TABLE sometable (somecolumn "
                            "ENUM('x','y','z'))")
        t1 = Table('sometable', MetaData(), Column('somecolumn',
                   Enum('x', 'y', 'z', native_enum=False)))
        self.assert_compile(schema.CreateTable(t1),
                            "CREATE TABLE sometable (somecolumn "
                            "VARCHAR(1), CHECK (somecolumn IN ('x', "
                            "'y', 'z')))")

    @testing.exclude('mysql', '<', (4,), "3.23 can't handle an ENUM of ''")
    @testing.uses_deprecated('Manually quoting ENUM value literals')
    def test_enum_parse(self):
        """More exercises for the ENUM type."""

        # MySQL 3.23 can't handle an ENUM of ''....

        enum_table = Table('mysql_enum', MetaData(testing.db),
            Column('e1', mysql.ENUM("'a'")),
            Column('e2', mysql.ENUM("''")),
            Column('e3', mysql.ENUM('a')),
            Column('e4', mysql.ENUM('')),
            Column('e5', mysql.ENUM("'a'", "''")),
            Column('e6', mysql.ENUM("''", "'a'")),
            Column('e7', mysql.ENUM("''", "'''a'''", "'b''b'", "''''")))

        for col in enum_table.c:
            self.assert_(repr(col))
        try:
            enum_table.create()
            reflected = Table('mysql_enum', MetaData(testing.db),
                              autoload=True)
            for t in enum_table, reflected:
                eq_(t.c.e1.type.enums, ("a",))
                eq_(t.c.e2.type.enums, ("",))
                eq_(t.c.e3.type.enums, ("a",))
                eq_(t.c.e4.type.enums, ("",))
                eq_(t.c.e5.type.enums, ("a", ""))
                eq_(t.c.e6.type.enums, ("", "a"))
                eq_(t.c.e7.type.enums, ("", "'a'", "b'b", "'"))
        finally:
            enum_table.drop()

class ReflectionTest(fixtures.TestBase, AssertsExecutionResults):

    __only_on__ = 'mysql'

    def test_default_reflection(self):
        """Test reflection of column defaults."""

        from sqlalchemy.dialects.mysql import VARCHAR
        def_table = Table(
            'mysql_def',
            MetaData(testing.db),
            Column('c1', VARCHAR(10, collation='utf8_unicode_ci'),
                   DefaultClause(''), nullable=False),
            Column('c2', String(10), DefaultClause('0')),
            Column('c3', String(10), DefaultClause('abc')),
            Column('c4', TIMESTAMP, DefaultClause('2009-04-05 12:00:00'
                   )),
            Column('c5', TIMESTAMP),
            Column('c6', TIMESTAMP,
                   DefaultClause(sql.text("CURRENT_TIMESTAMP "
                                          "ON UPDATE CURRENT_TIMESTAMP"))),
            )
        def_table.create()
        try:
            reflected = Table('mysql_def', MetaData(testing.db),
                              autoload=True)
        finally:
            def_table.drop()
        assert def_table.c.c1.server_default.arg == ''
        assert def_table.c.c2.server_default.arg == '0'
        assert def_table.c.c3.server_default.arg == 'abc'
        assert def_table.c.c4.server_default.arg \
            == '2009-04-05 12:00:00'
        assert str(reflected.c.c1.server_default.arg) == "''"
        assert str(reflected.c.c2.server_default.arg) == "'0'"
        assert str(reflected.c.c3.server_default.arg) == "'abc'"
        assert str(reflected.c.c4.server_default.arg) \
            == "'2009-04-05 12:00:00'"
        assert reflected.c.c5.default is None
        assert reflected.c.c5.server_default is None
        assert reflected.c.c6.default is None
        eq_(
            str(reflected.c.c6.server_default.arg).upper(),
            "CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"
        )
        reflected.create()
        try:
            reflected2 = Table('mysql_def', MetaData(testing.db),
                               autoload=True)
        finally:
            reflected.drop()
        assert str(reflected2.c.c1.server_default.arg) == "''"
        assert str(reflected2.c.c2.server_default.arg) == "'0'"
        assert str(reflected2.c.c3.server_default.arg) == "'abc'"
        assert str(reflected2.c.c4.server_default.arg) \
            == "'2009-04-05 12:00:00'"
        assert reflected.c.c5.default is None
        assert reflected.c.c5.server_default is None
        assert reflected.c.c6.default is None
        eq_(
            str(reflected.c.c6.server_default.arg).upper(),
            "CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"
        )

    def test_reflection_with_table_options(self):
        comment = r"""Comment types type speedily ' " \ '' Fun!"""

        def_table = Table('mysql_def', MetaData(testing.db),
            Column('c1', Integer()),
            mysql_engine='MEMORY',
            mysql_comment=comment,
            mysql_default_charset='utf8',
            mysql_auto_increment='5',
            mysql_avg_row_length='3',
            mysql_password='secret',
            mysql_connection='fish',
        )

        def_table.create()
        try:
            reflected = Table('mysql_def', MetaData(testing.db),
                          autoload=True)
        finally:
            def_table.drop()

        assert def_table.kwargs['mysql_engine'] == 'MEMORY'
        assert def_table.kwargs['mysql_comment'] == comment
        assert def_table.kwargs['mysql_default_charset'] == 'utf8'
        assert def_table.kwargs['mysql_auto_increment'] == '5'
        assert def_table.kwargs['mysql_avg_row_length'] == '3'
        assert def_table.kwargs['mysql_password'] == 'secret'
        assert def_table.kwargs['mysql_connection'] == 'fish'

        assert reflected.kwargs['mysql_engine'] == 'MEMORY'
        assert reflected.kwargs['mysql_comment'] == comment
        assert reflected.kwargs['mysql_default charset'] == 'utf8'
        assert reflected.kwargs['mysql_avg_row_length'] == '3'
        assert reflected.kwargs['mysql_connection'] == 'fish'

        # This field doesn't seem to be returned by mysql itself.
        #assert reflected.kwargs['mysql_password'] == 'secret'

        # This is explicitly ignored when reflecting schema.
        #assert reflected.kwargs['mysql_auto_increment'] == '5'

    def test_reflection_on_include_columns(self):
        """Test reflection of include_columns to be sure they respect case."""

        case_table = Table('mysql_case', MetaData(testing.db),
            Column('c1', String(10)),
            Column('C2', String(10)),
            Column('C3', String(10)))

        try:
            case_table.create()
            reflected = Table('mysql_case', MetaData(testing.db),
                              autoload=True, include_columns=['c1', 'C2'])
            for t in case_table, reflected:
                assert 'c1' in t.c.keys()
                assert 'C2' in t.c.keys()
            reflected2 = Table('mysql_case', MetaData(testing.db),
                              autoload=True, include_columns=['c1', 'c2'])
            assert 'c1' in reflected2.c.keys()
            for c in ['c2', 'C2', 'C3']:
                assert c not in reflected2.c.keys()
        finally:
            case_table.drop()

    @testing.exclude('mysql', '<', (5, 0, 0), 'early types are squirrely')
    @testing.uses_deprecated('Using String type with no length')
    @testing.uses_deprecated('Manually quoting ENUM value literals')
    def test_type_reflection(self):
        # (ask_for, roundtripped_as_if_different)
        specs = [( String(1), mysql.MSString(1), ),
                 ( String(3), mysql.MSString(3), ),
                 ( Text(), mysql.MSText(), ),
                 ( Unicode(1), mysql.MSString(1), ),
                 ( Unicode(3), mysql.MSString(3), ),
                 ( UnicodeText(), mysql.MSText(), ),
                 ( mysql.MSChar(1), ),
                 ( mysql.MSChar(3), ),
                 ( NCHAR(2), mysql.MSChar(2), ),
                 ( mysql.MSNChar(2), mysql.MSChar(2), ), # N is CREATE only
                 ( mysql.MSNVarChar(22), mysql.MSString(22), ),
                 ( SmallInteger(), mysql.MSSmallInteger(), ),
                 ( SmallInteger(), mysql.MSSmallInteger(4), ),
                 ( mysql.MSSmallInteger(), ),
                 ( mysql.MSSmallInteger(4), mysql.MSSmallInteger(4), ),
                 ( mysql.MSMediumInteger(), mysql.MSMediumInteger(), ),
                 ( mysql.MSMediumInteger(8), mysql.MSMediumInteger(8), ),
                 ( LargeBinary(3), mysql.TINYBLOB(), ),
                 ( LargeBinary(), mysql.BLOB() ),
                 ( mysql.MSBinary(3), mysql.MSBinary(3), ),
                 ( mysql.MSVarBinary(3),),
                 ( mysql.MSTinyBlob(),),
                 ( mysql.MSBlob(),),
                 ( mysql.MSBlob(1234), mysql.MSBlob()),
                 ( mysql.MSMediumBlob(),),
                 ( mysql.MSLongBlob(),),
                 ( mysql.ENUM("''","'fleem'"), ),
                 ]

        columns = [Column('c%i' % (i + 1), t[0]) for i, t in enumerate(specs)]

        db = testing.db
        m = MetaData(db)
        t_table = Table('mysql_types', m, *columns)
        try:
            m.create_all()

            m2 = MetaData(db)
            rt = Table('mysql_types', m2, autoload=True)
            try:
                db.execute('CREATE OR REPLACE VIEW mysql_types_v '
                           'AS SELECT * from mysql_types')
                rv = Table('mysql_types_v', m2, autoload=True)

                expected = [len(c) > 1 and c[1] or c[0] for c in specs]

                # Early 5.0 releases seem to report more "general" for columns
                # in a view, e.g. char -> varchar, tinyblob -> mediumblob
                #
                # Not sure exactly which point version has the fix.
                if db.dialect.server_version_info < (5, 0, 11):
                    tables = rt,
                else:
                    tables = rt, rv

                for table in tables:
                    for i, reflected in enumerate(table.c):
                        assert isinstance(reflected.type,
                                type(expected[i])), \
                            'element %d: %r not instance of %r' % (i,
                                reflected.type, type(expected[i]))
            finally:
                db.execute('DROP VIEW mysql_types_v')
        finally:
            m.drop_all()

    def test_autoincrement(self):
        meta = MetaData(testing.db)
        try:
            Table('ai_1', meta,
                  Column('int_y', Integer, primary_key=True),
                  Column('int_n', Integer, DefaultClause('0'),
                         primary_key=True),
                         mysql_engine='MyISAM')
            Table('ai_2', meta,
                  Column('int_y', Integer, primary_key=True),
                  Column('int_n', Integer, DefaultClause('0'),
                         primary_key=True),
                         mysql_engine='MyISAM')
            Table('ai_3', meta,
                  Column('int_n', Integer, DefaultClause('0'),
                         primary_key=True, autoincrement=False),
                  Column('int_y', Integer, primary_key=True),
                         mysql_engine='MyISAM')
            Table('ai_4', meta,
                  Column('int_n', Integer, DefaultClause('0'),
                         primary_key=True, autoincrement=False),
                  Column('int_n2', Integer, DefaultClause('0'),
                         primary_key=True, autoincrement=False),
                         mysql_engine='MyISAM')
            Table('ai_5', meta,
                  Column('int_y', Integer, primary_key=True),
                  Column('int_n', Integer, DefaultClause('0'),
                         primary_key=True, autoincrement=False),
                         mysql_engine='MyISAM')
            Table('ai_6', meta,
                  Column('o1', String(1), DefaultClause('x'),
                         primary_key=True),
                  Column('int_y', Integer, primary_key=True),
                         mysql_engine='MyISAM')
            Table('ai_7', meta,
                  Column('o1', String(1), DefaultClause('x'),
                         primary_key=True),
                  Column('o2', String(1), DefaultClause('x'),
                         primary_key=True),
                  Column('int_y', Integer, primary_key=True),
                         mysql_engine='MyISAM')
            Table('ai_8', meta,
                  Column('o1', String(1), DefaultClause('x'),
                         primary_key=True),
                  Column('o2', String(1), DefaultClause('x'),
                         primary_key=True),
                         mysql_engine='MyISAM')
            meta.create_all()

            table_names = ['ai_1', 'ai_2', 'ai_3', 'ai_4',
                           'ai_5', 'ai_6', 'ai_7', 'ai_8']
            mr = MetaData(testing.db)
            mr.reflect(only=table_names)

            for tbl in [mr.tables[name] for name in table_names]:
                for c in tbl.c:
                    if c.name.startswith('int_y'):
                        assert c.autoincrement
                    elif c.name.startswith('int_n'):
                        assert not c.autoincrement
                tbl.insert().execute()
                if 'int_y' in tbl.c:
                    assert select([tbl.c.int_y]).scalar() == 1
                    assert list(tbl.select().execute().first()).count(1) == 1
                else:
                    assert 1 not in list(tbl.select().execute().first())
        finally:
            meta.drop_all()



class SQLTest(fixtures.TestBase, AssertsCompiledSQL):
    """Tests MySQL-dialect specific compilation."""

    __dialect__ = mysql.dialect()

    def test_precolumns(self):
        dialect = self.__dialect__

        def gen(distinct=None, prefixes=None):
            kw = {}
            if distinct is not None:
                kw['distinct'] = distinct
            if prefixes is not None:
                kw['prefixes'] = prefixes
            return str(select(['q'], **kw).compile(dialect=dialect))

        eq_(gen(None), 'SELECT q')
        eq_(gen(True), 'SELECT DISTINCT q')

        assert_raises(
            exc.SADeprecationWarning,
            gen, 'DISTINCT'
        )

        eq_(gen(prefixes=['ALL']), 'SELECT ALL q')
        eq_(gen(prefixes=['DISTINCTROW']), 
                'SELECT DISTINCTROW q')

        # Interaction with MySQL prefix extensions
        eq_(
            gen(None, ['straight_join']),
            'SELECT straight_join q')
        eq_(
            gen(False, ['HIGH_PRIORITY', 'SQL_SMALL_RESULT', 'ALL']),
            'SELECT HIGH_PRIORITY SQL_SMALL_RESULT ALL q')
        eq_(
            gen(True, ['high_priority', sql.text('sql_cache')]),
            'SELECT high_priority sql_cache DISTINCT q')

    @testing.uses_deprecated
    def test_deprecated_distinct(self):
        dialect = self.__dialect__

        self.assert_compile(
            select(['q'], distinct='ALL'),
            'SELECT ALL q',
        )

        self.assert_compile(
            select(['q'], distinct='distinctROW'),
            'SELECT DISTINCTROW q',
        )

        self.assert_compile(
            select(['q'], distinct='ALL', 
                    prefixes=['HIGH_PRIORITY', 'SQL_SMALL_RESULT']),
            'SELECT HIGH_PRIORITY SQL_SMALL_RESULT ALL q'
        )

    def test_backslash_escaping(self):
        self.assert_compile(
            sql.column('foo').like('bar', escape='\\'),
            "foo LIKE %s ESCAPE '\\\\'"
        )

        dialect = mysql.dialect()
        dialect._backslash_escapes=False
        self.assert_compile(
            sql.column('foo').like('bar', escape='\\'),
            "foo LIKE %s ESCAPE '\\'",
            dialect=dialect
        )

    def test_limit(self):
        t = sql.table('t', sql.column('col1'), sql.column('col2'))

        self.assert_compile(
            select([t]).limit(10).offset(20),
            "SELECT t.col1, t.col2 FROM t  LIMIT %s, %s",
            {'param_1':20, 'param_2':10}
            )
        self.assert_compile(
            select([t]).limit(10),
            "SELECT t.col1, t.col2 FROM t  LIMIT %s", 
            {'param_1':10})

        self.assert_compile(
            select([t]).offset(10),
            "SELECT t.col1, t.col2 FROM t  LIMIT %s, 18446744073709551615",
            {'param_1':10}
            )

    def test_varchar_raise(self):
        for type_ in (
            String,
            VARCHAR,
            String(),
            VARCHAR(),
            NVARCHAR(),
            Unicode,
            Unicode(),
        ):
            type_ = sqltypes.to_instance(type_)
            assert_raises_message(
                exc.CompileError, 
                "VARCHAR requires a length on dialect mysql",
                type_.compile, 
            dialect=mysql.dialect())

            t1 = Table('sometable', MetaData(),
                Column('somecolumn', type_)
            )
            assert_raises_message(
                exc.CompileError,
                r"\(in table 'sometable', column 'somecolumn'\)\: "
                r"(?:N)?VARCHAR requires a length on dialect mysql",
                schema.CreateTable(t1).compile,
                dialect=mysql.dialect()
            )

    def test_update_limit(self):
        t = sql.table('t', sql.column('col1'), sql.column('col2'))

        self.assert_compile(
            t.update(values={'col1':123}),
            "UPDATE t SET col1=%s"
            )
        self.assert_compile(
            t.update(values={'col1':123}, mysql_limit=5),
            "UPDATE t SET col1=%s LIMIT 5"
            )
        self.assert_compile(
            t.update(values={'col1':123}, mysql_limit=None),
            "UPDATE t SET col1=%s"
            )
        self.assert_compile(
            t.update(t.c.col2==456, values={'col1':123}, mysql_limit=1),
            "UPDATE t SET col1=%s WHERE t.col2 = %s LIMIT 1"
            )

    def test_utc_timestamp(self):
        self.assert_compile(func.utc_timestamp(), "UTC_TIMESTAMP")

    def test_sysdate(self):
        self.assert_compile(func.sysdate(), "SYSDATE()")

    def test_cast(self):
        t = sql.table('t', sql.column('col'))
        m = mysql

        specs = [
            (Integer, "CAST(t.col AS SIGNED INTEGER)"),
            (INT, "CAST(t.col AS SIGNED INTEGER)"),
            (m.MSInteger, "CAST(t.col AS SIGNED INTEGER)"),
            (m.MSInteger(unsigned=True), "CAST(t.col AS UNSIGNED INTEGER)"),
            (SmallInteger, "CAST(t.col AS SIGNED INTEGER)"),
            (m.MSSmallInteger, "CAST(t.col AS SIGNED INTEGER)"),
            (m.MSTinyInteger, "CAST(t.col AS SIGNED INTEGER)"),
            # 'SIGNED INTEGER' is a bigint, so this is ok.
            (m.MSBigInteger, "CAST(t.col AS SIGNED INTEGER)"),
            (m.MSBigInteger(unsigned=False), "CAST(t.col AS SIGNED INTEGER)"),
            (m.MSBigInteger(unsigned=True), 
                            "CAST(t.col AS UNSIGNED INTEGER)"),
            (m.MSBit, "t.col"),

            # this is kind of sucky.  thank you default arguments!
            (NUMERIC, "CAST(t.col AS DECIMAL)"),
            (DECIMAL, "CAST(t.col AS DECIMAL)"),
            (Numeric, "CAST(t.col AS DECIMAL)"),
            (m.MSNumeric, "CAST(t.col AS DECIMAL)"),
            (m.MSDecimal, "CAST(t.col AS DECIMAL)"),

            (FLOAT, "t.col"),
            (Float, "t.col"),
            (m.MSFloat, "t.col"),
            (m.MSDouble, "t.col"),
            (m.MSReal, "t.col"),

            (TIMESTAMP, "CAST(t.col AS DATETIME)"),
            (DATETIME, "CAST(t.col AS DATETIME)"),
            (DATE, "CAST(t.col AS DATE)"),
            (TIME, "CAST(t.col AS TIME)"),
            (DateTime, "CAST(t.col AS DATETIME)"),
            (Date, "CAST(t.col AS DATE)"),
            (Time, "CAST(t.col AS TIME)"),
            (DateTime, "CAST(t.col AS DATETIME)"),
            (Date, "CAST(t.col AS DATE)"),
            (m.MSTime, "CAST(t.col AS TIME)"),
            (m.MSTimeStamp, "CAST(t.col AS DATETIME)"),
            (m.MSYear, "t.col"),
            (m.MSYear(2), "t.col"),
            (Interval, "t.col"),

            (String, "CAST(t.col AS CHAR)"),
            (Unicode, "CAST(t.col AS CHAR)"),
            (UnicodeText, "CAST(t.col AS CHAR)"),
            (VARCHAR, "CAST(t.col AS CHAR)"),
            (NCHAR, "CAST(t.col AS CHAR)"),
            (CHAR, "CAST(t.col AS CHAR)"),
            (CLOB, "CAST(t.col AS CHAR)"),
            (TEXT, "CAST(t.col AS CHAR)"),
            (String(32), "CAST(t.col AS CHAR(32))"),
            (Unicode(32), "CAST(t.col AS CHAR(32))"),
            (CHAR(32), "CAST(t.col AS CHAR(32))"),
            (m.MSString, "CAST(t.col AS CHAR)"),
            (m.MSText, "CAST(t.col AS CHAR)"),
            (m.MSTinyText, "CAST(t.col AS CHAR)"),
            (m.MSMediumText, "CAST(t.col AS CHAR)"),
            (m.MSLongText, "CAST(t.col AS CHAR)"),
            (m.MSNChar, "CAST(t.col AS CHAR)"),
            (m.MSNVarChar, "CAST(t.col AS CHAR)"),

            (LargeBinary, "CAST(t.col AS BINARY)"),
            (BLOB, "CAST(t.col AS BINARY)"),
            (m.MSBlob, "CAST(t.col AS BINARY)"),
            (m.MSBlob(32), "CAST(t.col AS BINARY)"),
            (m.MSTinyBlob, "CAST(t.col AS BINARY)"),
            (m.MSMediumBlob, "CAST(t.col AS BINARY)"),
            (m.MSLongBlob, "CAST(t.col AS BINARY)"),
            (m.MSBinary, "CAST(t.col AS BINARY)"),
            (m.MSBinary(32), "CAST(t.col AS BINARY)"),
            (m.MSVarBinary, "CAST(t.col AS BINARY)"),
            (m.MSVarBinary(32), "CAST(t.col AS BINARY)"),

            # maybe this could be changed to something more DWIM, needs
            # testing
            (Boolean, "t.col"),
            (BOOLEAN, "t.col"),

            (m.MSEnum, "t.col"),
            (m.MSEnum("1", "2"), "t.col"),
            (m.MSSet, "t.col"),
            (m.MSSet("1", "2"), "t.col"),
            ]

        for type_, expected in specs:
            self.assert_compile(cast(t.c.col, type_), expected)

    def test_no_cast_pre_4(self):
        self.assert_compile(
                    cast(Column('foo', Integer), String),
                    "CAST(foo AS CHAR)",
            )
        dialect = mysql.dialect()
        dialect.server_version_info = (3, 2, 3)
        self.assert_compile(
                    cast(Column('foo', Integer), String),
                    "foo",
                    dialect=dialect
            )

    def test_extract(self):
        t = sql.table('t', sql.column('col1'))

        for field in 'year', 'month', 'day':
            self.assert_compile(
                select([extract(field, t.c.col1)]),
                "SELECT EXTRACT(%s FROM t.col1) AS anon_1 FROM t" % field)

        # millsecondS to millisecond
        self.assert_compile(
            select([extract('milliseconds', t.c.col1)]),
            "SELECT EXTRACT(millisecond FROM t.col1) AS anon_1 FROM t")

    def test_too_long_index(self):
        exp = 'ix_zyrenian_zyme_zyzzogeton_zyzzogeton_zyrenian_zyme_zyz_5cd2'
        tname = 'zyrenian_zyme_zyzzogeton_zyzzogeton'
        cname = 'zyrenian_zyme_zyzzogeton_zo'

        t1 = Table(tname, MetaData(), 
                    Column(cname, Integer, index=True),
                )
        ix1 = list(t1.indexes)[0]

        self.assert_compile(
            schema.CreateIndex(ix1),
            "CREATE INDEX %s "
            "ON %s (%s)" % (exp, tname, cname),
            dialect=mysql.dialect()
        )

    def test_innodb_autoincrement(self):
        t1 = Table('sometable', MetaData(), Column('assigned_id',
                   Integer(), primary_key=True, autoincrement=False),
                   Column('id', Integer(), primary_key=True,
                   autoincrement=True), mysql_engine='InnoDB')
        self.assert_compile(schema.CreateTable(t1),
                            'CREATE TABLE sometable (assigned_id '
                            'INTEGER NOT NULL, id INTEGER NOT NULL '
                            'AUTO_INCREMENT, PRIMARY KEY (assigned_id, '
                            'id), KEY `idx_autoinc_id`(`id`))ENGINE=Inn'
                            'oDB')

        t1 = Table('sometable', MetaData(), Column('assigned_id',
                   Integer(), primary_key=True, autoincrement=True),
                   Column('id', Integer(), primary_key=True,
                   autoincrement=False), mysql_engine='InnoDB')
        self.assert_compile(schema.CreateTable(t1),
                            'CREATE TABLE sometable (assigned_id '
                            'INTEGER NOT NULL AUTO_INCREMENT, id '
                            'INTEGER NOT NULL, PRIMARY KEY '
                            '(assigned_id, id))ENGINE=InnoDB')

class SQLModeDetectionTest(fixtures.TestBase):
    __only_on__ = 'mysql'

    def _options(self, modes):
        def connect(con, record):
            cursor = con.cursor()
            print "DOING THiS:", "set sql_mode='%s'" % (",".join(modes))
            cursor.execute("set sql_mode='%s'" % (",".join(modes)))
        e = engines.testing_engine(options={
            'pool_events':[
                (connect, 'first_connect'),
                (connect, 'connect')
            ]
        })
        return e

    def test_backslash_escapes(self):
        engine = self._options(['NO_BACKSLASH_ESCAPES'])
        c = engine.connect()
        assert not engine.dialect._backslash_escapes
        c.close()
        engine.dispose()

        engine = self._options([])
        c = engine.connect()
        assert engine.dialect._backslash_escapes
        c.close()
        engine.dispose()

    def test_ansi_quotes(self):
        engine = self._options(['ANSI_QUOTES'])
        c = engine.connect()
        assert engine.dialect._server_ansiquotes
        c.close()
        engine.dispose()

    def test_combination(self):
        engine = self._options(['ANSI_QUOTES,NO_BACKSLASH_ESCAPES'])
        c = engine.connect()
        assert engine.dialect._server_ansiquotes
        assert not engine.dialect._backslash_escapes
        c.close()
        engine.dispose()

class RawReflectionTest(fixtures.TestBase):
    def setup(self):
        dialect = mysql.dialect()
        self.parser = mysql.MySQLTableDefinitionParser(dialect, dialect.identifier_preparer)

    def test_key_reflection(self):
        regex = self.parser._re_key

        assert regex.match('  PRIMARY KEY (`id`),')
        assert regex.match('  PRIMARY KEY USING BTREE (`id`),')
        assert regex.match('  PRIMARY KEY (`id`) USING BTREE,')
        assert regex.match('  PRIMARY KEY (`id`)')
        assert regex.match('  PRIMARY KEY USING BTREE (`id`)')
        assert regex.match('  PRIMARY KEY (`id`) USING BTREE')


class ExecutionTest(fixtures.TestBase):
    """Various MySQL execution special cases."""

    __only_on__ = 'mysql'

    def test_charset_caching(self):
        engine = engines.testing_engine()

        cx = engine.connect()
        meta = MetaData()
        charset = engine.dialect._detect_charset(cx)

        meta.reflect(cx)
        eq_(cx.dialect._connection_charset, charset)
        cx.close()

    def test_sysdate(self):
        d = testing.db.scalar(func.sysdate())
        assert isinstance(d, datetime.datetime)

class MatchTest(fixtures.TestBase, AssertsCompiledSQL):
    __only_on__ = 'mysql'

    @classmethod
    def setup_class(cls):
        global metadata, cattable, matchtable
        metadata = MetaData(testing.db)

        cattable = Table('cattable', metadata,
            Column('id', Integer, primary_key=True),
            Column('description', String(50)),
            mysql_engine='MyISAM'
        )
        matchtable = Table('matchtable', metadata,
            Column('id', Integer, primary_key=True),
            Column('title', String(200)),
            Column('category_id', Integer, ForeignKey('cattable.id')),
            mysql_engine='MyISAM'
        )
        metadata.create_all()

        cattable.insert().execute([
            {'id': 1, 'description': 'Python'},
            {'id': 2, 'description': 'Ruby'},
        ])
        matchtable.insert().execute([
            {'id': 1,
             'title': 'Agile Web Development with Rails',
             'category_id': 2},
            {'id': 2,
             'title': 'Dive Into Python',
             'category_id': 1},
            {'id': 3,
             'title': "Programming Matz's Ruby",
             'category_id': 2},
            {'id': 4,
             'title': 'The Definitive Guide to Django',
             'category_id': 1},
            {'id': 5,
             'title': 'Python in a Nutshell',
             'category_id': 1}
        ])

    @classmethod
    def teardown_class(cls):
        metadata.drop_all()

    @testing.fails_on('mysql+mysqlconnector', 'uses pyformat')
    def test_expression(self):
        format = testing.db.dialect.paramstyle == 'format' and '%s' or '?'
        self.assert_compile(
            matchtable.c.title.match('somstr'),
            "MATCH (matchtable.title) AGAINST (%s IN BOOLEAN MODE)" % format)

    @testing.fails_on('mysql+mysqldb', 'uses format')
    @testing.fails_on('mysql+pymysql', 'uses format')
    @testing.fails_on('mysql+oursql', 'uses format')
    @testing.fails_on('mysql+pyodbc', 'uses format')
    @testing.fails_on('mysql+zxjdbc', 'uses format')
    def test_expression(self):
        format = '%(title_1)s'
        self.assert_compile(
            matchtable.c.title.match('somstr'),
            "MATCH (matchtable.title) AGAINST (%s IN BOOLEAN MODE)" % format)

    def test_simple_match(self):
        results = (matchtable.select().
                   where(matchtable.c.title.match('python')).
                   order_by(matchtable.c.id).
                   execute().
                   fetchall())
        eq_([2, 5], [r.id for r in results])

    def test_simple_match_with_apostrophe(self):
        results = (matchtable.select().
                   where(matchtable.c.title.match("Matz's")).
                   execute().
                   fetchall())
        eq_([3], [r.id for r in results])

    def test_or_match(self):
        results1 = (matchtable.select().
                    where(or_(matchtable.c.title.match('nutshell'),
                              matchtable.c.title.match('ruby'))).
                    order_by(matchtable.c.id).
                    execute().
                    fetchall())
        eq_([3, 5], [r.id for r in results1])
        results2 = (matchtable.select().
                    where(matchtable.c.title.match('nutshell ruby')).
                    order_by(matchtable.c.id).
                    execute().
                    fetchall())
        eq_([3, 5], [r.id for r in results2])


    def test_and_match(self):
        results1 = (matchtable.select().
                    where(and_(matchtable.c.title.match('python'),
                               matchtable.c.title.match('nutshell'))).
                    execute().
                    fetchall())
        eq_([5], [r.id for r in results1])
        results2 = (matchtable.select().
                    where(matchtable.c.title.match('+python +nutshell')).
                    execute().
                    fetchall())
        eq_([5], [r.id for r in results2])

    def test_match_across_joins(self):
        results = (matchtable.select().
                   where(and_(cattable.c.id==matchtable.c.category_id,
                              or_(cattable.c.description.match('Ruby'),
                                  matchtable.c.title.match('nutshell')))).
                   order_by(matchtable.c.id).
                   execute().
                   fetchall())
        eq_([1, 3, 5], [r.id for r in results])


def colspec(c):
    return testing.db.dialect.ddl_compiler(
                    testing.db.dialect, None).get_column_specification(c)

