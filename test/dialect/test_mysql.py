from sqlalchemy.test.testing import eq_
import sets
from sqlalchemy import *
from sqlalchemy import sql, exc
from sqlalchemy.databases import mysql
from sqlalchemy.test.testing import eq_
from sqlalchemy.test import *


class TypesTest(TestBase, AssertsExecutionResults):
    "Test MySQL column types"

    __only_on__ = 'mysql'

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
            Column('enum1', mysql.MSEnum("'black'", "'white'")),
            Column('enum2', mysql.MSEnum("dog", "cat")),
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
            assert isinstance(t2.c.enum1.type, mysql.MSEnum)
            assert isinstance(t2.c.enum2.type, mysql.MSEnum)
            t2.drop()
            t2.create()
        finally:
            meta1.drop_all()

    def test_numeric(self):
        "Exercise type specification and options for numeric types."

        columns = [
            # column type, args, kwargs, expected ddl
            # e.g. Column(Integer(10, unsigned=True)) == 'INTEGER(10) UNSIGNED'
            (mysql.MSNumeric, [], {},
             'NUMERIC(10, 2)'),
            (mysql.MSNumeric, [None], {},
             'NUMERIC'),
            (mysql.MSNumeric, [12], {},
             'NUMERIC(12, 2)'),
            (mysql.MSNumeric, [12, 4], {'unsigned':True},
             'NUMERIC(12, 4) UNSIGNED'),
            (mysql.MSNumeric, [12, 4], {'zerofill':True},
             'NUMERIC(12, 4) ZEROFILL'),
            (mysql.MSNumeric, [12, 4], {'zerofill':True, 'unsigned':True},
             'NUMERIC(12, 4) UNSIGNED ZEROFILL'),

            (mysql.MSDecimal, [], {},
             'DECIMAL(10, 2)'),
            (mysql.MSDecimal, [None], {},
             'DECIMAL'),
            (mysql.MSDecimal, [12], {},
             'DECIMAL(12, 2)'),
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
        gen = testing.db.dialect.schemagenerator(testing.db.dialect, testing.db, None, None)

        for col in numeric_table.c:
            index = int(col.name[1:])
            self.assert_eq(gen.get_column_specification(col),
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
            (mysql.MSChar, [1], {'national':True, 'charset':'utf8', 'binary':True},
             'NATIONAL CHAR(1) BINARY'),
            (mysql.MSChar, [1], {'national':True, 'binary':True, 'unicode':True},
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

            (mysql.MSEnum, ["foo", "bar"], {'unicode':True},
             '''ENUM('foo','bar') UNICODE''')
           ]

        table_args = ['test_mysql_charset', MetaData(testing.db)]
        for index, spec in enumerate(columns):
            type_, args, kw, res = spec
            table_args.append(Column('c%s' % index, type_(*args, **kw)))

        charset_table = Table(*table_args)
        gen = testing.db.dialect.schemagenerator(testing.db.dialect, testing.db, None, None)

        for col in charset_table.c:
            index = int(col.name[1:])
            self.assert_eq(gen.get_column_specification(col),
                           "%s %s" % (col.name, columns[index][3]))
            self.assert_(repr(col))

        try:
            charset_table.create(checkfirst=True)
            assert True
        except:
            raise
        charset_table.drop()

    @testing.exclude('mysql', '<', (5, 0, 5), 'a 5.0+ feature')
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

        self.assert_eq(colspec(bit_table.c.b1), 'b1 BIT')
        self.assert_eq(colspec(bit_table.c.b2), 'b2 BIT')
        self.assert_eq(colspec(bit_table.c.b3), 'b3 BIT NOT NULL')
        self.assert_eq(colspec(bit_table.c.b4), 'b4 BIT(1)')
        self.assert_eq(colspec(bit_table.c.b5), 'b5 BIT(8)')
        self.assert_eq(colspec(bit_table.c.b6), 'b6 BIT(32)')
        self.assert_eq(colspec(bit_table.c.b7), 'b7 BIT(63)')
        self.assert_eq(colspec(bit_table.c.b8), 'b8 BIT(64)')

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
                    row = list(table.select().execute())[0]
                    try:
                        self.assert_(list(row) == expected)
                    except:
                        print "Storing %s" % store
                        print "Expected %s" % expected
                        print "Found %s" % list(row)
                        raise
                    table.delete().execute()

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
        """Test BOOL/TINYINT(1) compatability and reflection."""

        meta = MetaData(testing.db)
        bool_table = Table('mysql_bool', meta,
                           Column('b1', BOOLEAN),
                           Column('b2', mysql.MSBoolean),
                           Column('b3', mysql.MSTinyInteger(1)),
                           Column('b4', mysql.MSTinyInteger))

        self.assert_eq(colspec(bool_table.c.b1), 'b1 BOOL')
        self.assert_eq(colspec(bool_table.c.b2), 'b2 BOOL')
        self.assert_eq(colspec(bool_table.c.b3), 'b3 TINYINT(1)')
        self.assert_eq(colspec(bool_table.c.b4), 'b4 TINYINT')

        for col in bool_table.c:
            self.assert_(repr(col))
        try:
            meta.create_all()

            table = bool_table
            def roundtrip(store, expected=None):
                expected = expected or store
                table.insert(store).execute()
                row = list(table.select().execute())[0]
                try:
                    self.assert_(list(row) == expected)
                    for i, val in enumerate(expected):
                        if isinstance(val, bool):
                            self.assert_(val is row[i])
                except:
                    print "Storing %s" % store
                    print "Expected %s" % expected
                    print "Found %s" % list(row)
                    raise
                table.delete().execute()


            roundtrip([None, None, None, None])
            roundtrip([True, True, 1, 1])
            roundtrip([False, False, 0, 0])
            roundtrip([True, True, True, True], [True, True, 1, 1])
            roundtrip([False, False, 0, 0], [False, False, 0, 0])

            meta2 = MetaData(testing.db)
            # replace with reflected
            table = Table('mysql_bool', meta2, autoload=True)
            self.assert_eq(colspec(table.c.b3), 'b3 BOOL')

            roundtrip([None, None, None, None])
            roundtrip([True, True, 1, 1], [True, True, True, 1])
            roundtrip([False, False, 0, 0], [False, False, False, 0])
            roundtrip([True, True, True, True], [True, True, True, 1])
            roundtrip([False, False, 0, 0], [False, False, False, 0])
        finally:
            meta.drop_all()

    @testing.exclude('mysql', '<', (4, 1, 0), '4.1+ syntax')
    def test_timestamp(self):
        """Exercise funky TIMESTAMP default syntax."""

        meta = MetaData(testing.db)

        try:
            columns = [
                ([TIMESTAMP],
                 'TIMESTAMP'),
                ([mysql.MSTimeStamp],
                 'TIMESTAMP'),
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
                self.assert_eq(colspec(t.c.t), "t %s" % expected)
                self.assert_(repr(t.c.t))
                t.create()
                r = Table('mysql_ts%s' % idx, MetaData(testing.db),
                          autoload=True)
                if len(spec) > 1:
                    self.assert_(r.c.t is not None)
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
                row = list(table.select().execute())[0]
                self.assert_eq(list(row), [1950, 2050, None, 50, 1950])
                table.delete().execute()
                self.assert_(colspec(table.c.y1).startswith('y1 YEAR'))
                self.assert_eq(colspec(table.c.y4), 'y4 YEAR(2)')
                self.assert_eq(colspec(table.c.y5), 'y5 YEAR(4)')
        finally:
            meta.drop_all()


    def test_set(self):
        """Exercise the SET type."""

        meta = MetaData(testing.db)
        set_table = Table('mysql_set', meta,
                          Column('s1', mysql.MSSet("'dq'", "'sq'")),
                          Column('s2', mysql.MSSet("'a'")),
                          Column('s3', mysql.MSSet("'5'", "'7'", "'9'")))

        self.assert_eq(colspec(set_table.c.s1), "s1 SET('dq','sq')")
        self.assert_eq(colspec(set_table.c.s2), "s2 SET('a')")
        self.assert_eq(colspec(set_table.c.s3), "s3 SET('5','7','9')")

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
                    row = list(table.select().execute())[0]
                    try:
                        self.assert_(list(row) == expected)
                    except:
                        print "Storing %s" % store
                        print "Expected %s" % expected
                        print "Found %s" % list(row)
                        raise
                    table.delete().execute()

                roundtrip([None, None, None],[None] * 3)
                roundtrip(['', '', ''], [set([''])] * 3)

                roundtrip([set(['dq']), set(['a']), set(['5'])])
                roundtrip(['dq', 'a', '5'],
                          [set(['dq']), set(['a']), set(['5'])])
                roundtrip([1, 1, 1],
                          [set(['dq']), set(['a']), set(['5'])])
                roundtrip([set(['dq', 'sq']), None, set(['9', '5', '7'])])

            set_table.insert().execute({'s3':set(['5'])},
                                       {'s3':set(['5', '7'])},
                                       {'s3':set(['5', '7', '9'])},
                                       {'s3':set(['7', '9'])})
            rows = list(select(
                [set_table.c.s3],
                set_table.c.s3.in_([set(['5']), set(['5', '7'])])).execute())
            found = set([frozenset(row[0]) for row in rows])
            eq_(found,
                              set([frozenset(['5']), frozenset(['5', '7'])]))
        finally:
            meta.drop_all()

    def test_enum(self):
        """Exercise the ENUM type."""

        db = testing.db
        enum_table = Table('mysql_enum', MetaData(testing.db),
            Column('e1', mysql.MSEnum("'a'", "'b'")),
            Column('e2', mysql.MSEnum("'a'", "'b'"),
                   nullable=False),
            Column('e3', mysql.MSEnum("'a'", "'b'", strict=True)),
            Column('e4', mysql.MSEnum("'a'", "'b'", strict=True),
                   nullable=False),
            Column('e5', mysql.MSEnum("a", "b")),
            Column('e6', mysql.MSEnum("'a'", "b")),
            )

        self.assert_eq(colspec(enum_table.c.e1),
                       "e1 ENUM('a','b')")
        self.assert_eq(colspec(enum_table.c.e2),
                       "e2 ENUM('a','b') NOT NULL")
        self.assert_eq(colspec(enum_table.c.e3),
                       "e3 ENUM('a','b')")
        self.assert_eq(colspec(enum_table.c.e4),
                       "e4 ENUM('a','b') NOT NULL")
        self.assert_eq(colspec(enum_table.c.e5),
                       "e5 ENUM('a','b')")
        self.assert_eq(colspec(enum_table.c.e6),
                       "e6 ENUM('''a''','b')")
        enum_table.drop(checkfirst=True)
        enum_table.create()

        try:
            enum_table.insert().execute(e1=None, e2=None, e3=None, e4=None)
            self.assert_(False)
        except exc.SQLError:
            self.assert_(True)

        try:
            enum_table.insert().execute(e1='c', e2='c', e3='c',
                                        e4='c', e5='c', e6='c')
            self.assert_(False)
        except exc.InvalidRequestError:
            self.assert_(True)

        enum_table.insert().execute()
        enum_table.insert().execute(e1='a', e2='a', e3='a',
                                    e4='a', e5='a', e6="'a'")
        enum_table.insert().execute(e1='b', e2='b', e3='b',
                                    e4='b', e5='b', e6='b')

        res = enum_table.select().execute().fetchall()

        expected = [(None, 'a', None, 'a', None, None),
                    ('a', 'a', 'a', 'a', 'a', "'a'"),
                    ('b', 'b', 'b', 'b', 'b', 'b')]

        # This is known to fail with MySQLDB 1.2.2 beta versions
        # which return these as sets.Set(['a']), sets.Set(['b'])
        # (even on Pythons with __builtin__.set)
        if testing.db.dialect.dbapi.version_info < (1, 2, 2, 'beta', 3) and \
           testing.db.dialect.dbapi.version_info >= (1, 2, 2):
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

        self.assert_eq(res, expected)
        enum_table.drop()

    @testing.exclude('mysql', '<', (4,), "3.23 can't handle an ENUM of ''")
    def test_enum_parse(self):
        """More exercises for the ENUM type."""

        # MySQL 3.23 can't handle an ENUM of ''....

        enum_table = Table('mysql_enum', MetaData(testing.db),
            Column('e1', mysql.MSEnum("'a'")),
            Column('e2', mysql.MSEnum("''")),
            Column('e3', mysql.MSEnum('a')),
            Column('e4', mysql.MSEnum('')),
            Column('e5', mysql.MSEnum("'a'", "''")),
            Column('e6', mysql.MSEnum("''", "'a'")),
            Column('e7', mysql.MSEnum("''", "'''a'''", "'b''b'", "''''")))

        for col in enum_table.c:
            self.assert_(repr(col))
        try:
            enum_table.create()
            reflected = Table('mysql_enum', MetaData(testing.db),
                              autoload=True)
            for t in enum_table, reflected:
                eq_(t.c.e1.type.enums, ["a"])
                eq_(t.c.e2.type.enums, [""])
                eq_(t.c.e3.type.enums, ["a"])
                eq_(t.c.e4.type.enums, [""])
                eq_(t.c.e5.type.enums, ["a", ""])
                eq_(t.c.e6.type.enums, ["", "a"])
                eq_(t.c.e7.type.enums, ["", "'a'", "b'b", "'"])
        finally:
            enum_table.drop()

    def test_default_reflection(self):
        """Test reflection of column defaults."""

        def_table = Table('mysql_def', MetaData(testing.db),
            Column('c1', String(10), DefaultClause('')),
            Column('c2', String(10), DefaultClause('0')),
            Column('c3', String(10), DefaultClause('abc')))

        try:
            def_table.create()
            reflected = Table('mysql_def', MetaData(testing.db),
                              autoload=True)
            for t in def_table, reflected:
                assert t.c.c1.server_default.arg == ''
                assert t.c.c2.server_default.arg == '0'
                assert t.c.c3.server_default.arg == 'abc'
        finally:
            def_table.drop()

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
                 ( Binary(3), mysql.MSBlob(3), ),
                 ( Binary(), mysql.MSBlob() ),
                 ( mysql.MSBinary(3), mysql.MSBinary(3), ),
                 ( mysql.MSVarBinary(3),),
                 ( mysql.MSVarBinary(), mysql.MSBlob()),
                 ( mysql.MSTinyBlob(),),
                 ( mysql.MSBlob(),),
                 ( mysql.MSBlob(1234), mysql.MSBlob()),
                 ( mysql.MSMediumBlob(),),
                 ( mysql.MSLongBlob(),),
                 ( mysql.MSEnum("''","'fleem'"), ),
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
                if db.dialect.server_version_info(db.connect()) < (5, 0, 11):
                    tables = rt,
                else:
                    tables = rt, rv

                for table in tables:
                    for i, reflected in enumerate(table.c):
                        assert isinstance(reflected.type, type(expected[i]))
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
                         primary_key=True))
            Table('ai_2', meta,
                  Column('int_y', Integer, primary_key=True),
                  Column('int_n', Integer, DefaultClause('0'),
                         primary_key=True))
            Table('ai_3', meta,
                  Column('int_n', Integer, DefaultClause('0'),
                         primary_key=True, autoincrement=False),
                  Column('int_y', Integer, primary_key=True))
            Table('ai_4', meta,
                  Column('int_n', Integer, DefaultClause('0'),
                         primary_key=True, autoincrement=False),
                  Column('int_n2', Integer, DefaultClause('0'),
                         primary_key=True, autoincrement=False))
            Table('ai_5', meta,
                  Column('int_y', Integer, primary_key=True),
                  Column('int_n', Integer, DefaultClause('0'),
                         primary_key=True, autoincrement=False))
            Table('ai_6', meta,
                  Column('o1', String(1), DefaultClause('x'),
                         primary_key=True),
                  Column('int_y', Integer, primary_key=True))
            Table('ai_7', meta,
                  Column('o1', String(1), DefaultClause('x'),
                         primary_key=True),
                  Column('o2', String(1), DefaultClause('x'),
                         primary_key=True),
                  Column('int_y', Integer, primary_key=True))
            Table('ai_8', meta,
                  Column('o1', String(1), DefaultClause('x'),
                         primary_key=True),
                  Column('o2', String(1), DefaultClause('x'),
                         primary_key=True))
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
                    assert list(tbl.select().execute().fetchone()).count(1) == 1
                else:
                    assert 1 not in list(tbl.select().execute().fetchone())
        finally:
            meta.drop_all()

    def assert_eq(self, got, wanted):
        if got != wanted:
            print "Expected %s" % wanted
            print "Found %s" % got
        eq_(got, wanted)


class SQLTest(TestBase, AssertsCompiledSQL):
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
        eq_(gen(1), 'SELECT DISTINCT q')
        eq_(gen('diSTInct'), 'SELECT DISTINCT q')
        eq_(gen('DISTINCT'), 'SELECT DISTINCT q')

        # Standard SQL
        eq_(gen('all'), 'SELECT ALL q')
        eq_(gen('distinctrow'), 'SELECT DISTINCTROW q')

        # Interaction with MySQL prefix extensions
        eq_(
            gen(None, ['straight_join']),
            'SELECT straight_join q')
        eq_(
            gen('all', ['HIGH_PRIORITY SQL_SMALL_RESULT']),
            'SELECT HIGH_PRIORITY SQL_SMALL_RESULT ALL q')
        eq_(
            gen(True, ['high_priority', sql.text('sql_cache')]),
            'SELECT high_priority sql_cache DISTINCT q')

    def test_limit(self):
        t = sql.table('t', sql.column('col1'), sql.column('col2'))

        self.assert_compile(
            select([t]).limit(10).offset(20),
            "SELECT t.col1, t.col2 FROM t  LIMIT 20, 10"
            )
        self.assert_compile(
            select([t]).limit(10),
            "SELECT t.col1, t.col2 FROM t  LIMIT 10")
        self.assert_compile(
            select([t]).offset(10),
            "SELECT t.col1, t.col2 FROM t  LIMIT 10, 18446744073709551615"
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
            (m.MSBigInteger(unsigned=True), "CAST(t.col AS UNSIGNED INTEGER)"),
            (m.MSBit, "t.col"),

            # this is kind of sucky.  thank you default arguments!
            (NUMERIC, "CAST(t.col AS DECIMAL(10, 2))"),
            (DECIMAL, "CAST(t.col AS DECIMAL(10, 2))"),
            (Numeric, "CAST(t.col AS DECIMAL(10, 2))"),
            (m.MSNumeric, "CAST(t.col AS DECIMAL(10, 2))"),
            (m.MSDecimal, "CAST(t.col AS DECIMAL(10, 2))"),

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
            (m.MSDateTime, "CAST(t.col AS DATETIME)"),
            (m.MSDate, "CAST(t.col AS DATE)"),
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

            (Binary, "CAST(t.col AS BINARY)"),
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
            (m.MSBoolean, "t.col"),

            (m.MSEnum, "t.col"),
            (m.MSEnum("'1'", "'2'"), "t.col"),
            (m.MSSet, "t.col"),
            (m.MSSet("'1'", "'2'"), "t.col"),
            ]

        for type_, expected in specs:
            self.assert_compile(cast(t.c.col, type_), expected)

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


class RawReflectionTest(TestBase):
    def setup(self):
        self.dialect = mysql.dialect()
        self.reflector = mysql.MySQLSchemaReflector(
            self.dialect.identifier_preparer)

    def test_key_reflection(self):
        regex = self.reflector._re_key

        assert regex.match('  PRIMARY KEY (`id`),')
        assert regex.match('  PRIMARY KEY USING BTREE (`id`),')
        assert regex.match('  PRIMARY KEY (`id`) USING BTREE,')
        assert regex.match('  PRIMARY KEY (`id`)')
        assert regex.match('  PRIMARY KEY USING BTREE (`id`)')
        assert regex.match('  PRIMARY KEY (`id`) USING BTREE')


class ExecutionTest(TestBase):
    """Various MySQL execution special cases."""

    __only_on__ = 'mysql'

    def test_charset_caching(self):
        engine = engines.testing_engine()

        cx = engine.connect()
        meta = MetaData()

        assert ('mysql', 'charset') not in cx.info
        assert ('mysql', 'force_charset') not in cx.info

        cx.execute(text("SELECT 1")).fetchall()
        assert ('mysql', 'charset') not in cx.info

        meta.reflect(cx)
        assert ('mysql', 'charset') in cx.info

        cx.execute(text("SET @squiznart=123"))
        assert ('mysql', 'charset') in cx.info

        # the charset invalidation is very conservative
        cx.execute(text("SET TIMESTAMP = DEFAULT"))
        assert ('mysql', 'charset') not in cx.info

        cx.info[('mysql', 'force_charset')] = 'latin1'

        assert engine.dialect._detect_charset(cx) == 'latin1'
        assert cx.info[('mysql', 'charset')] == 'latin1'

        del cx.info[('mysql', 'force_charset')]
        del cx.info[('mysql', 'charset')]

        meta.reflect(cx)
        assert ('mysql', 'charset') in cx.info

        # String execution doesn't go through the detector.
        cx.execute("SET TIMESTAMP = DEFAULT")
        assert ('mysql', 'charset') in cx.info


class MatchTest(TestBase, AssertsCompiledSQL):
    __only_on__ = 'mysql'

    @classmethod
    def setup_class(cls):
        global metadata, cattable, matchtable
        metadata = MetaData(testing.db)

        cattable = Table('cattable', metadata,
            Column('id', Integer, primary_key=True),
            Column('description', String(50)),
        )
        matchtable = Table('matchtable', metadata,
            Column('id', Integer, primary_key=True),
            Column('title', String(200)),
            Column('category_id', Integer, ForeignKey('cattable.id')),
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
             'title': 'Programming Matz''s Ruby',
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

    def test_expression(self):
        self.assert_compile(
            matchtable.c.title.match('somstr'),
            "MATCH (matchtable.title) AGAINST (%s IN BOOLEAN MODE)")

    def test_simple_match(self):
        results = (matchtable.select().
                   where(matchtable.c.title.match('python')).
                   order_by(matchtable.c.id).
                   execute().
                   fetchall())
        eq_([2, 5], [r.id for r in results])

    def test_simple_match_with_apostrophe(self):
        results = (matchtable.select().
                   where(matchtable.c.title.match('"Matz''s"')).
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
    return testing.db.dialect.schemagenerator(testing.db.dialect,
        testing.db, None, None).get_column_specification(c)

