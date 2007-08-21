import testbase
import sets
from sqlalchemy import *
from sqlalchemy import sql, exceptions
from sqlalchemy.databases import mysql
from testlib import *


class TypesTest(AssertMixin):
    "Test MySQL column types"

    @testing.supported('mysql')
    def test_basic(self):
        meta1 = MetaData(testbase.db)
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
            Column('enum1', mysql.MSEnum("'black'", "'white'")),
            )
        try:
            table.drop(checkfirst=True)
            table.create()
            meta2 = MetaData(testbase.db)
            t2 = Table('mysql_types', meta2, autoload=True)
            assert isinstance(t2.c.num1.type, mysql.MSInteger)
            assert t2.c.num1.type.unsigned
            assert isinstance(t2.c.text1.type, mysql.MSLongText)
            assert isinstance(t2.c.text2.type, mysql.MSLongText)
            assert isinstance(t2.c.num2.type, mysql.MSBigInteger)
            assert isinstance(t2.c.num3.type, mysql.MSBigInteger)
            assert isinstance(t2.c.num4.type, mysql.MSDouble)
            assert isinstance(t2.c.num5.type, mysql.MSDouble)
            assert isinstance(t2.c.enum1.type, mysql.MSEnum)
            t2.drop()
            t2.create()
        finally:
            meta1.drop_all()

    @testing.supported('mysql')
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
            (mysql.MSDouble, [12], {},
             'DOUBLE(12, 2)'),
            (mysql.MSDouble, [12, 4], {'unsigned':True},
             'DOUBLE(12, 4) UNSIGNED'),
            (mysql.MSDouble, [12, 4], {'zerofill':True},
             'DOUBLE(12, 4) ZEROFILL'),
            (mysql.MSDouble, [12, 4], {'zerofill':True, 'unsigned':True},
             'DOUBLE(12, 4) UNSIGNED ZEROFILL'),

            (mysql.MSFloat, [], {},
             'FLOAT(10)'),
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

        table_args = ['test_mysql_numeric', MetaData(testbase.db)]
        for index, spec in enumerate(columns):
            type_, args, kw, res = spec
            table_args.append(Column('c%s' % index, type_(*args, **kw)))

        numeric_table = Table(*table_args)
        gen = testbase.db.dialect.schemagenerator(testbase.db.dialect, testbase.db, None, None)
        
        for col in numeric_table.c:
            index = int(col.name[1:])
            self.assert_eq(gen.get_column_specification(col),
                           "%s %s" % (col.name, columns[index][3]))

        try:
            numeric_table.create(checkfirst=True)
            assert True
        except:
            raise
        numeric_table.drop()
    
    @testing.supported('mysql')
    @testing.exclude('mysql', '<', (4, 1, 1))
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

            (mysql.MSEnum, ["'foo'", "'bar'"], {'unicode':True},
             '''ENUM('foo','bar') UNICODE''')
           ]

        table_args = ['test_mysql_charset', MetaData(testbase.db)]
        for index, spec in enumerate(columns):
            type_, args, kw, res = spec
            table_args.append(Column('c%s' % index, type_(*args, **kw)))

        charset_table = Table(*table_args)
        gen = testbase.db.dialect.schemagenerator(testbase.db.dialect, testbase.db, None, None)
        
        for col in charset_table.c:
            index = int(col.name[1:])
            self.assert_eq(gen.get_column_specification(col),
                           "%s %s" % (col.name, columns[index][3]))

        try:
            charset_table.create(checkfirst=True)
            assert True
        except:
            raise
        charset_table.drop()

    @testing.supported('mysql')
    @testing.exclude('mysql', '<', (5, 0, 5))
    def test_bit_50(self):
        """Exercise BIT types on 5.0+ (not valid for all engine types)"""
        
        meta = MetaData(testbase.db)
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

        try:
            meta.create_all()

            meta2 = MetaData(testbase.db)
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

    @testing.supported('mysql')
    def test_boolean(self):
        """Test BOOL/TINYINT(1) compatability and reflection."""

        meta = MetaData(testbase.db)
        bool_table = Table('mysql_bool', meta,
                           Column('b1', BOOLEAN),
                           Column('b2', mysql.MSBoolean),
                           Column('b3', mysql.MSTinyInteger(1)),
                           Column('b4', mysql.MSTinyInteger))

        self.assert_eq(colspec(bool_table.c.b1), 'b1 BOOL')
        self.assert_eq(colspec(bool_table.c.b2), 'b2 BOOL')
        self.assert_eq(colspec(bool_table.c.b3), 'b3 TINYINT(1)')
        self.assert_eq(colspec(bool_table.c.b4), 'b4 TINYINT')

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

            meta2 = MetaData(testbase.db)
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

    @testing.supported('mysql')
    @testing.exclude('mysql', '<', (4, 1, 0))
    def test_timestamp(self):
        """Exercise funky TIMESTAMP default syntax."""
    
        meta = MetaData(testbase.db)

        try:
            columns = [
                ([TIMESTAMP],
                 'TIMESTAMP'),
                ([mysql.MSTimeStamp],
                 'TIMESTAMP'),
                ([mysql.MSTimeStamp,
                  PassiveDefault(sql.text('CURRENT_TIMESTAMP'))],
                 "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"),
                ([mysql.MSTimeStamp,
                  PassiveDefault(sql.text("'1999-09-09 09:09:09'"))],
                 "TIMESTAMP DEFAULT '1999-09-09 09:09:09'"),
                ([mysql.MSTimeStamp,
                  PassiveDefault(sql.text("'1999-09-09 09:09:09' "
                                          "ON UPDATE CURRENT_TIMESTAMP"))],
                 "TIMESTAMP DEFAULT '1999-09-09 09:09:09' "
                 "ON UPDATE CURRENT_TIMESTAMP"),
                ([mysql.MSTimeStamp,
                  PassiveDefault(sql.text("CURRENT_TIMESTAMP "
                                          "ON UPDATE CURRENT_TIMESTAMP"))],
                 "TIMESTAMP DEFAULT CURRENT_TIMESTAMP "
                 "ON UPDATE CURRENT_TIMESTAMP"),
                ]
            for idx, (spec, expected) in enumerate(columns):
                t = Table('mysql_ts%s' % idx, meta,
                          Column('id', Integer, primary_key=True),
                          Column('t', *spec))
                self.assert_eq(colspec(t.c.t), "t %s" % expected)
                t.create()
                r = Table('mysql_ts%s' % idx, MetaData(testbase.db),
                          autoload=True)
                if len(spec) > 1:
                    self.assert_(r.c.t is not None)
        finally:
            meta.drop_all()

    @testing.supported('mysql')
    def test_year(self):
        """Exercise YEAR."""

        meta = MetaData(testbase.db)
        year_table = Table('mysql_year', meta,
                           Column('y1', mysql.MSYear),
                           Column('y2', mysql.MSYear),
                           Column('y3', mysql.MSYear),
                           Column('y4', mysql.MSYear(2)),
                           Column('y5', mysql.MSYear(4)))

        try:
            year_table.create()
            reflected = Table('mysql_year', MetaData(testbase.db),
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
        

    @testing.supported('mysql')
    def test_set(self):
        """Exercise the SET type."""

        meta = MetaData(testbase.db)
        set_table = Table('mysql_set', meta,
                          Column('s1', mysql.MSSet("'dq'", "'sq'")),
                          Column('s2', mysql.MSSet("'a'")),
                          Column('s3', mysql.MSSet("'5'", "'7'", "'9'")))

        self.assert_eq(colspec(set_table.c.s1), "s1 SET('dq','sq')")
        self.assert_eq(colspec(set_table.c.s2), "s2 SET('a')")
        self.assert_eq(colspec(set_table.c.s3), "s3 SET('5','7','9')")

        try:
            set_table.create()
            reflected = Table('mysql_set', MetaData(testbase.db),
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
        finally:
            meta.drop_all()

    @testing.supported('mysql')
    def test_enum(self):
        """Exercise the ENUM type."""
        
        db = testbase.db
        enum_table = Table('mysql_enum', MetaData(testbase.db),
            Column('e1', mysql.MSEnum("'a'", "'b'")),
            Column('e2', mysql.MSEnum("'a'", "'b'"),
                   nullable=False),
            Column('e3', mysql.MSEnum("'a'", "'b'", strict=True)),
            Column('e4', mysql.MSEnum("'a'", "'b'", strict=True),
                   nullable=False))

        self.assert_eq(colspec(enum_table.c.e1),
                       "e1 ENUM('a','b')")
        self.assert_eq(colspec(enum_table.c.e2),
                       "e2 ENUM('a','b') NOT NULL")
        self.assert_eq(colspec(enum_table.c.e3),
                       "e3 ENUM('a','b')")
        self.assert_eq(colspec(enum_table.c.e4),
                       "e4 ENUM('a','b') NOT NULL")
        enum_table.drop(checkfirst=True)
        enum_table.create()

        try:
            enum_table.insert().execute(e1=None, e2=None, e3=None, e4=None)
            self.assert_(False)
        except exceptions.SQLError:
            self.assert_(True)

        try:
            enum_table.insert().execute(e1='c', e2='c', e3='c', e4='c')
            self.assert_(False)
        except exceptions.InvalidRequestError:
            self.assert_(True)

        enum_table.insert().execute()
        enum_table.insert().execute(e1='a', e2='a', e3='a', e4='a')
        enum_table.insert().execute(e1='b', e2='b', e3='b', e4='b')

        res = enum_table.select().execute().fetchall()

        expected = [(None, 'a', None, 'a'),
                    ('a', 'a', 'a', 'a'),
                    ('b', 'b', 'b', 'b')]

        # This is known to fail with MySQLDB 1.2.2 beta versions
        # which return these as sets.Set(['a']), sets.Set(['b'])
        # (even on Pythons with __builtin__.set)
        if testbase.db.dialect.dbapi.version_info < (1, 2, 2, 'beta', 3) and \
           testbase.db.dialect.dbapi.version_info >= (1, 2, 2):
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

    @testing.supported('mysql')
    @testing.exclude('mysql', '<', (5, 0, 0))
    def test_type_reflection(self):
        # (ask_for, roundtripped_as_if_different)
        specs = [( String(), mysql.MSText(), ),
                 ( String(1), mysql.MSString(1), ),
                 ( String(3), mysql.MSString(3), ),
                 ( mysql.MSChar(1), ),
                 ( mysql.MSChar(3), ),
                 ( NCHAR(2), mysql.MSChar(2), ),
                 ( mysql.MSNChar(2), mysql.MSChar(2), ), # N is CREATE only
                 ( mysql.MSNVarChar(22), mysql.MSString(22), ),
                 ( SmallInteger(), mysql.MSSmallInteger(), ),
                 ( SmallInteger(4), mysql.MSSmallInteger(4), ),
                 ( mysql.MSSmallInteger(), ),
                 ( mysql.MSSmallInteger(4), mysql.MSSmallInteger(4), ),
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
                 ]

        columns = [Column('c%i' % (i + 1), t[0]) for i, t in enumerate(specs)]

        m = MetaData(testbase.db)
        t_table = Table('mysql_types', m, *columns)
        m.drop_all()
        m.create_all()
        
        m2 = MetaData(testbase.db)
        rt = Table('mysql_types', m2, autoload=True)

        expected = [len(c) > 1 and c[1] or c[0] for c in specs]
        for i, reflected in enumerate(rt.c):
            assert isinstance(reflected.type, type(expected[i]))

        m.drop_all()

    @testing.supported('mysql')
    def test_autoincrement(self):
        meta = MetaData(testbase.db)
        try:
            Table('ai_1', meta,
                  Column('int_y', Integer, primary_key=True),
                  Column('int_n', Integer, PassiveDefault('0'),
                         primary_key=True))
            Table('ai_2', meta,
                  Column('int_y', Integer, primary_key=True),
                  Column('int_n', Integer, PassiveDefault('0'),
                         primary_key=True))
            Table('ai_3', meta,
                  Column('int_n', Integer, PassiveDefault('0'),
                         primary_key=True, autoincrement=False),
                  Column('int_y', Integer, primary_key=True))
            Table('ai_4', meta,
                  Column('int_n', Integer, PassiveDefault('0'),
                         primary_key=True, autoincrement=False),
                  Column('int_n2', Integer, PassiveDefault('0'),
                         primary_key=True, autoincrement=False))
            Table('ai_5', meta,
                  Column('int_y', Integer, primary_key=True),
                  Column('int_n', Integer, PassiveDefault('0'),
                         primary_key=True, autoincrement=False))
            Table('ai_6', meta,
                  Column('o1', String(1), PassiveDefault('x'),
                         primary_key=True),
                  Column('int_y', Integer, primary_key=True))
            Table('ai_7', meta,
                  Column('o1', String(1), PassiveDefault('x'),
                         primary_key=True),
                  Column('o2', String(1), PassiveDefault('x'),
                         primary_key=True),
                  Column('int_y', Integer, primary_key=True))
            Table('ai_8', meta,
                  Column('o1', String(1), PassiveDefault('x'),
                         primary_key=True),
                  Column('o2', String(1), PassiveDefault('x'),
                         primary_key=True))
            meta.create_all()

            table_names = ['ai_1', 'ai_2', 'ai_3', 'ai_4',
                           'ai_5', 'ai_6', 'ai_7', 'ai_8']
            mr = MetaData(testbase.db)
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
        self.assertEqual(got, wanted)


class SQLTest(AssertMixin):
    """Tests MySQL-dialect specific compilation."""

    @testing.supported('mysql')
    def test_precolumns(self):
        dialect = testbase.db.dialect

        def gen(distinct=None, prefixes=None):
            kw = {}
            if distinct is not None:
                kw['distinct'] = distinct
            if prefixes is not None:
                kw['prefixes'] = prefixes
            return str(select(['q'], **kw).compile(dialect=dialect))

        self.assertEqual(gen(None), 'SELECT q')
        self.assertEqual(gen(True), 'SELECT DISTINCT q')
        self.assertEqual(gen(1), 'SELECT DISTINCT q')
        self.assertEqual(gen('diSTInct'), 'SELECT DISTINCT q')
        self.assertEqual(gen('DISTINCT'), 'SELECT DISTINCT q')

        # Standard SQL
        self.assertEqual(gen('all'), 'SELECT ALL q')
        self.assertEqual(gen('distinctrow'), 'SELECT DISTINCTROW q')

        # Interaction with MySQL prefix extensions
        self.assertEqual(
            gen(None, ['straight_join']),
            'SELECT straight_join q')
        self.assertEqual(
            gen('all', ['HIGH_PRIORITY SQL_SMALL_RESULT']),
            'SELECT HIGH_PRIORITY SQL_SMALL_RESULT ALL q')
        self.assertEqual(
            gen(True, ['high_priority', sql.text('sql_cache')]),
            'SELECT high_priority sql_cache DISTINCT q')


def colspec(c):
    return testbase.db.dialect.schemagenerator(testbase.db.dialect, 
        testbase.db, None, None).get_column_specification(c)

if __name__ == "__main__":
    testbase.main()
