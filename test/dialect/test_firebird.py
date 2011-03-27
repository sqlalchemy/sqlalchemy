from test.lib.testing import eq_, assert_raises
from sqlalchemy import *
from sqlalchemy.databases import firebird
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.sql import table, column
from test.lib import *


class DomainReflectionTest(fixtures.TestBase, AssertsExecutionResults):
    "Test Firebird domains"

    __only_on__ = 'firebird'

    @classmethod
    def setup_class(cls):
        con = testing.db.connect()
        try:
            con.execute('CREATE DOMAIN int_domain AS INTEGER DEFAULT '
                        '42 NOT NULL')
            con.execute('CREATE DOMAIN str_domain AS VARCHAR(255)')
            con.execute('CREATE DOMAIN rem_domain AS BLOB SUB_TYPE TEXT'
                        )
            con.execute('CREATE DOMAIN img_domain AS BLOB SUB_TYPE '
                        'BINARY')
        except ProgrammingError, e:
            if not 'attempt to store duplicate value' in str(e):
                raise e
        con.execute('''CREATE GENERATOR gen_testtable_id''')
        con.execute('''CREATE TABLE testtable (question int_domain,
                                   answer str_domain DEFAULT 'no answer',
                                   remark rem_domain DEFAULT '',
                                   photo img_domain,
                                   d date,
                                   t time,
                                   dt timestamp,
                                   redundant str_domain DEFAULT NULL)''')
        con.execute("ALTER TABLE testtable "
                    "ADD CONSTRAINT testtable_pk PRIMARY KEY "
                    "(question)")
        con.execute("CREATE TRIGGER testtable_autoid FOR testtable "
                    "   ACTIVE BEFORE INSERT AS"
                    "   BEGIN"
                    "     IF (NEW.question IS NULL) THEN"
                    "       NEW.question = gen_id(gen_testtable_id, 1);"
                    "   END")

    @classmethod
    def teardown_class(cls):
        con = testing.db.connect()
        con.execute('DROP TABLE testtable')
        con.execute('DROP DOMAIN int_domain')
        con.execute('DROP DOMAIN str_domain')
        con.execute('DROP DOMAIN rem_domain')
        con.execute('DROP DOMAIN img_domain')
        con.execute('DROP GENERATOR gen_testtable_id')

    def test_table_is_reflected(self):
        from sqlalchemy.types import Integer, Text, BLOB, String, Date, \
            Time, DateTime
        metadata = MetaData(testing.db)
        table = Table('testtable', metadata, autoload=True)
        eq_(set(table.columns.keys()), set([
            'question',
            'answer',
            'remark',
            'photo',
            'd',
            't',
            'dt',
            'redundant',
            ]),
                "Columns of reflected table didn't equal expected "
                "columns")
        eq_(table.c.question.primary_key, True)

        # disabled per http://www.sqlalchemy.org/trac/ticket/1660
        # eq_(table.c.question.sequence.name, 'gen_testtable_id')

        assert isinstance(table.c.question.type, Integer)
        eq_(table.c.question.server_default.arg.text, '42')
        assert isinstance(table.c.answer.type, String)
        assert table.c.answer.type.length == 255
        eq_(table.c.answer.server_default.arg.text, "'no answer'")
        assert isinstance(table.c.remark.type, Text)
        eq_(table.c.remark.server_default.arg.text, "''")
        assert isinstance(table.c.photo.type, BLOB)
        assert table.c.redundant.server_default is None

        # The following assume a Dialect 3 database

        assert isinstance(table.c.d.type, Date)
        assert isinstance(table.c.t.type, Time)
        assert isinstance(table.c.dt.type, DateTime)


class BuggyDomainReflectionTest(fixtures.TestBase, AssertsExecutionResults):
    """Test Firebird domains (and some other reflection bumps), 
    see [ticket:1663] and http://tracker.firebirdsql.org/browse/CORE-356"""

    __only_on__ = 'firebird'

    # NB: spacing and newlines are *significant* here!
    # PS: this test is superfluous on recent FB, where the issue 356 is probably fixed...

    AUTOINC_DM = """\
CREATE DOMAIN AUTOINC_DM
AS
NUMERIC(18,0)
"""

    MONEY_DM = """\
CREATE DOMAIN MONEY_DM
AS
NUMERIC(15,2)
DEFAULT 0
CHECK (VALUE BETWEEN -
9999999999999.99 AND +9999999999999.99)
"""

    NOSI_DM = """\
CREATE DOMAIN
NOSI_DM AS
CHAR(1)
DEFAULT 'N'
NOT NULL
CHECK (VALUE IN
('S', 'N'))
"""

    RIT_TESORERIA_CAPITOLO_DM = """\
CREATE DOMAIN RIT_TESORERIA_CAPITOLO_DM
AS
VARCHAR(6)
CHECK ((VALUE IS NULL) OR (VALUE =
UPPER(VALUE)))
"""

    DEF_ERROR_TB = """\
CREATE TABLE DEF_ERROR (
RITENUTAMOV_ID AUTOINC_DM
NOT NULL,
RITENUTA MONEY_DM,
INTERESSI MONEY_DM
DEFAULT
0,
STAMPATO_MODULO NOSI_DM DEFAULT 'S',
TESORERIA_CAPITOLO
RIT_TESORERIA_CAPITOLO_DM)
"""

    DEF_ERROR_NODOM_TB = """\
CREATE TABLE
DEF_ERROR_NODOM (
RITENUTAMOV_ID INTEGER NOT NULL,
RITENUTA NUMERIC(15,2) DEFAULT 0,
INTERESSI NUMERIC(15,2)
DEFAULT
0,
STAMPATO_MODULO CHAR(1) DEFAULT 'S',
TESORERIA_CAPITOLO
CHAR(1))
"""

    DOM_ID = """
CREATE DOMAIN DOM_ID INTEGER NOT NULL
"""

    TABLE_A = """\
CREATE TABLE A (
ID DOM_ID /* INTEGER NOT NULL */ DEFAULT 0 )
"""

    # the 'default' keyword is lower case here
    TABLE_B = """\
CREATE TABLE B (
ID DOM_ID /* INTEGER NOT NULL */ default 0 )
"""

    @classmethod
    def setup_class(cls):
        con = testing.db.connect()
        con.execute(cls.AUTOINC_DM)
        con.execute(cls.MONEY_DM)
        con.execute(cls.NOSI_DM)
        con.execute(cls.RIT_TESORERIA_CAPITOLO_DM)
        con.execute(cls.DEF_ERROR_TB)
        con.execute(cls.DEF_ERROR_NODOM_TB)

        con.execute(cls.DOM_ID)
        con.execute(cls.TABLE_A)
        con.execute(cls.TABLE_B)

    @classmethod
    def teardown_class(cls):
        con = testing.db.connect()
        con.execute('DROP TABLE a')
        con.execute("DROP TABLE b")
        con.execute('DROP DOMAIN dom_id')
        con.execute('DROP TABLE def_error_nodom')
        con.execute('DROP TABLE def_error')
        con.execute('DROP DOMAIN rit_tesoreria_capitolo_dm')
        con.execute('DROP DOMAIN nosi_dm')
        con.execute('DROP DOMAIN money_dm')
        con.execute('DROP DOMAIN autoinc_dm')

    def test_tables_are_reflected_same_way(self):
        metadata = MetaData(testing.db)

        table_dom = Table('def_error', metadata, autoload=True)
        table_nodom = Table('def_error_nodom', metadata, autoload=True)

        eq_(table_dom.c.interessi.server_default.arg.text,
            table_nodom.c.interessi.server_default.arg.text)
        eq_(table_dom.c.ritenuta.server_default.arg.text,
            table_nodom.c.ritenuta.server_default.arg.text)
        eq_(table_dom.c.stampato_modulo.server_default.arg.text,
            table_nodom.c.stampato_modulo.server_default.arg.text)

    def test_intermixed_comment(self):
        metadata = MetaData(testing.db)

        table_a = Table('a', metadata, autoload=True)

        eq_(table_a.c.id.server_default.arg.text, "0")

    def test_lowercase_default_name(self):
        metadata = MetaData(testing.db)

        table_b = Table('b', metadata, autoload=True)

        eq_(table_b.c.id.server_default.arg.text, "0")


class CompileTest(fixtures.TestBase, AssertsCompiledSQL):

    __dialect__ = firebird.FBDialect()

    def test_alias(self):
        t = table('sometable', column('col1'), column('col2'))
        s = select([t.alias()])
        self.assert_compile(s,
                            'SELECT sometable_1.col1, sometable_1.col2 '
                            'FROM sometable AS sometable_1')
        dialect = firebird.FBDialect()
        dialect._version_two = False
        self.assert_compile(s,
                            'SELECT sometable_1.col1, sometable_1.col2 '
                            'FROM sometable sometable_1',
                            dialect=dialect)

    def test_function(self):
        self.assert_compile(func.foo(1, 2), 'foo(:foo_1, :foo_2)')
        self.assert_compile(func.current_time(), 'CURRENT_TIME')
        self.assert_compile(func.foo(), 'foo')
        m = MetaData()
        t = Table('sometable', m, Column('col1', Integer), Column('col2'
                  , Integer))
        self.assert_compile(select([func.max(t.c.col1)]),
                            'SELECT max(sometable.col1) AS max_1 FROM '
                            'sometable')

    def test_substring(self):
        self.assert_compile(func.substring('abc', 1, 2),
                            'SUBSTRING(:substring_1 FROM :substring_2 '
                            'FOR :substring_3)')
        self.assert_compile(func.substring('abc', 1),
                            'SUBSTRING(:substring_1 FROM :substring_2)')

    def test_update_returning(self):
        table1 = table('mytable', column('myid', Integer), column('name'
                       , String(128)), column('description',
                       String(128)))
        u = update(table1, values=dict(name='foo'
                   )).returning(table1.c.myid, table1.c.name)
        self.assert_compile(u,
                            'UPDATE mytable SET name=:name RETURNING '
                            'mytable.myid, mytable.name')
        u = update(table1, values=dict(name='foo')).returning(table1)
        self.assert_compile(u,
                            'UPDATE mytable SET name=:name RETURNING '
                            'mytable.myid, mytable.name, '
                            'mytable.description')
        u = update(table1, values=dict(name='foo'
                   )).returning(func.length(table1.c.name))
        self.assert_compile(u,
                            'UPDATE mytable SET name=:name RETURNING '
                            'char_length(mytable.name) AS length_1')

    def test_insert_returning(self):
        table1 = table('mytable', column('myid', Integer), column('name'
                       , String(128)), column('description',
                       String(128)))
        i = insert(table1, values=dict(name='foo'
                   )).returning(table1.c.myid, table1.c.name)
        self.assert_compile(i,
                            'INSERT INTO mytable (name) VALUES (:name) '
                            'RETURNING mytable.myid, mytable.name')
        i = insert(table1, values=dict(name='foo')).returning(table1)
        self.assert_compile(i,
                            'INSERT INTO mytable (name) VALUES (:name) '
                            'RETURNING mytable.myid, mytable.name, '
                            'mytable.description')
        i = insert(table1, values=dict(name='foo'
                   )).returning(func.length(table1.c.name))
        self.assert_compile(i,
                            'INSERT INTO mytable (name) VALUES (:name) '
                            'RETURNING char_length(mytable.name) AS '
                            'length_1')

    def test_charset(self):
        """Exercise CHARACTER SET  options on string types."""

        columns = [(firebird.CHAR, [1], {}, 'CHAR(1)'), (firebird.CHAR,
                   [1], {'charset': 'OCTETS'},
                   'CHAR(1) CHARACTER SET OCTETS'), (firebird.VARCHAR,
                   [1], {}, 'VARCHAR(1)'), (firebird.VARCHAR, [1],
                   {'charset': 'OCTETS'},
                   'VARCHAR(1) CHARACTER SET OCTETS')]
        for type_, args, kw, res in columns:
            self.assert_compile(type_(*args, **kw), res)

class TypesTest(fixtures.TestBase):
    __only_on__ = 'firebird'

    @testing.provide_metadata
    def test_infinite_float(self):
        metadata = self.metadata
        t = Table('t', metadata, 
            Column('data', Float)
        )
        metadata.create_all()
        t.insert().execute(data=float('inf'))
        eq_(t.select().execute().fetchall(),
            [(float('inf'),)]
        )

class MiscTest(fixtures.TestBase):

    __only_on__ = 'firebird'

    @testing.provide_metadata
    def test_strlen(self):
        metadata = self.metadata

        # On FB the length() function is implemented by an external UDF,
        # strlen().  Various SA tests fail because they pass a parameter
        # to it, and that does not work (it always results the maximum
        # string length the UDF was declared to accept). This test
        # checks that at least it works ok in other cases.

        t = Table('t1', metadata, Column('id', Integer,
                  Sequence('t1idseq'), primary_key=True), Column('name'
                  , String(10)))
        metadata.create_all()
        t.insert(values=dict(name='dante')).execute()
        t.insert(values=dict(name='alighieri')).execute()
        select([func.count(t.c.id)], func.length(t.c.name)
               == 5).execute().first()[0] == 1

    def test_version_parsing(self):
        for string, result in [
            ("WI-V1.5.0.1234 Firebird 1.5", (1, 5, 1234, 'firebird')),
            ("UI-V6.3.2.18118 Firebird 2.1", (2, 1, 18118, 'firebird')),
            ("LI-V6.3.3.12981 Firebird 2.0", (2, 0, 12981, 'firebird')),
            ("WI-V8.1.1.333", (8, 1, 1, 'interbase')),
            ("WI-V8.1.1.333 Firebird 1.5", (1, 5, 333, 'firebird')),
        ]:
            eq_(
                testing.db.dialect._parse_version_info(string),
                result
            )

    @testing.provide_metadata
    def test_rowcount_flag(self):
        metadata = self.metadata
        engine = engines.testing_engine(options={'enable_rowcount'
                : True})
        assert engine.dialect.supports_sane_rowcount
        metadata.bind = engine
        t = Table('t1', metadata, Column('data', String(10)))
        metadata.create_all()
        r = t.insert().execute({'data': 'd1'}, {'data': 'd2'}, {'data'
                               : 'd3'})
        r = t.update().where(t.c.data == 'd2').values(data='d3'
                ).execute()
        eq_(r.rowcount, 1)
        r = t.delete().where(t.c.data == 'd3').execute()
        eq_(r.rowcount, 2)
        r = \
            t.delete().execution_options(enable_rowcount=False).execute()
        eq_(r.rowcount, -1)
        engine = engines.testing_engine(options={'enable_rowcount'
                : False})
        assert not engine.dialect.supports_sane_rowcount
        metadata.bind = engine
        r = t.insert().execute({'data': 'd1'}, {'data': 'd2'}, {'data'
                               : 'd3'})
        r = t.update().where(t.c.data == 'd2').values(data='d3'
                ).execute()
        eq_(r.rowcount, -1)
        r = t.delete().where(t.c.data == 'd3').execute()
        eq_(r.rowcount, -1)
        r = t.delete().execution_options(enable_rowcount=True).execute()
        eq_(r.rowcount, 1)

    def test_percents_in_text(self):
        for expr, result in (text("select '%' from rdb$database"), '%'
                             ), (text("select '%%' from rdb$database"),
                                 '%%'), \
            (text("select '%%%' from rdb$database"), '%%%'), \
            (text("select 'hello % world' from rdb$database"),
             'hello % world'):
            eq_(testing.db.scalar(expr), result)
