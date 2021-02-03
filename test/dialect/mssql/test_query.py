# -*- encoding: utf-8
from sqlalchemy import and_
from sqlalchemy import Column
from sqlalchemy import DDL
from sqlalchemy import desc
from sqlalchemy import event
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import Identity
from sqlalchemy import Integer
from sqlalchemy import literal
from sqlalchemy import or_
from sqlalchemy import PrimaryKeyConstraint
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import testing
from sqlalchemy import util
from sqlalchemy.dialects.mssql import base as mssql
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import engines
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.assertsql import CursorSQL
from sqlalchemy.testing.assertsql import DialectSQL
from sqlalchemy.util import ue


class IdentityInsertTest(fixtures.TablesTest, AssertsCompiledSQL):
    __only_on__ = "mssql"
    __dialect__ = mssql.MSDialect()
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "cattable",
            metadata,
            Column("id", Integer),
            Column("description", String(50)),
            PrimaryKeyConstraint("id", name="PK_cattable"),
        )

    def test_compiled(self):
        cattable = self.tables.cattable
        self.assert_compile(
            cattable.insert().values(id=9, description="Python"),
            "INSERT INTO cattable (id, description) "
            "VALUES (:id, :description)",
        )

    def test_execute(self, connection):
        conn = connection
        cattable = self.tables.cattable
        conn.execute(cattable.insert().values(id=9, description="Python"))

        cats = conn.execute(cattable.select().order_by(cattable.c.id))
        eq_([(9, "Python")], list(cats))

        result = conn.execute(cattable.insert().values(description="PHP"))
        eq_(result.inserted_primary_key, (10,))
        lastcat = conn.execute(cattable.select().order_by(desc(cattable.c.id)))
        eq_((10, "PHP"), lastcat.first())

    def test_executemany(self, connection):
        conn = connection
        cattable = self.tables.cattable
        conn.execute(
            cattable.insert(),
            [
                {"id": 89, "description": "Python"},
                {"id": 8, "description": "Ruby"},
                {"id": 3, "description": "Perl"},
                {"id": 1, "description": "Java"},
            ],
        )
        cats = conn.execute(cattable.select().order_by(cattable.c.id))
        eq_(
            [(1, "Java"), (3, "Perl"), (8, "Ruby"), (89, "Python")],
            list(cats),
        )
        conn.execute(
            cattable.insert(),
            [{"description": "PHP"}, {"description": "Smalltalk"}],
        )
        lastcats = conn.execute(
            cattable.select().order_by(desc(cattable.c.id)).limit(2)
        )
        eq_([(91, "Smalltalk"), (90, "PHP")], list(lastcats))

    def test_insert_plain_param(self, connection):
        conn = connection
        cattable = self.tables.cattable
        conn.execute(cattable.insert(), dict(id=5))
        eq_(conn.scalar(select(cattable.c.id)), 5)

    def test_insert_values_key_plain(self, connection):
        conn = connection
        cattable = self.tables.cattable
        conn.execute(cattable.insert().values(id=5))
        eq_(conn.scalar(select(cattable.c.id)), 5)

    def test_insert_values_key_expression(self, connection):
        conn = connection
        cattable = self.tables.cattable
        conn.execute(cattable.insert().values(id=literal(5)))
        eq_(conn.scalar(select(cattable.c.id)), 5)

    def test_insert_values_col_plain(self, connection):
        conn = connection
        cattable = self.tables.cattable
        conn.execute(cattable.insert().values({cattable.c.id: 5}))
        eq_(conn.scalar(select(cattable.c.id)), 5)

    def test_insert_values_col_expression(self, connection):
        conn = connection
        cattable = self.tables.cattable
        conn.execute(cattable.insert().values({cattable.c.id: literal(5)}))
        eq_(conn.scalar(select(cattable.c.id)), 5)


class QueryUnicodeTest(fixtures.TestBase):

    __only_on__ = "mssql"
    __backend__ = True

    @testing.requires.mssql_freetds
    @testing.requires.python2
    @testing.provide_metadata
    def test_convert_unicode(self, connection):
        meta = self.metadata
        t1 = Table(
            "unitest_table",
            meta,
            Column("id", Integer, primary_key=True),
            Column("descr", mssql.MSText()),
        )
        meta.create_all(connection)
        connection.execute(
            ue("insert into unitest_table values ('abc \xc3\xa9 def')").encode(
                "UTF-8"
            )
        )
        r = connection.execute(t1.select()).first()
        assert isinstance(
            r[1], util.text_type
        ), "%s is %s instead of unicode, working on %s" % (
            r[1],
            type(r[1]),
            meta.bind,
        )
        eq_(r[1], util.ue("abc \xc3\xa9 def"))


class QueryTest(testing.AssertsExecutionResults, fixtures.TestBase):
    __only_on__ = "mssql"
    __backend__ = True

    def test_fetchid_trigger(self, metadata, connection):
        # TODO: investigate test hang on mssql when connection fixture is used
        """
        Verify identity return value on inserting to a trigger table.

        MSSQL's OUTPUT INSERTED clause does not work for the
        case of a table having an identity (autoincrement)
        primary key column, and which also has a trigger configured
        to fire upon each insert and subsequently perform an
        insert into a different table.

        SQLALchemy's MSSQL dialect by default will attempt to
        use an OUTPUT_INSERTED clause, which in this case will
        raise the following error:

        ProgrammingError: (ProgrammingError) ('42000', 334,
        "[Microsoft][SQL Server Native Client 10.0][SQL Server]The
        target table 't1' of the DML statement cannot have any enabled
        triggers if the statement contains an OUTPUT clause without
        INTO clause.", 7748) 'INSERT INTO t1 (descr) OUTPUT inserted.id
        VALUES (?)' ('hello',)

        This test verifies a workaround, which is to rely on the
        older SCOPE_IDENTITY() call, which still works for this scenario.
        To enable the workaround, the Table must be instantiated
        with the init parameter 'implicit_returning = False'.
        """

        # TODO: this same test needs to be tried in a multithreaded context
        #      with multiple threads inserting to the same table.
        # TODO: check whether this error also occurs with clients other
        #      than the SQL Server Native Client. Maybe an assert_raises
        #      test should be written.
        meta = metadata
        t1 = Table(
            "t1",
            meta,
            Column("id", Integer, Identity(start=100), primary_key=True),
            Column("descr", String(200)),
            # the following flag will prevent the
            # MSSQLCompiler.returning_clause from getting called,
            # though the ExecutionContext will still have a
            # _select_lastrowid, so the SELECT SCOPE_IDENTITY() will
            # hopefully be called instead.
            implicit_returning=False,
        )
        t2 = Table(
            "t2",
            meta,
            Column("id", Integer, Identity(start=200), primary_key=True),
            Column("descr", String(200)),
        )

        event.listen(
            meta,
            "after_create",
            DDL(
                "create trigger paj on t1 for insert as "
                "insert into t2 (descr) select descr from inserted"
            ),
        )

        # this DROP is not actually needed since SQL Server transactional
        # DDL is reverting it with the connection fixture.  however,
        # since we can use "if exists" it's safe to have this here in
        # case things change.
        event.listen(
            meta, "before_drop", DDL("""drop trigger if exists paj""")
        )

        # seems to work with all linux drivers + backend.  not sure
        # if windows drivers / servers have different behavior here.
        meta.create_all(connection)
        r = connection.execute(t2.insert(), dict(descr="hello"))
        eq_(r.inserted_primary_key, (200,))
        r = connection.execute(t1.insert(), dict(descr="hello"))
        eq_(r.inserted_primary_key, (100,))

    @testing.provide_metadata
    def _test_disable_scope_identity(self):
        engine = engines.testing_engine(options={"use_scope_identity": False})
        metadata = self.metadata
        t1 = Table(
            "t1",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("data", String(50)),
            implicit_returning=False,
        )
        metadata.create_all(engine)

        with self.sql_execution_asserter(engine) as asserter:
            with engine.begin() as conn:
                conn.execute(t1.insert(), {"data": "somedata"})

        # TODO: need a dialect SQL that acts like Cursor SQL
        asserter.assert_(
            DialectSQL(
                "INSERT INTO t1 (data) VALUES (:data)", {"data": "somedata"}
            ),
            CursorSQL(
                "SELECT @@identity AS lastrowid", consume_statement=False
            ),
        )

    @testing.provide_metadata
    def test_enable_scope_identity(self):
        engine = engines.testing_engine(options={"use_scope_identity": True})
        metadata = self.metadata
        t1 = Table(
            "t1",
            metadata,
            Column("id", Integer, primary_key=True),
            implicit_returning=False,
        )
        metadata.create_all(engine)

        with self.sql_execution_asserter(engine) as asserter:
            with engine.begin() as conn:
                conn.execute(t1.insert())

        # even with pyodbc, we don't embed the scope identity on a
        # DEFAULT VALUES insert
        asserter.assert_(
            CursorSQL(
                "INSERT INTO t1 DEFAULT VALUES", consume_statement=False
            ),
            CursorSQL(
                "SELECT scope_identity() AS lastrowid", consume_statement=False
            ),
        )

    @testing.only_on("mssql+pyodbc")
    @testing.provide_metadata
    def test_embedded_scope_identity(self):
        engine = engines.testing_engine(options={"use_scope_identity": True})
        metadata = self.metadata
        t1 = Table(
            "t1",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("data", String(50)),
            implicit_returning=False,
        )
        metadata.create_all(engine)

        with self.sql_execution_asserter(engine) as asserter:
            with engine.begin() as conn:
                conn.execute(t1.insert(), {"data": "somedata"})

        # pyodbc-specific system
        asserter.assert_(
            CursorSQL(
                "INSERT INTO t1 (data) VALUES (?); select scope_identity()",
                ("somedata",),
                consume_statement=False,
            )
        )

    @testing.provide_metadata
    def test_insertid_schema(self, connection):
        meta = self.metadata

        tbl = Table(
            "test",
            meta,
            Column("id", Integer, primary_key=True),
            schema=testing.config.test_schema,
        )
        tbl.create(connection)
        connection.execute(tbl.insert(), {"id": 1})
        eq_(connection.scalar(tbl.select()), 1)

    @testing.provide_metadata
    def test_returning_no_autoinc(self, connection):
        meta = self.metadata
        table = Table(
            "t1",
            meta,
            Column("id", Integer, primary_key=True),
            Column("data", String(50)),
        )
        table.create(connection)
        result = connection.execute(
            table.insert()
            .values(id=1, data=func.lower("SomeString"))
            .returning(table.c.id, table.c.data)
        )
        eq_(result.fetchall(), [(1, "somestring")])

    @testing.provide_metadata
    def test_delete_schema(self, connection):
        meta = self.metadata

        tbl = Table(
            "test",
            meta,
            Column("id", Integer, primary_key=True),
            schema=testing.config.test_schema,
        )
        tbl.create(connection)
        connection.execute(tbl.insert(), {"id": 1})
        eq_(connection.scalar(tbl.select()), 1)
        connection.execute(tbl.delete(tbl.c.id == 1))
        eq_(connection.scalar(tbl.select()), None)

    @testing.provide_metadata
    def test_insertid_reserved(self, connection):
        meta = self.metadata
        table = Table("select", meta, Column("col", Integer, primary_key=True))
        table.create(connection)

        connection.execute(table.insert(), {"col": 7})
        eq_(connection.scalar(table.select()), 7)


class Foo(object):
    def __init__(self, **kw):
        for k in kw:
            setattr(self, k, kw[k])


def full_text_search_missing():
    """Test if full text search is not implemented and return False if
    it is and True otherwise."""

    if not testing.against("mssql"):
        return True

    with testing.db.connect() as conn:
        result = conn.exec_driver_sql(
            "SELECT cast(SERVERPROPERTY('IsFullTextInstalled') as integer)"
        )
        return result.scalar() == 0


class MatchTest(fixtures.TablesTest, AssertsCompiledSQL):

    __only_on__ = "mssql"
    __skip_if__ = (full_text_search_missing,)
    __backend__ = True

    run_setup_tables = "once"
    run_inserts = run_deletes = "once"

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "cattable",
            metadata,
            Column("id", Integer),
            Column("description", String(50)),
            PrimaryKeyConstraint("id", name="PK_cattable"),
        )
        Table(
            "matchtable",
            metadata,
            Column("id", Integer),
            Column("title", String(200)),
            Column("category_id", Integer, ForeignKey("cattable.id")),
            PrimaryKeyConstraint("id", name="PK_matchtable"),
        )

        event.listen(
            metadata,
            "before_create",
            DDL("CREATE FULLTEXT CATALOG Catalog AS DEFAULT"),
        )
        event.listen(
            metadata,
            "after_create",
            DDL(
                """CREATE FULLTEXT INDEX
                       ON cattable (description)
                       KEY INDEX PK_cattable"""
            ),
        )
        event.listen(
            metadata,
            "after_create",
            DDL(
                """CREATE FULLTEXT INDEX
                       ON matchtable (title)
                       KEY INDEX PK_matchtable"""
            ),
        )

        event.listen(
            metadata,
            "after_drop",
            DDL("DROP FULLTEXT CATALOG Catalog"),
        )

    @classmethod
    def setup_bind(cls):
        return testing.db.execution_options(isolation_level="AUTOCOMMIT")

    @classmethod
    def setup_test_class(cls):
        with testing.db.connect().execution_options(
            isolation_level="AUTOCOMMIT"
        ) as conn:
            try:
                conn.exec_driver_sql("DROP FULLTEXT CATALOG Catalog")
            except:
                pass

    @classmethod
    def insert_data(cls, connection):
        cattable, matchtable = cls.tables("cattable", "matchtable")

        connection.execute(
            cattable.insert(),
            [
                {"id": 1, "description": "Python"},
                {"id": 2, "description": "Ruby"},
            ],
        )
        connection.execute(
            matchtable.insert(),
            [
                {
                    "id": 1,
                    "title": "Web Development with Rails",
                    "category_id": 2,
                },
                {"id": 2, "title": "Dive Into Python", "category_id": 1},
                {
                    "id": 3,
                    "title": "Programming Matz's Ruby",
                    "category_id": 2,
                },
                {"id": 4, "title": "Guide to Django", "category_id": 1},
                {"id": 5, "title": "Python in a Nutshell", "category_id": 1},
            ],
        )
        # apparently this is needed!   index must run asynchronously
        connection.execute(DDL("WAITFOR DELAY '00:00:05'"))

    def test_expression(self):
        matchtable = self.tables.matchtable
        self.assert_compile(
            matchtable.c.title.match("somstr"),
            "CONTAINS (matchtable.title, ?)",
        )

    def test_simple_match(self, connection):
        matchtable = self.tables.matchtable
        results = connection.execute(
            matchtable.select()
            .where(matchtable.c.title.match("python"))
            .order_by(matchtable.c.id)
        ).fetchall()
        eq_([2, 5], [r.id for r in results])

    def test_simple_match_with_apostrophe(self, connection):
        matchtable = self.tables.matchtable
        results = connection.execute(
            matchtable.select().where(matchtable.c.title.match("Matz's"))
        ).fetchall()
        eq_([3], [r.id for r in results])

    def test_simple_prefix_match(self, connection):
        matchtable = self.tables.matchtable
        results = connection.execute(
            matchtable.select().where(matchtable.c.title.match('"nut*"'))
        ).fetchall()
        eq_([5], [r.id for r in results])

    def test_simple_inflectional_match(self, connection):
        matchtable = self.tables.matchtable
        results = connection.execute(
            matchtable.select().where(
                matchtable.c.title.match('FORMSOF(INFLECTIONAL, "dives")')
            )
        ).fetchall()
        eq_([2], [r.id for r in results])

    def test_or_match(self, connection):
        matchtable = self.tables.matchtable
        results1 = connection.execute(
            matchtable.select()
            .where(
                or_(
                    matchtable.c.title.match("nutshell"),
                    matchtable.c.title.match("ruby"),
                )
            )
            .order_by(matchtable.c.id)
        ).fetchall()
        eq_([3, 5], [r.id for r in results1])
        results2 = connection.execute(
            matchtable.select()
            .where(matchtable.c.title.match("nutshell OR ruby"))
            .order_by(matchtable.c.id)
        ).fetchall()
        eq_([3, 5], [r.id for r in results2])

    def test_and_match(self, connection):
        matchtable = self.tables.matchtable
        results1 = connection.execute(
            matchtable.select().where(
                and_(
                    matchtable.c.title.match("python"),
                    matchtable.c.title.match("nutshell"),
                )
            )
        ).fetchall()
        eq_([5], [r.id for r in results1])
        results2 = connection.execute(
            matchtable.select().where(
                matchtable.c.title.match("python AND nutshell")
            )
        ).fetchall()
        eq_([5], [r.id for r in results2])

    def test_match_across_joins(self, connection):
        matchtable = self.tables.matchtable
        cattable = self.tables.cattable
        results = connection.execute(
            matchtable.select()
            .where(
                and_(
                    cattable.c.id == matchtable.c.category_id,
                    or_(
                        cattable.c.description.match("Ruby"),
                        matchtable.c.title.match("nutshell"),
                    ),
                )
            )
            .order_by(matchtable.c.id)
        ).fetchall()
        eq_([1, 3, 5], [r.id for r in results])


class TableValuedTest(fixtures.TestBase):
    __backend__ = True
    __only_on__ = "mssql"

    @testing.fixture
    def scalar_strings(self, connection):
        connection.exec_driver_sql(
            """

CREATE FUNCTION scalar_strings (
)
RETURNS TABLE
AS
RETURN
    SELECT
        my_string
    FROM (
        VALUES ('some string'), ('some string'), ('some string')
    ) AS my_tab(my_string)
        """
        )
        yield
        connection.exec_driver_sql("DROP FUNCTION scalar_strings")

    @testing.fixture
    def two_strings(self, connection):
        connection.exec_driver_sql(
            """
CREATE FUNCTION three_pairs (
)
RETURNS TABLE
AS
RETURN
    SELECT
        s1 AS string1, s2 AS string2
    FROM (
        VALUES ('a', 'b'), ('c', 'd'), ('e', 'f')
    ) AS my_tab(s1, s2)
"""
        )
        yield
        connection.exec_driver_sql("DROP FUNCTION three_pairs")

    def test_scalar_strings_control(self, scalar_strings, connection):
        result = (
            connection.exec_driver_sql(
                "SELECT my_string FROM scalar_strings()"
            )
            .scalars()
            .all()
        )
        eq_(result, ["some string"] * 3)

    def test_scalar_strings_named_control(self, scalar_strings, connection):
        result = (
            connection.exec_driver_sql(
                "SELECT anon_1.my_string " "FROM scalar_strings() AS anon_1"
            )
            .scalars()
            .all()
        )
        eq_(result, ["some string"] * 3)

    def test_scalar_strings(self, scalar_strings, connection):
        fn = func.scalar_strings().table_valued("my_string")
        result = connection.execute(select(fn.c.my_string)).scalars().all()
        eq_(result, ["some string"] * 3)

    def test_two_strings_control(self, two_strings, connection):
        result = connection.exec_driver_sql(
            "SELECT string1, string2 FROM three_pairs ()"
        ).all()
        eq_(result, [("a", "b"), ("c", "d"), ("e", "f")])

    def test_two_strings(self, two_strings, connection):
        fn = func.three_pairs().table_valued("string1", "string2")
        result = connection.execute(select(fn.c.string1, fn.c.string2)).all()
        eq_(result, [("a", "b"), ("c", "d"), ("e", "f")])
