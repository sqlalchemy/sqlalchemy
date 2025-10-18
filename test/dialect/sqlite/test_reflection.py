"""SQLite-specific tests."""

from sqlalchemy import CheckConstraint
from sqlalchemy import Column
from sqlalchemy import DDL
from sqlalchemy import event
from sqlalchemy import exc
from sqlalchemy import Index
from sqlalchemy import inspect
from sqlalchemy import MetaData
from sqlalchemy import PrimaryKeyConstraint
from sqlalchemy import Table
from sqlalchemy import testing
from sqlalchemy import types as sqltypes
from sqlalchemy import UniqueConstraint
from sqlalchemy.dialects.sqlite import base as sqlite
from sqlalchemy.sql.elements import quoted_name
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.testing import is_true
from sqlalchemy.testing import mock
from sqlalchemy.types import Integer
from sqlalchemy.types import String


def exec_sql(engine, sql, *args, **kwargs):
    # TODO: convert all tests to not use this
    with engine.begin() as conn:
        conn.exec_driver_sql(sql, *args, **kwargs)


class ReflectHeadlessFKsTest(fixtures.TestBase):
    __only_on__ = "sqlite"
    __backend__ = True

    def setup_test(self):
        exec_sql(testing.db, "CREATE TABLE a (id INTEGER PRIMARY KEY)")
        # this syntax actually works on other DBs perhaps we'd want to add
        # tests to test_reflection
        exec_sql(
            testing.db, "CREATE TABLE b (id INTEGER PRIMARY KEY REFERENCES a)"
        )

    def teardown_test(self):
        exec_sql(testing.db, "drop table b")
        exec_sql(testing.db, "drop table a")

    def test_reflect_tables_fk_no_colref(self):
        meta = MetaData()
        a = Table("a", meta, autoload_with=testing.db)
        b = Table("b", meta, autoload_with=testing.db)

        assert b.c.id.references(a.c.id)


class KeywordInDatabaseNameTest(fixtures.TestBase):
    __only_on__ = "sqlite"
    __backend__ = True

    @testing.fixture
    def db_fixture(self, connection):
        connection.exec_driver_sql(
            'ATTACH %r AS "default"' % connection.engine.url.database
        )
        connection.exec_driver_sql(
            'CREATE TABLE "default".a (id INTEGER PRIMARY KEY)'
        )
        try:
            yield
        finally:
            connection.exec_driver_sql('drop table "default".a')
            connection.exec_driver_sql('DETACH DATABASE "default"')

    def test_reflect(self, connection, db_fixture):
        meta = MetaData(schema="default")
        meta.reflect(connection)
        assert "default.a" in meta.tables


class ConstraintReflectionTest(fixtures.TestBase):
    __only_on__ = "sqlite"
    __backend__ = True

    @classmethod
    def setup_test_class(cls):
        with testing.db.begin() as conn:
            conn.exec_driver_sql("CREATE TABLE a1 (id INTEGER PRIMARY KEY)")
            conn.exec_driver_sql("CREATE TABLE a2 (id INTEGER PRIMARY KEY)")
            conn.exec_driver_sql(
                "CREATE TABLE b (id INTEGER PRIMARY KEY, "
                "FOREIGN KEY(id) REFERENCES a1(id),"
                "FOREIGN KEY(id) REFERENCES a2(id)"
                ")"
            )
            conn.exec_driver_sql(
                "CREATE TABLE c (id INTEGER, "
                "CONSTRAINT bar PRIMARY KEY(id),"
                "CONSTRAINT foo1 FOREIGN KEY(id) REFERENCES a1(id),"
                "CONSTRAINT foo2 FOREIGN KEY(id) REFERENCES a2(id)"
                ")"
            )
            conn.exec_driver_sql(
                # the lower casing + inline is intentional here
                "CREATE TABLE d (id INTEGER, x INTEGER unique)"
            )
            conn.exec_driver_sql(
                # the lower casing + inline is intentional here
                "CREATE TABLE d1 "
                '(id INTEGER, "some ( STUPID n,ame" INTEGER unique)'
            )
            conn.exec_driver_sql(
                # the lower casing + inline is intentional here
                'CREATE TABLE d2 ( "some STUPID n,ame" INTEGER unique)'
            )
            conn.exec_driver_sql(
                # the lower casing + inline is intentional here
                'CREATE TABLE d3 ( "some STUPID n,ame" INTEGER NULL unique)'
            )

            conn.exec_driver_sql(
                # lower casing + inline is intentional
                "CREATE TABLE e (id INTEGER, x INTEGER references a2(id))"
            )
            conn.exec_driver_sql(
                'CREATE TABLE e1 (id INTEGER, "some ( STUPID n,ame" INTEGER '
                'references a2   ("some ( STUPID n,ame"))'
            )
            conn.exec_driver_sql(
                "CREATE TABLE e2 (id INTEGER, "
                '"some ( STUPID n,ame" INTEGER NOT NULL  '
                'references a2   ("some ( STUPID n,ame"))'
            )

            conn.exec_driver_sql(
                "CREATE TABLE f (x INTEGER, CONSTRAINT foo_fx UNIQUE(x))"
            )
            conn.exec_driver_sql(
                # intentional broken casing
                "CREATE TABLE h (x INTEGER, COnstraINT foo_hx unIQUE(x))"
            )
            conn.exec_driver_sql(
                "CREATE TABLE i (x INTEGER, y INTEGER, PRIMARY KEY(x, y))"
            )
            conn.exec_driver_sql(
                "CREATE TABLE j (id INTEGER, q INTEGER, p INTEGER, "
                "PRIMARY KEY(id), FOreiGN KEY(q,p) REFERENCes  i(x,y))"
            )
            conn.exec_driver_sql(
                "CREATE TABLE k (id INTEGER, q INTEGER, p INTEGER, "
                "PRIMARY KEY(id), "
                "conSTRAINT my_fk FOreiGN KEY (  q  , p  )   "
                "REFERENCes   i    (  x ,   y ))"
            )

            meta = MetaData()
            Table("l", meta, Column("bar", String, index=True), schema="main")

            Table(
                "m",
                meta,
                Column("id", Integer, primary_key=True),
                Column("x", String(30)),
                UniqueConstraint("x"),
            )

            Table(
                "p",
                meta,
                Column("id", Integer),
                PrimaryKeyConstraint("id", name="pk_name"),
            )

            Table("q", meta, Column("id", Integer), PrimaryKeyConstraint("id"))

            # intentional new line
            Table(
                "r",
                meta,
                Column("id", Integer),
                Column("value", Integer),
                Column("prefix", String),
                CheckConstraint("id > 0"),
                UniqueConstraint("prefix", name="prefix_named"),
                # Constraint definition with newline and tab characters
                CheckConstraint(
                    """((value > 0) AND \n\t(value < 100) AND \n\t
                      (value != 50))""",
                    name="ck_r_value_multiline",
                ),
                UniqueConstraint("value"),
                # Constraint name with special chars and 'check' in the name
                CheckConstraint("value IS NOT NULL", name="^check-r* #\n\t"),
                PrimaryKeyConstraint("id", name="pk_name"),
                # Constraint definition with special characters.
                CheckConstraint("prefix NOT GLOB '*[^-. /#,]*'"),
            )

            meta.create_all(conn)

            # will contain an "autoindex"
            conn.exec_driver_sql(
                "create table o (foo varchar(20) primary key)"
            )
            conn.exec_driver_sql(
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

            conn.exec_driver_sql(
                "CREATE TABLE deferrable_test (id INTEGER PRIMARY KEY, "
                "c1 INTEGER, c2 INTEGER, c3 INTEGER, c4 INTEGER, "
                "CONSTRAINT fk1 FOREIGN KEY (c1) REFERENCES a1(id) "
                "DEFERRABLE,"
                "CONSTRAINT fk2 FOREIGN KEY (c2) REFERENCES a1(id) "
                "NOT DEFERRABLE,"
                "CONSTRAINT fk3 FOREIGN KEY (c3) REFERENCES a2(id) "
                "ON UPDATE CASCADE "
                "DEFERRABLE INITIALLY DEFERRED,"
                "CONSTRAINT fk4 FOREIGN KEY (c4) REFERENCES a2(id) "
                "NOT DEFERRABLE INITIALLY IMMEDIATE)"
            )

            conn.exec_driver_sql(
                "CREATE TABLE cp ("
                "id INTEGER NOT NULL,\n"
                "q INTEGER, \n"
                "p INTEGER, \n"
                "CONSTRAINT cq CHECK (p = 1 OR (p > 2 AND p < 5)),\n"
                "PRIMARY KEY (id)\n"
                ")"
            )

            conn.exec_driver_sql(
                "CREATE TABLE cp_inline (\n"
                "id INTEGER NOT NULL,\n"
                "q INTEGER CHECK (q > 1 AND q < 6), \n"
                "p INTEGER CONSTRAINT cq CHECK (p = 1 OR (p > 2 AND p < 5)),\n"
                "PRIMARY KEY (id)\n"
                ")"
            )

            conn.exec_driver_sql(
                "CREATE TABLE implicit_referred (pk integer primary key)"
            )
            # single col foreign key with no referred column given,
            # must assume primary key of referred table
            conn.exec_driver_sql(
                "CREATE TABLE implicit_referrer "
                "(id integer REFERENCES implicit_referred)"
            )

            conn.exec_driver_sql(
                "CREATE TABLE implicit_referred_comp "
                "(pk1 integer, pk2 integer, primary key (pk1, pk2))"
            )
            # composite foreign key with no referred columns given,
            # must assume primary key of referred table
            conn.exec_driver_sql(
                "CREATE TABLE implicit_referrer_comp "
                "(id1 integer, id2 integer, foreign key(id1, id2) "
                "REFERENCES implicit_referred_comp)"
            )

            # worst case - FK that refers to nonexistent table so we can't
            # get pks.  requires FK pragma is turned off
            conn.exec_driver_sql(
                "CREATE TABLE implicit_referrer_comp_fake "
                "(id1 integer, id2 integer, foreign key(id1, id2) "
                "REFERENCES fake_table)"
            )

            # tables for issue #12924 - table names with CHECK/CONSTRAINT
            conn.exec_driver_sql(
                "CREATE TABLE oneline ( field INTEGER CHECK(field>0))"
            )
            conn.exec_driver_sql(
                "CREATE TABLE oneline_nested ( field INTEGER "
                "CHECK((field>0 and field<22) or (field>99 and field<1010)))"
            )
            conn.exec_driver_sql(
                "CREATE TABLE oneline_2constraints ( pk INTEGER "
                "CONSTRAINT pkname PRIMARY KEY, field INTEGER "
                "CONSTRAINT chname CHECK((field>0 and field<22) or "
                "(field>99 and field<1010)))"
            )
            conn.exec_driver_sql(
                "CREATE TABLE oneline_nameCHECK ( pk INTEGER "
                "CONSTRAINT pkname PRIMARY KEY )"
            )
            conn.exec_driver_sql(
                "CREATE TABLE oneline_nameCONSTRAINT "
                "( field INTEGER CHECK (field IN (1, 0, -1)) )"
            )
            conn.exec_driver_sql(
                "CREATE TABLE twochecks_oneline (\n"
                "field INTEGER,\n"
                "CHECK (field>1), CHECK(  field<9)\n"
                ")"
            )

            # Test all SQLite quote styles for constraint names
            conn.exec_driver_sql(
                "CREATE TABLE quote_styles ( "
                "field INTEGER, "
                'CONSTRAINT "double_quoted" CHECK (field > 0), '
                "CONSTRAINT 'single_quoted' CHECK (field < 100), "
                "CONSTRAINT [bracket_quoted] CHECK (field != 50), "
                "CONSTRAINT `backtick_quoted` CHECK (field >= 10)"
                ")"
            )

            # Test CHECK constraints with parentheses in string literals
            # These cases have unbalanced parens if we naively count all parens
            conn.exec_driver_sql(
                "CREATE TABLE parens_in_strings ("
                " field TEXT,"
                " CHECK (field != '('),"
                " CHECK (field != ')'),"
                " CHECK (field NOT LIKE '%('),"
                " CHECK (field IN (')', '(', 'test')),"
                # Escaped quotes (SQLite uses '' to escape quotes)
                " CHECK (field != 'it''s (not) valid'),"
                ' CHECK (field != "say ""(hello)"" "),'
                " CHECK (field NOT IN ('()', 'a''b''c', ')')),"
                # Complex nested cases with lots of unbalanced parens in
                # strings
                " CHECK (field != '((' OR field = ')))'),"
                ' CHECK (field LIKE "%))(%" OR field LIKE "%)(%")'
                ")"
            )

    @classmethod
    def teardown_test_class(cls):
        with testing.db.begin() as conn:
            for name in [
                "implicit_referrer_comp_fake",
                "implicit_referrer",
                "implicit_referred",
                "implicit_referrer_comp",
                "implicit_referred_comp",
                "m",
                "main.l",
                "k",
                "j",
                "i",
                "h",
                "f",
                "e",
                "e1",
                "d",
                "d1",
                "d2",
                "c",
                "b",
                "a1",
                "a2",
                "r",
                "oneline",
                "oneline_nested",
                "oneline_2constraints",
                "oneline_nameCHECK",
                "oneline_nameCONSTRAINT",
                "twochecks_oneline",
                "quote_styles",
                "parens_in_strings",
            ]:
                conn.exec_driver_sql("drop table %s" % name)

    @testing.fixture
    def temp_table_fixture(self, connection):
        connection.exec_driver_sql(
            "CREATE TEMPORARY TABLE g "
            "(x INTEGER, CONSTRAINT foo_gx UNIQUE(x))"
        )

        n = Table(
            "n",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column("x", String(30)),
            UniqueConstraint("x"),
            prefixes=["TEMPORARY"],
        )

        n.create(connection)
        try:
            yield
        finally:
            connection.exec_driver_sql("DROP TABLE g")
            n.drop(connection)

    def test_legacy_quoted_identifiers_unit(self):
        dialect = sqlite.dialect()
        dialect._broken_fk_pragma_quotes = True

        for row in [
            (0, None, "target", "tid", "id", None),
            (0, None, '"target"', "tid", "id", None),
            (0, None, "[target]", "tid", "id", None),
            (0, None, "'target'", "tid", "id", None),
            (0, None, "`target`", "tid", "id", None),
        ]:

            def _get_table_pragma(*arg, **kw):
                return [row]

            def _get_table_sql(*arg, **kw):
                return (
                    "CREATE TABLE foo "
                    "(tid INTEGER, "
                    "FOREIGN KEY(tid) REFERENCES %s (id))" % row[2]
                )

            with mock.patch.object(
                dialect, "_get_table_pragma", _get_table_pragma
            ):
                with mock.patch.object(
                    dialect, "_get_table_sql", _get_table_sql
                ):
                    fkeys = dialect.get_foreign_keys(None, "foo")
                    eq_(
                        fkeys,
                        [
                            {
                                "referred_table": "target",
                                "referred_columns": ["id"],
                                "referred_schema": None,
                                "name": None,
                                "constrained_columns": ["tid"],
                                "options": {},
                            }
                        ],
                    )

    def test_foreign_key_name_is_none(self):
        # and not "0"
        inspector = inspect(testing.db)
        fks = inspector.get_foreign_keys("b")
        eq_(
            fks,
            [
                {
                    "referred_table": "a1",
                    "referred_columns": ["id"],
                    "referred_schema": None,
                    "name": None,
                    "constrained_columns": ["id"],
                    "options": {},
                },
                {
                    "referred_table": "a2",
                    "referred_columns": ["id"],
                    "referred_schema": None,
                    "name": None,
                    "constrained_columns": ["id"],
                    "options": {},
                },
            ],
        )

    def test_foreign_key_name_is_not_none(self):
        inspector = inspect(testing.db)
        fks = inspector.get_foreign_keys("c")
        eq_(
            fks,
            [
                {
                    "referred_table": "a1",
                    "referred_columns": ["id"],
                    "referred_schema": None,
                    "name": "foo1",
                    "constrained_columns": ["id"],
                    "options": {},
                },
                {
                    "referred_table": "a2",
                    "referred_columns": ["id"],
                    "referred_schema": None,
                    "name": "foo2",
                    "constrained_columns": ["id"],
                    "options": {},
                },
            ],
        )

    def test_foreign_key_implicit_parent(self):
        inspector = inspect(testing.db)
        fks = inspector.get_foreign_keys("implicit_referrer")
        eq_(
            fks,
            [
                {
                    "name": None,
                    "constrained_columns": ["id"],
                    "referred_schema": None,
                    "referred_table": "implicit_referred",
                    "referred_columns": ["pk"],
                    "options": {},
                }
            ],
        )

    def test_foreign_key_composite_implicit_parent(self):
        inspector = inspect(testing.db)
        fks = inspector.get_foreign_keys("implicit_referrer_comp")
        eq_(
            fks,
            [
                {
                    "name": None,
                    "constrained_columns": ["id1", "id2"],
                    "referred_schema": None,
                    "referred_table": "implicit_referred_comp",
                    "referred_columns": ["pk1", "pk2"],
                    "options": {},
                }
            ],
        )

    def test_foreign_key_implicit_missing_parent(self):
        # test when the FK refers to a non-existent table and column names
        # aren't given.   only sqlite allows this case to exist
        inspector = inspect(testing.db)
        fks = inspector.get_foreign_keys("implicit_referrer_comp_fake")
        # the referred table doesn't exist but the operation does not fail
        eq_(
            fks,
            [
                {
                    "name": None,
                    "constrained_columns": ["id1", "id2"],
                    "referred_schema": None,
                    "referred_table": "fake_table",
                    "referred_columns": [],
                    "options": {},
                }
            ],
        )

    def test_foreign_key_implicit_missing_parent_reflection(self):
        # full Table reflection fails however, which is not a new behavior
        m = MetaData()
        assert_raises_message(
            exc.NoSuchTableError,
            "fake_table",
            Table,
            "implicit_referrer_comp_fake",
            m,
            autoload_with=testing.db,
        )

    def test_unnamed_inline_foreign_key(self):
        inspector = inspect(testing.db)
        fks = inspector.get_foreign_keys("e")
        eq_(
            fks,
            [
                {
                    "referred_table": "a2",
                    "referred_columns": ["id"],
                    "referred_schema": None,
                    "name": None,
                    "constrained_columns": ["x"],
                    "options": {},
                }
            ],
        )

    def test_unnamed_inline_foreign_key_quoted(self):
        inspector = inspect(testing.db)
        fks = inspector.get_foreign_keys("e1")
        eq_(
            fks,
            [
                {
                    "referred_table": "a2",
                    "referred_columns": ["some ( STUPID n,ame"],
                    "referred_schema": None,
                    "options": {},
                    "name": None,
                    "constrained_columns": ["some ( STUPID n,ame"],
                }
            ],
        )
        fks = inspector.get_foreign_keys("e2")
        eq_(
            fks,
            [
                {
                    "referred_table": "a2",
                    "referred_columns": ["some ( STUPID n,ame"],
                    "referred_schema": None,
                    "options": {},
                    "name": None,
                    "constrained_columns": ["some ( STUPID n,ame"],
                }
            ],
        )

    def test_foreign_key_composite_broken_casing(self):
        inspector = inspect(testing.db)
        fks = inspector.get_foreign_keys("j")
        eq_(
            fks,
            [
                {
                    "referred_table": "i",
                    "referred_columns": ["x", "y"],
                    "referred_schema": None,
                    "name": None,
                    "constrained_columns": ["q", "p"],
                    "options": {},
                }
            ],
        )
        fks = inspector.get_foreign_keys("k")
        eq_(
            fks,
            [
                {
                    "referred_table": "i",
                    "referred_columns": ["x", "y"],
                    "referred_schema": None,
                    "name": "my_fk",
                    "constrained_columns": ["q", "p"],
                    "options": {},
                }
            ],
        )

    def test_foreign_key_ondelete_onupdate(self):
        inspector = inspect(testing.db)
        fks = inspector.get_foreign_keys("onud_test")
        eq_(
            fks,
            [
                {
                    "referred_table": "a1",
                    "referred_columns": ["id"],
                    "referred_schema": None,
                    "name": "fk1",
                    "constrained_columns": ["c1"],
                    "options": {"ondelete": "SET NULL"},
                },
                {
                    "referred_table": "a1",
                    "referred_columns": ["id"],
                    "referred_schema": None,
                    "name": "fk2",
                    "constrained_columns": ["c2"],
                    "options": {"onupdate": "CASCADE"},
                },
                {
                    "referred_table": "a2",
                    "referred_columns": ["id"],
                    "referred_schema": None,
                    "name": "fk3",
                    "constrained_columns": ["c3"],
                    "options": {"ondelete": "CASCADE", "onupdate": "SET NULL"},
                },
                {
                    "referred_table": "a2",
                    "referred_columns": ["id"],
                    "referred_schema": None,
                    "name": "fk4",
                    "constrained_columns": ["c4"],
                    "options": {},
                },
            ],
        )

    def test_foreign_key_deferrable_initially(self):
        inspector = inspect(testing.db)
        fks = inspector.get_foreign_keys("deferrable_test")
        eq_(
            fks,
            [
                {
                    "referred_table": "a1",
                    "referred_columns": ["id"],
                    "referred_schema": None,
                    "name": "fk1",
                    "constrained_columns": ["c1"],
                    "options": {"deferrable": True},
                },
                {
                    "referred_table": "a1",
                    "referred_columns": ["id"],
                    "referred_schema": None,
                    "name": "fk2",
                    "constrained_columns": ["c2"],
                    "options": {"deferrable": False},
                },
                {
                    "referred_table": "a2",
                    "referred_columns": ["id"],
                    "referred_schema": None,
                    "name": "fk3",
                    "constrained_columns": ["c3"],
                    "options": {
                        "deferrable": True,
                        "initially": "DEFERRED",
                        "onupdate": "CASCADE",
                    },
                },
                {
                    "referred_table": "a2",
                    "referred_columns": ["id"],
                    "referred_schema": None,
                    "name": "fk4",
                    "constrained_columns": ["c4"],
                    "options": {"deferrable": False, "initially": "IMMEDIATE"},
                },
            ],
        )

    def test_foreign_key_options_unnamed_inline(self):
        with testing.db.begin() as conn:
            conn.exec_driver_sql(
                "create table foo (id integer, "
                "foreign key (id) references bar (id) on update cascade)"
            )

            insp = inspect(conn)
            eq_(
                insp.get_foreign_keys("foo"),
                [
                    {
                        "name": None,
                        "referred_columns": ["id"],
                        "referred_table": "bar",
                        "constrained_columns": ["id"],
                        "referred_schema": None,
                        "options": {"onupdate": "CASCADE"},
                    }
                ],
            )

    def test_dont_reflect_autoindex(self):
        inspector = inspect(testing.db)
        eq_(inspector.get_indexes("o"), [])
        eq_(
            inspector.get_indexes("o", include_auto_indexes=True),
            [
                {
                    "unique": 1,
                    "name": "sqlite_autoindex_o_1",
                    "column_names": ["foo"],
                    "dialect_options": {},
                }
            ],
        )

    def test_create_index_with_schema(self):
        """Test creation of index with explicit schema"""

        inspector = inspect(testing.db)
        eq_(
            inspector.get_indexes("l", schema="main"),
            [
                {
                    "unique": 0,
                    "name": "ix_main_l_bar",
                    "column_names": ["bar"],
                    "dialect_options": {},
                }
            ],
        )

    @testing.requires.sqlite_partial_indexes
    def test_reflect_partial_indexes(self, connection):
        connection.exec_driver_sql(
            "create table foo_with_partial_index (x integer, y integer)"
        )
        connection.exec_driver_sql(
            "create unique index ix_partial on "
            "foo_with_partial_index (x) where y > 10"
        )
        connection.exec_driver_sql(
            "create unique index ix_no_partial on "
            "foo_with_partial_index (x)"
        )
        connection.exec_driver_sql(
            "create unique index ix_partial2 on "
            "foo_with_partial_index (x, y) where "
            "y = 10 or abs(x) < 5"
        )

        inspector = inspect(connection)
        indexes = inspector.get_indexes("foo_with_partial_index")
        eq_(
            indexes,
            [
                {
                    "unique": 1,
                    "name": "ix_no_partial",
                    "column_names": ["x"],
                    "dialect_options": {},
                },
                {
                    "unique": 1,
                    "name": "ix_partial",
                    "column_names": ["x"],
                    "dialect_options": {"sqlite_where": mock.ANY},
                },
                {
                    "unique": 1,
                    "name": "ix_partial2",
                    "column_names": ["x", "y"],
                    "dialect_options": {"sqlite_where": mock.ANY},
                },
            ],
        )
        eq_(indexes[1]["dialect_options"]["sqlite_where"].text, "y > 10")
        eq_(
            indexes[2]["dialect_options"]["sqlite_where"].text,
            "y = 10 or abs(x) < 5",
        )

    def test_unique_constraint_named(self):
        inspector = inspect(testing.db)
        eq_(
            inspector.get_unique_constraints("f"),
            [{"column_names": ["x"], "name": "foo_fx"}],
        )

    def test_unique_constraint_named_broken_casing(self):
        inspector = inspect(testing.db)
        eq_(
            inspector.get_unique_constraints("h"),
            [{"column_names": ["x"], "name": "foo_hx"}],
        )

    def test_unique_constraint_named_broken_temp(
        self, connection, temp_table_fixture
    ):
        inspector = inspect(connection)
        eq_(
            inspector.get_unique_constraints("g"),
            [{"column_names": ["x"], "name": "foo_gx"}],
        )

    def test_unique_constraint_unnamed_inline(self):
        inspector = inspect(testing.db)
        eq_(
            inspector.get_unique_constraints("d"),
            [{"column_names": ["x"], "name": None}],
        )

    def test_unique_constraint_unnamed_inline_quoted(self):
        inspector = inspect(testing.db)
        eq_(
            inspector.get_unique_constraints("d1"),
            [{"column_names": ["some ( STUPID n,ame"], "name": None}],
        )
        eq_(
            inspector.get_unique_constraints("d2"),
            [{"column_names": ["some STUPID n,ame"], "name": None}],
        )
        eq_(
            inspector.get_unique_constraints("d3"),
            [{"column_names": ["some STUPID n,ame"], "name": None}],
        )

    def test_unique_constraint_unnamed_normal(self):
        inspector = inspect(testing.db)
        eq_(
            inspector.get_unique_constraints("m"),
            [{"column_names": ["x"], "name": None}],
        )

    def test_unique_constraint_unnamed_normal_temporary(
        self, connection, temp_table_fixture
    ):
        inspector = inspect(connection)
        eq_(
            inspector.get_unique_constraints("n"),
            [{"column_names": ["x"], "name": None}],
        )

    def test_unique_constraint_mixed_into_ck(self, connection):
        """test #11832"""

        inspector = inspect(connection)
        eq_(
            inspector.get_unique_constraints("r"),
            [
                {"name": "prefix_named", "column_names": ["prefix"]},
                {"name": None, "column_names": ["value"]},
            ],
        )

    def test_primary_key_constraint_mixed_into_ck(self, connection):
        """test #11832"""

        inspector = inspect(connection)
        eq_(
            inspector.get_pk_constraint("r"),
            {"constrained_columns": ["id"], "name": "pk_name"},
        )

    def test_primary_key_constraint_named(self):
        inspector = inspect(testing.db)
        eq_(
            inspector.get_pk_constraint("p"),
            {"constrained_columns": ["id"], "name": "pk_name"},
        )

    def test_primary_key_constraint_unnamed(self):
        inspector = inspect(testing.db)
        eq_(
            inspector.get_pk_constraint("q"),
            {"constrained_columns": ["id"], "name": None},
        )

    def test_primary_key_constraint_no_pk(self):
        inspector = inspect(testing.db)
        eq_(
            inspector.get_pk_constraint("d"),
            {"constrained_columns": [], "name": None},
        )

    def test_check_constraint_plain(self):
        inspector = inspect(testing.db)
        eq_(
            inspector.get_check_constraints("cp"),
            [
                {"sqltext": "p = 1 OR (p > 2 AND p < 5)", "name": "cq"},
            ],
        )

    def test_check_constraint_inline_plain(self):
        inspector = inspect(testing.db)
        eq_(
            inspector.get_check_constraints("cp_inline"),
            [
                {"sqltext": "p = 1 OR (p > 2 AND p < 5)", "name": "cq"},
                {"sqltext": "q > 1 AND q < 6", "name": None},
            ],
        )

    def test_check_constraint_multiline(self):
        """test for #11677"""

        inspector = inspect(testing.db)
        eq_(
            inspector.get_check_constraints("r"),
            [
                {"sqltext": "value IS NOT NULL", "name": "^check-r* #\n\t"},
                # Triple-quote multi-line definition should have added a
                # newline and whitespace:
                {
                    "sqltext": "((value > 0) AND \n\t(value < 100) AND \n\t\n"
                    "                      (value != 50))",
                    "name": "ck_r_value_multiline",
                },
                {"sqltext": "id > 0", "name": None},
                {"sqltext": "prefix NOT GLOB '*[^-. /#,]*'", "name": None},
            ],
        )

    @testing.combinations(
        ("plain_name", "plain_name"),
        ("name with spaces", "name with spaces"),
        ("plainname", "plainname"),
        ("[Code]", "[Code]"),
        (quoted_name("[Code]", quote=False), "Code"),
        argnames="colname,expected",
    )
    @testing.combinations(
        "uq",
        "uq_inline",
        "uq_inline_tab_before",  # tab before column params
        "uq_inline_tab_within",  # tab within column params
        "pk",
        "ix",
        argnames="constraint_type",
    )
    def test_constraint_cols(
        self, colname, expected, constraint_type, connection, metadata
    ):
        if constraint_type.startswith("uq_inline"):
            inline_create_sql = {
                "uq_inline": "CREATE TABLE t (%s INTEGER UNIQUE)",
                "uq_inline_tab_before": "CREATE TABLE t (%s\tINTEGER UNIQUE)",
                "uq_inline_tab_within": "CREATE TABLE t (%s INTEGER\tUNIQUE)",
            }

            t = Table("t", metadata, Column(colname, Integer))
            connection.exec_driver_sql(
                inline_create_sql[constraint_type]
                % connection.dialect.identifier_preparer.quote(colname)
            )
        else:
            t = Table("t", metadata, Column(colname, Integer))
            if constraint_type == "uq":
                constraint = UniqueConstraint(t.c[colname])
            elif constraint_type == "pk":
                constraint = PrimaryKeyConstraint(t.c[colname])
            elif constraint_type == "ix":
                constraint = Index("some_index", t.c[colname])
            else:
                assert False

            t.append_constraint(constraint)

            t.create(connection)

        if constraint_type in (
            "uq",
            "uq_inline",
            "uq_inline_tab_before",
            "uq_inline_tab_within",
        ):
            const = inspect(connection).get_unique_constraints("t")[0]
            eq_(const["column_names"], [expected])
        elif constraint_type == "pk":
            const = inspect(connection).get_pk_constraint("t")
            eq_(const["constrained_columns"], [expected])
        elif constraint_type == "ix":
            const = inspect(connection).get_indexes("t")[0]
            eq_(const["column_names"], [expected])
        else:
            assert False

    def test_check_constraint_oneline(self):
        inspector = inspect(testing.db)
        eq_(
            inspector.get_check_constraints("oneline"),
            [
                {"sqltext": "field>0", "name": None},
            ],
        )

    def test_check_constraint_oneline_nested(self):
        inspector = inspect(testing.db)
        eq_(
            inspector.get_check_constraints("oneline_nested"),
            [
                {
                    "sqltext": "(field>0 and field<22) "
                    "or (field>99 and field<1010)",
                    "name": None,
                },
            ],
        )

    def test_check_constraint_oneline_2constraints(self):
        inspector = inspect(testing.db)
        eq_(
            inspector.get_check_constraints("oneline_2constraints"),
            [
                {
                    "sqltext": "(field>0 and field<22) "
                    "or (field>99 and field<1010)",
                    "name": "chname",
                },
            ],
        )

    def test_check_constraint_oneline_nameCHECK(self):
        inspector = inspect(testing.db)
        eq_(
            inspector.get_check_constraints("oneline_nameCHECK"),
            [],
        )

    def test_check_constraint_oneline_nameCONSTRAINT(self):
        inspector = inspect(testing.db)
        eq_(
            inspector.get_check_constraints("oneline_nameCONSTRAINT"),
            [
                {"sqltext": "field IN (1, 0, -1)", "name": None},
            ],
        )

    def test_check_constraint_twochecks_oneline(self):
        inspector = inspect(testing.db)
        eq_(
            inspector.get_check_constraints("twochecks_oneline"),
            [
                {"sqltext": "field>1", "name": None},
                {"sqltext": "field<9", "name": None},
            ],
        )

    def test_check_constraint_quote_styles(self):
        """Test all SQLite identifier quote styles for constraint names.

        SQLite supports 4 quote styles: double quotes, single quotes,
        brackets, and backticks (for compatibility with other databases).
        """
        inspector = inspect(testing.db)
        eq_(
            inspector.get_check_constraints("quote_styles"),
            [
                {"sqltext": "field >= 10", "name": "backtick_quoted"},
                {"sqltext": "field != 50", "name": "bracket_quoted"},
                {"sqltext": "field > 0", "name": "double_quoted"},
                {"sqltext": "field < 100", "name": "single_quoted"},
            ],
        )

    def test_check_constraint_parens_in_strings(self):
        """Test CHECK constraints with parentheses inside string literals.

        Parentheses inside quoted strings should not be counted when
        matching balanced parentheses in the constraint expression.
        These test cases have unbalanced parens if strings are not handled,
        and include escaped quotes (SQLite uses '' to escape, not backslash).
        """
        inspector = inspect(testing.db)
        eq_(
            inspector.get_check_constraints("parens_in_strings"),
            [
                {"sqltext": "field != '('", "name": None},
                {"sqltext": "field != ')'", "name": None},
                {"sqltext": "field NOT LIKE '%('", "name": None},
                {"sqltext": "field IN (')', '(', 'test')", "name": None},
                # Escaped quotes with parens
                {"sqltext": "field != 'it''s (not) valid'", "name": None},
                {"sqltext": 'field != "say ""(hello)"" "', "name": None},
                {
                    "sqltext": "field NOT IN ('()', 'a''b''c', ')')",
                    "name": None,
                },
                # Complex nested cases
                {"sqltext": "field != '((' OR field = ')))'", "name": None},
                {
                    "sqltext": 'field LIKE "%))(%" OR field LIKE "%)(%"',
                    "name": None,
                },
            ],
        )


class TypeReflectionTest(fixtures.TestBase):
    __only_on__ = "sqlite"
    __backend__ = True

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
            (
                sqlite.DATE(storage_format="%(year)04d%(month)02d%(day)02d"),
                sqltypes.DATE(),
            ),
            (
                sqlite.TIME(
                    storage_format="%(hour)02d%(minute)02d%(second)02d"
                ),
                sqltypes.TIME(),
            ),
            (
                sqlite.DATETIME(
                    storage_format="%(year)04d%(month)02d%(day)02d"
                    "%(hour)02d%(minute)02d%(second)02d"
                ),
                sqltypes.DATETIME(),
            ),
        ]

    def _unsupported_args_fixture(self):
        return [
            ("INTEGER(5)", sqltypes.INTEGER()),
            ("DATETIME(6, 12)", sqltypes.DATETIME()),
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
            ("SOMETHING UNKNOWN", sqltypes.NUMERIC()),
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
                    go, ["Could not instantiate"], regex=True
                )
            else:
                final_type = dialect._resolve_type_affinity(from_)
            expected_type = type(to_)
            is_(type(final_type), expected_type)

    def _test_round_trip(self, fixture, warnings=False):
        from sqlalchemy import inspect

        for from_, to_ in self._fixture_as_string(fixture):
            with testing.db.begin() as conn:
                inspector = inspect(conn)
                conn.exec_driver_sql("CREATE TABLE foo (data %s)" % from_)
                try:
                    if warnings:

                        def go():
                            return inspector.get_columns("foo")[0]

                        col_info = testing.assert_warnings(
                            go, ["Could not instantiate"], regex=True
                        )
                    else:
                        col_info = inspector.get_columns("foo")[0]
                    expected_type = type(to_)
                    is_(type(col_info["type"]), expected_type)

                    # test args
                    for attr in ("scale", "precision", "length"):
                        if getattr(to_, attr, None) is not None:
                            eq_(
                                getattr(col_info["type"], attr),
                                getattr(to_, attr, None),
                            )
                finally:
                    conn.exec_driver_sql("DROP TABLE foo")

    def test_lookup_direct_lookup(self):
        self._test_lookup_direct(self._fixed_lookup_fixture())

    def test_lookup_direct_unsupported_args(self):
        self._test_lookup_direct(
            self._unsupported_args_fixture(), warnings=True
        )

    def test_lookup_direct_type_affinity(self):
        self._test_lookup_direct(self._type_affinity_fixture())

    def test_round_trip_direct_lookup(self):
        self._test_round_trip(self._fixed_lookup_fixture())

    def test_round_trip_direct_unsupported_args(self):
        self._test_round_trip(self._unsupported_args_fixture(), warnings=True)

    def test_round_trip_direct_type_affinity(self):
        self._test_round_trip(self._type_affinity_fixture())


class ReflectInternalSchemaTables(fixtures.TablesTest):
    __only_on__ = "sqlite"
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "sqliteatable",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("other", String(42)),
            sqlite_autoincrement=True,
        )
        view = "CREATE VIEW sqliteview AS SELECT * FROM sqliteatable"
        event.listen(metadata, "after_create", DDL(view))
        event.listen(metadata, "before_drop", DDL("DROP VIEW sqliteview"))

    def test_get_table_names(self, connection):
        insp = inspect(connection)

        res = insp.get_table_names(sqlite_include_internal=True)
        eq_(res, ["sqlite_sequence", "sqliteatable"])
        res = insp.get_table_names()
        eq_(res, ["sqliteatable"])

        meta = MetaData()
        meta.reflect(connection)
        eq_(len(meta.tables), 1)
        eq_(set(meta.tables), {"sqliteatable"})

    def test_get_view_names(self, connection):
        insp = inspect(connection)

        res = insp.get_view_names(sqlite_include_internal=True)
        eq_(res, ["sqliteview"])
        res = insp.get_view_names()
        eq_(res, ["sqliteview"])

    def test_get_temp_table_names(self, connection, metadata):
        Table(
            "sqlitetemptable",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("other", String(42)),
            sqlite_autoincrement=True,
            prefixes=["TEMPORARY"],
        ).create(connection)
        insp = inspect(connection)

        res = insp.get_temp_table_names(sqlite_include_internal=True)
        eq_(res, ["sqlite_sequence", "sqlitetemptable"])
        res = insp.get_temp_table_names()
        eq_(res, ["sqlitetemptable"])

    def test_get_temp_view_names(self, connection):
        view = (
            "CREATE TEMPORARY VIEW sqlitetempview AS "
            "SELECT * FROM sqliteatable"
        )
        connection.exec_driver_sql(view)
        insp = inspect(connection)
        try:
            res = insp.get_temp_view_names(sqlite_include_internal=True)
            eq_(res, ["sqlitetempview"])
            res = insp.get_temp_view_names()
            eq_(res, ["sqlitetempview"])
        finally:
            connection.exec_driver_sql("DROP VIEW sqlitetempview")


class ComputedReflectionTest(fixtures.TestBase):
    __only_on__ = "sqlite"
    __backend__ = True

    @testing.combinations(
        (
            """CREATE TABLE test1 (
                s VARCHAR,
                x VARCHAR GENERATED ALWAYS AS (s || 'x')
            );""",
            "test1",
            {"x": {"text": "s || 'x'", "stored": False}},
        ),
        (
            """CREATE TABLE test2 (
                s VARCHAR,
                x VARCHAR GENERATED ALWAYS AS (s || 'x'),
                y VARCHAR GENERATED ALWAYS AS (s || 'y')
            );""",
            "test2",
            {
                "x": {"text": "s || 'x'", "stored": False},
                "y": {"text": "s || 'y'", "stored": False},
            },
        ),
        (
            """CREATE TABLE test3 (
                s VARCHAR,
                x INTEGER GENERATED ALWAYS AS (INSTR(s, ","))
            );""",
            "test3",
            {"x": {"text": 'INSTR(s, ",")', "stored": False}},
        ),
        (
            """CREATE TABLE test4 (
                s VARCHAR,
                x INTEGER GENERATED ALWAYS AS (INSTR(s, ",")),
                y INTEGER GENERATED ALWAYS AS (INSTR(x, ",")));""",
            "test4",
            {
                "x": {"text": 'INSTR(s, ",")', "stored": False},
                "y": {"text": 'INSTR(x, ",")', "stored": False},
            },
        ),
        (
            """CREATE TABLE test5 (
                s VARCHAR,
                x VARCHAR GENERATED ALWAYS AS (s || 'x') STORED
            );""",
            "test5",
            {"x": {"text": "s || 'x'", "stored": True}},
        ),
        (
            """CREATE TABLE test6 (
                s VARCHAR,
                x VARCHAR GENERATED ALWAYS AS (s || 'x') STORED,
                y VARCHAR GENERATED ALWAYS AS (s || 'y') STORED
            );""",
            "test6",
            {
                "x": {"text": "s || 'x'", "stored": True},
                "y": {"text": "s || 'y'", "stored": True},
            },
        ),
        (
            """CREATE TABLE test7 (
                s VARCHAR,
                x INTEGER GENERATED ALWAYS AS (INSTR(s, ",")) STORED
            );""",
            "test7",
            {"x": {"text": 'INSTR(s, ",")', "stored": True}},
        ),
        (
            """CREATE TABLE test8 (
                s VARCHAR,
                x INTEGER GENERATED ALWAYS AS (INSTR(s, ",")) STORED,
                y INTEGER GENERATED ALWAYS AS (INSTR(x, ",")) STORED
            );""",
            "test8",
            {
                "x": {"text": 'INSTR(s, ",")', "stored": True},
                "y": {"text": 'INSTR(x, ",")', "stored": True},
            },
        ),
        (
            """CREATE TABLE test9 (
                id INTEGER PRIMARY KEY,
                s VARCHAR,
                x VARCHAR GENERATED ALWAYS AS (s || 'x')
            ) WITHOUT ROWID;""",
            "test9",
            {"x": {"text": "s || 'x'", "stored": False}},
        ),
        (
            """CREATE TABLE test_strict1 (
                s TEXT,
                x TEXT GENERATED ALWAYS AS (s || 'x')
            ) STRICT;""",
            "test_strict1",
            {"x": {"text": "s || 'x'", "stored": False}},
            testing.only_on("sqlite>=3.37.0"),
        ),
        (
            """CREATE TABLE test_strict2 (
                id INTEGER PRIMARY KEY,
                s TEXT,
                x TEXT GENERATED ALWAYS AS (s || 'x')
            ) STRICT, WITHOUT ROWID;""",
            "test_strict2",
            {"x": {"text": "s || 'x'", "stored": False}},
            testing.only_on("sqlite>=3.37.0"),
        ),
        (
            """CREATE TABLE test_strict3 (
                id INTEGER PRIMARY KEY,
                s TEXT,
                x TEXT GENERATED ALWAYS AS (s || 'x')
            ) WITHOUT ROWID, STRICT;""",
            "test_strict3",
            {"x": {"text": "s || 'x'", "stored": False}},
            testing.only_on("sqlite>=3.37.0"),
        ),
        argnames="table_ddl,table_name,spec",
        id_="asa",
    )
    @testing.requires.computed_columns
    def test_reflection(
        self, metadata, connection, table_ddl, table_name, spec
    ):
        connection.exec_driver_sql(table_ddl)

        tbl = Table(table_name, metadata, autoload_with=connection)
        seen = set(spec).intersection(tbl.c.keys())

        for col in tbl.c:
            if col.name not in seen:
                is_(col.computed, None)
            else:
                info = spec[col.name]
                msg = f"{tbl.name}-{col.name}"
                is_true(bool(col.computed))
                eq_(col.computed.sqltext.text, info["text"], msg)
                eq_(col.computed.persisted, info["stored"], msg)
