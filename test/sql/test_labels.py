from sqlalchemy import bindparam
from sqlalchemy import Boolean
from sqlalchemy import cast
from sqlalchemy import exc as exceptions
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import or_
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy import type_coerce
from sqlalchemy.engine import default
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import coercions
from sqlalchemy.sql import column
from sqlalchemy.sql import LABEL_STYLE_TABLENAME_PLUS_COL
from sqlalchemy.sql import roles
from sqlalchemy.sql import table
from sqlalchemy.sql.elements import _truncated_label
from sqlalchemy.sql.elements import ColumnElement
from sqlalchemy.sql.elements import WrapsColumnExpression
from sqlalchemy.sql.selectable import LABEL_STYLE_NONE
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import engines
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import mock
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table

IDENT_LENGTH = 29


class MaxIdentTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = "DefaultDialect"
    __backend__ = True

    table1 = table(
        "some_large_named_table",
        column("this_is_the_primarykey_column"),
        column("this_is_the_data_column"),
    )

    table2 = table(
        "table_with_exactly_29_characs",
        column("this_is_the_primarykey_column"),
        column("this_is_the_data_column"),
    )

    def _length_fixture(self, length=IDENT_LENGTH, positional=False):
        dialect = default.DefaultDialect()
        dialect.max_identifier_length = (
            dialect._user_defined_max_identifier_length
        ) = length
        if positional:
            dialect.paramstyle = "format"
            dialect.positional = True
        return dialect

    def _engine_fixture(self, length=IDENT_LENGTH):
        eng = engines.testing_engine()
        eng.dialect.max_identifier_length = (
            eng.dialect._user_defined_max_identifier_length
        ) = length
        return eng

    def test_label_length_raise_too_large(self):
        max_ident_length = testing.db.dialect.max_identifier_length
        eng = engines.testing_engine(
            options={"label_length": max_ident_length + 10}
        )
        assert_raises_message(
            exceptions.ArgumentError,
            "Label length of %d is greater than this dialect's maximum "
            "identifier length of %d"
            % (max_ident_length + 10, max_ident_length),
            eng.connect,
        )

    def test_label_length_custom_maxlen(self):
        max_ident_length = testing.db.dialect.max_identifier_length
        eng = engines.testing_engine(
            options={
                "label_length": max_ident_length + 10,
                "max_identifier_length": max_ident_length + 20,
            }
        )
        with eng.connect() as conn:
            eq_(conn.dialect.max_identifier_length, max_ident_length + 20)

    def test_label_length_custom_maxlen_dialect_only(self):
        dialect = default.DefaultDialect(max_identifier_length=47)
        eq_(dialect.max_identifier_length, 47)

    def test_label_length_custom_maxlen_user_set_manually(self):
        eng = engines.testing_engine()
        eng.dialect.max_identifier_length = 47

        # assume the dialect has no on-connect change
        with mock.patch.object(
            eng.dialect,
            "_check_max_identifier_length",
            side_effect=lambda conn: None,
        ):
            with eng.connect():
                pass

        # it was maintained
        eq_(eng.dialect.max_identifier_length, 47)

    def test_label_length_too_large_custom_maxlen(self):
        max_ident_length = testing.db.dialect.max_identifier_length
        eng = engines.testing_engine(
            options={
                "label_length": max_ident_length - 10,
                "max_identifier_length": max_ident_length - 20,
            }
        )
        assert_raises_message(
            exceptions.ArgumentError,
            "Label length of %d is greater than this dialect's maximum "
            "identifier length of %d"
            % (max_ident_length - 10, max_ident_length - 20),
            eng.connect,
        )

    def test_custom_max_identifier_length(self):
        max_ident_length = testing.db.dialect.max_identifier_length
        eng = engines.testing_engine(
            options={"max_identifier_length": max_ident_length + 20}
        )
        with eng.connect() as conn:
            eq_(conn.dialect.max_identifier_length, max_ident_length + 20)

    def test_max_identifier_length_onconnect(self):
        eng = engines.testing_engine()

        def _check_max_identifer_length(conn):
            return 47

        with mock.patch.object(
            eng.dialect,
            "_check_max_identifier_length",
            side_effect=_check_max_identifer_length,
        ) as mock_:
            with eng.connect():
                eq_(eng.dialect.max_identifier_length, 47)
        eq_(mock_.mock_calls, [mock.call(mock.ANY)])

    def test_max_identifier_length_onconnect_returns_none(self):
        eng = engines.testing_engine()

        max_ident_length = eng.dialect.max_identifier_length

        def _check_max_identifer_length(conn):
            return None

        with mock.patch.object(
            eng.dialect,
            "_check_max_identifier_length",
            side_effect=_check_max_identifer_length,
        ) as mock_:
            with eng.connect():
                eq_(eng.dialect.max_identifier_length, max_ident_length)
        eq_(mock_.mock_calls, [mock.call(mock.ANY)])

    def test_custom_max_identifier_length_onconnect(self):
        eng = engines.testing_engine(options={"max_identifier_length": 49})

        def _check_max_identifer_length(conn):
            return 47

        with mock.patch.object(
            eng.dialect,
            "_check_max_identifier_length",
            side_effect=_check_max_identifer_length,
        ) as mock_:
            with eng.connect():
                eq_(eng.dialect.max_identifier_length, 49)
        eq_(mock_.mock_calls, [])  # was not called

    def test_table_alias_1(self):
        self.assert_compile(
            self.table2.alias().select(),
            "SELECT "
            "table_with_exactly_29_c_1."
            "this_is_the_primarykey_column, "
            "table_with_exactly_29_c_1.this_is_the_data_column "
            "FROM "
            "table_with_exactly_29_characs "
            "AS table_with_exactly_29_c_1",
            dialect=self._length_fixture(),
        )

    def test_table_alias_2(self):
        table1 = self.table1
        table2 = self.table2
        ta = table2.alias()
        on = table1.c.this_is_the_data_column == ta.c.this_is_the_data_column
        self.assert_compile(
            select(table1, ta)
            .select_from(table1.join(ta, on))
            .where(ta.c.this_is_the_data_column == "data3")
            .set_label_style(LABEL_STYLE_NONE),
            "SELECT "
            "some_large_named_table.this_is_the_primarykey_column, "
            "some_large_named_table.this_is_the_data_column, "
            "table_with_exactly_29_c_1.this_is_the_primarykey_column, "
            "table_with_exactly_29_c_1.this_is_the_data_column "
            "FROM "
            "some_large_named_table "
            "JOIN "
            "table_with_exactly_29_characs "
            "AS "
            "table_with_exactly_29_c_1 "
            "ON "
            "some_large_named_table.this_is_the_data_column = "
            "table_with_exactly_29_c_1.this_is_the_data_column "
            "WHERE "
            "table_with_exactly_29_c_1.this_is_the_data_column = "
            ":this_is_the_data_column_1",
            dialect=self._length_fixture(),
        )

    def test_too_long_name_disallowed(self):
        m = MetaData()
        t = Table(
            "this_name_is_too_long_for_what_were_doing_in_this_test",
            m,
            Column("foo", Integer),
        )
        eng = self._engine_fixture()
        methods = (t.create, t.drop, m.create_all, m.drop_all)
        for meth in methods:
            assert_raises(exceptions.IdentifierError, meth, eng)

    def _assert_labeled_table1_select(self, s):
        table1 = self.table1
        compiled = s.compile(dialect=self._length_fixture())

        assert set(
            compiled._create_result_map()["some_large_named_table__2"][1]
        ).issuperset(
            [
                "some_large_named_table_this_is_the_data_column",
                "some_large_named_table__2",
                table1.c.this_is_the_data_column,
            ]
        )

        assert set(
            compiled._create_result_map()["some_large_named_table__1"][1]
        ).issuperset(
            [
                "some_large_named_table_this_is_the_primarykey_column",
                "some_large_named_table__1",
                table1.c.this_is_the_primarykey_column,
            ]
        )

    def test_result_map_use_labels(self):
        table1 = self.table1
        s = (
            table1.select()
            .set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)
            .order_by(table1.c.this_is_the_primarykey_column)
        )

        self._assert_labeled_table1_select(s)

    def test_result_map_limit(self):
        table1 = self.table1
        # some dialects such as oracle (and possibly ms-sql in a future
        # version) generate a subquery for limits/offsets. ensure that the
        # generated result map corresponds to the selected table, not the
        # select query
        s = (
            table1.select()
            .set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)
            .order_by(table1.c.this_is_the_primarykey_column)
            .limit(2)
        )
        self._assert_labeled_table1_select(s)

    def test_result_map_subquery(self):
        table1 = self.table1
        s = (
            table1.select()
            .where(table1.c.this_is_the_primarykey_column == 4)
            .alias("foo")
        )
        s2 = select(s)
        compiled = s2.compile(dialect=self._length_fixture())
        assert set(
            compiled._create_result_map()["this_is_the_data_column"][1]
        ).issuperset(["this_is_the_data_column", s.c.this_is_the_data_column])
        assert set(
            compiled._create_result_map()["this_is_the_primarykey__1"][1]
        ).issuperset(
            [
                "this_is_the_primarykey_column",
                "this_is_the_primarykey__1",
                s.c.this_is_the_primarykey_column,
            ]
        )

    def test_result_map_anon_alias(self):
        table1 = self.table1
        dialect = self._length_fixture()

        q = (
            table1.select()
            .where(table1.c.this_is_the_primarykey_column == 4)
            .alias()
        )
        s = select(q).set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)

        self.assert_compile(
            s,
            "SELECT "
            "anon_1.this_is_the_primarykey__2 AS anon_1_this_is_the_prim_1, "
            "anon_1.this_is_the_data_column AS anon_1_this_is_the_data_3 "
            "FROM ("
            "SELECT "
            "some_large_named_table."
            "this_is_the_primarykey_column AS this_is_the_primarykey__2, "
            "some_large_named_table."
            "this_is_the_data_column AS this_is_the_data_column "
            "FROM "
            "some_large_named_table "
            "WHERE "
            "some_large_named_table.this_is_the_primarykey_column "
            "= :this_is_the_primarykey__1"
            ") "
            "AS anon_1",
            dialect=dialect,
        )

        compiled = s.compile(dialect=dialect)
        assert set(
            compiled._create_result_map()["anon_1_this_is_the_data_3"][1]
        ).issuperset(
            [
                "anon_1_this_is_the_data_3",
                q.corresponding_column(table1.c.this_is_the_data_column),
            ]
        )

        assert set(
            compiled._create_result_map()["anon_1_this_is_the_prim_1"][1]
        ).issuperset(
            [
                "anon_1_this_is_the_prim_1",
                q.corresponding_column(table1.c.this_is_the_primarykey_column),
            ]
        )

    def test_column_bind_labels_1(self):
        table1 = self.table1

        s = table1.select().where(table1.c.this_is_the_primarykey_column == 4)
        self.assert_compile(
            s,
            "SELECT some_large_named_table.this_is_the_primarykey_column, "
            "some_large_named_table.this_is_the_data_column "
            "FROM some_large_named_table WHERE "
            "some_large_named_table.this_is_the_primarykey_column = "
            ":this_is_the_primarykey__1",
            checkparams={"this_is_the_primarykey__1": 4},
            dialect=self._length_fixture(),
        )

        self.assert_compile(
            s,
            "SELECT some_large_named_table.this_is_the_primarykey_column, "
            "some_large_named_table.this_is_the_data_column "
            "FROM some_large_named_table WHERE "
            "some_large_named_table.this_is_the_primarykey_column = "
            "%s",
            checkpositional=(4,),
            checkparams={"this_is_the_primarykey__1": 4},
            dialect=self._length_fixture(positional=True),
        )

    def test_column_bind_labels_2(self):
        table1 = self.table1

        s = table1.select().where(
            or_(
                table1.c.this_is_the_primarykey_column == 4,
                table1.c.this_is_the_primarykey_column == 2,
            )
        )
        self.assert_compile(
            s,
            "SELECT some_large_named_table.this_is_the_primarykey_column, "
            "some_large_named_table.this_is_the_data_column "
            "FROM some_large_named_table WHERE "
            "some_large_named_table.this_is_the_primarykey_column = "
            ":this_is_the_primarykey__1 OR "
            "some_large_named_table.this_is_the_primarykey_column = "
            ":this_is_the_primarykey__2",
            checkparams={
                "this_is_the_primarykey__1": 4,
                "this_is_the_primarykey__2": 2,
            },
            dialect=self._length_fixture(),
        )
        self.assert_compile(
            s,
            "SELECT some_large_named_table.this_is_the_primarykey_column, "
            "some_large_named_table.this_is_the_data_column "
            "FROM some_large_named_table WHERE "
            "some_large_named_table.this_is_the_primarykey_column = "
            "%s OR "
            "some_large_named_table.this_is_the_primarykey_column = "
            "%s",
            checkparams={
                "this_is_the_primarykey__1": 4,
                "this_is_the_primarykey__2": 2,
            },
            checkpositional=(4, 2),
            dialect=self._length_fixture(positional=True),
        )

    def test_bind_param_non_truncated(self):
        table1 = self.table1
        stmt = table1.insert().values(
            this_is_the_data_column=bindparam(
                "this_is_the_long_bindparam_name"
            )
        )
        compiled = stmt.compile(dialect=self._length_fixture(length=10))
        eq_(
            compiled.construct_params(
                params={"this_is_the_long_bindparam_name": 5}
            ),
            {"this_is_the_long_bindparam_name": 5},
        )

    def test_bind_param_truncated_named(self):
        table1 = self.table1
        bp = bindparam(_truncated_label("this_is_the_long_bindparam_name"))
        stmt = table1.insert().values(this_is_the_data_column=bp)
        compiled = stmt.compile(dialect=self._length_fixture(length=10))
        eq_(
            compiled.construct_params(
                params={"this_is_the_long_bindparam_name": 5}
            ),
            {"this_1": 5},
        )

    def test_bind_param_truncated_positional(self):
        table1 = self.table1
        bp = bindparam(_truncated_label("this_is_the_long_bindparam_name"))
        stmt = table1.insert().values(this_is_the_data_column=bp)
        compiled = stmt.compile(
            dialect=self._length_fixture(length=10, positional=True)
        )

        eq_(
            compiled.construct_params(
                params={"this_is_the_long_bindparam_name": 5}
            ),
            {"this_1": 5},
        )


class LabelLengthTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = "DefaultDialect"

    table1 = table(
        "some_large_named_table",
        column("this_is_the_primarykey_column"),
        column("this_is_the_data_column"),
    )

    table2 = table(
        "table_with_exactly_29_characs",
        column("this_is_the_primarykey_column"),
        column("this_is_the_data_column"),
    )

    def test_adjustable_1(self):
        table1 = self.table1
        q = (
            table1.select()
            .where(table1.c.this_is_the_primarykey_column == 4)
            .alias("foo")
        )
        x = select(q)
        compile_dialect = default.DefaultDialect(label_length=10)
        self.assert_compile(
            x,
            "SELECT "
            "foo.this_1, foo.this_2 "
            "FROM ("
            "SELECT "
            "some_large_named_table.this_is_the_primarykey_column "
            "AS this_1, "
            "some_large_named_table.this_is_the_data_column "
            "AS this_2 "
            "FROM "
            "some_large_named_table "
            "WHERE "
            "some_large_named_table.this_is_the_primarykey_column "
            "= :this_1"
            ") "
            "AS foo",
            dialect=compile_dialect,
        )

    def test_adjustable_2(self):
        table1 = self.table1

        q = (
            table1.select()
            .where(table1.c.this_is_the_primarykey_column == 4)
            .alias("foo")
        )
        x = select(q)

        compile_dialect = default.DefaultDialect(label_length=10)
        self.assert_compile(
            x,
            "SELECT "
            "foo.this_1, foo.this_2 "
            "FROM ("
            "SELECT "
            "some_large_named_table.this_is_the_primarykey_column "
            "AS this_1, "
            "some_large_named_table.this_is_the_data_column "
            "AS this_2 "
            "FROM "
            "some_large_named_table "
            "WHERE "
            "some_large_named_table.this_is_the_primarykey_column "
            "= :this_1"
            ") "
            "AS foo",
            dialect=compile_dialect,
        )

    def test_adjustable_3(self):
        table1 = self.table1

        compile_dialect = default.DefaultDialect(label_length=4)
        q = (
            table1.select()
            .where(table1.c.this_is_the_primarykey_column == 4)
            .alias("foo")
        )
        x = select(q)

        self.assert_compile(
            x,
            "SELECT "
            "foo._1, foo._2 "
            "FROM ("
            "SELECT "
            "some_large_named_table.this_is_the_primarykey_column "
            "AS _1, "
            "some_large_named_table.this_is_the_data_column "
            "AS _2 "
            "FROM "
            "some_large_named_table "
            "WHERE "
            "some_large_named_table.this_is_the_primarykey_column "
            "= :_1"
            ") "
            "AS foo",
            dialect=compile_dialect,
        )

    def test_adjustable_4(self):
        table1 = self.table1

        q = (
            table1.select()
            .where(table1.c.this_is_the_primarykey_column == 4)
            .alias()
        )
        x = select(q).set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)

        compile_dialect = default.DefaultDialect(label_length=10)
        self.assert_compile(
            x,
            "SELECT "
            "anon_1.this_2 AS anon_1, "
            "anon_1.this_4 AS anon_3 "
            "FROM ("
            "SELECT "
            "some_large_named_table.this_is_the_primarykey_column "
            "AS this_2, "
            "some_large_named_table.this_is_the_data_column "
            "AS this_4 "
            "FROM "
            "some_large_named_table "
            "WHERE "
            "some_large_named_table.this_is_the_primarykey_column "
            "= :this_1"
            ") "
            "AS anon_1",
            dialect=compile_dialect,
        )

    def test_adjustable_5(self):
        table1 = self.table1
        q = (
            table1.select()
            .where(table1.c.this_is_the_primarykey_column == 4)
            .alias()
        )
        x = select(q).set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)

        compile_dialect = default.DefaultDialect(label_length=4)
        self.assert_compile(
            x,
            "SELECT "
            "_1._2 AS _1, "
            "_1._4 AS _3 "
            "FROM ("
            "SELECT "
            "some_large_named_table.this_is_the_primarykey_column "
            "AS _2, "
            "some_large_named_table.this_is_the_data_column "
            "AS _4 "
            "FROM "
            "some_large_named_table "
            "WHERE "
            "some_large_named_table.this_is_the_primarykey_column "
            "= :_1"
            ") "
            "AS _1",
            dialect=compile_dialect,
        )

    def test_adjustable_result_schema_column_1(self):
        table1 = self.table1

        q = (
            table1.select()
            .where(table1.c.this_is_the_primarykey_column == 4)
            .set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)
            .alias("foo")
        )

        dialect = default.DefaultDialect(label_length=10)
        compiled = q.compile(dialect=dialect)

        assert set(compiled._create_result_map()["some_2"][1]).issuperset(
            [
                table1.c.this_is_the_data_column,
                "some_large_named_table_this_is_the_data_column",
                "some_2",
            ]
        )

        assert set(compiled._create_result_map()["some_1"][1]).issuperset(
            [
                table1.c.this_is_the_primarykey_column,
                "some_large_named_table_this_is_the_primarykey_column",
                "some_1",
            ]
        )

    def test_adjustable_result_schema_column_2(self):
        table1 = self.table1

        q = (
            table1.select()
            .where(table1.c.this_is_the_primarykey_column == 4)
            .alias("foo")
        )
        x = select(q)

        dialect = default.DefaultDialect(label_length=10)
        compiled = x.compile(dialect=dialect)

        assert set(compiled._create_result_map()["this_2"][1]).issuperset(
            [
                q.corresponding_column(table1.c.this_is_the_data_column),
                "this_is_the_data_column",
                "this_2",
            ]
        )

        assert set(compiled._create_result_map()["this_1"][1]).issuperset(
            [
                q.corresponding_column(table1.c.this_is_the_primarykey_column),
                "this_is_the_primarykey_column",
                "this_1",
            ]
        )

    def test_table_plus_column_exceeds_length(self):
        """test that the truncation only occurs when tablename + colname are
        concatenated, if they are individually under the label length.

        """

        compile_dialect = default.DefaultDialect(label_length=30)
        a_table = table("thirty_characters_table_xxxxxx", column("id"))

        other_table = table(
            "other_thirty_characters_table_",
            column("id"),
            column("thirty_characters_table_id"),
        )

        anon = a_table.alias()

        j1 = other_table.outerjoin(
            anon, anon.c.id == other_table.c.thirty_characters_table_id
        )

        self.assert_compile(
            select(other_table, anon)
            .select_from(j1)
            .set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL),
            "SELECT "
            "other_thirty_characters_table_.id "
            "AS other_thirty_characters__1, "
            "other_thirty_characters_table_.thirty_characters_table_id "
            "AS other_thirty_characters__2, "
            "thirty_characters_table__1.id "
            "AS thirty_characters_table__3 "
            "FROM "
            "other_thirty_characters_table_ "
            "LEFT OUTER JOIN "
            "thirty_characters_table_xxxxxx AS thirty_characters_table__1 "
            "ON thirty_characters_table__1.id = "
            "other_thirty_characters_table_.thirty_characters_table_id",
            dialect=compile_dialect,
        )

    def test_colnames_longer_than_labels_lowercase(self):
        t1 = table("a", column("abcde"))
        self._test_colnames_longer_than_labels(t1)

    def test_colnames_longer_than_labels_uppercase(self):
        m = MetaData()
        t1 = Table("a", m, Column("abcde", Integer))
        self._test_colnames_longer_than_labels(t1)

    def _test_colnames_longer_than_labels(self, t1):
        dialect = default.DefaultDialect(label_length=4)
        a1 = t1.alias(name="asdf")

        # 'abcde' is longer than 4, but rendered as itself
        # needs to have all characters
        s = select(a1)
        self.assert_compile(
            select(a1), "SELECT asdf.abcde FROM a AS asdf", dialect=dialect
        )
        compiled = s.compile(dialect=dialect)
        assert set(compiled._create_result_map()["abcde"][1]).issuperset(
            ["abcde", a1.c.abcde, "abcde"]
        )

        # column still there, but short label
        s = select(a1).set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)
        self.assert_compile(
            s, "SELECT asdf.abcde AS _1 FROM a AS asdf", dialect=dialect
        )
        compiled = s.compile(dialect=dialect)
        assert set(compiled._create_result_map()["_1"][1]).issuperset(
            ["asdf_abcde", a1.c.abcde, "_1"]
        )

    def test_label_overlap_unlabeled(self):
        """test that an anon col can't overlap with a fixed name, #3396"""

        table1 = table(
            "tablename", column("columnname_one"), column("columnn_1")
        )

        stmt = select(table1).set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)

        dialect = default.DefaultDialect(label_length=23)
        self.assert_compile(
            stmt,
            "SELECT tablename.columnname_one AS tablename_columnn_1, "
            "tablename.columnn_1 AS tablename_columnn_2 FROM tablename",
            dialect=dialect,
        )
        compiled = stmt.compile(dialect=dialect)
        eq_(
            set(compiled._create_result_map()),
            set(["tablename_columnn_1", "tablename_columnn_2"]),
        )


class ColExprLabelTest(fixtures.TestBase, AssertsCompiledSQL):
    """Test the :class:`.WrapsColumnExpression` mixin, which provides
    auto-labels that match a named expression

    """

    __dialect__ = "default"

    table1 = table("some_table", column("name"), column("value"))

    def _fixture(self):
        class SomeColThing(WrapsColumnExpression, ColumnElement):
            def __init__(self, expression):
                self.clause = coercions.expect(
                    roles.ExpressionElementRole, expression
                )

            @property
            def wrapped_column_expression(self):
                return self.clause

        @compiles(SomeColThing)
        def process(element, compiler, **kw):
            return "SOME_COL_THING(%s)" % compiler.process(
                element.clause, **kw
            )

        return SomeColThing

    def test_column_auto_label_dupes_label_style_none(self):
        expr = self._fixture()
        table1 = self.table1

        self.assert_compile(
            select(
                table1.c.name,
                table1.c.name,
                expr(table1.c.name),
                expr(table1.c.name),
            ).set_label_style(LABEL_STYLE_NONE),
            "SELECT some_table.name, some_table.name, "
            "SOME_COL_THING(some_table.name) AS name, "
            "SOME_COL_THING(some_table.name) AS name FROM some_table",
        )

    def test_column_auto_label_dupes_label_style_disambiguate(self):
        expr = self._fixture()
        table1 = self.table1

        self.assert_compile(
            select(
                table1.c.name,
                table1.c.name,
                expr(table1.c.name),
                expr(table1.c.name),
            ),
            "SELECT some_table.name, some_table.name AS name__1, "
            "SOME_COL_THING(some_table.name) AS some_table_name__1, "
            "SOME_COL_THING(some_table.name) AS some_table_name__2 "
            "FROM some_table",
        )

    def test_anon_expression_fallback(self):
        expr = self._fixture()
        table1 = self.table1

        self.assert_compile(
            select(table1.c.name + "foo", expr(table1.c.name + "foo")),
            "SELECT some_table.name || :name_1 AS anon_1, "
            "SOME_COL_THING(some_table.name || :name_2) AS anon_2 "
            "FROM some_table",
        )

    def test_anon_expression_fallback_use_labels(self):
        expr = self._fixture()
        table1 = self.table1

        self.assert_compile(
            select(
                table1.c.name + "foo", expr(table1.c.name + "foo")
            ).set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL),
            "SELECT some_table.name || :name_1 AS anon_1, "
            "SOME_COL_THING(some_table.name || :name_2) AS anon_2 "
            "FROM some_table",
        )

    def test_label_auto_label(self):
        expr = self._fixture()
        table1 = self.table1

        self.assert_compile(
            select(
                expr(table1.c.name.label("foo")),
                table1.c.name.label("bar"),
                table1.c.value,
            ),
            "SELECT SOME_COL_THING(some_table.name) AS foo, "
            "some_table.name AS bar, some_table.value FROM some_table",
        )

    def test_cast_auto_label_label_style_none(self):
        table1 = self.table1

        self.assert_compile(
            select(
                cast(table1.c.name, Integer),
                cast(table1.c.name, String),
                table1.c.name,
            ).set_label_style(LABEL_STYLE_NONE),
            "SELECT CAST(some_table.name AS INTEGER) AS name, "
            "CAST(some_table.name AS VARCHAR) AS name, "
            "some_table.name FROM some_table",
        )

    def test_cast_auto_label_label_style_disabmiguate(self):
        table1 = self.table1

        self.assert_compile(
            select(
                cast(table1.c.name, Integer),
                cast(table1.c.name, String),
                table1.c.name,
            ),
            "SELECT CAST(some_table.name AS INTEGER) AS name, "
            "CAST(some_table.name AS VARCHAR) AS some_table_name__1, "
            "some_table.name AS name_1 FROM some_table",
        )

    def test_type_coerce_auto_label_label_style_none(self):
        table1 = self.table1

        self.assert_compile(
            select(
                type_coerce(table1.c.name, Integer),
                type_coerce(table1.c.name, String),
                table1.c.name,
            ).set_label_style(LABEL_STYLE_NONE),
            # ideally type_coerce wouldn't label at all...
            "SELECT some_table.name AS name, "
            "some_table.name AS name, "
            "some_table.name FROM some_table",
        )

    def test_type_coerce_auto_label_label_style_disambiguate(self):
        table1 = self.table1

        self.assert_compile(
            select(
                type_coerce(table1.c.name, Integer),
                type_coerce(table1.c.name, String),
                table1.c.name,
            ),
            # ideally type_coerce wouldn't label at all...
            "SELECT some_table.name AS name, "
            "some_table.name AS some_table_name__1, "
            "some_table.name AS name_1 FROM some_table",
        )

    def test_boolean_auto_label(self):
        col = column("value", Boolean)

        self.assert_compile(
            select(~col, col),
            # not sure if this SQL is right but this is what it was
            # before the new labeling, just different label name
            "SELECT value = 0 AS value, value",
        )

    def test_label_auto_label_use_labels(self):
        expr = self._fixture()
        table1 = self.table1

        self.assert_compile(
            select(
                expr(table1.c.name.label("foo")),
                table1.c.name.label("bar"),
                table1.c.value,
            ).set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL),
            # the expr around label is treated the same way as plain column
            # with label
            "SELECT SOME_COL_THING(some_table.name) AS foo, "
            "some_table.name AS bar, "
            "some_table.value AS some_table_value FROM some_table",
        )

    def test_column_auto_label_dupes_use_labels(self):
        expr = self._fixture()
        table1 = self.table1

        self.assert_compile(
            select(
                table1.c.name,
                table1.c.name,
                expr(table1.c.name),
                expr(table1.c.name),
            ).set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL),
            "SELECT some_table.name AS some_table_name, "
            "some_table.name AS some_table_name__1, "
            "SOME_COL_THING(some_table.name) AS some_table_name_1, "
            "SOME_COL_THING(some_table.name) AS some_table_name_2 "
            "FROM some_table",
        )

    def test_column_auto_label_use_labels(self):
        expr = self._fixture()
        table1 = self.table1

        self.assert_compile(
            select(table1.c.name, expr(table1.c.value)).set_label_style(
                LABEL_STYLE_TABLENAME_PLUS_COL
            ),
            "SELECT some_table.name AS some_table_name, "
            "SOME_COL_THING(some_table.value) "
            "AS some_table_value FROM some_table",
        )
