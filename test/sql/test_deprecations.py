#! coding: utf-8

from copy import deepcopy

import pytest

from sqlalchemy import bindparam
from sqlalchemy import Column
from sqlalchemy import column
from sqlalchemy import create_engine
from sqlalchemy import DateTime
from sqlalchemy import exc
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import table
from sqlalchemy import testing
from sqlalchemy import text
from sqlalchemy import util
from sqlalchemy.engine import default
from sqlalchemy.schema import DDL
from sqlalchemy.sql import functions
from sqlalchemy.sql import util as sql_util
from sqlalchemy.sql.functions import GenericFunction
from sqlalchemy.sql.sqltypes import NullType
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import engines
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import in_
from sqlalchemy.testing import mock
from sqlalchemy.testing import not_in


class DeprecationWarningsTest(fixtures.TestBase):
    __backend__ = True

    def test_ident_preparer_force(self):
        preparer = testing.db.dialect.identifier_preparer
        preparer.quote("hi")
        with testing.expect_deprecated(
            "The IdentifierPreparer.quote.force parameter is deprecated"
        ):
            preparer.quote("hi", True)

        with testing.expect_deprecated(
            "The IdentifierPreparer.quote.force parameter is deprecated"
        ):
            preparer.quote("hi", False)

        preparer.quote_schema("hi")
        with testing.expect_deprecated(
            "The IdentifierPreparer.quote_schema.force parameter is deprecated"
        ):
            preparer.quote_schema("hi", True)

        with testing.expect_deprecated(
            "The IdentifierPreparer.quote_schema.force parameter is deprecated"
        ):
            preparer.quote_schema("hi", True)

    def test_string_convert_unicode(self):
        with testing.expect_deprecated(
            "The String.convert_unicode parameter is deprecated and "
            "will be removed in a future release."
        ):
            String(convert_unicode=True)

    def test_string_convert_unicode_force(self):
        with testing.expect_deprecated(
            "The String.convert_unicode parameter is deprecated and "
            "will be removed in a future release."
        ):
            String(convert_unicode="force")

    def test_engine_convert_unicode(self):
        with testing.expect_deprecated(
            "The create_engine.convert_unicode parameter and "
            "corresponding dialect-level"
        ):
            create_engine("mysql://", convert_unicode=True, module=mock.Mock())

    def test_join_condition_ignore_nonexistent_tables(self):
        m = MetaData()
        t1 = Table("t1", m, Column("id", Integer))
        t2 = Table(
            "t2", m, Column("id", Integer), Column("t1id", ForeignKey("t1.id"))
        )
        with testing.expect_deprecated(
            "The join_condition.ignore_nonexistent_tables "
            "parameter is deprecated"
        ):
            join_cond = sql_util.join_condition(
                t1, t2, ignore_nonexistent_tables=True
            )

        t1t2 = t1.join(t2)

        assert t1t2.onclause.compare(join_cond)

    def test_select_autocommit(self):
        with testing.expect_deprecated(
            "The select.autocommit parameter is deprecated and "
            "will be removed in a future release."
        ):
            select([column("x")], autocommit=True)

    def test_select_for_update(self):
        with testing.expect_deprecated(
            "The select.for_update parameter is deprecated and "
            "will be removed in a future release."
        ):
            select([column("x")], for_update=True)

    @testing.provide_metadata
    def test_table_useexisting(self):
        meta = self.metadata

        Table("t", meta, Column("x", Integer))
        meta.create_all()

        with testing.expect_deprecated(
            "The Table.useexisting parameter is deprecated and "
            "will be removed in a future release."
        ):
            Table("t", meta, useexisting=True, autoload_with=testing.db)

        with testing.expect_deprecated(
            "The Table.useexisting parameter is deprecated and "
            "will be removed in a future release."
        ):
            assert_raises_message(
                exc.ArgumentError,
                "useexisting is synonymous with extend_existing.",
                Table,
                "t",
                meta,
                useexisting=True,
                extend_existing=True,
                autoload_with=testing.db,
            )


class CaseSensitiveFunctionDeprecationsTest(fixtures.TestBase):
    def setup(self):
        self._registry = deepcopy(functions._registry)
        self._case_sensitive_registry = deepcopy(
            functions._case_sensitive_registry
        )
        functions._registry.clear()
        functions._case_sensitive_registry.clear()

    def teardown(self):
        functions._registry = self._registry
        functions._case_sensitive_registry = self._case_sensitive_registry

    def test_case_sensitive(self):
        reg = functions._registry["_default"]
        cs_reg = functions._case_sensitive_registry["_default"]

        class MYFUNC(GenericFunction):
            type = DateTime

        assert isinstance(func.MYFUNC().type, DateTime)
        assert isinstance(func.MyFunc().type, DateTime)
        assert isinstance(func.mYfUnC().type, DateTime)
        assert isinstance(func.myfunc().type, DateTime)

        in_("myfunc", reg)
        not_in("MYFUNC", reg)
        not_in("MyFunc", reg)
        in_("myfunc", cs_reg)
        eq_(set(cs_reg["myfunc"].keys()), set(["MYFUNC"]))

        with testing.expect_deprecated(
            "GenericFunction 'MyFunc' is already registered with"
            " different letter case, so the previously registered function "
            "'MYFUNC' is switched into case-sensitive mode. "
            "GenericFunction objects will be fully case-insensitive in a "
            "future release.",
            regex=False,
        ):

            class MyFunc(GenericFunction):
                type = Integer

        assert isinstance(func.MYFUNC().type, DateTime)
        assert isinstance(func.MyFunc().type, Integer)
        with pytest.raises(AssertionError):
            assert isinstance(func.mYfUnC().type, Integer)
        with pytest.raises(AssertionError):
            assert isinstance(func.myfunc().type, Integer)

        eq_(reg["myfunc"], functions._CASE_SENSITIVE)
        not_in("MYFUNC", reg)
        not_in("MyFunc", reg)
        in_("myfunc", cs_reg)
        eq_(set(cs_reg["myfunc"].keys()), set(["MYFUNC", "MyFunc"]))

    def test_replace_function_case_sensitive(self):
        reg = functions._registry["_default"]
        cs_reg = functions._case_sensitive_registry["_default"]

        class replaceable_func(GenericFunction):
            type = Integer
            identifier = "REPLACEABLE_FUNC"

        assert isinstance(func.REPLACEABLE_FUNC().type, Integer)
        assert isinstance(func.Replaceable_Func().type, Integer)
        assert isinstance(func.RePlAcEaBlE_fUnC().type, Integer)
        assert isinstance(func.replaceable_func().type, Integer)

        in_("replaceable_func", reg)
        not_in("REPLACEABLE_FUNC", reg)
        not_in("Replaceable_Func", reg)
        in_("replaceable_func", cs_reg)
        eq_(set(cs_reg["replaceable_func"].keys()), set(["REPLACEABLE_FUNC"]))

        with testing.expect_deprecated(
            "GenericFunction 'Replaceable_Func' is already registered with"
            " different letter case, so the previously registered function "
            "'REPLACEABLE_FUNC' is switched into case-sensitive mode. "
            "GenericFunction objects will be fully case-insensitive in a "
            "future release.",
            regex=False,
        ):

            class Replaceable_Func(GenericFunction):
                type = DateTime
                identifier = "Replaceable_Func"

        assert isinstance(func.REPLACEABLE_FUNC().type, Integer)
        assert isinstance(func.Replaceable_Func().type, DateTime)
        assert isinstance(func.RePlAcEaBlE_fUnC().type, NullType)
        assert isinstance(func.replaceable_func().type, NullType)

        eq_(reg["replaceable_func"], functions._CASE_SENSITIVE)
        not_in("REPLACEABLE_FUNC", reg)
        not_in("Replaceable_Func", reg)
        in_("replaceable_func", cs_reg)
        eq_(
            set(cs_reg["replaceable_func"].keys()),
            set(["REPLACEABLE_FUNC", "Replaceable_Func"]),
        )

        with testing.expect_warnings(
            "The GenericFunction 'REPLACEABLE_FUNC' is already registered and "
            "is going to be overriden.",
            regex=False,
        ):

            class replaceable_func_override(GenericFunction):
                type = DateTime
                identifier = "REPLACEABLE_FUNC"

        with testing.expect_deprecated(
            "GenericFunction(s) '['REPLACEABLE_FUNC', 'Replaceable_Func']' "
            "are already registered with different letter cases and might "
            "interact with 'replaceable_func'. GenericFunction objects will "
            "be fully case-insensitive in a future release.",
            regex=False,
        ):

            class replaceable_func_lowercase(GenericFunction):
                type = String
                identifier = "replaceable_func"

        with testing.expect_warnings(
            "The GenericFunction 'Replaceable_Func' is already registered and "
            "is going to be overriden.",
            regex=False,
        ):

            class Replaceable_Func_override(GenericFunction):
                type = Integer
                identifier = "Replaceable_Func"

        assert isinstance(func.REPLACEABLE_FUNC().type, DateTime)
        assert isinstance(func.Replaceable_Func().type, Integer)
        assert isinstance(func.RePlAcEaBlE_fUnC().type, NullType)
        assert isinstance(func.replaceable_func().type, String)

        eq_(reg["replaceable_func"], functions._CASE_SENSITIVE)
        not_in("REPLACEABLE_FUNC", reg)
        not_in("Replaceable_Func", reg)
        in_("replaceable_func", cs_reg)
        eq_(
            set(cs_reg["replaceable_func"].keys()),
            set(["REPLACEABLE_FUNC", "Replaceable_Func", "replaceable_func"]),
        )


class DDLListenerDeprecationsTest(fixtures.TestBase):
    def setup(self):
        self.bind = self.engine = engines.mock_engine()
        self.metadata = MetaData(self.bind)
        self.table = Table("t", self.metadata, Column("id", Integer))
        self.users = Table(
            "users",
            self.metadata,
            Column("user_id", Integer, primary_key=True),
            Column("user_name", String(40)),
        )

    def test_append_listener(self):
        metadata, table = self.metadata, self.table

        def fn(*a):
            return None

        with testing.expect_deprecated(".* is deprecated .*"):
            table.append_ddl_listener("before-create", fn)
        with testing.expect_deprecated(".* is deprecated .*"):
            assert_raises(
                exc.InvalidRequestError, table.append_ddl_listener, "blah", fn
            )

        with testing.expect_deprecated(".* is deprecated .*"):
            metadata.append_ddl_listener("before-create", fn)
        with testing.expect_deprecated(".* is deprecated .*"):
            assert_raises(
                exc.InvalidRequestError,
                metadata.append_ddl_listener,
                "blah",
                fn,
            )

    def test_deprecated_append_ddl_listener_table(self):
        metadata, users, engine = self.metadata, self.users, self.engine
        canary = []
        with testing.expect_deprecated(".* is deprecated .*"):
            users.append_ddl_listener(
                "before-create", lambda e, t, b: canary.append("mxyzptlk")
            )
        with testing.expect_deprecated(".* is deprecated .*"):
            users.append_ddl_listener(
                "after-create", lambda e, t, b: canary.append("klptzyxm")
            )
        with testing.expect_deprecated(".* is deprecated .*"):
            users.append_ddl_listener(
                "before-drop", lambda e, t, b: canary.append("xyzzy")
            )
        with testing.expect_deprecated(".* is deprecated .*"):
            users.append_ddl_listener(
                "after-drop", lambda e, t, b: canary.append("fnord")
            )

        metadata.create_all()
        assert "mxyzptlk" in canary
        assert "klptzyxm" in canary
        assert "xyzzy" not in canary
        assert "fnord" not in canary
        del engine.mock[:]
        canary[:] = []
        metadata.drop_all()
        assert "mxyzptlk" not in canary
        assert "klptzyxm" not in canary
        assert "xyzzy" in canary
        assert "fnord" in canary

    def test_deprecated_append_ddl_listener_metadata(self):
        metadata, engine = self.metadata, self.engine
        canary = []
        with testing.expect_deprecated(".* is deprecated .*"):
            metadata.append_ddl_listener(
                "before-create",
                lambda e, t, b, tables=None: canary.append("mxyzptlk"),
            )
        with testing.expect_deprecated(".* is deprecated .*"):
            metadata.append_ddl_listener(
                "after-create",
                lambda e, t, b, tables=None: canary.append("klptzyxm"),
            )
        with testing.expect_deprecated(".* is deprecated .*"):
            metadata.append_ddl_listener(
                "before-drop",
                lambda e, t, b, tables=None: canary.append("xyzzy"),
            )
        with testing.expect_deprecated(".* is deprecated .*"):
            metadata.append_ddl_listener(
                "after-drop",
                lambda e, t, b, tables=None: canary.append("fnord"),
            )

        metadata.create_all()
        assert "mxyzptlk" in canary
        assert "klptzyxm" in canary
        assert "xyzzy" not in canary
        assert "fnord" not in canary
        del engine.mock[:]
        canary[:] = []
        metadata.drop_all()
        assert "mxyzptlk" not in canary
        assert "klptzyxm" not in canary
        assert "xyzzy" in canary
        assert "fnord" in canary

    def test_filter_deprecated(self):
        cx = self.engine

        tbl = Table("t", MetaData(), Column("id", Integer))
        target = cx.name

        assert DDL("")._should_execute_deprecated("x", tbl, cx)
        with testing.expect_deprecated(".* is deprecated .*"):
            assert DDL("", on=target)._should_execute_deprecated("x", tbl, cx)
        with testing.expect_deprecated(".* is deprecated .*"):
            assert not DDL("", on="bogus")._should_execute_deprecated(
                "x", tbl, cx
            )
        with testing.expect_deprecated(".* is deprecated .*"):
            assert DDL(
                "", on=lambda d, x, y, z: True
            )._should_execute_deprecated("x", tbl, cx)
        with testing.expect_deprecated(".* is deprecated .*"):
            assert DDL(
                "", on=lambda d, x, y, z: z.engine.name != "bogus"
            )._should_execute_deprecated("x", tbl, cx)


class ConvertUnicodeDeprecationTest(fixtures.TestBase):

    __backend__ = True

    data = util.u(
        "Alors vous imaginez ma surprise, au lever du jour, quand "
        "une drôle de petite voix m’a réveillé. "
        "Elle disait: « S’il vous plaît… dessine-moi un mouton! »"
    )

    def test_unicode_warnings_dialectlevel(self):

        unicodedata = self.data

        with testing.expect_deprecated(
            "The create_engine.convert_unicode parameter and "
            "corresponding dialect-level"
        ):
            dialect = default.DefaultDialect(convert_unicode=True)
        dialect.supports_unicode_binds = False

        s = String()
        uni = s.dialect_impl(dialect).bind_processor(dialect)

        uni(util.b("x"))
        assert isinstance(uni(unicodedata), util.binary_type)

        eq_(uni(unicodedata), unicodedata.encode("utf-8"))

    def test_ignoring_unicode_error(self):
        """checks String(unicode_error='ignore') is passed to
        underlying codec."""

        unicodedata = self.data

        with testing.expect_deprecated(
            "The String.convert_unicode parameter is deprecated and "
            "will be removed in a future release.",
            "The String.unicode_errors parameter is deprecated and "
            "will be removed in a future release.",
        ):
            type_ = String(
                248, convert_unicode="force", unicode_error="ignore"
            )
        dialect = default.DefaultDialect(encoding="ascii")
        proc = type_.result_processor(dialect, 10)

        utfdata = unicodedata.encode("utf8")
        eq_(proc(utfdata), unicodedata.encode("ascii", "ignore").decode())


class ForUpdateTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = "default"

    def _assert_legacy(self, leg, read=False, nowait=False):
        t = table("t", column("c"))

        with testing.expect_deprecated(
            "The select.for_update parameter is deprecated and "
            "will be removed in a future release."
        ):
            s1 = select([t], for_update=leg)

        if leg is False:
            assert s1._for_update_arg is None
            assert s1.for_update is None
        else:
            eq_(s1._for_update_arg.read, read)
            eq_(s1._for_update_arg.nowait, nowait)
            eq_(s1.for_update, leg)

    def test_false_legacy(self):
        self._assert_legacy(False)

    def test_plain_true_legacy(self):
        self._assert_legacy(True)

    def test_read_legacy(self):
        self._assert_legacy("read", read=True)

    def test_nowait_legacy(self):
        self._assert_legacy("nowait", nowait=True)

    def test_read_nowait_legacy(self):
        self._assert_legacy("read_nowait", read=True, nowait=True)

    def test_unknown_mode(self):
        t = table("t", column("c"))

        with testing.expect_deprecated(
            "The select.for_update parameter is deprecated and "
            "will be removed in a future release."
        ):
            assert_raises_message(
                exc.ArgumentError,
                "Unknown for_update argument: 'unknown_mode'",
                t.select,
                t.c.c == 7,
                for_update="unknown_mode",
            )

    def test_legacy_setter(self):
        t = table("t", column("c"))
        s = select([t])
        s.for_update = "nowait"
        eq_(s._for_update_arg.nowait, True)


class TextTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = "default"

    def test_legacy_bindparam(self):
        with testing.expect_deprecated(
            "The text.bindparams parameter is deprecated"
        ):
            t = text(
                "select * from foo where lala=:bar and hoho=:whee",
                bindparams=[bindparam("bar", 4), bindparam("whee", 7)],
            )

        self.assert_compile(
            t,
            "select * from foo where lala=:bar and hoho=:whee",
            checkparams={"bar": 4, "whee": 7},
        )

    def test_legacy_typemap(self):
        table1 = table(
            "mytable",
            column("myid", Integer),
            column("name", String),
            column("description", String),
        )
        with testing.expect_deprecated(
            "The text.typemap parameter is deprecated"
        ):
            t = text(
                "select id, name from user",
                typemap=dict(id=Integer, name=String),
            )

        stmt = select([table1.c.myid]).select_from(
            table1.join(t, table1.c.myid == t.c.id)
        )
        compiled = stmt.compile()
        eq_(
            compiled._create_result_map(),
            {
                "myid": (
                    "myid",
                    (table1.c.myid, "myid", "myid"),
                    table1.c.myid.type,
                )
            },
        )

    def test_autocommit(self):
        with testing.expect_deprecated(
            "The text.autocommit parameter is deprecated"
        ):
            text("select id, name from user", autocommit=True)
