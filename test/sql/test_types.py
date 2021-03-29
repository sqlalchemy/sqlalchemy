# coding: utf-8
import datetime
import decimal
import importlib
import operator
import os

import sqlalchemy as sa
from sqlalchemy import and_
from sqlalchemy import ARRAY
from sqlalchemy import BigInteger
from sqlalchemy import bindparam
from sqlalchemy import BLOB
from sqlalchemy import BOOLEAN
from sqlalchemy import Boolean
from sqlalchemy import cast
from sqlalchemy import CHAR
from sqlalchemy import CLOB
from sqlalchemy import DATE
from sqlalchemy import Date
from sqlalchemy import DATETIME
from sqlalchemy import DateTime
from sqlalchemy import DECIMAL
from sqlalchemy import dialects
from sqlalchemy import distinct
from sqlalchemy import Enum
from sqlalchemy import exc
from sqlalchemy import FLOAT
from sqlalchemy import Float
from sqlalchemy import func
from sqlalchemy import inspection
from sqlalchemy import INTEGER
from sqlalchemy import Integer
from sqlalchemy import Interval
from sqlalchemy import JSON
from sqlalchemy import LargeBinary
from sqlalchemy import literal
from sqlalchemy import MetaData
from sqlalchemy import NCHAR
from sqlalchemy import NUMERIC
from sqlalchemy import Numeric
from sqlalchemy import NVARCHAR
from sqlalchemy import PickleType
from sqlalchemy import REAL
from sqlalchemy import select
from sqlalchemy import SMALLINT
from sqlalchemy import SmallInteger
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy import Text
from sqlalchemy import text
from sqlalchemy import TIME
from sqlalchemy import Time
from sqlalchemy import TIMESTAMP
from sqlalchemy import type_coerce
from sqlalchemy import TypeDecorator
from sqlalchemy import types
from sqlalchemy import Unicode
from sqlalchemy import util
from sqlalchemy import VARCHAR
import sqlalchemy.dialects.mysql as mysql
import sqlalchemy.dialects.oracle as oracle
import sqlalchemy.dialects.postgresql as pg
from sqlalchemy.engine import default
from sqlalchemy.schema import AddConstraint
from sqlalchemy.schema import CheckConstraint
from sqlalchemy.sql import column
from sqlalchemy.sql import ddl
from sqlalchemy.sql import elements
from sqlalchemy.sql import null
from sqlalchemy.sql import operators
from sqlalchemy.sql import sqltypes
from sqlalchemy.sql import table
from sqlalchemy.sql import visitors
from sqlalchemy.sql.sqltypes import TypeEngine
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import AssertsExecutionResults
from sqlalchemy.testing import engines
from sqlalchemy.testing import eq_
from sqlalchemy.testing import expect_deprecated_20
from sqlalchemy.testing import expect_warnings
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.testing import is_not
from sqlalchemy.testing import mock
from sqlalchemy.testing import pickleable
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import pep435_enum
from sqlalchemy.testing.schema import Table
from sqlalchemy.testing.util import picklers
from sqlalchemy.testing.util import round_decimal
from sqlalchemy.util import u


def _all_dialect_modules():
    return [
        importlib.import_module("sqlalchemy.dialects.%s" % d)
        for d in dialects.__all__
        if not d.startswith("_")
    ]


def _all_dialects():
    return [d.base.dialect() for d in _all_dialect_modules()]


def _types_for_mod(mod):
    for key in dir(mod):
        typ = getattr(mod, key)
        if not isinstance(typ, type) or not issubclass(typ, types.TypeEngine):
            continue
        yield typ


def _all_types(omit_special_types=False):
    seen = set()
    for typ in _types_for_mod(types):
        if omit_special_types and typ in (
            types.TypeDecorator,
            types.TypeEngine,
            types.Variant,
        ):
            continue

        if typ in seen:
            continue
        seen.add(typ)
        yield typ
    for dialect in _all_dialect_modules():
        for typ in _types_for_mod(dialect):
            if typ in seen:
                continue
            seen.add(typ)
            yield typ


class AdaptTest(fixtures.TestBase):
    @testing.combinations(((t,) for t in _types_for_mod(types)), id_="n")
    def test_uppercase_importable(self, typ):
        if typ.__name__ == typ.__name__.upper():
            assert getattr(sa, typ.__name__) is typ
            assert typ.__name__ in types.__all__

    @testing.combinations(
        ((d.name, d) for d in _all_dialects()), argnames="dialect", id_="ia"
    )
    @testing.combinations(
        (REAL(), "REAL"),
        (FLOAT(), "FLOAT"),
        (NUMERIC(), "NUMERIC"),
        (DECIMAL(), "DECIMAL"),
        (INTEGER(), "INTEGER"),
        (SMALLINT(), "SMALLINT"),
        (TIMESTAMP(), ("TIMESTAMP", "TIMESTAMP WITHOUT TIME ZONE")),
        (DATETIME(), "DATETIME"),
        (DATE(), "DATE"),
        (TIME(), ("TIME", "TIME WITHOUT TIME ZONE")),
        (CLOB(), "CLOB"),
        (VARCHAR(10), ("VARCHAR(10)", "VARCHAR(10 CHAR)")),
        (
            NVARCHAR(10),
            ("NVARCHAR(10)", "NATIONAL VARCHAR(10)", "NVARCHAR2(10)"),
        ),
        (CHAR(), "CHAR"),
        (NCHAR(), ("NCHAR", "NATIONAL CHAR")),
        (BLOB(), ("BLOB", "BLOB SUB_TYPE 0")),
        (BOOLEAN(), ("BOOLEAN", "BOOL", "INTEGER")),
        argnames="type_, expected",
        id_="ra",
    )
    def test_uppercase_rendering(self, dialect, type_, expected):
        """Test that uppercase types from types.py always render as their
        type.

        As of SQLA 0.6, using an uppercase type means you want specifically
        that type. If the database in use doesn't support that DDL, it (the DB
        backend) should raise an error - it means you should be using a
        lowercased (genericized) type.

        """

        if isinstance(expected, str):
            expected = (expected,)

        try:
            compiled = type_.compile(dialect=dialect)
        except NotImplementedError:
            return

        assert compiled in expected, "%r matches none of %r for dialect %s" % (
            compiled,
            expected,
            dialect.name,
        )

        assert (
            str(types.to_instance(type_)) in expected
        ), "default str() of type %r not expected, %r" % (type_, expected)

    def _adaptions():
        for typ in _all_types(omit_special_types=True):

            # up adapt from LowerCase to UPPERCASE,
            # as well as to all non-sqltypes
            up_adaptions = [typ] + typ.__subclasses__()
            yield "%s.%s" % (
                typ.__module__,
                typ.__name__,
            ), False, typ, up_adaptions
            for subcl in typ.__subclasses__():
                if (
                    subcl is not typ
                    and typ is not TypeDecorator
                    and "sqlalchemy" in subcl.__module__
                ):
                    yield "%s.%s" % (
                        subcl.__module__,
                        subcl.__name__,
                    ), True, subcl, [typ]

    @testing.combinations(_adaptions(), id_="iaaa")
    def test_adapt_method(self, is_down_adaption, typ, target_adaptions):
        """ensure all types have a working adapt() method,
        which creates a distinct copy.

        The distinct copy ensures that when we cache
        the adapted() form of a type against the original
        in a weak key dictionary, a cycle is not formed.

        This test doesn't test type-specific arguments of
        adapt() beyond their defaults.

        """

        if issubclass(typ, ARRAY):
            t1 = typ(String)
        else:
            t1 = typ()
        for cls in target_adaptions:
            if (is_down_adaption and issubclass(typ, sqltypes.Emulated)) or (
                not is_down_adaption and issubclass(cls, sqltypes.Emulated)
            ):
                continue

            # print("ADAPT %s -> %s" % (t1.__class__, cls))
            t2 = t1.adapt(cls)
            assert t1 is not t2

            if is_down_adaption:
                t2, t1 = t1, t2

            for k in t1.__dict__:
                if k in (
                    "impl",
                    "_is_oracle_number",
                    "_create_events",
                    "create_constraint",
                    "inherit_schema",
                    "schema",
                    "metadata",
                    "name",
                ):
                    continue
                # assert each value was copied, or that
                # the adapted type has a more specific
                # value than the original (i.e. SQL Server
                # applies precision=24 for REAL)
                assert (
                    getattr(t2, k) == t1.__dict__[k] or t1.__dict__[k] is None
                )

        eq_(t1.evaluates_none().should_evaluate_none, True)

    def test_python_type(self):
        eq_(types.Integer().python_type, int)
        eq_(types.Numeric().python_type, decimal.Decimal)
        eq_(types.Numeric(asdecimal=False).python_type, float)
        eq_(types.LargeBinary().python_type, util.binary_type)
        eq_(types.Float().python_type, float)
        eq_(types.Interval().python_type, datetime.timedelta)
        eq_(types.Date().python_type, datetime.date)
        eq_(types.DateTime().python_type, datetime.datetime)
        eq_(types.String().python_type, str)
        eq_(types.Unicode().python_type, util.text_type)
        eq_(types.Enum("one", "two", "three").python_type, str)

        assert_raises(
            NotImplementedError, lambda: types.TypeEngine().python_type
        )

    @testing.uses_deprecated()
    @testing.combinations(*[(t,) for t in _all_types(omit_special_types=True)])
    def test_repr(self, typ):
        if issubclass(typ, ARRAY):
            t1 = typ(String)
        else:
            t1 = typ()
        repr(t1)

    @testing.uses_deprecated()
    @testing.combinations(*[(t,) for t in _all_types(omit_special_types=True)])
    def test_str(self, typ):
        if issubclass(typ, ARRAY):
            t1 = typ(String)
        else:
            t1 = typ()
        str(t1)

    def test_str_third_party(self):
        class TINYINT(types.TypeEngine):
            __visit_name__ = "TINYINT"

        eq_(str(TINYINT()), "TINYINT")

    def test_str_third_party_uppercase_no_visit_name(self):
        class TINYINT(types.TypeEngine):
            pass

        eq_(str(TINYINT()), "TINYINT")

    def test_str_third_party_camelcase_no_visit_name(self):
        class TinyInt(types.TypeEngine):
            pass

        eq_(str(TinyInt()), "TinyInt()")

    def test_adapt_constructor_copy_override_kw(self):
        """test that adapt() can accept kw args that override
        the state of the original object.

        This essentially is testing the behavior of util.constructor_copy().

        """
        t1 = String(length=50)
        t2 = t1.adapt(Text)
        eq_(t2.length, 50)

    def test_convert_unicode_text_type(self):
        with testing.expect_deprecated(
            "The String.convert_unicode parameter is deprecated"
        ):
            eq_(types.String(convert_unicode=True).python_type, util.text_type)


class TypeAffinityTest(fixtures.TestBase):
    @testing.combinations(
        (String(), String),
        (VARCHAR(), String),
        (Date(), Date),
        (LargeBinary(), types._Binary),
        id_="rn",
    )
    def test_type_affinity(self, type_, affin):
        eq_(type_._type_affinity, affin)

    @testing.combinations(
        (Integer(), SmallInteger(), True),
        (Integer(), String(), False),
        (Integer(), Integer(), True),
        (Text(), String(), True),
        (Text(), Unicode(), True),
        (LargeBinary(), Integer(), False),
        (LargeBinary(), PickleType(), True),
        (PickleType(), LargeBinary(), True),
        (PickleType(), PickleType(), True),
        id_="rra",
    )
    def test_compare_type_affinity(self, t1, t2, comp):
        eq_(t1._compare_type_affinity(t2), comp, "%s %s" % (t1, t2))

    def test_decorator_doesnt_cache(self):
        from sqlalchemy.dialects import postgresql

        class MyType(TypeDecorator):
            impl = CHAR

            def load_dialect_impl(self, dialect):
                if dialect.name == "postgresql":
                    return dialect.type_descriptor(postgresql.UUID())
                else:
                    return dialect.type_descriptor(CHAR(32))

        t1 = MyType()
        d = postgresql.dialect()
        assert t1._type_affinity is String
        assert t1.dialect_impl(d)._type_affinity is postgresql.UUID


class AsGenericTest(fixtures.TestBase):
    @testing.combinations(
        (String(), String()),
        (VARCHAR(length=100), String(length=100)),
        (NVARCHAR(length=100), Unicode(length=100)),
        (DATE(), Date()),
        (pg.JSON(), sa.JSON()),
        (pg.ARRAY(sa.String), sa.ARRAY(sa.String)),
        (Enum("a", "b", "c"), Enum("a", "b", "c")),
        (pg.ENUM("a", "b", "c"), Enum("a", "b", "c")),
        (mysql.ENUM("a", "b", "c"), Enum("a", "b", "c")),
        (pg.INTERVAL(precision=5), Interval(native=True, second_precision=5)),
        (
            oracle.INTERVAL(second_precision=5, day_precision=5),
            Interval(native=True, day_precision=5, second_precision=5),
        ),
    )
    def test_as_generic(self, t1, t2):
        assert repr(t1.as_generic(allow_nulltype=False)) == repr(t2)

    @testing.combinations(
        *[
            (t,)
            for t in _all_types(omit_special_types=True)
            if not util.method_is_overridden(t, TypeEngine.as_generic)
        ]
    )
    def test_as_generic_all_types_heuristic(self, type_):
        if issubclass(type_, ARRAY):
            t1 = type_(String)
        else:
            t1 = type_()

        try:
            gentype = t1.as_generic()
        except NotImplementedError:
            pass
        else:
            assert isinstance(t1, gentype.__class__)
            assert isinstance(gentype, TypeEngine)

        gentype = t1.as_generic(allow_nulltype=True)
        if not isinstance(gentype, types.NULLTYPE.__class__):
            assert isinstance(t1, gentype.__class__)
            assert isinstance(gentype, TypeEngine)

    @testing.combinations(
        *[
            (t,)
            for t in _all_types(omit_special_types=True)
            if util.method_is_overridden(t, TypeEngine.as_generic)
        ]
    )
    def test_as_generic_all_types_custom(self, type_):
        if issubclass(type_, ARRAY):
            t1 = type_(String)
        else:
            t1 = type_()

        gentype = t1.as_generic(allow_nulltype=False)
        assert isinstance(gentype, TypeEngine)


class PickleTypesTest(fixtures.TestBase):
    @testing.combinations(
        ("Boo", Boolean()),
        ("Str", String()),
        ("Tex", Text()),
        ("Uni", Unicode()),
        ("Int", Integer()),
        ("Sma", SmallInteger()),
        ("Big", BigInteger()),
        ("Num", Numeric()),
        ("Flo", Float()),
        ("Dat", DateTime()),
        ("Dat", Date()),
        ("Tim", Time()),
        ("Lar", LargeBinary()),
        ("Pic", PickleType()),
        ("Int", Interval()),
        id_="ar",
    )
    def test_pickle_types(self, name, type_):
        column_type = Column(name, type_)
        meta = MetaData()
        Table("foo", meta, column_type)

        for loads, dumps in picklers():
            loads(dumps(column_type))
            loads(dumps(meta))


class _UserDefinedTypeFixture(object):
    @classmethod
    def define_tables(cls, metadata):
        class MyType(types.UserDefinedType):
            def get_col_spec(self):
                return "VARCHAR(100)"

            def bind_processor(self, dialect):
                def process(value):
                    return "BIND_IN" + value

                return process

            def result_processor(self, dialect, coltype):
                def process(value):
                    return value + "BIND_OUT"

                return process

            def adapt(self, typeobj):
                return typeobj()

        class MyDecoratedType(types.TypeDecorator):
            impl = String

            def bind_processor(self, dialect):
                impl_processor = super(MyDecoratedType, self).bind_processor(
                    dialect
                ) or (lambda value: value)

                def process(value):
                    return "BIND_IN" + impl_processor(value)

                return process

            def result_processor(self, dialect, coltype):
                impl_processor = super(MyDecoratedType, self).result_processor(
                    dialect, coltype
                ) or (lambda value: value)

                def process(value):
                    return impl_processor(value) + "BIND_OUT"

                return process

            def copy(self):
                return MyDecoratedType()

        class MyNewUnicodeType(types.TypeDecorator):
            impl = Unicode

            def process_bind_param(self, value, dialect):
                return "BIND_IN" + value

            def process_result_value(self, value, dialect):
                return value + "BIND_OUT"

            def copy(self):
                return MyNewUnicodeType(self.impl.length)

        class MyNewIntType(types.TypeDecorator):
            impl = Integer

            def process_bind_param(self, value, dialect):
                return value * 10

            def process_result_value(self, value, dialect):
                return value * 10

            def copy(self):
                return MyNewIntType()

        class MyNewIntSubClass(MyNewIntType):
            def process_result_value(self, value, dialect):
                return value * 15

            def copy(self):
                return MyNewIntSubClass()

        class MyUnicodeType(types.TypeDecorator):
            impl = Unicode

            def bind_processor(self, dialect):
                impl_processor = super(MyUnicodeType, self).bind_processor(
                    dialect
                ) or (lambda value: value)

                def process(value):
                    return "BIND_IN" + impl_processor(value)

                return process

            def result_processor(self, dialect, coltype):
                impl_processor = super(MyUnicodeType, self).result_processor(
                    dialect, coltype
                ) or (lambda value: value)

                def process(value):
                    return impl_processor(value) + "BIND_OUT"

                return process

            def copy(self):
                return MyUnicodeType(self.impl.length)

        class MyDecOfDec(types.TypeDecorator):
            impl = MyNewIntType

        Table(
            "users",
            metadata,
            Column("user_id", Integer, primary_key=True),
            # totall custom type
            Column("goofy", MyType, nullable=False),
            # decorated type with an argument, so its a String
            Column("goofy2", MyDecoratedType(50), nullable=False),
            Column("goofy4", MyUnicodeType(50), nullable=False),
            Column("goofy7", MyNewUnicodeType(50), nullable=False),
            Column("goofy8", MyNewIntType, nullable=False),
            Column("goofy9", MyNewIntSubClass, nullable=False),
            Column("goofy10", MyDecOfDec, nullable=False),
        )


class UserDefinedRoundTripTest(_UserDefinedTypeFixture, fixtures.TablesTest):
    __backend__ = True

    def _data_fixture(self, connection):
        users = self.tables.users
        connection.execute(
            users.insert(),
            dict(
                user_id=2,
                goofy="jack",
                goofy2="jack",
                goofy4=util.u("jack"),
                goofy7=util.u("jack"),
                goofy8=12,
                goofy9=12,
                goofy10=12,
            ),
        )
        connection.execute(
            users.insert(),
            dict(
                user_id=3,
                goofy="lala",
                goofy2="lala",
                goofy4=util.u("lala"),
                goofy7=util.u("lala"),
                goofy8=15,
                goofy9=15,
                goofy10=15,
            ),
        )
        connection.execute(
            users.insert(),
            dict(
                user_id=4,
                goofy="fred",
                goofy2="fred",
                goofy4=util.u("fred"),
                goofy7=util.u("fred"),
                goofy8=9,
                goofy9=9,
                goofy10=9,
            ),
        )

    def test_processing(self, connection):
        users = self.tables.users
        self._data_fixture(connection)

        result = connection.execute(
            users.select().order_by(users.c.user_id)
        ).fetchall()
        for assertstr, assertint, assertint2, row in zip(
            [
                "BIND_INjackBIND_OUT",
                "BIND_INlalaBIND_OUT",
                "BIND_INfredBIND_OUT",
            ],
            [1200, 1500, 900],
            [1800, 2250, 1350],
            result,
        ):
            for col in list(row)[1:5]:
                eq_(col, assertstr)
            eq_(row[5], assertint)
            eq_(row[6], assertint2)
            for col in row[3], row[4]:
                assert isinstance(col, util.text_type)

    def test_plain_in_typedec(self, connection):
        users = self.tables.users
        self._data_fixture(connection)

        stmt = (
            select(users.c.user_id, users.c.goofy8)
            .where(users.c.goofy8.in_([15, 9]))
            .order_by(users.c.user_id)
        )
        result = connection.execute(stmt, {"goofy": [15, 9]})
        eq_(result.fetchall(), [(3, 1500), (4, 900)])

    def test_plain_in_typedec_of_typedec(self, connection):
        users = self.tables.users
        self._data_fixture(connection)

        stmt = (
            select(users.c.user_id, users.c.goofy10)
            .where(users.c.goofy10.in_([15, 9]))
            .order_by(users.c.user_id)
        )
        result = connection.execute(stmt, {"goofy": [15, 9]})
        eq_(result.fetchall(), [(3, 1500), (4, 900)])

    def test_expanding_in_typedec(self, connection):
        users = self.tables.users
        self._data_fixture(connection)

        stmt = (
            select(users.c.user_id, users.c.goofy8)
            .where(users.c.goofy8.in_(bindparam("goofy", expanding=True)))
            .order_by(users.c.user_id)
        )
        result = connection.execute(stmt, {"goofy": [15, 9]})
        eq_(result.fetchall(), [(3, 1500), (4, 900)])

    def test_expanding_in_typedec_of_typedec(self, connection):
        users = self.tables.users
        self._data_fixture(connection)

        stmt = (
            select(users.c.user_id, users.c.goofy10)
            .where(users.c.goofy10.in_(bindparam("goofy", expanding=True)))
            .order_by(users.c.user_id)
        )
        result = connection.execute(stmt, {"goofy": [15, 9]})
        eq_(result.fetchall(), [(3, 1500), (4, 900)])


class UserDefinedTest(
    _UserDefinedTypeFixture, fixtures.TablesTest, AssertsCompiledSQL
):

    run_create_tables = None
    run_inserts = None
    run_deletes = None

    """tests user-defined types."""

    def test_typedecorator_literal_render(self):
        class MyType(types.TypeDecorator):
            impl = String

            def process_literal_param(self, value, dialect):
                return "HI->%s<-THERE" % value

        self.assert_compile(
            select(literal("test", MyType)),
            "SELECT 'HI->test<-THERE' AS anon_1",
            dialect="default",
            literal_binds=True,
        )

    def test_kw_colspec(self):
        class MyType(types.UserDefinedType):
            def get_col_spec(self, **kw):
                return "FOOB %s" % kw["type_expression"].name

        class MyOtherType(types.UserDefinedType):
            def get_col_spec(self):
                return "BAR"

        t = Table("t", MetaData(), Column("bar", MyType, nullable=False))

        self.assert_compile(ddl.CreateColumn(t.c.bar), "bar FOOB bar NOT NULL")

        t = Table("t", MetaData(), Column("bar", MyOtherType, nullable=False))
        self.assert_compile(ddl.CreateColumn(t.c.bar), "bar BAR NOT NULL")

    def test_typedecorator_literal_render_fallback_bound(self):
        # fall back to process_bind_param for literal
        # value rendering.
        class MyType(types.TypeDecorator):
            impl = String

            def process_bind_param(self, value, dialect):
                return "HI->%s<-THERE" % value

        self.assert_compile(
            select(literal("test", MyType)),
            "SELECT 'HI->test<-THERE' AS anon_1",
            dialect="default",
            literal_binds=True,
        )

    def test_typedecorator_impl(self):
        for impl_, exp, kw in [
            (Float, "FLOAT", {}),
            (Float, "FLOAT(2)", {"precision": 2}),
            (Float(2), "FLOAT(2)", {"precision": 4}),
            (Numeric(19, 2), "NUMERIC(19, 2)", {}),
        ]:
            for dialect_ in (
                dialects.postgresql,
                dialects.mssql,
                dialects.mysql,
            ):
                dialect_ = dialect_.dialect()

                raw_impl = types.to_instance(impl_, **kw)

                class MyType(types.TypeDecorator):
                    impl = impl_

                dec_type = MyType(**kw)

                eq_(dec_type.impl.__class__, raw_impl.__class__)

                raw_dialect_impl = raw_impl.dialect_impl(dialect_)
                dec_dialect_impl = dec_type.dialect_impl(dialect_)
                eq_(dec_dialect_impl.__class__, MyType)
                eq_(
                    raw_dialect_impl.__class__, dec_dialect_impl.impl.__class__
                )

                self.assert_compile(MyType(**kw), exp, dialect=dialect_)

    def test_user_defined_typedec_impl(self):
        class MyType(types.TypeDecorator):
            impl = Float

            def load_dialect_impl(self, dialect):
                if dialect.name == "sqlite":
                    return String(50)
                else:
                    return super(MyType, self).load_dialect_impl(dialect)

        sl = dialects.sqlite.dialect()
        pg = dialects.postgresql.dialect()
        t = MyType()
        self.assert_compile(t, "VARCHAR(50)", dialect=sl)
        self.assert_compile(t, "FLOAT", dialect=pg)
        eq_(
            t.dialect_impl(dialect=sl).impl.__class__,
            String().dialect_impl(dialect=sl).__class__,
        )
        eq_(
            t.dialect_impl(dialect=pg).impl.__class__,
            Float().dialect_impl(pg).__class__,
        )

    @testing.combinations((Boolean,), (Enum,))
    def test_typedecorator_schematype_constraint(self, typ):
        class B(TypeDecorator):
            impl = typ

        t1 = Table("t1", MetaData(), Column("q", B(create_constraint=True)))
        eq_(
            len([c for c in t1.constraints if isinstance(c, CheckConstraint)]),
            1,
        )

    def test_type_decorator_repr(self):
        class MyType(TypeDecorator):
            impl = VARCHAR

        eq_(repr(MyType(45)), "MyType(length=45)")

    def test_user_defined_typedec_impl_bind(self):
        class TypeOne(types.TypeEngine):
            def bind_processor(self, dialect):
                def go(value):
                    return value + " ONE"

                return go

        class TypeTwo(types.TypeEngine):
            def bind_processor(self, dialect):
                def go(value):
                    return value + " TWO"

                return go

        class MyType(types.TypeDecorator):
            impl = TypeOne

            def load_dialect_impl(self, dialect):
                if dialect.name == "sqlite":
                    return TypeOne()
                else:
                    return TypeTwo()

            def process_bind_param(self, value, dialect):
                return "MYTYPE " + value

        sl = dialects.sqlite.dialect()
        pg = dialects.postgresql.dialect()
        t = MyType()
        eq_(t._cached_bind_processor(sl)("foo"), "MYTYPE foo ONE")
        eq_(t._cached_bind_processor(pg)("foo"), "MYTYPE foo TWO")

    def test_user_defined_dialect_specific_args(self):
        class MyType(types.UserDefinedType):
            def __init__(self, foo="foo", **kwargs):
                super(MyType, self).__init__()
                self.foo = foo
                self.dialect_specific_args = kwargs

            def adapt(self, cls):
                return cls(foo=self.foo, **self.dialect_specific_args)

        t = MyType(bar="bar")
        a = t.dialect_impl(testing.db.dialect)
        eq_(a.foo, "foo")
        eq_(a.dialect_specific_args["bar"], "bar")


class StringConvertUnicodeTest(fixtures.TestBase):
    @testing.combinations((Unicode,), (String,), argnames="datatype")
    @testing.combinations((True,), (False,), argnames="convert_unicode")
    @testing.combinations(
        (String.RETURNS_CONDITIONAL,),
        (String.RETURNS_BYTES,),
        (String.RETURNS_UNICODE),
        argnames="returns_unicode_strings",
    )
    def test_convert_unicode(
        self, datatype, convert_unicode, returns_unicode_strings
    ):
        s1 = datatype()
        dialect = mock.Mock(
            returns_unicode_strings=returns_unicode_strings,
            encoding="utf-8",
            convert_unicode=convert_unicode,
        )

        proc = s1.result_processor(dialect, None)

        string = u("méil")
        bytestring = string.encode("utf-8")

        if (
            datatype is Unicode or convert_unicode
        ) and returns_unicode_strings in (
            String.RETURNS_CONDITIONAL,
            String.RETURNS_BYTES,
        ):
            eq_(proc(bytestring), string)

            if returns_unicode_strings is String.RETURNS_CONDITIONAL:
                eq_(proc(string), string)
            else:
                if util.py3k:
                    # trying to decode a unicode
                    assert_raises(TypeError, proc, string)
                else:
                    assert_raises(UnicodeEncodeError, proc, string)
        else:
            is_(proc, None)


class TypeCoerceCastTest(fixtures.TablesTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        class MyType(types.TypeDecorator):
            impl = String(50)

            def process_bind_param(self, value, dialect):
                return "BIND_IN" + str(value)

            def process_result_value(self, value, dialect):
                return value + "BIND_OUT"

        cls.MyType = MyType

        Table("t", metadata, Column("data", String(50)))

    def test_insert_round_trip_cast(self, connection):
        self._test_insert_round_trip(cast, connection)

    def test_insert_round_trip_type_coerce(self, connection):
        self._test_insert_round_trip(type_coerce, connection)

    def _test_insert_round_trip(self, coerce_fn, conn):
        MyType = self.MyType
        t = self.tables.t

        conn.execute(t.insert().values(data=coerce_fn("d1", MyType)))

        eq_(
            conn.execute(select(coerce_fn(t.c.data, MyType))).fetchall(),
            [("BIND_INd1BIND_OUT",)],
        )

    def test_coerce_from_nulltype_cast(self, connection):
        self._test_coerce_from_nulltype(cast, connection)

    def test_coerce_from_nulltype_type_coerce(self, connection):
        self._test_coerce_from_nulltype(type_coerce, connection)

    def _test_coerce_from_nulltype(self, coerce_fn, conn):
        MyType = self.MyType

        # test coerce from nulltype - e.g. use an object that
        # does't match to a known type
        class MyObj(object):
            def __str__(self):
                return "THISISMYOBJ"

        t = self.tables.t

        conn.execute(t.insert().values(data=coerce_fn(MyObj(), MyType)))

        eq_(
            conn.execute(select(coerce_fn(t.c.data, MyType))).fetchall(),
            [("BIND_INTHISISMYOBJBIND_OUT",)],
        )

    def test_vs_non_coerced_cast(self, connection):
        self._test_vs_non_coerced(cast, connection)

    def test_vs_non_coerced_type_coerce(self, connection):
        self._test_vs_non_coerced(type_coerce, connection)

    def _test_vs_non_coerced(self, coerce_fn, conn):
        MyType = self.MyType
        t = self.tables.t

        conn.execute(t.insert().values(data=coerce_fn("d1", MyType)))

        eq_(
            conn.execute(
                select(t.c.data, coerce_fn(t.c.data, MyType))
            ).fetchall(),
            [("BIND_INd1", "BIND_INd1BIND_OUT")],
        )

    def test_vs_non_coerced_alias_cast(self, connection):
        self._test_vs_non_coerced_alias(cast, connection)

    def test_vs_non_coerced_alias_type_coerce(self, connection):
        self._test_vs_non_coerced_alias(type_coerce, connection)

    def _test_vs_non_coerced_alias(self, coerce_fn, conn):
        MyType = self.MyType
        t = self.tables.t

        conn.execute(t.insert().values(data=coerce_fn("d1", MyType)))

        eq_(
            conn.execute(
                select(t.c.data.label("x"), coerce_fn(t.c.data, MyType))
                .alias()
                .select()
            ).fetchall(),
            [("BIND_INd1", "BIND_INd1BIND_OUT")],
        )

    def test_vs_non_coerced_where_cast(self, connection):
        self._test_vs_non_coerced_where(cast, connection)

    def test_vs_non_coerced_where_type_coerce(self, connection):
        self._test_vs_non_coerced_where(type_coerce, connection)

    def _test_vs_non_coerced_where(self, coerce_fn, conn):
        MyType = self.MyType

        t = self.tables.t
        conn.execute(t.insert().values(data=coerce_fn("d1", MyType)))

        # coerce on left side
        eq_(
            conn.execute(
                select(t.c.data, coerce_fn(t.c.data, MyType)).where(
                    coerce_fn(t.c.data, MyType) == "d1"
                )
            ).fetchall(),
            [("BIND_INd1", "BIND_INd1BIND_OUT")],
        )

        # coerce on right side
        eq_(
            conn.execute(
                select(t.c.data, coerce_fn(t.c.data, MyType)).where(
                    t.c.data == coerce_fn("d1", MyType)
                )
            ).fetchall(),
            [("BIND_INd1", "BIND_INd1BIND_OUT")],
        )

    def test_coerce_none_cast(self, connection):
        self._test_coerce_none(cast, connection)

    def test_coerce_none_type_coerce(self, connection):
        self._test_coerce_none(type_coerce, connection)

    def _test_coerce_none(self, coerce_fn, conn):
        MyType = self.MyType

        t = self.tables.t
        conn.execute(t.insert().values(data=coerce_fn("d1", MyType)))
        eq_(
            conn.execute(
                select(t.c.data, coerce_fn(t.c.data, MyType)).where(
                    t.c.data == coerce_fn(None, MyType)
                )
            ).fetchall(),
            [],
        )

        eq_(
            conn.execute(
                select(t.c.data, coerce_fn(t.c.data, MyType)).where(
                    coerce_fn(t.c.data, MyType) == None
                )
            ).fetchall(),  # noqa
            [],
        )

    def test_resolve_clause_element_cast(self, connection):
        self._test_resolve_clause_element(cast, connection)

    def test_resolve_clause_element_type_coerce(self, connection):
        self._test_resolve_clause_element(type_coerce, connection)

    def _test_resolve_clause_element(self, coerce_fn, conn):
        MyType = self.MyType

        t = self.tables.t
        conn.execute(t.insert().values(data=coerce_fn("d1", MyType)))

        class MyFoob(object):
            def __clause_element__(self):
                return t.c.data

        eq_(
            conn.execute(
                select(t.c.data, coerce_fn(MyFoob(), MyType))
            ).fetchall(),
            [("BIND_INd1", "BIND_INd1BIND_OUT")],
        )

    def test_cast_replace_col_w_bind(self, connection):
        self._test_replace_col_w_bind(cast, connection)

    def test_type_coerce_replace_col_w_bind(self, connection):
        self._test_replace_col_w_bind(type_coerce, connection)

    def _test_replace_col_w_bind(self, coerce_fn, conn):
        MyType = self.MyType

        t = self.tables.t
        conn.execute(t.insert().values(data=coerce_fn("d1", MyType)))

        stmt = select(t.c.data, coerce_fn(t.c.data, MyType))

        def col_to_bind(col):
            if col is t.c.data:
                return bindparam(None, "x", type_=col.type, unique=True)
            return None

        # ensure we evaluate the expression so that we can see
        # the clone resets this info
        stmt.compile()

        new_stmt = visitors.replacement_traverse(stmt, {}, col_to_bind)

        # original statement
        eq_(
            conn.execute(stmt).fetchall(),
            [("BIND_INd1", "BIND_INd1BIND_OUT")],
        )

        # replaced with binds; CAST can't affect the bound parameter
        # on the way in here
        eq_(
            conn.execute(new_stmt).fetchall(),
            [("x", "BIND_INxBIND_OUT")]
            if coerce_fn is type_coerce
            else [("x", "xBIND_OUT")],
        )

    def test_cast_bind(self, connection):
        self._test_bind(cast, connection)

    def test_type_bind(self, connection):
        self._test_bind(type_coerce, connection)

    def _test_bind(self, coerce_fn, conn):
        MyType = self.MyType

        t = self.tables.t
        conn.execute(t.insert().values(data=coerce_fn("d1", MyType)))

        stmt = select(
            bindparam(None, "x", String(50), unique=True),
            coerce_fn(bindparam(None, "x", String(50), unique=True), MyType),
        )

        eq_(
            conn.execute(stmt).fetchall(),
            [("x", "BIND_INxBIND_OUT")]
            if coerce_fn is type_coerce
            else [("x", "xBIND_OUT")],
        )

    def test_cast_existing_typed(self, connection):
        MyType = self.MyType
        coerce_fn = cast

        # when cast() is given an already typed value,
        # the type does not take effect on the value itself.
        eq_(
            connection.scalar(select(coerce_fn(literal("d1"), MyType))),
            "d1BIND_OUT",
        )

    def test_type_coerce_existing_typed(self, connection):
        MyType = self.MyType
        coerce_fn = type_coerce
        t = self.tables.t

        # type_coerce does upgrade the given expression to the
        # given type.

        connection.execute(
            t.insert().values(data=coerce_fn(literal("d1"), MyType))
        )

        eq_(
            connection.execute(select(coerce_fn(t.c.data, MyType))).fetchall(),
            [("BIND_INd1BIND_OUT",)],
        )


class VariantBackendTest(fixtures.TestBase, AssertsCompiledSQL):
    __backend__ = True

    @testing.fixture
    def variant_roundtrip(self, metadata, connection):
        def run(datatype, data, assert_data):
            t = Table(
                "t",
                metadata,
                Column("data", datatype),
            )
            t.create(connection)

            connection.execute(t.insert(), [{"data": elem} for elem in data])
            eq_(
                connection.execute(select(t).order_by(t.c.data)).all(),
                [(elem,) for elem in assert_data],
            )

            eq_(
                # test an IN, which in 1.4 is an expanding
                connection.execute(
                    select(t).where(t.c.data.in_(data)).order_by(t.c.data)
                ).all(),
                [(elem,) for elem in assert_data],
            )

        return run

    def test_type_decorator_variant_one_roundtrip(self, variant_roundtrip):
        class Foo(TypeDecorator):
            impl = String(50)

        if testing.against("postgresql"):
            data = [5, 6, 10]
        else:
            data = ["five", "six", "ten"]
        variant_roundtrip(
            Foo().with_variant(Integer, "postgresql"), data, data
        )

    def test_type_decorator_variant_two(self, variant_roundtrip):
        class UTypeOne(types.UserDefinedType):
            def get_col_spec(self):
                return "VARCHAR(50)"

            def bind_processor(self, dialect):
                def process(value):
                    return value + "UONE"

                return process

        class UTypeTwo(types.UserDefinedType):
            def get_col_spec(self):
                return "VARCHAR(50)"

            def bind_processor(self, dialect):
                def process(value):
                    return value + "UTWO"

                return process

        variant = UTypeOne()
        for db in ["postgresql", "mysql", "mariadb"]:
            variant = variant.with_variant(UTypeTwo(), db)

        class Foo(TypeDecorator):
            impl = variant

        if testing.against("postgresql"):
            data = assert_data = [5, 6, 10]
        elif testing.against("mysql") or testing.against("mariadb"):
            data = ["five", "six", "ten"]
            assert_data = ["fiveUTWO", "sixUTWO", "tenUTWO"]
        else:
            data = ["five", "six", "ten"]
            assert_data = ["fiveUONE", "sixUONE", "tenUONE"]

        variant_roundtrip(
            Foo().with_variant(Integer, "postgresql"), data, assert_data
        )

    def test_type_decorator_variant_three(self, variant_roundtrip):
        class Foo(TypeDecorator):
            impl = String

        if testing.against("postgresql"):
            data = ["five", "six", "ten"]
        else:
            data = [5, 6, 10]

        variant_roundtrip(
            Integer().with_variant(Foo(), "postgresql"), data, data
        )

    def test_type_decorator_compile_variant_one(self):
        class Foo(TypeDecorator):
            impl = String

        self.assert_compile(
            Foo().with_variant(Integer, "sqlite"),
            "INTEGER",
            dialect=dialects.sqlite.dialect(),
        )

        self.assert_compile(
            Foo().with_variant(Integer, "sqlite"),
            "VARCHAR",
            dialect=dialects.postgresql.dialect(),
        )

    def test_type_decorator_compile_variant_two(self):
        class UTypeOne(types.UserDefinedType):
            def get_col_spec(self):
                return "UTYPEONE"

            def bind_processor(self, dialect):
                def process(value):
                    return value + "UONE"

                return process

        class UTypeTwo(types.UserDefinedType):
            def get_col_spec(self):
                return "UTYPETWO"

            def bind_processor(self, dialect):
                def process(value):
                    return value + "UTWO"

                return process

        variant = UTypeOne().with_variant(UTypeTwo(), "postgresql")

        class Foo(TypeDecorator):
            impl = variant

        self.assert_compile(
            Foo().with_variant(Integer, "sqlite"),
            "INTEGER",
            dialect=dialects.sqlite.dialect(),
        )

        self.assert_compile(
            Foo().with_variant(Integer, "sqlite"),
            "UTYPETWO",
            dialect=dialects.postgresql.dialect(),
        )

    def test_type_decorator_compile_variant_three(self):
        class Foo(TypeDecorator):
            impl = String

        self.assert_compile(
            Integer().with_variant(Foo(), "postgresql"),
            "INTEGER",
            dialect=dialects.sqlite.dialect(),
        )

        self.assert_compile(
            Integer().with_variant(Foo(), "postgresql"),
            "VARCHAR",
            dialect=dialects.postgresql.dialect(),
        )


class VariantTest(fixtures.TestBase, AssertsCompiledSQL):
    def setup_test(self):
        class UTypeOne(types.UserDefinedType):
            def get_col_spec(self):
                return "UTYPEONE"

            def bind_processor(self, dialect):
                def process(value):
                    return value + "UONE"

                return process

        class UTypeTwo(types.UserDefinedType):
            def get_col_spec(self):
                return "UTYPETWO"

            def bind_processor(self, dialect):
                def process(value):
                    return value + "UTWO"

                return process

        class UTypeThree(types.UserDefinedType):
            def get_col_spec(self):
                return "UTYPETHREE"

        self.UTypeOne = UTypeOne
        self.UTypeTwo = UTypeTwo
        self.UTypeThree = UTypeThree
        self.variant = self.UTypeOne().with_variant(
            self.UTypeTwo(), "postgresql"
        )
        self.composite = self.variant.with_variant(self.UTypeThree(), "mysql")

    def test_illegal_dupe(self):
        v = self.UTypeOne().with_variant(self.UTypeTwo(), "postgresql")
        assert_raises_message(
            exc.ArgumentError,
            "Dialect 'postgresql' is already present "
            "in the mapping for this Variant",
            lambda: v.with_variant(self.UTypeThree(), "postgresql"),
        )

    def test_compile(self):
        self.assert_compile(self.variant, "UTYPEONE", use_default_dialect=True)
        self.assert_compile(
            self.variant, "UTYPEONE", dialect=dialects.mysql.dialect()
        )
        self.assert_compile(
            self.variant, "UTYPETWO", dialect=dialects.postgresql.dialect()
        )

    def test_to_instance(self):
        self.assert_compile(
            self.UTypeOne().with_variant(self.UTypeTwo, "postgresql"),
            "UTYPETWO",
            dialect=dialects.postgresql.dialect(),
        )

    def test_compile_composite(self):
        self.assert_compile(
            self.composite, "UTYPEONE", use_default_dialect=True
        )
        self.assert_compile(
            self.composite, "UTYPETHREE", dialect=dialects.mysql.dialect()
        )
        self.assert_compile(
            self.composite, "UTYPETWO", dialect=dialects.postgresql.dialect()
        )

    def test_bind_process(self):
        eq_(
            self.variant._cached_bind_processor(dialects.mysql.dialect())(
                "foo"
            ),
            "fooUONE",
        )
        eq_(
            self.variant._cached_bind_processor(default.DefaultDialect())(
                "foo"
            ),
            "fooUONE",
        )
        eq_(
            self.variant._cached_bind_processor(dialects.postgresql.dialect())(
                "foo"
            ),
            "fooUTWO",
        )

    def test_bind_process_composite(self):
        assert (
            self.composite._cached_bind_processor(dialects.mysql.dialect())
            is None
        )
        eq_(
            self.composite._cached_bind_processor(default.DefaultDialect())(
                "foo"
            ),
            "fooUONE",
        )
        eq_(
            self.composite._cached_bind_processor(
                dialects.postgresql.dialect()
            )("foo"),
            "fooUTWO",
        )

    def test_comparator_variant(self):
        expr = column("x", self.variant) == "bar"
        is_(expr.right.type, self.variant)

    @testing.only_on("sqlite")
    @testing.provide_metadata
    def test_round_trip(self, connection):
        variant = self.UTypeOne().with_variant(self.UTypeTwo(), "sqlite")

        t = Table("t", self.metadata, Column("x", variant))
        t.create(connection)

        connection.execute(t.insert(), dict(x="foo"))

        eq_(connection.scalar(select(t.c.x).where(t.c.x == "foo")), "fooUTWO")

    @testing.only_on("sqlite")
    @testing.provide_metadata
    def test_round_trip_sqlite_datetime(self, connection):
        variant = DateTime().with_variant(
            dialects.sqlite.DATETIME(truncate_microseconds=True), "sqlite"
        )

        t = Table("t", self.metadata, Column("x", variant))
        t.create(connection)

        connection.execute(
            t.insert(),
            dict(x=datetime.datetime(2015, 4, 18, 10, 15, 17, 4839)),
        )

        eq_(
            connection.scalar(
                select(t.c.x).where(
                    t.c.x == datetime.datetime(2015, 4, 18, 10, 15, 17, 1059)
                )
            ),
            datetime.datetime(2015, 4, 18, 10, 15, 17),
        )


class UnicodeTest(fixtures.TestBase):

    """Exercise the Unicode and related types.

    Note:  unicode round trip tests are now in
    sqlalchemy/testing/suite/test_types.py.

    """

    __backend__ = True

    data = util.u(
        "Alors vous imaginez ma surprise, au lever du jour, quand "
        "une drôle de petite voix m’a réveillé. "
        "Elle disait: « S’il vous plaît… dessine-moi un mouton! »"
    )

    def test_unicode_warnings_typelevel_native_unicode(self):

        unicodedata = self.data
        u = Unicode()
        dialect = default.DefaultDialect()
        dialect.supports_unicode_binds = True
        uni = u.dialect_impl(dialect).bind_processor(dialect)
        if util.py3k:
            assert_raises(exc.SAWarning, uni, b"x")
            assert isinstance(uni(unicodedata), str)
        else:
            assert_raises(exc.SAWarning, uni, "x")
            assert isinstance(uni(unicodedata), unicode)  # noqa

    def test_unicode_warnings_typelevel_sqla_unicode(self):
        unicodedata = self.data
        u = Unicode()
        dialect = default.DefaultDialect()
        dialect.supports_unicode_binds = False
        uni = u.dialect_impl(dialect).bind_processor(dialect)
        assert_raises(exc.SAWarning, uni, util.b("x"))
        assert isinstance(uni(unicodedata), util.binary_type)

        eq_(uni(unicodedata), unicodedata.encode("utf-8"))

    def test_unicode_warnings_totally_wrong_type(self):
        u = Unicode()
        dialect = default.DefaultDialect()
        dialect.supports_unicode_binds = False
        uni = u.dialect_impl(dialect).bind_processor(dialect)
        with expect_warnings(
            "Unicode type received non-unicode bind param value 5."
        ):
            eq_(uni(5), 5)


class EnumTest(AssertsCompiledSQL, fixtures.TablesTest):
    __backend__ = True

    SomeEnum = pep435_enum("SomeEnum")

    one = SomeEnum("one", 1)
    two = SomeEnum("two", 2)
    three = SomeEnum("three", 3, "four")
    a_member = SomeEnum("AMember", "a")
    b_member = SomeEnum("BMember", "b")

    SomeOtherEnum = pep435_enum("SomeOtherEnum")

    other_one = SomeOtherEnum("one", 1)
    other_two = SomeOtherEnum("two", 2)
    other_three = SomeOtherEnum("three", 3)
    other_a_member = SomeOtherEnum("AMember", "a")
    other_b_member = SomeOtherEnum("BMember", "b")

    @staticmethod
    def get_enum_string_values(some_enum):
        return [str(v.value) for v in some_enum.__members__.values()]

    @classmethod
    def define_tables(cls, metadata):
        # note create_constraint has changed in 1.4 as of #5367
        Table(
            "enum_table",
            metadata,
            Column("id", Integer, primary_key=True),
            Column(
                "someenum",
                Enum(
                    "one",
                    "two",
                    "three",
                    name="myenum",
                    create_constraint=True,
                ),
            ),
        )

        Table(
            "non_native_enum_table",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=False),
            Column(
                "someenum",
                Enum(
                    "one",
                    "two",
                    "three",
                    native_enum=False,
                    create_constraint=True,
                ),
            ),
            Column(
                "someotherenum",
                Enum(
                    "one",
                    "two",
                    "three",
                    native_enum=False,
                    validate_strings=True,
                ),
            ),
        )

        Table(
            "stdlib_enum_table",
            metadata,
            Column("id", Integer, primary_key=True),
            Column(
                "someenum",
                Enum(cls.SomeEnum, create_constraint=True, omit_aliases=False),
            ),
        )
        Table(
            "stdlib_enum_table_no_alias",
            metadata,
            Column("id", Integer, primary_key=True),
            Column(
                "someenum",
                Enum(
                    cls.SomeEnum,
                    create_constraint=True,
                    omit_aliases=True,
                    name="someenum_no_alias",
                ),
            ),
        )

        Table(
            "stdlib_enum_table2",
            metadata,
            Column("id", Integer, primary_key=True),
            Column(
                "someotherenum",
                Enum(
                    cls.SomeOtherEnum,
                    values_callable=EnumTest.get_enum_string_values,
                    create_constraint=True,
                ),
            ),
        )

    def test_python_type(self):
        eq_(types.Enum(self.SomeOtherEnum).python_type, self.SomeOtherEnum)

    def test_pickle_types(self):
        global SomeEnum
        SomeEnum = self.SomeEnum
        for loads, dumps in picklers():
            column_types = [
                Column("Enu", Enum("x", "y", "z", name="somename")),
                Column("En2", Enum(self.SomeEnum, omit_aliases=False)),
            ]
            for column_type in column_types:
                meta = MetaData()
                Table("foo", meta, column_type)
                loads(dumps(column_type))
                loads(dumps(meta))

    def test_validators_pep435(self):
        type_ = Enum(self.SomeEnum, omit_aliases=False)
        validate_type = Enum(
            self.SomeEnum, validate_strings=True, omit_aliases=False
        )

        bind_processor = type_.bind_processor(testing.db.dialect)
        bind_processor_validates = validate_type.bind_processor(
            testing.db.dialect
        )
        eq_(bind_processor("one"), "one")
        eq_(bind_processor(self.one), "one")
        eq_(bind_processor("foo"), "foo")
        assert_raises_message(
            LookupError,
            "'5' is not among the defined enum values. Enum name: someenum. "
            "Possible values: one, two, three, ..., BMember",
            bind_processor,
            5,
        )

        assert_raises_message(
            LookupError,
            "'foo' is not among the defined enum values. Enum name: someenum. "
            "Possible values: one, two, three, ..., BMember",
            bind_processor_validates,
            "foo",
        )

        result_processor = type_.result_processor(testing.db.dialect, None)

        eq_(result_processor("one"), self.one)
        assert_raises_message(
            LookupError,
            "'foo' is not among the defined enum values. Enum name: someenum. "
            "Possible values: one, two, three, ..., BMember",
            result_processor,
            "foo",
        )

        literal_processor = type_.literal_processor(testing.db.dialect)
        validate_literal_processor = validate_type.literal_processor(
            testing.db.dialect
        )
        eq_(literal_processor("one"), "'one'")

        eq_(literal_processor("foo"), "'foo'")

        assert_raises_message(
            LookupError,
            "'5' is not among the defined enum values. Enum name: someenum. "
            "Possible values: one, two, three, ..., BMember",
            literal_processor,
            5,
        )

        assert_raises_message(
            LookupError,
            "'foo' is not among the defined enum values. Enum name: someenum. "
            "Possible values: one, two, three, ..., BMember",
            validate_literal_processor,
            "foo",
        )

    def test_validators_plain(self):
        type_ = Enum("one", "two")
        validate_type = Enum("one", "two", validate_strings=True)

        bind_processor = type_.bind_processor(testing.db.dialect)
        bind_processor_validates = validate_type.bind_processor(
            testing.db.dialect
        )
        eq_(bind_processor("one"), "one")
        eq_(bind_processor("foo"), "foo")
        assert_raises_message(
            LookupError,
            "'5' is not among the defined enum values. Enum name: None. "
            "Possible values: one, two",
            bind_processor,
            5,
        )

        assert_raises_message(
            LookupError,
            "'foo' is not among the defined enum values. Enum name: None. "
            "Possible values: one, two",
            bind_processor_validates,
            "foo",
        )

        result_processor = type_.result_processor(testing.db.dialect, None)

        eq_(result_processor("one"), "one")
        assert_raises_message(
            LookupError,
            "'foo' is not among the defined enum values. Enum name: None. "
            "Possible values: one, two",
            result_processor,
            "foo",
        )

        literal_processor = type_.literal_processor(testing.db.dialect)
        validate_literal_processor = validate_type.literal_processor(
            testing.db.dialect
        )
        eq_(literal_processor("one"), "'one'")
        eq_(literal_processor("foo"), "'foo'")
        assert_raises_message(
            LookupError,
            "'5' is not among the defined enum values. Enum name: None. "
            "Possible values: one, two",
            literal_processor,
            5,
        )

        assert_raises_message(
            LookupError,
            "'foo' is not among the defined enum values. Enum name: None. "
            "Possible values: one, two",
            validate_literal_processor,
            "foo",
        )

    def test_enum_raise_lookup_ellipses(self):
        type_ = Enum("one", "twothreefourfivesix", "seven", "eight")
        bind_processor = type_.bind_processor(testing.db.dialect)

        eq_(bind_processor("one"), "one")
        assert_raises_message(
            LookupError,
            "'5' is not among the defined enum values. Enum name: None. "
            "Possible values: one, twothreefou.., seven, eight",
            bind_processor,
            5,
        )

    def test_enum_raise_lookup_none(self):
        type_ = Enum()
        bind_processor = type_.bind_processor(testing.db.dialect)

        assert_raises_message(
            LookupError,
            "'5' is not among the defined enum values. Enum name: None. "
            "Possible values: None",
            bind_processor,
            5,
        )

    def test_validators_not_in_like_roundtrip(self, connection):
        enum_table = self.tables["non_native_enum_table"]

        connection.execute(
            enum_table.insert(),
            [
                {"id": 1, "someenum": "two"},
                {"id": 2, "someenum": "two"},
                {"id": 3, "someenum": "one"},
            ],
        )

        eq_(
            connection.execute(
                enum_table.select()
                .where(enum_table.c.someenum.like("%wo%"))
                .order_by(enum_table.c.id)
            ).fetchall(),
            [(1, "two", None), (2, "two", None)],
        )

    def test_validators_not_in_concatenate_roundtrip(self, connection):
        enum_table = self.tables["non_native_enum_table"]

        connection.execute(
            enum_table.insert(),
            [
                {"id": 1, "someenum": "two"},
                {"id": 2, "someenum": "two"},
                {"id": 3, "someenum": "one"},
            ],
        )

        eq_(
            connection.execute(
                select("foo" + enum_table.c.someenum).order_by(enum_table.c.id)
            ).fetchall(),
            [("footwo",), ("footwo",), ("fooone",)],
        )

    def test_round_trip(self, connection):
        enum_table = self.tables["enum_table"]

        connection.execute(
            enum_table.insert(),
            [
                {"id": 1, "someenum": "two"},
                {"id": 2, "someenum": "two"},
                {"id": 3, "someenum": "one"},
            ],
        )

        eq_(
            connection.execute(
                enum_table.select().order_by(enum_table.c.id)
            ).fetchall(),
            [(1, "two"), (2, "two"), (3, "one")],
        )

    def test_null_round_trip(self, connection):
        enum_table = self.tables.enum_table
        non_native_enum_table = self.tables.non_native_enum_table

        connection.execute(enum_table.insert(), {"id": 1, "someenum": None})
        eq_(connection.scalar(select(enum_table.c.someenum)), None)

        connection.execute(
            non_native_enum_table.insert(), {"id": 1, "someenum": None}
        )
        eq_(connection.scalar(select(non_native_enum_table.c.someenum)), None)

    @testing.requires.enforces_check_constraints
    def test_check_constraint(self, connection):
        assert_raises(
            (
                exc.IntegrityError,
                exc.ProgrammingError,
                exc.OperationalError,
                # PyMySQL raising InternalError until
                # https://github.com/PyMySQL/PyMySQL/issues/607 is resolved
                exc.InternalError,
            ),
            connection.exec_driver_sql,
            "insert into non_native_enum_table "
            "(id, someenum) values(1, 'four')",
        )

    @testing.requires.enforces_check_constraints
    @testing.provide_metadata
    def test_variant_we_are_default(self):
        # test that the "variant" does not create a constraint
        t = Table(
            "my_table",
            self.metadata,
            Column(
                "data",
                Enum(
                    "one",
                    "two",
                    "three",
                    native_enum=False,
                    name="e1",
                    create_constraint=True,
                ).with_variant(
                    Enum(
                        "four",
                        "five",
                        "six",
                        native_enum=False,
                        name="e2",
                        create_constraint=True,
                    ),
                    "some_other_db",
                ),
            ),
            mysql_engine="InnoDB",
        )

        eq_(
            len([c for c in t.constraints if isinstance(c, CheckConstraint)]),
            2,
        )

        self.metadata.create_all(testing.db)

        # not using the connection fixture because we need to rollback and
        # start again in the middle
        with testing.db.connect() as connection:
            # postgresql needs this in order to continue after the exception
            trans = connection.begin()
            assert_raises(
                (exc.DBAPIError,),
                connection.exec_driver_sql,
                "insert into my_table " "(data) values('four')",
            )
            trans.rollback()

            with connection.begin():
                connection.exec_driver_sql(
                    "insert into my_table (data) values ('two')"
                )
                eq_(connection.execute(select(t.c.data)).scalar(), "two")

    @testing.requires.enforces_check_constraints
    @testing.provide_metadata
    def test_variant_we_are_not_default(self):
        # test that the "variant" does not create a constraint
        t = Table(
            "my_table",
            self.metadata,
            Column(
                "data",
                Enum(
                    "one",
                    "two",
                    "three",
                    native_enum=False,
                    name="e1",
                    create_constraint=True,
                ).with_variant(
                    Enum(
                        "four",
                        "five",
                        "six",
                        native_enum=False,
                        name="e2",
                        create_constraint=True,
                    ),
                    testing.db.dialect.name,
                ),
            ),
        )

        # ensure Variant isn't exploding the constraints
        eq_(
            len([c for c in t.constraints if isinstance(c, CheckConstraint)]),
            2,
        )

        self.metadata.create_all(testing.db)

        # not using the connection fixture because we need to rollback and
        # start again in the middle
        with testing.db.connect() as connection:
            # postgresql needs this in order to continue after the exception
            trans = connection.begin()
            assert_raises(
                (exc.DBAPIError,),
                connection.exec_driver_sql,
                "insert into my_table " "(data) values('two')",
            )
            trans.rollback()

            with connection.begin():
                connection.exec_driver_sql(
                    "insert into my_table (data) values ('four')"
                )
                eq_(connection.execute(select(t.c.data)).scalar(), "four")

    def test_skip_check_constraint(self, connection):
        connection.exec_driver_sql(
            "insert into non_native_enum_table "
            "(id, someotherenum) values(1, 'four')"
        )
        eq_(
            connection.exec_driver_sql(
                "select someotherenum from non_native_enum_table"
            ).scalar(),
            "four",
        )
        assert_raises_message(
            LookupError,
            "'four' is not among the defined enum values. "
            "Enum name: None. Possible values: one, two, three",
            connection.scalar,
            select(self.tables.non_native_enum_table.c.someotherenum),
        )

    def test_non_native_round_trip(self, connection):
        non_native_enum_table = self.tables["non_native_enum_table"]

        connection.execute(
            non_native_enum_table.insert(),
            [
                {"id": 1, "someenum": "two"},
                {"id": 2, "someenum": "two"},
                {"id": 3, "someenum": "one"},
            ],
        )

        eq_(
            connection.execute(
                select(
                    non_native_enum_table.c.id,
                    non_native_enum_table.c.someenum,
                ).order_by(non_native_enum_table.c.id)
            ).fetchall(),
            [(1, "two"), (2, "two"), (3, "one")],
        )

    def test_pep435_default_sort_key(self):
        one, two, a_member, b_member = (
            self.one,
            self.two,
            self.a_member,
            self.b_member,
        )
        typ = Enum(self.SomeEnum, omit_aliases=False)

        is_(typ.sort_key_function.__func__, typ._db_value_for_elem.__func__)

        eq_(
            sorted([two, one, a_member, b_member], key=typ.sort_key_function),
            [a_member, b_member, one, two],
        )

    def test_pep435_custom_sort_key(self):
        one, two, a_member, b_member = (
            self.one,
            self.two,
            self.a_member,
            self.b_member,
        )

        def sort_enum_key_value(value):
            return str(value.value)

        typ = Enum(
            self.SomeEnum,
            sort_key_function=sort_enum_key_value,
            omit_aliases=False,
        )
        is_(typ.sort_key_function, sort_enum_key_value)

        eq_(
            sorted([two, one, a_member, b_member], key=typ.sort_key_function),
            [one, two, a_member, b_member],
        )

    def test_pep435_no_sort_key(self):
        typ = Enum(self.SomeEnum, sort_key_function=None, omit_aliases=False)
        is_(typ.sort_key_function, None)

    def test_pep435_enum_round_trip(self, connection):
        stdlib_enum_table = self.tables["stdlib_enum_table"]

        connection.execute(
            stdlib_enum_table.insert(),
            [
                {"id": 1, "someenum": self.SomeEnum.two},
                {"id": 2, "someenum": self.SomeEnum.two},
                {"id": 3, "someenum": self.SomeEnum.one},
                {"id": 4, "someenum": self.SomeEnum.three},
                {"id": 5, "someenum": self.SomeEnum.four},
                {"id": 6, "someenum": "three"},
                {"id": 7, "someenum": "four"},
            ],
        )

        eq_(
            connection.execute(
                stdlib_enum_table.select().order_by(stdlib_enum_table.c.id)
            ).fetchall(),
            [
                (1, self.SomeEnum.two),
                (2, self.SomeEnum.two),
                (3, self.SomeEnum.one),
                (4, self.SomeEnum.three),
                (5, self.SomeEnum.three),
                (6, self.SomeEnum.three),
                (7, self.SomeEnum.three),
            ],
        )

    def test_pep435_enum_values_callable_round_trip(self, connection):
        stdlib_enum_table_custom_values = self.tables["stdlib_enum_table2"]

        connection.execute(
            stdlib_enum_table_custom_values.insert(),
            [
                {"id": 1, "someotherenum": self.SomeOtherEnum.AMember},
                {"id": 2, "someotherenum": self.SomeOtherEnum.BMember},
                {"id": 3, "someotherenum": self.SomeOtherEnum.AMember},
            ],
        )

        eq_(
            connection.execute(
                stdlib_enum_table_custom_values.select().order_by(
                    stdlib_enum_table_custom_values.c.id
                )
            ).fetchall(),
            [
                (1, self.SomeOtherEnum.AMember),
                (2, self.SomeOtherEnum.BMember),
                (3, self.SomeOtherEnum.AMember),
            ],
        )

    def test_pep435_enum_expanding_in(self, connection):
        stdlib_enum_table_custom_values = self.tables["stdlib_enum_table2"]

        connection.execute(
            stdlib_enum_table_custom_values.insert(),
            [
                {"id": 1, "someotherenum": self.SomeOtherEnum.one},
                {"id": 2, "someotherenum": self.SomeOtherEnum.two},
                {"id": 3, "someotherenum": self.SomeOtherEnum.three},
            ],
        )

        stmt = (
            stdlib_enum_table_custom_values.select()
            .where(
                stdlib_enum_table_custom_values.c.someotherenum.in_(
                    bindparam("member", expanding=True)
                )
            )
            .order_by(stdlib_enum_table_custom_values.c.id)
        )
        eq_(
            connection.execute(
                stmt,
                {"member": [self.SomeOtherEnum.one, self.SomeOtherEnum.three]},
            ).fetchall(),
            [(1, self.SomeOtherEnum.one), (3, self.SomeOtherEnum.three)],
        )

    def test_adapt(self):
        from sqlalchemy.dialects.postgresql import ENUM

        e1 = Enum("one", "two", "three", native_enum=False)

        false_adapt = e1.adapt(ENUM)
        eq_(false_adapt.native_enum, False)
        assert not isinstance(false_adapt, ENUM)

        e1 = Enum("one", "two", "three", native_enum=True)
        true_adapt = e1.adapt(ENUM)
        eq_(true_adapt.native_enum, True)
        assert isinstance(true_adapt, ENUM)

        e1 = Enum(
            "one",
            "two",
            "three",
            name="foo",
            schema="bar",
            metadata=MetaData(),
        )
        eq_(e1.adapt(ENUM).name, "foo")
        eq_(e1.adapt(ENUM).schema, "bar")
        is_(e1.adapt(ENUM).metadata, e1.metadata)
        eq_(e1.adapt(Enum).name, "foo")
        eq_(e1.adapt(Enum).schema, "bar")
        is_(e1.adapt(Enum).metadata, e1.metadata)
        e1 = Enum(self.SomeEnum, omit_aliases=False)
        eq_(e1.adapt(ENUM).name, "someenum")
        eq_(
            e1.adapt(ENUM).enums,
            ["one", "two", "three", "four", "AMember", "BMember"],
        )

        e1_vc = Enum(
            self.SomeOtherEnum, values_callable=EnumTest.get_enum_string_values
        )
        eq_(e1_vc.adapt(ENUM).name, "someotherenum")
        eq_(e1_vc.adapt(ENUM).enums, ["1", "2", "3", "a", "b"])

    def test_adapt_length(self):
        from sqlalchemy.dialects.postgresql import ENUM

        e1 = Enum("one", "two", "three", length=50, native_enum=False)
        eq_(e1.adapt(ENUM).length, 50)
        eq_(e1.adapt(Enum).length, 50)

        e1 = Enum("one", "two", "three")
        eq_(e1.length, 5)
        eq_(e1.adapt(ENUM).length, 5)
        eq_(e1.adapt(Enum).length, 5)

    @testing.provide_metadata
    def test_create_metadata_bound_no_crash(self):
        m1 = self.metadata
        Enum("a", "b", "c", metadata=m1, name="ncenum")

        m1.create_all(testing.db)

    def test_non_native_constraint_custom_type(self):
        class Foob(object):
            def __init__(self, name):
                self.name = name

        class MyEnum(TypeDecorator):
            def __init__(self, values):
                self.impl = Enum(
                    *[v.name for v in values],
                    name="myenum",
                    native_enum=False,
                    create_constraint=True
                )

            # future method
            def process_literal_param(self, value, dialect):
                return value.name

            def process_bind_param(self, value, dialect):
                return value.name

        m = MetaData()
        t1 = Table("t", m, Column("x", MyEnum([Foob("a"), Foob("b")])))
        const = [c for c in t1.constraints if isinstance(c, CheckConstraint)][
            0
        ]

        self.assert_compile(
            AddConstraint(const),
            "ALTER TABLE t ADD CONSTRAINT myenum CHECK (x IN ('a', 'b'))",
            dialect="default",
        )

    def test_lookup_failure(self, connection):
        assert_raises(
            exc.StatementError,
            connection.execute,
            self.tables["non_native_enum_table"].insert(),
            {"id": 4, "someotherenum": "four"},
        )

    def test_mock_engine_no_prob(self):
        """ensure no 'checkfirst' queries are run when enums
        are created with checkfirst=False"""

        e = engines.mock_engine()
        t = Table(
            "t1",
            MetaData(),
            Column("x", Enum("x", "y", name="pge", create_constraint=True)),
        )
        t.create(e, checkfirst=False)
        # basically looking for the start of
        # the constraint, or the ENUM def itself,
        # depending on backend.
        assert "('x'," in e.print_sql()

    @testing.uses_deprecated(".*convert_unicode")
    def test_repr(self):
        e = Enum(
            "x",
            "y",
            name="somename",
            convert_unicode=True,
            quote=True,
            inherit_schema=True,
            native_enum=False,
        )
        eq_(
            repr(e),
            "Enum('x', 'y', name='somename', "
            "inherit_schema=True, native_enum=False)",
        )

    def test_length_native(self):
        e = Enum("x", "y", "long", length=42)

        eq_(e.length, len("long"))

        # no error is raised
        e = Enum("x", "y", "long", length=1)
        eq_(e.length, len("long"))

    def test_length_raises(self):
        assert_raises_message(
            ValueError,
            "When provided, length must be larger or equal.*",
            Enum,
            "x",
            "y",
            "long",
            native_enum=False,
            length=1,
        )

    def test_no_length_non_native(self):
        e = Enum("x", "y", "long", native_enum=False)
        eq_(e.length, len("long"))

    def test_length_non_native(self):
        e = Enum("x", "y", "long", native_enum=False, length=42)
        eq_(e.length, 42)

    def test_omit_aliases(self, connection):
        table0 = self.tables["stdlib_enum_table"]
        type0 = table0.c.someenum.type
        eq_(type0.enums, ["one", "two", "three", "four", "AMember", "BMember"])

        table = self.tables["stdlib_enum_table_no_alias"]

        type_ = table.c.someenum.type
        eq_(type_.enums, ["one", "two", "three", "AMember", "BMember"])

        connection.execute(
            table.insert(),
            [
                {"id": 1, "someenum": self.SomeEnum.three},
                {"id": 2, "someenum": self.SomeEnum.four},
            ],
        )
        eq_(
            connection.execute(table.select().order_by(table.c.id)).fetchall(),
            [(1, self.SomeEnum.three), (2, self.SomeEnum.three)],
        )

    def test_omit_warn(self):
        with expect_deprecated_20(
            r"The provided enum someenum contains the aliases \['four'\]"
        ):
            Enum(self.SomeEnum)


MyPickleType = None


class BinaryTest(fixtures.TablesTest, AssertsExecutionResults):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        global MyPickleType

        class MyPickleType(types.TypeDecorator):
            impl = PickleType

            def process_bind_param(self, value, dialect):
                if value:
                    value.stuff = "this is modified stuff"
                return value

            def process_result_value(self, value, dialect):
                if value:
                    value.stuff = "this is the right stuff"
                return value

        Table(
            "binary_table",
            metadata,
            Column(
                "primary_id",
                Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column("data", LargeBinary),
            Column("data_slice", LargeBinary(100)),
            Column("misc", String(30)),
            Column("pickled", PickleType),
            Column("mypickle", MyPickleType),
        )

    @testing.requires.non_broken_binary
    def test_round_trip(self, connection):
        binary_table = self.tables.binary_table

        testobj1 = pickleable.Foo("im foo 1")
        testobj2 = pickleable.Foo("im foo 2")
        testobj3 = pickleable.Foo("im foo 3")

        stream1 = self.load_stream("binary_data_one.dat")
        stream2 = self.load_stream("binary_data_two.dat")
        connection.execute(
            binary_table.insert(),
            dict(
                primary_id=1,
                misc="binary_data_one.dat",
                data=stream1,
                data_slice=stream1[0:100],
                pickled=testobj1,
                mypickle=testobj3,
            ),
        )
        connection.execute(
            binary_table.insert(),
            dict(
                primary_id=2,
                misc="binary_data_two.dat",
                data=stream2,
                data_slice=stream2[0:99],
                pickled=testobj2,
            ),
        )
        connection.execute(
            binary_table.insert(),
            dict(
                primary_id=3,
                misc="binary_data_two.dat",
                data=None,
                data_slice=stream2[0:99],
                pickled=None,
            ),
        )

        for stmt in (
            binary_table.select(order_by=binary_table.c.primary_id),
            text(
                "select * from binary_table order by binary_table.primary_id",
            ).columns(
                **{
                    "pickled": PickleType,
                    "mypickle": MyPickleType,
                    "data": LargeBinary,
                    "data_slice": LargeBinary,
                }
            ),
        ):
            result = connection.execute(stmt).fetchall()
            eq_(stream1, result[0]._mapping["data"])
            eq_(stream1[0:100], result[0]._mapping["data_slice"])
            eq_(stream2, result[1]._mapping["data"])
            eq_(testobj1, result[0]._mapping["pickled"])
            eq_(testobj2, result[1]._mapping["pickled"])
            eq_(testobj3.moredata, result[0]._mapping["mypickle"].moredata)
            eq_(
                result[0]._mapping["mypickle"].stuff, "this is the right stuff"
            )

    @testing.requires.binary_comparisons
    def test_comparison(self, connection):
        """test that type coercion occurs on comparison for binary"""
        binary_table = self.tables.binary_table

        expr = binary_table.c.data == "foo"
        assert isinstance(expr.right.type, LargeBinary)

        data = os.urandom(32)
        connection.execute(binary_table.insert(), dict(data=data))
        eq_(
            connection.scalar(
                select(func.count("*"))
                .select_from(binary_table)
                .where(binary_table.c.data == data)
            ),
            1,
        )

    @testing.requires.binary_literals
    def test_literal_roundtrip(self, connection):
        compiled = select(cast(literal(util.b("foo")), LargeBinary)).compile(
            dialect=testing.db.dialect, compile_kwargs={"literal_binds": True}
        )
        result = connection.execute(compiled)
        eq_(result.scalar(), util.b("foo"))

    def test_bind_processor_no_dbapi(self):
        b = LargeBinary()
        eq_(b.bind_processor(default.DefaultDialect()), None)

    def load_stream(self, name):
        f = os.path.join(os.path.dirname(__file__), "..", name)
        with open(f, mode="rb") as o:
            return o.read()


class JSONTest(fixtures.TestBase):
    def setup_test(self):
        metadata = MetaData()
        self.test_table = Table(
            "test_table",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("test_column", JSON),
        )
        self.jsoncol = self.test_table.c.test_column

        self.dialect = default.DefaultDialect()
        self.dialect._json_serializer = None
        self.dialect._json_deserializer = None

    def test_bind_serialize_default(self):
        proc = self.test_table.c.test_column.type._cached_bind_processor(
            self.dialect
        )
        eq_(
            proc({"A": [1, 2, 3, True, False]}),
            '{"A": [1, 2, 3, true, false]}',
        )

    def test_bind_serialize_None(self):
        proc = self.test_table.c.test_column.type._cached_bind_processor(
            self.dialect
        )
        eq_(proc(None), "null")

    def test_bind_serialize_none_as_null(self):
        proc = JSON(none_as_null=True)._cached_bind_processor(self.dialect)
        eq_(proc(None), None)
        eq_(proc(null()), None)

    def test_bind_serialize_null(self):
        proc = self.test_table.c.test_column.type._cached_bind_processor(
            self.dialect
        )
        eq_(proc(null()), None)

    def test_result_deserialize_default(self):
        proc = self.test_table.c.test_column.type._cached_result_processor(
            self.dialect, None
        )
        eq_(
            proc('{"A": [1, 2, 3, true, false]}'),
            {"A": [1, 2, 3, True, False]},
        )

    def test_result_deserialize_null(self):
        proc = self.test_table.c.test_column.type._cached_result_processor(
            self.dialect, None
        )
        eq_(proc("null"), None)

    def test_result_deserialize_None(self):
        proc = self.test_table.c.test_column.type._cached_result_processor(
            self.dialect, None
        )
        eq_(proc(None), None)

    def _dialect_index_fixture(self, int_processor, str_processor):
        class MyInt(Integer):
            def bind_processor(self, dialect):
                return lambda value: value + 10

            def literal_processor(self, diaect):
                return lambda value: str(value + 15)

        class MyString(String):
            def bind_processor(self, dialect):
                return lambda value: value + "10"

            def literal_processor(self, diaect):
                return lambda value: value + "15"

        class MyDialect(default.DefaultDialect):
            colspecs = {}
            if int_processor:
                colspecs[Integer] = MyInt
            if str_processor:
                colspecs[String] = MyString

        return MyDialect()

    def test_index_bind_proc_int(self):
        expr = self.test_table.c.test_column[5]

        int_dialect = self._dialect_index_fixture(True, True)
        non_int_dialect = self._dialect_index_fixture(False, True)

        bindproc = expr.right.type._cached_bind_processor(int_dialect)
        eq_(bindproc(expr.right.value), 15)

        bindproc = expr.right.type._cached_bind_processor(non_int_dialect)
        eq_(bindproc(expr.right.value), 5)

    def test_index_literal_proc_int(self):
        expr = self.test_table.c.test_column[5]

        int_dialect = self._dialect_index_fixture(True, True)
        non_int_dialect = self._dialect_index_fixture(False, True)

        bindproc = expr.right.type._cached_literal_processor(int_dialect)
        eq_(bindproc(expr.right.value), "20")

        bindproc = expr.right.type._cached_literal_processor(non_int_dialect)
        eq_(bindproc(expr.right.value), "5")

    def test_index_bind_proc_str(self):
        expr = self.test_table.c.test_column["five"]

        str_dialect = self._dialect_index_fixture(True, True)
        non_str_dialect = self._dialect_index_fixture(False, False)

        bindproc = expr.right.type._cached_bind_processor(str_dialect)
        eq_(bindproc(expr.right.value), "five10")

        bindproc = expr.right.type._cached_bind_processor(non_str_dialect)
        eq_(bindproc(expr.right.value), "five")

    def test_index_literal_proc_str(self):
        expr = self.test_table.c.test_column["five"]

        str_dialect = self._dialect_index_fixture(True, True)
        non_str_dialect = self._dialect_index_fixture(False, False)

        bindproc = expr.right.type._cached_literal_processor(str_dialect)
        eq_(bindproc(expr.right.value), "five15")

        bindproc = expr.right.type._cached_literal_processor(non_str_dialect)
        eq_(bindproc(expr.right.value), "'five'")


class ArrayTest(fixtures.TestBase):
    def _myarray_fixture(self):
        class MyArray(ARRAY):
            pass

        return MyArray

    def test_array_index_map_dimensions(self):
        col = column("x", ARRAY(Integer, dimensions=3))
        is_(col[5].type._type_affinity, ARRAY)
        eq_(col[5].type.dimensions, 2)
        is_(col[5][6].type._type_affinity, ARRAY)
        eq_(col[5][6].type.dimensions, 1)
        is_(col[5][6][7].type._type_affinity, Integer)

    def test_array_getitem_single_type(self):
        m = MetaData()
        arrtable = Table(
            "arrtable",
            m,
            Column("intarr", ARRAY(Integer)),
            Column("strarr", ARRAY(String)),
        )
        is_(arrtable.c.intarr[1].type._type_affinity, Integer)
        is_(arrtable.c.strarr[1].type._type_affinity, String)

    def test_array_getitem_slice_type(self):
        m = MetaData()
        arrtable = Table(
            "arrtable",
            m,
            Column("intarr", ARRAY(Integer)),
            Column("strarr", ARRAY(String)),
        )
        is_(arrtable.c.intarr[1:3].type._type_affinity, ARRAY)
        is_(arrtable.c.strarr[1:3].type._type_affinity, ARRAY)

    def test_array_getitem_slice_type_dialect_level(self):
        MyArray = self._myarray_fixture()
        m = MetaData()
        arrtable = Table(
            "arrtable",
            m,
            Column("intarr", MyArray(Integer)),
            Column("strarr", MyArray(String)),
        )
        is_(arrtable.c.intarr[1:3].type._type_affinity, ARRAY)
        is_(arrtable.c.strarr[1:3].type._type_affinity, ARRAY)

        # but the slice returns the actual type
        assert isinstance(arrtable.c.intarr[1:3].type, MyArray)
        assert isinstance(arrtable.c.strarr[1:3].type, MyArray)


MyCustomType = MyTypeDec = None


class ExpressionTest(
    fixtures.TablesTest, AssertsExecutionResults, AssertsCompiledSQL
):
    __dialect__ = "default"

    @classmethod
    def define_tables(cls, metadata):
        global MyCustomType, MyTypeDec

        class MyCustomType(types.UserDefinedType):
            def get_col_spec(self):
                return "INT"

            def bind_processor(self, dialect):
                def process(value):
                    return value * 10

                return process

            def result_processor(self, dialect, coltype):
                def process(value):
                    return value / 10

                return process

        class MyOldCustomType(MyCustomType):
            def adapt_operator(self, op):
                return {
                    operators.add: operators.sub,
                    operators.sub: operators.add,
                }.get(op, op)

        class MyTypeDec(types.TypeDecorator):
            impl = String

            def process_bind_param(self, value, dialect):
                return "BIND_IN" + str(value)

            def process_result_value(self, value, dialect):
                return value + "BIND_OUT"

        class MyDecOfDec(types.TypeDecorator):
            impl = MyTypeDec

        Table(
            "test",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("data", String(30)),
            Column("atimestamp", Date),
            Column("avalue", MyCustomType),
            Column("bvalue", MyTypeDec(50)),
            Column("cvalue", MyDecOfDec(50)),
        )

    @classmethod
    def insert_data(cls, connection):
        test_table = cls.tables.test
        connection.execute(
            test_table.insert(),
            {
                "id": 1,
                "data": "somedata",
                "atimestamp": datetime.date(2007, 10, 15),
                "avalue": 25,
                "bvalue": "foo",
                "cvalue": "foo",
            },
        )

    def test_control(self, connection):
        test_table = self.tables.test
        assert (
            connection.exec_driver_sql("select avalue from test").scalar()
            == 250
        )

        eq_(
            connection.execute(test_table.select()).fetchall(),
            [
                (
                    1,
                    "somedata",
                    datetime.date(2007, 10, 15),
                    25,
                    "BIND_INfooBIND_OUT",
                    "BIND_INfooBIND_OUT",
                )
            ],
        )

    def test_bind_adapt(self, connection):
        # test an untyped bind gets the left side's type

        test_table = self.tables.test

        expr = test_table.c.atimestamp == bindparam("thedate")
        eq_(expr.right.type._type_affinity, Date)

        eq_(
            connection.execute(
                select(
                    test_table.c.id,
                    test_table.c.data,
                    test_table.c.atimestamp,
                ).where(expr),
                {"thedate": datetime.date(2007, 10, 15)},
            ).fetchall(),
            [(1, "somedata", datetime.date(2007, 10, 15))],
        )

        expr = test_table.c.avalue == bindparam("somevalue")
        eq_(expr.right.type._type_affinity, MyCustomType)

        eq_(
            connection.execute(
                test_table.select().where(expr), {"somevalue": 25}
            ).fetchall(),
            [
                (
                    1,
                    "somedata",
                    datetime.date(2007, 10, 15),
                    25,
                    "BIND_INfooBIND_OUT",
                    "BIND_INfooBIND_OUT",
                )
            ],
        )

        expr = test_table.c.bvalue == bindparam("somevalue")
        eq_(expr.right.type._type_affinity, String)

        eq_(
            connection.execute(
                test_table.select().where(expr), {"somevalue": "foo"}
            ).fetchall(),
            [
                (
                    1,
                    "somedata",
                    datetime.date(2007, 10, 15),
                    25,
                    "BIND_INfooBIND_OUT",
                    "BIND_INfooBIND_OUT",
                )
            ],
        )

    def test_grouped_bind_adapt(self):
        test_table = self.tables.test

        expr = test_table.c.atimestamp == elements.Grouping(
            bindparam("thedate")
        )
        eq_(expr.right.type._type_affinity, Date)
        eq_(expr.right.element.type._type_affinity, Date)

        expr = test_table.c.atimestamp == elements.Grouping(
            elements.Grouping(bindparam("thedate"))
        )
        eq_(expr.right.type._type_affinity, Date)
        eq_(expr.right.element.type._type_affinity, Date)
        eq_(expr.right.element.element.type._type_affinity, Date)

    def test_bind_adapt_update(self):
        test_table = self.tables.test

        bp = bindparam("somevalue")
        stmt = test_table.update().values(avalue=bp)
        compiled = stmt.compile()
        eq_(bp.type._type_affinity, types.NullType)
        eq_(compiled.binds["somevalue"].type._type_affinity, MyCustomType)

    def test_bind_adapt_insert(self):
        test_table = self.tables.test
        bp = bindparam("somevalue")

        stmt = test_table.insert().values(avalue=bp)
        compiled = stmt.compile()
        eq_(bp.type._type_affinity, types.NullType)
        eq_(compiled.binds["somevalue"].type._type_affinity, MyCustomType)

    def test_bind_adapt_expression(self):
        test_table = self.tables.test

        bp = bindparam("somevalue")
        stmt = test_table.c.avalue == bp
        eq_(bp.type._type_affinity, types.NullType)
        eq_(stmt.right.type._type_affinity, MyCustomType)

    def test_literal_adapt(self):
        # literals get typed based on the types dictionary, unless
        # compatible with the left side type

        expr = column("foo", String) == 5
        eq_(expr.right.type._type_affinity, Integer)

        expr = column("foo", String) == "asdf"
        eq_(expr.right.type._type_affinity, String)

        expr = column("foo", CHAR) == 5
        eq_(expr.right.type._type_affinity, Integer)

        expr = column("foo", CHAR) == "asdf"
        eq_(expr.right.type.__class__, CHAR)

    @testing.combinations(
        (5, Integer),
        (2.65, Float),
        (True, Boolean),
        (decimal.Decimal("2.65"), Numeric),
        (datetime.date(2015, 7, 20), Date),
        (datetime.time(10, 15, 20), Time),
        (datetime.datetime(2015, 7, 20, 10, 15, 20), DateTime),
        (datetime.timedelta(seconds=5), Interval),
        (None, types.NullType),
    )
    def test_actual_literal_adapters(self, data, expected):
        is_(literal(data).type.__class__, expected)

    def test_typedec_operator_adapt(self, connection):
        test_table = self.tables.test

        expr = test_table.c.bvalue + "hi"

        assert expr.type.__class__ is MyTypeDec
        assert expr.right.type.__class__ is MyTypeDec

        eq_(
            connection.execute(select(expr.label("foo"))).scalar(),
            "BIND_INfooBIND_INhiBIND_OUT",
        )

    def test_typedec_is_adapt(self):
        class CoerceNothing(TypeDecorator):
            coerce_to_is_types = ()
            impl = Integer

        class CoerceBool(TypeDecorator):
            coerce_to_is_types = (bool,)
            impl = Boolean

        class CoerceNone(TypeDecorator):
            coerce_to_is_types = (type(None),)
            impl = Integer

        c1 = column("x", CoerceNothing())
        c2 = column("x", CoerceBool())
        c3 = column("x", CoerceNone())

        self.assert_compile(
            and_(c1 == None, c2 == None, c3 == None),  # noqa
            "x = :x_1 AND x = :x_2 AND x IS NULL",
        )
        self.assert_compile(
            and_(c1 == True, c2 == True, c3 == True),  # noqa
            "x = :x_1 AND x = true AND x = :x_2",
            dialect=default.DefaultDialect(supports_native_boolean=True),
        )
        self.assert_compile(
            and_(c1 == 3, c2 == 3, c3 == 3),
            "x = :x_1 AND x = :x_2 AND x = :x_3",
            dialect=default.DefaultDialect(supports_native_boolean=True),
        )
        self.assert_compile(
            and_(c1.is_(True), c2.is_(True), c3.is_(True)),
            "x IS :x_1 AND x IS true AND x IS :x_2",
            dialect=default.DefaultDialect(supports_native_boolean=True),
        )

    def test_typedec_righthand_coercion(self, connection):
        class MyTypeDec(types.TypeDecorator):
            impl = String

            def process_bind_param(self, value, dialect):
                return "BIND_IN" + str(value)

            def process_result_value(self, value, dialect):
                return value + "BIND_OUT"

        tab = table("test", column("bvalue", MyTypeDec))
        expr = tab.c.bvalue + 6

        self.assert_compile(
            expr, "test.bvalue || :bvalue_1", use_default_dialect=True
        )

        is_(expr.right.type.__class__, MyTypeDec)
        is_(expr.type.__class__, MyTypeDec)

        eq_(
            connection.execute(select(expr.label("foo"))).scalar(),
            "BIND_INfooBIND_IN6BIND_OUT",
        )

    def test_variant_righthand_coercion_honors_wrapped(self):
        my_json_normal = JSON()
        my_json_variant = JSON().with_variant(String(), "sqlite")

        tab = table(
            "test",
            column("avalue", my_json_normal),
            column("bvalue", my_json_variant),
        )
        expr = tab.c.avalue["foo"] == "bar"

        is_(expr.right.type._type_affinity, String)
        is_not(expr.right.type, my_json_normal)

        expr = tab.c.bvalue["foo"] == "bar"

        is_(expr.right.type._type_affinity, String)
        is_not(expr.right.type, my_json_variant)

    def test_variant_righthand_coercion_returns_self(self):
        my_datetime_normal = DateTime()
        my_datetime_variant = DateTime().with_variant(
            dialects.sqlite.DATETIME(truncate_microseconds=False), "sqlite"
        )

        tab = table(
            "test",
            column("avalue", my_datetime_normal),
            column("bvalue", my_datetime_variant),
        )
        expr = tab.c.avalue == datetime.datetime(2015, 10, 14, 15, 17, 18)

        is_(expr.right.type._type_affinity, DateTime)
        is_(expr.right.type, my_datetime_normal)

        expr = tab.c.bvalue == datetime.datetime(2015, 10, 14, 15, 17, 18)

        is_(expr.right.type, my_datetime_variant)

    def test_bind_typing(self):
        from sqlalchemy.sql import column

        class MyFoobarType(types.UserDefinedType):
            pass

        class Foo(object):
            pass

        # unknown type + integer, right hand bind
        # coerces to given type
        expr = column("foo", MyFoobarType) + 5
        assert expr.right.type._type_affinity is MyFoobarType

        # untyped bind - it gets assigned MyFoobarType
        bp = bindparam("foo")
        expr = column("foo", MyFoobarType) + bp
        assert bp.type._type_affinity is types.NullType  # noqa
        assert expr.right.type._type_affinity is MyFoobarType

        expr = column("foo", MyFoobarType) + bindparam("foo", type_=Integer)
        assert expr.right.type._type_affinity is types.Integer

        # unknown type + unknown, right hand bind
        # coerces to the left
        expr = column("foo", MyFoobarType) + Foo()
        assert expr.right.type._type_affinity is MyFoobarType

        # including for non-commutative ops
        expr = column("foo", MyFoobarType) - Foo()
        assert expr.right.type._type_affinity is MyFoobarType

        expr = column("foo", MyFoobarType) - datetime.date(2010, 8, 25)
        assert expr.right.type._type_affinity is MyFoobarType

    def test_date_coercion(self):
        expr = column("bar", types.NULLTYPE) - column("foo", types.TIMESTAMP)
        eq_(expr.type._type_affinity, types.NullType)

        expr = func.sysdate() - column("foo", types.TIMESTAMP)
        eq_(expr.type._type_affinity, types.Interval)

        expr = func.current_date() - column("foo", types.TIMESTAMP)
        eq_(expr.type._type_affinity, types.Interval)

    def test_interval_coercion(self):
        expr = column("bar", types.Interval) + column("foo", types.Date)
        eq_(expr.type._type_affinity, types.DateTime)

        expr = column("bar", types.Interval) * column("foo", types.Numeric)
        eq_(expr.type._type_affinity, types.Interval)

    @testing.combinations(
        (operator.add,),
        (operator.mul,),
        (operator.truediv,),
        (operator.sub,),
        argnames="op",
        id_="n",
    )
    @testing.combinations(
        (Numeric(10, 2),), (Integer(),), argnames="other", id_="r"
    )
    def test_numerics_coercion(self, op, other):
        expr = op(column("bar", types.Numeric(10, 2)), column("foo", other))
        assert isinstance(expr.type, types.Numeric)
        expr = op(column("foo", other), column("bar", types.Numeric(10, 2)))
        assert isinstance(expr.type, types.Numeric)

    def test_asdecimal_int_to_numeric(self):
        expr = column("a", Integer) * column("b", Numeric(asdecimal=False))
        is_(expr.type.asdecimal, False)

        expr = column("a", Integer) * column("b", Numeric())
        is_(expr.type.asdecimal, True)

        expr = column("a", Integer) * column("b", Float())
        is_(expr.type.asdecimal, False)
        assert isinstance(expr.type, Float)

    def test_asdecimal_numeric_to_int(self):
        expr = column("a", Numeric(asdecimal=False)) * column("b", Integer)
        is_(expr.type.asdecimal, False)

        expr = column("a", Numeric()) * column("b", Integer)
        is_(expr.type.asdecimal, True)

        expr = column("a", Float()) * column("b", Integer)
        is_(expr.type.asdecimal, False)
        assert isinstance(expr.type, Float)

    def test_null_comparison(self):
        eq_(
            str(column("a", types.NullType()) + column("b", types.NullType())),
            "a + b",
        )

    def test_expression_typing(self):
        expr = column("bar", Integer) - 3

        eq_(expr.type._type_affinity, Integer)

        expr = bindparam("bar") + bindparam("foo")
        eq_(expr.type, types.NULLTYPE)

    def test_distinct(self, connection):
        test_table = self.tables.test

        s = select(distinct(test_table.c.avalue))
        eq_(connection.execute(s).scalar(), 25)

        s = select(test_table.c.avalue.distinct())
        eq_(connection.execute(s).scalar(), 25)

        assert distinct(test_table.c.data).type == test_table.c.data.type
        assert test_table.c.data.distinct().type == test_table.c.data.type

    def test_detect_coercion_of_builtins(self):
        @inspection._self_inspects
        class SomeSQLAThing(object):
            def __repr__(self):
                return "some_sqla_thing()"

        class SomeOtherThing(object):
            pass

        assert_raises_message(
            exc.ArgumentError,
            r"SQL expression element or literal value expected, got "
            r"some_sqla_thing\(\).",
            lambda: column("a", String) == SomeSQLAThing(),
        )

        is_(bindparam("x", SomeOtherThing()).type, types.NULLTYPE)

    def test_detect_coercion_not_fooled_by_mock(self):
        m1 = mock.Mock()
        is_(bindparam("x", m1).type, types.NULLTYPE)


class CompileTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = "default"

    @testing.requires.unbounded_varchar
    def test_string_plain(self):
        self.assert_compile(String(), "VARCHAR")

    def test_string_length(self):
        self.assert_compile(String(50), "VARCHAR(50)")

    def test_string_collation(self):
        self.assert_compile(
            String(50, collation="FOO"), 'VARCHAR(50) COLLATE "FOO"'
        )

    def test_char_plain(self):
        self.assert_compile(CHAR(), "CHAR")

    def test_char_length(self):
        self.assert_compile(CHAR(50), "CHAR(50)")

    def test_char_collation(self):
        self.assert_compile(
            CHAR(50, collation="FOO"), 'CHAR(50) COLLATE "FOO"'
        )

    def test_text_plain(self):
        self.assert_compile(Text(), "TEXT")

    def test_text_length(self):
        self.assert_compile(Text(50), "TEXT(50)")

    def test_text_collation(self):
        self.assert_compile(Text(collation="FOO"), 'TEXT COLLATE "FOO"')

    def test_default_compile_pg_inet(self):
        self.assert_compile(
            dialects.postgresql.INET(), "INET", allow_dialect_select=True
        )

    def test_default_compile_pg_float(self):
        self.assert_compile(
            dialects.postgresql.FLOAT(), "FLOAT", allow_dialect_select=True
        )

    def test_default_compile_mysql_integer(self):
        self.assert_compile(
            dialects.mysql.INTEGER(display_width=5),
            "INTEGER",
            allow_dialect_select=True,
        )

        self.assert_compile(
            dialects.mysql.INTEGER(display_width=5),
            "INTEGER(5)",
            dialect="mysql",
        )

    def test_numeric_plain(self):
        self.assert_compile(types.NUMERIC(), "NUMERIC")

    def test_numeric_precision(self):
        self.assert_compile(types.NUMERIC(2), "NUMERIC(2)")

    def test_numeric_scale(self):
        self.assert_compile(types.NUMERIC(2, 4), "NUMERIC(2, 4)")

    def test_decimal_plain(self):
        self.assert_compile(types.DECIMAL(), "DECIMAL")

    def test_decimal_precision(self):
        self.assert_compile(types.DECIMAL(2), "DECIMAL(2)")

    def test_decimal_scale(self):
        self.assert_compile(types.DECIMAL(2, 4), "DECIMAL(2, 4)")

    def test_kwarg_legacy_typecompiler(self):
        from sqlalchemy.sql import compiler

        class SomeTypeCompiler(compiler.GenericTypeCompiler):
            # transparently decorated w/ kw decorator
            def visit_VARCHAR(self, type_):
                return "MYVARCHAR"

            # not affected
            def visit_INTEGER(self, type_, **kw):
                return "MYINTEGER %s" % kw["type_expression"].name

        dialect = default.DefaultDialect()
        dialect.type_compiler = SomeTypeCompiler(dialect)
        self.assert_compile(
            ddl.CreateColumn(Column("bar", VARCHAR(50))),
            "bar MYVARCHAR",
            dialect=dialect,
        )
        self.assert_compile(
            ddl.CreateColumn(Column("bar", INTEGER)),
            "bar MYINTEGER bar",
            dialect=dialect,
        )


class TestKWArgPassThru(AssertsCompiledSQL, fixtures.TestBase):
    __backend__ = True

    def test_user_defined(self):
        """test that dialects pass the column through on DDL."""

        class MyType(types.UserDefinedType):
            def get_col_spec(self, **kw):
                return "FOOB %s" % kw["type_expression"].name

        m = MetaData()
        t = Table("t", m, Column("bar", MyType, nullable=False))
        self.assert_compile(ddl.CreateColumn(t.c.bar), "bar FOOB bar NOT NULL")


class NumericRawSQLTest(fixtures.TestBase):

    """Test what DBAPIs and dialects return without any typing
    information supplied at the SQLA level.

    """

    __backend__ = True

    def _fixture(self, connection, metadata, type_, data):
        t = Table("t", metadata, Column("val", type_))
        metadata.create_all(connection)
        connection.execute(t.insert(), dict(val=data))

    @testing.fails_on("sqlite", "Doesn't provide Decimal results natively")
    @testing.provide_metadata
    def test_decimal_fp(self, connection):
        metadata = self.metadata
        self._fixture(
            connection, metadata, Numeric(10, 5), decimal.Decimal("45.5")
        )
        val = connection.exec_driver_sql("select val from t").scalar()
        assert isinstance(val, decimal.Decimal)
        eq_(val, decimal.Decimal("45.5"))

    @testing.fails_on("sqlite", "Doesn't provide Decimal results natively")
    @testing.provide_metadata
    def test_decimal_int(self, connection):
        metadata = self.metadata
        self._fixture(
            connection, metadata, Numeric(10, 5), decimal.Decimal("45")
        )
        val = connection.exec_driver_sql("select val from t").scalar()
        assert isinstance(val, decimal.Decimal)
        eq_(val, decimal.Decimal("45"))

    @testing.provide_metadata
    def test_ints(self, connection):
        metadata = self.metadata
        self._fixture(connection, metadata, Integer, 45)
        val = connection.exec_driver_sql("select val from t").scalar()
        assert isinstance(val, util.int_types)
        eq_(val, 45)

    @testing.provide_metadata
    def test_float(self, connection):
        metadata = self.metadata
        self._fixture(connection, metadata, Float, 46.583)
        val = connection.exec_driver_sql("select val from t").scalar()
        assert isinstance(val, float)

        # some DBAPIs have unusual float handling
        if testing.against("oracle+cx_oracle", "mysql+oursql", "firebird"):
            eq_(round_decimal(val, 3), 46.583)
        else:
            eq_(val, 46.583)


class IntervalTest(fixtures.TablesTest, AssertsExecutionResults):

    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "intervals",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("native_interval", Interval()),
            Column(
                "native_interval_args",
                Interval(day_precision=3, second_precision=6),
            ),
            Column("non_native_interval", Interval(native=False)),
        )

    def test_non_native_adapt(self):
        interval = Interval(native=False)
        adapted = interval.dialect_impl(testing.db.dialect)
        assert isinstance(adapted, Interval)
        assert adapted.native is False
        eq_(str(adapted), "DATETIME")

    def test_roundtrip(self, connection):
        interval_table = self.tables.intervals

        small_delta = datetime.timedelta(days=15, seconds=5874)
        delta = datetime.timedelta(14)
        connection.execute(
            interval_table.insert(),
            dict(
                native_interval=small_delta,
                native_interval_args=delta,
                non_native_interval=delta,
            ),
        )
        row = connection.execute(interval_table.select()).first()
        eq_(row.native_interval, small_delta)
        eq_(row.native_interval_args, delta)
        eq_(row.non_native_interval, delta)

    def test_null(self, connection):
        interval_table = self.tables.intervals

        connection.execute(
            interval_table.insert(),
            dict(
                id=1,
                native_inverval=None,
                non_native_interval=None,
            ),
        )
        row = connection.execute(interval_table.select()).first()
        eq_(row.native_interval, None)
        eq_(row.native_interval_args, None)
        eq_(row.non_native_interval, None)


class IntegerTest(fixtures.TestBase):
    __backend__ = True

    def test_integer_literal_processor(self):
        typ = Integer()
        eq_(typ._cached_literal_processor(testing.db.dialect)(5), "5")

        assert_raises(
            ValueError,
            typ._cached_literal_processor(testing.db.dialect),
            "notanint",
        )


class BooleanTest(
    fixtures.TablesTest, AssertsExecutionResults, AssertsCompiledSQL
):

    """test edge cases for booleans.  Note that the main boolean test suite
    is now in testing/suite/test_types.py

    the default value of create_constraint was changed to False in
    version 1.4 with #5367.

    """

    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "boolean_table",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=False),
            Column("value", Boolean(create_constraint=True)),
            Column("unconstrained_value", Boolean()),
        )

    @testing.requires.enforces_check_constraints
    @testing.requires.non_native_boolean_unconstrained
    def test_constraint(self, connection):
        assert_raises(
            (
                exc.IntegrityError,
                exc.ProgrammingError,
                exc.OperationalError,
                exc.InternalError,  # older pymysql's do this
            ),
            connection.exec_driver_sql,
            "insert into boolean_table (id, value) values(1, 5)",
        )

    @testing.skip_if(lambda: testing.db.dialect.supports_native_boolean)
    def test_unconstrained(self, connection):
        connection.exec_driver_sql(
            "insert into boolean_table (id, unconstrained_value)"
            "values (1, 5)"
        )

    def test_non_native_constraint_custom_type(self):
        class Foob(object):
            def __init__(self, value):
                self.value = value

        class MyBool(TypeDecorator):
            impl = Boolean(create_constraint=True)

            # future method
            def process_literal_param(self, value, dialect):
                return value.value

            def process_bind_param(self, value, dialect):
                return value.value

        m = MetaData()
        t1 = Table("t", m, Column("x", MyBool()))
        const = [c for c in t1.constraints if isinstance(c, CheckConstraint)][
            0
        ]

        self.assert_compile(
            AddConstraint(const),
            "ALTER TABLE t ADD CHECK (x IN (0, 1))",
            dialect="sqlite",
        )

    @testing.skip_if(lambda: testing.db.dialect.supports_native_boolean)
    def test_nonnative_processor_coerces_to_onezero(self):
        boolean_table = self.tables.boolean_table
        with testing.db.connect() as conn:
            assert_raises_message(
                exc.StatementError,
                "Value 5 is not None, True, or False",
                conn.execute,
                boolean_table.insert(),
                {"id": 1, "unconstrained_value": 5},
            )

    @testing.requires.non_native_boolean_unconstrained
    def test_nonnative_processor_coerces_integer_to_boolean(self, connection):
        boolean_table = self.tables.boolean_table
        connection.exec_driver_sql(
            "insert into boolean_table (id, unconstrained_value) "
            "values (1, 5)"
        )

        eq_(
            connection.exec_driver_sql(
                "select unconstrained_value from boolean_table"
            ).scalar(),
            5,
        )

        eq_(
            connection.scalar(select(boolean_table.c.unconstrained_value)),
            True,
        )

    def test_bind_processor_coercion_native_true(self):
        proc = Boolean().bind_processor(
            mock.Mock(supports_native_boolean=True)
        )
        is_(proc(True), True)

    def test_bind_processor_coercion_native_false(self):
        proc = Boolean().bind_processor(
            mock.Mock(supports_native_boolean=True)
        )
        is_(proc(False), False)

    def test_bind_processor_coercion_native_none(self):
        proc = Boolean().bind_processor(
            mock.Mock(supports_native_boolean=True)
        )
        is_(proc(None), None)

    def test_bind_processor_coercion_native_0(self):
        proc = Boolean().bind_processor(
            mock.Mock(supports_native_boolean=True)
        )
        is_(proc(0), False)

    def test_bind_processor_coercion_native_1(self):
        proc = Boolean().bind_processor(
            mock.Mock(supports_native_boolean=True)
        )
        is_(proc(1), True)

    def test_bind_processor_coercion_native_str(self):
        proc = Boolean().bind_processor(
            mock.Mock(supports_native_boolean=True)
        )
        assert_raises_message(
            TypeError, "Not a boolean value: 'foo'", proc, "foo"
        )

    def test_bind_processor_coercion_native_int_out_of_range(self):
        proc = Boolean().bind_processor(
            mock.Mock(supports_native_boolean=True)
        )
        assert_raises_message(
            ValueError, "Value 15 is not None, True, or False", proc, 15
        )

    def test_bind_processor_coercion_nonnative_true(self):
        proc = Boolean().bind_processor(
            mock.Mock(supports_native_boolean=False)
        )
        eq_(proc(True), 1)

    def test_bind_processor_coercion_nonnative_false(self):
        proc = Boolean().bind_processor(
            mock.Mock(supports_native_boolean=False)
        )
        eq_(proc(False), 0)

    def test_bind_processor_coercion_nonnative_none(self):
        proc = Boolean().bind_processor(
            mock.Mock(supports_native_boolean=False)
        )
        is_(proc(None), None)

    def test_bind_processor_coercion_nonnative_0(self):
        proc = Boolean().bind_processor(
            mock.Mock(supports_native_boolean=False)
        )
        eq_(proc(0), 0)

    def test_bind_processor_coercion_nonnative_1(self):
        proc = Boolean().bind_processor(
            mock.Mock(supports_native_boolean=False)
        )
        eq_(proc(1), 1)

    def test_bind_processor_coercion_nonnative_str(self):
        proc = Boolean().bind_processor(
            mock.Mock(supports_native_boolean=False)
        )
        assert_raises_message(
            TypeError, "Not a boolean value: 'foo'", proc, "foo"
        )

    def test_bind_processor_coercion_nonnative_int_out_of_range(self):
        proc = Boolean().bind_processor(
            mock.Mock(supports_native_boolean=False)
        )
        assert_raises_message(
            ValueError, "Value 15 is not None, True, or False", proc, 15
        )

    def test_literal_processor_coercion_native_true(self):
        proc = Boolean().literal_processor(
            default.DefaultDialect(supports_native_boolean=True)
        )
        eq_(proc(True), "true")

    def test_literal_processor_coercion_native_false(self):
        proc = Boolean().literal_processor(
            default.DefaultDialect(supports_native_boolean=True)
        )
        eq_(proc(False), "false")

    def test_literal_processor_coercion_native_1(self):
        proc = Boolean().literal_processor(
            default.DefaultDialect(supports_native_boolean=True)
        )
        eq_(proc(1), "true")

    def test_literal_processor_coercion_native_0(self):
        proc = Boolean().literal_processor(
            default.DefaultDialect(supports_native_boolean=True)
        )
        eq_(proc(0), "false")

    def test_literal_processor_coercion_native_str(self):
        proc = Boolean().literal_processor(
            default.DefaultDialect(supports_native_boolean=True)
        )
        assert_raises_message(
            TypeError, "Not a boolean value: 'foo'", proc, "foo"
        )

    def test_literal_processor_coercion_native_int_out_of_range(self):
        proc = Boolean().literal_processor(
            default.DefaultDialect(supports_native_boolean=True)
        )
        assert_raises_message(
            ValueError, "Value 15 is not None, True, or False", proc, 15
        )

    def test_literal_processor_coercion_nonnative_true(self):
        proc = Boolean().literal_processor(
            default.DefaultDialect(supports_native_boolean=False)
        )
        eq_(proc(True), "1")

    def test_literal_processor_coercion_nonnative_false(self):
        proc = Boolean().literal_processor(
            default.DefaultDialect(supports_native_boolean=False)
        )
        eq_(proc(False), "0")

    def test_literal_processor_coercion_nonnative_1(self):
        proc = Boolean().literal_processor(
            default.DefaultDialect(supports_native_boolean=False)
        )
        eq_(proc(1), "1")

    def test_literal_processor_coercion_nonnative_0(self):
        proc = Boolean().literal_processor(
            default.DefaultDialect(supports_native_boolean=False)
        )
        eq_(proc(0), "0")

    def test_literal_processor_coercion_nonnative_str(self):
        proc = Boolean().literal_processor(
            default.DefaultDialect(supports_native_boolean=False)
        )
        assert_raises_message(
            TypeError, "Not a boolean value: 'foo'", proc, "foo"
        )


class PickleTest(fixtures.TestBase):
    def test_eq_comparison(self):
        p1 = PickleType()

        for obj in (
            {"1": "2"},
            pickleable.Bar(5, 6),
            pickleable.OldSchool(10, 11),
        ):
            assert p1.compare_values(p1.copy_value(obj), obj)

        assert_raises(
            NotImplementedError,
            p1.compare_values,
            pickleable.BrokenComparable("foo"),
            pickleable.BrokenComparable("foo"),
        )

    def test_nonmutable_comparison(self):
        p1 = PickleType()

        for obj in (
            {"1": "2"},
            pickleable.Bar(5, 6),
            pickleable.OldSchool(10, 11),
        ):
            assert p1.compare_values(p1.copy_value(obj), obj)


class CallableTest(fixtures.TestBase):
    @testing.provide_metadata
    def test_callable_as_arg(self, connection):
        ucode = util.partial(Unicode)

        thing_table = Table("thing", self.metadata, Column("name", ucode(20)))
        assert isinstance(thing_table.c.name.type, Unicode)
        thing_table.create(connection)

    @testing.provide_metadata
    def test_callable_as_kwarg(self, connection):
        ucode = util.partial(Unicode)

        thang_table = Table(
            "thang",
            self.metadata,
            Column("name", type_=ucode(20), primary_key=True),
        )
        assert isinstance(thang_table.c.name.type, Unicode)
        thang_table.create(connection)


class LiteralTest(fixtures.TestBase):
    __backend__ = True

    @testing.combinations(
        ("datetime", datetime.datetime.now()),
        ("date", datetime.date.today()),
        ("time", datetime.time()),
        argnames="value",
        id_="ia",
    )
    @testing.skip_if(testing.requires.datetime_literals)
    def test_render_datetime(self, value):
        lit = literal(value)

        assert_raises_message(
            NotImplementedError,
            "Don't know how to literal-quote value.*",
            lit.compile,
            dialect=testing.db.dialect,
            compile_kwargs={"literal_binds": True},
        )
