# coding: utf-8
from sqlalchemy.testing import eq_, is_, assert_raises, \
    assert_raises_message, expect_warnings
import decimal
import datetime
import os
from sqlalchemy import (
    Unicode, MetaData, PickleType, Boolean, TypeDecorator, Integer,
    Interval, Float, Numeric, Text, CHAR, String, distinct, select, bindparam,
    and_, func, Date, LargeBinary, literal, cast, text, Enum,
    type_coerce, VARCHAR, Time, DateTime, BigInteger, SmallInteger, BOOLEAN,
    BLOB, NCHAR, NVARCHAR, CLOB, TIME, DATE, DATETIME, TIMESTAMP, SMALLINT,
    INTEGER, DECIMAL, NUMERIC, FLOAT, REAL, ARRAY, JSON)
from sqlalchemy.sql import ddl
from sqlalchemy.sql import visitors
from sqlalchemy import inspection
from sqlalchemy import exc, types, util, dialects
from sqlalchemy.util import OrderedDict
for name in dialects.__all__:
    __import__("sqlalchemy.dialects.%s" % name)
from sqlalchemy.sql import operators, column, table, null
from sqlalchemy.schema import CheckConstraint, AddConstraint
from sqlalchemy.engine import default
from sqlalchemy.testing.schema import Table, Column
from sqlalchemy import testing
from sqlalchemy.testing import AssertsCompiledSQL, AssertsExecutionResults, \
    engines, pickleable
from sqlalchemy.testing.util import picklers
from sqlalchemy.testing.util import round_decimal
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import mock




class AdaptTest(fixtures.TestBase):

    def _all_dialect_modules(self):
        return [
            getattr(dialects, d)
            for d in dialects.__all__
            if not d.startswith('_')
        ]

    def _all_dialects(self):
        return [d.base.dialect() for d in
                self._all_dialect_modules()]

    def _types_for_mod(self, mod):
        for key in dir(mod):
            typ = getattr(mod, key)
            if not isinstance(typ, type) or \
                    not issubclass(typ, types.TypeEngine):
                continue
            yield typ

    def _all_types(self):
        for typ in self._types_for_mod(types):
            yield typ
        for dialect in self._all_dialect_modules():
            for typ in self._types_for_mod(dialect):
                yield typ

    def test_uppercase_importable(self):
        import sqlalchemy as sa
        for typ in self._types_for_mod(types):
            if typ.__name__ == typ.__name__.upper():
                assert getattr(sa, typ.__name__) is typ
                assert typ.__name__ in types.__all__

    def test_uppercase_rendering(self):
        """Test that uppercase types from types.py always render as their
        type.

        As of SQLA 0.6, using an uppercase type means you want specifically
        that type. If the database in use doesn't support that DDL, it (the DB
        backend) should raise an error - it means you should be using a
        lowercased (genericized) type.

        """

        for dialect in self._all_dialects():
            for type_, expected in (
                (REAL, "REAL"),
                (FLOAT, "FLOAT"),
                (NUMERIC, "NUMERIC"),
                (DECIMAL, "DECIMAL"),
                (INTEGER, "INTEGER"),
                (SMALLINT, "SMALLINT"),
                (TIMESTAMP, ("TIMESTAMP", "TIMESTAMP WITHOUT TIME ZONE")),
                (DATETIME, "DATETIME"),
                (DATE, "DATE"),
                (TIME, ("TIME", "TIME WITHOUT TIME ZONE")),
                (CLOB, "CLOB"),
                (VARCHAR(10), ("VARCHAR(10)", "VARCHAR(10 CHAR)")),
                (NVARCHAR(10), (
                    "NVARCHAR(10)", "NATIONAL VARCHAR(10)", "NVARCHAR2(10)")),
                (CHAR, "CHAR"),
                (NCHAR, ("NCHAR", "NATIONAL CHAR")),
                (BLOB, ("BLOB", "BLOB SUB_TYPE 0")),
                (BOOLEAN, ("BOOLEAN", "BOOL", "INTEGER"))
            ):
                if isinstance(expected, str):
                    expected = (expected, )

                try:
                    compiled = types.to_instance(type_).\
                        compile(dialect=dialect)
                except NotImplementedError:
                    continue

                assert compiled in expected, \
                    "%r matches none of %r for dialect %s" % \
                    (compiled, expected, dialect.name)

                assert str(types.to_instance(type_)) in expected, \
                    "default str() of type %r not expected, %r" % \
                    (type_, expected)

    @testing.uses_deprecated()
    def test_adapt_method(self):
        """ensure all types have a working adapt() method,
        which creates a distinct copy.

        The distinct copy ensures that when we cache
        the adapted() form of a type against the original
        in a weak key dictionary, a cycle is not formed.

        This test doesn't test type-specific arguments of
        adapt() beyond their defaults.

        """

        def adaptions():
            for typ in self._all_types():
                up_adaptions = [typ] + typ.__subclasses__()
                yield False, typ, up_adaptions
                for subcl in typ.__subclasses__():
                    if subcl is not typ and typ is not TypeDecorator and \
                            "sqlalchemy" in subcl.__module__:
                        yield True, subcl, [typ]

        for is_down_adaption, typ, target_adaptions in adaptions():
            if typ in (types.TypeDecorator, types.TypeEngine, types.Variant):
                continue
            elif issubclass(typ, ARRAY):
                t1 = typ(String)
            else:
                t1 = typ()
            for cls in target_adaptions:
                if not issubclass(typ, types.Enum) and \
                        issubclass(cls, types.Enum):
                    continue
                if cls.__module__.startswith("test"):
                    continue

                # print("ADAPT %s -> %s" % (t1.__class__, cls))
                t2 = t1.adapt(cls)
                assert t1 is not t2

                if is_down_adaption:
                    t2, t1 = t1, t2

                for k in t1.__dict__:
                    if k in ('impl', '_is_oracle_number', '_create_events'):
                        continue
                    # assert each value was copied, or that
                    # the adapted type has a more specific
                    # value than the original (i.e. SQL Server
                    # applies precision=24 for REAL)
                    assert \
                        getattr(t2, k) == t1.__dict__[k] or \
                        t1.__dict__[k] is None

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
        eq_(types.String(convert_unicode=True).python_type, util.text_type)
        eq_(types.Enum('one', 'two', 'three').python_type, str)

        assert_raises(
            NotImplementedError,
            lambda: types.TypeEngine().python_type
        )

    @testing.uses_deprecated()
    def test_repr(self):
        for typ in self._all_types():
            if typ in (types.TypeDecorator, types.TypeEngine, types.Variant):
                continue
            elif issubclass(typ, ARRAY):
                t1 = typ(String)
            else:
                t1 = typ()
            repr(t1)

    def test_adapt_constructor_copy_override_kw(self):
        """test that adapt() can accept kw args that override
        the state of the original object.

        This essentially is testing the behavior of util.constructor_copy().

        """
        t1 = String(length=50, convert_unicode=False)
        t2 = t1.adapt(Text, convert_unicode=True)
        eq_(
            t2.length, 50
        )
        eq_(
            t2.convert_unicode, True
        )


class TypeAffinityTest(fixtures.TestBase):

    def test_type_affinity(self):
        for type_, affin in [
            (String(), String),
            (VARCHAR(), String),
            (Date(), Date),
            (LargeBinary(), types._Binary)
        ]:
            eq_(type_._type_affinity, affin)

        for t1, t2, comp in [
            (Integer(), SmallInteger(), True),
            (Integer(), String(), False),
            (Integer(), Integer(), True),
            (Text(), String(), True),
            (Text(), Unicode(), True),
            (LargeBinary(), Integer(), False),
            (LargeBinary(), PickleType(), True),
            (PickleType(), LargeBinary(), True),
            (PickleType(), PickleType(), True),
        ]:
            eq_(t1._compare_type_affinity(t2), comp, "%s %s" % (t1, t2))

    def test_decorator_doesnt_cache(self):
        from sqlalchemy.dialects import postgresql

        class MyType(TypeDecorator):
            impl = CHAR

            def load_dialect_impl(self, dialect):
                if dialect.name == 'postgresql':
                    return dialect.type_descriptor(postgresql.UUID())
                else:
                    return dialect.type_descriptor(CHAR(32))

        t1 = MyType()
        d = postgresql.dialect()
        assert t1._type_affinity is String
        assert t1.dialect_impl(d)._type_affinity is postgresql.UUID


class PickleTypesTest(fixtures.TestBase):

    def test_pickle_types(self):
        for loads, dumps in picklers():
            column_types = [
                Column('Boo', Boolean()),
                Column('Str', String()),
                Column('Tex', Text()),
                Column('Uni', Unicode()),
                Column('Int', Integer()),
                Column('Sma', SmallInteger()),
                Column('Big', BigInteger()),
                Column('Num', Numeric()),
                Column('Flo', Float()),
                Column('Dat', DateTime()),
                Column('Dat', Date()),
                Column('Tim', Time()),
                Column('Lar', LargeBinary()),
                Column('Pic', PickleType()),
                Column('Int', Interval()),
            ]
            for column_type in column_types:
                meta = MetaData()
                Table('foo', meta, column_type)
                loads(dumps(column_type))
                loads(dumps(meta))


class UserDefinedTest(fixtures.TablesTest, AssertsCompiledSQL):

    """tests user-defined types."""

    def test_processing(self):
        users = self.tables.users
        users.insert().execute(
            user_id=2, goofy='jack', goofy2='jack', goofy4=util.u('jack'),
            goofy7=util.u('jack'), goofy8=12, goofy9=12)
        users.insert().execute(
            user_id=3, goofy='lala', goofy2='lala', goofy4=util.u('lala'),
            goofy7=util.u('lala'), goofy8=15, goofy9=15)
        users.insert().execute(
            user_id=4, goofy='fred', goofy2='fred', goofy4=util.u('fred'),
            goofy7=util.u('fred'), goofy8=9, goofy9=9)

        l = users.select().order_by(users.c.user_id).execute().fetchall()
        for assertstr, assertint, assertint2, row in zip(
            [
                "BIND_INjackBIND_OUT", "BIND_INlalaBIND_OUT",
                "BIND_INfredBIND_OUT"],
            [1200, 1500, 900],
            [1800, 2250, 1350],
            l
        ):
            for col in list(row)[1:5]:
                eq_(col, assertstr)
            eq_(row[5], assertint)
            eq_(row[6], assertint2)
            for col in row[3], row[4]:
                assert isinstance(col, util.text_type)

    def test_typedecorator_literal_render(self):
        class MyType(types.TypeDecorator):
            impl = String

            def process_literal_param(self, value, dialect):
                return "HI->%s<-THERE" % value

        self.assert_compile(
            select([literal("test", MyType)]),
            "SELECT 'HI->test<-THERE' AS anon_1",
            dialect='default',
            literal_binds=True
        )

    def test_kw_colspec(self):
        class MyType(types.UserDefinedType):
            def get_col_spec(self, **kw):
                return "FOOB %s" % kw['type_expression'].name

        class MyOtherType(types.UserDefinedType):
            def get_col_spec(self):
                return "BAR"

        self.assert_compile(
            ddl.CreateColumn(Column('bar', MyType)),
            "bar FOOB bar"
        )
        self.assert_compile(
            ddl.CreateColumn(Column('bar', MyOtherType)),
            "bar BAR"
        )

    def test_typedecorator_literal_render_fallback_bound(self):
        # fall back to process_bind_param for literal
        # value rendering.
        class MyType(types.TypeDecorator):
            impl = String

            def process_bind_param(self, value, dialect):
                return "HI->%s<-THERE" % value

        self.assert_compile(
            select([literal("test", MyType)]),
            "SELECT 'HI->test<-THERE' AS anon_1",
            dialect='default',
            literal_binds=True
        )

    def test_typedecorator_impl(self):
        for impl_, exp, kw in [
            (Float, "FLOAT", {}),
            (Float, "FLOAT(2)", {'precision': 2}),
            (Float(2), "FLOAT(2)", {'precision': 4}),
            (Numeric(19, 2), "NUMERIC(19, 2)", {}),
        ]:
            for dialect_ in (
                    dialects.postgresql, dialects.mssql, dialects.mysql):
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
                    raw_dialect_impl.__class__,
                    dec_dialect_impl.impl.__class__)

                self.assert_compile(
                    MyType(**kw),
                    exp,
                    dialect=dialect_
                )

    def test_user_defined_typedec_impl(self):
        class MyType(types.TypeDecorator):
            impl = Float

            def load_dialect_impl(self, dialect):
                if dialect.name == 'sqlite':
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
            String().dialect_impl(dialect=sl).__class__
        )
        eq_(
            t.dialect_impl(dialect=pg).impl.__class__,
            Float().dialect_impl(pg).__class__
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
                if dialect.name == 'sqlite':
                    return TypeOne()
                else:
                    return TypeTwo()

            def process_bind_param(self, value, dialect):
                return "MYTYPE " + value
        sl = dialects.sqlite.dialect()
        pg = dialects.postgresql.dialect()
        t = MyType()
        eq_(
            t._cached_bind_processor(sl)('foo'),
            "MYTYPE foo ONE"
        )
        eq_(
            t._cached_bind_processor(pg)('foo'),
            "MYTYPE foo TWO"
        )

    def test_user_defined_dialect_specific_args(self):
        class MyType(types.UserDefinedType):

            def __init__(self, foo='foo', **kwargs):
                super(MyType, self).__init__()
                self.foo = foo
                self.dialect_specific_args = kwargs

            def adapt(self, cls):
                return cls(foo=self.foo, **self.dialect_specific_args)
        t = MyType(bar='bar')
        a = t.dialect_impl(testing.db.dialect)
        eq_(a.foo, 'foo')
        eq_(a.dialect_specific_args['bar'], 'bar')

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
                impl_processor = super(MyDecoratedType, self).\
                    bind_processor(dialect) or (lambda value: value)

                def process(value):
                    return "BIND_IN" + impl_processor(value)
                return process

            def result_processor(self, dialect, coltype):
                impl_processor = super(MyDecoratedType, self).\
                    result_processor(dialect, coltype) or (lambda value: value)

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
                impl_processor = super(MyUnicodeType, self).\
                    bind_processor(dialect) or (lambda value: value)

                def process(value):
                    return "BIND_IN" + impl_processor(value)
                return process

            def result_processor(self, dialect, coltype):
                impl_processor = super(MyUnicodeType, self).\
                    result_processor(dialect, coltype) or (lambda value: value)

                def process(value):
                    return impl_processor(value) + "BIND_OUT"
                return process

            def copy(self):
                return MyUnicodeType(self.impl.length)

        Table(
            'users', metadata,
            Column('user_id', Integer, primary_key=True),
            # totall custom type
            Column('goofy', MyType, nullable=False),

            # decorated type with an argument, so its a String
            Column('goofy2', MyDecoratedType(50), nullable=False),

            Column('goofy4', MyUnicodeType(50), nullable=False),
            Column('goofy7', MyNewUnicodeType(50), nullable=False),
            Column('goofy8', MyNewIntType, nullable=False),
            Column('goofy9', MyNewIntSubClass, nullable=False),
        )


class TypeCoerceCastTest(fixtures.TablesTest):

    @classmethod
    def define_tables(cls, metadata):
        class MyType(types.TypeDecorator):
            impl = String(50)

            def process_bind_param(self, value, dialect):
                return "BIND_IN" + str(value)

            def process_result_value(self, value, dialect):
                return value + "BIND_OUT"

        cls.MyType = MyType

        Table('t', metadata, Column('data', String(50)))

    @testing.fails_on(
        "oracle", "oracle doesn't like CAST in the VALUES of an INSERT")
    @testing.fails_on(
        "mysql", "mysql dialect warns on skipped CAST")
    def test_insert_round_trip_cast(self):
        self._test_insert_round_trip(cast)

    def test_insert_round_trip_type_coerce(self):
        self._test_insert_round_trip(type_coerce)

    def _test_insert_round_trip(self, coerce_fn):
        MyType = self.MyType
        t = self.tables.t

        t.insert().values(data=coerce_fn('d1', MyType)).execute()

        eq_(
            select([coerce_fn(t.c.data, MyType)]).execute().fetchall(),
            [('BIND_INd1BIND_OUT', )]
        )

    @testing.fails_on(
        "oracle", "ORA-00906: missing left parenthesis - "
        "seems to be CAST(:param AS type)")
    @testing.fails_on(
        "mysql", "mysql dialect warns on skipped CAST")
    def test_coerce_from_nulltype_cast(self):
        self._test_coerce_from_nulltype(cast)

    def test_coerce_from_nulltype_type_coerce(self):
        self._test_coerce_from_nulltype(type_coerce)

    def _test_coerce_from_nulltype(self, coerce_fn):
        MyType = self.MyType

        # test coerce from nulltype - e.g. use an object that
        # does't match to a known type
        class MyObj(object):

            def __str__(self):
                return "THISISMYOBJ"

        t = self.tables.t

        t.insert().values(data=coerce_fn(MyObj(), MyType)).execute()

        eq_(
            select([coerce_fn(t.c.data, MyType)]).execute().fetchall(),
            [('BIND_INTHISISMYOBJBIND_OUT',)]
        )

    @testing.fails_on(
        "oracle", "oracle doesn't like CAST in the VALUES of an INSERT")
    @testing.fails_on(
        "mysql", "mysql dialect warns on skipped CAST")
    def test_vs_non_coerced_cast(self):
        self._test_vs_non_coerced(cast)

    def test_vs_non_coerced_type_coerce(self):
        self._test_vs_non_coerced(type_coerce)

    def _test_vs_non_coerced(self, coerce_fn):
        MyType = self.MyType
        t = self.tables.t

        t.insert().values(data=coerce_fn('d1', MyType)).execute()

        eq_(
            select(
                [t.c.data, coerce_fn(t.c.data, MyType)]).execute().fetchall(),
            [('BIND_INd1', 'BIND_INd1BIND_OUT')]
        )

    @testing.fails_on(
        "oracle", "oracle doesn't like CAST in the VALUES of an INSERT")
    @testing.fails_on(
        "mysql", "mysql dialect warns on skipped CAST")
    def test_vs_non_coerced_alias_cast(self):
        self._test_vs_non_coerced_alias(cast)

    def test_vs_non_coerced_alias_type_coerce(self):
        self._test_vs_non_coerced_alias(type_coerce)

    def _test_vs_non_coerced_alias(self, coerce_fn):
        MyType = self.MyType
        t = self.tables.t

        t.insert().values(data=coerce_fn('d1', MyType)).execute()

        eq_(
            select([t.c.data, coerce_fn(t.c.data, MyType)]).
            alias().select().execute().fetchall(),
            [('BIND_INd1', 'BIND_INd1BIND_OUT')]
        )

    @testing.fails_on(
        "oracle", "oracle doesn't like CAST in the VALUES of an INSERT")
    @testing.fails_on(
        "mysql", "mysql dialect warns on skipped CAST")
    def test_vs_non_coerced_where_cast(self):
        self._test_vs_non_coerced_where(cast)

    def test_vs_non_coerced_where_type_coerce(self):
        self._test_vs_non_coerced_where(type_coerce)

    def _test_vs_non_coerced_where(self, coerce_fn):
        MyType = self.MyType

        t = self.tables.t
        t.insert().values(data=coerce_fn('d1', MyType)).execute()

        # coerce on left side
        eq_(
            select([t.c.data, coerce_fn(t.c.data, MyType)]).
            where(coerce_fn(t.c.data, MyType) == 'd1').execute().fetchall(),
            [('BIND_INd1', 'BIND_INd1BIND_OUT')]
        )

        # coerce on right side
        eq_(
            select([t.c.data, coerce_fn(t.c.data, MyType)]).
            where(t.c.data == coerce_fn('d1', MyType)).execute().fetchall(),
            [('BIND_INd1', 'BIND_INd1BIND_OUT')]
        )

    @testing.fails_on(
        "oracle", "oracle doesn't like CAST in the VALUES of an INSERT")
    @testing.fails_on(
        "mysql", "mysql dialect warns on skipped CAST")
    def test_coerce_none_cast(self):
        self._test_coerce_none(cast)

    def test_coerce_none_type_coerce(self):
        self._test_coerce_none(type_coerce)

    def _test_coerce_none(self, coerce_fn):
        MyType = self.MyType

        t = self.tables.t
        t.insert().values(data=coerce_fn('d1', MyType)).execute()
        eq_(
            select([t.c.data, coerce_fn(t.c.data, MyType)]).
            where(t.c.data == coerce_fn(None, MyType)).execute().fetchall(),
            []
        )

        eq_(
            select([t.c.data, coerce_fn(t.c.data, MyType)]).
            where(coerce_fn(t.c.data, MyType) == None).  # noqa
            execute().fetchall(),
            []
        )

    @testing.fails_on(
        "oracle", "oracle doesn't like CAST in the VALUES of an INSERT")
    @testing.fails_on(
        "mysql", "mysql dialect warns on skipped CAST")
    def test_resolve_clause_element_cast(self):
        self._test_resolve_clause_element(cast)

    def test_resolve_clause_element_type_coerce(self):
        self._test_resolve_clause_element(type_coerce)

    def _test_resolve_clause_element(self, coerce_fn):
        MyType = self.MyType

        t = self.tables.t
        t.insert().values(data=coerce_fn('d1', MyType)).execute()

        class MyFoob(object):

            def __clause_element__(self):
                return t.c.data

        eq_(
            testing.db.execute(
                select([t.c.data, coerce_fn(MyFoob(), MyType)])
            ).fetchall(),
            [('BIND_INd1', 'BIND_INd1BIND_OUT')]
        )

    def test_cast_replace_col_w_bind(self):
        self._test_replace_col_w_bind(cast)

    def test_type_coerce_replace_col_w_bind(self):
        self._test_replace_col_w_bind(type_coerce)

    def _test_replace_col_w_bind(self, coerce_fn):
        MyType = self.MyType

        t = self.tables.t
        t.insert().values(data=coerce_fn('d1', MyType)).execute()

        stmt = select([t.c.data, coerce_fn(t.c.data, MyType)])

        def col_to_bind(col):
            if col is t.c.data:
                return bindparam(None, "x", type_=col.type, unique=True)
            return None

        # ensure we evaulate the expression so that we can see
        # the clone resets this info
        stmt.compile()

        new_stmt = visitors.replacement_traverse(stmt, {}, col_to_bind)

        # original statement
        eq_(
            testing.db.execute(stmt).fetchall(),
            [('BIND_INd1', 'BIND_INd1BIND_OUT')]
        )

        # replaced with binds; CAST can't affect the bound parameter
        # on the way in here
        eq_(
            testing.db.execute(new_stmt).fetchall(),
            [('x', 'BIND_INxBIND_OUT')] if coerce_fn is type_coerce
            else [('x', 'xBIND_OUT')]
        )

    def test_cast_bind(self):
        self._test_bind(cast)

    def test_type_bind(self):
        self._test_bind(type_coerce)

    def _test_bind(self, coerce_fn):
        MyType = self.MyType

        t = self.tables.t
        t.insert().values(data=coerce_fn('d1', MyType)).execute()

        stmt = select([
            bindparam(None, "x", String(50), unique=True),
            coerce_fn(bindparam(None, "x", String(50), unique=True), MyType)
        ])

        eq_(
            testing.db.execute(stmt).fetchall(),
            [('x', 'BIND_INxBIND_OUT')] if coerce_fn is type_coerce
            else [('x', 'xBIND_OUT')]
        )

    @testing.fails_on(
        "oracle", "ORA-00906: missing left parenthesis - "
        "seems to be CAST(:param AS type)")
    @testing.fails_on(
        "mysql", "mysql dialect warns on skipped CAST")
    def test_cast_existing_typed(self):
        MyType = self.MyType
        coerce_fn = cast

        # when cast() is given an already typed value,
        # the type does not take effect on the value itself.
        eq_(
            testing.db.scalar(
                select([coerce_fn(literal('d1'), MyType)])
            ),
            'd1BIND_OUT'
        )

    def test_type_coerce_existing_typed(self):
        MyType = self.MyType
        coerce_fn = type_coerce
        t = self.tables.t

        # type_coerce does upgrade the given expression to the
        # given type.

        t.insert().values(data=coerce_fn(literal('d1'), MyType)).execute()

        eq_(
            select([coerce_fn(t.c.data, MyType)]).execute().fetchall(),
            [('BIND_INd1BIND_OUT', )])



class VariantTest(fixtures.TestBase, AssertsCompiledSQL):

    def setup(self):
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
            self.UTypeTwo(), 'postgresql')
        self.composite = self.variant.with_variant(self.UTypeThree(), 'mysql')

    def test_illegal_dupe(self):
        v = self.UTypeOne().with_variant(
            self.UTypeTwo(), 'postgresql'
        )
        assert_raises_message(
            exc.ArgumentError,
            "Dialect 'postgresql' is already present "
            "in the mapping for this Variant",
            lambda: v.with_variant(self.UTypeThree(), 'postgresql')
        )

    def test_compile(self):
        self.assert_compile(
            self.variant,
            "UTYPEONE",
            use_default_dialect=True
        )
        self.assert_compile(
            self.variant,
            "UTYPEONE",
            dialect=dialects.mysql.dialect()
        )
        self.assert_compile(
            self.variant,
            "UTYPETWO",
            dialect=dialects.postgresql.dialect()
        )

    def test_to_instance(self):
        self.assert_compile(
            self.UTypeOne().with_variant(self.UTypeTwo, "postgresql"),
            "UTYPETWO",
            dialect=dialects.postgresql.dialect()
        )

    def test_compile_composite(self):
        self.assert_compile(
            self.composite,
            "UTYPEONE",
            use_default_dialect=True
        )
        self.assert_compile(
            self.composite,
            "UTYPETHREE",
            dialect=dialects.mysql.dialect()
        )
        self.assert_compile(
            self.composite,
            "UTYPETWO",
            dialect=dialects.postgresql.dialect()
        )

    def test_bind_process(self):
        eq_(
            self.variant._cached_bind_processor(
                dialects.mysql.dialect())('foo'),
            'fooUONE'
        )
        eq_(
            self.variant._cached_bind_processor(
                default.DefaultDialect())('foo'),
            'fooUONE'
        )
        eq_(
            self.variant._cached_bind_processor(
                dialects.postgresql.dialect())('foo'),
            'fooUTWO'
        )

    def test_bind_process_composite(self):
        assert self.composite._cached_bind_processor(
            dialects.mysql.dialect()) is None
        eq_(
            self.composite._cached_bind_processor(
                default.DefaultDialect())('foo'),
            'fooUONE'
        )
        eq_(
            self.composite._cached_bind_processor(
                dialects.postgresql.dialect())('foo'),
            'fooUTWO'
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
        "Elle disait: « S’il vous plaît… dessine-moi un mouton! »")

    def test_unicode_warnings_typelevel_native_unicode(self):

        unicodedata = self.data
        u = Unicode()
        dialect = default.DefaultDialect()
        dialect.supports_unicode_binds = True
        uni = u.dialect_impl(dialect).bind_processor(dialect)
        if util.py3k:
            assert_raises(exc.SAWarning, uni, b'x')
            assert isinstance(uni(unicodedata), str)
        else:
            assert_raises(exc.SAWarning, uni, 'x')
            assert isinstance(uni(unicodedata), unicode)  # noqa

    def test_unicode_warnings_typelevel_sqla_unicode(self):
        unicodedata = self.data
        u = Unicode()
        dialect = default.DefaultDialect()
        dialect.supports_unicode_binds = False
        uni = u.dialect_impl(dialect).bind_processor(dialect)
        assert_raises(exc.SAWarning, uni, util.b('x'))
        assert isinstance(uni(unicodedata), util.binary_type)

        eq_(uni(unicodedata), unicodedata.encode('utf-8'))

    def test_unicode_warnings_totally_wrong_type(self):
        u = Unicode()
        dialect = default.DefaultDialect()
        dialect.supports_unicode_binds = False
        uni = u.dialect_impl(dialect).bind_processor(dialect)
        with expect_warnings(
                "Unicode type received non-unicode bind param value 5."):
            eq_(uni(5), 5)

    def test_unicode_warnings_dialectlevel(self):

        unicodedata = self.data

        dialect = default.DefaultDialect(convert_unicode=True)
        dialect.supports_unicode_binds = False

        s = String()
        uni = s.dialect_impl(dialect).bind_processor(dialect)

        uni(util.b('x'))
        assert isinstance(uni(unicodedata), util.binary_type)

        eq_(uni(unicodedata), unicodedata.encode('utf-8'))

    def test_ignoring_unicode_error(self):
        """checks String(unicode_error='ignore') is passed to
        underlying codec."""

        unicodedata = self.data

        type_ = String(248, convert_unicode='force', unicode_error='ignore')
        dialect = default.DefaultDialect(encoding='ascii')
        proc = type_.result_processor(dialect, 10)

        utfdata = unicodedata.encode('utf8')
        eq_(
            proc(utfdata),
            unicodedata.encode('ascii', 'ignore').decode()
        )


class EnumTest(AssertsCompiledSQL, fixtures.TablesTest):
    __backend__ = True

    class SomeEnum(object):
        # Implements PEP 435 in the minimal fashion needed by SQLAlchemy
        __members__ = OrderedDict()

        def __init__(self, name, value):
            self.name = name
            self.value = value
            self.__members__[name] = self
            setattr(self.__class__, name, self)

    one = SomeEnum('one', 1)
    two = SomeEnum('two', 2)
    three = SomeEnum('three', 3)

    @classmethod
    def define_tables(cls, metadata):
        Table(
            'enum_table', metadata, Column("id", Integer, primary_key=True),
            Column('someenum', Enum('one', 'two', 'three', name='myenum'))
        )

        Table(
            'non_native_enum_table', metadata,
            Column("id", Integer, primary_key=True),
            Column('someenum', Enum('one', 'two', 'three', native_enum=False)),
            Column('someotherenum',
                Enum('one', 'two', 'three',
                     create_constraint=False, native_enum=False,
                     validate_strings=True)),
        )

        Table(
            'stdlib_enum_table', metadata,
            Column("id", Integer, primary_key=True),
            Column('someenum', Enum(cls.SomeEnum))
        )

    def test_python_type(self):
        eq_(types.Enum(self.SomeEnum).python_type, self.SomeEnum)

    def test_pickle_types(self):
        global SomeEnum
        SomeEnum = self.SomeEnum
        for loads, dumps in picklers():
            column_types = [
                Column('Enu', Enum('x', 'y', 'z', name="somename")),
                Column('En2', Enum(self.SomeEnum)),
            ]
            for column_type in column_types:
                meta = MetaData()
                Table('foo', meta, column_type)
                loads(dumps(column_type))
                loads(dumps(meta))

    def test_validators_pep435(self):
        type_ = Enum(self.SomeEnum)
        validate_type = Enum(self.SomeEnum, validate_strings=True)

        bind_processor = type_.bind_processor(testing.db.dialect)
        bind_processor_validates = validate_type.bind_processor(
            testing.db.dialect)
        eq_(bind_processor('one'), "one")
        eq_(bind_processor(self.one), "one")
        eq_(bind_processor("foo"), "foo")
        assert_raises_message(
            LookupError,
            '"5" is not among the defined enum values',
            bind_processor, 5
        )

        assert_raises_message(
            LookupError,
            '"foo" is not among the defined enum values',
            bind_processor_validates, "foo"
        )

        result_processor = type_.result_processor(testing.db.dialect, None)

        eq_(result_processor('one'), self.one)
        assert_raises_message(
            LookupError,
            '"foo" is not among the defined enum values',
            result_processor, "foo"
        )

        literal_processor = type_.literal_processor(testing.db.dialect)
        validate_literal_processor = validate_type.literal_processor(
            testing.db.dialect)
        eq_(literal_processor("one"), "'one'")

        eq_(literal_processor("foo"), "'foo'")

        assert_raises_message(
            LookupError,
            '"5" is not among the defined enum values',
            literal_processor, 5
        )

        assert_raises_message(
            LookupError,
            '"foo" is not among the defined enum values',
            validate_literal_processor, "foo"
        )

    def test_validators_plain(self):
        type_ = Enum("one", "two")
        validate_type = Enum("one", "two", validate_strings=True)

        bind_processor = type_.bind_processor(testing.db.dialect)
        bind_processor_validates = validate_type.bind_processor(
            testing.db.dialect)
        eq_(bind_processor('one'), "one")
        eq_(bind_processor('foo'), "foo")
        assert_raises_message(
            LookupError,
            '"5" is not among the defined enum values',
            bind_processor, 5
        )

        assert_raises_message(
            LookupError,
            '"foo" is not among the defined enum values',
            bind_processor_validates, "foo"
        )

        result_processor = type_.result_processor(testing.db.dialect, None)

        eq_(result_processor('one'), "one")
        assert_raises_message(
            LookupError,
            '"foo" is not among the defined enum values',
            result_processor, "foo"
        )

        literal_processor = type_.literal_processor(testing.db.dialect)
        validate_literal_processor = validate_type.literal_processor(
            testing.db.dialect)
        eq_(literal_processor("one"), "'one'")
        eq_(literal_processor("foo"), "'foo'")
        assert_raises_message(
            LookupError,
            '"5" is not among the defined enum values',
            literal_processor, 5
        )

        assert_raises_message(
            LookupError,
            '"foo" is not among the defined enum values',
            validate_literal_processor, "foo"
        )

    def test_validators_not_in_like_roundtrip(self):
        enum_table = self.tables['non_native_enum_table']

        enum_table.insert().execute([
            {'id': 1, 'someenum': 'two'},
            {'id': 2, 'someenum': 'two'},
            {'id': 3, 'someenum': 'one'},
        ])

        eq_(
            enum_table.select().
            where(enum_table.c.someenum.like('%wo%')).
            order_by(enum_table.c.id).execute().fetchall(),
            [
                (1, 'two', None),
                (2, 'two', None),
            ]
        )

    @testing.fails_on(
        'postgresql+zxjdbc',
        'zxjdbc fails on ENUM: column "XXX" is of type XXX '
        'but expression is of type character varying')
    def test_round_trip(self):
        enum_table = self.tables['enum_table']

        enum_table.insert().execute([
            {'id': 1, 'someenum': 'two'},
            {'id': 2, 'someenum': 'two'},
            {'id': 3, 'someenum': 'one'},
        ])

        eq_(
            enum_table.select().order_by(enum_table.c.id).execute().fetchall(),
            [
                (1, 'two'),
                (2, 'two'),
                (3, 'one'),
            ]
        )

    def test_null_round_trip(self):
        enum_table = self.tables.enum_table
        non_native_enum_table = self.tables.non_native_enum_table

        with testing.db.connect() as conn:
            conn.execute(enum_table.insert(), {"id": 1, "someenum": None})
            eq_(conn.scalar(select([enum_table.c.someenum])), None)

        with testing.db.connect() as conn:
            conn.execute(
                non_native_enum_table.insert(), {"id": 1, "someenum": None})
            eq_(conn.scalar(select([non_native_enum_table.c.someenum])), None)


    @testing.fails_on(
        'mysql',
        "The CHECK clause is parsed but ignored by all storage engines.")
    @testing.fails_on(
        'mssql', "FIXME: MS-SQL 2005 doesn't honor CHECK ?!?")
    def test_check_constraint(self):
        assert_raises(
            (exc.IntegrityError, exc.ProgrammingError),
            testing.db.execute,
            "insert into non_native_enum_table "
            "(id, someenum) values(1, 'four')")

    def test_skip_check_constraint(self):
        with testing.db.connect() as conn:
            conn.execute(
                "insert into non_native_enum_table "
                "(id, someotherenum) values(1, 'four')"
            )
            eq_(
                conn.scalar("select someotherenum from non_native_enum_table"),
                "four")
            assert_raises_message(
                LookupError,
                '"four" is not among the defined enum values',
                conn.scalar,
                select([self.tables.non_native_enum_table.c.someotherenum])
            )

    def test_non_native_round_trip(self):
        non_native_enum_table = self.tables['non_native_enum_table']

        non_native_enum_table.insert().execute([
            {'id': 1, 'someenum': 'two'},
            {'id': 2, 'someenum': 'two'},
            {'id': 3, 'someenum': 'one'},
        ])

        eq_(
            select([
                non_native_enum_table.c.id,
                non_native_enum_table.c.someenum]).
            order_by(non_native_enum_table.c.id).execute().fetchall(),
            [
                (1, 'two'),
                (2, 'two'),
                (3, 'one'),
            ]
        )

    def test_pep435_enum_round_trip(self):
        stdlib_enum_table = self.tables['stdlib_enum_table']

        stdlib_enum_table.insert().execute([
            {'id': 1, 'someenum': self.SomeEnum.two},
            {'id': 2, 'someenum': self.SomeEnum.two},
            {'id': 3, 'someenum': self.SomeEnum.one},
        ])

        eq_(
            stdlib_enum_table.select().
            order_by(stdlib_enum_table.c.id).execute().fetchall(),
            [
                (1, self.SomeEnum.two),
                (2, self.SomeEnum.two),
                (3, self.SomeEnum.one),
            ]
        )

    def test_adapt(self):
        from sqlalchemy.dialects.postgresql import ENUM
        e1 = Enum('one', 'two', 'three', native_enum=False)
        eq_(e1.adapt(ENUM).native_enum, False)
        e1 = Enum('one', 'two', 'three', native_enum=True)
        eq_(e1.adapt(ENUM).native_enum, True)
        e1 = Enum('one', 'two', 'three', name='foo', schema='bar')
        eq_(e1.adapt(ENUM).name, 'foo')
        eq_(e1.adapt(ENUM).schema, 'bar')
        e1 = Enum(self.SomeEnum)
        eq_(e1.adapt(ENUM).name, 'someenum')
        eq_(e1.adapt(ENUM).enums, ['one', 'two', 'three'])

    @testing.provide_metadata
    def test_create_metadata_bound_no_crash(self):
        m1 = self.metadata
        Enum('a', 'b', 'c', metadata=m1, name='ncenum')

        m1.create_all(testing.db)

    def test_non_native_constraint_custom_type(self):
        class Foob(object):

            def __init__(self, name):
                self.name = name

        class MyEnum(TypeDecorator):

            def __init__(self, values):
                self.impl = Enum(
                    *[v.name for v in values], name="myenum",
                    native_enum=False)

            # future method
            def process_literal_param(self, value, dialect):
                return value.name

            def process_bind_param(self, value, dialect):
                return value.name

        m = MetaData()
        t1 = Table('t', m, Column('x', MyEnum([Foob('a'), Foob('b')])))
        const = [
            c for c in t1.constraints if isinstance(c, CheckConstraint)][0]

        self.assert_compile(
            AddConstraint(const),
            "ALTER TABLE t ADD CONSTRAINT myenum CHECK (x IN ('a', 'b'))",
            dialect="default"
        )

    def test_lookup_failure(self):
        assert_raises(
            exc.StatementError,
            self.tables['non_native_enum_table'].insert().execute,
            {'id': 4, 'someotherenum': 'four'}
        )

    def test_mock_engine_no_prob(self):
        """ensure no 'checkfirst' queries are run when enums
        are created with checkfirst=False"""

        e = engines.mock_engine()
        t = Table('t1', MetaData(), Column('x', Enum("x", "y", name="pge")))
        t.create(e, checkfirst=False)
        # basically looking for the start of
        # the constraint, or the ENUM def itself,
        # depending on backend.
        assert "('x'," in e.print_sql()

    def test_repr(self):
        e = Enum(
            "x", "y", name="somename", convert_unicode=True, quote=True,
            inherit_schema=True, native_enum=False)
        eq_(
            repr(e),
            "Enum('x', 'y', name='somename', "
            "inherit_schema=True, native_enum=False)")

binary_table = MyPickleType = metadata = None


class BinaryTest(fixtures.TestBase, AssertsExecutionResults):

    @classmethod
    def setup_class(cls):
        global binary_table, MyPickleType, metadata

        class MyPickleType(types.TypeDecorator):
            impl = PickleType

            def process_bind_param(self, value, dialect):
                if value:
                    value.stuff = 'this is modified stuff'
                return value

            def process_result_value(self, value, dialect):
                if value:
                    value.stuff = 'this is the right stuff'
                return value

        metadata = MetaData(testing.db)
        binary_table = Table(
            'binary_table', metadata,
            Column(
                'primary_id', Integer, primary_key=True,
                test_needs_autoincrement=True),
            Column('data', LargeBinary),
            Column('data_slice', LargeBinary(100)),
            Column('misc', String(30)),
            Column('pickled', PickleType),
            Column('mypickle', MyPickleType)
        )
        metadata.create_all()

    @engines.close_first
    def teardown(self):
        binary_table.delete().execute()

    @classmethod
    def teardown_class(cls):
        metadata.drop_all()

    def test_round_trip(self):
        testobj1 = pickleable.Foo('im foo 1')
        testobj2 = pickleable.Foo('im foo 2')
        testobj3 = pickleable.Foo('im foo 3')

        stream1 = self.load_stream('binary_data_one.dat')
        stream2 = self.load_stream('binary_data_two.dat')
        binary_table.insert().execute(
            primary_id=1, misc='binary_data_one.dat', data=stream1,
            data_slice=stream1[0:100], pickled=testobj1, mypickle=testobj3)
        binary_table.insert().execute(
            primary_id=2, misc='binary_data_two.dat', data=stream2,
            data_slice=stream2[0:99], pickled=testobj2)
        binary_table.insert().execute(
            primary_id=3, misc='binary_data_two.dat', data=None,
            data_slice=stream2[0:99], pickled=None)

        for stmt in (
            binary_table.select(order_by=binary_table.c.primary_id),
            text(
                "select * from binary_table order by binary_table.primary_id",
                typemap={
                    'pickled': PickleType, 'mypickle': MyPickleType,
                    'data': LargeBinary, 'data_slice': LargeBinary},
                bind=testing.db)
        ):
            l = stmt.execute().fetchall()
            eq_(stream1, l[0]['data'])
            eq_(stream1[0:100], l[0]['data_slice'])
            eq_(stream2, l[1]['data'])
            eq_(testobj1, l[0]['pickled'])
            eq_(testobj2, l[1]['pickled'])
            eq_(testobj3.moredata, l[0]['mypickle'].moredata)
            eq_(l[0]['mypickle'].stuff, 'this is the right stuff')

    @testing.requires.binary_comparisons
    def test_comparison(self):
        """test that type coercion occurs on comparison for binary"""

        expr = binary_table.c.data == 'foo'
        assert isinstance(expr.right.type, LargeBinary)

        data = os.urandom(32)
        binary_table.insert().execute(data=data)
        eq_(
            select([func.count('*')]).select_from(binary_table).
            where(binary_table.c.data == data).scalar(), 1)

    @testing.requires.binary_literals
    def test_literal_roundtrip(self):
        compiled = select([cast(literal(util.b("foo")), LargeBinary)]).compile(
            dialect=testing.db.dialect, compile_kwargs={"literal_binds": True})
        result = testing.db.execute(compiled)
        eq_(result.scalar(), util.b("foo"))

    def test_bind_processor_no_dbapi(self):
        b = LargeBinary()
        eq_(b.bind_processor(default.DefaultDialect()), None)

    def load_stream(self, name):
        f = os.path.join(os.path.dirname(__file__), "..", name)
        with open(f, mode='rb') as o:
            return o.read()


class JSONTest(fixtures.TestBase):

    def setup(self):
        metadata = MetaData()
        self.test_table = Table('test_table', metadata,
                                Column('id', Integer, primary_key=True),
                                Column('test_column', JSON),
                                )
        self.jsoncol = self.test_table.c.test_column

        self.dialect = default.DefaultDialect()
        self.dialect._json_serializer = None
        self.dialect._json_deserializer = None

    def test_bind_serialize_default(self):
        proc = self.test_table.c.test_column.type._cached_bind_processor(
            self.dialect)
        eq_(
            proc({"A": [1, 2, 3, True, False]}),
            '{"A": [1, 2, 3, true, false]}'
        )

    def test_bind_serialize_None(self):
        proc = self.test_table.c.test_column.type._cached_bind_processor(
            self.dialect)
        eq_(
            proc(None),
            'null'
        )

    def test_bind_serialize_none_as_null(self):
        proc = JSON(none_as_null=True)._cached_bind_processor(
            self.dialect)
        eq_(
            proc(None),
            None
        )
        eq_(
            proc(null()),
            None
        )

    def test_bind_serialize_null(self):
        proc = self.test_table.c.test_column.type._cached_bind_processor(
            self.dialect)
        eq_(
            proc(null()),
            None
        )

    def test_result_deserialize_default(self):
        proc = self.test_table.c.test_column.type._cached_result_processor(
            self.dialect, None)
        eq_(
            proc('{"A": [1, 2, 3, true, false]}'),
            {"A": [1, 2, 3, True, False]}
        )

    def test_result_deserialize_null(self):
        proc = self.test_table.c.test_column.type._cached_result_processor(
            self.dialect, None)
        eq_(
            proc('null'),
            None
        )

    def test_result_deserialize_None(self):
        proc = self.test_table.c.test_column.type._cached_result_processor(
            self.dialect, None)
        eq_(
            proc(None),
            None
        )

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
        expr = self.test_table.c.test_column['five']

        str_dialect = self._dialect_index_fixture(True, True)
        non_str_dialect = self._dialect_index_fixture(False, False)

        bindproc = expr.right.type._cached_bind_processor(str_dialect)
        eq_(bindproc(expr.right.value), 'five10')

        bindproc = expr.right.type._cached_bind_processor(non_str_dialect)
        eq_(bindproc(expr.right.value), 'five')

    def test_index_literal_proc_str(self):
        expr = self.test_table.c.test_column['five']

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
        col = column('x', ARRAY(Integer, dimensions=3))
        is_(
            col[5].type._type_affinity, ARRAY
        )
        eq_(
            col[5].type.dimensions, 2
        )
        is_(
            col[5][6].type._type_affinity, ARRAY
        )
        eq_(
            col[5][6].type.dimensions, 1
        )
        is_(
            col[5][6][7].type._type_affinity, Integer
        )

    def test_array_getitem_single_type(self):
        m = MetaData()
        arrtable = Table(
            'arrtable', m,
            Column('intarr', ARRAY(Integer)),
            Column('strarr', ARRAY(String)),
        )
        is_(arrtable.c.intarr[1].type._type_affinity, Integer)
        is_(arrtable.c.strarr[1].type._type_affinity, String)

    def test_array_getitem_slice_type(self):
        m = MetaData()
        arrtable = Table(
            'arrtable', m,
            Column('intarr', ARRAY(Integer)),
            Column('strarr', ARRAY(String)),
        )
        is_(arrtable.c.intarr[1:3].type._type_affinity, ARRAY)
        is_(arrtable.c.strarr[1:3].type._type_affinity, ARRAY)

    def test_array_getitem_slice_type_dialect_level(self):
        MyArray = self._myarray_fixture()
        m = MetaData()
        arrtable = Table(
            'arrtable', m,
            Column('intarr', MyArray(Integer)),
            Column('strarr', MyArray(String)),
        )
        is_(arrtable.c.intarr[1:3].type._type_affinity, ARRAY)
        is_(arrtable.c.strarr[1:3].type._type_affinity, ARRAY)

        # but the slice returns the actual type
        assert isinstance(arrtable.c.intarr[1:3].type, MyArray)
        assert isinstance(arrtable.c.strarr[1:3].type, MyArray)


test_table = meta = MyCustomType = MyTypeDec = None


class ExpressionTest(
        fixtures.TestBase, AssertsExecutionResults, AssertsCompiledSQL):
    __dialect__ = 'default'

    @classmethod
    def setup_class(cls):
        global test_table, meta, MyCustomType, MyTypeDec

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
                    operators.sub: operators.add}.get(op, op)

        class MyTypeDec(types.TypeDecorator):
            impl = String

            def process_bind_param(self, value, dialect):
                return "BIND_IN" + str(value)

            def process_result_value(self, value, dialect):
                return value + "BIND_OUT"

        meta = MetaData(testing.db)
        test_table = Table(
            'test', meta,
            Column('id', Integer, primary_key=True),
            Column('data', String(30)),
            Column('atimestamp', Date),
            Column('avalue', MyCustomType),
            Column('bvalue', MyTypeDec(50)),
        )

        meta.create_all()

        test_table.insert().execute({
            'id': 1, 'data': 'somedata',
            'atimestamp': datetime.date(2007, 10, 15), 'avalue': 25,
            'bvalue': 'foo'})

    @classmethod
    def teardown_class(cls):
        meta.drop_all()

    def test_control(self):
        assert testing.db.execute("select avalue from test").scalar() == 250

        eq_(
            test_table.select().execute().fetchall(),
            [(1, 'somedata', datetime.date(2007, 10, 15), 25,
              'BIND_INfooBIND_OUT')]
        )

    def test_bind_adapt(self):
        # test an untyped bind gets the left side's type
        expr = test_table.c.atimestamp == bindparam("thedate")
        eq_(expr.right.type._type_affinity, Date)

        eq_(
            testing.db.execute(
                select([
                    test_table.c.id, test_table.c.data,
                    test_table.c.atimestamp]).where(expr),
                {"thedate": datetime.date(2007, 10, 15)}).fetchall(), [
                    (1, 'somedata', datetime.date(2007, 10, 15))]
        )

        expr = test_table.c.avalue == bindparam("somevalue")
        eq_(expr.right.type._type_affinity, MyCustomType)

        eq_(
            testing.db.execute(
                test_table.select().where(expr), {'somevalue': 25}
            ).fetchall(), [(
                1, 'somedata', datetime.date(2007, 10, 15), 25,
                'BIND_INfooBIND_OUT')]
        )

        expr = test_table.c.bvalue == bindparam("somevalue")
        eq_(expr.right.type._type_affinity, String)

        eq_(
            testing.db.execute(
                test_table.select().where(expr), {"somevalue": "foo"}
            ).fetchall(), [(
                1, 'somedata', datetime.date(2007, 10, 15), 25,
                'BIND_INfooBIND_OUT')]
        )

    def test_bind_adapt_update(self):
        bp = bindparam("somevalue")
        stmt = test_table.update().values(avalue=bp)
        compiled = stmt.compile()
        eq_(bp.type._type_affinity, types.NullType)
        eq_(compiled.binds['somevalue'].type._type_affinity, MyCustomType)

    def test_bind_adapt_insert(self):
        bp = bindparam("somevalue")
        stmt = test_table.insert().values(avalue=bp)
        compiled = stmt.compile()
        eq_(bp.type._type_affinity, types.NullType)
        eq_(compiled.binds['somevalue'].type._type_affinity, MyCustomType)

    def test_bind_adapt_expression(self):
        bp = bindparam("somevalue")
        stmt = test_table.c.avalue == bp
        eq_(bp.type._type_affinity, types.NullType)
        eq_(stmt.right.type._type_affinity, MyCustomType)

    def test_literal_adapt(self):
        # literals get typed based on the types dictionary, unless
        # compatible with the left side type

        expr = column('foo', String) == 5
        eq_(expr.right.type._type_affinity, Integer)

        expr = column('foo', String) == "asdf"
        eq_(expr.right.type._type_affinity, String)

        expr = column('foo', CHAR) == 5
        eq_(expr.right.type._type_affinity, Integer)

        expr = column('foo', CHAR) == "asdf"
        eq_(expr.right.type.__class__, CHAR)

    def test_typedec_operator_adapt(self):
        expr = test_table.c.bvalue + "hi"

        assert expr.type.__class__ is MyTypeDec
        assert expr.right.type.__class__ is MyTypeDec

        eq_(
            testing.db.execute(select([expr.label('foo')])).scalar(),
            "BIND_INfooBIND_INhiBIND_OUT"
        )

    def test_typedec_is_adapt(self):
        class CoerceNothing(TypeDecorator):
            coerce_to_is_types = ()
            impl = Integer

        class CoerceBool(TypeDecorator):
            coerce_to_is_types = (bool, )
            impl = Boolean

        class CoerceNone(TypeDecorator):
            coerce_to_is_types = (type(None),)
            impl = Integer

        c1 = column('x', CoerceNothing())
        c2 = column('x', CoerceBool())
        c3 = column('x', CoerceNone())

        self.assert_compile(
            and_(c1 == None, c2 == None, c3 == None),  # noqa
            "x = :x_1 AND x = :x_2 AND x IS NULL"
        )
        self.assert_compile(
            and_(c1 == True, c2 == True, c3 == True),  # noqa
            "x = :x_1 AND x = true AND x = :x_2",
            dialect=default.DefaultDialect(supports_native_boolean=True)
        )
        self.assert_compile(
            and_(c1 == 3, c2 == 3, c3 == 3),
            "x = :x_1 AND x = :x_2 AND x = :x_3",
            dialect=default.DefaultDialect(supports_native_boolean=True)
        )
        self.assert_compile(
            and_(c1.is_(True), c2.is_(True), c3.is_(True)),
            "x IS :x_1 AND x IS true AND x IS :x_2",
            dialect=default.DefaultDialect(supports_native_boolean=True)
        )

    def test_typedec_righthand_coercion(self):
        class MyTypeDec(types.TypeDecorator):
            impl = String

            def process_bind_param(self, value, dialect):
                return "BIND_IN" + str(value)

            def process_result_value(self, value, dialect):
                return value + "BIND_OUT"

        tab = table('test', column('bvalue', MyTypeDec))
        expr = tab.c.bvalue + 6

        self.assert_compile(
            expr,
            "test.bvalue || :bvalue_1",
            use_default_dialect=True
        )

        assert expr.type.__class__ is MyTypeDec
        eq_(
            testing.db.execute(select([expr.label('foo')])).scalar(),
            "BIND_INfooBIND_IN6BIND_OUT"
        )

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
        assert bp.type._type_affinity is types.NullType
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
        from sqlalchemy.sql import column

        expr = column('bar', types.NULLTYPE) - column('foo', types.TIMESTAMP)
        eq_(expr.type._type_affinity, types.NullType)

        expr = func.sysdate() - column('foo', types.TIMESTAMP)
        eq_(expr.type._type_affinity, types.Interval)

        expr = func.current_date() - column('foo', types.TIMESTAMP)
        eq_(expr.type._type_affinity, types.Interval)

    def test_numerics_coercion(self):
        from sqlalchemy.sql import column
        import operator

        for op in (operator.add, operator.mul, operator.truediv, operator.sub):
            for other in (Numeric(10, 2), Integer):
                expr = op(
                    column('bar', types.Numeric(10, 2)),
                    column('foo', other)
                )
                assert isinstance(expr.type, types.Numeric)
                expr = op(
                    column('foo', other),
                    column('bar', types.Numeric(10, 2))
                )
                assert isinstance(expr.type, types.Numeric)

    def test_null_comparison(self):
        eq_(
            str(column('a', types.NullType()) + column('b', types.NullType())),
            "a + b"
        )

    def test_expression_typing(self):
        expr = column('bar', Integer) - 3

        eq_(expr.type._type_affinity, Integer)

        expr = bindparam('bar') + bindparam('foo')
        eq_(expr.type, types.NULLTYPE)

    def test_distinct(self):
        s = select([distinct(test_table.c.avalue)])
        eq_(testing.db.execute(s).scalar(), 25)

        s = select([test_table.c.avalue.distinct()])
        eq_(testing.db.execute(s).scalar(), 25)

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
            r"Object some_sqla_thing\(\) is not legal as a SQL literal value",
            lambda: column('a', String) == SomeSQLAThing()
        )

        is_(
            bindparam('x', SomeOtherThing()).type,
            types.NULLTYPE
        )

    def test_detect_coercion_not_fooled_by_mock(self):
        m1 = mock.Mock()
        is_(
            bindparam('x', m1).type,
            types.NULLTYPE
        )



class CompileTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = 'default'

    @testing.requires.unbounded_varchar
    def test_string_plain(self):
        self.assert_compile(String(), "VARCHAR")

    def test_string_length(self):
        self.assert_compile(String(50), "VARCHAR(50)")

    def test_string_collation(self):
        self.assert_compile(
            String(50, collation="FOO"), 'VARCHAR(50) COLLATE "FOO"')

    def test_char_plain(self):
        self.assert_compile(CHAR(), "CHAR")

    def test_char_length(self):
        self.assert_compile(CHAR(50), "CHAR(50)")

    def test_char_collation(self):
        self.assert_compile(
            CHAR(50, collation="FOO"), 'CHAR(50) COLLATE "FOO"')

    def test_text_plain(self):
        self.assert_compile(Text(), "TEXT")

    def test_text_length(self):
        self.assert_compile(Text(50), "TEXT(50)")

    def test_text_collation(self):
        self.assert_compile(
            Text(collation="FOO"), 'TEXT COLLATE "FOO"')

    def test_default_compile_pg_inet(self):
        self.assert_compile(
            dialects.postgresql.INET(), "INET", allow_dialect_select=True)

    def test_default_compile_pg_float(self):
        self.assert_compile(
            dialects.postgresql.FLOAT(), "FLOAT", allow_dialect_select=True)

    def test_default_compile_mysql_integer(self):
        self.assert_compile(
            dialects.mysql.INTEGER(display_width=5), "INTEGER(5)",
            allow_dialect_select=True)

    def test_numeric_plain(self):
        self.assert_compile(types.NUMERIC(), 'NUMERIC')

    def test_numeric_precision(self):
        self.assert_compile(types.NUMERIC(2), 'NUMERIC(2)')

    def test_numeric_scale(self):
        self.assert_compile(types.NUMERIC(2, 4), 'NUMERIC(2, 4)')

    def test_decimal_plain(self):
        self.assert_compile(types.DECIMAL(), 'DECIMAL')

    def test_decimal_precision(self):
        self.assert_compile(types.DECIMAL(2), 'DECIMAL(2)')

    def test_decimal_scale(self):
        self.assert_compile(types.DECIMAL(2, 4), 'DECIMAL(2, 4)')

    def test_kwarg_legacy_typecompiler(self):
        from sqlalchemy.sql import compiler

        class SomeTypeCompiler(compiler.GenericTypeCompiler):
            # transparently decorated w/ kw decorator
            def visit_VARCHAR(self, type_):
                return "MYVARCHAR"

            # not affected
            def visit_INTEGER(self, type_, **kw):
                return "MYINTEGER %s" % kw['type_expression'].name

        dialect = default.DefaultDialect()
        dialect.type_compiler = SomeTypeCompiler(dialect)
        self.assert_compile(
            ddl.CreateColumn(Column('bar', VARCHAR(50))),
            "bar MYVARCHAR",
            dialect=dialect
        )
        self.assert_compile(
            ddl.CreateColumn(Column('bar', INTEGER)),
            "bar MYINTEGER bar",
            dialect=dialect
        )


class TestKWArgPassThru(AssertsCompiledSQL, fixtures.TestBase):
    __backend__ = True

    def test_user_defined(self):
        """test that dialects pass the column through on DDL."""

        class MyType(types.UserDefinedType):
            def get_col_spec(self, **kw):
                return "FOOB %s" % kw['type_expression'].name

        m = MetaData()
        t = Table('t', m, Column('bar', MyType))
        self.assert_compile(
            ddl.CreateColumn(t.c.bar),
            "bar FOOB bar"
        )


class NumericRawSQLTest(fixtures.TestBase):

    """Test what DBAPIs and dialects return without any typing
    information supplied at the SQLA level.

    """

    def _fixture(self, metadata, type, data):
        t = Table('t', metadata, Column("val", type))
        metadata.create_all()
        t.insert().execute(val=data)

    @testing.fails_on('sqlite', "Doesn't provide Decimal results natively")
    @testing.provide_metadata
    def test_decimal_fp(self):
        metadata = self.metadata
        self._fixture(metadata, Numeric(10, 5), decimal.Decimal("45.5"))
        val = testing.db.execute("select val from t").scalar()
        assert isinstance(val, decimal.Decimal)
        eq_(val, decimal.Decimal("45.5"))

    @testing.fails_on('sqlite', "Doesn't provide Decimal results natively")
    @testing.provide_metadata
    def test_decimal_int(self):
        metadata = self.metadata
        self._fixture(metadata, Numeric(10, 5), decimal.Decimal("45"))
        val = testing.db.execute("select val from t").scalar()
        assert isinstance(val, decimal.Decimal)
        eq_(val, decimal.Decimal("45"))

    @testing.provide_metadata
    def test_ints(self):
        metadata = self.metadata
        self._fixture(metadata, Integer, 45)
        val = testing.db.execute("select val from t").scalar()
        assert isinstance(val, util.int_types)
        eq_(val, 45)

    @testing.provide_metadata
    def test_float(self):
        metadata = self.metadata
        self._fixture(metadata, Float, 46.583)
        val = testing.db.execute("select val from t").scalar()
        assert isinstance(val, float)

        # some DBAPIs have unusual float handling
        if testing.against('oracle+cx_oracle', 'mysql+oursql', 'firebird'):
            eq_(round_decimal(val, 3), 46.583)
        else:
            eq_(val, 46.583)

interval_table = metadata = None


class IntervalTest(fixtures.TestBase, AssertsExecutionResults):

    @classmethod
    def setup_class(cls):
        global interval_table, metadata
        metadata = MetaData(testing.db)
        interval_table = Table(
            "intervaltable", metadata,
            Column(
                "id", Integer, primary_key=True,
                test_needs_autoincrement=True),
            Column("native_interval", Interval()),
            Column(
                "native_interval_args",
                Interval(day_precision=3, second_precision=6)),
            Column(
                "non_native_interval", Interval(native=False)),
        )
        metadata.create_all()

    @engines.close_first
    def teardown(self):
        interval_table.delete().execute()

    @classmethod
    def teardown_class(cls):
        metadata.drop_all()

    def test_non_native_adapt(self):
        interval = Interval(native=False)
        adapted = interval.dialect_impl(testing.db.dialect)
        assert isinstance(adapted, Interval)
        assert adapted.native is False
        eq_(str(adapted), "DATETIME")

    @testing.fails_on(
        "postgresql+zxjdbc",
        "Not yet known how to pass values of the INTERVAL type")
    @testing.fails_on(
        "oracle+zxjdbc",
        "Not yet known how to pass values of the INTERVAL type")
    def test_roundtrip(self):
        small_delta = datetime.timedelta(days=15, seconds=5874)
        delta = datetime.timedelta(414)
        interval_table.insert().execute(
            native_interval=small_delta, native_interval_args=delta,
            non_native_interval=delta)
        row = interval_table.select().execute().first()
        eq_(row['native_interval'], small_delta)
        eq_(row['native_interval_args'], delta)
        eq_(row['non_native_interval'], delta)

    @testing.fails_on(
        "oracle+zxjdbc",
        "Not yet known how to pass values of the INTERVAL type")
    def test_null(self):
        interval_table.insert().execute(
            id=1, native_inverval=None, non_native_interval=None)
        row = interval_table.select().execute().first()
        eq_(row['native_interval'], None)
        eq_(row['native_interval_args'], None)
        eq_(row['non_native_interval'], None)


class BooleanTest(
        fixtures.TablesTest, AssertsExecutionResults, AssertsCompiledSQL):

    """test edge cases for booleans.  Note that the main boolean test suite
    is now in testing/suite/test_types.py

    """
    @classmethod
    def define_tables(cls, metadata):
        Table(
            'boolean_table', metadata,
            Column('id', Integer, primary_key=True, autoincrement=False),
            Column('value', Boolean),
            Column('unconstrained_value', Boolean(create_constraint=False)),
        )

    @testing.fails_on(
        'mysql',
        "The CHECK clause is parsed but ignored by all storage engines.")
    @testing.fails_on(
        'mssql', "FIXME: MS-SQL 2005 doesn't honor CHECK ?!?")
    @testing.skip_if(lambda: testing.db.dialect.supports_native_boolean)
    def test_constraint(self):
        assert_raises(
            (exc.IntegrityError, exc.ProgrammingError),
            testing.db.execute,
            "insert into boolean_table (id, value) values(1, 5)")

    @testing.skip_if(lambda: testing.db.dialect.supports_native_boolean)
    def test_unconstrained(self):
        testing.db.execute(
            "insert into boolean_table (id, unconstrained_value)"
            "values (1, 5)")

    def test_non_native_constraint_custom_type(self):
        class Foob(object):

            def __init__(self, value):
                self.value = value

        class MyBool(TypeDecorator):
            impl = Boolean()

            # future method
            def process_literal_param(self, value, dialect):
                return value.value

            def process_bind_param(self, value, dialect):
                return value.value

        m = MetaData()
        t1 = Table('t', m, Column('x', MyBool()))
        const = [
            c for c in t1.constraints if isinstance(c, CheckConstraint)][0]

        self.assert_compile(
            AddConstraint(const),
            "ALTER TABLE t ADD CHECK (x IN (0, 1))",
            dialect="sqlite"
        )

    @testing.skip_if(lambda: testing.db.dialect.supports_native_boolean)
    def test_nonnative_processor_coerces_to_onezero(self):
        boolean_table = self.tables.boolean_table
        with testing.db.connect() as conn:
            conn.execute(
                boolean_table.insert(),
                {"id": 1, "unconstrained_value": 5}
            )

            eq_(
                conn.scalar("select unconstrained_value from boolean_table"),
                1
            )

    @testing.skip_if(lambda: testing.db.dialect.supports_native_boolean)
    def test_nonnative_processor_coerces_integer_to_boolean(self):
        boolean_table = self.tables.boolean_table
        with testing.db.connect() as conn:
            conn.execute(
                "insert into boolean_table (id, unconstrained_value) values (1, 5)"
            )

            eq_(
                conn.scalar("select unconstrained_value from boolean_table"),
                5
            )

            eq_(
                conn.scalar(select([boolean_table.c.unconstrained_value])),
                True
            )

class PickleTest(fixtures.TestBase):

    def test_eq_comparison(self):
        p1 = PickleType()

        for obj in (
            {'1': '2'},
            pickleable.Bar(5, 6),
            pickleable.OldSchool(10, 11)
        ):
            assert p1.compare_values(p1.copy_value(obj), obj)

        assert_raises(
            NotImplementedError, p1.compare_values,
            pickleable.BrokenComparable('foo'),
            pickleable.BrokenComparable('foo'))

    def test_nonmutable_comparison(self):
        p1 = PickleType()

        for obj in (
            {'1': '2'},
            pickleable.Bar(5, 6),
            pickleable.OldSchool(10, 11)
        ):
            assert p1.compare_values(p1.copy_value(obj), obj)

meta = None


class CallableTest(fixtures.TestBase):

    @classmethod
    def setup_class(cls):
        global meta
        meta = MetaData(testing.db)

    @classmethod
    def teardown_class(cls):
        meta.drop_all()

    def test_callable_as_arg(self):
        ucode = util.partial(Unicode)

        thing_table = Table(
            'thing', meta, Column('name', ucode(20))
        )
        assert isinstance(thing_table.c.name.type, Unicode)
        thing_table.create()

    def test_callable_as_kwarg(self):
        ucode = util.partial(Unicode)

        thang_table = Table(
            'thang', meta, Column('name', type_=ucode(20), primary_key=True)
        )
        assert isinstance(thang_table.c.name.type, Unicode)
        thang_table.create()
