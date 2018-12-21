# coding: utf-8

import datetime
import decimal

from .. import config
from .. import fixtures
from ..assertions import eq_
from ..config import requirements
from ..schema import Column
from ..schema import Table
from ... import and_
from ... import BigInteger
from ... import Boolean
from ... import cast
from ... import Date
from ... import DateTime
from ... import Float
from ... import Integer
from ... import JSON
from ... import literal
from ... import MetaData
from ... import null
from ... import Numeric
from ... import select
from ... import String
from ... import testing
from ... import Text
from ... import Time
from ... import TIMESTAMP
from ... import type_coerce
from ... import Unicode
from ... import UnicodeText
from ... import util
from ...ext.declarative import declarative_base
from ...orm import Session
from ...util import u


class _LiteralRoundTripFixture(object):
    supports_whereclause = True

    @testing.provide_metadata
    def _literal_round_trip(self, type_, input_, output, filter_=None):
        """test literal rendering """

        # for literal, we test the literal render in an INSERT
        # into a typed column.  we can then SELECT it back as its
        # official type; ideally we'd be able to use CAST here
        # but MySQL in particular can't CAST fully
        t = Table("t", self.metadata, Column("x", type_))
        t.create()

        with testing.db.connect() as conn:
            for value in input_:
                ins = (
                    t.insert()
                    .values(x=literal(value))
                    .compile(
                        dialect=testing.db.dialect,
                        compile_kwargs=dict(literal_binds=True),
                    )
                )
                conn.execute(ins)

            if self.supports_whereclause:
                stmt = t.select().where(t.c.x == literal(value))
            else:
                stmt = t.select()

            stmt = stmt.compile(
                dialect=testing.db.dialect,
                compile_kwargs=dict(literal_binds=True),
            )
            for row in conn.execute(stmt):
                value = row[0]
                if filter_ is not None:
                    value = filter_(value)
                assert value in output


class _UnicodeFixture(_LiteralRoundTripFixture):
    __requires__ = ("unicode_data",)

    data = u(
        "Alors vous imaginez ma 🐍 surprise, au lever du jour, "
        "quand une drôle de petite 🐍 voix m’a réveillé. Elle "
        "disait: « S’il vous plaît… dessine-moi 🐍 un mouton! »"
    )

    @property
    def supports_whereclause(self):
        return config.requirements.expressions_against_unbounded_text.enabled

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "unicode_table",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("unicode_data", cls.datatype),
        )

    def test_round_trip(self):
        unicode_table = self.tables.unicode_table

        config.db.execute(unicode_table.insert(), {"unicode_data": self.data})

        row = config.db.execute(select([unicode_table.c.unicode_data])).first()

        eq_(row, (self.data,))
        assert isinstance(row[0], util.text_type)

    def test_round_trip_executemany(self):
        unicode_table = self.tables.unicode_table

        config.db.execute(
            unicode_table.insert(),
            [{"unicode_data": self.data} for i in range(3)],
        )

        rows = config.db.execute(
            select([unicode_table.c.unicode_data])
        ).fetchall()
        eq_(rows, [(self.data,) for i in range(3)])
        for row in rows:
            assert isinstance(row[0], util.text_type)

    def _test_empty_strings(self):
        unicode_table = self.tables.unicode_table

        config.db.execute(unicode_table.insert(), {"unicode_data": u("")})
        row = config.db.execute(select([unicode_table.c.unicode_data])).first()
        eq_(row, (u(""),))

    def test_literal(self):
        self._literal_round_trip(self.datatype, [self.data], [self.data])

    def test_literal_non_ascii(self):
        self._literal_round_trip(
            self.datatype, [util.u("réve🐍 illé")], [util.u("réve🐍 illé")]
        )


class UnicodeVarcharTest(_UnicodeFixture, fixtures.TablesTest):
    __requires__ = ("unicode_data",)
    __backend__ = True

    datatype = Unicode(255)

    @requirements.empty_strings_varchar
    def test_empty_strings_varchar(self):
        self._test_empty_strings()


class UnicodeTextTest(_UnicodeFixture, fixtures.TablesTest):
    __requires__ = "unicode_data", "text_type"
    __backend__ = True

    datatype = UnicodeText()

    @requirements.empty_strings_text
    def test_empty_strings_text(self):
        self._test_empty_strings()


class TextTest(_LiteralRoundTripFixture, fixtures.TablesTest):
    __requires__ = ("text_type",)
    __backend__ = True

    @property
    def supports_whereclause(self):
        return config.requirements.expressions_against_unbounded_text.enabled

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "text_table",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("text_data", Text),
        )

    def test_text_roundtrip(self):
        text_table = self.tables.text_table

        config.db.execute(text_table.insert(), {"text_data": "some text"})
        row = config.db.execute(select([text_table.c.text_data])).first()
        eq_(row, ("some text",))

    def test_text_empty_strings(self):
        text_table = self.tables.text_table

        config.db.execute(text_table.insert(), {"text_data": ""})
        row = config.db.execute(select([text_table.c.text_data])).first()
        eq_(row, ("",))

    def test_literal(self):
        self._literal_round_trip(Text, ["some text"], ["some text"])

    def test_literal_non_ascii(self):
        self._literal_round_trip(
            Text, [util.u("réve🐍 illé")], [util.u("réve🐍 illé")]
        )

    def test_literal_quoting(self):
        data = """some 'text' hey "hi there" that's text"""
        self._literal_round_trip(Text, [data], [data])

    def test_literal_backslashes(self):
        data = r"backslash one \ backslash two \\ end"
        self._literal_round_trip(Text, [data], [data])

    def test_literal_percentsigns(self):
        data = r"percent % signs %% percent"
        self._literal_round_trip(Text, [data], [data])


class StringTest(_LiteralRoundTripFixture, fixtures.TestBase):
    __backend__ = True

    @requirements.unbounded_varchar
    def test_nolength_string(self):
        metadata = MetaData()
        foo = Table("foo", metadata, Column("one", String))

        foo.create(config.db)
        foo.drop(config.db)

    def test_literal(self):
        # note that in Python 3, this invokes the Unicode
        # datatype for the literal part because all strings are unicode
        self._literal_round_trip(String(40), ["some text"], ["some text"])

    def test_literal_non_ascii(self):
        self._literal_round_trip(
            String(40), [util.u("réve🐍 illé")], [util.u("réve🐍 illé")]
        )

    def test_literal_quoting(self):
        data = """some 'text' hey "hi there" that's text"""
        self._literal_round_trip(String(40), [data], [data])

    def test_literal_backslashes(self):
        data = r"backslash one \ backslash two \\ end"
        self._literal_round_trip(String(40), [data], [data])


class _DateFixture(_LiteralRoundTripFixture):
    compare = None

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "date_table",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("date_data", cls.datatype),
        )

    def test_round_trip(self):
        date_table = self.tables.date_table

        config.db.execute(date_table.insert(), {"date_data": self.data})

        row = config.db.execute(select([date_table.c.date_data])).first()

        compare = self.compare or self.data
        eq_(row, (compare,))
        assert isinstance(row[0], type(compare))

    def test_null(self):
        date_table = self.tables.date_table

        config.db.execute(date_table.insert(), {"date_data": None})

        row = config.db.execute(select([date_table.c.date_data])).first()
        eq_(row, (None,))

    @testing.requires.datetime_literals
    def test_literal(self):
        compare = self.compare or self.data
        self._literal_round_trip(self.datatype, [self.data], [compare])


class DateTimeTest(_DateFixture, fixtures.TablesTest):
    __requires__ = ("datetime",)
    __backend__ = True
    datatype = DateTime
    data = datetime.datetime(2012, 10, 15, 12, 57, 18)


class DateTimeMicrosecondsTest(_DateFixture, fixtures.TablesTest):
    __requires__ = ("datetime_microseconds",)
    __backend__ = True
    datatype = DateTime
    data = datetime.datetime(2012, 10, 15, 12, 57, 18, 396)


class TimestampMicrosecondsTest(_DateFixture, fixtures.TablesTest):
    __requires__ = ("timestamp_microseconds",)
    __backend__ = True
    datatype = TIMESTAMP
    data = datetime.datetime(2012, 10, 15, 12, 57, 18, 396)


class TimeTest(_DateFixture, fixtures.TablesTest):
    __requires__ = ("time",)
    __backend__ = True
    datatype = Time
    data = datetime.time(12, 57, 18)


class TimeMicrosecondsTest(_DateFixture, fixtures.TablesTest):
    __requires__ = ("time_microseconds",)
    __backend__ = True
    datatype = Time
    data = datetime.time(12, 57, 18, 396)


class DateTest(_DateFixture, fixtures.TablesTest):
    __requires__ = ("date",)
    __backend__ = True
    datatype = Date
    data = datetime.date(2012, 10, 15)


class DateTimeCoercedToDateTimeTest(_DateFixture, fixtures.TablesTest):
    __requires__ = "date", "date_coerces_from_datetime"
    __backend__ = True
    datatype = Date
    data = datetime.datetime(2012, 10, 15, 12, 57, 18)
    compare = datetime.date(2012, 10, 15)


class DateTimeHistoricTest(_DateFixture, fixtures.TablesTest):
    __requires__ = ("datetime_historic",)
    __backend__ = True
    datatype = DateTime
    data = datetime.datetime(1850, 11, 10, 11, 52, 35)


class DateHistoricTest(_DateFixture, fixtures.TablesTest):
    __requires__ = ("date_historic",)
    __backend__ = True
    datatype = Date
    data = datetime.date(1727, 4, 1)


class IntegerTest(_LiteralRoundTripFixture, fixtures.TestBase):
    __backend__ = True

    def test_literal(self):
        self._literal_round_trip(Integer, [5], [5])

    def test_huge_int(self):
        self._round_trip(BigInteger, 1376537018368127)

    @testing.provide_metadata
    def _round_trip(self, datatype, data):
        metadata = self.metadata
        int_table = Table(
            "integer_table",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("integer_data", datatype),
        )

        metadata.create_all(config.db)

        config.db.execute(int_table.insert(), {"integer_data": data})

        row = config.db.execute(select([int_table.c.integer_data])).first()

        eq_(row, (data,))

        if util.py3k:
            assert isinstance(row[0], int)
        else:
            assert isinstance(row[0], (long, int))  # noqa


class NumericTest(_LiteralRoundTripFixture, fixtures.TestBase):
    __backend__ = True

    @testing.emits_warning(r".*does \*not\* support Decimal objects natively")
    @testing.provide_metadata
    def _do_test(self, type_, input_, output, filter_=None, check_scale=False):
        metadata = self.metadata
        t = Table("t", metadata, Column("x", type_))
        t.create()
        t.insert().execute([{"x": x} for x in input_])

        result = {row[0] for row in t.select().execute()}
        output = set(output)
        if filter_:
            result = set(filter_(x) for x in result)
            output = set(filter_(x) for x in output)
        eq_(result, output)
        if check_scale:
            eq_([str(x) for x in result], [str(x) for x in output])

    @testing.emits_warning(r".*does \*not\* support Decimal objects natively")
    def test_render_literal_numeric(self):
        self._literal_round_trip(
            Numeric(precision=8, scale=4),
            [15.7563, decimal.Decimal("15.7563")],
            [decimal.Decimal("15.7563")],
        )

    @testing.emits_warning(r".*does \*not\* support Decimal objects natively")
    def test_render_literal_numeric_asfloat(self):
        self._literal_round_trip(
            Numeric(precision=8, scale=4, asdecimal=False),
            [15.7563, decimal.Decimal("15.7563")],
            [15.7563],
        )

    def test_render_literal_float(self):
        self._literal_round_trip(
            Float(4),
            [15.7563, decimal.Decimal("15.7563")],
            [15.7563],
            filter_=lambda n: n is not None and round(n, 5) or None,
        )

    @testing.requires.precision_generic_float_type
    def test_float_custom_scale(self):
        self._do_test(
            Float(None, decimal_return_scale=7, asdecimal=True),
            [15.7563827, decimal.Decimal("15.7563827")],
            [decimal.Decimal("15.7563827")],
            check_scale=True,
        )

    def test_numeric_as_decimal(self):
        self._do_test(
            Numeric(precision=8, scale=4),
            [15.7563, decimal.Decimal("15.7563")],
            [decimal.Decimal("15.7563")],
        )

    def test_numeric_as_float(self):
        self._do_test(
            Numeric(precision=8, scale=4, asdecimal=False),
            [15.7563, decimal.Decimal("15.7563")],
            [15.7563],
        )

    @testing.requires.fetch_null_from_numeric
    def test_numeric_null_as_decimal(self):
        self._do_test(Numeric(precision=8, scale=4), [None], [None])

    @testing.requires.fetch_null_from_numeric
    def test_numeric_null_as_float(self):
        self._do_test(
            Numeric(precision=8, scale=4, asdecimal=False), [None], [None]
        )

    @testing.requires.floats_to_four_decimals
    def test_float_as_decimal(self):
        self._do_test(
            Float(precision=8, asdecimal=True),
            [15.7563, decimal.Decimal("15.7563"), None],
            [decimal.Decimal("15.7563"), None],
        )

    def test_float_as_float(self):
        self._do_test(
            Float(precision=8),
            [15.7563, decimal.Decimal("15.7563")],
            [15.7563],
            filter_=lambda n: n is not None and round(n, 5) or None,
        )

    def test_float_coerce_round_trip(self):
        expr = 15.7563

        val = testing.db.scalar(select([literal(expr)]))
        eq_(val, expr)

    # this does not work in MySQL, see #4036, however we choose not
    # to render CAST unconditionally since this is kind of an edge case.

    @testing.requires.implicit_decimal_binds
    @testing.emits_warning(r".*does \*not\* support Decimal objects natively")
    def test_decimal_coerce_round_trip(self):
        expr = decimal.Decimal("15.7563")

        val = testing.db.scalar(select([literal(expr)]))
        eq_(val, expr)

    @testing.emits_warning(r".*does \*not\* support Decimal objects natively")
    def test_decimal_coerce_round_trip_w_cast(self):
        expr = decimal.Decimal("15.7563")

        val = testing.db.scalar(select([cast(expr, Numeric(10, 4))]))
        eq_(val, expr)

    @testing.requires.precision_numerics_general
    def test_precision_decimal(self):
        numbers = set(
            [
                decimal.Decimal("54.234246451650"),
                decimal.Decimal("0.004354"),
                decimal.Decimal("900.0"),
            ]
        )

        self._do_test(Numeric(precision=18, scale=12), numbers, numbers)

    @testing.requires.precision_numerics_enotation_large
    def test_enotation_decimal(self):
        """test exceedingly small decimals.

        Decimal reports values with E notation when the exponent
        is greater than 6.

        """

        numbers = set(
            [
                decimal.Decimal("1E-2"),
                decimal.Decimal("1E-3"),
                decimal.Decimal("1E-4"),
                decimal.Decimal("1E-5"),
                decimal.Decimal("1E-6"),
                decimal.Decimal("1E-7"),
                decimal.Decimal("1E-8"),
                decimal.Decimal("0.01000005940696"),
                decimal.Decimal("0.00000005940696"),
                decimal.Decimal("0.00000000000696"),
                decimal.Decimal("0.70000000000696"),
                decimal.Decimal("696E-12"),
            ]
        )
        self._do_test(Numeric(precision=18, scale=14), numbers, numbers)

    @testing.requires.precision_numerics_enotation_large
    def test_enotation_decimal_large(self):
        """test exceedingly large decimals.

        """

        numbers = set(
            [
                decimal.Decimal("4E+8"),
                decimal.Decimal("5748E+15"),
                decimal.Decimal("1.521E+15"),
                decimal.Decimal("00000000000000.1E+12"),
            ]
        )
        self._do_test(Numeric(precision=25, scale=2), numbers, numbers)

    @testing.requires.precision_numerics_many_significant_digits
    def test_many_significant_digits(self):
        numbers = set(
            [
                decimal.Decimal("31943874831932418390.01"),
                decimal.Decimal("319438950232418390.273596"),
                decimal.Decimal("87673.594069654243"),
            ]
        )
        self._do_test(Numeric(precision=38, scale=12), numbers, numbers)

    @testing.requires.precision_numerics_retains_significant_digits
    def test_numeric_no_decimal(self):
        numbers = set([decimal.Decimal("1.000")])
        self._do_test(
            Numeric(precision=5, scale=3), numbers, numbers, check_scale=True
        )


class BooleanTest(_LiteralRoundTripFixture, fixtures.TablesTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "boolean_table",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=False),
            Column("value", Boolean),
            Column("unconstrained_value", Boolean(create_constraint=False)),
        )

    def test_render_literal_bool(self):
        self._literal_round_trip(Boolean(), [True, False], [True, False])

    def test_round_trip(self):
        boolean_table = self.tables.boolean_table

        config.db.execute(
            boolean_table.insert(),
            {"id": 1, "value": True, "unconstrained_value": False},
        )

        row = config.db.execute(
            select(
                [boolean_table.c.value, boolean_table.c.unconstrained_value]
            )
        ).first()

        eq_(row, (True, False))
        assert isinstance(row[0], bool)

    def test_null(self):
        boolean_table = self.tables.boolean_table

        config.db.execute(
            boolean_table.insert(),
            {"id": 1, "value": None, "unconstrained_value": None},
        )

        row = config.db.execute(
            select(
                [boolean_table.c.value, boolean_table.c.unconstrained_value]
            )
        ).first()

        eq_(row, (None, None))

    def test_whereclause(self):
        # testing "WHERE <column>" renders a compatible expression
        boolean_table = self.tables.boolean_table

        with config.db.connect() as conn:
            conn.execute(
                boolean_table.insert(),
                [
                    {"id": 1, "value": True, "unconstrained_value": True},
                    {"id": 2, "value": False, "unconstrained_value": False},
                ],
            )

            eq_(
                conn.scalar(
                    select([boolean_table.c.id]).where(boolean_table.c.value)
                ),
                1,
            )
            eq_(
                conn.scalar(
                    select([boolean_table.c.id]).where(
                        boolean_table.c.unconstrained_value
                    )
                ),
                1,
            )
            eq_(
                conn.scalar(
                    select([boolean_table.c.id]).where(~boolean_table.c.value)
                ),
                2,
            )
            eq_(
                conn.scalar(
                    select([boolean_table.c.id]).where(
                        ~boolean_table.c.unconstrained_value
                    )
                ),
                2,
            )


class JSONTest(_LiteralRoundTripFixture, fixtures.TablesTest):
    __requires__ = ("json_type",)
    __backend__ = True

    datatype = JSON

    data1 = {"key1": "value1", "key2": "value2"}

    data2 = {
        "Key 'One'": "value1",
        "key two": "value2",
        "key three": "value ' three '",
    }

    data3 = {
        "key1": [1, 2, 3],
        "key2": ["one", "two", "three"],
        "key3": [{"four": "five"}, {"six": "seven"}],
    }

    data4 = ["one", "two", "three"]

    data5 = {
        "nested": {
            "elem1": [{"a": "b", "c": "d"}, {"e": "f", "g": "h"}],
            "elem2": {"elem3": {"elem4": "elem5"}},
        }
    }

    data6 = {"a": 5, "b": "some value", "c": {"foo": "bar"}}

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "data_table",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(30), nullable=False),
            Column("data", cls.datatype),
            Column("nulldata", cls.datatype(none_as_null=True)),
        )

    def test_round_trip_data1(self):
        self._test_round_trip(self.data1)

    def _test_round_trip(self, data_element):
        data_table = self.tables.data_table

        config.db.execute(
            data_table.insert(), {"name": "row1", "data": data_element}
        )

        row = config.db.execute(select([data_table.c.data])).first()

        eq_(row, (data_element,))

    def test_round_trip_none_as_sql_null(self):
        col = self.tables.data_table.c["nulldata"]

        with config.db.connect() as conn:
            conn.execute(
                self.tables.data_table.insert(), {"name": "r1", "data": None}
            )

            eq_(
                conn.scalar(
                    select([self.tables.data_table.c.name]).where(
                        col.is_(null())
                    )
                ),
                "r1",
            )

            eq_(conn.scalar(select([col])), None)

    def test_round_trip_json_null_as_json_null(self):
        col = self.tables.data_table.c["data"]

        with config.db.connect() as conn:
            conn.execute(
                self.tables.data_table.insert(),
                {"name": "r1", "data": JSON.NULL},
            )

            eq_(
                conn.scalar(
                    select([self.tables.data_table.c.name]).where(
                        cast(col, String) == "null"
                    )
                ),
                "r1",
            )

            eq_(conn.scalar(select([col])), None)

    def test_round_trip_none_as_json_null(self):
        col = self.tables.data_table.c["data"]

        with config.db.connect() as conn:
            conn.execute(
                self.tables.data_table.insert(), {"name": "r1", "data": None}
            )

            eq_(
                conn.scalar(
                    select([self.tables.data_table.c.name]).where(
                        cast(col, String) == "null"
                    )
                ),
                "r1",
            )

            eq_(conn.scalar(select([col])), None)

    def _criteria_fixture(self):
        config.db.execute(
            self.tables.data_table.insert(),
            [
                {"name": "r1", "data": self.data1},
                {"name": "r2", "data": self.data2},
                {"name": "r3", "data": self.data3},
                {"name": "r4", "data": self.data4},
                {"name": "r5", "data": self.data5},
                {"name": "r6", "data": self.data6},
            ],
        )

    def _test_index_criteria(self, crit, expected, test_literal=True):
        self._criteria_fixture()
        with config.db.connect() as conn:
            stmt = select([self.tables.data_table.c.name]).where(crit)

            eq_(conn.scalar(stmt), expected)

            if test_literal:
                literal_sql = str(
                    stmt.compile(
                        config.db, compile_kwargs={"literal_binds": True}
                    )
                )

                eq_(conn.scalar(literal_sql), expected)

    def test_crit_spaces_in_key(self):
        name = self.tables.data_table.c.name
        col = self.tables.data_table.c["data"]

        # limit the rows here to avoid PG error
        # "cannot extract field from a non-object", which is
        # fixed in 9.4 but may exist in 9.3
        self._test_index_criteria(
            and_(
                name.in_(["r1", "r2", "r3"]),
                cast(col["key two"], String) == '"value2"',
            ),
            "r2",
        )

    @config.requirements.json_array_indexes
    def test_crit_simple_int(self):
        name = self.tables.data_table.c.name
        col = self.tables.data_table.c["data"]

        # limit the rows here to avoid PG error
        # "cannot extract array element from a non-array", which is
        # fixed in 9.4 but may exist in 9.3
        self._test_index_criteria(
            and_(name == "r4", cast(col[1], String) == '"two"'), "r4"
        )

    def test_crit_mixed_path(self):
        col = self.tables.data_table.c["data"]
        self._test_index_criteria(
            cast(col[("key3", 1, "six")], String) == '"seven"', "r3"
        )

    def test_crit_string_path(self):
        col = self.tables.data_table.c["data"]
        self._test_index_criteria(
            cast(col[("nested", "elem2", "elem3", "elem4")], String)
            == '"elem5"',
            "r5",
        )

    def test_crit_against_string_basic(self):
        name = self.tables.data_table.c.name
        col = self.tables.data_table.c["data"]

        self._test_index_criteria(
            and_(name == "r6", cast(col["b"], String) == '"some value"'), "r6"
        )

    def test_crit_against_string_coerce_type(self):
        name = self.tables.data_table.c.name
        col = self.tables.data_table.c["data"]

        self._test_index_criteria(
            and_(
                name == "r6",
                cast(col["b"], String) == type_coerce("some value", JSON),
            ),
            "r6",
            test_literal=False,
        )

    def test_crit_against_int_basic(self):
        name = self.tables.data_table.c.name
        col = self.tables.data_table.c["data"]

        self._test_index_criteria(
            and_(name == "r6", cast(col["a"], String) == "5"), "r6"
        )

    def test_crit_against_int_coerce_type(self):
        name = self.tables.data_table.c.name
        col = self.tables.data_table.c["data"]

        self._test_index_criteria(
            and_(name == "r6", cast(col["a"], String) == type_coerce(5, JSON)),
            "r6",
            test_literal=False,
        )

    def test_unicode_round_trip(self):
        with config.db.connect() as conn:
            conn.execute(
                self.tables.data_table.insert(),
                {
                    "name": "r1",
                    "data": {
                        util.u("réve🐍 illé"): util.u("réve🐍 illé"),
                        "data": {"k1": util.u("drôl🐍e")},
                    },
                },
            )

            eq_(
                conn.scalar(select([self.tables.data_table.c.data])),
                {
                    util.u("réve🐍 illé"): util.u("réve🐍 illé"),
                    "data": {"k1": util.u("drôl🐍e")},
                },
            )

    def test_eval_none_flag_orm(self):

        Base = declarative_base()

        class Data(Base):
            __table__ = self.tables.data_table

        s = Session(testing.db)

        d1 = Data(name="d1", data=None, nulldata=None)
        s.add(d1)
        s.commit()

        s.bulk_insert_mappings(
            Data, [{"name": "d2", "data": None, "nulldata": None}]
        )
        eq_(
            s.query(
                cast(self.tables.data_table.c.data, String()),
                cast(self.tables.data_table.c.nulldata, String),
            )
            .filter(self.tables.data_table.c.name == "d1")
            .first(),
            ("null", None),
        )
        eq_(
            s.query(
                cast(self.tables.data_table.c.data, String()),
                cast(self.tables.data_table.c.nulldata, String),
            )
            .filter(self.tables.data_table.c.name == "d2")
            .first(),
            ("null", None),
        )


__all__ = (
    "UnicodeVarcharTest",
    "UnicodeTextTest",
    "JSONTest",
    "DateTest",
    "DateTimeTest",
    "TextTest",
    "NumericTest",
    "IntegerTest",
    "DateTimeHistoricTest",
    "DateTimeCoercedToDateTimeTest",
    "TimeMicrosecondsTest",
    "TimestampMicrosecondsTest",
    "TimeTest",
    "DateTimeMicrosecondsTest",
    "DateHistoricTest",
    "StringTest",
    "BooleanTest",
)
