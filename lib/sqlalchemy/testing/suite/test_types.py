# coding: utf-8

from .. import fixtures, config
from ..assertions import eq_
from ..config import requirements
from sqlalchemy import Integer, Unicode, UnicodeText, select
from sqlalchemy import Date, DateTime, Time, MetaData, String, \
            Text, Numeric, Float, literal
from ..schema import Table, Column
from ... import testing
import decimal
import datetime
from ...util import u
from ... import util


class _LiteralRoundTripFixture(object):
    @testing.provide_metadata
    def _literal_round_trip(self, type_, input_, output, filter_=None):
        """test literal rendering """

        # for literal, we test the literal render in an INSERT
        # into a typed column.  we can then SELECT it back as it's
        # official type; ideally we'd be able to use CAST here
        # but MySQL in particular can't CAST fully
        t = Table('t', self.metadata, Column('x', type_))
        t.create()

        for value in input_:
            ins = t.insert().values(x=literal(value)).compile(
                            dialect=testing.db.dialect,
                            compile_kwargs=dict(literal_binds=True)
                        )
            testing.db.execute(ins)

        for row in t.select().execute():
            value = row[0]
            if filter_ is not None:
                value = filter_(value)
            assert value in output


class _UnicodeFixture(_LiteralRoundTripFixture):
    __requires__ = 'unicode_data',

    data = u("Alors vous imaginez ma surprise, au lever du jour, "\
                "quand une drôle de petite voix m’a réveillé. Elle "\
                "disait: « S’il vous plaît… dessine-moi un mouton! »")

    @classmethod
    def define_tables(cls, metadata):
        Table('unicode_table', metadata,
            Column('id', Integer, primary_key=True,
                        test_needs_autoincrement=True),
            Column('unicode_data', cls.datatype),
            )

    def test_round_trip(self):
        unicode_table = self.tables.unicode_table

        config.db.execute(
            unicode_table.insert(),
            {
                'unicode_data': self.data,
            }
        )

        row = config.db.execute(
                    select([
                            unicode_table.c.unicode_data,
                    ])
                ).first()

        eq_(
            row,
            (self.data, )
        )
        assert isinstance(row[0], util.text_type)

    def test_round_trip_executemany(self):
        unicode_table = self.tables.unicode_table

        config.db.execute(
            unicode_table.insert(),
            [
                {
                    'unicode_data': self.data,
                }
                for i in range(3)
            ]
        )

        rows = config.db.execute(
                    select([
                            unicode_table.c.unicode_data,
                    ])
                ).fetchall()
        eq_(
            rows,
            [(self.data, ) for i in range(3)]
        )
        for row in rows:
            assert isinstance(row[0], util.text_type)

    def _test_empty_strings(self):
        unicode_table = self.tables.unicode_table

        config.db.execute(
            unicode_table.insert(),
            {"unicode_data": u('')}
        )
        row = config.db.execute(
                    select([unicode_table.c.unicode_data])
                ).first()
        eq_(row, (u(''),))

    def test_literal(self):
        self._literal_round_trip(self.datatype, [self.data], [self.data])


class UnicodeVarcharTest(_UnicodeFixture, fixtures.TablesTest):
    __requires__ = 'unicode_data',

    datatype = Unicode(255)

    @requirements.empty_strings_varchar
    def test_empty_strings_varchar(self):
        self._test_empty_strings()


class UnicodeTextTest(_UnicodeFixture, fixtures.TablesTest):
    __requires__ = 'unicode_data', 'text_type'

    datatype = UnicodeText()

    @requirements.empty_strings_text
    def test_empty_strings_text(self):
        self._test_empty_strings()

class TextTest(_LiteralRoundTripFixture, fixtures.TablesTest):
    @classmethod
    def define_tables(cls, metadata):
        Table('text_table', metadata,
            Column('id', Integer, primary_key=True,
                        test_needs_autoincrement=True),
            Column('text_data', Text),
            )

    def test_text_roundtrip(self):
        text_table = self.tables.text_table

        config.db.execute(
            text_table.insert(),
            {"text_data": 'some text'}
        )
        row = config.db.execute(
                    select([text_table.c.text_data])
                ).first()
        eq_(row, ('some text',))

    def test_text_empty_strings(self):
        text_table = self.tables.text_table

        config.db.execute(
            text_table.insert(),
            {"text_data": ''}
        )
        row = config.db.execute(
                    select([text_table.c.text_data])
                ).first()
        eq_(row, ('',))

    def test_literal(self):
        self._literal_round_trip(Text, ["some text"], ["some text"])

    def test_literal_quoting(self):
        data = '''some 'text' hey "hi there" that's text'''
        self._literal_round_trip(Text, [data], [data])

    def test_literal_backslashes(self):
        data = r'backslash one \ backslash two \\ end'
        self._literal_round_trip(Text, [data], [data])

class StringTest(_LiteralRoundTripFixture, fixtures.TestBase):
    @requirements.unbounded_varchar
    def test_nolength_string(self):
        metadata = MetaData()
        foo = Table('foo', metadata,
                    Column('one', String)
                )

        foo.create(config.db)
        foo.drop(config.db)

    def test_literal(self):
        self._literal_round_trip(String(40), ["some text"], ["some text"])

    def test_literal_quoting(self):
        data = '''some 'text' hey "hi there" that's text'''
        self._literal_round_trip(String(40), [data], [data])

    def test_literal_backslashes(self):
        data = r'backslash one \ backslash two \\ end'
        self._literal_round_trip(Text, [data], [data])


class _DateFixture(_LiteralRoundTripFixture):
    compare = None

    @classmethod
    def define_tables(cls, metadata):
        Table('date_table', metadata,
            Column('id', Integer, primary_key=True,
                        test_needs_autoincrement=True),
            Column('date_data', cls.datatype),
            )

    def test_round_trip(self):
        date_table = self.tables.date_table

        config.db.execute(
            date_table.insert(),
            {'date_data': self.data}
        )

        row = config.db.execute(
                    select([
                            date_table.c.date_data,
                    ])
                ).first()

        compare = self.compare or self.data
        eq_(row,
            (compare, ))
        assert isinstance(row[0], type(compare))

    def test_null(self):
        date_table = self.tables.date_table

        config.db.execute(
            date_table.insert(),
            {'date_data': None}
        )

        row = config.db.execute(
                    select([
                            date_table.c.date_data,
                    ])
                ).first()
        eq_(row, (None,))

    @testing.requires.datetime_literals
    def test_literal(self):
        compare = self.compare or self.data
        self._literal_round_trip(self.datatype, [self.data], [compare])



class DateTimeTest(_DateFixture, fixtures.TablesTest):
    __requires__ = 'datetime',
    datatype = DateTime
    data = datetime.datetime(2012, 10, 15, 12, 57, 18)


class DateTimeMicrosecondsTest(_DateFixture, fixtures.TablesTest):
    __requires__ = 'datetime_microseconds',
    datatype = DateTime
    data = datetime.datetime(2012, 10, 15, 12, 57, 18, 396)


class TimeTest(_DateFixture, fixtures.TablesTest):
    __requires__ = 'time',
    datatype = Time
    data = datetime.time(12, 57, 18)


class TimeMicrosecondsTest(_DateFixture, fixtures.TablesTest):
    __requires__ = 'time_microseconds',
    datatype = Time
    data = datetime.time(12, 57, 18, 396)


class DateTest(_DateFixture, fixtures.TablesTest):
    __requires__ = 'date',
    datatype = Date
    data = datetime.date(2012, 10, 15)


class DateTimeCoercedToDateTimeTest(_DateFixture, fixtures.TablesTest):
    __requires__ = 'date',
    datatype = Date
    data = datetime.datetime(2012, 10, 15, 12, 57, 18)
    compare = datetime.date(2012, 10, 15)


class DateTimeHistoricTest(_DateFixture, fixtures.TablesTest):
    __requires__ = 'datetime_historic',
    datatype = DateTime
    data = datetime.datetime(1850, 11, 10, 11, 52, 35)


class DateHistoricTest(_DateFixture, fixtures.TablesTest):
    __requires__ = 'date_historic',
    datatype = Date
    data = datetime.date(1727, 4, 1)


class IntegerTest(_LiteralRoundTripFixture, fixtures.TestBase):
    def test_literal(self):
        self._literal_round_trip(Integer, [5], [5])

class NumericTest(_LiteralRoundTripFixture, fixtures.TestBase):

    @testing.emits_warning(r".*does \*not\* support Decimal objects natively")
    @testing.provide_metadata
    def _do_test(self, type_, input_, output, filter_=None, check_scale=False):
        metadata = self.metadata
        t = Table('t', metadata, Column('x', type_))
        t.create()
        t.insert().execute([{'x':x} for x in input_])

        result = set([row[0] for row in t.select().execute()])
        output = set(output)
        if filter_:
            result = set(filter_(x) for x in result)
            output = set(filter_(x) for x in output)
        eq_(result, output)
        if check_scale:
            eq_(
                [str(x) for x in result],
                [str(x) for x in output],
            )


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
            [15.7563,],
            filter_=lambda n: n is not None and round(n, 5) or None
        )


    @testing.requires.precision_generic_float_type
    def test_float_custom_scale(self):
        self._do_test(
            Float(None, decimal_return_scale=7, asdecimal=True),
            [15.7563827, decimal.Decimal("15.7563827")],
            [decimal.Decimal("15.7563827"),],
            check_scale=True
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
        self._do_test(
            Numeric(precision=8, scale=4),
            [None],
            [None],
        )

    @testing.requires.fetch_null_from_numeric
    def test_numeric_null_as_float(self):
        self._do_test(
            Numeric(precision=8, scale=4, asdecimal=False),
            [None],
            [None],
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
            filter_=lambda n: n is not None and round(n, 5) or None
        )


    @testing.requires.precision_numerics_general
    def test_precision_decimal(self):
        numbers = set([
            decimal.Decimal("54.234246451650"),
            decimal.Decimal("0.004354"),
            decimal.Decimal("900.0"),
        ])

        self._do_test(
            Numeric(precision=18, scale=12),
            numbers,
            numbers,
        )


    @testing.requires.precision_numerics_enotation_large
    def test_enotation_decimal(self):
        """test exceedingly small decimals.

        Decimal reports values with E notation when the exponent
        is greater than 6.

        """

        numbers = set([
            decimal.Decimal('1E-2'),
            decimal.Decimal('1E-3'),
            decimal.Decimal('1E-4'),
            decimal.Decimal('1E-5'),
            decimal.Decimal('1E-6'),
            decimal.Decimal('1E-7'),
            decimal.Decimal('1E-8'),
            decimal.Decimal("0.01000005940696"),
            decimal.Decimal("0.00000005940696"),
            decimal.Decimal("0.00000000000696"),
            decimal.Decimal("0.70000000000696"),
            decimal.Decimal("696E-12"),
        ])
        self._do_test(
            Numeric(precision=18, scale=14),
            numbers,
            numbers
        )


    @testing.requires.precision_numerics_enotation_large
    def test_enotation_decimal_large(self):
        """test exceedingly large decimals.

        """

        numbers = set([
            decimal.Decimal('4E+8'),
            decimal.Decimal("5748E+15"),
            decimal.Decimal('1.521E+15'),
            decimal.Decimal('00000000000000.1E+12'),
        ])
        self._do_test(
            Numeric(precision=25, scale=2),
            numbers,
            numbers
        )

    @testing.requires.precision_numerics_many_significant_digits
    def test_many_significant_digits(self):
        numbers = set([
            decimal.Decimal("31943874831932418390.01"),
            decimal.Decimal("319438950232418390.273596"),
            decimal.Decimal("87673.594069654243"),
        ])
        self._do_test(
            Numeric(precision=38, scale=12),
            numbers,
            numbers
        )

    @testing.requires.precision_numerics_retains_significant_digits
    def test_numeric_no_decimal(self):
        numbers = set([
            decimal.Decimal("1.000")
        ])
        self._do_test(
            Numeric(precision=5, scale=3),
            numbers,
            numbers,
            check_scale=True
        )



__all__ = ('UnicodeVarcharTest', 'UnicodeTextTest',
            'DateTest', 'DateTimeTest', 'TextTest',
            'NumericTest', 'IntegerTest',
            'DateTimeHistoricTest', 'DateTimeCoercedToDateTimeTest',
            'TimeMicrosecondsTest', 'TimeTest', 'DateTimeMicrosecondsTest',
            'DateHistoricTest', 'StringTest')
