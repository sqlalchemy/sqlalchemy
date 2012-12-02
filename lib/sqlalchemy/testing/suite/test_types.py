# coding: utf-8

from .. import fixtures, config
from ..assertions import eq_
from ..config import requirements
from sqlalchemy import Integer, Unicode, UnicodeText, select
from sqlalchemy import Date, DateTime, Time, MetaData, String
from ..schema import Table, Column
import datetime


class _UnicodeFixture(object):
    __requires__ = 'unicode_data',

    data = u"Alors vous imaginez ma surprise, au lever du jour, "\
                u"quand une drôle de petite voix m’a réveillé. Elle "\
                u"disait: « S’il vous plaît… dessine-moi un mouton! »"

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
        assert isinstance(row[0], unicode)

    def test_round_trip_executemany(self):
        unicode_table = self.tables.unicode_table

        config.db.execute(
            unicode_table.insert(),
            [
                {
                    'unicode_data': self.data,
                }
                for i in xrange(3)
            ]
        )

        rows = config.db.execute(
                    select([
                            unicode_table.c.unicode_data,
                    ])
                ).fetchall()
        eq_(
            rows,
            [(self.data, ) for i in xrange(3)]
        )
        for row in rows:
            assert isinstance(row[0], unicode)

    def _test_empty_strings(self):
        unicode_table = self.tables.unicode_table

        config.db.execute(
            unicode_table.insert(),
            {"unicode_data": u''}
        )
        row = config.db.execute(
                    select([unicode_table.c.unicode_data])
                ).first()
        eq_(row, (u'',))


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


class StringTest(fixtures.TestBase):
    @requirements.unbounded_varchar
    def test_nolength_string(self):
        metadata = MetaData()
        foo = Table('foo', metadata,
                    Column('one', String)
                )

        foo.create(config.db)
        foo.drop(config.db)


class _DateFixture(object):
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


__all__ = ('UnicodeVarcharTest', 'UnicodeTextTest',
            'DateTest', 'DateTimeTest',
            'DateTimeHistoricTest', 'DateTimeCoercedToDateTimeTest',
            'TimeMicrosecondsTest', 'TimeTest', 'DateTimeMicrosecondsTest',
            'DateHistoricTest', 'StringTest')
