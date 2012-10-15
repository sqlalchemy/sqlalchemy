# coding: utf-8

from .. import fixtures, config
from ..assertions import eq_
from ..config import requirements
from sqlalchemy import Integer, Unicode, UnicodeText, select
from ..schema import Table, Column


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


    @requirements.empty_strings
    def test_empty_strings(self):
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


class UnicodeTextTest(_UnicodeFixture, fixtures.TablesTest):
    __requires__ = 'unicode_data', 'text_type'

    datatype = UnicodeText()

__all__ = ('UnicodeVarcharTest', 'UnicodeTextTest')