# coding: utf-8

from .. import fixtures, config
from ..assertions import eq_
from ..config import requirements
from sqlalchemy import Integer, Unicode, UnicodeText, select
from ..schema import Table, Column


class UnicodeTest(fixtures.TablesTest):
    __requires__ = 'unicode_data',

    data = u"Alors vous imaginez ma surprise, au lever du jour, "\
                u"quand une drôle de petite voix m’a réveillé. Elle "\
                u"disait: « S’il vous plaît… dessine-moi un mouton! »"

    @classmethod
    def define_tables(cls, metadata):
        Table('unicode_table', metadata,
            Column('id', Integer, primary_key=True,
                        test_needs_autoincrement=True),
            Column('unicode_varchar', Unicode(250)),
            Column('unicode_text', UnicodeText),
            )

    def test_round_trip(self):
        unicode_table = self.tables.unicode_table

        config.db.execute(
            unicode_table.insert(),
            {
                'unicode_varchar': self.data,
                'unicode_text': self.data
            }
        )

        row = config.db.execute(
                    select([
                            unicode_table.c.unicode_varchar,
                            unicode_table.c.unicode_text
                    ])
                ).first()

        eq_(
            row,
            (self.data, self.data)
        )
        assert isinstance(row[0], unicode)
        assert isinstance(row[1], unicode)

    def test_round_trip_executemany(self):
        unicode_table = self.tables.unicode_table

        config.db.execute(
            unicode_table.insert(),
            [
                {
                    'unicode_varchar': self.data,
                    'unicode_text': self.data
                }
                for i in xrange(3)
            ]
        )

        rows = config.db.execute(
                    select([
                            unicode_table.c.unicode_varchar,
                            unicode_table.c.unicode_text
                    ])
                ).fetchall()
        eq_(
            rows,
            [(self.data, self.data) for i in xrange(3)]
        )
        for row in rows:
            assert isinstance(row[0], unicode)
            assert isinstance(row[1], unicode)


    @requirements.empty_strings
    def test_empty_strings(self):
        unicode_table = self.tables.unicode_table

        config.db.execute(
            unicode_table.insert(),
            {"unicode_varchar": u''}
        )
        row = config.db.execute(
                    select([unicode_table.c.unicode_varchar])
                ).first()
        eq_(row, (u'',))


__all__ = ('UnicodeTest',)