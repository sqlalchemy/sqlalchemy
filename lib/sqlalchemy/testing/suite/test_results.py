from .. import fixtures, config
from ..config import requirements
from .. import exclusions
from ..assertions import eq_
from .. import engines

from sqlalchemy import Integer, String, select, util, sql, DateTime
import datetime
from ..schema import Table, Column


class RowFetchTest(fixtures.TablesTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table('plain_pk', metadata,
              Column('id', Integer, primary_key=True),
              Column('data', String(50))
              )
        Table('has_dates', metadata,
              Column('id', Integer, primary_key=True),
              Column('today', DateTime)
              )

    @classmethod
    def insert_data(cls):
        config.db.execute(
            cls.tables.plain_pk.insert(),
            [
                {"id": 1, "data": "d1"},
                {"id": 2, "data": "d2"},
                {"id": 3, "data": "d3"},
            ]
        )

        config.db.execute(
            cls.tables.has_dates.insert(),
            [
                {"id": 1, "today": datetime.datetime(2006, 5, 12, 12, 0, 0)}
            ]
        )

    def test_via_string(self):
        row = config.db.execute(
            self.tables.plain_pk.select().
            order_by(self.tables.plain_pk.c.id)
        ).first()

        eq_(
            row['id'], 1
        )
        eq_(
            row['data'], "d1"
        )

    def test_via_int(self):
        row = config.db.execute(
            self.tables.plain_pk.select().
            order_by(self.tables.plain_pk.c.id)
        ).first()

        eq_(
            row[0], 1
        )
        eq_(
            row[1], "d1"
        )

    def test_via_col_object(self):
        row = config.db.execute(
            self.tables.plain_pk.select().
            order_by(self.tables.plain_pk.c.id)
        ).first()

        eq_(
            row[self.tables.plain_pk.c.id], 1
        )
        eq_(
            row[self.tables.plain_pk.c.data], "d1"
        )

    @requirements.duplicate_names_in_cursor_description
    def test_row_with_dupe_names(self):
        result = config.db.execute(
            select([self.tables.plain_pk.c.data,
                    self.tables.plain_pk.c.data.label('data')]).
            order_by(self.tables.plain_pk.c.id)
        )
        row = result.first()
        eq_(result.keys(), ['data', 'data'])
        eq_(row, ('d1', 'd1'))

    def test_row_w_scalar_select(self):
        """test that a scalar select as a column is returned as such
        and that type conversion works OK.

        (this is half a SQLAlchemy Core test and half to catch database
        backends that may have unusual behavior with scalar selects.)

        """
        datetable = self.tables.has_dates
        s = select([datetable.alias('x').c.today]).as_scalar()
        s2 = select([datetable.c.id, s.label('somelabel')])
        row = config.db.execute(s2).first()

        eq_(row['somelabel'], datetime.datetime(2006, 5, 12, 12, 0, 0))


class PercentSchemaNamesTest(fixtures.TablesTest):
    """tests using percent signs, spaces in table and column names.

    This is a very fringe use case, doesn't work for MySQL
    or Postgresql.  the requirement, "percent_schema_names",
    is marked "skip" by default.

    """

    __requires__ = ('percent_schema_names', )

    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        cls.tables.percent_table = Table('percent%table', metadata,
                                         Column("percent%", Integer),
                                         Column(
                                             "spaces % more spaces", Integer),
                                         )
        cls.tables.lightweight_percent_table = sql.table(
            'percent%table', sql.column("percent%"),
            sql.column("spaces % more spaces")
        )

    def test_single_roundtrip(self):
        percent_table = self.tables.percent_table
        for params in [
            {'percent%': 5, 'spaces % more spaces': 12},
            {'percent%': 7, 'spaces % more spaces': 11},
            {'percent%': 9, 'spaces % more spaces': 10},
            {'percent%': 11, 'spaces % more spaces': 9}
        ]:
            config.db.execute(percent_table.insert(), params)
        self._assert_table()

    def test_executemany_roundtrip(self):
        percent_table = self.tables.percent_table
        config.db.execute(
            percent_table.insert(),
            {'percent%': 5, 'spaces % more spaces': 12}
        )
        config.db.execute(
            percent_table.insert(),
            [{'percent%': 7, 'spaces % more spaces': 11},
             {'percent%': 9, 'spaces % more spaces': 10},
             {'percent%': 11, 'spaces % more spaces': 9}]
        )
        self._assert_table()

    def _assert_table(self):
        percent_table = self.tables.percent_table
        lightweight_percent_table = self.tables.lightweight_percent_table

        for table in (
                percent_table,
                percent_table.alias(),
                lightweight_percent_table,
                lightweight_percent_table.alias()):
            eq_(
                list(
                    config.db.execute(
                        table.select().order_by(table.c['percent%'])
                    )
                ),
                [
                    (5, 12),
                    (7, 11),
                    (9, 10),
                    (11, 9)
                ]
            )

            eq_(
                list(
                    config.db.execute(
                        table.select().
                        where(table.c['spaces % more spaces'].in_([9, 10])).
                        order_by(table.c['percent%']),
                    )
                ),
                [
                    (9, 10),
                    (11, 9)
                ]
            )

            row = config.db.execute(table.select().
                                    order_by(table.c['percent%'])).first()
            eq_(row['percent%'], 5)
            eq_(row['spaces % more spaces'], 12)

            eq_(row[table.c['percent%']], 5)
            eq_(row[table.c['spaces % more spaces']], 12)

        config.db.execute(
            percent_table.update().values(
                {percent_table.c['spaces % more spaces']: 15}
            )
        )

        eq_(
            list(
                config.db.execute(
                    percent_table.
                    select().
                    order_by(percent_table.c['percent%'])
                )
            ),
            [(5, 15), (7, 15), (9, 15), (11, 15)]
        )
