from .. import fixtures, config
from ..config import requirements
from .. import exclusions
from ..assertions import eq_
from .. import engines
from ... import testing

from sqlalchemy import Integer, String, select, util, sql, DateTime, text, func
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
    or PostgreSQL.  the requirement, "percent_schema_names",
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


class ServerSideCursorsTest(fixtures.TestBase, testing.AssertsExecutionResults):

    __requires__ = ('server_side_cursors', )

    __backend__ = True

    def _is_server_side(self, cursor):
        if self.engine.dialect.driver == "psycopg2":
            return cursor.name
        elif self.engine.dialect.driver == 'pymysql':
            sscursor = __import__('pymysql.cursors').cursors.SSCursor
            return isinstance(cursor, sscursor)
        elif self.engine.dialect.driver == "mysqldb":
            sscursor = __import__('MySQLdb.cursors').cursors.SSCursor
            return isinstance(cursor, sscursor)
        else:
            return False

    def _fixture(self, server_side_cursors):
        self.engine = engines.testing_engine(
            options={'server_side_cursors': server_side_cursors}
        )
        return self.engine

    def tearDown(self):
        engines.testing_reaper.close_all()
        self.engine.dispose()

    def test_global_string(self):
        engine = self._fixture(True)
        result = engine.execute('select 1')
        assert self._is_server_side(result.cursor)

    def test_global_text(self):
        engine = self._fixture(True)
        result = engine.execute(text('select 1'))
        assert self._is_server_side(result.cursor)

    def test_global_expr(self):
        engine = self._fixture(True)
        result = engine.execute(select([1]))
        assert self._is_server_side(result.cursor)

    def test_global_off_explicit(self):
        engine = self._fixture(False)
        result = engine.execute(text('select 1'))

        # It should be off globally ...

        assert not self._is_server_side(result.cursor)

    def test_stmt_option(self):
        engine = self._fixture(False)

        s = select([1]).execution_options(stream_results=True)
        result = engine.execute(s)

        # ... but enabled for this one.

        assert self._is_server_side(result.cursor)

    def test_conn_option(self):
        engine = self._fixture(False)

        # and this one
        result = \
            engine.connect().execution_options(stream_results=True).\
            execute('select 1'
                    )
        assert self._is_server_side(result.cursor)

    def test_stmt_enabled_conn_option_disabled(self):
        engine = self._fixture(False)

        s = select([1]).execution_options(stream_results=True)

        # not this one
        result = \
            engine.connect().execution_options(stream_results=False).\
            execute(s)
        assert not self._is_server_side(result.cursor)

    def test_stmt_option_disabled(self):
        engine = self._fixture(True)
        s = select([1]).execution_options(stream_results=False)
        result = engine.execute(s)
        assert not self._is_server_side(result.cursor)

    def test_aliases_and_ss(self):
        engine = self._fixture(False)
        s1 = select([1]).execution_options(stream_results=True).alias()
        result = engine.execute(s1)
        assert self._is_server_side(result.cursor)

        # s1's options shouldn't affect s2 when s2 is used as a
        # from_obj.
        s2 = select([1], from_obj=s1)
        result = engine.execute(s2)
        assert not self._is_server_side(result.cursor)

    def test_for_update_expr(self):
        engine = self._fixture(True)
        s1 = select([1], for_update=True)
        result = engine.execute(s1)
        assert self._is_server_side(result.cursor)

    def test_for_update_string(self):
        engine = self._fixture(True)
        result = engine.execute('SELECT 1 FOR UPDATE')
        assert self._is_server_side(result.cursor)

    def test_text_no_ss(self):
        engine = self._fixture(False)
        s = text('select 42')
        result = engine.execute(s)
        assert not self._is_server_side(result.cursor)

    def test_text_ss_option(self):
        engine = self._fixture(False)
        s = text('select 42').execution_options(stream_results=True)
        result = engine.execute(s)
        assert self._is_server_side(result.cursor)

    @testing.provide_metadata
    def test_roundtrip(self):
        md = self.metadata

        engine = self._fixture(True)
        test_table = Table('test_table', md,
                           Column('id', Integer, primary_key=True),
                           Column('data', String(50)))
        test_table.create(checkfirst=True)
        test_table.insert().execute(data='data1')
        test_table.insert().execute(data='data2')
        eq_(test_table.select().order_by(test_table.c.id).execute().fetchall(),
            [(1, 'data1'), (2, 'data2')])
        test_table.update().where(
            test_table.c.id == 2).values(
            data=test_table.c.data +
            ' updated').execute()
        eq_(test_table.select().order_by(test_table.c.id).execute().fetchall(),
            [(1, 'data1'), (2, 'data2 updated')])
        test_table.delete().execute()
        eq_(select([func.count('*')]).select_from(test_table).scalar(), 0)
