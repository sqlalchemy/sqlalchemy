from .. import fixtures, config
from ..assertions import eq_

from sqlalchemy import util
from sqlalchemy import Integer, String, select, func, bindparam
from sqlalchemy import testing

from ..schema import Table, Column


class OrderByLabelTest(fixtures.TablesTest):
    """Test the dialect sends appropriate ORDER BY expressions when
    labels are used.

    This essentially exercises the "supports_simple_order_by_label"
    setting.

    """
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table("some_table", metadata,
              Column('id', Integer, primary_key=True),
              Column('x', Integer),
              Column('y', Integer),
              Column('q', String(50)),
              Column('p', String(50))
              )

    @classmethod
    def insert_data(cls):
        config.db.execute(
            cls.tables.some_table.insert(),
            [
                {"id": 1, "x": 1, "y": 2, "q": "q1", "p": "p3"},
                {"id": 2, "x": 2, "y": 3, "q": "q2", "p": "p2"},
                {"id": 3, "x": 3, "y": 4, "q": "q3", "p": "p1"},
            ]
        )

    def _assert_result(self, select, result):
        eq_(
            config.db.execute(select).fetchall(),
            result
        )

    def test_plain(self):
        table = self.tables.some_table
        lx = table.c.x.label('lx')
        self._assert_result(
            select([lx]).order_by(lx),
            [(1, ), (2, ), (3, )]
        )

    def test_composed_int(self):
        table = self.tables.some_table
        lx = (table.c.x + table.c.y).label('lx')
        self._assert_result(
            select([lx]).order_by(lx),
            [(3, ), (5, ), (7, )]
        )

    def test_composed_multiple(self):
        table = self.tables.some_table
        lx = (table.c.x + table.c.y).label('lx')
        ly = (func.lower(table.c.q) + table.c.p).label('ly')
        self._assert_result(
            select([lx, ly]).order_by(lx, ly.desc()),
            [(3, util.u('q1p3')), (5, util.u('q2p2')), (7, util.u('q3p1'))]
        )

    def test_plain_desc(self):
        table = self.tables.some_table
        lx = table.c.x.label('lx')
        self._assert_result(
            select([lx]).order_by(lx.desc()),
            [(3, ), (2, ), (1, )]
        )

    def test_composed_int_desc(self):
        table = self.tables.some_table
        lx = (table.c.x + table.c.y).label('lx')
        self._assert_result(
            select([lx]).order_by(lx.desc()),
            [(7, ), (5, ), (3, )]
        )

    def test_group_by_composed(self):
        table = self.tables.some_table
        expr = (table.c.x + table.c.y).label('lx')
        stmt = select([func.count(table.c.id), expr]).group_by(expr).order_by(expr)
        self._assert_result(
            stmt,
            [(1, 3), (1, 5), (1, 7)]
        )


class LimitOffsetTest(fixtures.TablesTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table("some_table", metadata,
              Column('id', Integer, primary_key=True),
              Column('x', Integer),
              Column('y', Integer))

    @classmethod
    def insert_data(cls):
        config.db.execute(
            cls.tables.some_table.insert(),
            [
                {"id": 1, "x": 1, "y": 2},
                {"id": 2, "x": 2, "y": 3},
                {"id": 3, "x": 3, "y": 4},
                {"id": 4, "x": 4, "y": 5},
            ]
        )

    def _assert_result(self, select, result, params=()):
        eq_(
            config.db.execute(select, params).fetchall(),
            result
        )

    def test_simple_limit(self):
        table = self.tables.some_table
        self._assert_result(
            select([table]).order_by(table.c.id).limit(2),
            [(1, 1, 2), (2, 2, 3)]
        )

    def test_simple_offset(self):
        table = self.tables.some_table
        self._assert_result(
            select([table]).order_by(table.c.id).offset(2),
            [(3, 3, 4), (4, 4, 5)]
        )

    def test_simple_limit_offset(self):
        table = self.tables.some_table
        self._assert_result(
            select([table]).order_by(table.c.id).limit(2).offset(1),
            [(2, 2, 3), (3, 3, 4)]
        )

    def test_limit_offset_nobinds(self):
        """test that 'literal binds' mode works - no bound params."""

        table = self.tables.some_table
        stmt = select([table]).order_by(table.c.id).limit(2).offset(1)
        sql = stmt.compile(
            dialect=config.db.dialect,
            compile_kwargs={"literal_binds": True})
        sql = str(sql)

        self._assert_result(
            sql,
            [(2, 2, 3), (3, 3, 4)]
        )

    @testing.requires.bound_limit_offset
    def test_bound_limit(self):
        table = self.tables.some_table
        self._assert_result(
            select([table]).order_by(table.c.id).limit(bindparam('l')),
            [(1, 1, 2), (2, 2, 3)],
            params={"l": 2}
        )

    @testing.requires.bound_limit_offset
    def test_bound_offset(self):
        table = self.tables.some_table
        self._assert_result(
            select([table]).order_by(table.c.id).offset(bindparam('o')),
            [(3, 3, 4), (4, 4, 5)],
            params={"o": 2}
        )

    @testing.requires.bound_limit_offset
    def test_bound_limit_offset(self):
        table = self.tables.some_table
        self._assert_result(
            select([table]).order_by(table.c.id).
            limit(bindparam("l")).offset(bindparam("o")),
            [(2, 2, 3), (3, 3, 4)],
            params={"l": 2, "o": 1}
        )
