from .. import fixtures, config
from ..assertions import eq_

from sqlalchemy import util
from sqlalchemy import Integer, String, select, func

from ..schema import Table, Column


class OrderByLabelTest(fixtures.TablesTest):
    """Test the dialect sends appropriate ORDER BY expressions when
    labels are used.

    This essentially exercises the "supports_simple_order_by_label"
    setting.

    """
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
