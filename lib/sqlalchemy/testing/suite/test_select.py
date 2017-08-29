from .. import fixtures, config
from ..assertions import eq_

from sqlalchemy import util
from sqlalchemy import Integer, String, select, func, bindparam, union, tuple_
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

    @testing.requires.offset
    def test_simple_offset(self):
        table = self.tables.some_table
        self._assert_result(
            select([table]).order_by(table.c.id).offset(2),
            [(3, 3, 4), (4, 4, 5)]
        )

    @testing.requires.offset
    def test_simple_limit_offset(self):
        table = self.tables.some_table
        self._assert_result(
            select([table]).order_by(table.c.id).limit(2).offset(1),
            [(2, 2, 3), (3, 3, 4)]
        )

    @testing.requires.offset
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


class CompoundSelectTest(fixtures.TablesTest):
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

    def test_plain_union(self):
        table = self.tables.some_table
        s1 = select([table]).where(table.c.id == 2)
        s2 = select([table]).where(table.c.id == 3)

        u1 = union(s1, s2)
        self._assert_result(
            u1.order_by(u1.c.id),
            [(2, 2, 3), (3, 3, 4)]
        )

    def test_select_from_plain_union(self):
        table = self.tables.some_table
        s1 = select([table]).where(table.c.id == 2)
        s2 = select([table]).where(table.c.id == 3)

        u1 = union(s1, s2).alias().select()
        self._assert_result(
            u1.order_by(u1.c.id),
            [(2, 2, 3), (3, 3, 4)]
        )

    @testing.requires.order_by_col_from_union
    @testing.requires.parens_in_union_contained_select_w_limit_offset
    def test_limit_offset_selectable_in_unions(self):
        table = self.tables.some_table
        s1 = select([table]).where(table.c.id == 2).\
            limit(1).order_by(table.c.id)
        s2 = select([table]).where(table.c.id == 3).\
            limit(1).order_by(table.c.id)

        u1 = union(s1, s2).limit(2)
        self._assert_result(
            u1.order_by(u1.c.id),
            [(2, 2, 3), (3, 3, 4)]
        )

    @testing.requires.parens_in_union_contained_select_wo_limit_offset
    def test_order_by_selectable_in_unions(self):
        table = self.tables.some_table
        s1 = select([table]).where(table.c.id == 2).\
            order_by(table.c.id)
        s2 = select([table]).where(table.c.id == 3).\
            order_by(table.c.id)

        u1 = union(s1, s2).limit(2)
        self._assert_result(
            u1.order_by(u1.c.id),
            [(2, 2, 3), (3, 3, 4)]
        )

    def test_distinct_selectable_in_unions(self):
        table = self.tables.some_table
        s1 = select([table]).where(table.c.id == 2).\
            distinct()
        s2 = select([table]).where(table.c.id == 3).\
            distinct()

        u1 = union(s1, s2).limit(2)
        self._assert_result(
            u1.order_by(u1.c.id),
            [(2, 2, 3), (3, 3, 4)]
        )

    @testing.requires.parens_in_union_contained_select_w_limit_offset
    def test_limit_offset_in_unions_from_alias(self):
        table = self.tables.some_table
        s1 = select([table]).where(table.c.id == 2).\
            limit(1).order_by(table.c.id)
        s2 = select([table]).where(table.c.id == 3).\
            limit(1).order_by(table.c.id)

        # this necessarily has double parens
        u1 = union(s1, s2).alias()
        self._assert_result(
            u1.select().limit(2).order_by(u1.c.id),
            [(2, 2, 3), (3, 3, 4)]
        )

    def test_limit_offset_aliased_selectable_in_unions(self):
        table = self.tables.some_table
        s1 = select([table]).where(table.c.id == 2).\
            limit(1).order_by(table.c.id).alias().select()
        s2 = select([table]).where(table.c.id == 3).\
            limit(1).order_by(table.c.id).alias().select()

        u1 = union(s1, s2).limit(2)
        self._assert_result(
            u1.order_by(u1.c.id),
            [(2, 2, 3), (3, 3, 4)]
        )


class ExpandingBoundInTest(fixtures.TablesTest):
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

    def test_bound_in_scalar(self):
        table = self.tables.some_table

        stmt = select([table.c.id]).where(
            table.c.x.in_(bindparam('q', expanding=True)))

        self._assert_result(
            stmt,
            [(2, ), (3, ), (4, )],
            params={"q": [2, 3, 4]},
        )

    @testing.requires.tuple_in
    def test_bound_in_two_tuple(self):
        table = self.tables.some_table

        stmt = select([table.c.id]).where(
            tuple_(table.c.x, table.c.y).in_(bindparam('q', expanding=True)))

        self._assert_result(
            stmt,
            [(2, ), (3, ), (4, )],
            params={"q": [(2, 3), (3, 4), (4, 5)]},
        )
