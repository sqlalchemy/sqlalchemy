from test.lib import fixtures
from test.lib.testing import AssertsCompiledSQL
from sqlalchemy.sql import table, column, select, func, literal
from sqlalchemy.dialects import mssql
from sqlalchemy.engine import default

class CTETest(fixtures.TestBase, AssertsCompiledSQL):

    __dialect__ = 'default'

    def test_nonrecursive(self):
        orders = table('orders', 
            column('region'),
            column('amount'),
            column('product'),
            column('quantity')
        )

        regional_sales = select([
                            orders.c.region, 
                            func.sum(orders.c.amount).label('total_sales')
                        ]).group_by(orders.c.region).cte("regional_sales")

        top_regions = select([regional_sales.c.region]).\
                where(
                    regional_sales.c.total_sales > 
                    select([
                        func.sum(regional_sales.c.total_sales)/10
                    ])
                ).cte("top_regions")

        s = select([
                    orders.c.region, 
                    orders.c.product, 
                    func.sum(orders.c.quantity).label("product_units"), 
                    func.sum(orders.c.amount).label("product_sales")
            ]).where(orders.c.region.in_(
                select([top_regions.c.region])
            )).group_by(orders.c.region, orders.c.product)

        # needs to render regional_sales first as top_regions
        # refers to it
        self.assert_compile(
            s,
            "WITH regional_sales AS (SELECT orders.region AS region, "
            "sum(orders.amount) AS total_sales FROM orders "
            "GROUP BY orders.region), "
            "top_regions AS (SELECT "
            "regional_sales.region AS region FROM regional_sales "
            "WHERE regional_sales.total_sales > "
            "(SELECT sum(regional_sales.total_sales) / :sum_1 AS "
            "anon_1 FROM regional_sales)) "
            "SELECT orders.region, orders.product, "
            "sum(orders.quantity) AS product_units, "
            "sum(orders.amount) AS product_sales "
            "FROM orders WHERE orders.region "
            "IN (SELECT top_regions.region FROM top_regions) "
            "GROUP BY orders.region, orders.product"
        )

    def test_recursive(self):
        parts = table('parts', 
            column('part'),
            column('sub_part'),
            column('quantity'),
        )

        included_parts = select([
                            parts.c.sub_part, 
                            parts.c.part, 
                            parts.c.quantity]).\
                            where(parts.c.part=='our part').\
                                cte(recursive=True)

        incl_alias = included_parts.alias()
        parts_alias = parts.alias()
        included_parts = included_parts.union(
            select([
                parts_alias.c.part, 
                parts_alias.c.sub_part, 
                parts_alias.c.quantity]).\
                where(parts_alias.c.part==incl_alias.c.sub_part)
            )

        s = select([
            included_parts.c.sub_part, 
            func.sum(included_parts.c.quantity).label('total_quantity')]).\
            select_from(included_parts.join(
                    parts,included_parts.c.part==parts.c.part)).\
            group_by(included_parts.c.sub_part)
        self.assert_compile(s, 
                "WITH RECURSIVE anon_1(sub_part, part, quantity) "
                "AS (SELECT parts.sub_part AS sub_part, parts.part "
                "AS part, parts.quantity AS quantity FROM parts "
                "WHERE parts.part = :part_1 UNION SELECT parts_1.part "
                "AS part, parts_1.sub_part AS sub_part, parts_1.quantity "
                "AS quantity FROM parts AS parts_1, anon_1 AS anon_2 "
                "WHERE parts_1.part = anon_2.sub_part) "
                "SELECT anon_1.sub_part, "
                "sum(anon_1.quantity) AS total_quantity FROM anon_1 "
                "JOIN parts ON anon_1.part = parts.part "
                "GROUP BY anon_1.sub_part"
            )

        # quick check that the "WITH RECURSIVE" varies per
        # dialect
        self.assert_compile(s, 
                "WITH anon_1(sub_part, part, quantity) "
                "AS (SELECT parts.sub_part AS sub_part, parts.part "
                "AS part, parts.quantity AS quantity FROM parts "
                "WHERE parts.part = :part_1 UNION SELECT parts_1.part "
                "AS part, parts_1.sub_part AS sub_part, parts_1.quantity "
                "AS quantity FROM parts AS parts_1, anon_1 AS anon_2 "
                "WHERE parts_1.part = anon_2.sub_part) "
                "SELECT anon_1.sub_part, "
                "sum(anon_1.quantity) AS total_quantity FROM anon_1 "
                "JOIN parts ON anon_1.part = parts.part "
                "GROUP BY anon_1.sub_part",
                dialect=mssql.dialect()
            )

    def test_union(self):
        orders = table('orders', 
            column('region'),
            column('amount'),
        )

        regional_sales = select([
                            orders.c.region,
                            orders.c.amount
                        ]).cte("regional_sales")

        s = select([regional_sales.c.region]).\
                where(
                    regional_sales.c.amount > 500
                )

        self.assert_compile(s, 
            "WITH regional_sales AS "
            "(SELECT orders.region AS region, "
            "orders.amount AS amount FROM orders) "
            "SELECT regional_sales.region "
            "FROM regional_sales WHERE "
            "regional_sales.amount > :amount_1")

        s = s.union_all(
            select([regional_sales.c.region]).\
                where(
                    regional_sales.c.amount < 300
                )
        )
        self.assert_compile(s, 
            "WITH regional_sales AS "
            "(SELECT orders.region AS region, "
            "orders.amount AS amount FROM orders) "
            "SELECT regional_sales.region FROM regional_sales "
            "WHERE regional_sales.amount > :amount_1 "
            "UNION ALL SELECT regional_sales.region "
            "FROM regional_sales WHERE "
            "regional_sales.amount < :amount_2")

    def test_reserved_quote(self):
        orders = table('orders', 
            column('order'),
        )
        s = select([orders.c.order]).cte("regional_sales", recursive=True)
        s = select([s.c.order])
        self.assert_compile(s,
            'WITH RECURSIVE regional_sales("order") AS '
            '(SELECT orders."order" AS "order" '
            "FROM orders)"
            ' SELECT regional_sales."order" '
            "FROM regional_sales"
            )

    def test_positional_binds(self):
        orders = table('orders', 
            column('order'),
        )
        s = select([orders.c.order, literal("x")]).cte("regional_sales")
        s = select([s.c.order, literal("y")])
        dialect = default.DefaultDialect()
        dialect.positional = True
        dialect.paramstyle = 'numeric'
        self.assert_compile(s,
            'WITH regional_sales AS (SELECT orders."order" '
            'AS "order", :1 AS anon_2 FROM orders) SELECT '
            'regional_sales."order", :2 AS anon_1 FROM regional_sales',
            checkpositional=('x', 'y'),
            dialect=dialect
        )

        self.assert_compile(s.union(s),
            'WITH regional_sales AS (SELECT orders."order" '
            'AS "order", :1 AS anon_2 FROM orders) SELECT '
            'regional_sales."order", :2 AS anon_1 FROM regional_sales '
            'UNION SELECT regional_sales."order", :3 AS anon_1 '
            'FROM regional_sales',
            checkpositional=('x', 'y', 'y'),
            dialect=dialect
        )

        s = select([orders.c.order]).\
            where(orders.c.order=='x').cte("regional_sales")
        s = select([s.c.order]).where(s.c.order=="y")
        self.assert_compile(s,
            'WITH regional_sales AS (SELECT orders."order" AS '
            '"order" FROM orders WHERE orders."order" = :1) '
            'SELECT regional_sales."order" FROM regional_sales '
            'WHERE regional_sales."order" = :2',
            checkpositional=('x', 'y'),
            dialect=dialect
        )
