from sqlalchemy.testing import fixtures, eq_
from sqlalchemy.testing import AssertsCompiledSQL, assert_raises_message
from sqlalchemy.sql import table, column, select, func, literal, exists, and_
from sqlalchemy.dialects import mssql
from sqlalchemy.engine import default
from sqlalchemy.exc import CompileError


class CTETest(fixtures.TestBase, AssertsCompiledSQL):

    __dialect__ = 'default_enhanced'

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
                regional_sales.c.total_sales > select([
                    func.sum(regional_sales.c.total_sales) / 10
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
            where(parts.c.part == 'our part').\
            cte(recursive=True)

        incl_alias = included_parts.alias()
        parts_alias = parts.alias()
        included_parts = included_parts.union(
            select([
                parts_alias.c.sub_part,
                parts_alias.c.part,
                parts_alias.c.quantity]).
            where(parts_alias.c.part == incl_alias.c.sub_part)
        )

        s = select([
            included_parts.c.sub_part,
            func.sum(included_parts.c.quantity).label('total_quantity')]).\
            select_from(included_parts.join(
                parts, included_parts.c.part == parts.c.part)).\
            group_by(included_parts.c.sub_part)
        self.assert_compile(
            s, "WITH RECURSIVE anon_1(sub_part, part, quantity) "
            "AS (SELECT parts.sub_part AS sub_part, parts.part "
            "AS part, parts.quantity AS quantity FROM parts "
            "WHERE parts.part = :part_1 UNION "
            "SELECT parts_1.sub_part AS sub_part, "
            "parts_1.part AS part, parts_1.quantity "
            "AS quantity FROM parts AS parts_1, anon_1 AS anon_2 "
            "WHERE parts_1.part = anon_2.sub_part) "
            "SELECT anon_1.sub_part, "
            "sum(anon_1.quantity) AS total_quantity FROM anon_1 "
            "JOIN parts ON anon_1.part = parts.part "
            "GROUP BY anon_1.sub_part")

        # quick check that the "WITH RECURSIVE" varies per
        # dialect
        self.assert_compile(
            s, "WITH anon_1(sub_part, part, quantity) "
            "AS (SELECT parts.sub_part AS sub_part, parts.part "
            "AS part, parts.quantity AS quantity FROM parts "
            "WHERE parts.part = :part_1 UNION "
            "SELECT parts_1.sub_part AS sub_part, "
            "parts_1.part AS part, parts_1.quantity "
            "AS quantity FROM parts AS parts_1, anon_1 AS anon_2 "
            "WHERE parts_1.part = anon_2.sub_part) "
            "SELECT anon_1.sub_part, "
            "sum(anon_1.quantity) AS total_quantity FROM anon_1 "
            "JOIN parts ON anon_1.part = parts.part "
            "GROUP BY anon_1.sub_part", dialect=mssql.dialect())

    def test_recursive_union_no_alias_one(self):
        s1 = select([literal(0).label("x")])
        cte = s1.cte(name="cte", recursive=True)
        cte = cte.union_all(
            select([cte.c.x + 1]).where(cte.c.x < 10)
        )
        s2 = select([cte])
        self.assert_compile(s2,
                            "WITH RECURSIVE cte(x) AS "
                            "(SELECT :param_1 AS x UNION ALL "
                            "SELECT cte.x + :x_1 AS anon_1 "
                            "FROM cte WHERE cte.x < :x_2) "
                            "SELECT cte.x FROM cte"
                            )

    def test_recursive_union_no_alias_two(self):
        """

        pg's example:

            WITH RECURSIVE t(n) AS (
                VALUES (1)
              UNION ALL
                SELECT n+1 FROM t WHERE n < 100
            )
            SELECT sum(n) FROM t;

        """

        # I know, this is the PG VALUES keyword,
        # we're cheating here.  also yes we need the SELECT,
        # sorry PG.
        t = select([func.values(1).label("n")]).cte("t", recursive=True)
        t = t.union_all(select([t.c.n + 1]).where(t.c.n < 100))
        s = select([func.sum(t.c.n)])
        self.assert_compile(s,
                            "WITH RECURSIVE t(n) AS "
                            "(SELECT values(:values_1) AS n "
                            "UNION ALL SELECT t.n + :n_1 AS anon_1 "
                            "FROM t "
                            "WHERE t.n < :n_2) "
                            "SELECT sum(t.n) AS sum_1 FROM t"
                            )

    def test_recursive_union_no_alias_three(self):
        # like test one, but let's refer to the CTE
        # in a sibling CTE.

        s1 = select([literal(0).label("x")])
        cte = s1.cte(name="cte", recursive=True)

        # can't do it here...
        # bar = select([cte]).cte('bar')
        cte = cte.union_all(
            select([cte.c.x + 1]).where(cte.c.x < 10)
        )
        bar = select([cte]).cte('bar')

        s2 = select([cte, bar])
        self.assert_compile(s2,
                            "WITH RECURSIVE cte(x) AS "
                            "(SELECT :param_1 AS x UNION ALL "
                            "SELECT cte.x + :x_1 AS anon_1 "
                            "FROM cte WHERE cte.x < :x_2), "
                            "bar AS (SELECT cte.x AS x FROM cte) "
                            "SELECT cte.x, bar.x FROM cte, bar"
                            )

    def test_recursive_union_no_alias_four(self):
        # like test one and three, but let's refer
        # previous version of "cte".  here we test
        # how the compiler resolves multiple instances
        # of "cte".

        s1 = select([literal(0).label("x")])
        cte = s1.cte(name="cte", recursive=True)

        bar = select([cte]).cte('bar')
        cte = cte.union_all(
            select([cte.c.x + 1]).where(cte.c.x < 10)
        )

        # outer cte rendered first, then bar, which
        # includes "inner" cte
        s2 = select([cte, bar])
        self.assert_compile(s2,
                            "WITH RECURSIVE cte(x) AS "
                            "(SELECT :param_1 AS x UNION ALL "
                            "SELECT cte.x + :x_1 AS anon_1 "
                            "FROM cte WHERE cte.x < :x_2), "
                            "bar AS (SELECT cte.x AS x FROM cte) "
                            "SELECT cte.x, bar.x FROM cte, bar"
                            )

        # bar rendered, only includes "inner" cte,
        # "outer" cte isn't present
        s2 = select([bar])
        self.assert_compile(s2,
                            "WITH RECURSIVE cte(x) AS "
                            "(SELECT :param_1 AS x), "
                            "bar AS (SELECT cte.x AS x FROM cte) "
                            "SELECT bar.x FROM bar"
                            )

        # bar rendered, but then the "outer"
        # cte is rendered.
        s2 = select([bar, cte])
        self.assert_compile(
            s2, "WITH RECURSIVE bar AS (SELECT cte.x AS x FROM cte), "
            "cte(x) AS "
            "(SELECT :param_1 AS x UNION ALL "
            "SELECT cte.x + :x_1 AS anon_1 "
            "FROM cte WHERE cte.x < :x_2) "
            "SELECT bar.x, cte.x FROM bar, cte")

    def test_conflicting_names(self):
        """test a flat out name conflict."""

        s1 = select([1])
        c1 = s1.cte(name='cte1', recursive=True)
        s2 = select([1])
        c2 = s2.cte(name='cte1', recursive=True)

        s = select([c1, c2])
        assert_raises_message(
            CompileError,
            "Multiple, unrelated CTEs found "
            "with the same name: 'cte1'",
            s.compile
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

        s = select(
            [regional_sales.c.region]).where(
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
            select([regional_sales.c.region]).
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

    def test_multi_subq_quote(self):
        cte = select([literal(1).label("id")]).cte(name='CTE')

        s1 = select([cte.c.id]).alias()
        s2 = select([cte.c.id]).alias()

        s = select([s1, s2])
        self.assert_compile(
            s,
            'WITH "CTE" AS (SELECT :param_1 AS id) '
            'SELECT anon_1.id, anon_2.id FROM '
            '(SELECT "CTE".id AS id FROM "CTE") AS anon_1, '
            '(SELECT "CTE".id AS id FROM "CTE") AS anon_2'
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
        self.assert_compile(
            s,
            'WITH regional_sales AS (SELECT orders."order" '
            'AS "order", :1 AS anon_2 FROM orders) SELECT '
            'regional_sales."order", :2 AS anon_1 FROM regional_sales',
            checkpositional=(
                'x',
                'y'),
            dialect=dialect)

        self.assert_compile(
            s.union(s), 'WITH regional_sales AS (SELECT orders."order" '
            'AS "order", :1 AS anon_2 FROM orders) SELECT '
            'regional_sales."order", :2 AS anon_1 FROM regional_sales '
            'UNION SELECT regional_sales."order", :3 AS anon_1 '
            'FROM regional_sales', checkpositional=(
                'x', 'y', 'y'), dialect=dialect)

        s = select([orders.c.order]).\
            where(orders.c.order == 'x').cte("regional_sales")
        s = select([s.c.order]).where(s.c.order == "y")
        self.assert_compile(
            s, 'WITH regional_sales AS (SELECT orders."order" AS '
            '"order" FROM orders WHERE orders."order" = :1) '
            'SELECT regional_sales."order" FROM regional_sales '
            'WHERE regional_sales."order" = :2', checkpositional=(
                'x', 'y'), dialect=dialect)

    def test_positional_binds_2(self):
        orders = table('orders',
                       column('order'),
                       )
        s = select([orders.c.order, literal("x")]).cte("regional_sales")
        s = select([s.c.order, literal("y")])
        dialect = default.DefaultDialect()
        dialect.positional = True
        dialect.paramstyle = 'numeric'
        s1 = select([orders.c.order]).where(orders.c.order == 'x').\
            cte("regional_sales_1")

        s1a = s1.alias()

        s2 = select([orders.c.order == 'y', s1a.c.order,
                     orders.c.order, s1.c.order]).\
            where(orders.c.order == 'z').\
            cte("regional_sales_2")

        s3 = select([s2])

        self.assert_compile(
            s3,
            'WITH regional_sales_1 AS (SELECT orders."order" AS "order" '
            'FROM orders WHERE orders."order" = :1), regional_sales_2 AS '
            '(SELECT orders."order" = :2 AS anon_1, '
            'anon_2."order" AS "order", '
            'orders."order" AS "order", '
            'regional_sales_1."order" AS "order" FROM orders, '
            'regional_sales_1 '
            'AS anon_2, regional_sales_1 '
            'WHERE orders."order" = :3) SELECT regional_sales_2.anon_1, '
            'regional_sales_2."order" FROM regional_sales_2',
            checkpositional=('x', 'y', 'z'), dialect=dialect)

    def test_positional_binds_2_asliteral(self):
        orders = table('orders',
                       column('order'),
                       )
        s = select([orders.c.order, literal("x")]).cte("regional_sales")
        s = select([s.c.order, literal("y")])
        dialect = default.DefaultDialect()
        dialect.positional = True
        dialect.paramstyle = 'numeric'
        s1 = select([orders.c.order]).where(orders.c.order == 'x').\
            cte("regional_sales_1")

        s1a = s1.alias()

        s2 = select([orders.c.order == 'y', s1a.c.order,
                     orders.c.order, s1.c.order]).\
            where(orders.c.order == 'z').\
            cte("regional_sales_2")

        s3 = select([s2])

        self.assert_compile(
            s3,
            'WITH regional_sales_1 AS '
            '(SELECT orders."order" AS "order" '
            'FROM orders '
            'WHERE orders."order" = \'x\'), '
            'regional_sales_2 AS '
            '(SELECT orders."order" = \'y\' AS anon_1, '
            'anon_2."order" AS "order", orders."order" AS "order", '
            'regional_sales_1."order" AS "order" '
            'FROM orders, regional_sales_1 AS anon_2, regional_sales_1 '
            'WHERE orders."order" = \'z\') '
            'SELECT regional_sales_2.anon_1, regional_sales_2."order" '
            'FROM regional_sales_2',
            checkpositional=(), dialect=dialect,
            literal_binds=True)

    def test_all_aliases(self):
        orders = table('order', column('order'))
        s = select([orders.c.order]).cte("regional_sales")

        r1 = s.alias()
        r2 = s.alias()

        s2 = select([r1, r2]).where(r1.c.order > r2.c.order)

        self.assert_compile(
            s2,
            'WITH regional_sales AS (SELECT "order"."order" '
            'AS "order" FROM "order") '
            'SELECT anon_1."order", anon_2."order" '
            'FROM regional_sales AS anon_1, '
            'regional_sales AS anon_2 WHERE anon_1."order" > anon_2."order"'
        )

        s3 = select(
            [orders]).select_from(
            orders.join(
                r1,
                r1.c.order == orders.c.order))

        self.assert_compile(
            s3,
            'WITH regional_sales AS '
            '(SELECT "order"."order" AS "order" '
            'FROM "order")'
            ' SELECT "order"."order" '
            'FROM "order" JOIN regional_sales AS anon_1 '
            'ON anon_1."order" = "order"."order"'
        )

    def test_suffixes(self):
        orders = table('order', column('order'))
        s = select([orders.c.order]).cte("regional_sales")
        s = s.suffix_with("pg suffix", dialect='postgresql')
        s = s.suffix_with('oracle suffix', dialect='oracle')
        stmt = select([orders]).where(orders.c.order > s.c.order)

        self.assert_compile(
            stmt,
            'WITH regional_sales AS (SELECT "order"."order" AS "order" '
            'FROM "order")  SELECT "order"."order" FROM "order", '
            'regional_sales WHERE "order"."order" > regional_sales."order"'
        )

        self.assert_compile(
            stmt,
            'WITH regional_sales AS (SELECT "order"."order" AS "order" '
            'FROM "order") oracle suffix  '
            'SELECT "order"."order" FROM "order", '
            'regional_sales WHERE "order"."order" > regional_sales."order"',
            dialect='oracle'
        )

        self.assert_compile(
            stmt,
            'WITH regional_sales AS (SELECT "order"."order" AS "order" '
            'FROM "order") pg suffix  SELECT "order"."order" FROM "order", '
            'regional_sales WHERE "order"."order" > regional_sales."order"',
            dialect='postgresql'
        )

    def test_upsert_from_select(self):
        orders = table(
            'orders',
            column('region'),
            column('amount'),
            column('product'),
            column('quantity')
        )

        upsert = (
            orders.update()
            .where(orders.c.region == 'Region1')
            .values(amount=1.0, product='Product1', quantity=1)
            .returning(*(orders.c._all_columns)).cte('upsert'))

        insert = orders.insert().from_select(
            orders.c.keys(),
            select([
                literal('Region1'), literal(1.0),
                literal('Product1'), literal(1)
            ]).where(~exists(upsert.select()))
        )

        self.assert_compile(
            insert,
            "WITH upsert AS (UPDATE orders SET amount=:amount, "
            "product=:product, quantity=:quantity "
            "WHERE orders.region = :region_1 "
            "RETURNING orders.region, orders.amount, "
            "orders.product, orders.quantity) "
            "INSERT INTO orders (region, amount, product, quantity) "
            "SELECT :param_1 AS anon_1, :param_2 AS anon_2, "
            ":param_3 AS anon_3, :param_4 AS anon_4 WHERE NOT (EXISTS "
            "(SELECT upsert.region, upsert.amount, upsert.product, "
            "upsert.quantity FROM upsert))"
        )

    def test_anon_update_cte(self):
        orders = table(
            'orders',
            column('region')
        )
        stmt = orders.update().where(orders.c.region == 'x').\
            values(region='y').\
            returning(orders.c.region).cte()

        self.assert_compile(
            stmt.select(),
            "WITH anon_1 AS (UPDATE orders SET region=:region "
            "WHERE orders.region = :region_1 RETURNING orders.region) "
            "SELECT anon_1.region FROM anon_1"
        )

    def test_anon_insert_cte(self):
        orders = table(
            'orders',
            column('region')
        )
        stmt = orders.insert().\
            values(region='y').\
            returning(orders.c.region).cte()

        self.assert_compile(
            stmt.select(),
            "WITH anon_1 AS (INSERT INTO orders (region) "
            "VALUES (:region) RETURNING orders.region) "
            "SELECT anon_1.region FROM anon_1"
        )

    def test_pg_example_one(self):
        products = table('products', column('id'), column('date'))
        products_log = table('products_log', column('id'), column('date'))

        moved_rows = products.delete().where(and_(
            products.c.date >= 'dateone',
            products.c.date < 'datetwo')).returning(*products.c).\
            cte('moved_rows')

        stmt = products_log.insert().from_select(
            products_log.c, moved_rows.select())
        self.assert_compile(
            stmt,
            "WITH moved_rows AS "
            "(DELETE FROM products WHERE products.date >= :date_1 "
            "AND products.date < :date_2 "
            "RETURNING products.id, products.date) "
            "INSERT INTO products_log (id, date) "
            "SELECT moved_rows.id, moved_rows.date FROM moved_rows"
        )

    def test_pg_example_two(self):
        products = table('products', column('id'), column('price'))

        t = products.update().values(price='someprice').\
            returning(*products.c).cte('t')
        stmt = t.select()
        assert 'autocommit' not in stmt._execution_options
        eq_(stmt.compile().execution_options['autocommit'], True)

        self.assert_compile(
            stmt,
            "WITH t AS "
            "(UPDATE products SET price=:price "
            "RETURNING products.id, products.price) "
            "SELECT t.id, t.price "
            "FROM t"
        )

    def test_pg_example_three(self):

        parts = table(
            'parts',
            column('part'),
            column('sub_part'),
        )

        included_parts = select([
            parts.c.sub_part,
            parts.c.part]).\
            where(parts.c.part == 'our part').\
            cte("included_parts", recursive=True)

        pr = included_parts.alias('pr')
        p = parts.alias('p')
        included_parts = included_parts.union_all(
            select([
                p.c.sub_part,
                p.c.part]).
            where(p.c.part == pr.c.sub_part)
        )
        stmt = parts.delete().where(
            parts.c.part.in_(select([included_parts.c.part]))).returning(
            parts.c.part)

        # the outer RETURNING is a bonus over what PG's docs have
        self.assert_compile(
            stmt,
            "WITH RECURSIVE included_parts(sub_part, part) AS "
            "(SELECT parts.sub_part AS sub_part, parts.part AS part "
            "FROM parts "
            "WHERE parts.part = :part_1 "
            "UNION ALL SELECT p.sub_part AS sub_part, p.part AS part "
            "FROM parts AS p, included_parts AS pr "
            "WHERE p.part = pr.sub_part) "
            "DELETE FROM parts WHERE parts.part IN "
            "(SELECT included_parts.part FROM included_parts) "
            "RETURNING parts.part"
        )

    def test_insert_in_the_cte(self):
        products = table('products', column('id'), column('price'))

        cte = products.insert().values(id=1, price=27.0).\
            returning(*products.c).cte('pd')

        stmt = select([cte])

        assert 'autocommit' not in stmt._execution_options
        eq_(stmt.compile().execution_options['autocommit'], True)

        self.assert_compile(
            stmt,
            "WITH pd AS "
            "(INSERT INTO products (id, price) VALUES (:id, :price) "
            "RETURNING products.id, products.price) "
            "SELECT pd.id, pd.price "
            "FROM pd"
        )

    def test_update_pulls_from_cte(self):
        products = table('products', column('id'), column('price'))

        cte = products.select().cte('pd')
        assert 'autocommit' not in cte._execution_options

        stmt = products.update().where(products.c.price == cte.c.price)
        eq_(stmt.compile().execution_options['autocommit'], True)

        self.assert_compile(
            stmt,
            "WITH pd AS "
            "(SELECT products.id AS id, products.price AS price "
            "FROM products) "
            "UPDATE products SET id=:id, price=:price FROM pd "
            "WHERE products.price = pd.price"
        )
