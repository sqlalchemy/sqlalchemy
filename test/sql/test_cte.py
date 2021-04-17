from sqlalchemy import testing
from sqlalchemy.dialects import mssql
from sqlalchemy.engine import default
from sqlalchemy.exc import CompileError
from sqlalchemy.sql import and_
from sqlalchemy.sql import bindparam
from sqlalchemy.sql import column
from sqlalchemy.sql import cte
from sqlalchemy.sql import exists
from sqlalchemy.sql import func
from sqlalchemy.sql import literal
from sqlalchemy.sql import select
from sqlalchemy.sql import table
from sqlalchemy.sql.elements import quoted_name
from sqlalchemy.sql.selectable import CTE
from sqlalchemy.sql.visitors import cloned_traverse
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures


class CTETest(fixtures.TestBase, AssertsCompiledSQL):

    __dialect__ = "default_enhanced"

    def test_nonrecursive(self):
        orders = table(
            "orders",
            column("region"),
            column("amount"),
            column("product"),
            column("quantity"),
        )

        regional_sales = (
            select(
                orders.c.region,
                func.sum(orders.c.amount).label("total_sales"),
            )
            .group_by(orders.c.region)
            .cte("regional_sales")
        )

        top_regions = (
            select(regional_sales.c.region)
            .where(
                regional_sales.c.total_sales
                > select(
                    func.sum(regional_sales.c.total_sales) / 10
                ).scalar_subquery()
            )
            .cte("top_regions")
        )

        s = (
            select(
                orders.c.region,
                orders.c.product,
                func.sum(orders.c.quantity).label("product_units"),
                func.sum(orders.c.amount).label("product_sales"),
            )
            .where(orders.c.region.in_(select(top_regions.c.region)))
            .group_by(orders.c.region, orders.c.product)
        )

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
            "GROUP BY orders.region, orders.product",
        )

    def test_recursive(self):
        parts = table(
            "parts", column("part"), column("sub_part"), column("quantity")
        )

        included_parts = (
            select(parts.c.sub_part, parts.c.part, parts.c.quantity)
            .where(parts.c.part == "our part")
            .cte(recursive=True)
        )

        incl_alias = included_parts.alias()
        parts_alias = parts.alias()
        included_parts = included_parts.union(
            select(
                parts_alias.c.sub_part,
                parts_alias.c.part,
                parts_alias.c.quantity,
            ).where(parts_alias.c.part == incl_alias.c.sub_part)
        )

        s = (
            select(
                included_parts.c.sub_part,
                func.sum(included_parts.c.quantity).label("total_quantity"),
            )
            .select_from(
                included_parts.join(
                    parts, included_parts.c.part == parts.c.part
                )
            )
            .group_by(included_parts.c.sub_part)
        )
        self.assert_compile(
            s,
            "WITH RECURSIVE anon_1(sub_part, part, quantity) "
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
            "GROUP BY anon_1.sub_part",
        )

        # quick check that the "WITH RECURSIVE" varies per
        # dialect
        self.assert_compile(
            s,
            "WITH anon_1(sub_part, part, quantity) "
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
            "GROUP BY anon_1.sub_part",
            dialect=mssql.dialect(),
        )

    def test_recursive_inner_cte_unioned_to_alias(self):
        parts = table(
            "parts", column("part"), column("sub_part"), column("quantity")
        )

        included_parts = (
            select(parts.c.sub_part, parts.c.part, parts.c.quantity)
            .where(parts.c.part == "our part")
            .cte(recursive=True)
        )

        incl_alias = included_parts.alias("incl")
        parts_alias = parts.alias()
        included_parts = incl_alias.union(
            select(
                parts_alias.c.sub_part,
                parts_alias.c.part,
                parts_alias.c.quantity,
            ).where(parts_alias.c.part == incl_alias.c.sub_part)
        )

        s = (
            select(
                included_parts.c.sub_part,
                func.sum(included_parts.c.quantity).label("total_quantity"),
            )
            .select_from(
                included_parts.join(
                    parts, included_parts.c.part == parts.c.part
                )
            )
            .group_by(included_parts.c.sub_part)
        )
        self.assert_compile(
            s,
            "WITH RECURSIVE incl(sub_part, part, quantity) "
            "AS (SELECT parts.sub_part AS sub_part, parts.part "
            "AS part, parts.quantity AS quantity FROM parts "
            "WHERE parts.part = :part_1 UNION "
            "SELECT parts_1.sub_part AS sub_part, "
            "parts_1.part AS part, parts_1.quantity "
            "AS quantity FROM parts AS parts_1, incl "
            "WHERE parts_1.part = incl.sub_part) "
            "SELECT incl.sub_part, "
            "sum(incl.quantity) AS total_quantity FROM incl "
            "JOIN parts ON incl.part = parts.part "
            "GROUP BY incl.sub_part",
        )

    def test_recursive_union_no_alias_one(self):
        s1 = select(literal(0).label("x"))
        cte = s1.cte(name="cte", recursive=True)
        cte = cte.union_all(select(cte.c.x + 1).where(cte.c.x < 10))
        s2 = select(cte)
        self.assert_compile(
            s2,
            "WITH RECURSIVE cte(x) AS "
            "(SELECT :param_1 AS x UNION ALL "
            "SELECT cte.x + :x_1 AS anon_1 "
            "FROM cte WHERE cte.x < :x_2) "
            "SELECT cte.x FROM cte",
        )

    def test_recursive_union_alias_one(self):
        s1 = select(literal(0).label("x"))
        cte = s1.cte(name="cte", recursive=True)
        cte = cte.union_all(select(cte.c.x + 1).where(cte.c.x < 10)).alias(
            "cr1"
        )
        s2 = select(cte)
        self.assert_compile(
            s2,
            "WITH RECURSIVE cte(x) AS "
            "(SELECT :param_1 AS x UNION ALL "
            "SELECT cte.x + :x_1 AS anon_1 "
            "FROM cte WHERE cte.x < :x_2) "
            "SELECT cr1.x FROM cte AS cr1",
        )

    def test_recursive_union_no_alias_two(self):
        """

        pg's example::

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
        t = select(func.values(1).label("n")).cte("t", recursive=True)
        t = t.union_all(select(t.c.n + 1).where(t.c.n < 100))
        s = select(func.sum(t.c.n))
        self.assert_compile(
            s,
            "WITH RECURSIVE t(n) AS "
            "(SELECT values(:values_1) AS n "
            "UNION ALL SELECT t.n + :n_1 AS anon_1 "
            "FROM t "
            "WHERE t.n < :n_2) "
            "SELECT sum(t.n) AS sum_1 FROM t",
        )

    def test_recursive_union_alias_two(self):
        """"""

        # I know, this is the PG VALUES keyword,
        # we're cheating here.  also yes we need the SELECT,
        # sorry PG.
        t = select(func.values(1).label("n")).cte("t", recursive=True)
        t = t.union_all(select(t.c.n + 1).where(t.c.n < 100)).alias("ta")
        s = select(func.sum(t.c.n))
        self.assert_compile(
            s,
            "WITH RECURSIVE t(n) AS "
            "(SELECT values(:values_1) AS n "
            "UNION ALL SELECT t.n + :n_1 AS anon_1 "
            "FROM t "
            "WHERE t.n < :n_2) "
            "SELECT sum(ta.n) AS sum_1 FROM t AS ta",
        )

    def test_recursive_union_no_alias_three(self):
        # like test one, but let's refer to the CTE
        # in a sibling CTE.

        s1 = select(literal(0).label("x"))
        cte = s1.cte(name="cte", recursive=True)

        # can't do it here...
        # bar = select(cte).cte('bar')
        cte = cte.union_all(select(cte.c.x + 1).where(cte.c.x < 10))
        bar = select(cte).cte("bar")

        s2 = select(cte, bar)
        self.assert_compile(
            s2,
            "WITH RECURSIVE cte(x) AS "
            "(SELECT :param_1 AS x UNION ALL "
            "SELECT cte.x + :x_2 AS anon_1 "
            "FROM cte WHERE cte.x < :x_3), "
            "bar AS (SELECT cte.x AS x FROM cte) "
            "SELECT cte.x, bar.x AS x_1 FROM cte, bar",
        )

    def test_recursive_union_alias_three(self):
        # like test one, but let's refer to the CTE
        # in a sibling CTE.

        s1 = select(literal(0).label("x"))
        cte = s1.cte(name="cte", recursive=True)

        # can't do it here...
        # bar = select(cte).cte('bar')
        cte = cte.union_all(select(cte.c.x + 1).where(cte.c.x < 10)).alias(
            "cs1"
        )
        bar = select(cte).cte("bar").alias("cs2")

        s2 = select(cte, bar)
        self.assert_compile(
            s2,
            "WITH RECURSIVE cte(x) AS "
            "(SELECT :param_1 AS x UNION ALL "
            "SELECT cte.x + :x_2 AS anon_1 "
            "FROM cte WHERE cte.x < :x_3), "
            "bar AS (SELECT cs1.x AS x FROM cte AS cs1) "
            "SELECT cs1.x, cs2.x AS x_1 FROM cte AS cs1, bar AS cs2",
        )

    def test_recursive_union_no_alias_four(self):
        # like test one and three, but let's refer
        # previous version of "cte".  here we test
        # how the compiler resolves multiple instances
        # of "cte".

        s1 = select(literal(0).label("x"))
        cte = s1.cte(name="cte", recursive=True)

        bar = select(cte).cte("bar")
        cte = cte.union_all(select(cte.c.x + 1).where(cte.c.x < 10))

        # outer cte rendered first, then bar, which
        # includes "inner" cte
        s2 = select(cte, bar)
        self.assert_compile(
            s2,
            "WITH RECURSIVE cte(x) AS "
            "(SELECT :param_1 AS x UNION ALL "
            "SELECT cte.x + :x_2 AS anon_1 "
            "FROM cte WHERE cte.x < :x_3), "
            "bar AS (SELECT cte.x AS x FROM cte) "
            "SELECT cte.x, bar.x AS x_1 FROM cte, bar",
        )

        # bar rendered, only includes "inner" cte,
        # "outer" cte isn't present
        s2 = select(bar)
        self.assert_compile(
            s2,
            "WITH RECURSIVE cte(x) AS "
            "(SELECT :param_1 AS x), "
            "bar AS (SELECT cte.x AS x FROM cte) "
            "SELECT bar.x FROM bar",
        )

        # bar rendered, but then the "outer"
        # cte is rendered.
        s2 = select(bar, cte)
        self.assert_compile(
            s2,
            "WITH RECURSIVE bar AS (SELECT cte.x AS x FROM cte), "
            "cte(x) AS "
            "(SELECT :param_1 AS x UNION ALL "
            "SELECT cte.x + :x_2 AS anon_1 "
            "FROM cte WHERE cte.x < :x_3) "
            "SELECT bar.x, cte.x AS x_1 FROM bar, cte",
        )

    def test_recursive_union_alias_four(self):
        # like test one and three, but let's refer
        # previous version of "cte".  here we test
        # how the compiler resolves multiple instances
        # of "cte".

        s1 = select(literal(0).label("x"))
        cte = s1.cte(name="cte", recursive=True)

        bar = select(cte).cte("bar").alias("cs1")
        cte = cte.union_all(select(cte.c.x + 1).where(cte.c.x < 10)).alias(
            "cs2"
        )

        # outer cte rendered first, then bar, which
        # includes "inner" cte
        s2 = select(cte, bar)
        self.assert_compile(
            s2,
            "WITH RECURSIVE cte(x) AS "
            "(SELECT :param_1 AS x UNION ALL "
            "SELECT cte.x + :x_2 AS anon_1 "
            "FROM cte WHERE cte.x < :x_3), "
            "bar AS (SELECT cte.x AS x FROM cte) "
            "SELECT cs2.x, cs1.x AS x_1 FROM cte AS cs2, bar AS cs1",
        )

        # bar rendered, only includes "inner" cte,
        # "outer" cte isn't present
        s2 = select(bar)
        self.assert_compile(
            s2,
            "WITH RECURSIVE cte(x) AS "
            "(SELECT :param_1 AS x), "
            "bar AS (SELECT cte.x AS x FROM cte) "
            "SELECT cs1.x FROM bar AS cs1",
        )

        # bar rendered, but then the "outer"
        # cte is rendered.
        s2 = select(bar, cte)
        self.assert_compile(
            s2,
            "WITH RECURSIVE bar AS (SELECT cte.x AS x FROM cte), "
            "cte(x) AS "
            "(SELECT :param_1 AS x UNION ALL "
            "SELECT cte.x + :x_2 AS anon_1 "
            "FROM cte WHERE cte.x < :x_3) "
            "SELECT cs1.x, cs2.x AS x_1 FROM bar AS cs1, cte AS cs2",
        )

    def test_conflicting_names(self):
        """test a flat out name conflict."""

        s1 = select(1)
        c1 = s1.cte(name="cte1", recursive=True)
        s2 = select(1)
        c2 = s2.cte(name="cte1", recursive=True)

        s = select(c1, c2)
        assert_raises_message(
            CompileError,
            "Multiple, unrelated CTEs found " "with the same name: 'cte1'",
            s.compile,
        )

    def test_union(self):
        orders = table("orders", column("region"), column("amount"))

        regional_sales = select(orders.c.region, orders.c.amount).cte(
            "regional_sales"
        )

        s = select(regional_sales.c.region).where(
            regional_sales.c.amount > 500
        )

        self.assert_compile(
            s,
            "WITH regional_sales AS "
            "(SELECT orders.region AS region, "
            "orders.amount AS amount FROM orders) "
            "SELECT regional_sales.region "
            "FROM regional_sales WHERE "
            "regional_sales.amount > :amount_1",
        )

        s = s.union_all(
            select(regional_sales.c.region).where(
                regional_sales.c.amount < 300
            )
        )
        self.assert_compile(
            s,
            "WITH regional_sales AS "
            "(SELECT orders.region AS region, "
            "orders.amount AS amount FROM orders) "
            "SELECT regional_sales.region FROM regional_sales "
            "WHERE regional_sales.amount > :amount_1 "
            "UNION ALL SELECT regional_sales.region "
            "FROM regional_sales WHERE "
            "regional_sales.amount < :amount_2",
        )

    def test_union_cte_aliases(self):
        orders = table("orders", column("region"), column("amount"))

        regional_sales = (
            select(orders.c.region, orders.c.amount)
            .cte("regional_sales")
            .alias("rs")
        )

        s = select(regional_sales.c.region).where(
            regional_sales.c.amount > 500
        )

        self.assert_compile(
            s,
            "WITH regional_sales AS "
            "(SELECT orders.region AS region, "
            "orders.amount AS amount FROM orders) "
            "SELECT rs.region "
            "FROM regional_sales AS rs WHERE "
            "rs.amount > :amount_1",
        )

        s = s.union_all(
            select(regional_sales.c.region).where(
                regional_sales.c.amount < 300
            )
        )
        self.assert_compile(
            s,
            "WITH regional_sales AS "
            "(SELECT orders.region AS region, "
            "orders.amount AS amount FROM orders) "
            "SELECT rs.region FROM regional_sales AS rs "
            "WHERE rs.amount > :amount_1 "
            "UNION ALL SELECT rs.region "
            "FROM regional_sales AS rs WHERE "
            "rs.amount < :amount_2",
        )

        cloned = cloned_traverse(s, {}, {})
        self.assert_compile(
            cloned,
            "WITH regional_sales AS "
            "(SELECT orders.region AS region, "
            "orders.amount AS amount FROM orders) "
            "SELECT rs.region FROM regional_sales AS rs "
            "WHERE rs.amount > :amount_1 "
            "UNION ALL SELECT rs.region "
            "FROM regional_sales AS rs WHERE "
            "rs.amount < :amount_2",
        )

    def test_cloned_alias(self):
        entity = table(
            "entity", column("id"), column("employer_id"), column("name")
        )
        tag = table("tag", column("tag"), column("entity_id"))

        tags = (
            select(tag.c.entity_id, func.array_agg(tag.c.tag).label("tags"))
            .group_by(tag.c.entity_id)
            .cte("unaliased_tags")
        )

        entity_tags = tags.alias(name="entity_tags")
        employer_tags = tags.alias(name="employer_tags")

        q = (
            select(entity.c.name)
            .select_from(
                entity.outerjoin(
                    entity_tags, tags.c.entity_id == entity.c.id
                ).outerjoin(
                    employer_tags, tags.c.entity_id == entity.c.employer_id
                )
            )
            .where(entity_tags.c.tags.op("@>")(bindparam("tags")))
            .where(employer_tags.c.tags.op("@>")(bindparam("tags")))
        )

        self.assert_compile(
            q,
            "WITH unaliased_tags AS "
            "(SELECT tag.entity_id AS entity_id, array_agg(tag.tag) AS tags "
            "FROM tag GROUP BY tag.entity_id)"
            " SELECT entity.name "
            "FROM entity "
            "LEFT OUTER JOIN unaliased_tags AS entity_tags ON "
            "unaliased_tags.entity_id = entity.id "
            "LEFT OUTER JOIN unaliased_tags AS employer_tags ON "
            "unaliased_tags.entity_id = entity.employer_id "
            "WHERE (entity_tags.tags @> :tags) AND "
            "(employer_tags.tags @> :tags)",
        )

        cloned = q.params(tags=["tag1", "tag2"])
        self.assert_compile(
            cloned,
            "WITH unaliased_tags AS "
            "(SELECT tag.entity_id AS entity_id, array_agg(tag.tag) AS tags "
            "FROM tag GROUP BY tag.entity_id)"
            " SELECT entity.name "
            "FROM entity "
            "LEFT OUTER JOIN unaliased_tags AS entity_tags ON "
            "unaliased_tags.entity_id = entity.id "
            "LEFT OUTER JOIN unaliased_tags AS employer_tags ON "
            "unaliased_tags.entity_id = entity.employer_id "
            "WHERE (entity_tags.tags @> :tags) AND "
            "(employer_tags.tags @> :tags)",
        )

    def test_reserved_quote(self):
        orders = table("orders", column("order"))
        s = select(orders.c.order).cte("regional_sales", recursive=True)
        s = select(s.c.order)
        self.assert_compile(
            s,
            'WITH RECURSIVE regional_sales("order") AS '
            '(SELECT orders."order" AS "order" '
            "FROM orders)"
            ' SELECT regional_sales."order" '
            "FROM regional_sales",
        )

    def test_multi_subq_quote(self):
        cte = select(literal(1).label("id")).cte(name="CTE")

        s1 = select(cte.c.id).alias()
        s2 = select(cte.c.id).alias()

        s = select(s1, s2)
        self.assert_compile(
            s,
            'WITH "CTE" AS (SELECT :param_1 AS id) '
            "SELECT anon_1.id, anon_2.id AS id_1 FROM "
            '(SELECT "CTE".id AS id FROM "CTE") AS anon_1, '
            '(SELECT "CTE".id AS id FROM "CTE") AS anon_2',
        )

    def test_multi_subq_alias(self):
        cte = select(literal(1).label("id")).cte(name="cte1").alias("aa")

        s1 = select(cte.c.id).alias()
        s2 = select(cte.c.id).alias()

        s = select(s1, s2)
        self.assert_compile(
            s,
            "WITH cte1 AS (SELECT :param_1 AS id) "
            "SELECT anon_1.id, anon_2.id AS id_1 FROM "
            "(SELECT aa.id AS id FROM cte1 AS aa) AS anon_1, "
            "(SELECT aa.id AS id FROM cte1 AS aa) AS anon_2",
        )

    def test_cte_refers_to_aliased_cte_twice(self):
        # test issue #4204
        a = table("a", column("id"))
        b = table("b", column("id"), column("fid"))
        c = table("c", column("id"), column("fid"))

        cte1 = select(a.c.id).cte(name="cte1")

        aa = cte1.alias("aa")

        cte2 = (
            select(b.c.id)
            .select_from(b.join(aa, b.c.fid == aa.c.id))
            .cte(name="cte2")
        )

        cte3 = (
            select(c.c.id)
            .select_from(c.join(aa, c.c.fid == aa.c.id))
            .cte(name="cte3")
        )

        stmt = select(cte3.c.id, cte2.c.id).select_from(
            cte2.join(cte3, cte2.c.id == cte3.c.id)
        )
        self.assert_compile(
            stmt,
            "WITH cte1 AS (SELECT a.id AS id FROM a), "
            "cte2 AS (SELECT b.id AS id FROM b "
            "JOIN cte1 AS aa ON b.fid = aa.id), "
            "cte3 AS (SELECT c.id AS id FROM c "
            "JOIN cte1 AS aa ON c.fid = aa.id) "
            "SELECT cte3.id, cte2.id AS id_1 "
            "FROM cte2 JOIN cte3 ON cte2.id = cte3.id",
        )

    def test_named_alias_no_quote(self):
        cte = select(literal(1).label("id")).cte(name="CTE")

        s1 = select(cte.c.id).alias(name="no_quotes")

        s = select(s1)
        self.assert_compile(
            s,
            'WITH "CTE" AS (SELECT :param_1 AS id) '
            "SELECT no_quotes.id FROM "
            '(SELECT "CTE".id AS id FROM "CTE") AS no_quotes',
        )

    def test_named_alias_quote(self):
        cte = select(literal(1).label("id")).cte(name="CTE")

        s1 = select(cte.c.id).alias(name="Quotes Required")

        s = select(s1)
        self.assert_compile(
            s,
            'WITH "CTE" AS (SELECT :param_1 AS id) '
            'SELECT "Quotes Required".id FROM '
            '(SELECT "CTE".id AS id FROM "CTE") AS "Quotes Required"',
        )

    def test_named_alias_disable_quote(self):
        cte = select(literal(1).label("id")).cte(
            name=quoted_name("CTE", quote=False)
        )

        s1 = select(cte.c.id).alias(name=quoted_name("DontQuote", quote=False))

        s = select(s1)
        self.assert_compile(
            s,
            "WITH CTE AS (SELECT :param_1 AS id) "
            "SELECT DontQuote.id FROM "
            "(SELECT CTE.id AS id FROM CTE) AS DontQuote",
        )

    def test_positional_binds(self):
        orders = table("orders", column("order"))
        s = select(orders.c.order, literal("x")).cte("regional_sales")
        s = select(s.c.order, literal("y"))
        dialect = default.DefaultDialect()
        dialect.positional = True
        dialect.paramstyle = "numeric"
        self.assert_compile(
            s,
            'WITH regional_sales AS (SELECT orders."order" '
            'AS "order", :1 AS anon_2 FROM orders) SELECT '
            'regional_sales."order", :2 AS anon_1 FROM regional_sales',
            checkpositional=("x", "y"),
            dialect=dialect,
        )

        self.assert_compile(
            s.union(s),
            'WITH regional_sales AS (SELECT orders."order" '
            'AS "order", :1 AS anon_2 FROM orders) SELECT '
            'regional_sales."order", :2 AS anon_1 FROM regional_sales '
            'UNION SELECT regional_sales."order", :3 AS anon_1 '
            "FROM regional_sales",
            checkpositional=("x", "y", "y"),
            dialect=dialect,
        )

        s = (
            select(orders.c.order)
            .where(orders.c.order == "x")
            .cte("regional_sales")
        )
        s = select(s.c.order).where(s.c.order == "y")
        self.assert_compile(
            s,
            'WITH regional_sales AS (SELECT orders."order" AS '
            '"order" FROM orders WHERE orders."order" = :1) '
            'SELECT regional_sales."order" FROM regional_sales '
            'WHERE regional_sales."order" = :2',
            checkpositional=("x", "y"),
            dialect=dialect,
        )

    def test_positional_binds_2(self):
        orders = table("orders", column("order"))
        s = select(orders.c.order, literal("x")).cte("regional_sales")
        s = select(s.c.order, literal("y"))
        dialect = default.DefaultDialect()
        dialect.positional = True
        dialect.paramstyle = "numeric"
        s1 = (
            select(orders.c.order)
            .where(orders.c.order == "x")
            .cte("regional_sales_1")
        )

        s1a = s1.alias()

        s2 = (
            select(
                orders.c.order == "y",
                s1a.c.order,
                orders.c.order,
                s1.c.order,
            )
            .where(orders.c.order == "z")
            .cte("regional_sales_2")
        )

        s3 = select(s2)

        self.assert_compile(
            s3,
            'WITH regional_sales_1 AS (SELECT orders."order" AS "order" '
            'FROM orders WHERE orders."order" = :1), regional_sales_2 AS '
            '(SELECT orders."order" = :2 AS anon_1, '
            'anon_2."order" AS "order", '
            'orders."order" AS order_1, '
            'regional_sales_1."order" AS order_2 FROM orders, '
            "regional_sales_1 "
            "AS anon_2, regional_sales_1 "
            'WHERE orders."order" = :3) SELECT regional_sales_2.anon_1, '
            'regional_sales_2."order", regional_sales_2.order_1, '
            "regional_sales_2.order_2 FROM regional_sales_2",
            checkpositional=("x", "y", "z"),
            dialect=dialect,
        )

    def test_positional_binds_2_asliteral(self):
        orders = table("orders", column("order"))
        s = select(orders.c.order, literal("x")).cte("regional_sales")
        s = select(s.c.order, literal("y"))
        dialect = default.DefaultDialect()
        dialect.positional = True
        dialect.paramstyle = "numeric"
        s1 = (
            select(orders.c.order)
            .where(orders.c.order == "x")
            .cte("regional_sales_1")
        )

        s1a = s1.alias()

        s2 = (
            select(
                orders.c.order == "y",
                s1a.c.order,
                orders.c.order,
                s1.c.order,
            )
            .where(orders.c.order == "z")
            .cte("regional_sales_2")
        )

        s3 = select(s2)

        self.assert_compile(
            s3,
            "WITH regional_sales_1 AS "
            '(SELECT orders."order" AS "order" '
            "FROM orders "
            "WHERE orders.\"order\" = 'x'), "
            "regional_sales_2 AS "
            "(SELECT orders.\"order\" = 'y' AS anon_1, "
            'anon_2."order" AS "order", orders."order" AS order_1, '
            'regional_sales_1."order" AS order_2 '
            "FROM orders, regional_sales_1 AS anon_2, regional_sales_1 "
            "WHERE orders.\"order\" = 'z') "
            "SELECT regional_sales_2.anon_1, "
            'regional_sales_2."order", regional_sales_2.order_1, '
            "regional_sales_2.order_2 FROM regional_sales_2",
            checkpositional=(),
            dialect=dialect,
            literal_binds=True,
        )

    def test_all_aliases(self):
        orders = table("order", column("order"))
        s = select(orders.c.order).cte("regional_sales")

        r1 = s.alias()
        r2 = s.alias()

        s2 = select(r1, r2).where(r1.c.order > r2.c.order)

        self.assert_compile(
            s2,
            'WITH regional_sales AS (SELECT "order"."order" '
            'AS "order" FROM "order") '
            'SELECT anon_1."order", anon_2."order" AS order_1 '
            "FROM regional_sales AS anon_1, "
            'regional_sales AS anon_2 WHERE anon_1."order" > anon_2."order"',
        )

        s3 = select(orders).select_from(
            orders.join(r1, r1.c.order == orders.c.order)
        )

        self.assert_compile(
            s3,
            "WITH regional_sales AS "
            '(SELECT "order"."order" AS "order" '
            'FROM "order")'
            ' SELECT "order"."order" '
            'FROM "order" JOIN regional_sales AS anon_1 '
            'ON anon_1."order" = "order"."order"',
        )

    def test_prefixes(self):
        orders = table("order", column("order"))
        s = select(orders.c.order).cte("regional_sales")
        s = s.prefix_with("NOT MATERIALIZED", dialect="postgresql")
        stmt = select(orders).where(orders.c.order > s.c.order)

        self.assert_compile(
            stmt,
            'WITH regional_sales AS (SELECT "order"."order" AS "order" '
            'FROM "order") SELECT "order"."order" FROM "order", '
            'regional_sales WHERE "order"."order" > regional_sales."order"',
        )

        self.assert_compile(
            stmt,
            "WITH regional_sales AS NOT MATERIALIZED "
            '(SELECT "order"."order" AS "order" '
            'FROM "order") SELECT "order"."order" FROM "order", '
            'regional_sales WHERE "order"."order" > regional_sales."order"',
            dialect="postgresql",
        )

    def test_suffixes(self):
        orders = table("order", column("order"))
        s = select(orders.c.order).cte("regional_sales")
        s = s.suffix_with("pg suffix", dialect="postgresql")
        s = s.suffix_with("oracle suffix", dialect="oracle")
        stmt = select(orders).where(orders.c.order > s.c.order)

        self.assert_compile(
            stmt,
            'WITH regional_sales AS (SELECT "order"."order" AS "order" '
            'FROM "order")  SELECT "order"."order" FROM "order", '
            'regional_sales WHERE "order"."order" > regional_sales."order"',
        )

        self.assert_compile(
            stmt,
            'WITH regional_sales AS (SELECT "order"."order" AS "order" '
            'FROM "order") oracle suffix  '
            'SELECT "order"."order" FROM "order", '
            'regional_sales WHERE "order"."order" > regional_sales."order"',
            dialect="oracle",
        )

        self.assert_compile(
            stmt,
            'WITH regional_sales AS (SELECT "order"."order" AS "order" '
            'FROM "order") pg suffix  SELECT "order"."order" FROM "order", '
            'regional_sales WHERE "order"."order" > regional_sales."order"',
            dialect="postgresql",
        )

    def test_upsert_from_select(self):
        orders = table(
            "orders",
            column("region"),
            column("amount"),
            column("product"),
            column("quantity"),
        )

        upsert = (
            orders.update()
            .where(orders.c.region == "Region1")
            .values(amount=1.0, product="Product1", quantity=1)
            .returning(*(orders.c._all_columns))
            .cte("upsert")
        )

        insert = orders.insert().from_select(
            orders.c.keys(),
            select(
                literal("Region1"),
                literal(1.0),
                literal("Product1"),
                literal(1),
            ).where(~exists(upsert.select())),
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
            "upsert.quantity FROM upsert))",
        )

        eq_(insert.compile().isinsert, True)

    @testing.combinations(
        ("default_enhanced",),
        ("postgresql",),
    )
    def test_select_from_update_cte(self, dialect):
        t1 = table("table_1", column("id"), column("val"))

        t2 = table("table_2", column("id"), column("val"))

        upd = (
            t1.update()
            .values(val=t2.c.val)
            .where(t1.c.id == t2.c.id)
            .returning(t1.c.id, t1.c.val)
        )

        cte = upd.cte("update_cte")

        qry = select(cte)

        self.assert_compile(
            qry,
            "WITH update_cte AS (UPDATE table_1 SET val=table_2.val "
            "FROM table_2 WHERE table_1.id = table_2.id "
            "RETURNING table_1.id, table_1.val) "
            "SELECT update_cte.id, update_cte.val FROM update_cte",
            dialect=dialect,
        )

    @testing.combinations(
        ("default_enhanced",),
        ("postgresql",),
    )
    def test_select_from_delete_cte(self, dialect):
        t1 = table("table_1", column("id"), column("val"))

        t2 = table("table_2", column("id"), column("val"))

        dlt = (
            t1.delete().where(t1.c.id == t2.c.id).returning(t1.c.id, t1.c.val)
        )

        cte = dlt.cte("delete_cte")

        qry = select(cte)

        if dialect == "postgresql":
            self.assert_compile(
                qry,
                "WITH delete_cte AS (DELETE FROM table_1 USING table_2 "
                "WHERE table_1.id = table_2.id RETURNING table_1.id, "
                "table_1.val) SELECT delete_cte.id, delete_cte.val "
                "FROM delete_cte",
                dialect=dialect,
            )
        else:
            self.assert_compile(
                qry,
                "WITH delete_cte AS (DELETE FROM table_1 , table_2 "
                "WHERE table_1.id = table_2.id "
                "RETURNING table_1.id, table_1.val) "
                "SELECT delete_cte.id, delete_cte.val FROM delete_cte",
                dialect=dialect,
            )

    def test_anon_update_cte(self):
        orders = table("orders", column("region"))
        stmt = (
            orders.update()
            .where(orders.c.region == "x")
            .values(region="y")
            .returning(orders.c.region)
            .cte()
        )

        self.assert_compile(
            stmt.select(),
            "WITH anon_1 AS (UPDATE orders SET region=:region "
            "WHERE orders.region = :region_1 RETURNING orders.region) "
            "SELECT anon_1.region FROM anon_1",
        )

        eq_(stmt.select().compile().isupdate, False)

    def test_anon_insert_cte(self):
        orders = table("orders", column("region"))
        stmt = (
            orders.insert().values(region="y").returning(orders.c.region).cte()
        )

        self.assert_compile(
            stmt.select(),
            "WITH anon_1 AS (INSERT INTO orders (region) "
            "VALUES (:region) RETURNING orders.region) "
            "SELECT anon_1.region FROM anon_1",
        )
        eq_(stmt.select().compile().isinsert, False)

    def test_pg_example_one(self):
        products = table("products", column("id"), column("date"))
        products_log = table("products_log", column("id"), column("date"))

        moved_rows = (
            products.delete()
            .where(
                and_(products.c.date >= "dateone", products.c.date < "datetwo")
            )
            .returning(*products.c)
            .cte("moved_rows")
        )

        stmt = products_log.insert().from_select(
            products_log.c, moved_rows.select()
        )
        self.assert_compile(
            stmt,
            "WITH moved_rows AS "
            "(DELETE FROM products WHERE products.date >= :date_1 "
            "AND products.date < :date_2 "
            "RETURNING products.id, products.date) "
            "INSERT INTO products_log (id, date) "
            "SELECT moved_rows.id, moved_rows.date FROM moved_rows",
        )
        eq_(stmt.compile().isinsert, True)
        eq_(stmt.compile().isdelete, False)

    def test_pg_example_one_select_only(self):
        products = table("products", column("id"), column("date"))

        moved_rows = (
            products.delete()
            .where(
                and_(products.c.date >= "dateone", products.c.date < "datetwo")
            )
            .returning(*products.c)
            .cte("moved_rows")
        )

        stmt = moved_rows.select()

        self.assert_compile(
            stmt,
            "WITH moved_rows AS "
            "(DELETE FROM products WHERE products.date >= :date_1 "
            "AND products.date < :date_2 "
            "RETURNING products.id, products.date) "
            "SELECT moved_rows.id, moved_rows.date FROM moved_rows",
        )

        eq_(stmt.compile().isdelete, False)

    def test_pg_example_two(self):
        products = table("products", column("id"), column("price"))

        t = (
            products.update()
            .values(price="someprice")
            .returning(*products.c)
            .cte("t")
        )
        stmt = t.select()
        assert "autocommit" not in stmt._execution_options
        eq_(stmt.compile().execution_options["autocommit"], True)

        self.assert_compile(
            stmt,
            "WITH t AS "
            "(UPDATE products SET price=:price "
            "RETURNING products.id, products.price) "
            "SELECT t.id, t.price "
            "FROM t",
        )
        eq_(stmt.compile().isupdate, False)

    def test_pg_example_three(self):

        parts = table("parts", column("part"), column("sub_part"))

        included_parts = (
            select(parts.c.sub_part, parts.c.part)
            .where(parts.c.part == "our part")
            .cte("included_parts", recursive=True)
        )

        pr = included_parts.alias("pr")
        p = parts.alias("p")
        included_parts = included_parts.union_all(
            select(p.c.sub_part, p.c.part).where(p.c.part == pr.c.sub_part)
        )
        stmt = (
            parts.delete()
            .where(parts.c.part.in_(select(included_parts.c.part)))
            .returning(parts.c.part)
        )

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
            "RETURNING parts.part",
        )

    def test_insert_in_the_cte(self):
        products = table("products", column("id"), column("price"))

        cte = (
            products.insert()
            .values(id=1, price=27.0)
            .returning(*products.c)
            .cte("pd")
        )

        stmt = select(cte)

        assert "autocommit" not in stmt._execution_options
        eq_(stmt.compile().execution_options["autocommit"], True)

        self.assert_compile(
            stmt,
            "WITH pd AS "
            "(INSERT INTO products (id, price) VALUES (:id, :price) "
            "RETURNING products.id, products.price) "
            "SELECT pd.id, pd.price "
            "FROM pd",
        )
        eq_(stmt.compile().isinsert, False)

    def test_update_pulls_from_cte(self):
        products = table("products", column("id"), column("price"))

        cte = products.select().cte("pd")
        assert "autocommit" not in cte.select()._execution_options

        stmt = products.update().where(products.c.price == cte.c.price)
        eq_(stmt.compile().execution_options["autocommit"], True)

        self.assert_compile(
            stmt,
            "WITH pd AS "
            "(SELECT products.id AS id, products.price AS price "
            "FROM products) "
            "UPDATE products SET id=:id, price=:price FROM pd "
            "WHERE products.price = pd.price",
        )
        eq_(stmt.compile().isupdate, True)

    def test_standalone_function(self):
        a = table("a", column("x"))
        a_stmt = select(a)

        stmt = select(cte(a_stmt))

        self.assert_compile(
            stmt,
            "WITH anon_1 AS (SELECT a.x AS x FROM a) "
            "SELECT anon_1.x FROM anon_1",
        )

    def test_no_alias_construct(self):
        a = table("a", column("x"))
        a_stmt = select(a)

        assert_raises_message(
            NotImplementedError,
            "The CTE class is not intended to be constructed directly.  "
            r"Please use the cte\(\) standalone function",
            CTE,
            a_stmt,
            "foo",
        )
