from sqlalchemy import Column
from sqlalchemy import delete
from sqlalchemy import Integer
from sqlalchemy import LABEL_STYLE_TABLENAME_PLUS_COL
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import testing
from sqlalchemy import text
from sqlalchemy import update
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

    def test_recursive_w_anon_labels(self):
        parts = table(
            "parts", column("part"), column("sub_part"), column("quantity")
        )

        included_parts = (
            select(
                parts.c.sub_part.label(None),
                parts.c.part.label(None),
                parts.c.quantity,
            )
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
            ).where(parts_alias.c.part == incl_alias.c[0])
        )

        s = (
            select(
                included_parts.c[0],
                func.sum(included_parts.c.quantity).label("total_quantity"),
            )
            .select_from(
                included_parts.join(parts, included_parts.c[1] == parts.c.part)
            )
            .group_by(included_parts.c[0])
        )
        self.assert_compile(
            s,
            "WITH RECURSIVE anon_1(sub_part_1, part_1, quantity) "
            "AS (SELECT parts.sub_part AS sub_part_1, parts.part "
            "AS part_1, parts.quantity AS quantity FROM parts "
            "WHERE parts.part = :part_2 UNION "
            "SELECT parts_1.sub_part AS sub_part, "
            "parts_1.part AS part, parts_1.quantity "
            "AS quantity FROM parts AS parts_1, anon_1 AS anon_2 "
            "WHERE parts_1.part = anon_2.sub_part_1) "
            "SELECT anon_1.sub_part_1, "
            "sum(anon_1.quantity) AS total_quantity FROM anon_1 "
            "JOIN parts ON anon_1.part_1 = parts.part "
            "GROUP BY anon_1.sub_part_1",
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

    def test_with_recursive_no_name_currently_buggy(self):
        s1 = select(1)
        c1 = s1.cte(name="cte1", recursive=True)

        # this is nonsensical at the moment
        self.assert_compile(
            select(c1),
            'WITH RECURSIVE cte1("1") AS (SELECT 1) SELECT cte1.1 FROM cte1',
        )

        # however, so is subquery, which is worse as it isn't even trying
        # to quote "1" as a label
        self.assert_compile(
            select(s1.subquery()), "SELECT anon_1.1 FROM (SELECT 1) AS anon_1"
        )

    def test_wrecur_dupe_col_names(self):
        """test #6710"""

        manager = table("manager", column("id"))
        employee = table("employee", column("id"), column("manager_id"))

        top_q = select(employee, manager).join_from(
            employee, manager, employee.c.manager_id == manager.c.id
        )

        top_q = top_q.cte("cte", recursive=True)

        bottom_q = (
            select(employee, manager)
            .join_from(
                employee, manager, employee.c.manager_id == manager.c.id
            )
            .join(top_q, top_q.c.id == employee.c.id)
        )

        rec_cte = select(top_q.union_all(bottom_q))
        self.assert_compile(
            rec_cte,
            "WITH RECURSIVE cte(id, manager_id, id_1) AS "
            "(SELECT employee.id AS id, employee.manager_id AS manager_id, "
            "manager.id AS id_1 FROM employee JOIN manager "
            "ON employee.manager_id = manager.id UNION ALL "
            "SELECT employee.id AS id, employee.manager_id AS manager_id, "
            "manager.id AS id_1 FROM employee JOIN manager ON "
            "employee.manager_id = manager.id "
            "JOIN cte ON cte.id = employee.id) "
            "SELECT cte.id, cte.manager_id, cte.id_1 FROM cte",
        )

    def test_wrecur_dupe_col_names_w_grouping(self):
        """test #6710

        by adding order_by() to the top query, the CTE will have
        a compound select with the first element a SelectStatementGrouping
        object, which we can test has the correct methods for the compiler
        to call upon.

        """

        manager = table("manager", column("id"))
        employee = table("employee", column("id"), column("manager_id"))

        top_q = (
            select(employee, manager)
            .join_from(
                employee, manager, employee.c.manager_id == manager.c.id
            )
            .order_by(employee.c.id)
            .cte("cte", recursive=True)
        )

        bottom_q = (
            select(employee, manager)
            .join_from(
                employee, manager, employee.c.manager_id == manager.c.id
            )
            .join(top_q, top_q.c.id == employee.c.id)
        )

        rec_cte = select(top_q.union_all(bottom_q))

        self.assert_compile(
            rec_cte,
            "WITH RECURSIVE cte(id, manager_id, id_1) AS "
            "((SELECT employee.id AS id, employee.manager_id AS manager_id, "
            "manager.id AS id_1 FROM employee JOIN manager "
            "ON employee.manager_id = manager.id ORDER BY employee.id) "
            "UNION ALL "
            "SELECT employee.id AS id, employee.manager_id AS manager_id, "
            "manager.id AS id_1 FROM employee JOIN manager ON "
            "employee.manager_id = manager.id "
            "JOIN cte ON cte.id = employee.id) "
            "SELECT cte.id, cte.manager_id, cte.id_1 FROM cte",
        )

    def test_wrecur_ovlp_lbls_plus_dupes_separate_keys_use_labels(self):
        """test a condition related to #6710.

        also see test_compiler->
        test_overlapping_labels_plus_dupes_separate_keys_use_labels

        for a non cte form of this test.

        """

        m = MetaData()
        foo = Table(
            "foo",
            m,
            Column("id", Integer),
            Column("bar_id", Integer, key="bb"),
        )
        foo_bar = Table("foo_bar", m, Column("id", Integer, key="bb"))

        stmt = select(
            foo.c.id,
            foo.c.bb,
            foo_bar.c.bb,
            foo.c.bb,
            foo.c.id,
            foo.c.bb,
            foo_bar.c.bb,
            foo_bar.c.bb,
        ).set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)

        cte = stmt.cte(recursive=True)

        self.assert_compile(
            select(cte),
            "WITH RECURSIVE anon_1(foo_id, foo_bar_id, foo_bar_id_1) AS "
            "(SELECT foo.id AS foo_id, foo.bar_id AS foo_bar_id, "
            "foo_bar.id AS foo_bar_id_1, foo.bar_id AS foo_bar_id__1, "
            "foo.id AS foo_id__1, foo.bar_id AS foo_bar_id__1, "
            "foo_bar.id AS foo_bar_id__2, foo_bar.id AS foo_bar_id__2 "
            "FROM foo, foo_bar) "
            "SELECT anon_1.foo_id, anon_1.foo_bar_id, anon_1.foo_bar_id_1, "
            "anon_1.foo_bar_id AS foo_bar_id_2, anon_1.foo_id AS foo_id_1, "
            "anon_1.foo_bar_id AS foo_bar_id_3, "
            "anon_1.foo_bar_id_1 AS foo_bar_id_1_1, "
            "anon_1.foo_bar_id_1 AS foo_bar_id_1_2 FROM anon_1",
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
            "WITH upsert AS (UPDATE orders SET amount=:param_5, "
            "product=:param_6, quantity=:param_7 "
            "WHERE orders.region = :region_1 "
            "RETURNING orders.region, orders.amount, "
            "orders.product, orders.quantity) "
            "INSERT INTO orders (region, amount, product, quantity) "
            "SELECT :param_1 AS anon_1, :param_2 AS anon_2, "
            ":param_3 AS anon_3, :param_4 AS anon_4 WHERE NOT (EXISTS "
            "(SELECT upsert.region, upsert.amount, upsert.product, "
            "upsert.quantity FROM upsert))",
            checkparams={
                "param_1": "Region1",
                "param_2": 1.0,
                "param_3": "Product1",
                "param_4": 1,
                "param_5": 1.0,
                "param_6": "Product1",
                "param_7": 1,
                "region_1": "Region1",
            },
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
            "WITH anon_1 AS (UPDATE orders SET region=:param_1 "
            "WHERE orders.region = :region_1 RETURNING orders.region) "
            "SELECT anon_1.region FROM anon_1",
            checkparams={"param_1": "y", "region_1": "x"},
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
            "VALUES (:param_1) RETURNING orders.region) "
            "SELECT anon_1.region FROM anon_1",
            checkparams={"param_1": "y"},
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
            "(UPDATE products SET price=:param_1 "
            "RETURNING products.id, products.price) "
            "SELECT t.id, t.price "
            "FROM t",
            checkparams={"param_1": "someprice"},
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
            "(INSERT INTO products (id, price) VALUES (:param_1, :param_2) "
            "RETURNING products.id, products.price) "
            "SELECT pd.id, pd.price "
            "FROM pd",
            checkparams={"param_1": 1, "param_2": 27.0},
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

    def test_update_against_cte_directly(self):
        """test #6464

        for UPDATE, I'm not sure this is a valid syntax on any platform.

        """
        products = table("products", column("id"), column("price"))

        cte = products.select().cte("pd")
        assert "autocommit" not in cte.select()._execution_options

        stmt = update(cte)
        eq_(stmt.compile().execution_options["autocommit"], True)

        self.assert_compile(
            stmt,
            "WITH pd AS (SELECT products.id AS id, products.price AS price "
            "FROM products) UPDATE pd SET id=:id, price=:price",
        )
        eq_(stmt.compile().isupdate, True)

    def test_delete_against_cte_directly(self):
        """test #6464.

        SQL-Server specific arrangement seems to allow
        DELETE from a CTE directly.

        """
        products = table("products", column("id"), column("price"))

        cte = products.select().cte("pd")
        assert "autocommit" not in cte.select()._execution_options

        stmt = delete(cte)
        eq_(stmt.compile().execution_options["autocommit"], True)

        self.assert_compile(
            stmt,
            "WITH pd AS (SELECT products.id AS id, products.price AS price "
            "FROM products) DELETE FROM pd",
        )
        eq_(stmt.compile().isdelete, True)

    def test_delete_against_user_textual_cte(self):
        """test #6464.

        Test the user's exact arrangement.

        """

        q = select(
            text(
                "name, date_hour, "
                "ROW_NUMBER() OVER(PARTITION BY name, date_hour "
                "ORDER BY value DESC)"
                " AS RN FROM testtable"
            )
        )
        cte = q.cte("deldup")
        stmt = delete(cte).where(text("RN > 1"))
        eq_(stmt.compile().execution_options["autocommit"], True)

        self.assert_compile(
            stmt,
            "WITH deldup AS (SELECT name, date_hour, ROW_NUMBER() "
            "OVER(PARTITION BY name, date_hour ORDER BY value DESC) "
            "AS RN FROM testtable) DELETE FROM deldup WHERE RN > 1",
        )
        eq_(stmt.compile().isdelete, True)

    def test_select_uses_independent_cte(self):
        products = table("products", column("id"), column("price"))

        upd_cte = (
            products.update().values(price=10).where(products.c.price > 50)
        ).cte()

        stmt = products.select().where(products.c.price < 45).add_cte(upd_cte)

        self.assert_compile(
            stmt,
            "WITH anon_1 AS (UPDATE products SET price=:param_1 "
            "WHERE products.price > :price_1) "
            "SELECT products.id, products.price "
            "FROM products WHERE products.price < :price_2",
            checkparams={"param_1": 10, "price_1": 50, "price_2": 45},
        )

    def test_compound_select_uses_independent_cte(self):
        products = table("products", column("id"), column("price"))

        upd_cte = (
            products.update().values(price=10).where(products.c.price > 50)
        ).cte()

        stmt = (
            products.select()
            .where(products.c.price < 45)
            .union(products.select().where(products.c.price > 90))
            .add_cte(upd_cte)
        )

        self.assert_compile(
            stmt,
            "WITH anon_1 AS (UPDATE products SET price=:param_1 "
            "WHERE products.price > :price_1) "
            "SELECT products.id, products.price "
            "FROM products WHERE products.price < :price_2 "
            "UNION "
            "SELECT products.id, products.price "
            "FROM products WHERE products.price > :price_3",
            checkparams={
                "param_1": 10,
                "price_1": 50,
                "price_2": 45,
                "price_3": 90,
            },
        )

    def test_insert_uses_independent_cte(self):
        products = table("products", column("id"), column("price"))

        upd_cte = (
            products.update().values(price=10).where(products.c.price > 50)
        ).cte()

        stmt = (
            products.insert().values({"id": 1, "price": 20}).add_cte(upd_cte)
        )

        self.assert_compile(
            stmt,
            "WITH anon_1 AS (UPDATE products SET price=:param_1 "
            "WHERE products.price > :price_1) "
            "INSERT INTO products (id, price) VALUES (:id, :price)",
            checkparams={"id": 1, "price": 20, "param_1": 10, "price_1": 50},
        )

    def test_insert_from_select_uses_independent_cte(self):
        """test #7036"""

        t1 = table("table1", column("id1"), column("a"))

        t2 = table("table2", column("id2"), column("b"))

        ins1 = t1.insert().from_select(["id1", "a"], select(1, text("'a'")))

        cte1 = ins1.cte("cte1")

        ins2 = t2.insert().from_select(["id2", "b"], select(2, text("'b'")))

        ins2 = ins2.add_cte(cte1)

        self.assert_compile(
            ins2,
            "WITH cte1 AS "
            "(INSERT INTO table1 (id1, a) SELECT 1, 'a') "
            "INSERT INTO table2 (id2, b) SELECT 2, 'b'",
            checkparams={},
        )

    def test_update_uses_independent_cte(self):
        products = table("products", column("id"), column("price"))

        upd_cte = (
            products.update().values(price=10).where(products.c.price > 50)
        ).cte()

        stmt = (
            products.update()
            .values(price=5)
            .where(products.c.price < 50)
            .add_cte(upd_cte)
        )

        self.assert_compile(
            stmt,
            "WITH anon_1 AS (UPDATE products SET price=:param_1 "
            "WHERE products.price > :price_1) UPDATE products "
            "SET price=:price WHERE products.price < :price_2",
            checkparams={
                "param_1": 10,
                "price": 5,
                "price_1": 50,
                "price_2": 50,
            },
        )

    def test_update_w_insert_independent_cte(self):
        products = table("products", column("id"), column("price"))

        ins_cte = (products.insert().values({"id": 1, "price": 10})).cte()

        stmt = (
            products.update()
            .values(price=5)
            .where(products.c.price < 50)
            .add_cte(ins_cte)
        )

        self.assert_compile(
            stmt,
            "WITH anon_1 AS (INSERT INTO products (id, price) "
            "VALUES (:param_1, :param_2)) "
            "UPDATE products SET price=:price WHERE products.price < :price_1",
            checkparams={
                "price": 5,
                "param_1": 1,
                "param_2": 10,
                "price_1": 50,
            },
        )

    def test_delete_uses_independent_cte(self):
        products = table("products", column("id"), column("price"))

        upd_cte = (
            products.update().values(price=10).where(products.c.price > 50)
        ).cte()

        stmt = products.delete().where(products.c.price < 45).add_cte(upd_cte)

        self.assert_compile(
            stmt,
            "WITH anon_1 AS (UPDATE products SET price=:param_1 "
            "WHERE products.price > :price_1) "
            "DELETE FROM products WHERE products.price < :price_2",
            checkparams={"param_1": 10, "price_1": 50, "price_2": 45},
        )

    def test_independent_cte_can_be_referenced(self):
        products = table("products", column("id"), column("price"))

        cte = products.select().cte("pd")

        stmt = (
            products.update()
            .where(products.c.price == cte.c.price)
            .add_cte(cte)
        )

        self.assert_compile(
            stmt,
            "WITH pd AS "
            "(SELECT products.id AS id, products.price AS price "
            "FROM products) "
            "UPDATE products SET id=:id, price=:price FROM pd "
            "WHERE products.price = pd.price",
        )

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


class NestingCTETest(fixtures.TestBase, AssertsCompiledSQL):

    __dialect__ = "default_enhanced"

    def test_select_with_nesting_cte_in_cte(self):
        nesting_cte = select(literal(1).label("inner_cte")).cte(
            "nesting", nesting=True
        )
        stmt = select(
            select(nesting_cte.c.inner_cte.label("outer_cte")).cte("cte")
        )

        self.assert_compile(
            stmt,
            "WITH cte AS (WITH nesting AS (SELECT :param_1 AS inner_cte) "
            "SELECT nesting.inner_cte AS outer_cte FROM nesting) "
            "SELECT cte.outer_cte FROM cte",
        )

    def test_nesting_cte_in_cte_with_same_name(self):
        nesting_cte = select(literal(1).label("inner_cte")).cte(
            "some_cte", nesting=True
        )
        stmt = select(
            select(nesting_cte.c.inner_cte.label("outer_cte")).cte("some_cte")
        )

        self.assert_compile(
            stmt,
            "WITH some_cte AS (WITH some_cte AS "
            "(SELECT :param_1 AS inner_cte) "
            "SELECT some_cte.inner_cte AS outer_cte "
            "FROM some_cte) "
            "SELECT some_cte.outer_cte FROM some_cte",
        )

    def test_nesting_cte_at_top_level(self):
        nesting_cte = select(literal(1).label("val")).cte(
            "nesting_cte", nesting=True
        )
        cte = select(literal(2).label("val")).cte("cte")
        stmt = select(nesting_cte.c.val, cte.c.val)

        self.assert_compile(
            stmt,
            "WITH nesting_cte AS (SELECT :param_1 AS val)"
            ", cte AS (SELECT :param_2 AS val)"
            " SELECT nesting_cte.val, cte.val AS val_1 FROM nesting_cte, cte",
        )

    def test_double_nesting_cte_in_cte(self):
        """
        Validate that the SELECT in the 2nd nesting CTE does not render
        the 1st CTE.

        It implies that nesting CTE level is taken in account.
        """
        select_1_cte = select(literal(1).label("inner_cte")).cte(
            "nesting_1", nesting=True
        )
        select_2_cte = select(literal(2).label("inner_cte")).cte(
            "nesting_2", nesting=True
        )

        stmt = select(
            select(
                select_1_cte.c.inner_cte.label("outer_1"),
                select_2_cte.c.inner_cte.label("outer_2"),
            ).cte("cte")
        )

        self.assert_compile(
            stmt,
            "WITH cte AS ("
            "WITH nesting_1 AS (SELECT :param_1 AS inner_cte)"
            ", nesting_2 AS (SELECT :param_2 AS inner_cte)"
            " SELECT nesting_1.inner_cte AS outer_1"
            ", nesting_2.inner_cte AS outer_2"
            " FROM nesting_1, nesting_2"
            ") SELECT cte.outer_1, cte.outer_2 FROM cte",
        )

    def test_double_nesting_cte_with_cross_reference_in_cte(self):
        select_1_cte = select(literal(1).label("inner_cte_1")).cte(
            "nesting_1", nesting=True
        )
        select_2_cte = select(
            (select_1_cte.c.inner_cte_1 + 1).label("inner_cte_2")
        ).cte("nesting_2", nesting=True)

        # 1 next 2

        nesting_cte_1_2 = select(select_1_cte, select_2_cte).cte("cte")
        stmt_1_2 = select(nesting_cte_1_2)
        self.assert_compile(
            stmt_1_2,
            "WITH cte AS ("
            "WITH nesting_1 AS (SELECT :param_1 AS inner_cte_1)"
            ", nesting_2 AS (SELECT nesting_1.inner_cte_1 + :inner_cte_1_1"
            " AS inner_cte_2 FROM nesting_1)"
            " SELECT nesting_1.inner_cte_1 AS inner_cte_1"
            ", nesting_2.inner_cte_2 AS inner_cte_2"
            " FROM nesting_1, nesting_2"
            ") SELECT cte.inner_cte_1, cte.inner_cte_2 FROM cte",
        )

        # 2 next 1

        # Reorganize order with add_cte
        nesting_cte_2_1 = (
            select(select_2_cte, select_1_cte).add_cte(select_1_cte).cte("cte")
        )
        stmt_2_1 = select(nesting_cte_2_1)
        self.assert_compile(
            stmt_2_1,
            "WITH cte AS ("
            "WITH nesting_1 AS (SELECT :param_1 AS inner_cte_1)"
            ", nesting_2 AS (SELECT nesting_1.inner_cte_1 + :inner_cte_1_1"
            " AS inner_cte_2 FROM nesting_1)"
            " SELECT nesting_2.inner_cte_2 AS inner_cte_2"
            ", nesting_1.inner_cte_1 AS inner_cte_1"
            " FROM nesting_2, nesting_1"
            ") SELECT cte.inner_cte_2, cte.inner_cte_1 FROM cte",
        )

    def test_nesting_cte_in_nesting_cte_in_cte(self):
        select_1_cte = select(literal(1).label("inner_cte")).cte(
            "nesting_1", nesting=True
        )
        select_2_cte = select(select_1_cte.c.inner_cte.label("inner_2")).cte(
            "nesting_2", nesting=True
        )

        stmt = select(
            select(select_2_cte.c.inner_2.label("outer_cte")).cte("cte")
        )

        self.assert_compile(
            stmt,
            "WITH cte AS ("
            "WITH nesting_2 AS ("
            "WITH nesting_1 AS (SELECT :param_1 AS inner_cte)"
            " SELECT nesting_1.inner_cte AS inner_2 FROM nesting_1"
            ") SELECT nesting_2.inner_2 AS outer_cte FROM nesting_2"
            ") SELECT cte.outer_cte FROM cte",
        )

    def test_compound_select_with_nesting_cte_in_cte(self):
        select_1_cte = select(literal(1).label("inner_cte")).cte(
            "nesting_1", nesting=True
        )
        select_2_cte = select(literal(2).label("inner_cte")).cte(
            "nesting_2", nesting=True
        )

        nesting_cte = (
            select(select_1_cte).union(select(select_2_cte)).subquery()
        )

        stmt = select(
            select(nesting_cte.c.inner_cte.label("outer_cte")).cte("cte")
        )

        self.assert_compile(
            stmt,
            "WITH cte AS ("
            "SELECT anon_1.inner_cte AS outer_cte FROM ("
            "WITH nesting_1 AS (SELECT :param_1 AS inner_cte)"
            ", nesting_2 AS (SELECT :param_2 AS inner_cte)"
            " SELECT nesting_1.inner_cte AS inner_cte FROM nesting_1"
            " UNION"
            " SELECT nesting_2.inner_cte AS inner_cte FROM nesting_2"
            ") AS anon_1"
            ") SELECT cte.outer_cte FROM cte",
        )

    def test_nesting_cte_in_recursive_cte(self):
        nesting_cte = select(literal(1).label("inner_cte")).cte(
            "nesting", nesting=True
        )
        stmt = select(
            select(nesting_cte.c.inner_cte.label("outer_cte")).cte(
                "cte", recursive=True
            )
        )

        self.assert_compile(
            stmt,
            "WITH RECURSIVE cte(outer_cte) AS (WITH nesting AS "
            "(SELECT :param_1 AS inner_cte) "
            "SELECT nesting.inner_cte AS outer_cte FROM nesting) "
            "SELECT cte.outer_cte FROM cte",
        )

    def test_recursive_nesting_cte_in_cte(self):
        nesting_cte = select(literal(1).label("inner_cte")).cte(
            "nesting", nesting=True, recursive=True
        )
        stmt = select(
            select(nesting_cte.c.inner_cte.label("outer_cte")).cte("cte")
        )

        self.assert_compile(
            stmt,
            "WITH cte AS (WITH RECURSIVE nesting(inner_cte) AS "
            "(SELECT :param_1 AS inner_cte) "
            "SELECT nesting.inner_cte AS outer_cte FROM nesting) "
            "SELECT cte.outer_cte FROM cte",
        )

    def test_same_nested_cte_is_not_generated_twice(self):
        # Same = name and query
        nesting_cte_used_twice = select(literal(1).label("inner_cte_1")).cte(
            "nesting_cte", nesting=True
        )
        select_add_cte = select(
            (nesting_cte_used_twice.c.inner_cte_1 + 1).label("next_value")
        ).cte("nesting_2", nesting=True)

        union_cte = (
            select(
                (nesting_cte_used_twice.c.inner_cte_1 - 1).label("next_value")
            )
            .union(select(select_add_cte))
            .cte("wrapper", nesting=True)
        )

        stmt = (
            select(union_cte)
            .add_cte(nesting_cte_used_twice)
            .union(select(nesting_cte_used_twice))
        )

        self.assert_compile(
            stmt,
            "WITH nesting_cte AS "
            "(SELECT :param_1 AS inner_cte_1)"
            ", wrapper AS "
            "(WITH nesting_2 AS "
            "(SELECT nesting_cte.inner_cte_1 + :inner_cte_1_2 "
            "AS next_value "
            "FROM nesting_cte)"
            " SELECT nesting_cte.inner_cte_1 - :inner_cte_1_1 "
            "AS next_value "
            "FROM nesting_cte UNION SELECT nesting_2.next_value AS next_value "
            "FROM nesting_2)"
            " SELECT wrapper.next_value "
            "FROM wrapper UNION SELECT nesting_cte.inner_cte_1 "
            "FROM nesting_cte",
        )

    def test_recursive_nesting_cte_in_recursive_cte(self):
        nesting_cte = select(literal(1).label("inner_cte")).cte(
            "nesting", nesting=True, recursive=True
        )
        stmt = select(
            select(nesting_cte.c.inner_cte.label("outer_cte")).cte(
                "cte", recursive=True
            )
        )

        self.assert_compile(
            stmt,
            "WITH RECURSIVE cte(outer_cte) AS "
            "(WITH RECURSIVE nesting(inner_cte) "
            "AS (SELECT :param_1 AS inner_cte) "
            "SELECT nesting.inner_cte AS outer_cte FROM nesting) "
            "SELECT cte.outer_cte FROM cte",
        )

    def test_select_from_insert_cte_with_nesting(self):
        products = table("products", column("id"), column("price"))

        generator_cte = select(
            literal(1).label("id"), literal(27.0).label("price")
        ).cte("generator", nesting=True)

        cte = (
            products.insert()
            .from_select(
                [products.c.id, products.c.price],
                select(generator_cte),
            )
            .returning(*products.c)
            .cte("insert_cte")
        )

        stmt = select(cte)

        assert "autocommit" not in stmt._execution_options

        eq_(stmt.compile().execution_options["autocommit"], True)

        self.assert_compile(
            stmt,
            "WITH insert_cte AS "
            "(WITH generator AS "
            "(SELECT :param_1 AS id, :param_2 AS price) "
            "INSERT INTO products (id, price) "
            "SELECT generator.id AS id, generator.price "
            "AS price FROM generator "
            "RETURNING products.id, products.price) "
            "SELECT insert_cte.id, insert_cte.price "
            "FROM insert_cte",
        )
        eq_(stmt.compile().isinsert, False)

    def test_select_from_update_cte_with_nesting(self):
        t1 = table("table_1", column("id"), column("price"))

        generator_cte = select(
            literal(1).label("id"), literal(27.0).label("price")
        ).cte("generator", nesting=True)

        cte = (
            t1.update()
            .values(price=generator_cte.c.price)
            .where(t1.c.id == generator_cte.c.id)
            .returning(t1.c.id, t1.c.price)
        ).cte("update_cte")

        qry = select(cte)

        self.assert_compile(
            qry,
            "WITH update_cte AS "
            "(WITH generator AS "
            "(SELECT :param_1 AS id, :param_2 AS price) "
            "UPDATE table_1 SET price=generator.price "
            "FROM generator WHERE table_1.id = generator.id "
            "RETURNING table_1.id, table_1.price) "
            "SELECT update_cte.id, update_cte.price FROM update_cte",
        )

    def test_select_from_delete_cte_with_nesting(self):
        t1 = table("table_1", column("id"), column("price"))

        generator_cte = select(literal(1).label("id")).cte(
            "generator", nesting=True
        )

        dlt = (
            t1.delete()
            .where(t1.c.id == generator_cte.c.id)
            .returning(t1.c.id, t1.c.price)
        )

        cte = dlt.cte("delete_cte")

        qry = select(cte)

        self.assert_compile(
            qry,
            "WITH delete_cte AS "
            "(WITH generator AS "
            "(SELECT %(param_1)s AS id) "
            "DELETE FROM table_1 USING generator "
            "WHERE table_1.id = generator.id RETURNING table_1.id, "
            "table_1.price) SELECT delete_cte.id, delete_cte.price "
            "FROM delete_cte",
            dialect="postgresql",
        )

    def test_compound_select_with_nesting_cte_in_custom_order(self):
        select_1_cte = select(literal(1).label("inner_cte")).cte(
            "nesting_1", nesting=True
        )
        select_2_cte = select(literal(2).label("inner_cte")).cte(
            "nesting_2", nesting=True
        )

        nesting_cte = (
            select(select_1_cte)
            .union(select(select_2_cte))
            # Generate "select_2_cte" first
            .add_cte(select_2_cte)
            .subquery()
        )

        stmt = select(
            select(nesting_cte.c.inner_cte.label("outer_cte")).cte("cte")
        )

        self.assert_compile(
            stmt,
            "WITH cte AS ("
            "SELECT anon_1.inner_cte AS outer_cte FROM ("
            "WITH nesting_2 AS (SELECT :param_1 AS inner_cte)"
            ", nesting_1 AS (SELECT :param_2 AS inner_cte)"
            " SELECT nesting_1.inner_cte AS inner_cte FROM nesting_1"
            " UNION"
            " SELECT nesting_2.inner_cte AS inner_cte FROM nesting_2"
            ") AS anon_1"
            ") SELECT cte.outer_cte FROM cte",
        )

    def test_recursive_cte_referenced_multiple_times_with_nesting_cte(self):
        rec_root = select(literal(1).label("the_value")).cte(
            "recursive_cte", recursive=True
        )

        # Allow to reference the recursive CTE more than once
        rec_root_ref = rec_root.select().cte(
            "allow_multiple_ref", nesting=True
        )
        should_continue = select(
            exists(
                select(rec_root_ref.c.the_value)
                .where(rec_root_ref.c.the_value < 10)
                .limit(1)
            ).label("val")
        ).cte("should_continue", nesting=True)

        rec_part_1 = select(rec_root_ref.c.the_value * 2).where(
            should_continue.c.val != True
        )
        rec_part_2 = select(rec_root_ref.c.the_value * 3).where(
            should_continue.c.val != True
        )

        rec_part = rec_part_1.add_cte(rec_root_ref).union_all(rec_part_2)

        rec_cte = rec_root.union_all(rec_part)

        stmt = rec_cte.select()

        self.assert_compile(
            stmt,
            "WITH RECURSIVE recursive_cte(the_value) AS ("
            "SELECT :param_1 AS the_value UNION ALL ("
            "WITH allow_multiple_ref AS ("
            "SELECT recursive_cte.the_value AS the_value FROM recursive_cte)"
            ", should_continue AS (SELECT EXISTS ("
            "SELECT allow_multiple_ref.the_value FROM allow_multiple_ref"
            " WHERE allow_multiple_ref.the_value < :the_value_2"
            " LIMIT :param_2) AS val) "
            "SELECT allow_multiple_ref.the_value * :the_value_1 AS anon_1"
            " FROM allow_multiple_ref, should_continue "
            "WHERE should_continue.val != true"
            " UNION ALL SELECT allow_multiple_ref.the_value * :the_value_3"
            " AS anon_2 FROM allow_multiple_ref, should_continue"
            " WHERE should_continue.val != true))"
            " SELECT recursive_cte.the_value FROM recursive_cte",
        )
