from sqlalchemy import cast
from sqlalchemy import Column
from sqlalchemy import exc
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import testing
from sqlalchemy import true
from sqlalchemy import tuple_
from sqlalchemy import union
from sqlalchemy.sql import column
from sqlalchemy.sql import literal
from sqlalchemy.sql import table
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import eq_
from sqlalchemy.testing import expect_raises_message
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_

table1 = table(
    "mytable",
    column("myid", Integer),
    column("name", String),
    column("description", String),
)

table2 = table(
    "myothertable", column("otherid", Integer), column("othername", String)
)

metadata = MetaData()


parent = Table(
    "parent",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("data", String(50)),
)
child = Table(
    "child",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("parent_id", ForeignKey("parent.id")),
    Column("data", String(50)),
)

grandchild = Table(
    "grandchild",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("child_id", ForeignKey("child.id")),
)


class SelectTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = "default"

    def test_old_bracket_style_fail(self):
        with expect_raises_message(
            exc.ArgumentError,
            r"Column expression, FROM clause, or other columns clause .*"
            r".*Did you mean to say",
        ):
            select([table1.c.myid])

    def test_new_calling_style(self):
        stmt = select(table1.c.myid).where(table1.c.myid == table2.c.otherid)

        self.assert_compile(
            stmt,
            "SELECT mytable.myid FROM mytable, myothertable "
            "WHERE mytable.myid = myothertable.otherid",
        )

    @testing.combinations(
        (
            lambda tbl: select().select_from(tbl).where(tbl.c.id == 123),
            "SELECT FROM tbl WHERE tbl.id = :id_1",
        ),
        (lambda tbl: select().where(true()), "SELECT WHERE 1 = 1"),
        (
            lambda tbl: select()
            .select_from(tbl)
            .where(tbl.c.id == 123)
            .exists(),
            "EXISTS (SELECT FROM tbl WHERE tbl.id = :id_1)",
        ),
    )
    def test_select_no_columns(self, stmt, expected):
        """test #9440"""

        tbl = table("tbl", column("id"))

        stmt = testing.resolve_lambda(stmt, tbl=tbl)

        self.assert_compile(stmt, expected)

    def test_new_calling_style_clauseelement_thing_that_has_iter(self):
        class Thing:
            def __clause_element__(self):
                return table1

            def __iter__(self):
                return iter(["a", "b", "c"])

        stmt = select(Thing())
        self.assert_compile(
            stmt,
            "SELECT mytable.myid, mytable.name, "
            "mytable.description FROM mytable",
        )

    def test_new_calling_style_inspectable_ce_thing_that_has_iter(self):
        class Thing:
            def __iter__(self):
                return iter(["a", "b", "c"])

        class InspectedThing:
            def __clause_element__(self):
                return table1

        from sqlalchemy.inspection import _inspects

        @_inspects(Thing)
        def _ce(thing):
            return InspectedThing()

        stmt = select(Thing())
        self.assert_compile(
            stmt,
            "SELECT mytable.myid, mytable.name, "
            "mytable.description FROM mytable",
        )

    def test_join_nofrom_implicit_left_side_explicit_onclause(self):
        stmt = select(table1).join(table2, table1.c.myid == table2.c.otherid)

        self.assert_compile(
            stmt,
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable JOIN myothertable "
            "ON mytable.myid = myothertable.otherid",
        )

    def test_join_nofrom_implicit_left_side_explicit_onclause_3level(self):
        stmt = (
            select(parent)
            .join(child, child.c.parent_id == parent.c.id)
            .join(grandchild, grandchild.c.child_id == child.c.id)
        )

        self.assert_compile(
            stmt,
            "SELECT parent.id, parent.data FROM parent JOIN child "
            "ON child.parent_id = parent.id "
            "JOIN grandchild ON grandchild.child_id = child.id",
        )

    def test_join_nofrom_explicit_left_side_explicit_onclause(self):
        stmt = select(table1).join_from(
            table1, table2, table1.c.myid == table2.c.otherid
        )

        self.assert_compile(
            stmt,
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable JOIN myothertable "
            "ON mytable.myid = myothertable.otherid",
        )

    def test_outerjoin_nofrom_explicit_left_side_explicit_onclause(self):
        stmt = select(table1).outerjoin_from(
            table1, table2, table1.c.myid == table2.c.otherid
        )

        self.assert_compile(
            stmt,
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable LEFT OUTER JOIN myothertable "
            "ON mytable.myid = myothertable.otherid",
        )

    def test_join_nofrom_implicit_left_side_implicit_onclause(self):
        stmt = select(parent).join(child)

        self.assert_compile(
            stmt,
            "SELECT parent.id, parent.data FROM parent JOIN child "
            "ON parent.id = child.parent_id",
        )

    def test_join_nofrom_implicit_left_side_implicit_onclause_3level(self):
        stmt = select(parent).join(child).join(grandchild)

        self.assert_compile(
            stmt,
            "SELECT parent.id, parent.data FROM parent JOIN child "
            "ON parent.id = child.parent_id "
            "JOIN grandchild ON child.id = grandchild.child_id",
        )

    def test_join_nofrom_explicit_left_side_implicit_onclause(self):
        stmt = select(parent).join_from(parent, child)

        self.assert_compile(
            stmt,
            "SELECT parent.id, parent.data FROM parent JOIN child "
            "ON parent.id = child.parent_id",
        )

    def test_join_froms_implicit_left_side_explicit_onclause(self):
        stmt = (
            select(table1)
            .select_from(table1)
            .join(table2, table1.c.myid == table2.c.otherid)
        )

        self.assert_compile(
            stmt,
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable JOIN myothertable "
            "ON mytable.myid = myothertable.otherid",
        )

    def test_join_froms_explicit_left_side_explicit_onclause(self):
        stmt = (
            select(table1)
            .select_from(table1)
            .join_from(table1, table2, table1.c.myid == table2.c.otherid)
        )

        self.assert_compile(
            stmt,
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable JOIN myothertable "
            "ON mytable.myid = myothertable.otherid",
        )

    def test_join_froms_implicit_left_side_implicit_onclause(self):
        stmt = select(parent).select_from(parent).join(child)

        self.assert_compile(
            stmt,
            "SELECT parent.id, parent.data FROM parent JOIN child "
            "ON parent.id = child.parent_id",
        )

    def test_join_froms_explicit_left_side_implicit_onclause(self):
        stmt = select(parent).select_from(parent).join_from(parent, child)

        self.assert_compile(
            stmt,
            "SELECT parent.id, parent.data FROM parent JOIN child "
            "ON parent.id = child.parent_id",
        )

    def test_join_implicit_left_side_wo_cols_onelevel(self):
        """test issue #6503"""
        stmt = select(parent).join(child).with_only_columns(child.c.id)

        self.assert_compile(
            stmt,
            "SELECT child.id FROM parent "
            "JOIN child ON parent.id = child.parent_id",
        )

    def test_join_implicit_left_side_wo_cols_onelevel_union(self):
        """test issue #6698, regression from #6503.

        this issue didn't affect Core but testing it here anyway."""
        stmt = select(parent).join(child).with_only_columns(child.c.id)

        stmt = stmt.union(select(child.c.id))
        self.assert_compile(
            stmt,
            "SELECT child.id FROM parent "
            "JOIN child ON parent.id = child.parent_id "
            "UNION "
            "SELECT child.id FROM child",
        )

    def test_join_implicit_left_side_wo_cols_twolevel(self):
        """test issue #6503"""
        stmt = (
            select(parent)
            .join(child)
            .with_only_columns(child.c.id)
            .join(grandchild)
            .with_only_columns(grandchild.c.id)
        )

        self.assert_compile(
            stmt,
            "SELECT grandchild.id FROM parent "
            "JOIN child ON parent.id = child.parent_id "
            "JOIN grandchild ON child.id = grandchild.child_id",
        )

    def test_join_implicit_left_side_wo_cols_twolevel_union(self):
        """test issue #6698, regression from #6503.

        this issue didn't affect Core but testing it here anyway."""
        stmt = (
            select(parent)
            .join(child)
            .with_only_columns(child.c.id)
            .join(grandchild)
            .with_only_columns(grandchild.c.id)
        )

        stmt = union(stmt, select(grandchild.c.id))
        self.assert_compile(
            stmt,
            "SELECT grandchild.id FROM parent "
            "JOIN child ON parent.id = child.parent_id "
            "JOIN grandchild ON child.id = grandchild.child_id "
            "UNION "
            "SELECT grandchild.id FROM grandchild",
        )

    def test_right_nested_inner_join(self):
        inner = child.join(grandchild)

        stmt = select(parent).outerjoin_from(parent, inner)

        self.assert_compile(
            stmt,
            "SELECT parent.id, parent.data FROM parent "
            "LEFT OUTER JOIN "
            "(child JOIN grandchild ON child.id = grandchild.child_id) "
            "ON parent.id = child.parent_id",
        )

    def test_joins_w_filter_by(self):
        stmt = (
            select(parent)
            .filter_by(data="p1")
            .join(child)
            .filter_by(data="c1")
            .join_from(table1, table2, table1.c.myid == table2.c.otherid)
            .filter_by(otherid=5)
        )

        self.assert_compile(
            stmt,
            "SELECT parent.id, parent.data FROM parent JOIN child "
            "ON parent.id = child.parent_id, mytable JOIN myothertable "
            "ON mytable.myid = myothertable.otherid "
            "WHERE parent.data = :data_1 AND child.data = :data_2 "
            "AND myothertable.otherid = :otherid_1",
            checkparams={"data_1": "p1", "data_2": "c1", "otherid_1": 5},
        )

    def test_filter_by_from_col(self):
        stmt = select(table1.c.myid).filter_by(name="foo")
        self.assert_compile(
            stmt,
            "SELECT mytable.myid FROM mytable WHERE mytable.name = :name_1",
        )

    def test_filter_by_from_func(self):
        """test #6414"""
        stmt = select(func.count(table1.c.myid)).filter_by(name="foo")
        self.assert_compile(
            stmt,
            "SELECT count(mytable.myid) AS count_1 "
            "FROM mytable WHERE mytable.name = :name_1",
        )

    def test_filter_by_from_func_not_the_first_arg(self):
        """test #6414"""
        stmt = select(func.bar(True, table1.c.myid)).filter_by(name="foo")
        self.assert_compile(
            stmt,
            "SELECT bar(:bar_2, mytable.myid) AS bar_1 "
            "FROM mytable WHERE mytable.name = :name_1",
        )

    def test_filter_by_from_cast(self):
        """test #6414"""
        stmt = select(cast(table1.c.myid, Integer)).filter_by(name="foo")
        self.assert_compile(
            stmt,
            "SELECT CAST(mytable.myid AS INTEGER) AS myid "
            "FROM mytable WHERE mytable.name = :name_1",
        )

    def test_filter_by_from_binary(self):
        """test #6414"""
        stmt = select(table1.c.myid == 5).filter_by(name="foo")
        self.assert_compile(
            stmt,
            "SELECT mytable.myid = :myid_1 AS anon_1 "
            "FROM mytable WHERE mytable.name = :name_1",
        )

    def test_filter_by_from_label(self):
        """test #6414"""
        stmt = select(table1.c.myid.label("some_id")).filter_by(name="foo")
        self.assert_compile(
            stmt,
            "SELECT mytable.myid AS some_id "
            "FROM mytable WHERE mytable.name = :name_1",
        )

    def test_filter_by_no_property_from_table(self):
        assert_raises_message(
            exc.InvalidRequestError,
            'Entity namespace for "mytable" has no property "foo"',
            select(table1).filter_by,
            foo="bar",
        )

    def test_filter_by_no_property_from_col(self):
        assert_raises_message(
            exc.InvalidRequestError,
            'Entity namespace for "mytable.myid" has no property "foo"',
            select(table1.c.myid).filter_by,
            foo="bar",
        )

    def test_select_tuple_outer(self):
        stmt = select(tuple_(table1.c.myid, table1.c.name))

        assert_raises_message(
            exc.CompileError,
            r"Most backends don't support SELECTing from a tuple\(\) object.  "
            "If this is an ORM query, consider using the Bundle object.",
            stmt.compile,
        )

    def test_select_tuple_subquery(self):
        subq = select(
            table1.c.name, tuple_(table1.c.myid, table1.c.name)
        ).subquery()

        stmt = select(subq.c.name)

        # if we aren't fetching it, then render it
        self.assert_compile(
            stmt,
            "SELECT anon_1.name FROM (SELECT mytable.name AS name, "
            "(mytable.myid, mytable.name) AS anon_2 FROM mytable) AS anon_1",
        )

    @testing.combinations(
        ("union_all", "UNION ALL"),
        ("union", "UNION"),
        ("intersect_all", "INTERSECT ALL"),
        ("intersect", "INTERSECT"),
        ("except_all", "EXCEPT ALL"),
        ("except_", "EXCEPT"),
    )
    def test_select_multiple_compound_elements(self, methname, joiner):
        stmt = select(literal(1))
        meth = getattr(stmt, methname)
        stmt = meth(select(literal(2)), select(literal(3)))

        self.assert_compile(
            stmt,
            "SELECT :param_1 AS anon_1"
            " %(joiner)s SELECT :param_2 AS anon_2"
            " %(joiner)s SELECT :param_3 AS anon_3" % {"joiner": joiner},
        )


class ColumnCollectionAsSelectTest(fixtures.TestBase, AssertsCompiledSQL):
    """tests related to #8285."""

    __dialect__ = "default"

    def test_c_collection_as_from(self):
        stmt = select(parent.c)

        # this works because _all_selected_columns expands out
        # ClauseList.  it does so in the same way that it works for
        # Table already.  so this is free
        eq_(stmt._all_selected_columns, [parent.c.id, parent.c.data])

        self.assert_compile(stmt, "SELECT parent.id, parent.data FROM parent")

    def test_c_sub_collection_str_stmt(self):
        stmt = select(table1.c["myid", "description"])

        self.assert_compile(
            stmt, "SELECT mytable.myid, mytable.description FROM mytable"
        )

        subq = stmt.subquery()
        self.assert_compile(
            select(subq.c[0]).where(subq.c.description == "x"),
            "SELECT anon_1.myid FROM (SELECT mytable.myid AS myid, "
            "mytable.description AS description FROM mytable) AS anon_1 "
            "WHERE anon_1.description = :description_1",
        )

    def test_c_sub_collection_int_stmt(self):
        stmt = select(table1.c[2, 0])

        self.assert_compile(
            stmt, "SELECT mytable.description, mytable.myid FROM mytable"
        )

        subq = stmt.subquery()
        self.assert_compile(
            select(subq.c.myid).where(subq.c[1] == "x"),
            "SELECT anon_1.myid FROM (SELECT mytable.description AS "
            "description, mytable.myid AS myid FROM mytable) AS anon_1 "
            "WHERE anon_1.myid = :myid_1",
        )

    def test_c_sub_collection_str(self):
        coll = table1.c["myid", "description"]
        is_(coll.myid, table1.c.myid)

        eq_(list(coll), [table1.c.myid, table1.c.description])

    def test_c_sub_collection_int(self):
        coll = table1.c[2, 0]

        is_(coll.myid, table1.c.myid)

        eq_(list(coll), [table1.c.description, table1.c.myid])

    def test_c_sub_collection_positive_slice(self):
        coll = table1.c[0:2]

        is_(coll.myid, table1.c.myid)
        is_(coll.name, table1.c.name)

        eq_(list(coll), [table1.c.myid, table1.c.name])

    def test_c_sub_collection_negative_slice(self):
        coll = table1.c[-2:]

        is_(coll.name, table1.c.name)
        is_(coll.description, table1.c.description)

        eq_(list(coll), [table1.c.name, table1.c.description])

    def test_missing_key(self):
        with expect_raises_message(KeyError, "unknown"):
            table1.c["myid", "unknown"]

    def test_missing_index(self):
        with expect_raises_message(IndexError, "5"):
            table1.c["myid", 5]
