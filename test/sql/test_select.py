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
from sqlalchemy import tuple_
from sqlalchemy import union
from sqlalchemy.sql import column
from sqlalchemy.sql import table
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import fixtures


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


class FutureSelectTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = "default"

    def test_new_calling_style(self):
        stmt = select(table1.c.myid).where(table1.c.myid == table2.c.otherid)

        self.assert_compile(
            stmt,
            "SELECT mytable.myid FROM mytable, myothertable "
            "WHERE mytable.myid = myothertable.otherid",
        )

    def test_new_calling_style_clauseelement_thing_that_has_iter(self):
        class Thing(object):
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
        class Thing(object):
            def __iter__(self):
                return iter(["a", "b", "c"])

        class InspectedThing(object):
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
