from sqlalchemy import Column
from sqlalchemy import exc
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy.future import select as future_select
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


class FutureSelectTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = "default"

    def test_join_nofrom_implicit_left_side_explicit_onclause(self):
        stmt = future_select(table1).join(
            table2, table1.c.myid == table2.c.otherid
        )

        self.assert_compile(
            stmt,
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable JOIN myothertable "
            "ON mytable.myid = myothertable.otherid",
        )

    def test_join_nofrom_explicit_left_side_explicit_onclause(self):
        stmt = future_select(table1).join_from(
            table1, table2, table1.c.myid == table2.c.otherid
        )

        self.assert_compile(
            stmt,
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable JOIN myothertable "
            "ON mytable.myid = myothertable.otherid",
        )

    def test_join_nofrom_implicit_left_side_implicit_onclause(self):
        stmt = future_select(parent).join(child)

        self.assert_compile(
            stmt,
            "SELECT parent.id, parent.data FROM parent JOIN child "
            "ON parent.id = child.parent_id",
        )

    def test_join_nofrom_explicit_left_side_implicit_onclause(self):
        stmt = future_select(parent).join_from(parent, child)

        self.assert_compile(
            stmt,
            "SELECT parent.id, parent.data FROM parent JOIN child "
            "ON parent.id = child.parent_id",
        )

    def test_join_froms_implicit_left_side_explicit_onclause(self):
        stmt = (
            future_select(table1)
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
            future_select(table1)
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
        stmt = future_select(parent).select_from(parent).join(child)

        self.assert_compile(
            stmt,
            "SELECT parent.id, parent.data FROM parent JOIN child "
            "ON parent.id = child.parent_id",
        )

    def test_join_froms_explicit_left_side_implicit_onclause(self):
        stmt = (
            future_select(parent).select_from(parent).join_from(parent, child)
        )

        self.assert_compile(
            stmt,
            "SELECT parent.id, parent.data FROM parent JOIN child "
            "ON parent.id = child.parent_id",
        )

    def test_joins_w_filter_by(self):
        stmt = (
            future_select(parent)
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

    def test_filter_by_no_property(self):
        assert_raises_message(
            exc.InvalidRequestError,
            'Entity namespace for "mytable" has no property "foo"',
            future_select(table1).filter_by,
            foo="bar",
        )
