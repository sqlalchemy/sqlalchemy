from sqlalchemy import exc
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import Integer
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy import tuple_
from sqlalchemy.orm import aliased
from sqlalchemy.orm import Bundle
from sqlalchemy.orm import mapper
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session
from sqlalchemy.sql.elements import ClauseList
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.fixtures import fixture_session
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table


class BundleTest(fixtures.MappedTest, AssertsCompiledSQL):
    __dialect__ = "default"

    run_inserts = "once"
    run_setup_mappers = "once"
    run_deletes = None

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "data",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("d1", String(10)),
            Column("d2", String(10)),
            Column("d3", String(10)),
        )

        Table(
            "other",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data_id", ForeignKey("data.id")),
            Column("o1", String(10)),
        )

    @classmethod
    def setup_classes(cls):
        class Data(cls.Basic):
            pass

        class Other(cls.Basic):
            pass

    @classmethod
    def setup_mappers(cls):
        mapper(
            cls.classes.Data,
            cls.tables.data,
            properties={"others": relationship(cls.classes.Other)},
        )
        mapper(cls.classes.Other, cls.tables.other)

    @classmethod
    def insert_data(cls, connection):
        sess = Session(connection)
        sess.add_all(
            [
                cls.classes.Data(
                    d1="d%dd1" % i,
                    d2="d%dd2" % i,
                    d3="d%dd3" % i,
                    others=[
                        cls.classes.Other(o1="d%do%d" % (i, j))
                        for j in range(5)
                    ],
                )
                for i in range(10)
            ]
        )
        sess.commit()

    def test_tuple_suggests_bundle(self, connection):
        Data, Other = self.classes("Data", "Other")

        sess = Session(connection)
        q = sess.query(tuple_(Data.id, Other.id)).join(Data.others)

        assert_raises_message(
            exc.CompileError,
            r"Most backends don't support SELECTing from a tuple\(\) object.  "
            "If this is an ORM query, consider using the Bundle object.",
            q.all,
        )

    def test_tuple_suggests_bundle_future(self, connection):
        Data, Other = self.classes("Data", "Other")

        stmt = select(tuple_(Data.id, Other.id)).join(Data.others)

        sess = Session(connection, future=True)

        assert_raises_message(
            exc.CompileError,
            r"Most backends don't support SELECTing from a tuple\(\) object.  "
            "If this is an ORM query, consider using the Bundle object.",
            sess.execute,
            stmt,
        )

    def test_same_named_col_clauselist(self):
        Data, Other = self.classes("Data", "Other")
        bundle = Bundle("pk", Data.id, Other.id)

        self.assert_compile(ClauseList(Data.id, Other.id), "data.id, other.id")
        self.assert_compile(bundle.__clause_element__(), "data.id, other.id")

    def test_same_named_col_in_orderby(self):
        Data, Other = self.classes("Data", "Other")
        bundle = Bundle("pk", Data.id, Other.id)
        sess = fixture_session()

        self.assert_compile(
            sess.query(Data, Other).order_by(bundle),
            "SELECT data.id AS data_id, data.d1 AS data_d1, "
            "data.d2 AS data_d2, data.d3 AS data_d3, "
            "other.id AS other_id, other.data_id AS other_data_id, "
            "other.o1 AS other_o1 "
            "FROM data, other ORDER BY data.id, other.id",
        )

    def test_same_named_col_in_fetch(self):
        Data, Other = self.classes("Data", "Other")
        bundle = Bundle("pk", Data.id, Other.id)
        sess = fixture_session()

        eq_(
            sess.query(bundle)
            .filter(Data.id == Other.id)
            .filter(Data.id < 3)
            .all(),
            [((1, 1),), ((2, 2),)],
        )

    def test_c_attr(self):
        Data = self.classes.Data

        b1 = Bundle("b1", Data.d1, Data.d2)

        self.assert_compile(
            select(b1.c.d1, b1.c.d2), "SELECT data.d1, data.d2 FROM data"
        )

    def test_result(self):
        Data = self.classes.Data
        sess = fixture_session()

        b1 = Bundle("b1", Data.d1, Data.d2)

        eq_(
            sess.query(b1).filter(b1.c.d1.between("d3d1", "d5d1")).all(),
            [(("d3d1", "d3d2"),), (("d4d1", "d4d2"),), (("d5d1", "d5d2"),)],
        )

    def test_subclass(self):
        Data = self.classes.Data
        sess = fixture_session()

        class MyBundle(Bundle):
            def create_row_processor(self, query, procs, labels):
                def proc(row):
                    return dict(zip(labels, (proc(row) for proc in procs)))

                return proc

        b1 = MyBundle("b1", Data.d1, Data.d2)

        eq_(
            sess.query(b1).filter(b1.c.d1.between("d3d1", "d5d1")).all(),
            [
                ({"d2": "d3d2", "d1": "d3d1"},),
                ({"d2": "d4d2", "d1": "d4d1"},),
                ({"d2": "d5d2", "d1": "d5d1"},),
            ],
        )

    def test_multi_bundle(self):
        Data = self.classes.Data
        Other = self.classes.Other

        d1 = aliased(Data)

        b1 = Bundle("b1", d1.d1, d1.d2)
        b2 = Bundle("b2", Data.d1, Other.o1)

        sess = fixture_session()

        q = (
            sess.query(b1, b2)
            .join(Data.others)
            .join(d1, d1.id == Data.id)
            .filter(b1.c.d1 == "d3d1")
        )
        eq_(
            q.all(),
            [
                (("d3d1", "d3d2"), ("d3d1", "d3o0")),
                (("d3d1", "d3d2"), ("d3d1", "d3o1")),
                (("d3d1", "d3d2"), ("d3d1", "d3o2")),
                (("d3d1", "d3d2"), ("d3d1", "d3o3")),
                (("d3d1", "d3d2"), ("d3d1", "d3o4")),
            ],
        )

    def test_multi_bundle_future(self):
        Data = self.classes.Data
        Other = self.classes.Other

        d1 = aliased(Data)

        b1 = Bundle("b1", d1.d1, d1.d2)
        b2 = Bundle("b2", Data.d1, Other.o1)

        sess = Session(testing.db, future=True)

        stmt = (
            select(b1, b2)
            .join(Data.others)
            .join(d1, d1.id == Data.id)
            .filter(b1.c.d1 == "d3d1")
        )

        eq_(
            sess.execute(stmt).all(),
            [
                (("d3d1", "d3d2"), ("d3d1", "d3o0")),
                (("d3d1", "d3d2"), ("d3d1", "d3o1")),
                (("d3d1", "d3d2"), ("d3d1", "d3o2")),
                (("d3d1", "d3d2"), ("d3d1", "d3o3")),
                (("d3d1", "d3d2"), ("d3d1", "d3o4")),
            ],
        )

    def test_single_entity_legacy_query(self):
        Data = self.classes.Data
        sess = fixture_session()

        b1 = Bundle("b1", Data.d1, Data.d2, single_entity=True)

        eq_(
            sess.query(b1).filter(b1.c.d1.between("d3d1", "d5d1")).all(),
            [("d3d1", "d3d2"), ("d4d1", "d4d2"), ("d5d1", "d5d2")],
        )

    def test_labeled_cols_non_single_entity_legacy_query(self):
        Data = self.classes.Data
        sess = fixture_session()

        b1 = Bundle("b1", Data.d1.label("x"), Data.d2.label("y"))

        eq_(
            sess.query(b1).filter(b1.c.x.between("d3d1", "d5d1")).all(),
            [(("d3d1", "d3d2"),), (("d4d1", "d4d2"),), (("d5d1", "d5d2"),)],
        )

    def test_labeled_cols_single_entity_legacy_query(self):
        Data = self.classes.Data
        sess = fixture_session()

        b1 = Bundle(
            "b1", Data.d1.label("x"), Data.d2.label("y"), single_entity=True
        )

        eq_(
            sess.query(b1).filter(b1.c.x.between("d3d1", "d5d1")).all(),
            [("d3d1", "d3d2"), ("d4d1", "d4d2"), ("d5d1", "d5d2")],
        )

    def test_labeled_cols_as_rows_future(self):
        Data = self.classes.Data
        sess = fixture_session()

        b1 = Bundle("b1", Data.d1.label("x"), Data.d2.label("y"))

        stmt = select(b1).filter(b1.c.x.between("d3d1", "d5d1"))

        eq_(
            sess.execute(stmt).all(),
            [(("d3d1", "d3d2"),), (("d4d1", "d4d2"),), (("d5d1", "d5d2"),)],
        )

    def test_labeled_cols_as_scalars_future(self):
        Data = self.classes.Data
        sess = fixture_session()

        b1 = Bundle("b1", Data.d1.label("x"), Data.d2.label("y"))

        stmt = select(b1).filter(b1.c.x.between("d3d1", "d5d1"))
        eq_(
            sess.execute(stmt).scalars().all(),
            [("d3d1", "d3d2"), ("d4d1", "d4d2"), ("d5d1", "d5d2")],
        )

    def test_single_entity_flag_is_legacy_w_future(self):
        Data = self.classes.Data
        sess = Session(testing.db, future=True)

        # flag has no effect
        b1 = Bundle("b1", Data.d1, Data.d2, single_entity=True)

        stmt = select(b1).filter(b1.c.d1.between("d3d1", "d5d1"))

        with testing.expect_deprecated_20(
            "The Bundle.single_entity flag has no effect when "
            "using 2.0 style execution."
        ):
            rows = sess.execute(stmt).all()
        eq_(
            rows,
            [(("d3d1", "d3d2"),), (("d4d1", "d4d2"),), (("d5d1", "d5d2"),)],
        )

    def test_as_scalars_future(self):
        Data = self.classes.Data
        sess = Session(testing.db)

        b1 = Bundle("b1", Data.d1, Data.d2)

        stmt = select(b1).filter(b1.c.d1.between("d3d1", "d5d1"))
        eq_(
            sess.execute(stmt).scalars().all(),
            [("d3d1", "d3d2"), ("d4d1", "d4d2"), ("d5d1", "d5d2")],
        )

    def test_single_entity_flag_but_multi_entities(self):
        Data = self.classes.Data
        sess = fixture_session()

        b1 = Bundle("b1", Data.d1, Data.d2, single_entity=True)
        b2 = Bundle("b1", Data.d3, single_entity=True)

        eq_(
            sess.query(b1, b2).filter(b1.c.d1.between("d3d1", "d5d1")).all(),
            [
                (("d3d1", "d3d2"), ("d3d3",)),
                (("d4d1", "d4d2"), ("d4d3",)),
                (("d5d1", "d5d2"), ("d5d3",)),
            ],
        )

    def test_bundle_nesting(self):
        Data = self.classes.Data
        sess = fixture_session()

        b1 = Bundle("b1", Data.d1, Bundle("b2", Data.d2, Data.d3))

        eq_(
            sess.query(b1)
            .filter(b1.c.d1.between("d3d1", "d7d1"))
            .filter(b1.c.b2.c.d2.between("d4d2", "d6d2"))
            .all(),
            [
                (("d4d1", ("d4d2", "d4d3")),),
                (("d5d1", ("d5d2", "d5d3")),),
                (("d6d1", ("d6d2", "d6d3")),),
            ],
        )

    def test_bundle_nesting_unions(self):
        Data = self.classes.Data
        sess = fixture_session()

        b1 = Bundle("b1", Data.d1, Bundle("b2", Data.d2, Data.d3))

        q1 = (
            sess.query(b1)
            .filter(b1.c.d1.between("d3d1", "d7d1"))
            .filter(b1.c.b2.c.d2.between("d4d2", "d5d2"))
        )

        q2 = (
            sess.query(b1)
            .filter(b1.c.d1.between("d3d1", "d7d1"))
            .filter(b1.c.b2.c.d2.between("d5d2", "d6d2"))
        )

        eq_(
            q1.union(q2).all(),
            [
                (("d4d1", ("d4d2", "d4d3")),),
                (("d5d1", ("d5d2", "d5d3")),),
                (("d6d1", ("d6d2", "d6d3")),),
            ],
        )

        # naming structure is preserved
        row = q1.union(q2).first()
        eq_(row.b1.d1, "d4d1")
        eq_(row.b1.b2.d2, "d4d2")

    def test_query_count(self):
        Data = self.classes.Data
        b1 = Bundle("b1", Data.d1, Data.d2)
        eq_(fixture_session().query(b1).count(), 10)

    def test_join_relationship(self):
        Data = self.classes.Data

        sess = fixture_session()
        b1 = Bundle("b1", Data.d1, Data.d2)
        q = sess.query(b1).join(Data.others)
        self.assert_compile(
            q,
            "SELECT data.d1 AS data_d1, data.d2 "
            "AS data_d2 FROM data "
            "JOIN other ON data.id = other.data_id",
        )

    def test_join_selectable(self):
        Data = self.classes.Data
        Other = self.classes.Other

        sess = fixture_session()
        b1 = Bundle("b1", Data.d1, Data.d2)
        q = sess.query(b1).join(Other)
        self.assert_compile(
            q,
            "SELECT data.d1 AS data_d1, data.d2 AS data_d2 "
            "FROM data "
            "JOIN other ON data.id = other.data_id",
        )

    def test_joins_from_adapted_entities(self):
        Data = self.classes.Data

        # test for #1853 in terms of bundles
        # specifically this exercises adapt_to_selectable()

        b1 = Bundle("b1", Data.id, Data.d1, Data.d2)

        session = fixture_session()
        first = session.query(b1)
        second = session.query(b1)
        unioned = first.union(second)
        subquery = session.query(Data.id).subquery()
        joined = unioned.outerjoin(subquery, subquery.c.id == Data.id)
        joined = joined.order_by(Data.id, Data.d1, Data.d2)

        self.assert_compile(
            joined,
            "SELECT anon_1.data_id AS anon_1_data_id, "
            "anon_1.data_d1 AS anon_1_data_d1, "
            "anon_1.data_d2 AS anon_1_data_d2 FROM "
            "(SELECT data.id AS data_id, data.d1 AS data_d1, "
            "data.d2 AS data_d2 FROM "
            "data UNION SELECT data.id AS data_id, data.d1 AS data_d1, "
            "data.d2 AS data_d2 FROM data) AS anon_1 "
            "LEFT OUTER JOIN (SELECT data.id AS id FROM data) AS anon_2 "
            "ON anon_2.id = anon_1.data_id "
            "ORDER BY anon_1.data_id, anon_1.data_d1, anon_1.data_d2",
        )

        # tuple nesting still occurs
        eq_(
            joined.all(),
            [
                ((1, "d0d1", "d0d2"),),
                ((2, "d1d1", "d1d2"),),
                ((3, "d2d1", "d2d2"),),
                ((4, "d3d1", "d3d2"),),
                ((5, "d4d1", "d4d2"),),
                ((6, "d5d1", "d5d2"),),
                ((7, "d6d1", "d6d2"),),
                ((8, "d7d1", "d7d2"),),
                ((9, "d8d1", "d8d2"),),
                ((10, "d9d1", "d9d2"),),
            ],
        )

    def test_filter_by(self):
        Data = self.classes.Data

        b1 = Bundle("b1", Data.id, Data.d1, Data.d2)

        sess = fixture_session()

        self.assert_compile(
            sess.query(b1).filter_by(d1="d1"),
            "SELECT data.id AS data_id, data.d1 AS data_d1, "
            "data.d2 AS data_d2 FROM data WHERE data.d1 = :d1_1",
        )

    def test_clause_expansion(self):
        Data = self.classes.Data

        b1 = Bundle("b1", Data.id, Data.d1, Data.d2)

        sess = fixture_session()
        self.assert_compile(
            sess.query(Data).order_by(b1),
            "SELECT data.id AS data_id, data.d1 AS data_d1, "
            "data.d2 AS data_d2, data.d3 AS data_d3 FROM data "
            "ORDER BY data.id, data.d1, data.d2",
        )

        self.assert_compile(
            sess.query(func.row_number().over(order_by=b1)),
            "SELECT row_number() OVER (ORDER BY data.id, data.d1, data.d2) "
            "AS anon_1 FROM data",
        )

    def test_non_mapped_columns_non_single_entity(self):
        data_table = self.tables.data

        b1 = Bundle("b1", data_table.c.d1, data_table.c.d2)

        sess = fixture_session()
        eq_(
            sess.query(b1).filter(b1.c.d1.between("d3d1", "d5d1")).all(),
            [(("d3d1", "d3d2"),), (("d4d1", "d4d2"),), (("d5d1", "d5d2"),)],
        )

    def test_non_mapped_columns_single_entity(self):
        data_table = self.tables.data

        b1 = Bundle("b1", data_table.c.d1, data_table.c.d2, single_entity=True)

        sess = fixture_session()
        eq_(
            sess.query(b1).filter(b1.c.d1.between("d3d1", "d5d1")).all(),
            [("d3d1", "d3d2"), ("d4d1", "d4d2"), ("d5d1", "d5d2")],
        )
