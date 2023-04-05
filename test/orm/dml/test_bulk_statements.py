from __future__ import annotations

import contextlib
from typing import Any
from typing import List
from typing import Optional
from typing import Set
import uuid

from sqlalchemy import event
from sqlalchemy import exc
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import Identity
from sqlalchemy import insert
from sqlalchemy import inspect
from sqlalchemy import literal
from sqlalchemy import literal_column
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy import update
from sqlalchemy.orm import aliased
from sqlalchemy.orm import column_property
from sqlalchemy.orm import load_only
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import orm_insert_sentinel
from sqlalchemy.orm import Session
from sqlalchemy.testing import config
from sqlalchemy.testing import eq_
from sqlalchemy.testing import expect_raises_message
from sqlalchemy.testing import expect_warnings
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import mock
from sqlalchemy.testing import provision
from sqlalchemy.testing.assertsql import CompiledSQL
from sqlalchemy.testing.assertsql import Conditional
from sqlalchemy.testing.entities import ComparableEntity
from sqlalchemy.testing.fixtures import fixture_session


class InsertStmtTest(testing.AssertsExecutionResults, fixtures.TestBase):
    __backend__ = True

    @testing.variation(
        "style",
        [
            "no_executemany",
            ("no_sort_by", testing.requires.insert_returning),
            ("all_enabled", testing.requires.insert_returning),
        ],
    )
    @testing.variation("sort_by_parameter_order", [True, False])
    def test_no_returning_error(
        self,
        decl_base,
        testing_engine,
        style: testing.Variation,
        sort_by_parameter_order,
    ):
        class A(fixtures.ComparableEntity, decl_base):
            __tablename__ = "a"
            id: Mapped[int] = mapped_column(Identity(), primary_key=True)
            data: Mapped[str]
            x: Mapped[Optional[int]] = mapped_column("xcol")

        engine = testing_engine()

        if style.no_executemany:
            engine.dialect.use_insertmanyvalues = False
            engine.dialect.insert_executemany_returning = False
            engine.dialect.insert_executemany_returning_sort_by_parameter_order = (  # noqa: E501
                False
            )
        elif style.no_sort_by:
            engine.dialect.use_insertmanyvalues = True
            engine.dialect.insert_executemany_returning = True
            engine.dialect.insert_executemany_returning_sort_by_parameter_order = (  # noqa: E501
                False
            )
        elif style.all_enabled:
            engine.dialect.use_insertmanyvalues = True
            engine.dialect.insert_executemany_returning = True
            engine.dialect.insert_executemany_returning_sort_by_parameter_order = (  # noqa: E501
                True
            )
        else:
            style.fail()

        decl_base.metadata.create_all(engine)
        s = Session(engine)

        if style.all_enabled or (
            style.no_sort_by and not sort_by_parameter_order
        ):
            result = s.scalars(
                insert(A).returning(
                    A, sort_by_parameter_order=bool(sort_by_parameter_order)
                ),
                [
                    {"data": "d3", "x": 5},
                    {"data": "d4", "x": 6},
                ],
            )
            eq_(set(result.all()), {A(data="d3", x=5), A(data="d4", x=6)})

        else:
            with expect_raises_message(
                exc.InvalidRequestError,
                r"Can't use explicit RETURNING for bulk INSERT operation.*"
                rf"""executemany with RETURNING{
                    ' and sort by parameter order'
                    if sort_by_parameter_order else ''
                } is """
                r"not enabled for this dialect",
            ):
                s.scalars(
                    insert(A).returning(
                        A,
                        sort_by_parameter_order=bool(sort_by_parameter_order),
                    ),
                    [
                        {"data": "d3", "x": 5},
                        {"data": "d4", "x": 6},
                    ],
                )

    def test_omit_returning_ok(self, decl_base):
        class A(decl_base):
            __tablename__ = "a"
            id: Mapped[int] = mapped_column(Identity(), primary_key=True)
            data: Mapped[str]
            x: Mapped[Optional[int]] = mapped_column("xcol")

        decl_base.metadata.create_all(testing.db)
        s = fixture_session()

        s.execute(
            insert(A),
            [
                {"data": "d3", "x": 5},
                {"data": "d4", "x": 6},
            ],
        )
        eq_(
            s.execute(select(A.data, A.x).order_by(A.id)).all(),
            [("d3", 5), ("d4", 6)],
        )

    @testing.requires.insert_returning
    def test_insert_returning_cols_dont_give_me_defaults(self, decl_base):
        """test #9685"""

        class User(decl_base):
            __tablename__ = "users"

            id: Mapped[int] = mapped_column(Identity(), primary_key=True)

            name: Mapped[str] = mapped_column()
            other_thing: Mapped[Optional[str]]
            server_thing: Mapped[str] = mapped_column(server_default="thing")

        decl_base.metadata.create_all(testing.db)
        insert_stmt = insert(User).returning(User.id)

        s = fixture_session()

        with self.sql_execution_asserter() as asserter:
            result = s.execute(
                insert_stmt,
                [
                    {"name": "some name 1"},
                    {"name": "some name 2"},
                    {"name": "some name 3"},
                ],
            )

        eq_(result.all(), [(1,), (2,), (3,)])

        asserter.assert_(
            CompiledSQL(
                "INSERT INTO users (name) VALUES (:name) "
                "RETURNING users.id",
                [
                    {"name": "some name 1"},
                    {"name": "some name 2"},
                    {"name": "some name 3"},
                ],
            ),
        )

    @testing.requires.insert_returning
    @testing.skip_if(
        "oracle", "oracle doesn't like the no-FROM SELECT inside of an INSERT"
    )
    def test_insert_from_select_col_property(self, decl_base):
        """test #9273"""

        class User(ComparableEntity, decl_base):
            __tablename__ = "users"

            id: Mapped[int] = mapped_column(primary_key=True)

            name: Mapped[str] = mapped_column()
            age: Mapped[int] = mapped_column()

            is_adult: Mapped[bool] = column_property(age >= 18)

        decl_base.metadata.create_all(testing.db)

        stmt = select(
            literal(1).label("id"),
            literal("John").label("name"),
            literal(30).label("age"),
        )

        insert_stmt = (
            insert(User)
            .from_select(["id", "name", "age"], stmt)
            .returning(User)
        )

        s = fixture_session()
        result = s.scalars(insert_stmt)

        eq_(result.all(), [User(id=1, name="John", age=30)])


class BulkDMLReturningInhTest:
    use_sentinel = False
    randomize_returning = False

    def assert_for_downgrade(self, *, sort_by_parameter_order):
        if (
            not sort_by_parameter_order
            or not self.randomize_returning
            or not testing.against(["postgresql", "mssql", "mariadb"])
        ):
            return contextlib.nullcontext()
        else:
            return expect_warnings("Batches were downgraded")

    @classmethod
    def setup_bind(cls):
        if cls.randomize_returning:
            new_eng = config.db.execution_options()

            @event.listens_for(new_eng, "engine_connect")
            def eng_connect(connection):
                fixtures.insertmanyvalues_fixture(
                    connection,
                    randomize_rows=True,
                    # there should be no sentinel downgrades for any of
                    # these three dbs.  sqlite has downgrades
                    warn_on_downgraded=testing.against(
                        ["postgresql", "mssql", "mariadb"]
                    ),
                )

            return new_eng
        else:
            return config.db

    def test_insert_col_key_also_works_currently(self):
        """using the column key, not mapped attr key.

        right now this passes through to the INSERT.  when doing this with
        an UPDATE, it tends to fail because the synchronize session
        strategies can't match "xcol" back.  however w/ INSERT we aren't
        doing that, so there's no place this gets checked.   UPDATE also
        succeeds if synchronize_session is turned off.

        """
        A, B = self.classes("A", "B")

        s = fixture_session(bind=self.bind)
        s.execute(insert(A).values(type="a", data="d", xcol=10))
        eq_(s.scalars(select(A.x)).all(), [10])

    @testing.combinations("default", "session_disable", "opt_disable")
    def test_autoflush(self, autoflush_option):
        A = self.classes.A

        s = fixture_session(bind=self.bind)

        a1 = A(data="x1")
        s.add(a1)

        if autoflush_option == "default":
            s.execute(insert(A).values(type="a", data="x2"))
            assert inspect(a1).persistent
            eq_(s.scalars(select(A.data).order_by(A.id)).all(), ["x1", "x2"])
        elif autoflush_option == "session_disable":
            with s.no_autoflush:
                s.execute(insert(A).values(type="a", data="x2"))
                assert inspect(a1).pending
                eq_(s.scalars(select(A.data).order_by(A.id)).all(), ["x2"])
        elif autoflush_option == "opt_disable":
            s.execute(
                insert(A).values(type="a", data="x2"),
                execution_options={"autoflush": False},
            )
            assert inspect(a1).pending
            with s.no_autoflush:
                eq_(s.scalars(select(A.data).order_by(A.id)).all(), ["x2"])
        else:
            assert False

    @testing.variation("use_returning", [True, False])
    @testing.variation("sort_by_parameter_order", [True, False])
    def test_heterogeneous_keys(self, use_returning, sort_by_parameter_order):
        A, B = self.classes("A", "B")

        values = [
            {"data": "d3", "x": 5, "type": "a"},
            {"data": "d4", "x": 6, "type": "a"},
            {"data": "d5", "type": "a"},
            {"data": "d6", "x": 8, "y": 9, "type": "a"},
            {"data": "d7", "x": 12, "y": 12, "type": "a"},
            {"data": "d8", "x": 7, "type": "a"},
        ]

        s = fixture_session(bind=self.bind)

        stmt = insert(A)
        if use_returning:
            stmt = stmt.returning(
                A, sort_by_parameter_order=bool(sort_by_parameter_order)
            )

        with self.sql_execution_asserter() as asserter:
            result = s.execute(stmt, values)

        if use_returning:
            if self.use_sentinel and sort_by_parameter_order:
                _sentinel_col = ", _sentinel"
                _sentinel_returning = ", a._sentinel"
                _sentinel_param = ", :_sentinel"
            else:
                _sentinel_col = _sentinel_param = _sentinel_returning = ""
            # note no sentinel col is used when there is only one row
            asserter.assert_(
                CompiledSQL(
                    f"INSERT INTO a (type, data, xcol{_sentinel_col}) VALUES "
                    f"(:type, :data, :xcol{_sentinel_param}) "
                    f"RETURNING a.id, a.type, a.data, a.xcol, a.y"
                    f"{_sentinel_returning}",
                    [
                        {"type": "a", "data": "d3", "xcol": 5},
                        {"type": "a", "data": "d4", "xcol": 6},
                    ],
                ),
                CompiledSQL(
                    "INSERT INTO a (type, data) VALUES (:type, :data) "
                    "RETURNING a.id, a.type, a.data, a.xcol, a.y",
                    [{"type": "a", "data": "d5"}],
                ),
                CompiledSQL(
                    f"INSERT INTO a (type, data, xcol, y{_sentinel_col}) "
                    f"VALUES (:type, :data, :xcol, :y{_sentinel_param}) "
                    f"RETURNING a.id, a.type, a.data, a.xcol, a.y"
                    f"{_sentinel_returning}",
                    [
                        {"type": "a", "data": "d6", "xcol": 8, "y": 9},
                        {"type": "a", "data": "d7", "xcol": 12, "y": 12},
                    ],
                ),
                CompiledSQL(
                    "INSERT INTO a (type, data, xcol) "
                    "VALUES (:type, :data, :xcol) "
                    "RETURNING a.id, a.type, a.data, a.xcol, a.y",
                    [{"type": "a", "data": "d8", "xcol": 7}],
                ),
            )
        else:
            asserter.assert_(
                CompiledSQL(
                    "INSERT INTO a (type, data, xcol) VALUES "
                    "(:type, :data, :xcol)",
                    [
                        {"type": "a", "data": "d3", "xcol": 5},
                        {"type": "a", "data": "d4", "xcol": 6},
                    ],
                ),
                CompiledSQL(
                    "INSERT INTO a (type, data) VALUES (:type, :data)",
                    [{"type": "a", "data": "d5"}],
                ),
                CompiledSQL(
                    "INSERT INTO a (type, data, xcol, y) "
                    "VALUES (:type, :data, :xcol, :y)",
                    [
                        {"type": "a", "data": "d6", "xcol": 8, "y": 9},
                        {"type": "a", "data": "d7", "xcol": 12, "y": 12},
                    ],
                ),
                CompiledSQL(
                    "INSERT INTO a (type, data, xcol) "
                    "VALUES (:type, :data, :xcol)",
                    [{"type": "a", "data": "d8", "xcol": 7}],
                ),
            )

        if use_returning:
            with self.assert_statement_count(testing.db, 0):
                eq_(
                    set(result.scalars().all()),
                    {
                        A(data="d3", id=mock.ANY, type="a", x=5, y=None),
                        A(data="d4", id=mock.ANY, type="a", x=6, y=None),
                        A(data="d5", id=mock.ANY, type="a", x=None, y=None),
                        A(data="d6", id=mock.ANY, type="a", x=8, y=9),
                        A(data="d7", id=mock.ANY, type="a", x=12, y=12),
                        A(data="d8", id=mock.ANY, type="a", x=7, y=None),
                    },
                )

    @testing.combinations(
        "strings",
        "cols",
        "strings_w_exprs",
        "cols_w_exprs",
        argnames="paramstyle",
    )
    @testing.variation(
        "single_element", [True, (False, testing.requires.multivalues_inserts)]
    )
    def test_single_values_returning_fn(self, paramstyle, single_element):
        """test using insert().values().

        these INSERT statements go straight in as a single execute without any
        insertmanyreturning or bulk_insert_mappings thing going on.  the
        advantage here is that SQL expressions can be used in the values also.
        Disadvantage is none of the automation for inheritance mappers.

        """
        A, B = self.classes("A", "B")

        if paramstyle == "strings":
            values = [
                {"data": "d3", "x": 5, "y": 9, "type": "a"},
                {"data": "d4", "x": 10, "y": 8, "type": "a"},
            ]
        elif paramstyle == "cols":
            values = [
                {A.data: "d3", A.x: 5, A.y: 9, A.type: "a"},
                {A.data: "d4", A.x: 10, A.y: 8, A.type: "a"},
            ]
        elif paramstyle == "strings_w_exprs":
            values = [
                {"data": func.lower("D3"), "x": 5, "y": 9, "type": "a"},
                {
                    "data": "d4",
                    "x": literal_column("5") + 5,
                    "y": 8,
                    "type": "a",
                },
            ]
        elif paramstyle == "cols_w_exprs":
            values = [
                {A.data: func.lower("D3"), A.x: 5, A.y: 9, A.type: "a"},
                {
                    A.data: "d4",
                    A.x: literal_column("5") + 5,
                    A.y: 8,
                    A.type: "a",
                },
            ]
        else:
            assert False

        s = fixture_session(bind=self.bind)

        if single_element:
            if paramstyle.startswith("strings"):
                stmt = (
                    insert(A)
                    .values(**values[0])
                    .returning(A, func.upper(A.data, type_=String))
                )
            else:
                stmt = (
                    insert(A)
                    .values(values[0])
                    .returning(A, func.upper(A.data, type_=String))
                )
        else:
            stmt = (
                insert(A)
                .values(values)
                .returning(A, func.upper(A.data, type_=String))
            )

        for i in range(3):
            result = s.execute(stmt)
            expected: List[Any] = [(A(data="d3", x=5, y=9), "D3")]
            if not single_element:
                expected.append((A(data="d4", x=10, y=8), "D4"))
            eq_(result.all(), expected)

    def test_bulk_w_sql_expressions(self):
        A, B = self.classes("A", "B")

        data = [
            {"x": 5, "y": 9, "type": "a"},
            {
                "x": 10,
                "y": 8,
                "type": "a",
            },
        ]

        s = fixture_session(bind=self.bind)

        stmt = (
            insert(A)
            .values(data=func.lower("DD"))
            .returning(A, func.upper(A.data, type_=String))
        )

        for i in range(3):
            result = s.execute(stmt, data)
            expected: Set[Any] = {
                (A(data="dd", x=5, y=9), "DD"),
                (A(data="dd", x=10, y=8), "DD"),
            }
            eq_(set(result.all()), expected)

    def test_bulk_w_sql_expressions_subclass(self):
        A, B = self.classes("A", "B")

        data = [
            {"bd": "bd1", "x": 1, "y": 2, "z": 3, "q": 4},
            {"bd": "bd2", "x": 5, "y": 6, "z": 7, "q": 8},
        ]

        s = fixture_session(bind=self.bind)

        stmt = (
            insert(B)
            .values(data=func.lower("DD"))
            .returning(B, func.upper(B.data, type_=String))
        )

        for i in range(3):
            result = s.execute(stmt, data)
            expected: Set[Any] = {
                (B(bd="bd1", data="dd", q=4, type="b", x=1, y=2, z=3), "DD"),
                (B(bd="bd2", data="dd", q=8, type="b", x=5, y=6, z=7), "DD"),
            }
            eq_(set(result), expected)

    @testing.combinations(True, False, argnames="use_ordered")
    def test_bulk_upd_w_sql_expressions_no_ordered_values(self, use_ordered):
        A, B = self.classes("A", "B")

        s = fixture_session(bind=self.bind)

        stmt = update(B).ordered_values(
            ("data", func.lower("DD_UPDATE")),
            ("z", literal_column("3 + 12")),
        )
        with expect_raises_message(
            exc.InvalidRequestError,
            r"bulk ORM UPDATE does not support ordered_values\(\) "
            r"for custom UPDATE",
        ):
            s.execute(
                stmt,
                [
                    {"id": 5, "bd": "bd1_updated"},
                    {"id": 6, "bd": "bd2_updated"},
                ],
            )

    def test_bulk_upd_w_sql_expressions_subclass(self):
        A, B = self.classes("A", "B")

        s = fixture_session(bind=self.bind)

        data = [
            {"data": "d3", "bd": "bd1", "x": 1, "y": 2, "z": 3, "q": 4},
            {"data": "d4", "bd": "bd2", "x": 5, "y": 6, "z": 7, "q": 8},
        ]
        ids = {
            row.data: row.id
            for row in s.execute(insert(B).returning(B.id, B.data), data)
        }

        stmt = update(B).values(
            data=func.lower("DD_UPDATE"), z=literal_column("3 + 12")
        )

        result = s.execute(
            stmt,
            [
                {"id": ids["d3"], "bd": "bd1_updated"},
                {"id": ids["d4"], "bd": "bd2_updated"},
            ],
        )

        # this is a nullresult at the moment
        assert result is not None

        eq_(
            set(s.scalars(select(B))),
            {
                B(
                    bd="bd1_updated",
                    data="dd_update",
                    id=ids["d3"],
                    q=4,
                    type="b",
                    x=1,
                    y=2,
                    z=15,
                ),
                B(
                    bd="bd2_updated",
                    data="dd_update",
                    id=ids["d4"],
                    q=8,
                    type="b",
                    x=5,
                    y=6,
                    z=15,
                ),
            },
        )

    def test_single_returning_fn(self):
        A, B = self.classes("A", "B")

        s = fixture_session(bind=self.bind)
        for i in range(3):
            result = s.execute(
                insert(A).returning(A, func.upper(A.data, type_=String)),
                [{"data": "d3"}, {"data": "d4"}],
            )
            eq_(set(result), {(A(data="d3"), "D3"), (A(data="d4"), "D4")})

    @testing.variation("single_element", [True, False])
    def test_subclass_no_returning(self, single_element):
        A, B = self.classes("A", "B")

        s = fixture_session(bind=self.bind)

        if single_element:
            data = {"data": "d3", "bd": "bd1", "x": 1, "y": 2, "z": 3, "q": 4}
        else:
            data = [
                {"data": "d3", "bd": "bd1", "x": 1, "y": 2, "z": 3, "q": 4},
                {"data": "d4", "bd": "bd2", "x": 5, "y": 6, "z": 7, "q": 8},
            ]

        result = s.execute(insert(B), data)
        assert result._soft_closed

    @testing.variation("sort_by_parameter_order", [True, False])
    @testing.variation("single_element", [True, False])
    def test_subclass_load_only(self, single_element, sort_by_parameter_order):
        """test that load_only() prevents additional attributes from being
        populated.

        """
        A, B = self.classes("A", "B")

        s = fixture_session(bind=self.bind)

        if single_element:
            data = {"data": "d3", "bd": "bd1", "x": 1, "y": 2, "z": 3, "q": 4}
        else:
            data = [
                {"data": "d3", "bd": "bd1", "x": 1, "y": 2, "z": 3, "q": 4},
                {"data": "d4", "bd": "bd2", "x": 5, "y": 6, "z": 7, "q": 8},
            ]

        for i in range(3):
            # tests both caching and that the data dictionaries aren't
            # mutated...
            result = s.execute(
                insert(B)
                .returning(
                    B,
                    sort_by_parameter_order=bool(sort_by_parameter_order),
                )
                .options(load_only(B.data, B.y, B.q)),
                data,
            )
            objects = result.scalars().all()
            for obj in objects:
                assert "data" in obj.__dict__
                assert "q" in obj.__dict__
                assert "z" not in obj.__dict__
                assert "x" not in obj.__dict__

            expected = [
                B(data="d3", bd="bd1", x=1, y=2, z=3, q=4),
            ]
            if not single_element:
                expected.append(B(data="d4", bd="bd2", x=5, y=6, z=7, q=8))

            if sort_by_parameter_order:
                coll = list
            else:
                coll = set
            eq_(coll(objects), coll(expected))

    @testing.variation("single_element", [True, False])
    def test_subclass_load_only_doesnt_fetch_cols(self, single_element):
        """test that when using load_only(), the actual INSERT statement
        does not include the deferred columns

        """
        A, B = self.classes("A", "B")

        s = fixture_session(bind=self.bind)

        data = [
            {"data": "d3", "bd": "bd1", "x": 1, "y": 2, "z": 3, "q": 4},
            {"data": "d4", "bd": "bd2", "x": 5, "y": 6, "z": 7, "q": 8},
        ]
        if single_element:
            data = data[0]

        with self.sql_execution_asserter() as asserter:

            # tests both caching and that the data dictionaries aren't
            # mutated...

            # note that if we don't put B.id here, accessing .id on the
            # B object for joined inheritance is triggering a SELECT
            # (and not for single inheritance). this seems not great, but is
            # likely a different issue
            result = s.execute(
                insert(B)
                .returning(B)
                .options(load_only(B.id, B.data, B.y, B.q)),
                data,
            )
            objects = result.scalars().all()
            if single_element:
                id0 = objects[0].id
                id1 = None
            else:
                id0, id1 = objects[0].id, objects[1].id

        if inspect(B).single or inspect(B).concrete:
            expected_params = [
                {
                    "type": "b",
                    "data": "d3",
                    "xcol": 1,
                    "y": 2,
                    "bd": "bd1",
                    "zcol": 3,
                    "q": 4,
                },
                {
                    "type": "b",
                    "data": "d4",
                    "xcol": 5,
                    "y": 6,
                    "bd": "bd2",
                    "zcol": 7,
                    "q": 8,
                },
            ]
            if single_element:
                expected_params[1:] = []
            # RETURNING only includes PK, discriminator, then the cols
            # we asked for data, y, q.  xcol, z, bd are omitted

            if inspect(B).single:
                asserter.assert_(
                    CompiledSQL(
                        "INSERT INTO a (type, data, xcol, y, bd, zcol, q) "
                        "VALUES "
                        "(:type, :data, :xcol, :y, :bd, :zcol, :q) "
                        "RETURNING a.id, a.type, a.data, a.y, a.q",
                        expected_params,
                    ),
                )
            else:
                asserter.assert_(
                    CompiledSQL(
                        "INSERT INTO b (type, data, xcol, y, bd, zcol, q) "
                        "VALUES "
                        "(:type, :data, :xcol, :y, :bd, :zcol, :q) "
                        "RETURNING b.id, b.type, b.data, b.y, b.q",
                        expected_params,
                    ),
                )
        else:
            a_data = [
                {"type": "b", "data": "d3", "xcol": 1, "y": 2},
                {"type": "b", "data": "d4", "xcol": 5, "y": 6},
            ]
            b_data = [
                {"id": id0, "bd": "bd1", "zcol": 3, "q": 4},
                {"id": id1, "bd": "bd2", "zcol": 7, "q": 8},
            ]
            if single_element:
                a_data[1:] = []
                b_data[1:] = []
            # RETURNING only includes PK, discriminator, then the cols
            # we asked for data, y, q.  xcol, z, bd are omitted.  plus they
            # are broken out correctly in the two statements.

            asserter.assert_(
                Conditional(
                    self.use_sentinel and not single_element,
                    [
                        CompiledSQL(
                            "INSERT INTO a (type, data, xcol, y, _sentinel) "
                            "VALUES "
                            "(:type, :data, :xcol, :y, :_sentinel) "
                            "RETURNING a.id, a.type, a.data, a.y, a._sentinel",
                            a_data,
                        ),
                        CompiledSQL(
                            "INSERT INTO b (id, bd, zcol, q, _sentinel) "
                            "VALUES (:id, :bd, :zcol, :q, :_sentinel) "
                            "RETURNING b.id, b.q, b._sentinel",
                            b_data,
                        ),
                    ],
                    [
                        CompiledSQL(
                            "INSERT INTO a (type, data, xcol, y) VALUES "
                            "(:type, :data, :xcol, :y) "
                            "RETURNING a.id, a.type, a.data, a.y",
                            a_data,
                        ),
                        Conditional(
                            single_element,
                            [
                                CompiledSQL(
                                    "INSERT INTO b (id, bd, zcol, q) "
                                    "VALUES (:id, :bd, :zcol, :q) "
                                    "RETURNING b.id, b.q",
                                    b_data,
                                ),
                            ],
                            [
                                CompiledSQL(
                                    "INSERT INTO b (id, bd, zcol, q) "
                                    "VALUES (:id, :bd, :zcol, :q) "
                                    "RETURNING b.id, b.q, b.id AS id__1",
                                    b_data,
                                ),
                            ],
                        ),
                    ],
                )
            )

    @testing.variation("single_element", [True, False])
    def test_subclass_returning_bind_expr(self, single_element):
        A, B = self.classes("A", "B")

        s = fixture_session(bind=self.bind)

        if single_element:
            data = {"data": "d3", "bd": "bd1", "x": 1, "y": 2, "z": 3, "q": 4}
        else:
            data = [
                {"data": "d3", "bd": "bd1", "x": 1, "y": 2, "z": 3, "q": 4},
                {"data": "d4", "bd": "bd2", "x": 5, "y": 6, "z": 7, "q": 8},
            ]
        # note there's a fix in compiler.py ->
        # _deliver_insertmanyvalues_batches
        # for this re: the parameter rendering that isn't tested anywhere
        # else.  two different versions of the bug for both positional
        # and non
        result = s.execute(insert(B).returning(B.data, B.y, B.q + 5), data)
        if single_element:
            eq_(result.all(), [("d3", 2, 9)])
        else:
            eq_(set(result), {("d3", 2, 9), ("d4", 6, 13)})

    def test_subclass_bulk_update(self):
        A, B = self.classes("A", "B")

        s = fixture_session(bind=self.bind)

        data = [
            {"data": "d3", "bd": "bd1", "x": 1, "y": 2, "z": 3, "q": 4},
            {"data": "d4", "bd": "bd2", "x": 5, "y": 6, "z": 7, "q": 8},
        ]
        ids = {
            row.data: row.id
            for row in s.execute(insert(B).returning(B.id, B.data), data).all()
        }

        result = s.execute(
            update(B),
            [
                {"id": ids["d3"], "data": "d3_updated", "bd": "bd1_updated"},
                {"id": ids["d4"], "data": "d4_updated", "bd": "bd2_updated"},
            ],
        )

        # this is a nullresult at the moment
        assert result is not None

        eq_(
            set(s.scalars(select(B))),
            {
                B(
                    bd="bd1_updated",
                    data="d3_updated",
                    id=ids["d3"],
                    q=4,
                    type="b",
                    x=1,
                    y=2,
                    z=3,
                ),
                B(
                    bd="bd2_updated",
                    data="d4_updated",
                    id=ids["d4"],
                    q=8,
                    type="b",
                    x=5,
                    y=6,
                    z=7,
                ),
            },
        )

    @testing.variation("single_element", [True, False])
    @testing.variation("sort_by_parameter_order", [True, False])
    def test_subclass_return_just_subclass_ids(
        self, single_element, sort_by_parameter_order
    ):
        A, B = self.classes("A", "B")

        s = fixture_session(bind=self.bind)

        if single_element:
            data = {"data": "d3", "bd": "bd1", "x": 1, "y": 2, "z": 3, "q": 4}
        else:
            data = [
                {"data": "d3", "bd": "bd1", "x": 1, "y": 2, "z": 3, "q": 4},
                {"data": "d4", "bd": "bd2", "x": 5, "y": 6, "z": 7, "q": 8},
            ]

        ids = s.execute(
            insert(B).returning(
                B.id,
                B.data,
                sort_by_parameter_order=bool(sort_by_parameter_order),
            ),
            data,
        )
        actual_ids = s.execute(select(B.id, B.data).order_by(B.id))

        if sort_by_parameter_order:
            coll = list
        else:
            coll = set

        eq_(coll(ids), coll(actual_ids))

    @testing.variation("insert_strategy", ["orm", "bulk", "bulk_ordered"])
    @testing.requires.provisioned_upsert
    def test_base_class_upsert(self, insert_strategy):
        """upsert is really tricky.   if you dont have any data updated,
        then you dont get the rows back and things dont work so well.

        so we need to be careful how much we document this because this is
        still a thorny use case.

        """
        A = self.classes.A

        s = fixture_session(bind=self.bind)

        initial_data = [
            {"data": "d3", "x": 1, "y": 2, "q": 4},
            {"data": "d4", "x": 5, "y": 6, "q": 8},
        ]
        ids = {
            row.data: row.id
            for row in s.execute(
                insert(A).returning(A.id, A.data), initial_data
            )
        }

        upsert_data = [
            {
                "id": ids["d3"],
                "type": "a",
                "data": "d3",
                "x": 1,
                "y": 2,
            },
            {
                "id": 32,
                "type": "a",
                "data": "d32",
                "x": 19,
                "y": 5,
            },
            {
                "id": ids["d4"],
                "type": "a",
                "data": "d4",
                "x": 5,
                "y": 6,
            },
            {
                "id": 28,
                "type": "a",
                "data": "d28",
                "x": 9,
                "y": 15,
            },
        ]

        stmt = provision.upsert(
            config,
            A,
            (A,),
            set_lambda=lambda inserted: {"data": inserted.data + " upserted"},
            sort_by_parameter_order=insert_strategy.bulk_ordered,
        )

        if insert_strategy.orm:
            result = s.scalars(stmt.values(upsert_data))
        elif insert_strategy.bulk or insert_strategy.bulk_ordered:
            with self.assert_for_downgrade(
                sort_by_parameter_order=insert_strategy.bulk_ordered
            ):
                result = s.scalars(stmt, upsert_data)
        else:
            insert_strategy.fail()

        eq_(
            set(result.all()),
            {
                A(data="d3 upserted", id=ids["d3"], type="a", x=1, y=2),
                A(data="d32", id=32, type="a", x=19, y=5),
                A(data="d4 upserted", id=ids["d4"], type="a", x=5, y=6),
                A(data="d28", id=28, type="a", x=9, y=15),
            },
        )

    @testing.combinations(
        "orm",
        "bulk",
        argnames="insert_strategy",
    )
    @testing.variation("sort_by_parameter_order", [True, False])
    @testing.requires.provisioned_upsert
    def test_subclass_upsert(self, insert_strategy, sort_by_parameter_order):
        """note this is overridden in the joined version to expect failure"""

        A, B = self.classes("A", "B")

        s = fixture_session(bind=self.bind)

        idd3 = 1
        idd4 = 2
        id32 = 32
        id28 = 28

        initial_data = [
            {
                "id": idd3,
                "data": "d3",
                "bd": "bd1",
                "x": 1,
                "y": 2,
                "z": 3,
                "q": 4,
            },
            {
                "id": idd4,
                "data": "d4",
                "bd": "bd2",
                "x": 5,
                "y": 6,
                "z": 7,
                "q": 8,
            },
        ]
        ids = {
            row.data: row.id
            for row in s.execute(
                insert(B).returning(
                    B.id, B.data, sort_by_parameter_order=True
                ),
                initial_data,
            )
        }

        upsert_data = [
            {
                "id": ids["d3"],
                "type": "b",
                "data": "d3",
                "bd": "bd1_upserted",
                "x": 1,
                "y": 2,
                "z": 33,
                "q": 44,
            },
            {
                "id": id32,
                "type": "b",
                "data": "d32",
                "bd": "bd 32",
                "x": 19,
                "y": 5,
                "z": 20,
                "q": 21,
            },
            {
                "id": ids["d4"],
                "type": "b",
                "bd": "bd2_upserted",
                "data": "d4",
                "x": 5,
                "y": 6,
                "z": 77,
                "q": 88,
            },
            {
                "id": id28,
                "type": "b",
                "data": "d28",
                "bd": "bd 28",
                "x": 9,
                "y": 15,
                "z": 10,
                "q": 11,
            },
        ]

        stmt = provision.upsert(
            config,
            B,
            (B,),
            set_lambda=lambda inserted: {
                "data": inserted.data + " upserted",
                "bd": inserted.bd + " upserted",
            },
            sort_by_parameter_order=bool(sort_by_parameter_order),
        )

        with self.assert_for_downgrade(
            sort_by_parameter_order=bool(sort_by_parameter_order)
        ):
            result = s.scalars(stmt, upsert_data)
        eq_(
            set(result),
            {
                B(
                    bd="bd1_upserted upserted",
                    data="d3 upserted",
                    id=ids["d3"],
                    q=4,
                    type="b",
                    x=1,
                    y=2,
                    z=3,
                ),
                B(
                    bd="bd 32",
                    data="d32",
                    id=32,
                    q=21,
                    type="b",
                    x=19,
                    y=5,
                    z=20,
                ),
                B(
                    bd="bd2_upserted upserted",
                    data="d4 upserted",
                    id=ids["d4"],
                    q=8,
                    type="b",
                    x=5,
                    y=6,
                    z=7,
                ),
                B(
                    bd="bd 28",
                    data="d28",
                    id=28,
                    q=11,
                    type="b",
                    x=9,
                    y=15,
                    z=10,
                ),
            },
        )


@testing.combinations(
    (
        "no_sentinel",
        False,
    ),
    (
        "w_sentinel",
        True,
    ),
    argnames="use_sentinel",
    id_="ia",
)
@testing.combinations(
    (
        "nonrandom",
        False,
    ),
    (
        "random",
        True,
    ),
    argnames="randomize_returning",
    id_="ia",
)
class BulkDMLReturningJoinedInhTest(
    BulkDMLReturningInhTest, fixtures.DeclarativeMappedTest
):

    __requires__ = ("insert_returning", "insert_executemany_returning")
    __backend__ = True

    use_sentinel = False
    randomize_returning = False

    @classmethod
    def setup_classes(cls):
        decl_base = cls.DeclarativeBasic

        class A(fixtures.ComparableEntity, decl_base):
            __tablename__ = "a"
            id: Mapped[int] = mapped_column(Identity(), primary_key=True)
            type: Mapped[str]
            data: Mapped[str]
            x: Mapped[Optional[int]] = mapped_column("xcol")
            y: Mapped[Optional[int]]

            if cls.use_sentinel:
                _sentinel: Mapped[int] = orm_insert_sentinel()

            __mapper_args__ = {
                "polymorphic_identity": "a",
                "polymorphic_on": "type",
            }

        class B(A):
            __tablename__ = "b"
            id: Mapped[int] = mapped_column(
                ForeignKey("a.id"), primary_key=True
            )
            bd: Mapped[str]
            z: Mapped[Optional[int]] = mapped_column("zcol")
            q: Mapped[Optional[int]]

            if cls.use_sentinel:
                _sentinel: Mapped[int] = orm_insert_sentinel()

            __mapper_args__ = {"polymorphic_identity": "b"}

    @testing.combinations(
        "orm",
        "bulk",
        argnames="insert_strategy",
    )
    @testing.combinations(
        True,
        False,
        argnames="single_param",
    )
    @testing.variation("sort_by_parameter_order", [True, False])
    @testing.requires.provisioned_upsert
    def test_subclass_upsert(
        self,
        insert_strategy,
        single_param,
        sort_by_parameter_order,
    ):
        A, B = self.classes("A", "B")

        s = fixture_session(bind=self.bind)

        initial_data = [
            {"data": "d3", "bd": "bd1", "x": 1, "y": 2, "z": 3, "q": 4},
            {"data": "d4", "bd": "bd2", "x": 5, "y": 6, "z": 7, "q": 8},
        ]
        ids = s.scalars(
            insert(B).returning(B.id, sort_by_parameter_order=True),
            initial_data,
        ).all()

        upsert_data = [
            {
                "id": ids[0],
                "type": "b",
            },
            {
                "id": 32,
                "type": "b",
            },
        ]
        if single_param:
            upsert_data = upsert_data[0]

        stmt = provision.upsert(
            config,
            B,
            (B,),
            set_lambda=lambda inserted: {
                "bd": inserted.bd + " upserted",
            },
            sort_by_parameter_order=bool(sort_by_parameter_order),
        )

        with expect_raises_message(
            exc.InvalidRequestError,
            r"bulk INSERT with a 'post values' clause \(typically upsert\) "
            r"not supported for multi-table mapper",
        ):
            s.scalars(stmt, upsert_data)


@testing.combinations(
    (
        "nonrandom",
        False,
    ),
    (
        "random",
        True,
    ),
    argnames="randomize_returning",
    id_="ia",
)
class BulkDMLReturningSingleInhTest(
    BulkDMLReturningInhTest, fixtures.DeclarativeMappedTest
):
    __requires__ = ("insert_returning", "insert_executemany_returning")
    __backend__ = True

    @classmethod
    def setup_classes(cls):
        decl_base = cls.DeclarativeBasic

        class A(fixtures.ComparableEntity, decl_base):
            __tablename__ = "a"
            id: Mapped[int] = mapped_column(Identity(), primary_key=True)
            type: Mapped[str]
            data: Mapped[str]
            x: Mapped[Optional[int]] = mapped_column("xcol")
            y: Mapped[Optional[int]]

            __mapper_args__ = {
                "polymorphic_identity": "a",
                "polymorphic_on": "type",
            }

        class B(A):
            bd: Mapped[str] = mapped_column(nullable=True)
            z: Mapped[Optional[int]] = mapped_column("zcol")
            q: Mapped[Optional[int]]

            __mapper_args__ = {"polymorphic_identity": "b"}


@testing.combinations(
    (
        "nonrandom",
        False,
    ),
    (
        "random",
        True,
    ),
    argnames="randomize_returning",
    id_="ia",
)
class BulkDMLReturningConcreteInhTest(
    BulkDMLReturningInhTest, fixtures.DeclarativeMappedTest
):
    __requires__ = ("insert_returning", "insert_executemany_returning")
    __backend__ = True

    @classmethod
    def setup_classes(cls):
        decl_base = cls.DeclarativeBasic

        class A(fixtures.ComparableEntity, decl_base):
            __tablename__ = "a"
            id: Mapped[int] = mapped_column(Identity(), primary_key=True)
            type: Mapped[str]
            data: Mapped[str]
            x: Mapped[Optional[int]] = mapped_column("xcol")
            y: Mapped[Optional[int]]

            __mapper_args__ = {
                "polymorphic_identity": "a",
                "polymorphic_on": "type",
            }

        class B(A):
            __tablename__ = "b"
            id: Mapped[int] = mapped_column(Identity(), primary_key=True)
            type: Mapped[str]
            data: Mapped[str]
            x: Mapped[Optional[int]] = mapped_column("xcol")
            y: Mapped[Optional[int]]

            bd: Mapped[str] = mapped_column(nullable=True)
            z: Mapped[Optional[int]] = mapped_column("zcol")
            q: Mapped[Optional[int]]

            __mapper_args__ = {
                "polymorphic_identity": "b",
                "concrete": True,
                "polymorphic_on": "type",
            }


class CTETest(fixtures.DeclarativeMappedTest):
    __requires__ = ("insert_returning", "ctes_on_dml")
    __backend__ = True

    @classmethod
    def setup_classes(cls):
        decl_base = cls.DeclarativeBasic

        class User(fixtures.ComparableEntity, decl_base):
            __tablename__ = "users"
            id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
            username: Mapped[str]

    @testing.combinations(
        ("cte_aliased", True),
        ("cte", False),
        argnames="wrap_cte_in_aliased",
        id_="ia",
    )
    @testing.combinations(
        ("use_union", True),
        ("no_union", False),
        argnames="use_a_union",
        id_="ia",
    )
    @testing.combinations(
        "from_statement", "aliased", "direct", argnames="fetch_entity_type"
    )
    def test_select_from_insert_cte(
        self, wrap_cte_in_aliased, use_a_union, fetch_entity_type
    ):
        """test the use case from #8544; SELECT that selects from a
        CTE INSERT...RETURNING.

        """
        User = self.classes.User

        id_ = uuid.uuid4()

        cte = (
            insert(User)
            .values(id=id_, username="some user")
            .returning(User)
            .cte()
        )
        if wrap_cte_in_aliased:
            cte = aliased(User, cte)

        if use_a_union:
            stmt = select(User).where(User.id == id_).union(select(cte))
        else:
            stmt = select(cte)

        if fetch_entity_type == "from_statement":
            outer_stmt = select(User).from_statement(stmt)
            expect_entity = True
        elif fetch_entity_type == "aliased":
            outer_stmt = select(aliased(User, stmt.subquery()))
            expect_entity = True
        elif fetch_entity_type == "direct":
            outer_stmt = stmt
            expect_entity = not use_a_union and wrap_cte_in_aliased
        else:
            assert False

        sess = fixture_session(bind=self.bind)
        with self.sql_execution_asserter() as asserter:

            if not expect_entity:
                row = sess.execute(outer_stmt).one()
                eq_(row, (id_, "some user"))
            else:
                new_user = sess.scalars(outer_stmt).one()
                eq_(new_user, User(id=id_, username="some user"))

        cte_sql = (
            "(INSERT INTO users (id, username) "
            "VALUES (:param_1, :param_2) "
            "RETURNING users.id, users.username)"
        )

        if fetch_entity_type == "aliased" and not use_a_union:
            expected = (
                f"WITH anon_2 AS {cte_sql} "
                "SELECT anon_1.id, anon_1.username "
                "FROM (SELECT anon_2.id AS id, anon_2.username AS username "
                "FROM anon_2) AS anon_1"
            )
        elif not use_a_union:
            expected = (
                f"WITH anon_1 AS {cte_sql} "
                "SELECT anon_1.id, anon_1.username FROM anon_1"
            )
        elif fetch_entity_type == "aliased":
            expected = (
                f"WITH anon_2 AS {cte_sql} SELECT anon_1.id, anon_1.username "
                "FROM (SELECT users.id AS id, users.username AS username "
                "FROM users WHERE users.id = :id_1 "
                "UNION SELECT anon_2.id AS id, anon_2.username AS username "
                "FROM anon_2) AS anon_1"
            )
        else:
            expected = (
                f"WITH anon_1 AS {cte_sql} "
                "SELECT users.id, users.username FROM users "
                "WHERE users.id = :id_1 "
                "UNION SELECT anon_1.id, anon_1.username FROM anon_1"
            )

        asserter.assert_(
            CompiledSQL(expected, [{"param_1": id_, "param_2": "some user"}])
        )
