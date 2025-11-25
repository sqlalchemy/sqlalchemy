from __future__ import annotations

import contextlib
import datetime
from typing import Any
from typing import List
from typing import Optional
from typing import Set
import uuid

from sqlalchemy import bindparam
from sqlalchemy import Computed
from sqlalchemy import event
from sqlalchemy import exc
from sqlalchemy import FetchedValue
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import Identity
from sqlalchemy import insert
from sqlalchemy import inspect
from sqlalchemy import Integer
from sqlalchemy import literal
from sqlalchemy import literal_column
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy import update
from sqlalchemy.orm import aliased
from sqlalchemy.orm import Bundle
from sqlalchemy.orm import column_property
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import immediateload
from sqlalchemy.orm import joinedload
from sqlalchemy.orm import lazyload
from sqlalchemy.orm import load_only
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import orm_insert_sentinel
from sqlalchemy.orm import relationship
from sqlalchemy.orm import selectinload
from sqlalchemy.orm import Session
from sqlalchemy.orm import subqueryload
from sqlalchemy.sql import coercions
from sqlalchemy.sql import roles
from sqlalchemy.testing import config
from sqlalchemy.testing import eq_
from sqlalchemy.testing import expect_deprecated
from sqlalchemy.testing import expect_raises_message
from sqlalchemy.testing import expect_warnings
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.testing import mock
from sqlalchemy.testing import provision
from sqlalchemy.testing.assertsql import CompiledSQL
from sqlalchemy.testing.assertsql import Conditional
from sqlalchemy.testing.entities import ComparableEntity
from sqlalchemy.testing.fixtures import fixture_session
from sqlalchemy.types import NullType


class InsertStmtTest(testing.AssertsExecutionResults, fixtures.TestBase):
    __sparse_driver_backend__ = True

    @testing.variation(
        "style",
        [
            ("default", testing.requires.insert_returning),
            "no_executemany",
            ("no_sort_by", testing.requires.insert_returning),
            ("all_enabled", testing.requires.insert_returning),
        ],
    )
    @testing.variation("sort_by_parameter_order", [True, False])
    @testing.variation("enable_implicit_returning", [True, False])
    def test_no_returning_error(
        self,
        decl_base,
        testing_engine,
        style: testing.Variation,
        sort_by_parameter_order,
        enable_implicit_returning,
    ):
        class A(ComparableEntity, decl_base):
            __tablename__ = "a"
            id: Mapped[int] = mapped_column(Identity(), primary_key=True)
            data: Mapped[str]
            x: Mapped[Optional[int]] = mapped_column("xcol")

            if not enable_implicit_returning:
                __table_args__ = {"implicit_returning": False}

        engine = testing_engine()

        if style.default:
            pass
        elif style.no_executemany:
            engine.dialect.use_insertmanyvalues = False
            engine.dialect.use_insertmanyvalues_wo_returning = False
            engine.dialect.insert_executemany_returning = False
            engine.dialect.insert_executemany_returning_sort_by_parameter_order = (  # noqa: E501
                False
            )
        elif style.no_sort_by:
            engine.dialect.use_insertmanyvalues = True
            engine.dialect.use_insertmanyvalues_wo_returning = True
            engine.dialect.insert_executemany_returning = True
            engine.dialect.insert_executemany_returning_sort_by_parameter_order = (  # noqa: E501
                False
            )
        elif style.all_enabled:
            engine.dialect.use_insertmanyvalues = True
            engine.dialect.use_insertmanyvalues_wo_returning = True
            engine.dialect.insert_executemany_returning = True
            engine.dialect.insert_executemany_returning_sort_by_parameter_order = (  # noqa: E501
                True
            )
        else:
            style.fail()

        decl_base.metadata.create_all(engine)
        s = Session(engine)

        if (
            style.all_enabled
            or (style.no_sort_by and not sort_by_parameter_order)
            or style.default
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

    @testing.variation("render_nulls", [True, False])
    def test_render_nulls(self, decl_base, render_nulls):
        """test #10575"""

        class A(decl_base):
            __tablename__ = "a"
            id: Mapped[int] = mapped_column(Identity(), primary_key=True)
            data: Mapped[str]
            x: Mapped[Optional[int]]

        decl_base.metadata.create_all(testing.db)
        s = fixture_session()

        with self.sql_execution_asserter() as asserter:
            stmt = insert(A)
            if render_nulls:
                stmt = stmt.execution_options(render_nulls=True)

            s.execute(
                stmt,
                [
                    {"data": "d3", "x": 5},
                    {"data": "d4", "x": 6},
                    {"data": "d5", "x": 6},
                    {"data": "d6", "x": None},
                    {"data": "d7", "x": 6},
                ],
            )

        if render_nulls:
            asserter.assert_(
                CompiledSQL(
                    "INSERT INTO a (data, x) VALUES (:data, :x)",
                    [
                        {"data": "d3", "x": 5},
                        {"data": "d4", "x": 6},
                        {"data": "d5", "x": 6},
                        {"data": "d6", "x": None},
                        {"data": "d7", "x": 6},
                    ],
                ),
            )
        else:
            asserter.assert_(
                CompiledSQL(
                    "INSERT INTO a (data, x) VALUES (:data, :x)",
                    [
                        {"data": "d3", "x": 5},
                        {"data": "d4", "x": 6},
                        {"data": "d5", "x": 6},
                    ],
                ),
                CompiledSQL(
                    "INSERT INTO a (data) VALUES (:data)", [{"data": "d6"}]
                ),
                CompiledSQL(
                    "INSERT INTO a (data, x) VALUES (:data, :x)",
                    [{"data": "d7", "x": 6}],
                ),
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
    @testing.variation(
        "insert_type",
        [("values", testing.requires.multivalues_inserts), "bulk"],
    )
    def test_returning_col_property(
        self, decl_base, insert_type: testing.Variation
    ):
        """test #12326"""

        class User(ComparableEntity, decl_base):
            __tablename__ = "user"

            id: Mapped[int] = mapped_column(
                primary_key=True, autoincrement=False
            )
            name: Mapped[str]
            age: Mapped[int]

        decl_base.metadata.create_all(testing.db)

        a_alias = aliased(User)
        User.colprop = column_property(
            select(func.max(a_alias.age))
            .where(a_alias.id != User.id)
            .scalar_subquery()
        )

        sess = fixture_session()

        if insert_type.values:
            stmt = insert(User).values(
                [
                    dict(id=1, name="john", age=25),
                    dict(id=2, name="jack", age=47),
                    dict(id=3, name="jill", age=29),
                    dict(id=4, name="jane", age=37),
                ],
            )
            params = None
        elif insert_type.bulk:
            stmt = insert(User)
            params = [
                dict(id=1, name="john", age=25),
                dict(id=2, name="jack", age=47),
                dict(id=3, name="jill", age=29),
                dict(id=4, name="jane", age=37),
            ]
        else:
            insert_type.fail()

        stmt = stmt.returning(User)

        result = sess.execute(stmt, params=params)

        # the RETURNING doesn't have the column property in it.
        # so to load these, they are all lazy loaded
        with self.sql_execution_asserter() as asserter:
            eq_(
                result.scalars().all(),
                [
                    User(id=1, name="john", age=25, colprop=47),
                    User(id=2, name="jack", age=47, colprop=37),
                    User(id=3, name="jill", age=29, colprop=47),
                    User(id=4, name="jane", age=37, colprop=47),
                ],
            )

        # assert they're all lazy loaded
        asserter.assert_(
            *[
                CompiledSQL(
                    'SELECT (SELECT max(user_1.age) AS max_1 FROM "user" '
                    'AS user_1 WHERE user_1.id != "user".id) AS anon_1 '
                    'FROM "user" WHERE "user".id = :pk_1'
                )
                for i in range(4)
            ]
        )

    @testing.requires.insert_returning
    @testing.requires.returning_star
    @testing.variation(
        "insert_type",
        ["bulk", ("values", testing.requires.multivalues_inserts), "single"],
    )
    def test_insert_returning_star(self, decl_base, insert_type):
        """test #10192"""

        class User(decl_base):
            __tablename__ = "users"

            id: Mapped[int] = mapped_column(Identity(), primary_key=True)

            name: Mapped[str] = mapped_column()
            other_thing: Mapped[Optional[str]]
            server_thing: Mapped[str] = mapped_column(server_default="thing")

        decl_base.metadata.create_all(testing.db)
        insert_stmt = insert(User).returning(literal_column("*"))

        s = fixture_session()

        if insert_type.bulk or insert_type.single:
            with expect_raises_message(
                exc.CompileError,
                r"Can't use RETURNING \* with bulk ORM INSERT.",
            ):
                if insert_type.bulk:
                    s.execute(
                        insert_stmt,
                        [
                            {"name": "some name 1"},
                            {"name": "some name 2"},
                            {"name": "some name 3"},
                        ],
                    )
                else:
                    s.execute(
                        insert_stmt,
                        {"name": "some name 1"},
                    )
            return
        elif insert_type.values:
            with self.sql_execution_asserter() as asserter:
                result = s.execute(
                    insert_stmt.values(
                        [
                            {"name": "some name 1"},
                            {"name": "some name 2"},
                            {"name": "some name 3"},
                        ],
                    )
                )

            eq_(
                result.all(),
                [
                    (1, "some name 1", None, "thing"),
                    (2, "some name 2", None, "thing"),
                    (3, "some name 3", None, "thing"),
                ],
            )
            asserter.assert_(
                CompiledSQL(
                    "INSERT INTO users (name) VALUES (:name_m0), "
                    "(:name_m1), (:name_m2) RETURNING *",
                    [
                        {
                            "name_m0": "some name 1",
                            "name_m1": "some name 2",
                            "name_m2": "some name 3",
                        }
                    ],
                ),
            )
        else:
            insert_type.fail()

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

    @testing.requires.insert_returning
    @testing.variation(
        "insert_type",
        ["bulk", ("values", testing.requires.multivalues_inserts), "single"],
    )
    def test_insert_returning_bundle(self, decl_base, insert_type):
        """test #10776"""

        class User(decl_base):
            __tablename__ = "users"

            id: Mapped[int] = mapped_column(Identity(), primary_key=True)

            name: Mapped[str] = mapped_column()
            x: Mapped[int]
            y: Mapped[int]

        decl_base.metadata.create_all(testing.db)
        insert_stmt = insert(User).returning(
            User.name, Bundle("mybundle", User.id, User.x, User.y)
        )

        s = fixture_session()

        if insert_type.bulk:
            result = s.execute(
                insert_stmt,
                [
                    {"name": "some name 1", "x": 1, "y": 2},
                    {"name": "some name 2", "x": 2, "y": 3},
                    {"name": "some name 3", "x": 3, "y": 4},
                ],
            )
        elif insert_type.values:
            result = s.execute(
                insert_stmt.values(
                    [
                        {"name": "some name 1", "x": 1, "y": 2},
                        {"name": "some name 2", "x": 2, "y": 3},
                        {"name": "some name 3", "x": 3, "y": 4},
                    ],
                )
            )
        elif insert_type.single:
            result = s.execute(
                insert_stmt, {"name": "some name 1", "x": 1, "y": 2}
            )
        else:
            insert_type.fail()

        if insert_type.single:
            eq_(result.all(), [("some name 1", (1, 1, 2))])
        else:
            eq_(
                result.all(),
                [
                    ("some name 1", (1, 1, 2)),
                    ("some name 2", (2, 2, 3)),
                    ("some name 3", (3, 3, 4)),
                ],
            )

    @testing.variation(
        "use_returning", [(True, testing.requires.insert_returning), False]
    )
    @testing.variation("use_multiparams", [True, False])
    @testing.variation("bindparam_in_expression", [True, False])
    @testing.combinations(
        "auto", "raw", "bulk", "orm", argnames="dml_strategy"
    )
    def test_alt_bindparam_names(
        self,
        use_returning,
        decl_base,
        use_multiparams,
        dml_strategy,
        bindparam_in_expression,
    ):
        class A(decl_base):
            __tablename__ = "a"

            id: Mapped[int] = mapped_column(Identity(), primary_key=True)

            x: Mapped[int]
            y: Mapped[int]

        decl_base.metadata.create_all(testing.db)

        s = fixture_session()

        if bindparam_in_expression:
            stmt = insert(A).values(y=literal(3) * (bindparam("q") + 15))
        else:
            stmt = insert(A).values(y=bindparam("q"))

        if dml_strategy != "auto":
            # it really should work with any strategy
            stmt = stmt.execution_options(dml_strategy=dml_strategy)

        if use_returning:
            stmt = stmt.returning(A.x, A.y)

        if use_multiparams:
            if bindparam_in_expression:
                expected_qs = [60, 69, 81]
            else:
                expected_qs = [5, 8, 12]

            result = s.execute(
                stmt,
                [
                    {"q": 5, "x": 10},
                    {"q": 8, "x": 11},
                    {"q": 12, "x": 12},
                ],
            )
        else:
            if bindparam_in_expression:
                expected_qs = [60]
            else:
                expected_qs = [5]

            result = s.execute(stmt, {"q": 5, "x": 10})
        if use_returning:
            if use_multiparams:
                eq_(
                    result.all(),
                    [
                        (10, expected_qs[0]),
                        (11, expected_qs[1]),
                        (12, expected_qs[2]),
                    ],
                )
            else:
                eq_(result.first(), (10, expected_qs[0]))

    @testing.variation("populate_existing", [True, False])
    @testing.requires.provisioned_upsert
    @testing.requires.update_returning
    def test_upsert_populate_existing(self, decl_base, populate_existing):
        """test #9742"""

        class Employee(ComparableEntity, decl_base):
            __tablename__ = "employee"

            uuid: Mapped[uuid.UUID] = mapped_column(primary_key=True)
            user_name: Mapped[str] = mapped_column(nullable=False)

        decl_base.metadata.create_all(testing.db)
        s = fixture_session()

        uuid1 = uuid.uuid4()
        uuid2 = uuid.uuid4()
        e1 = Employee(uuid=uuid1, user_name="e1 old name")
        e2 = Employee(uuid=uuid2, user_name="e2 old name")
        s.add_all([e1, e2])
        s.flush()

        stmt = provision.upsert(
            config,
            Employee,
            (Employee,),
            set_lambda=lambda inserted: {"user_name": inserted.user_name},
        ).values(
            [
                dict(uuid=uuid1, user_name="e1 new name"),
                dict(uuid=uuid2, user_name="e2 new name"),
            ]
        )
        if populate_existing:
            rows = s.scalars(
                stmt, execution_options={"populate_existing": True}
            )
            # SPECIAL: before we actually receive the returning rows,
            # the existing objects have not been updated yet
            eq_(e1.user_name, "e1 old name")
            eq_(e2.user_name, "e2 old name")

            eq_(
                set(rows),
                {
                    Employee(uuid=uuid1, user_name="e1 new name"),
                    Employee(uuid=uuid2, user_name="e2 new name"),
                },
            )

            # now they are updated
            eq_(e1.user_name, "e1 new name")
            eq_(e2.user_name, "e2 new name")
        else:
            # no populate existing
            rows = s.scalars(stmt)
            eq_(e1.user_name, "e1 old name")
            eq_(e2.user_name, "e2 old name")
            eq_(
                set(rows),
                {
                    Employee(uuid=uuid1, user_name="e1 old name"),
                    Employee(uuid=uuid2, user_name="e2 old name"),
                },
            )
            eq_(e1.user_name, "e1 old name")
            eq_(e2.user_name, "e2 old name")
        s.commit()
        s.expire_all()
        eq_(e1.user_name, "e1 new name")
        eq_(e2.user_name, "e2 new name")


class UpdateStmtTest(testing.AssertsExecutionResults, fixtures.TestBase):
    __sparse_driver_backend__ = True

    @testing.variation(
        "use_onupdate",
        [
            "none",
            "server",
            "callable",
            "clientsql",
            ("computed", testing.requires.computed_columns),
        ],
    )
    def test_bulk_update_onupdates(
        self,
        decl_base,
        use_onupdate,
    ):
        """assert that for now, bulk ORM update by primary key does not
        expire or refresh onupdates."""

        class Employee(ComparableEntity, decl_base):
            __tablename__ = "employee"

            uuid: Mapped[uuid.UUID] = mapped_column(primary_key=True)
            user_name: Mapped[str] = mapped_column(String(200), nullable=False)

            if use_onupdate.server:
                some_server_value: Mapped[str] = mapped_column(
                    server_onupdate=FetchedValue()
                )
            elif use_onupdate.callable:
                some_server_value: Mapped[str] = mapped_column(
                    onupdate=lambda: "value 2"
                )
            elif use_onupdate.clientsql:
                some_server_value: Mapped[str] = mapped_column(
                    onupdate=literal("value 2")
                )
            elif use_onupdate.computed:
                some_server_value: Mapped[str] = mapped_column(
                    String(255),
                    Computed(user_name + " computed value"),
                    nullable=True,
                )
            else:
                some_server_value: Mapped[str]

        decl_base.metadata.create_all(testing.db)
        s = fixture_session()

        uuid1 = uuid.uuid4()

        if use_onupdate.computed:
            server_old_value, server_new_value = (
                "e1 old name computed value",
                "e1 new name computed value",
            )
            e1 = Employee(uuid=uuid1, user_name="e1 old name")
        else:
            server_old_value, server_new_value = ("value 1", "value 2")
            e1 = Employee(
                uuid=uuid1,
                user_name="e1 old name",
                some_server_value="value 1",
            )
        s.add(e1)
        s.flush()

        # for computed col, make sure e1.some_server_value is loaded.
        # this will already be the case for all RETURNING backends, so this
        # suits just MySQL.
        if use_onupdate.computed:
            e1.some_server_value

        stmt = update(Employee)

        # perform out of band UPDATE on server value to simulate
        # a computed col
        if use_onupdate.none or use_onupdate.server:
            s.connection().execute(
                update(Employee.__table__).values(some_server_value="value 2")
            )

        execution_options = {}

        s.execute(
            stmt,
            execution_options=execution_options,
            params=[{"uuid": uuid1, "user_name": "e1 new name"}],
        )

        assert "some_server_value" in e1.__dict__
        eq_(e1.some_server_value, server_old_value)

        # do a full expire, now the new value is definitely there
        s.commit()
        s.expire_all()
        eq_(e1.some_server_value, server_new_value)

    @testing.variation(
        "returning_executemany",
        [
            ("returning", testing.requires.update_returning),
            "executemany",
            "plain",
        ],
    )
    @testing.variation("bindparam_in_expression", [True, False])
    # TODO: setting "bulk" here is all over the place as well, UPDATE is not
    # too settled
    @testing.combinations("auto", "orm", argnames="dml_strategy")
    @testing.combinations(
        "evaluate", "fetch", None, argnames="synchronize_strategy"
    )
    def test_alt_bindparam_names(
        self,
        decl_base,
        returning_executemany,
        dml_strategy,
        bindparam_in_expression,
        synchronize_strategy,
    ):
        class A(decl_base):
            __tablename__ = "a"

            id: Mapped[int] = mapped_column(
                primary_key=True, autoincrement=False
            )

            x: Mapped[int]
            y: Mapped[int]

        decl_base.metadata.create_all(testing.db)

        s = fixture_session()

        s.add_all(
            [A(id=1, x=1, y=1), A(id=2, x=2, y=2), A(id=3, x=3, y=3)],
        )
        s.commit()

        if bindparam_in_expression:
            stmt = (
                update(A)
                .values(y=literal(3) * (bindparam("q") + 15))
                .where(A.id == bindparam("b_id"))
            )
        else:
            stmt = (
                update(A)
                .values(y=bindparam("q"))
                .where(A.id == bindparam("b_id"))
            )

        if dml_strategy != "auto":
            # it really should work with any strategy
            stmt = stmt.execution_options(dml_strategy=dml_strategy)

        if returning_executemany.returning:
            stmt = stmt.returning(A.x, A.y)

        if synchronize_strategy in (None, "evaluate", "fetch"):
            stmt = stmt.execution_options(
                synchronize_session=synchronize_strategy
            )

        if returning_executemany.executemany:
            if bindparam_in_expression:
                expected_qs = [60, 69, 81]
            else:
                expected_qs = [5, 8, 12]

            if dml_strategy != "orm":
                params = [
                    {"id": 1, "b_id": 1, "q": 5, "x": 10},
                    {"id": 2, "b_id": 2, "q": 8, "x": 11},
                    {"id": 3, "b_id": 3, "q": 12, "x": 12},
                ]
            else:
                params = [
                    {"b_id": 1, "q": 5, "x": 10},
                    {"b_id": 2, "q": 8, "x": 11},
                    {"b_id": 3, "q": 12, "x": 12},
                ]

            _expect_raises = None

            if synchronize_strategy == "fetch":
                if dml_strategy != "orm":
                    _expect_raises = expect_raises_message(
                        exc.InvalidRequestError,
                        r"The 'fetch' synchronization strategy is not "
                        r"available for 'bulk' ORM updates "
                        r"\(i.e. multiple parameter sets\)",
                    )
                elif not testing.db.dialect.update_executemany_returning:
                    # no backend supports this except Oracle
                    _expect_raises = expect_raises_message(
                        exc.InvalidRequestError,
                        r"For synchronize_session='fetch', can't use multiple "
                        r"parameter sets in ORM mode, which this backend does "
                        r"not support with RETURNING",
                    )

            elif synchronize_strategy == "evaluate" and dml_strategy != "orm":
                _expect_raises = expect_raises_message(
                    exc.InvalidRequestError,
                    "bulk synchronize of persistent objects not supported",
                )

            if _expect_raises:
                with _expect_raises:
                    result = s.execute(stmt, params)
                return

            result = s.execute(stmt, params)
        else:
            if bindparam_in_expression:
                expected_qs = [60]
            else:
                expected_qs = [5]

            result = s.execute(stmt, {"b_id": 1, "q": 5, "x": 10})

        if returning_executemany.returning:
            eq_(result.first(), (10, expected_qs[0]))

        elif returning_executemany.executemany:
            eq_(
                s.execute(select(A.x, A.y).order_by(A.id)).all(),
                [
                    (10, expected_qs[0]),
                    (11, expected_qs[1]),
                    (12, expected_qs[2]),
                ],
            )

    @testing.variation("add_where", [True, False])
    @testing.variation("multi_row", ["multirow", "singlerow", "listwsingle"])
    def test_bulk_update_no_pk(self, decl_base, add_where, multi_row):
        """test #9917"""

        class A(decl_base):
            __tablename__ = "a"

            id: Mapped[int] = mapped_column(
                primary_key=True, autoincrement=False
            )

            x: Mapped[int]
            y: Mapped[int]

        decl_base.metadata.create_all(testing.db)

        s = fixture_session()

        s.add_all(
            [A(id=1, x=1, y=1), A(id=2, x=2, y=2), A(id=3, x=3, y=3)],
        )
        s.commit()

        stmt = update(A)
        if add_where:
            stmt = stmt.where(A.x > 1)

        if multi_row.multirow:
            data = [
                {"x": 3, "y": 8},
                {"x": 5, "y": 9},
                {"x": 12, "y": 15},
            ]

            stmt = stmt.execution_options(synchronize_session=None)
        elif multi_row.listwsingle:
            data = [
                {"x": 5, "y": 9},
            ]

            stmt = stmt.execution_options(synchronize_session=None)
        elif multi_row.singlerow:
            data = {"x": 5, "y": 9}
        else:
            multi_row.fail()

        if multi_row.multirow or multi_row.listwsingle:
            with expect_raises_message(
                exc.InvalidRequestError,
                r"No primary key value supplied for column\(s\) a.id; per-row "
                "ORM Bulk UPDATE by Primary Key requires that records contain "
                "primary key values",
            ):
                s.execute(stmt, data)
        else:
            with self.sql_execution_asserter() as asserter:
                s.execute(stmt, data)

            if add_where:
                asserter.assert_(
                    CompiledSQL(
                        "UPDATE a SET x=:x, y=:y WHERE a.x > :x_1",
                        [{"x": 5, "y": 9, "x_1": 1}],
                    ),
                )
            else:
                asserter.assert_(
                    CompiledSQL("UPDATE a SET x=:x, y=:y", [{"x": 5, "y": 9}]),
                )

    @testing.variation("multi_row", ["multirow", "singlerow", "listwsingle"])
    @testing.requires.update_returning
    @testing.requires.returning_star
    def test_bulk_update_returning_star(self, decl_base, multi_row):
        class A(decl_base):
            __tablename__ = "a"

            id: Mapped[int] = mapped_column(
                primary_key=True, autoincrement=False
            )

            x: Mapped[int]
            y: Mapped[int]

        decl_base.metadata.create_all(testing.db)

        s = fixture_session()

        s.add_all(
            [A(id=1, x=1, y=1), A(id=2, x=2, y=2), A(id=3, x=3, y=3)],
        )
        s.commit()

        stmt = update(A).returning(literal_column("*"))

        if multi_row.multirow:
            data = [
                {"x": 3, "y": 8},
                {"x": 5, "y": 9},
                {"x": 12, "y": 15},
            ]

            stmt = stmt.execution_options(synchronize_session=None)
        elif multi_row.listwsingle:
            data = [
                {"x": 5, "y": 9},
            ]

            stmt = stmt.execution_options(synchronize_session=None)
        elif multi_row.singlerow:
            data = {"x": 5, "y": 9}
        else:
            multi_row.fail()

        if multi_row.multirow or multi_row.listwsingle:
            with expect_raises_message(
                exc.InvalidRequestError, "No primary key value supplied"
            ):
                s.execute(stmt, data)
                return
        else:
            result = s.execute(stmt, data)
            eq_(result.all(), [(1, 5, 9), (2, 5, 9), (3, 5, 9)])

    @testing.requires.update_returning
    def test_bulk_update_returning_bundle(self, decl_base):
        class A(decl_base):
            __tablename__ = "a"

            id: Mapped[int] = mapped_column(
                primary_key=True, autoincrement=False
            )

            x: Mapped[int]
            y: Mapped[int]

        decl_base.metadata.create_all(testing.db)

        s = fixture_session()

        s.add_all(
            [A(id=1, x=1, y=1), A(id=2, x=2, y=2), A(id=3, x=3, y=3)],
        )
        s.commit()

        stmt = update(A).returning(Bundle("mybundle", A.id, A.x), A.y)

        data = {"x": 5, "y": 9}

        result = s.execute(stmt, data)
        eq_(result.all(), [((1, 5), 9), ((2, 5), 9), ((3, 5), 9)])

    def test_bulk_update_w_where_one(self, decl_base):
        """test use case in #9595"""

        class A(decl_base):
            __tablename__ = "a"

            id: Mapped[int] = mapped_column(
                primary_key=True, autoincrement=False
            )

            x: Mapped[int]
            y: Mapped[int]

        decl_base.metadata.create_all(testing.db)

        s = fixture_session()

        s.add_all(
            [A(id=1, x=1, y=1), A(id=2, x=2, y=2), A(id=3, x=3, y=3)],
        )
        s.commit()

        stmt = (
            update(A)
            .where(A.x > 1)
            .execution_options(synchronize_session=None)
        )

        s.execute(
            stmt,
            [
                {"id": 1, "x": 3, "y": 8},
                {"id": 2, "x": 5, "y": 9},
                {"id": 3, "x": 12, "y": 15},
            ],
        )

        eq_(
            s.execute(select(A.id, A.x, A.y).order_by(A.id)).all(),
            [(1, 1, 1), (2, 5, 9), (3, 12, 15)],
        )

    def test_bulk_update_w_where_two(self, decl_base):
        class User(decl_base):
            __tablename__ = "user"

            id: Mapped[int] = mapped_column(
                primary_key=True, autoincrement=False
            )
            name: Mapped[str]
            age: Mapped[int]

        decl_base.metadata.create_all(testing.db)

        sess = fixture_session()
        sess.execute(
            insert(User),
            [
                dict(id=1, name="john", age=25),
                dict(id=2, name="jack", age=47),
                dict(id=3, name="jill", age=29),
                dict(id=4, name="jane", age=37),
            ],
        )

        sess.execute(
            update(User)
            .where(User.age > bindparam("gtage"))
            .values(age=bindparam("dest_age"))
            .execution_options(synchronize_session=None),
            [
                {"id": 1, "gtage": 28, "dest_age": 40},
                {"id": 2, "gtage": 20, "dest_age": 45},
            ],
        )

        eq_(
            sess.execute(
                select(User.id, User.name, User.age).order_by(User.id)
            ).all(),
            [
                (1, "john", 25),
                (2, "jack", 45),
                (3, "jill", 29),
                (4, "jane", 37),
            ],
        )

    @testing.requires.update_returning
    def test_returning_col_property(self, decl_base):
        """test #12326"""

        class User(ComparableEntity, decl_base):
            __tablename__ = "user"

            id: Mapped[int] = mapped_column(
                primary_key=True, autoincrement=False
            )
            name: Mapped[str]
            age: Mapped[int]

        decl_base.metadata.create_all(testing.db)

        a_alias = aliased(User)
        User.colprop = column_property(
            select(func.max(a_alias.age))
            .where(a_alias.id != User.id)
            .scalar_subquery()
        )

        sess = fixture_session()

        sess.execute(
            insert(User),
            [
                dict(id=1, name="john", age=25),
                dict(id=2, name="jack", age=47),
                dict(id=3, name="jill", age=29),
                dict(id=4, name="jane", age=37),
            ],
        )

        stmt = (
            update(User).values(age=30).where(User.age == 29).returning(User)
        )

        row = sess.execute(stmt).one()
        eq_(row[0], User(id=3, name="jill", age=30, colprop=47))


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

    @testing.variation(
        "insert_strategy",
        ["orm", "bulk", "bulk_ordered", "bulk_w_embedded_bindparam"],
    )
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
        elif insert_strategy.bulk_w_embedded_bindparam:
            # test related to #9583, specific user case in
            # https://github.com/sqlalchemy/sqlalchemy/discussions/9581#discussioncomment-5504077  # noqa: E501
            stmt = stmt.values(
                y=select(bindparam("qq1", type_=Integer)).scalar_subquery()
            )
            for d in upsert_data:
                d["qq1"] = d.pop("y")
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
    __sparse_driver_backend__ = True

    use_sentinel = False
    randomize_returning = False

    @classmethod
    def setup_classes(cls):
        decl_base = cls.DeclarativeBasic

        class A(ComparableEntity, decl_base):
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
    __sparse_driver_backend__ = True

    @classmethod
    def setup_classes(cls):
        decl_base = cls.DeclarativeBasic

        class A(ComparableEntity, decl_base):
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
    __sparse_driver_backend__ = True

    @classmethod
    def setup_classes(cls):
        decl_base = cls.DeclarativeBasic

        class A(ComparableEntity, decl_base):
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
    __sparse_driver_backend__ = True

    @classmethod
    def setup_classes(cls):
        decl_base = cls.DeclarativeBasic

        class User(ComparableEntity, decl_base):
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


class EagerLoadTest(
    fixtures.DeclarativeMappedTest, testing.AssertsExecutionResults
):
    run_inserts = "each"
    __requires__ = ("insert_returning",)

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class A(Base):
            __tablename__ = "a"
            id: Mapped[int] = mapped_column(
                Integer, Identity(), primary_key=True
            )
            cs = relationship("C")

        class B(Base):
            __tablename__ = "b"
            id: Mapped[int] = mapped_column(
                Integer, Identity(), primary_key=True
            )
            a_id: Mapped[int] = mapped_column(ForeignKey("a.id"))
            a = relationship("A")

        class C(Base):
            __tablename__ = "c"
            id: Mapped[int] = mapped_column(
                Integer, Identity(), primary_key=True
            )
            a_id: Mapped[int] = mapped_column(ForeignKey("a.id"))

    @classmethod
    def insert_data(cls, connection):
        A = cls.classes.A
        C = cls.classes.C
        with Session(connection) as sess:
            sess.add_all(
                [
                    A(id=1, cs=[C(id=1), C(id=2)]),
                    A(id=2),
                    A(id=3, cs=[C(id=3), C(id=4)]),
                ]
            )
            sess.commit()

    @testing.fixture
    def fixture_with_loader_opt(self):
        def go(lazy):
            class Base(DeclarativeBase):
                pass

            class A(Base):
                __tablename__ = "a"
                id: Mapped[int] = mapped_column(Integer, primary_key=True)

            class B(Base):
                __tablename__ = "b"
                id: Mapped[int] = mapped_column(Integer, primary_key=True)
                a_id: Mapped[int] = mapped_column(ForeignKey("a.id"))
                a = relationship("A", lazy=lazy)

            return A, B

        return go

    @testing.combinations(
        (selectinload,),
        (immediateload,),
    )
    def test_insert_supported(self, loader):
        A, B = self.classes("A", "B")

        sess = fixture_session()

        result = sess.execute(
            insert(B).returning(B).options(loader(B.a)),
            [
                {"id": 1, "a_id": 1},
                {"id": 2, "a_id": 1},
                {"id": 3, "a_id": 2},
                {"id": 4, "a_id": 3},
                {"id": 5, "a_id": 3},
            ],
        ).scalars()

        for b in result:
            assert "a" in b.__dict__

    @testing.combinations(
        (joinedload,),
        (subqueryload,),
    )
    def test_insert_not_supported(self, loader):
        """test #11853"""

        A, B = self.classes("A", "B")

        sess = fixture_session()

        stmt = insert(B).returning(B).options(loader(B.a))

        with expect_deprecated(
            f"The {loader.__name__} loader option is not compatible "
            "with DML statements",
        ):
            sess.execute(stmt, [{"id": 1, "a_id": 1}])

    @testing.combinations(
        (joinedload,),
        (subqueryload,),
        (selectinload,),
        (immediateload,),
    )
    def test_secondary_opt_ok(self, loader):
        A, B = self.classes("A", "B")

        sess = fixture_session()

        opt = selectinload(B.a)
        opt = getattr(opt, loader.__name__)(A.cs)

        result = sess.execute(
            insert(B).returning(B).options(opt),
            [
                {"id": 1, "a_id": 1},
                {"id": 2, "a_id": 1},
                {"id": 3, "a_id": 2},
                {"id": 4, "a_id": 3},
                {"id": 5, "a_id": 3},
            ],
        ).scalars()

        for b in result:
            assert "a" in b.__dict__
            assert "cs" in b.a.__dict__

    @testing.combinations(
        ("joined",),
        ("select",),
        ("subquery",),
        ("selectin",),
        ("immediate",),
        argnames="lazy_opt",
    )
    def test_insert_handles_implicit(self, fixture_with_loader_opt, lazy_opt):
        """test #11853"""

        A, B = fixture_with_loader_opt(lazy_opt)

        sess = fixture_session()

        for b_obj in sess.execute(
            insert(B).returning(B),
            [
                {"id": 1, "a_id": 1},
                {"id": 2, "a_id": 1},
                {"id": 3, "a_id": 2},
                {"id": 4, "a_id": 3},
                {"id": 5, "a_id": 3},
            ],
        ).scalars():

            if lazy_opt in ("select", "joined", "subquery"):
                # these aren't supported by DML
                assert "a" not in b_obj.__dict__
            else:
                # the other three are
                assert "a" in b_obj.__dict__

    @testing.combinations(
        (lazyload,), (selectinload,), (immediateload,), argnames="loader_opt"
    )
    @testing.combinations(
        (joinedload,),
        (subqueryload,),
        (selectinload,),
        (immediateload,),
        (lazyload,),
        argnames="secondary_opt",
    )
    def test_secondary_w_criteria_caching(self, loader_opt, secondary_opt):
        """test #11855"""
        A, B, C = self.classes("A", "B", "C")

        for i in range(3):
            with fixture_session() as sess:

                opt = loader_opt(B.a)
                opt = getattr(opt, secondary_opt.__name__)(
                    A.cs.and_(C.a_id == 1)
                )
                stmt = insert(B).returning(B).options(opt)

                b1 = sess.scalar(stmt, [{"a_id": 1}])

                eq_({c.id for c in b1.a.cs}, {1, 2})

                opt = loader_opt(B.a)
                opt = getattr(opt, secondary_opt.__name__)(
                    A.cs.and_(C.a_id == 3)
                )

                stmt = insert(B).returning(B).options(opt)

                b3 = sess.scalar(stmt, [{"a_id": 3}])

                eq_({c.id for c in b3.a.cs}, {3, 4})


class DMLCompileScenariosTest(testing.AssertsCompiledSQL, fixtures.TestBase):
    __dialect__ = "default_enhanced"  # for UPDATE..FROM

    @testing.variation("style", ["insert", "upsert"])
    def test_insert_values_from_primary_table_only(self, decl_base, style):
        """test for #12692"""

        class A(decl_base):
            __tablename__ = "a"
            id: Mapped[int] = mapped_column(Identity(), primary_key=True)
            data: Mapped[int]

        class B(decl_base):
            __tablename__ = "b"
            id: Mapped[int] = mapped_column(Identity(), primary_key=True)
            data: Mapped[str]

        stmt = insert(A.__table__)

        # we're trying to exercise codepaths in orm/bulk_persistence.py that
        # would only apply to an insert() statement against the ORM entity,
        # e.g. insert(A).  In the update() case, the WHERE clause can also
        # pull in the ORM entity, which is how we found the issue here, but
        # for INSERT there's no current method that does this; returning()
        # could do this in theory but currently doesnt.  So for now, cheat,
        # and pretend there's some conversion that's going to propagate
        # from an ORM expression
        coercions.expect(
            roles.WhereHavingRole, B.id == 5, apply_propagate_attrs=stmt
        )

        if style.insert:
            stmt = stmt.values(data=123)

            # assert that the ORM did not get involved, putting B.data as the
            # key in the dictionary
            is_(stmt._values["data"].type._type_affinity, NullType)
        elif style.upsert:
            stmt = stmt.values([{"data": 123}, {"data": 456}])

            # assert that the ORM did not get involved, putting B.data as the
            # keys in the dictionaries
            eq_(stmt._multi_values, ([{"data": 123}, {"data": 456}],))
        else:
            style.fail()

    def test_update_values_from_primary_table_only(self, decl_base):
        """test for #12692"""

        class A(decl_base):
            __tablename__ = "a"
            id: Mapped[int] = mapped_column(Identity(), primary_key=True)
            data: Mapped[str]
            updated_at: Mapped[datetime.datetime] = mapped_column(
                onupdate=func.now()
            )

        class B(decl_base):
            __tablename__ = "b"
            id: Mapped[int] = mapped_column(Identity(), primary_key=True)
            data: Mapped[str]
            updated_at: Mapped[datetime.datetime] = mapped_column(
                onupdate=func.now()
            )

        stmt = update(A.__table__).where(B.id == 1).values(data="some data")
        self.assert_compile(
            stmt,
            "UPDATE a SET data=:data, updated_at=now() "
            "FROM b WHERE b.id = :id_1",
        )
