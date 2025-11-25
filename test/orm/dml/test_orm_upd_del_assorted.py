from __future__ import annotations

import uuid

from sqlalchemy import Computed
from sqlalchemy import delete
from sqlalchemy import FetchedValue
from sqlalchemy import insert
from sqlalchemy import Integer
from sqlalchemy import literal
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy import update
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import Session
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.entities import ComparableEntity
from sqlalchemy.testing.fixtures import fixture_session
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table


class LoadFromReturningTest(fixtures.MappedTest):
    __sparse_driver_backend__ = True
    __requires__ = ("insert_returning",)

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "users",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("name", String(32)),
            Column("age_int", Integer),
        )

    @classmethod
    def setup_classes(cls):
        class User(cls.Comparable):
            pass

        class Address(cls.Comparable):
            pass

    @classmethod
    def insert_data(cls, connection):
        users = cls.tables.users

        connection.execute(
            users.insert(),
            [
                dict(id=1, name="john", age_int=25),
                dict(id=2, name="jack", age_int=47),
                dict(id=3, name="jill", age_int=29),
                dict(id=4, name="jane", age_int=37),
            ],
        )

    @classmethod
    def setup_mappers(cls):
        User = cls.classes.User
        users = cls.tables.users

        cls.mapper_registry.map_imperatively(
            User,
            users,
            properties={
                "age": users.c.age_int,
            },
        )

    @testing.requires.update_returning
    @testing.combinations(True, False, argnames="use_from_statement")
    def test_load_from_update(self, connection, use_from_statement):
        User = self.classes.User

        stmt = (
            update(User)
            .where(User.name.in_(["jack", "jill"]))
            .values(age=User.age + 5)
            .returning(User)
        )

        if use_from_statement:
            # this is now a legacy-ish case, because as of 2.0 you can just
            # use returning() directly to get the objects back.
            #
            # when from_statement is used, the UPDATE statement is no
            # longer interpreted by
            # BulkUDCompileState.orm_pre_session_exec or
            # BulkUDCompileState.orm_setup_cursor_result.  The compilation
            # level routines still take place though
            stmt = select(User).from_statement(stmt)

        with Session(connection) as sess:
            rows = sess.execute(stmt).scalars().all()

            eq_(
                rows,
                [User(name="jack", age=52), User(name="jill", age=34)],
            )

    @testing.combinations(
        ("single",),
        ("multiple", testing.requires.multivalues_inserts),
        argnames="params",
    )
    @testing.combinations(True, False, argnames="use_from_statement")
    def test_load_from_insert(self, connection, params, use_from_statement):
        User = self.classes.User

        if params == "multiple":
            values = [
                {User.id: 5, User.age: 25, User.name: "spongebob"},
                {User.id: 6, User.age: 30, User.name: "patrick"},
                {User.id: 7, User.age: 35, User.name: "squidward"},
            ]
        elif params == "single":
            values = {User.id: 5, User.age: 25, User.name: "spongebob"}
        else:
            assert False

        stmt = insert(User).values(values).returning(User)

        if use_from_statement:
            stmt = select(User).from_statement(stmt)

        with Session(connection) as sess:
            rows = sess.execute(stmt).scalars().all()

            if params == "multiple":
                eq_(
                    rows,
                    [
                        User(name="spongebob", age=25),
                        User(name="patrick", age=30),
                        User(name="squidward", age=35),
                    ],
                )
            elif params == "single":
                eq_(
                    rows,
                    [User(name="spongebob", age=25)],
                )
            else:
                assert False

    @testing.requires.delete_returning
    @testing.combinations(True, False, argnames="use_from_statement")
    def test_load_from_delete(self, connection, use_from_statement):
        User = self.classes.User

        stmt = (
            delete(User).where(User.name.in_(["jack", "jill"])).returning(User)
        )

        if use_from_statement:
            stmt = select(User).from_statement(stmt)

        with Session(connection) as sess:
            rows = sess.execute(stmt).scalars().all()

            eq_(
                rows,
                [User(name="jack", age=47), User(name="jill", age=29)],
            )

            # TODO: state of above objects should be "deleted"


class OnUpdatePopulationTest(fixtures.TestBase):
    __sparse_driver_backend__ = True

    @testing.variation("populate_existing", [True, False])
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
    @testing.variation(
        "use_returning",
        [
            ("returning", testing.requires.update_returning),
            ("defaults", testing.requires.update_returning),
            "none",
        ],
    )
    @testing.variation("synchronize", ["auto", "fetch", "evaluate"])
    @testing.variation("pk_order", ["first", "middle"])
    def test_update_populate_existing(
        self,
        decl_base,
        populate_existing,
        use_onupdate,
        use_returning,
        synchronize,
        pk_order,
    ):
        """test #11912 and #11917"""

        class Employee(ComparableEntity, decl_base):
            __tablename__ = "employee"

            if pk_order.first:
                uuid: Mapped[uuid.UUID] = mapped_column(primary_key=True)
            user_name: Mapped[str] = mapped_column(String(200), nullable=False)

            if pk_order.middle:
                uuid: Mapped[uuid.UUID] = mapped_column(primary_key=True)

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

        stmt = (
            update(Employee)
            .values(user_name="e1 new name")
            .where(Employee.uuid == uuid1)
        )

        if use_returning.returning:
            stmt = stmt.returning(Employee)
        elif use_returning.defaults:
            # NOTE: the return_defaults case here has not been analyzed for
            # #11912 or #11917.   future enhancements may change its behavior
            stmt = stmt.return_defaults()

        # perform out of band UPDATE on server value to simulate
        # a computed col
        if use_onupdate.none or use_onupdate.server:
            s.connection().execute(
                update(Employee.__table__).values(some_server_value="value 2")
            )

        execution_options = {}

        if populate_existing:
            execution_options["populate_existing"] = True

        if synchronize.evaluate:
            execution_options["synchronize_session"] = "evaluate"
        if synchronize.fetch:
            execution_options["synchronize_session"] = "fetch"

        if use_returning.returning:
            rows = s.scalars(stmt, execution_options=execution_options)
        else:
            s.execute(stmt, execution_options=execution_options)

        if (
            use_onupdate.clientsql
            or use_onupdate.server
            or use_onupdate.computed
        ):
            if not use_returning.defaults:
                # if server-side onupdate was generated, the col should have
                # been expired
                assert "some_server_value" not in e1.__dict__

                # and refreshes when called.  this is even if we have RETURNING
                # rows we didn't fetch yet.
                eq_(e1.some_server_value, server_new_value)
            else:
                # using return defaults here is not expiring.   have not
                # researched why, it may be because the explicit
                # return_defaults interferes with the ORMs call
                assert "some_server_value" in e1.__dict__
                eq_(e1.some_server_value, server_old_value)

        elif use_onupdate.callable:
            if not use_returning.defaults or not synchronize.fetch:
                # for python-side onupdate, col is populated with local value
                assert "some_server_value" in e1.__dict__

                # and is refreshed
                eq_(e1.some_server_value, server_new_value)
            else:
                assert "some_server_value" in e1.__dict__

                # and is not refreshed
                eq_(e1.some_server_value, server_old_value)

        else:
            # no onupdate, then the value was not touched yet,
            # even if we used RETURNING with populate_existing, because
            # we did not fetch the rows yet
            assert "some_server_value" in e1.__dict__
            eq_(e1.some_server_value, server_old_value)

        # now see if we can fetch rows
        if use_returning.returning:

            if populate_existing or not use_onupdate.none:
                eq_(
                    set(rows),
                    {
                        Employee(
                            uuid=uuid1,
                            user_name="e1 new name",
                            some_server_value=server_new_value,
                        ),
                    },
                )

            else:
                # if no populate existing and no server default, that column
                # is not touched at all
                eq_(
                    set(rows),
                    {
                        Employee(
                            uuid=uuid1,
                            user_name="e1 new name",
                            some_server_value=server_old_value,
                        ),
                    },
                )

        if use_returning.defaults:
            # as mentioned above, the return_defaults() case here remains
            # unanalyzed.
            if synchronize.fetch or (
                use_onupdate.clientsql
                or use_onupdate.server
                or use_onupdate.computed
                or use_onupdate.none
            ):
                eq_(e1.some_server_value, server_old_value)
            else:
                eq_(e1.some_server_value, server_new_value)

        elif (
            populate_existing and use_returning.returning
        ) or not use_onupdate.none:
            eq_(e1.some_server_value, server_new_value)
        else:
            # no onupdate specified, and no populate existing with returning,
            # the attribute is not refreshed
            eq_(e1.some_server_value, server_old_value)

        # do a full expire, now the new value is definitely there
        s.commit()
        s.expire_all()
        eq_(e1.some_server_value, server_new_value)


class PGIssue11849Test(fixtures.DeclarativeMappedTest):
    __sparse_driver_backend__ = True
    __only_on__ = ("postgresql",)

    @classmethod
    def setup_classes(cls):

        from sqlalchemy.dialects.postgresql import JSONB

        Base = cls.DeclarativeBasic

        class TestTbl(Base):
            __tablename__ = "testtbl"

            test_id = Column(Integer, primary_key=True)
            test_field = Column(JSONB)

    def test_issue_11849(self):
        TestTbl = self.classes.TestTbl

        session = fixture_session()

        obj = TestTbl(
            test_id=1, test_field={"test1": 1, "test2": "2", "test3": [3, "3"]}
        )
        session.add(obj)

        query = (
            update(TestTbl)
            .where(TestTbl.test_id == 1)
            .values(test_field=TestTbl.test_field + {"test3": {"test4": 4}})
        )
        session.execute(query)

        # not loaded
        assert "test_field" not in obj.__dict__

        # synchronizes on load
        eq_(obj.test_field, {"test1": 1, "test2": "2", "test3": {"test4": 4}})
