from __future__ import annotations

import re
import uuid

from sqlalchemy import Computed
from sqlalchemy import delete
from sqlalchemy import exc
from sqlalchemy import FetchedValue
from sqlalchemy import ForeignKey
from sqlalchemy import insert
from sqlalchemy import Integer
from sqlalchemy import literal
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy import update
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import eq_
from sqlalchemy.testing import expect_raises_message
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


class _FilterByDMLSuite(fixtures.MappedTest, AssertsCompiledSQL):
    """Base test suite for filter_by() on ORM DML statements.

    Tests filter_by() functionality for UPDATE and DELETE with ORM entities,
    verifying it can locate attributes across multiple joined tables and
    raises AmbiguousColumnError for ambiguous names.
    """

    __dialect__ = "default_enhanced"

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "users",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("name", String(30), nullable=False),
            Column("department_id", ForeignKey("departments.id")),
        )
        Table(
            "addresses",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("user_id", None, ForeignKey("users.id")),
            Column("name", String(30), nullable=False),
            Column("email_address", String(50), nullable=False),
        )
        Table(
            "dingalings",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("address_id", None, ForeignKey("addresses.id")),
            Column("data", String(30)),
        )
        Table(
            "departments",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("name", String(30)),
        )

    @classmethod
    def setup_classes(cls):
        class User(cls.Comparable):
            pass

        class Address(cls.Comparable):
            pass

        class Dingaling(cls.Comparable):
            pass

        class Department(cls.Comparable):
            pass

    @classmethod
    def setup_mappers(cls):
        User = cls.classes.User
        users = cls.tables.users

        Address = cls.classes.Address
        addresses = cls.tables.addresses

        Dingaling = cls.classes.Dingaling
        dingalings = cls.tables.dingalings

        Department = cls.classes.Department
        departments = cls.tables.departments

        cls.mapper_registry.map_imperatively(
            User,
            users,
            properties={
                "addresses": relationship(Address),
                "department": relationship(Department),
            },
        )
        cls.mapper_registry.map_imperatively(
            Address,
            addresses,
            properties={"dingalings": relationship(Dingaling)},
        )
        cls.mapper_registry.map_imperatively(Dingaling, dingalings)
        cls.mapper_registry.map_imperatively(Department, departments)

    def test_filter_by_basic(self, one_table_statement):
        """Test filter_by with a single ORM entity."""
        stmt = one_table_statement

        stmt = stmt.filter_by(name="somename")
        self.assert_compile(
            stmt,
            re.compile(r"(?:UPDATE|DELETE) .* WHERE users\.name = :name_1"),
            params={"name_1": "somename"},
        )

    def test_filter_by_two_tables_ambiguous_id(self, two_table_statement):
        """Test filter_by raises error when 'id' is ambiguous."""
        stmt = two_table_statement

        # Filter by 'id' which exists in both tables - should raise error
        with expect_raises_message(
            exc.AmbiguousColumnError,
            'Attribute name "id" is ambiguous',
        ):
            stmt.filter_by(id=5)

    def test_filter_by_two_tables_secondary(self, two_table_statement):
        """Test filter_by finds attribute in secondary table."""
        stmt = two_table_statement

        # Filter by 'email_address' which only exists in addresses table
        stmt = stmt.filter_by(email_address="test@example.com")
        self.assert_compile(
            stmt,
            re.compile(
                r"(?:UPDATE|DELETE) .* addresses\.email_address = "
                r":email_address_1"
            ),
        )

    def test_filter_by_three_tables_ambiguous(self, three_table_statement):
        """Test filter_by raises AmbiguousColumnError for ambiguous
        names."""
        stmt = three_table_statement

        # interestingly, UPDATE/DELETE dont use an ORM specific version
        # for filter_by() entity lookup, unlike SELECT
        with expect_raises_message(
            exc.AmbiguousColumnError,
            'Attribute name "name" is ambiguous; it exists in multiple FROM '
            "clause entities "
            r"\((?:users(?:, )?|dingalings(?:, )?|addresses(?:, )?){3}\).",
        ):
            stmt.filter_by(name="ambiguous")

    def test_filter_by_four_tables_ambiguous(self, four_table_statement):
        """test the ellipses version of the ambiguous message"""
        stmt = four_table_statement

        # interestingly, UPDATE/DELETE dont use an ORM specific version
        # for filter_by() entity lookup, unlike SELECT
        with expect_raises_message(
            exc.AmbiguousColumnError,
            r'Attribute name "name" is ambiguous; it exists in multiple '
            r"FROM clause entities "
            r"\((?:dingalings, |departments, |users, |addresses, ){3}\.\.\. "
            r"\(4 total\)\)",
        ):
            stmt.filter_by(name="ambiguous")

    def test_filter_by_three_tables_notfound(self, three_table_statement):
        """test the three or fewer table not found message"""
        stmt = three_table_statement

        with expect_raises_message(
            exc.InvalidRequestError,
            r'None of the FROM clause entities have a property "unknown". '
            r"Searched entities: (?:dingalings(?:, )?"
            r"|users(?:, )?|addresses(?:, )?){3}",
        ):
            stmt.filter_by(unknown="notfound")

    def test_filter_by_four_tables_notfound(self, four_table_statement):
        """test the ellipses version of the not found message"""
        stmt = four_table_statement

        with expect_raises_message(
            exc.InvalidRequestError,
            r'None of the FROM clause entities have a property "unknown". '
            r"Searched entities: "
            r"(?:dingalings, |departments, |users, |addresses, ){3}\.\.\. "
            r"\(4 total\)",
        ):
            stmt.filter_by(unknown="notfound")

    def test_filter_by_three_tables_primary(self, three_table_statement):
        """Test filter_by finds attribute in primary table with three
        tables."""
        stmt = three_table_statement

        # Filter by 'id' - ambiguous across all three tables
        with expect_raises_message(
            exc.AmbiguousColumnError,
            'Attribute name "id" is ambiguous',
        ):
            stmt.filter_by(id=5)

    def test_filter_by_three_tables_secondary(self, three_table_statement):
        """Test filter_by finds attribute in secondary table."""
        stmt = three_table_statement

        # Filter by 'email_address' which only exists in Address
        stmt = stmt.filter_by(email_address="test@example.com")
        self.assert_compile(
            stmt,
            re.compile(
                r"(?:UPDATE|DELETE) .* addresses\.email_address = "
                r":email_address_1"
            ),
        )

    def test_filter_by_three_tables_tertiary(self, three_table_statement):
        """Test filter_by finds attribute in third table (Dingaling)."""
        stmt = three_table_statement

        # Filter by 'data' which only exists in dingalings
        stmt = stmt.filter_by(data="somedata")
        self.assert_compile(
            stmt,
            re.compile(r"(?:UPDATE|DELETE) .* dingalings\.data = :data_1"),
        )

    def test_filter_by_three_tables_user_id(self, three_table_statement):
        """Test filter_by finds user_id in Address (unambiguous)."""
        stmt = three_table_statement

        # Filter by 'user_id' which only exists in addresses
        stmt = stmt.filter_by(user_id=7)
        self.assert_compile(
            stmt,
            re.compile(
                r"(?:UPDATE|DELETE) .* addresses\.user_id = :user_id_1"
            ),
        )

    def test_filter_by_three_tables_address_id(self, three_table_statement):
        """Test filter_by finds address_id in Dingaling (unambiguous)."""
        stmt = three_table_statement

        # Filter by 'address_id' which only exists in dingalings
        stmt = stmt.filter_by(address_id=3)
        self.assert_compile(
            stmt,
            re.compile(
                r"(?:UPDATE|DELETE) .* dingalings\.address_id = "
                r":address_id_1"
            ),
        )


class UpdateFilterByTest(_FilterByDMLSuite):
    @testing.fixture
    def one_table_statement(self):
        User = self.classes.User

        return update(User).values(name="newname")

    @testing.fixture
    def two_table_statement(self):
        User = self.classes.User
        Address = self.classes.Address

        return (
            update(User)
            .values(name="newname")
            .where(User.id == Address.user_id)
        )

    @testing.fixture
    def three_table_statement(self):
        User = self.classes.User
        Address = self.classes.Address
        Dingaling = self.classes.Dingaling

        return (
            update(User)
            .values(name="newname")
            .where(User.id == Address.user_id)
            .where(Address.id == Dingaling.address_id)
        )

    @testing.fixture
    def four_table_statement(self):
        User = self.classes.User
        Address = self.classes.Address
        Dingaling = self.classes.Dingaling
        Department = self.classes.Department

        return (
            update(User)
            .values(name="newname")
            .where(User.id == Address.user_id)
            .where(Address.id == Dingaling.address_id)
            .where(Department.id == User.department_id)
        )


class DeleteFilterByTest(_FilterByDMLSuite):
    @testing.fixture
    def one_table_statement(self):
        User = self.classes.User

        return delete(User)

    @testing.fixture
    def two_table_statement(self):
        User = self.classes.User
        Address = self.classes.Address

        return delete(User).where(User.id == Address.user_id)

    @testing.fixture
    def three_table_statement(self):
        User = self.classes.User
        Address = self.classes.Address
        Dingaling = self.classes.Dingaling

        return (
            delete(User)
            .where(User.id == Address.user_id)
            .where(Address.id == Dingaling.address_id)
        )

    @testing.fixture
    def four_table_statement(self):
        User = self.classes.User
        Address = self.classes.Address
        Dingaling = self.classes.Dingaling
        Department = self.classes.Department

        return (
            delete(User)
            .where(User.id == Address.user_id)
            .where(Address.id == Dingaling.address_id)
            .where(Department.id == User.department_id)
        )
