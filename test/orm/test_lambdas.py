import random

from sqlalchemy import exc
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import lambda_stmt
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy import update
from sqlalchemy.future import select
from sqlalchemy.orm import aliased
from sqlalchemy.orm import mapper
from sqlalchemy.orm import relationship
from sqlalchemy.orm import selectinload
from sqlalchemy.orm import Session
from sqlalchemy.orm import subqueryload
from sqlalchemy.sql.selectable import LABEL_STYLE_TABLENAME_PLUS_COL
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.fixtures import fixture_session
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table
from .inheritance import _poly_fixtures
from .test_query import QueryTest


class LambdaTest(QueryTest, AssertsCompiledSQL):
    __dialect__ = "default"

    # we want to test the lambda expiration logic so use backend
    # to exercise that

    __backend__ = True
    run_setup_mappers = None

    @testing.fixture
    def plain_fixture(self):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        mapper(
            User,
            users,
            properties={
                "addresses": relationship(Address, back_populates="user")
            },
        )

        mapper(
            Address,
            addresses,
            properties={
                "user": relationship(User, back_populates="addresses")
            },
        )

        return User, Address

    def test_user_cols_single_lambda(self, plain_fixture):
        User, Address = plain_fixture

        q = select(lambda: (User.id, User.name)).select_from(lambda: User)

        self.assert_compile(q, "SELECT users.id, users.name FROM users")

    def test_user_cols_single_lambda_query(self, plain_fixture):
        User, Address = plain_fixture

        s = fixture_session()
        q = s.query(lambda: (User.id, User.name)).select_from(lambda: User)

        self.assert_compile(
            q,
            "SELECT users.id AS users_id, users.name AS users_name FROM users",
        )

    def test_multiple_entities_single_lambda(self, plain_fixture):
        User, Address = plain_fixture

        q = select(lambda: (User, Address)).join(lambda: User.addresses)

        self.assert_compile(
            q,
            "SELECT users.id, users.name, addresses.id AS id_1, "
            "addresses.user_id, addresses.email_address "
            "FROM users JOIN addresses ON users.id = addresses.user_id",
        )

    def test_cols_round_trip(self, plain_fixture):
        User, Address = plain_fixture

        s = Session(testing.db, future=True)

        # note this does a traversal + _clone of the InstrumentedAttribute
        # for the first time ever
        def query(names):
            stmt = lambda_stmt(
                lambda: select(User.name, Address.email_address)
                .where(User.name.in_(names))
                .join(User.addresses)
            ) + (lambda s: s.order_by(User.id, Address.id))

            return s.execute(stmt)

        def go1():
            r1 = query(["ed"])
            eq_(
                r1.all(),
                [
                    ("ed", "ed@wood.com"),
                    ("ed", "ed@bettyboop.com"),
                    ("ed", "ed@lala.com"),
                ],
            )

        def go2():
            r1 = query(["ed", "fred"])
            eq_(
                r1.all(),
                [
                    ("ed", "ed@wood.com"),
                    ("ed", "ed@bettyboop.com"),
                    ("ed", "ed@lala.com"),
                    ("fred", "fred@fred.com"),
                ],
            )

        for i in range(5):
            fn = random.choice([go1, go2])
            fn()

    @testing.combinations(
        (True, True),
        (True, False),
        (False, False),
        argnames="use_aliased,use_indirect_access",
    )
    def test_entity_round_trip(
        self, plain_fixture, use_aliased, use_indirect_access
    ):
        User, Address = plain_fixture

        s = Session(testing.db, future=True)

        if use_aliased:
            if use_indirect_access:

                def query(names):
                    class Foo(object):
                        def __init__(self):
                            self.u1 = aliased(User)

                    f1 = Foo()

                    stmt = lambda_stmt(
                        lambda: select(f1.u1)
                        .where(f1.u1.name.in_(names))
                        .options(selectinload(f1.u1.addresses)),
                        track_on=[f1.u1],
                    ).add_criteria(
                        lambda s: s.order_by(f1.u1.id), track_on=[f1.u1]
                    )

                    return s.execute(stmt)

            else:

                def query(names):
                    u1 = aliased(User)
                    stmt = lambda_stmt(
                        lambda: select(u1)
                        .where(u1.name.in_(names))
                        .options(selectinload(u1.addresses))
                    ) + (lambda s: s.order_by(u1.id))

                    return s.execute(stmt)

        else:

            def query(names):
                stmt = lambda_stmt(
                    lambda: select(User)
                    .where(User.name.in_(names))
                    .options(selectinload(User.addresses))
                ) + (lambda s: s.order_by(User.id))

                return s.execute(stmt)

        def go1():
            r1 = query(["ed"])
            eq_(
                r1.scalars().all(),
                [User(name="ed", addresses=[Address(), Address(), Address()])],
            )

        def go2():
            r1 = query(["ed", "fred"])
            eq_(
                r1.scalars().all(),
                [
                    User(
                        name="ed", addresses=[Address(), Address(), Address()]
                    ),
                    User(name="fred", addresses=[Address()]),
                ],
            )

        for i in range(5):
            fn = random.choice([go1, go2])
            self.assert_sql_count(testing.db, fn, 2)

    def test_lambdas_rejected_in_options(self, plain_fixture):
        User, Address = plain_fixture

        assert_raises_message(
            exc.ArgumentError,
            "Cacheable Core or ORM object expected, got",
            select(lambda: User).options,
            lambda: subqueryload(User.addresses),
        )

    def test_subqueryload_internal_lambda(self, plain_fixture):
        User, Address = plain_fixture

        s = Session(testing.db, future=True)

        def query(names):
            stmt = (
                select(lambda: User)
                .where(lambda: User.name.in_(names))
                .options(subqueryload(User.addresses))
                .order_by(lambda: User.id)
            )

            return s.execute(stmt)

        def go1():
            r1 = query(["ed"])
            eq_(
                r1.scalars().all(),
                [User(name="ed", addresses=[Address(), Address(), Address()])],
            )

        def go2():
            r1 = query(["ed", "fred"])
            eq_(
                r1.scalars().all(),
                [
                    User(
                        name="ed", addresses=[Address(), Address(), Address()]
                    ),
                    User(name="fred", addresses=[Address()]),
                ],
            )

        for i in range(5):
            fn = random.choice([go1, go2])
            self.assert_sql_count(testing.db, fn, 2)

    def test_subqueryload_external_lambda_caveats(self, plain_fixture):
        User, Address = plain_fixture

        s = Session(testing.db, future=True)

        def query(names):
            stmt = lambda_stmt(
                lambda: select(User)
                .where(User.name.in_(names))
                .options(subqueryload(User.addresses))
            ) + (lambda s: s.order_by(User.id))

            return s.execute(stmt)

        def go1():
            r1 = query(["ed"])
            eq_(
                r1.scalars().all(),
                [User(name="ed", addresses=[Address(), Address(), Address()])],
            )

        def go2():
            r1 = query(["ed", "fred"])
            eq_(
                r1.scalars().all(),
                [
                    User(
                        name="ed", addresses=[Address(), Address(), Address()]
                    ),
                    User(name="fred", addresses=[Address()]),
                ],
            )

        for i in range(5):
            fn = random.choice([go1, go2])
            with testing.expect_warnings(
                'subqueryloader for "User.addresses" must invoke lambda '
                r"callable at .*LambdaElement\(<code object <lambda> "
                r".*test_lambdas.py.* in order to produce a new query, "
                r"decreasing the efficiency of caching"
            ):
                self.assert_sql_count(testing.db, fn, 2)

    def test_does_filter_aliasing_work(self, plain_fixture):
        User, Address = plain_fixture

        s = Session(testing.db, future=True)

        # aliased=True is to be deprecated, other filter lambdas
        # that go into effect include polymorphic filtering.
        q = (
            s.query(lambda: User)
            .join(lambda: User.addresses, aliased=True)
            .filter(lambda: Address.email_address == "foo")
        )
        self.assert_compile(
            q,
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users JOIN addresses AS addresses_1 "
            "ON users.id = addresses_1.user_id "
            "WHERE addresses_1.email_address = :email_address_1",
        )

    @testing.combinations(
        lambda s, User, Address: s.query(lambda: User).join(lambda: Address),
        lambda s, User, Address: s.query(lambda: User).join(
            lambda: User.addresses
        ),
        lambda s, User, Address: s.query(lambda: User).join(
            lambda: Address, lambda: User.addresses
        ),
        lambda s, User, Address: s.query(lambda: User).join(
            Address, lambda: User.addresses
        ),
        lambda s, User, Address: s.query(lambda: User).join(
            lambda: Address, User.addresses
        ),
        lambda User, Address: select(lambda: User)
        .join(lambda: Address)
        .set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL),
        lambda User, Address: select(lambda: User)
        .join(lambda: User.addresses)
        .set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL),
        lambda User, Address: select(lambda: User)
        .join(lambda: Address, lambda: User.addresses)
        .set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL),
        lambda User, Address: select(lambda: User)
        .join(Address, lambda: User.addresses)
        .set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL),
        lambda User, Address: select(lambda: User)
        .join(lambda: Address, User.addresses)
        .set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL),
        argnames="test_case",
    )
    def test_join_entity_arg(self, plain_fixture, test_case):
        User, Address = plain_fixture

        s = Session(testing.db, future=True)

        stmt = testing.resolve_lambda(test_case, **locals())
        self.assert_compile(
            stmt,
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users JOIN addresses ON users.id = addresses.user_id",
        )


class PolymorphicTest(_poly_fixtures._Polymorphic):
    run_setup_mappers = "once"
    __dialect__ = "default"

    def test_join_second_prop_lambda(self):
        Company = self.classes.Company
        Manager = self.classes.Manager

        s = Session(future=True)

        q = s.query(Company).join(lambda: Manager, lambda: Company.employees)

        self.assert_compile(
            q,
            "SELECT companies.company_id AS companies_company_id, "
            "companies.name AS companies_name FROM companies "
            "JOIN (people JOIN managers ON people.person_id = "
            "managers.person_id) ON companies.company_id = people.company_id",
        )


class UpdateDeleteTest(fixtures.MappedTest):
    __backend__ = True

    run_setup_mappers = "once"

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
        Table(
            "addresses",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("user_id", ForeignKey("users.id")),
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

        Address = cls.classes.Address
        addresses = cls.tables.addresses

        mapper(
            User,
            users,
            properties={
                "age": users.c.age_int,
                "addresses": relationship(Address),
            },
        )
        mapper(Address, addresses)

    def test_update(self):
        User, Address = self.classes("User", "Address")

        s = Session(testing.db, future=True)

        def go(ids, values):
            stmt = lambda_stmt(lambda: update(User).where(User.id.in_(ids)))
            s.execute(
                stmt,
                values,
                # note this currently just unrolls the lambda on the statement.
                # so lambda caching for updates is not actually that useful
                # unless synchronize_session is turned off.
                # evaluate is similar just doesn't work for IN yet.
                execution_options={"synchronize_session": "fetch"},
            )

        go([1, 2], {"name": "jack2"})
        eq_(
            s.execute(select(User.id, User.name).order_by(User.id)).all(),
            [(1, "jack2"), (2, "jack2"), (3, "jill"), (4, "jane")],
        )

        go([3], {"name": "jane2"})
        eq_(
            s.execute(select(User.id, User.name).order_by(User.id)).all(),
            [(1, "jack2"), (2, "jack2"), (3, "jane2"), (4, "jane")],
        )
