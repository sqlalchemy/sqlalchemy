import itertools

import sqlalchemy as sa
from sqlalchemy import and_
from sqlalchemy import desc
from sqlalchemy import exc as sa_exc
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import Integer
from sqlalchemy import lateral
from sqlalchemy import literal_column
from sqlalchemy import MetaData
from sqlalchemy import not_
from sqlalchemy import or_
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import testing
from sqlalchemy import true
from sqlalchemy.engine import default
from sqlalchemy.orm import aliased
from sqlalchemy.orm import backref
from sqlalchemy.orm import join
from sqlalchemy.orm import joinedload
from sqlalchemy.orm import mapper
from sqlalchemy.orm import outerjoin
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session
from sqlalchemy.orm import synonym
from sqlalchemy.sql.selectable import LABEL_STYLE_TABLENAME_PLUS_COL
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.fixtures import fixture_session
from sqlalchemy.testing.schema import Column
from test.orm import _fixtures
from .inheritance import _poly_fixtures
from .test_query import QueryTest


class InheritedTest(_poly_fixtures._Polymorphic):
    run_setup_mappers = "once"


class InheritedJoinTest(InheritedTest, AssertsCompiledSQL):
    def test_single_prop(self):
        Company = self.classes.Company

        sess = fixture_session()

        self.assert_compile(
            sess.query(Company).join(Company.employees),
            "SELECT companies.company_id AS companies_company_id, "
            "companies.name AS companies_name "
            "FROM companies JOIN people "
            "ON companies.company_id = people.company_id",
            use_default_dialect=True,
        )

    def test_join_to_selectable(self):
        people, Company, engineers, Engineer = (
            self.tables.people,
            self.classes.Company,
            self.tables.engineers,
            self.classes.Engineer,
        )

        sess = fixture_session()

        self.assert_compile(
            sess.query(Company)
            .join(people.join(engineers), Company.employees)
            .filter(Engineer.name == "dilbert"),
            "SELECT companies.company_id AS companies_company_id, "
            "companies.name AS companies_name "
            "FROM companies JOIN (people "
            "JOIN engineers ON people.person_id = "
            "engineers.person_id) ON companies.company_id = "
            "people.company_id WHERE people.name = :name_1",
            use_default_dialect=True,
        )

    def test_force_via_select_from(self):
        Company, Engineer = self.classes.Company, self.classes.Engineer

        sess = fixture_session()

        self.assert_compile(
            sess.query(Company)
            .filter(Company.company_id == Engineer.company_id)
            .filter(Engineer.primary_language == "java"),
            "SELECT companies.company_id AS companies_company_id, "
            "companies.name AS companies_name "
            "FROM companies, people, engineers "
            "WHERE companies.company_id = people.company_id "
            "AND engineers.primary_language "
            "= :primary_language_1",
            use_default_dialect=True,
        )

        self.assert_compile(
            sess.query(Company)
            .select_from(Company, Engineer)
            .filter(Company.company_id == Engineer.company_id)
            .filter(Engineer.primary_language == "java"),
            "SELECT companies.company_id AS companies_company_id, "
            "companies.name AS companies_name "
            "FROM companies, people JOIN engineers "
            "ON people.person_id = engineers.person_id "
            "WHERE companies.company_id = people.company_id "
            "AND engineers.primary_language ="
            " :primary_language_1",
            use_default_dialect=True,
        )

    def test_single_prop_of_type(self):
        Company, Engineer = self.classes.Company, self.classes.Engineer

        sess = fixture_session()

        self.assert_compile(
            sess.query(Company).join(Company.employees.of_type(Engineer)),
            "SELECT companies.company_id AS companies_company_id, "
            "companies.name AS companies_name "
            "FROM companies JOIN "
            "(people JOIN engineers "
            "ON people.person_id = engineers.person_id) "
            "ON companies.company_id = people.company_id",
            use_default_dialect=True,
        )

    def test_explicit_polymorphic_join_one(self):
        Company, Engineer = self.classes.Company, self.classes.Engineer

        sess = fixture_session()

        self.assert_compile(
            sess.query(Company)
            .join(Engineer)
            .filter(Engineer.engineer_name == "vlad"),
            "SELECT companies.company_id AS companies_company_id, "
            "companies.name AS companies_name "
            "FROM companies JOIN (people JOIN engineers "
            "ON people.person_id = engineers.person_id) "
            "ON "
            "companies.company_id = people.company_id "
            "WHERE engineers.engineer_name = :engineer_name_1",
            use_default_dialect=True,
        )

    def test_explicit_polymorphic_join_two(self):
        Company, Engineer = self.classes.Company, self.classes.Engineer

        sess = fixture_session()
        self.assert_compile(
            sess.query(Company)
            .join(Engineer, Company.company_id == Engineer.company_id)
            .filter(Engineer.engineer_name == "vlad"),
            "SELECT companies.company_id AS companies_company_id, "
            "companies.name AS companies_name "
            "FROM companies JOIN "
            "(people JOIN engineers "
            "ON people.person_id = engineers.person_id) "
            "ON "
            "companies.company_id = people.company_id "
            "WHERE engineers.engineer_name = :engineer_name_1",
            use_default_dialect=True,
        )

    def test_auto_aliasing_multi_link(self):
        # test [ticket:2903]
        sess = fixture_session()

        Company, Engineer, Manager, Boss = (
            self.classes.Company,
            self.classes.Engineer,
            self.classes.Manager,
            self.classes.Boss,
        )
        q = (
            sess.query(Company)
            .join(Company.employees.of_type(Engineer))
            .join(Company.employees.of_type(Manager))
            .join(Company.employees.of_type(Boss))
        )

        self.assert_compile(
            q,
            "SELECT companies.company_id AS companies_company_id, "
            "companies.name AS companies_name FROM companies "
            "JOIN (people JOIN engineers "
            "ON people.person_id = engineers.person_id) "
            "ON companies.company_id = people.company_id "
            "JOIN (people AS people_1 JOIN managers AS managers_1 "
            "ON people_1.person_id = managers_1.person_id) "
            "ON companies.company_id = people_1.company_id "
            "JOIN (people AS people_2 JOIN managers AS managers_2 "
            "ON people_2.person_id = managers_2.person_id JOIN boss AS boss_1 "
            "ON managers_2.person_id = boss_1.boss_id) "
            "ON companies.company_id = people_2.company_id",
            use_default_dialect=True,
        )


class JoinOnSynonymTest(_fixtures.FixtureTest, AssertsCompiledSQL):
    __dialect__ = "default"

    @classmethod
    def setup_mappers(cls):
        User = cls.classes.User
        Address = cls.classes.Address
        users, addresses = (cls.tables.users, cls.tables.addresses)
        mapper(
            User,
            users,
            properties={
                "addresses": relationship(Address),
                "ad_syn": synonym("addresses"),
            },
        )
        mapper(Address, addresses)

    def test_join_on_synonym(self):
        User = self.classes.User
        self.assert_compile(
            fixture_session().query(User).join(User.ad_syn),
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users JOIN addresses ON users.id = addresses.user_id",
        )


class JoinTest(QueryTest, AssertsCompiledSQL):
    __dialect__ = "default"

    @testing.combinations_list(
        set(
            itertools.product(
                [
                    "relationship",
                    "relationship_only",
                    "string_relationship",
                    "string_relationship_only",
                    "none",
                    "explicit",
                    "table_none",
                    "table_explicit",
                ],
                [True, False],
            )
        ).difference(
            [
                ("string_relationship", False),
                ("string_relationship_only", False),
            ]
        ),
        argnames="onclause_type, use_legacy",
    )
    def test_filter_by_from_join(self, onclause_type, use_legacy):
        User, Address = self.classes("User", "Address")
        (address_table,) = self.tables("addresses")
        (user_table,) = self.tables("users")

        if use_legacy:
            sess = fixture_session()
            q = sess.query(User)
        else:
            q = select(User).set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)

        if onclause_type == "relationship":
            q = q.join(Address, User.addresses)
        elif onclause_type == "string_relationship":
            q = q.join(Address, "addresses")
        elif onclause_type == "relationship_only":
            q = q.join(User.addresses)
        elif onclause_type == "string_relationship_only":
            q = q.join("addresses")
        elif onclause_type == "none":
            q = q.join(Address)
        elif onclause_type == "explicit":
            q = q.join(Address, User.id == Address.user_id)
        elif onclause_type == "table_none":
            q = q.join(address_table)
        elif onclause_type == "table_explicit":
            q = q.join(
                address_table, user_table.c.id == address_table.c.user_id
            )
        else:
            assert False

        q2 = q.filter_by(email_address="foo")

        self.assert_compile(
            q2,
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users JOIN addresses ON users.id = addresses.user_id "
            "WHERE addresses.email_address = :email_address_1",
        )

        if use_legacy:
            q2 = q.reset_joinpoint().filter_by(name="user")
            self.assert_compile(
                q2,
                "SELECT users.id AS users_id, users.name AS users_name "
                "FROM users JOIN addresses ON users.id = addresses.user_id "
                "WHERE users.name = :name_1",
            )

    def test_invalid_kwarg_join(self):
        User = self.classes.User
        sess = fixture_session()
        assert_raises_message(
            TypeError,
            "unknown arguments: bar, foob",
            sess.query(User).join,
            "address",
            foob="bar",
            bar="bat",
        )
        assert_raises_message(
            TypeError,
            "unknown arguments: bar, foob",
            sess.query(User).outerjoin,
            "address",
            foob="bar",
            bar="bat",
        )

    def test_left_w_no_entity(self):
        User = self.classes.User
        Address = self.classes.Address

        sess = fixture_session()

        self.assert_compile(
            sess.query(User, literal_column("x")).join(Address),
            "SELECT users.id AS users_id, users.name AS users_name, x "
            "FROM users JOIN addresses ON users.id = addresses.user_id",
        )

        self.assert_compile(
            sess.query(literal_column("x"), User).join(Address),
            "SELECT x, users.id AS users_id, users.name AS users_name "
            "FROM users JOIN addresses ON users.id = addresses.user_id",
        )

    def test_left_is_none_and_query_has_no_entities(self):
        Address = self.classes.Address

        sess = fixture_session()

        assert_raises_message(
            sa_exc.InvalidRequestError,
            r"No entities to join from; please use select_from\(\) to "
            r"establish the left entity/selectable of this join",
            sess.query().join(Address)._compile_context,
        )

    def test_isouter_flag(self):
        User = self.classes.User

        self.assert_compile(
            fixture_session().query(User).join(User.orders, isouter=True),
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users LEFT OUTER JOIN orders ON users.id = orders.user_id",
        )

    def test_full_flag(self):
        User = self.classes.User

        self.assert_compile(
            fixture_session().query(User).outerjoin(User.orders, full=True),
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users FULL OUTER JOIN orders ON users.id = orders.user_id",
        )

    def test_single_prop_1(self):
        User = self.classes.User

        sess = fixture_session()
        self.assert_compile(
            sess.query(User).join(User.orders),
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users JOIN orders ON users.id = orders.user_id",
        )

    def test_single_prop_2(self):
        Order, User = (self.classes.Order, self.classes.User)

        sess = fixture_session()
        self.assert_compile(
            sess.query(User).join(Order.user),
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM orders JOIN users ON users.id = orders.user_id",
        )

    def test_single_prop_3(self):
        Order, User = (self.classes.Order, self.classes.User)

        sess = fixture_session()
        oalias1 = aliased(Order)

        self.assert_compile(
            sess.query(User).join(oalias1.user),
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM orders AS orders_1 JOIN users "
            "ON users.id = orders_1.user_id",
        )

    def test_single_prop_4(self):
        (
            Order,
            User,
        ) = (self.classes.Order, self.classes.User)

        sess = fixture_session()
        oalias1 = aliased(Order)
        oalias2 = aliased(Order)
        # another nonsensical query.  (from [ticket:1537]).
        # in this case, the contract of "left to right" is honored
        self.assert_compile(
            sess.query(User).join(oalias1.user).join(oalias2.user),
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM orders AS orders_1 JOIN users "
            "ON users.id = orders_1.user_id, "
            "orders AS orders_2 JOIN users ON users.id = orders_2.user_id",
        )

    def test_single_prop_6(self):
        User = self.classes.User

        sess = fixture_session()
        ualias = aliased(User)
        self.assert_compile(
            sess.query(ualias).join(ualias.orders),
            "SELECT users_1.id AS users_1_id, users_1.name AS users_1_name "
            "FROM users AS users_1 JOIN orders ON users_1.id = orders.user_id",
        )

    def test_single_prop_9(self):
        User = self.classes.User

        sess = fixture_session()

        subq = (
            sess.query(User)
            .filter(User.name == "ed")
            .set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)
            .subquery()
        )

        ua = aliased(User, subq)

        self.assert_compile(
            sess.query(ua).join(ua.orders),
            "SELECT anon_1.users_id AS anon_1_users_id, "
            "anon_1.users_name AS anon_1_users_name "
            "FROM (SELECT users.id AS users_id, users.name AS users_name "
            "FROM users "
            "WHERE users.name = :name_1) AS anon_1 JOIN orders "
            "ON anon_1.users_id = orders.user_id",
        )

    def test_single_prop_12(self):
        Order, User, Address = (
            self.classes.Order,
            self.classes.User,
            self.classes.Address,
        )

        sess = fixture_session()
        oalias1 = aliased(Order)
        # test #1 for [ticket:1706]
        ualias = aliased(User)
        self.assert_compile(
            sess.query(ualias)
            .join(oalias1, ualias.orders)
            .join(Address, ualias.addresses),
            "SELECT users_1.id AS users_1_id, users_1.name AS "
            "users_1_name FROM users AS users_1 JOIN orders AS orders_1 "
            "ON users_1.id = orders_1.user_id JOIN addresses ON users_1.id "
            "= addresses.user_id",
        )

    def test_single_prop_13(self):
        Order, User, Address = (
            self.classes.Order,
            self.classes.User,
            self.classes.Address,
        )

        sess = fixture_session()
        # test #2 for [ticket:1706]
        ualias = aliased(User)
        ualias2 = aliased(User)
        self.assert_compile(
            sess.query(ualias)
            .join(Address, ualias.addresses)
            .join(ualias2, Address.user)
            .join(Order, ualias.orders),
            "SELECT users_1.id AS users_1_id, users_1.name AS users_1_name "
            "FROM users "
            "AS users_1 JOIN addresses ON users_1.id = addresses.user_id "
            "JOIN users AS users_2 "
            "ON users_2.id = addresses.user_id JOIN orders "
            "ON users_1.id = orders.user_id",
        )

    def test_overlapping_paths_one(self):
        User = self.classes.User
        Order = self.classes.Order

        sess = fixture_session()

        # test overlapping paths.   User->orders is used by both joins, but
        # rendered once.
        self.assert_compile(
            sess.query(User)
            .join(User.orders)
            .join(Order.items)
            .join(User.orders)
            .join(Order.address),
            "SELECT users.id AS users_id, users.name AS users_name FROM users "
            "JOIN orders "
            "ON users.id = orders.user_id "
            "JOIN order_items AS order_items_1 "
            "ON orders.id = order_items_1.order_id "
            "JOIN items ON items.id = order_items_1.item_id JOIN addresses "
            "ON addresses.id = orders.address_id",
        )

    def test_overlapping_paths_multilevel(self):
        User = self.classes.User
        Order = self.classes.Order
        Address = self.classes.Address

        s = fixture_session()
        q = (
            s.query(User)
            .join(User.orders)
            .join(User.addresses)
            .join(User.orders)
            .join(Order.items)
            .join(User.addresses)
            .join(Address.dingaling)
        )
        self.assert_compile(
            q,
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users JOIN orders ON users.id = orders.user_id "
            "JOIN addresses ON users.id = addresses.user_id "
            "JOIN order_items AS order_items_1 ON orders.id = "
            "order_items_1.order_id "
            "JOIN items ON items.id = order_items_1.item_id "
            "JOIN dingalings ON addresses.id = dingalings.address_id",
        )

    def test_join_nonmapped_column(self):
        """test that the search for a 'left' doesn't trip on non-mapped cols"""

        Order, User = self.classes.Order, self.classes.User

        sess = fixture_session()

        # intentionally join() with a non-existent "left" side
        self.assert_compile(
            sess.query(User.id, literal_column("foo")).join(Order.user),
            "SELECT users.id AS users_id, foo FROM "
            "orders JOIN users ON users.id = orders.user_id",
        )

    def test_backwards_join(self):
        User, Address = self.classes.User, self.classes.Address

        # a more controversial feature.  join from
        # User->Address, but the onclause is Address.user.

        sess = fixture_session()

        eq_(
            sess.query(User)
            .join(Address.user)
            .filter(Address.email_address == "ed@wood.com")
            .all(),
            [User(id=8, name="ed")],
        )

        # its actually not so controversial if you view it in terms
        # of multiple entities.
        eq_(
            sess.query(User, Address)
            .join(Address.user)
            .filter(Address.email_address == "ed@wood.com")
            .all(),
            [(User(id=8, name="ed"), Address(email_address="ed@wood.com"))],
        )

        # this was the controversial part.  now, raise an error if the feature
        # is abused.
        # before the error raise was added, this would silently work.....
        assert_raises(
            sa_exc.InvalidRequestError,
            sess.query(User).join(Address, Address.user)._compile_context,
        )

        # but this one would silently fail
        adalias = aliased(Address)
        assert_raises(
            sa_exc.InvalidRequestError,
            sess.query(User).join(adalias, Address.user)._compile_context,
        )

    def test_multiple_with_aliases(self):
        Order, User = self.classes.Order, self.classes.User

        sess = fixture_session()

        ualias = aliased(User)
        oalias1 = aliased(Order)
        oalias2 = aliased(Order)
        self.assert_compile(
            sess.query(ualias)
            .join(oalias1, ualias.orders)
            .join(oalias2, ualias.orders)
            .filter(or_(oalias1.user_id == 9, oalias2.user_id == 7)),
            "SELECT users_1.id AS users_1_id, users_1.name AS users_1_name "
            "FROM users AS users_1 "
            "JOIN orders AS orders_1 ON users_1.id = orders_1.user_id "
            "JOIN orders AS orders_2 ON "
            "users_1.id = orders_2.user_id "
            "WHERE orders_1.user_id = :user_id_1 "
            "OR orders_2.user_id = :user_id_2",
            use_default_dialect=True,
        )

    def test_select_from_orm_joins(self):
        User, Order = self.classes.User, self.classes.Order

        sess = fixture_session()

        ualias = aliased(User)
        oalias1 = aliased(Order)
        oalias2 = aliased(Order)

        self.assert_compile(
            join(User, oalias2, User.id == oalias2.user_id),
            "users JOIN orders AS orders_1 ON users.id = orders_1.user_id",
            use_default_dialect=True,
        )

        self.assert_compile(
            join(User, oalias2, User.id == oalias2.user_id, full=True),
            "users FULL OUTER JOIN orders AS orders_1 "
            "ON users.id = orders_1.user_id",
            use_default_dialect=True,
        )

        self.assert_compile(
            join(User, oalias2, User.id == oalias2.user_id, isouter=True),
            "users LEFT OUTER JOIN orders AS orders_1 "
            "ON users.id = orders_1.user_id",
            use_default_dialect=True,
        )

        self.assert_compile(
            join(
                User,
                oalias2,
                User.id == oalias2.user_id,
                isouter=True,
                full=True,
            ),
            "users FULL OUTER JOIN orders AS orders_1 "
            "ON users.id = orders_1.user_id",
            use_default_dialect=True,
        )

        self.assert_compile(
            join(User, oalias1).join(oalias2),
            "users JOIN orders AS orders_1 ON users.id = orders_1.user_id "
            "JOIN orders AS orders_2 ON users.id = orders_2.user_id",
            use_default_dialect=True,
        )

        self.assert_compile(
            join(User, oalias1).join(oalias2, isouter=True),
            "users JOIN orders AS orders_1 ON users.id = orders_1.user_id "
            "LEFT OUTER JOIN orders AS orders_2 "
            "ON users.id = orders_2.user_id",
            use_default_dialect=True,
        )

        self.assert_compile(
            join(User, oalias1).join(oalias2, full=True),
            "users JOIN orders AS orders_1 ON users.id = orders_1.user_id "
            "FULL OUTER JOIN orders AS orders_2 "
            "ON users.id = orders_2.user_id",
            use_default_dialect=True,
        )

        self.assert_compile(
            join(User, oalias1).join(oalias2, full=True, isouter=True),
            "users JOIN orders AS orders_1 ON users.id = orders_1.user_id "
            "FULL OUTER JOIN orders AS orders_2 "
            "ON users.id = orders_2.user_id",
            use_default_dialect=True,
        )

        self.assert_compile(
            join(ualias, oalias1, ualias.orders),
            "users AS users_1 JOIN orders AS orders_1 "
            "ON users_1.id = orders_1.user_id",
            use_default_dialect=True,
        )

        self.assert_compile(
            sess.query(ualias).select_from(
                join(ualias, oalias1, ualias.orders)
            ),
            "SELECT users_1.id AS users_1_id, users_1.name AS users_1_name "
            "FROM users AS users_1 "
            "JOIN orders AS orders_1 ON users_1.id = orders_1.user_id",
            use_default_dialect=True,
        )

        self.assert_compile(
            sess.query(User, ualias).select_from(
                join(ualias, oalias1, ualias.orders)
            ),
            "SELECT users.id AS users_id, users.name AS users_name, "
            "users_1.id AS users_1_id, "
            "users_1.name AS users_1_name FROM users, users AS users_1 "
            "JOIN orders AS orders_1 ON users_1.id = orders_1.user_id",
            use_default_dialect=True,
        )

        # this fails (and we cant quite fix right now).
        if False:
            self.assert_compile(
                sess.query(User, ualias)
                .join(oalias1, ualias.orders)
                .join(oalias2, User.id == oalias2.user_id)
                .filter(or_(oalias1.user_id == 9, oalias2.user_id == 7)),
                "SELECT users.id AS users_id, users.name AS users_name, "
                "users_1.id AS users_1_id, users_1.name AS "
                "users_1_name FROM users JOIN orders AS orders_2 "
                "ON users.id = orders_2.user_id, "
                "users AS users_1 JOIN orders AS orders_1 "
                "ON users_1.id = orders_1.user_id  "
                "WHERE orders_1.user_id = :user_id_1 "
                "OR orders_2.user_id = :user_id_2",
                use_default_dialect=True,
            )

        # this is the same thing using explicit orm.join() (which now offers
        # multiple again)
        self.assert_compile(
            sess.query(User, ualias)
            .select_from(
                join(ualias, oalias1, ualias.orders),
                join(User, oalias2, User.id == oalias2.user_id),
            )
            .filter(or_(oalias1.user_id == 9, oalias2.user_id == 7)),
            "SELECT users.id AS users_id, users.name AS users_name, "
            "users_1.id AS users_1_id, users_1.name AS "
            "users_1_name FROM users AS users_1 JOIN orders AS orders_1 "
            "ON users_1.id = orders_1.user_id, "
            "users JOIN orders AS orders_2 ON users.id = orders_2.user_id "
            "WHERE orders_1.user_id = :user_id_1 "
            "OR orders_2.user_id = :user_id_2",
            use_default_dialect=True,
        )

    def test_overlapping_backwards_joins(self):
        User, Order = self.classes.User, self.classes.Order

        sess = fixture_session()

        oalias1 = aliased(Order)
        oalias2 = aliased(Order)

        # this is invalid SQL - joins from orders_1/orders_2 to User twice.
        # but that is what was asked for so they get it !
        self.assert_compile(
            sess.query(User).join(oalias1.user).join(oalias2.user),
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM orders AS orders_1 "
            "JOIN users ON users.id = orders_1.user_id, orders AS orders_2 "
            "JOIN users ON users.id = orders_2.user_id",
            use_default_dialect=True,
        )

    def test_replace_multiple_from_clause(self):
        """test adding joins onto multiple FROM clauses"""

        User, Order, Address = (
            self.classes.User,
            self.classes.Order,
            self.classes.Address,
        )

        sess = fixture_session()

        self.assert_compile(
            sess.query(Address, User)
            .join(Address.dingaling)
            .join(User.orders)
            .join(Order.items),
            "SELECT addresses.id AS addresses_id, "
            "addresses.user_id AS addresses_user_id, "
            "addresses.email_address AS addresses_email_address, "
            "users.id AS users_id, "
            "users.name AS users_name FROM addresses JOIN dingalings "
            "ON addresses.id = dingalings.address_id, "
            "users JOIN orders ON users.id = orders.user_id "
            "JOIN order_items AS order_items_1 "
            "ON orders.id = order_items_1.order_id JOIN items "
            "ON items.id = order_items_1.item_id",
            use_default_dialect=True,
        )

    def test_invalid_join_entity_from_single_from_clause(self):
        Address, Item = (self.classes.Address, self.classes.Item)
        sess = fixture_session()

        q = sess.query(Address).select_from(Address)

        assert_raises_message(
            sa.exc.InvalidRequestError,
            "Don't know how to join to .*Item.*. "
            r"Please use the .select_from\(\) "
            "method to establish an explicit left side, as well as",
            q.join(Item)._compile_context,
        )

    def test_invalid_join_entity_from_no_from_clause(self):
        Address, Item = (self.classes.Address, self.classes.Item)
        sess = fixture_session()

        q = sess.query(Address)

        assert_raises_message(
            sa.exc.InvalidRequestError,
            "Don't know how to join to .*Item.*. "
            r"Please use the .select_from\(\) "
            "method to establish an explicit left side, as well as",
            q.join(Item)._compile_context,
        )

    def test_invalid_join_entity_from_multiple_from_clause(self):
        """test adding joins onto multiple FROM clauses where
        we still need to say there's nothing to JOIN from"""

        User, Address, Item = (
            self.classes.User,
            self.classes.Address,
            self.classes.Item,
        )
        sess = fixture_session()

        q = sess.query(Address, User).join(Address.dingaling).join(User.orders)

        assert_raises_message(
            sa.exc.InvalidRequestError,
            "Don't know how to join to .*Item.*. "
            r"Please use the .select_from\(\) "
            "method to establish an explicit left side, as well as",
            q.join(Item)._compile_context,
        )

    def test_join_explicit_left_multiple_from_clause(self):
        """test adding joins onto multiple FROM clauses where
        it is ambiguous which FROM should be used when an
        ON clause is given"""

        User = self.classes.User

        sess = fixture_session()

        u1 = aliased(User)

        # in this case, two FROM objects, one
        # is users, the other is u1_alias.
        # User.addresses looks for the "users" table and can match
        # to both u1_alias and users if the match is not specific enough
        q = sess.query(User, u1).select_from(User, u1).join(User.addresses)

        self.assert_compile(
            q,
            "SELECT users.id AS users_id, users.name AS users_name, "
            "users_1.id AS users_1_id, users_1.name AS users_1_name "
            "FROM users AS users_1, "
            "users JOIN addresses ON users.id = addresses.user_id",
        )

        q = sess.query(User, u1).select_from(User, u1).join(u1.addresses)

        self.assert_compile(
            q,
            "SELECT users.id AS users_id, users.name AS users_name, "
            "users_1.id AS users_1_id, users_1.name AS users_1_name "
            "FROM users, "
            "users AS users_1 JOIN addresses "
            "ON users_1.id = addresses.user_id",
        )

    def test_join_explicit_left_multiple_adapted(self):
        """test adding joins onto multiple FROM clauses where
        it is ambiguous which FROM should be used when an
        ON clause is given"""

        User = self.classes.User

        sess = fixture_session()

        u1 = aliased(User)
        u2 = aliased(User)

        # in this case, two FROM objects, one
        # is users, the other is u1_alias.
        # User.addresses looks for the "users" table and can match
        # to both u1_alias and users if the match is not specific enough
        assert_raises_message(
            sa_exc.InvalidRequestError,
            "Can't identify which entity in which to assign the "
            "left side of this join.",
            sess.query(u1, u2)
            .select_from(u1, u2)
            .join(User.addresses)
            ._compile_context,
        )

        # more specific ON clause
        self.assert_compile(
            sess.query(u1, u2).select_from(u1, u2).join(u2.addresses),
            "SELECT users_1.id AS users_1_id, users_1.name AS users_1_name, "
            "users_2.id AS users_2_id, users_2.name AS users_2_name "
            "FROM users AS users_1, "
            "users AS users_2 JOIN addresses "
            "ON users_2.id = addresses.user_id",
        )

    def test_join_entity_from_multiple_from_clause(self):
        """test adding joins onto multiple FROM clauses where
        it is ambiguous which FROM should be used"""

        User, Order, Address, Dingaling = (
            self.classes.User,
            self.classes.Order,
            self.classes.Address,
            self.classes.Dingaling,
        )

        sess = fixture_session()

        q = sess.query(Address, User).join(Address.dingaling).join(User.orders)

        a1 = aliased(Address)

        assert_raises_message(
            sa.exc.InvalidRequestError,
            "Can't determine which FROM clause to join from, there are "
            "multiple FROMS which can join to this entity. "
            r"Please use the .select_from\(\) "
            "method to establish an explicit left side, as well as",
            q.join(a1)._compile_context,
        )

        # to resolve, add an ON clause

        # the user->orders join is chosen to join to a1
        self.assert_compile(
            q.join(a1, Order.address_id == a1.id),
            "SELECT addresses.id AS addresses_id, "
            "addresses.user_id AS addresses_user_id, "
            "addresses.email_address AS addresses_email_address, "
            "users.id AS users_id, users.name AS users_name "
            "FROM addresses JOIN dingalings "
            "ON addresses.id = dingalings.address_id, "
            "users JOIN orders "
            "ON users.id = orders.user_id "
            "JOIN addresses AS addresses_1 "
            "ON orders.address_id = addresses_1.id",
        )

        # the address->dingalings join is chosen to join to a1
        self.assert_compile(
            q.join(a1, Dingaling.address_id == a1.id),
            "SELECT addresses.id AS addresses_id, "
            "addresses.user_id AS addresses_user_id, "
            "addresses.email_address AS addresses_email_address, "
            "users.id AS users_id, users.name AS users_name "
            "FROM addresses JOIN dingalings "
            "ON addresses.id = dingalings.address_id "
            "JOIN addresses AS addresses_1 "
            "ON dingalings.address_id = addresses_1.id, "
            "users JOIN orders ON users.id = orders.user_id",
        )

    def test_join_entity_from_multiple_entities(self):
        """test adding joins onto multiple FROM clauses where
        it is ambiguous which FROM should be used"""

        Order, Address, Dingaling = (
            self.classes.Order,
            self.classes.Address,
            self.classes.Dingaling,
        )

        sess = fixture_session()

        q = sess.query(Order, Dingaling)

        a1 = aliased(Address)

        assert_raises_message(
            sa.exc.InvalidRequestError,
            "Can't determine which FROM clause to join from, there are "
            "multiple FROMS which can join to this entity. "
            r"Please use the .select_from\(\) "
            "method to establish an explicit left side, as well as",
            q.join(a1)._compile_context,
        )

        # to resolve, add an ON clause

        # Order is chosen to join to a1
        self.assert_compile(
            q.join(a1, Order.address_id == a1.id),
            "SELECT orders.id AS orders_id, orders.user_id AS orders_user_id, "
            "orders.address_id AS orders_address_id, "
            "orders.description AS orders_description, "
            "orders.isopen AS orders_isopen, dingalings.id AS dingalings_id, "
            "dingalings.address_id AS dingalings_address_id, "
            "dingalings.data AS dingalings_data "
            "FROM dingalings, orders "
            "JOIN addresses AS addresses_1 "
            "ON orders.address_id = addresses_1.id",
        )

        # Dingaling is chosen to join to a1
        self.assert_compile(
            q.join(a1, Dingaling.address_id == a1.id),
            "SELECT orders.id AS orders_id, orders.user_id AS orders_user_id, "
            "orders.address_id AS orders_address_id, "
            "orders.description AS orders_description, "
            "orders.isopen AS orders_isopen, dingalings.id AS dingalings_id, "
            "dingalings.address_id AS dingalings_address_id, "
            "dingalings.data AS dingalings_data "
            "FROM orders, dingalings JOIN addresses AS addresses_1 "
            "ON dingalings.address_id = addresses_1.id",
        )

    def test_clause_present_in_froms_twice_w_onclause(self):
        # test [ticket:4584]
        Order, Address, User = (
            self.classes.Order,
            self.classes.Address,
            self.classes.User,
        )

        sess = fixture_session()

        a1 = aliased(Address)

        q = sess.query(Order).select_from(Order, a1, User)
        assert_raises_message(
            sa.exc.InvalidRequestError,
            "Can't determine which FROM clause to join from, there are "
            "multiple FROMS which can join to this entity. "
            r"Please use the .select_from\(\) "
            "method to establish an explicit left side, as well as",
            q.outerjoin(a1)._compile_context,
        )

        # the condition which occurs here is: Query._from_obj contains both
        # "a1" by itself as well as a join that "a1" is part of.
        # find_left_clause_to_join_from() needs to include removal of froms
        # that are in the _hide_froms of joins the same way
        # Selectable._get_display_froms does.
        q = sess.query(Order).select_from(Order, a1, User)
        q = q.outerjoin(a1, a1.id == Order.address_id)
        q = q.outerjoin(User, a1.user_id == User.id)

        self.assert_compile(
            q,
            "SELECT orders.id AS orders_id, orders.user_id AS orders_user_id, "
            "orders.address_id AS orders_address_id, "
            "orders.description AS orders_description, "
            "orders.isopen AS orders_isopen "
            "FROM orders "
            "LEFT OUTER JOIN addresses AS addresses_1 "
            "ON addresses_1.id = orders.address_id "
            "LEFT OUTER JOIN users ON addresses_1.user_id = users.id",
        )

    def test_clause_present_in_froms_twice_wo_onclause(self):
        # test [ticket:4584]
        Address, Dingaling, User = (
            self.classes.Address,
            self.classes.Dingaling,
            self.classes.User,
        )

        sess = fixture_session()

        a1 = aliased(Address)

        # the condition which occurs here is: Query._from_obj contains both
        # "a1" by itself as well as a join that "a1" is part of.
        # find_left_clause_to_join_from() needs to include removal of froms
        # that are in the _hide_froms of joins the same way
        # Selectable._get_display_froms does.
        q = sess.query(User).select_from(Dingaling, a1, User)
        q = q.outerjoin(a1, User.id == a1.user_id)
        q = q.outerjoin(Dingaling)

        self.assert_compile(
            q,
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users LEFT OUTER JOIN addresses AS addresses_1 "
            "ON users.id = addresses_1.user_id "
            "LEFT OUTER JOIN dingalings "
            "ON addresses_1.id = dingalings.address_id",
        )

    def test_pure_expression(self):
        # this was actually false-passing due to the assertions
        # fixture not following the regular codepath for Query
        addresses, users = self.tables.addresses, self.tables.users

        sess = fixture_session()

        self.assert_compile(
            sess.query(users).join(addresses),
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users JOIN addresses ON users.id = addresses.user_id",
        )

    def test_no_onclause(self):
        Item, User, Order = (
            self.classes.Item,
            self.classes.User,
            self.classes.Order,
        )

        sess = fixture_session()

        eq_(
            sess.query(User)
            .select_from(join(User, Order).join(Item, Order.items))
            .filter(Item.description == "item 4")
            .all(),
            [User(name="jack")],
        )

        eq_(
            sess.query(User.name)
            .select_from(join(User, Order).join(Item, Order.items))
            .filter(Item.description == "item 4")
            .all(),
            [("jack",)],
        )

        eq_(
            sess.query(User)
            .join(Order)
            .join(Item, Order.items)
            .filter(Item.description == "item 4")
            .all(),
            [User(name="jack")],
        )

    def test_clause_onclause(self):
        Item, Order, users, order_items, User = (
            self.classes.Item,
            self.classes.Order,
            self.tables.users,
            self.tables.order_items,
            self.classes.User,
        )

        sess = fixture_session()

        eq_(
            sess.query(User)
            .join(Order, User.id == Order.user_id)
            .join(order_items, Order.id == order_items.c.order_id)
            .join(Item, order_items.c.item_id == Item.id)
            .filter(Item.description == "item 4")
            .all(),
            [User(name="jack")],
        )

        eq_(
            sess.query(User.name)
            .join(Order, User.id == Order.user_id)
            .join(order_items, Order.id == order_items.c.order_id)
            .join(Item, order_items.c.item_id == Item.id)
            .filter(Item.description == "item 4")
            .all(),
            [("jack",)],
        )

        ualias = aliased(User)
        eq_(
            sess.query(ualias.name)
            .join(Order, ualias.id == Order.user_id)
            .join(order_items, Order.id == order_items.c.order_id)
            .join(Item, order_items.c.item_id == Item.id)
            .filter(Item.description == "item 4")
            .all(),
            [("jack",)],
        )

        # explicit onclause with from_self(), means
        # the onclause must be aliased against the query's custom
        # FROM object
        subq = sess.query(User).order_by(User.id).offset(2).subquery()
        ua = aliased(User, subq)
        eq_(
            sess.query(ua).join(Order, ua.id == Order.user_id).all(),
            [User(name="fred")],
        )

        # same with an explicit select_from()
        eq_(
            sess.query(User)
            .select_entity_from(
                select(users).order_by(User.id).offset(2).alias()
            )
            .join(Order, User.id == Order.user_id)
            .all(),
            [User(name="fred")],
        )

    def test_aliased_classes(self):
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()

        (user7, user8, user9, user10) = sess.query(User).all()
        (address1, address2, address3, address4, address5) = sess.query(
            Address
        ).all()
        expected = [
            (user7, address1),
            (user8, address2),
            (user8, address3),
            (user8, address4),
            (user9, address5),
            (user10, None),
        ]

        q = sess.query(User)
        AdAlias = aliased(Address)
        q = q.add_entity(AdAlias).select_from(outerjoin(User, AdAlias))
        result = q.order_by(User.id, AdAlias.id).all()
        eq_(result, expected)

        sess.expunge_all()

        q = sess.query(User).add_entity(AdAlias)
        result = (
            q.select_from(outerjoin(User, AdAlias))
            .filter(AdAlias.email_address == "ed@bettyboop.com")
            .all()
        )
        eq_(result, [(user8, address3)])

        result = (
            q.select_from(outerjoin(User, AdAlias, "addresses"))
            .filter(AdAlias.email_address == "ed@bettyboop.com")
            .all()
        )
        eq_(result, [(user8, address3)])

        result = (
            q.select_from(outerjoin(User, AdAlias, User.id == AdAlias.user_id))
            .filter(AdAlias.email_address == "ed@bettyboop.com")
            .all()
        )
        eq_(result, [(user8, address3)])

        # this is the first test where we are joining "backwards" - from
        # AdAlias to User even though
        # the query is against User
        q = sess.query(User, AdAlias)
        result = (
            q.join(AdAlias.user)
            .filter(User.name == "ed")
            .order_by(User.id, AdAlias.id)
        )
        eq_(
            result.all(),
            [(user8, address2), (user8, address3), (user8, address4)],
        )

        q = (
            sess.query(User, AdAlias)
            .select_from(join(AdAlias, User, AdAlias.user))
            .filter(User.name == "ed")
        )
        eq_(
            result.all(),
            [(user8, address2), (user8, address3), (user8, address4)],
        )

    def test_expression_onclauses(self):
        Order, User = self.classes.Order, self.classes.User

        sess = fixture_session()

        subq = sess.query(User).subquery()

        self.assert_compile(
            sess.query(User).join(subq, User.name == subq.c.name),
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users JOIN (SELECT users.id AS id, users.name "
            "AS name FROM users) AS anon_1 ON users.name = anon_1.name",
            use_default_dialect=True,
        )

        subq = sess.query(Order).subquery()
        self.assert_compile(
            sess.query(User).join(subq, User.id == subq.c.user_id),
            "SELECT users.id AS users_id, users.name AS users_name FROM "
            "users JOIN (SELECT orders.id AS id, orders.user_id AS user_id, "
            "orders.address_id AS address_id, orders.description AS "
            "description, orders.isopen AS isopen FROM orders) AS "
            "anon_1 ON users.id = anon_1.user_id",
            use_default_dialect=True,
        )

        self.assert_compile(
            sess.query(User).join(Order, User.id == Order.user_id),
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users JOIN orders ON users.id = orders.user_id",
            use_default_dialect=True,
        )

    def test_aliased_classes_m2m(self):
        Item, Order = self.classes.Item, self.classes.Order

        sess = fixture_session()

        (order1, order2, order3, order4, order5) = sess.query(Order).all()
        (item1, item2, item3, item4, item5) = sess.query(Item).all()
        expected = [
            (order1, item1),
            (order1, item2),
            (order1, item3),
            (order2, item1),
            (order2, item2),
            (order2, item3),
            (order3, item3),
            (order3, item4),
            (order3, item5),
            (order4, item1),
            (order4, item5),
            (order5, item5),
        ]

        q = sess.query(Order)
        q = (
            q.add_entity(Item)
            .select_from(join(Order, Item, "items"))
            .order_by(Order.id, Item.id)
        )
        result = q.all()
        eq_(result, expected)

        IAlias = aliased(Item)
        q = (
            sess.query(Order, IAlias)
            .select_from(join(Order, IAlias, "items"))
            .filter(IAlias.description == "item 3")
        )
        result = q.all()
        eq_(result, [(order1, item3), (order2, item3), (order3, item3)])

    def test_joins_from_adapted_entities(self):
        User = self.classes.User

        # test for #1853

        session = fixture_session()
        first = session.query(User)
        second = session.query(User)
        unioned = first.union(second)
        subquery = session.query(User.id).subquery()
        join = subquery, subquery.c.id == User.id
        joined = unioned.outerjoin(*join)
        self.assert_compile(
            joined,
            "SELECT anon_1.users_id AS "
            "anon_1_users_id, anon_1.users_name AS "
            "anon_1_users_name FROM (SELECT users.id "
            "AS users_id, users.name AS users_name "
            "FROM users UNION SELECT users.id AS "
            "users_id, users.name AS users_name FROM "
            "users) AS anon_1 LEFT OUTER JOIN (SELECT "
            "users.id AS id FROM users) AS anon_2 ON "
            "anon_2.id = anon_1.users_id",
            use_default_dialect=True,
        )

        first = session.query(User.id)
        second = session.query(User.id)
        unioned = first.union(second)
        subquery = session.query(User.id).subquery()
        join = subquery, subquery.c.id == User.id
        joined = unioned.outerjoin(*join)
        self.assert_compile(
            joined,
            "SELECT anon_1.users_id AS anon_1_users_id "
            "FROM (SELECT users.id AS users_id FROM "
            "users UNION SELECT users.id AS users_id "
            "FROM users) AS anon_1 LEFT OUTER JOIN "
            "(SELECT users.id AS id FROM users) AS "
            "anon_2 ON anon_2.id = anon_1.users_id",
            use_default_dialect=True,
        )

    def test_joins_from_adapted_entities_isouter(self):
        User = self.classes.User

        # test for #1853

        session = fixture_session()
        first = session.query(User)
        second = session.query(User)
        unioned = first.union(second)
        subquery = session.query(User.id).subquery()
        join = subquery, subquery.c.id == User.id
        joined = unioned.join(*join, isouter=True)
        self.assert_compile(
            joined,
            "SELECT anon_1.users_id AS "
            "anon_1_users_id, anon_1.users_name AS "
            "anon_1_users_name FROM (SELECT users.id "
            "AS users_id, users.name AS users_name "
            "FROM users UNION SELECT users.id AS "
            "users_id, users.name AS users_name FROM "
            "users) AS anon_1 LEFT OUTER JOIN (SELECT "
            "users.id AS id FROM users) AS anon_2 ON "
            "anon_2.id = anon_1.users_id",
            use_default_dialect=True,
        )

        first = session.query(User.id)
        second = session.query(User.id)
        unioned = first.union(second)
        subquery = session.query(User.id).subquery()
        join = subquery, subquery.c.id == User.id
        joined = unioned.join(*join, isouter=True)
        self.assert_compile(
            joined,
            "SELECT anon_1.users_id AS anon_1_users_id "
            "FROM (SELECT users.id AS users_id FROM "
            "users UNION SELECT users.id AS users_id "
            "FROM users) AS anon_1 LEFT OUTER JOIN "
            "(SELECT users.id AS id FROM users) AS "
            "anon_2 ON anon_2.id = anon_1.users_id",
            use_default_dialect=True,
        )

    def test_overlap_with_aliases(self):
        orders, User, users = (
            self.tables.orders,
            self.classes.User,
            self.tables.users,
        )
        Order = self.classes.Order

        oalias = orders.alias("oalias")

        result = (
            fixture_session()
            .query(User)
            .select_from(users.join(oalias))
            .filter(
                oalias.c.description.in_(["order 1", "order 2", "order 3"])
            )
            .join(User.orders)
            .join(Order.items)
            .order_by(User.id)
            .all()
        )
        assert [User(id=7, name="jack"), User(id=9, name="fred")] == result

        result = (
            fixture_session()
            .query(User)
            .select_from(users.join(oalias))
            .filter(
                oalias.c.description.in_(["order 1", "order 2", "order 3"])
            )
            .join(User.orders)
            .join(Order.items)
            .filter_by(id=4)
            .all()
        )
        assert [User(id=7, name="jack")] == result

    def test_aliased_order_by(self):
        User = self.classes.User

        sess = fixture_session()

        ualias = aliased(User)
        eq_(
            sess.query(User, ualias)
            .filter(User.id > ualias.id)
            .order_by(desc(ualias.id), User.name)
            .all(),
            [
                (User(id=10, name="chuck"), User(id=9, name="fred")),
                (User(id=10, name="chuck"), User(id=8, name="ed")),
                (User(id=9, name="fred"), User(id=8, name="ed")),
                (User(id=10, name="chuck"), User(id=7, name="jack")),
                (User(id=8, name="ed"), User(id=7, name="jack")),
                (User(id=9, name="fred"), User(id=7, name="jack")),
            ],
        )

    def test_plain_table(self):
        addresses, User = self.tables.addresses, self.classes.User

        sess = fixture_session()

        eq_(
            sess.query(User.name)
            .join(addresses, User.id == addresses.c.user_id)
            .order_by(User.id)
            .all(),
            [("jack",), ("ed",), ("ed",), ("ed",), ("fred",)],
        )

    def test_no_joinpoint_expr(self):
        User, users = self.classes.User, self.tables.users

        sess = fixture_session()

        # these are consistent regardless of
        # select_from() being present.

        assert_raises_message(
            sa_exc.InvalidRequestError,
            "Don't know how to join to .*User.*. "
            r"Please use the .select_from\(\) "
            "method to establish an explicit left side, as well as",
            sess.query(users.c.id).join(User)._compile_context,
        )

        assert_raises_message(
            sa_exc.InvalidRequestError,
            "Don't know how to join to .*User.* "
            r"Please use the .select_from\(\) "
            "method to establish an explicit left side, as well as",
            sess.query(users.c.id)
            .select_from(users)
            .join(User)
            ._compile_context,
        )

    def test_on_clause_no_right_side_one(self):
        User = self.classes.User
        Address = self.classes.Address
        sess = fixture_session()

        # coercions does not catch this due to the
        # legacy=True flag for JoinTargetRole
        assert_raises_message(
            sa_exc.ArgumentError,
            "Expected mapped entity or selectable/table as join target",
            sess.query(User).join(User.id == Address.user_id)._compile_context,
        )

    def test_on_clause_no_right_side_one_future(self):
        User = self.classes.User
        Address = self.classes.Address

        # future mode can raise a more specific error at the coercions level
        assert_raises_message(
            sa_exc.ArgumentError,
            "Join target, typically a FROM expression, "
            "or ORM relationship attribute expected",
            select(User).join,
            User.id == Address.user_id,
        )

    def test_on_clause_no_right_side_two(self):
        User = self.classes.User
        Address = self.classes.Address
        sess = fixture_session()

        assert_raises_message(
            sa_exc.ArgumentError,
            "Join target Address.user_id does not refer to a mapped entity",
            sess.query(User).join(Address.user_id)._compile_context,
        )

    def test_on_clause_no_right_side_two_future(self):
        User = self.classes.User
        Address = self.classes.Address

        stmt = select(User).join(Address.user_id)

        assert_raises_message(
            sa_exc.ArgumentError,
            "Join target Address.user_id does not refer to a mapped entity",
            stmt.compile,
        )

    def test_select_from(self):
        """Test that the left edge of the join can be set reliably with
        select_from()."""

        Item, Order, User = (
            self.classes.Item,
            self.classes.Order,
            self.classes.User,
        )

        sess = fixture_session()
        self.assert_compile(
            sess.query(Item.id)
            .select_from(User)
            .join(User.orders)
            .join(Order.items),
            "SELECT items.id AS items_id FROM users JOIN orders ON "
            "users.id = orders.user_id JOIN order_items AS order_items_1 "
            "ON orders.id = order_items_1.order_id JOIN items ON items.id = "
            "order_items_1.item_id",
            use_default_dialect=True,
        )

        # here, the join really wants to add a second FROM clause
        # for "Item".  but select_from disallows that
        self.assert_compile(
            sess.query(Item.id)
            .select_from(User)
            .join(Item, User.id == Item.id),
            "SELECT items.id AS items_id FROM users JOIN items "
            "ON users.id = items.id",
            use_default_dialect=True,
        )


class JoinFromSelectableTest(fixtures.MappedTest, AssertsCompiledSQL):
    __dialect__ = "default"
    run_setup_mappers = "once"

    @classmethod
    def define_tables(cls, metadata):
        Table("table1", metadata, Column("id", Integer, primary_key=True))
        Table(
            "table2",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("t1_id", Integer),
        )

    @classmethod
    def setup_classes(cls):
        table1, table2 = cls.tables.table1, cls.tables.table2

        class T1(cls.Comparable):
            pass

        class T2(cls.Comparable):
            pass

        mapper(T1, table1)
        mapper(T2, table2)

    def test_select_mapped_to_mapped_explicit_left(self):
        T1, T2 = self.classes.T1, self.classes.T2

        sess = fixture_session()
        subq = (
            sess.query(T2.t1_id, func.count(T2.id).label("count"))
            .group_by(T2.t1_id)
            .subquery()
        )

        self.assert_compile(
            sess.query(subq.c.count, T1.id)
            .select_from(subq)
            .join(T1, subq.c.t1_id == T1.id),
            "SELECT anon_1.count AS anon_1_count, table1.id AS table1_id "
            "FROM (SELECT table2.t1_id AS t1_id, "
            "count(table2.id) AS count FROM table2 "
            "GROUP BY table2.t1_id) AS anon_1 JOIN table1 "
            "ON anon_1.t1_id = table1.id",
        )

    def test_select_mapped_to_mapped_implicit_left(self):
        T1, T2 = self.classes.T1, self.classes.T2

        sess = fixture_session()
        subq = (
            sess.query(T2.t1_id, func.count(T2.id).label("count"))
            .group_by(T2.t1_id)
            .subquery()
        )

        self.assert_compile(
            sess.query(subq.c.count, T1.id).join(T1, subq.c.t1_id == T1.id),
            "SELECT anon_1.count AS anon_1_count, table1.id AS table1_id "
            "FROM (SELECT table2.t1_id AS t1_id, "
            "count(table2.id) AS count FROM table2 "
            "GROUP BY table2.t1_id) AS anon_1 JOIN table1 "
            "ON anon_1.t1_id = table1.id",
        )

    def test_select_mapped_to_select_explicit_left(self):
        T1, T2 = self.classes.T1, self.classes.T2

        sess = fixture_session()
        subq = (
            sess.query(T2.t1_id, func.count(T2.id).label("count"))
            .group_by(T2.t1_id)
            .subquery()
        )

        self.assert_compile(
            sess.query(subq.c.count, T1.id)
            .select_from(T1)
            .join(subq, subq.c.t1_id == T1.id),
            "SELECT anon_1.count AS anon_1_count, table1.id AS table1_id "
            "FROM table1 JOIN (SELECT table2.t1_id AS t1_id, "
            "count(table2.id) AS count FROM table2 GROUP BY table2.t1_id) "
            "AS anon_1 ON anon_1.t1_id = table1.id",
        )

    def test_select_mapped_to_select_implicit_left(self):
        T1, T2 = self.classes.T1, self.classes.T2

        sess = fixture_session()
        subq = (
            sess.query(T2.t1_id, func.count(T2.id).label("count"))
            .group_by(T2.t1_id)
            .subquery()
        )

        # without select_from
        self.assert_compile(
            sess.query(subq.c.count, T1.id).join(subq, subq.c.t1_id == T1.id),
            "SELECT anon_1.count AS anon_1_count, table1.id AS table1_id "
            "FROM table1 JOIN "
            "(SELECT table2.t1_id AS t1_id, count(table2.id) AS count "
            "FROM table2 GROUP BY table2.t1_id) "
            "AS anon_1 ON anon_1.t1_id = table1.id",
        )

        # with select_from, same query
        self.assert_compile(
            sess.query(subq.c.count, T1.id)
            .select_from(T1)
            .join(subq, subq.c.t1_id == T1.id),
            "SELECT anon_1.count AS anon_1_count, table1.id AS table1_id "
            "FROM table1 JOIN "
            "(SELECT table2.t1_id AS t1_id, count(table2.id) AS count "
            "FROM table2 GROUP BY table2.t1_id) "
            "AS anon_1 ON anon_1.t1_id = table1.id",
        )

    def test_mapped_select_to_mapped_implicit_left(self):
        T1, T2 = self.classes.T1, self.classes.T2

        sess = fixture_session()
        subq = (
            sess.query(T2.t1_id, func.count(T2.id).label("count"))
            .group_by(T2.t1_id)
            .subquery()
        )

        # without select_from
        self.assert_compile(
            sess.query(T1.id, subq.c.count).join(T1, subq.c.t1_id == T1.id),
            "SELECT table1.id AS table1_id, anon_1.count AS anon_1_count "
            "FROM (SELECT table2.t1_id AS t1_id, count(table2.id) AS count "
            "FROM table2 GROUP BY table2.t1_id) AS anon_1 "
            "JOIN table1 ON anon_1.t1_id = table1.id",
        )

        # with select_from, same query
        self.assert_compile(
            sess.query(T1.id, subq.c.count)
            .select_from(subq)
            .join(T1, subq.c.t1_id == T1.id),
            "SELECT table1.id AS table1_id, anon_1.count AS anon_1_count "
            "FROM (SELECT table2.t1_id AS t1_id, count(table2.id) AS count "
            "FROM table2 GROUP BY table2.t1_id) AS anon_1 "
            "JOIN table1 ON anon_1.t1_id = table1.id",
        )

    def test_mapped_select_to_mapped_explicit_left(self):
        T1, T2 = self.classes.T1, self.classes.T2

        sess = fixture_session()
        subq = (
            sess.query(T2.t1_id, func.count(T2.id).label("count"))
            .group_by(T2.t1_id)
            .subquery()
        )

        self.assert_compile(
            sess.query(T1.id, subq.c.count)
            .select_from(subq)
            .join(T1, subq.c.t1_id == T1.id),
            "SELECT table1.id AS table1_id, anon_1.count AS anon_1_count "
            "FROM (SELECT table2.t1_id AS t1_id, count(table2.id) AS count "
            "FROM table2 GROUP BY table2.t1_id) AS anon_1 JOIN table1 "
            "ON anon_1.t1_id = table1.id",
        )

    def test_mapped_select_to_select_explicit_left(self):
        T1, T2 = self.classes.T1, self.classes.T2

        sess = fixture_session()
        subq = (
            sess.query(T2.t1_id, func.count(T2.id).label("count"))
            .group_by(T2.t1_id)
            .subquery()
        )

        self.assert_compile(
            sess.query(T1.id, subq.c.count)
            .select_from(T1)
            .join(subq, subq.c.t1_id == T1.id),
            "SELECT table1.id AS table1_id, anon_1.count AS anon_1_count "
            "FROM table1 JOIN (SELECT table2.t1_id AS t1_id, "
            "count(table2.id) AS count "
            "FROM table2 GROUP BY table2.t1_id) AS anon_1 "
            "ON anon_1.t1_id = table1.id",
        )

    def test_mapped_select_to_select_implicit_left(self):
        T1, T2 = self.classes.T1, self.classes.T2

        sess = fixture_session()
        subq = (
            sess.query(T2.t1_id, func.count(T2.id).label("count"))
            .group_by(T2.t1_id)
            .subquery()
        )

        self.assert_compile(
            sess.query(T1.id, subq.c.count).join(subq, subq.c.t1_id == T1.id),
            "SELECT table1.id AS table1_id, anon_1.count AS anon_1_count "
            "FROM table1 JOIN (SELECT table2.t1_id AS t1_id, "
            "count(table2.id) AS count "
            "FROM table2 GROUP BY table2.t1_id) AS anon_1 "
            "ON anon_1.t1_id = table1.id",
        )


class SelfRefMixedTest(fixtures.MappedTest, AssertsCompiledSQL):
    run_setup_mappers = "once"
    __dialect__ = default.DefaultDialect()

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "nodes",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("parent_id", Integer, ForeignKey("nodes.id")),
        )

        Table(
            "sub_table",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("node_id", Integer, ForeignKey("nodes.id")),
        )

        Table(
            "assoc_table",
            metadata,
            Column("left_id", Integer, ForeignKey("nodes.id")),
            Column("right_id", Integer, ForeignKey("nodes.id")),
        )

    @classmethod
    def setup_classes(cls):
        nodes, assoc_table, sub_table = (
            cls.tables.nodes,
            cls.tables.assoc_table,
            cls.tables.sub_table,
        )

        class Node(cls.Comparable):
            pass

        class Sub(cls.Comparable):
            pass

        mapper(
            Node,
            nodes,
            properties={
                "children": relationship(
                    Node,
                    lazy="select",
                    join_depth=3,
                    backref=backref("parent", remote_side=[nodes.c.id]),
                ),
                "subs": relationship(Sub),
                "assoc": relationship(
                    Node,
                    secondary=assoc_table,
                    primaryjoin=nodes.c.id == assoc_table.c.left_id,
                    secondaryjoin=nodes.c.id == assoc_table.c.right_id,
                ),
            },
        )
        mapper(Sub, sub_table)

    def test_o2m_aliased_plus_o2m(self):
        Node, Sub = self.classes.Node, self.classes.Sub

        sess = fixture_session()
        n1 = aliased(Node)

        self.assert_compile(
            sess.query(Node).join(n1, Node.children).join(Sub, n1.subs),
            "SELECT nodes.id AS nodes_id, nodes.parent_id AS nodes_parent_id "
            "FROM nodes JOIN nodes AS nodes_1 ON nodes.id = nodes_1.parent_id "
            "JOIN sub_table ON nodes_1.id = sub_table.node_id",
        )

        self.assert_compile(
            sess.query(Node).join(n1, Node.children).join(Sub, Node.subs),
            "SELECT nodes.id AS nodes_id, nodes.parent_id AS nodes_parent_id "
            "FROM nodes JOIN nodes AS nodes_1 ON nodes.id = nodes_1.parent_id "
            "JOIN sub_table ON nodes.id = sub_table.node_id",
        )

    def test_m2m_aliased_plus_o2m(self):
        Node, Sub = self.classes.Node, self.classes.Sub

        sess = fixture_session()
        n1 = aliased(Node)

        self.assert_compile(
            sess.query(Node).join(n1, Node.assoc).join(Sub, n1.subs),
            "SELECT nodes.id AS nodes_id, nodes.parent_id AS nodes_parent_id "
            "FROM nodes JOIN assoc_table AS assoc_table_1 ON nodes.id = "
            "assoc_table_1.left_id JOIN nodes AS nodes_1 ON nodes_1.id = "
            "assoc_table_1.right_id JOIN sub_table "
            "ON nodes_1.id = sub_table.node_id",
        )

        self.assert_compile(
            sess.query(Node).join(n1, Node.assoc).join(Sub, Node.subs),
            "SELECT nodes.id AS nodes_id, nodes.parent_id AS nodes_parent_id "
            "FROM nodes JOIN assoc_table AS assoc_table_1 ON nodes.id = "
            "assoc_table_1.left_id JOIN nodes AS nodes_1 ON nodes_1.id = "
            "assoc_table_1.right_id JOIN sub_table "
            "ON nodes.id = sub_table.node_id",
        )


class CreateJoinsTest(fixtures.ORMTest, AssertsCompiledSQL):
    __dialect__ = "default"

    def _inherits_fixture(self):
        m = MetaData()
        base = Table("base", m, Column("id", Integer, primary_key=True))
        a = Table(
            "a",
            m,
            Column("id", Integer, ForeignKey("base.id"), primary_key=True),
            Column("b_id", Integer, ForeignKey("b.id")),
        )
        b = Table(
            "b",
            m,
            Column("id", Integer, ForeignKey("base.id"), primary_key=True),
            Column("c_id", Integer, ForeignKey("c.id")),
        )
        c = Table(
            "c",
            m,
            Column("id", Integer, ForeignKey("base.id"), primary_key=True),
        )

        class Base(object):
            pass

        class A(Base):
            pass

        class B(Base):
            pass

        class C(Base):
            pass

        mapper(Base, base)
        mapper(
            A,
            a,
            inherits=Base,
            properties={"b": relationship(B, primaryjoin=a.c.b_id == b.c.id)},
        )
        mapper(
            B,
            b,
            inherits=Base,
            properties={"c": relationship(C, primaryjoin=b.c.c_id == c.c.id)},
        )
        mapper(C, c, inherits=Base)
        return A, B, C, Base

    def test_double_level_aliased_exists(self):
        A, B, C, Base = self._inherits_fixture()
        s = fixture_session()
        self.assert_compile(
            s.query(A).filter(A.b.has(B.c.has(C.id == 5))),
            "SELECT a.id AS a_id, base.id AS base_id, a.b_id AS a_b_id "
            "FROM base JOIN a ON base.id = a.id WHERE "
            "EXISTS (SELECT 1 FROM (SELECT base.id AS base_id, b.id AS "
            "b_id, b.c_id AS b_c_id FROM base JOIN b ON base.id = b.id) "
            "AS anon_1 WHERE a.b_id = anon_1.b_id AND (EXISTS "
            "(SELECT 1 FROM (SELECT base.id AS base_id, c.id AS c_id "
            "FROM base JOIN c ON base.id = c.id) AS anon_2 "
            "WHERE anon_1.b_c_id = anon_2.c_id AND anon_2.c_id = :id_1"
            ")))",
        )


class JoinToNonPolyAliasesTest(fixtures.MappedTest, AssertsCompiledSQL):
    """test joins to an aliased selectable and that we can refer to that
    aliased selectable in filter criteria.

    Basically testing that the aliasing Query applies to with_polymorphic
    targets doesn't leak into non-polymorphic mappers.


    """

    __dialect__ = "default"
    run_create_tables = None
    run_deletes = None

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "parent",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("data", String(50)),
        )
        Table(
            "child",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("parent_id", Integer, ForeignKey("parent.id")),
            Column("data", String(50)),
        )

    @classmethod
    def setup_mappers(cls):
        parent, child = cls.tables.parent, cls.tables.child

        class Parent(cls.Comparable):
            pass

        class Child(cls.Comparable):
            pass

        mp = mapper(Parent, parent)
        mapper(Child, child)

        derived = select(child).alias()
        npc = aliased(Child, derived)
        cls.npc = npc
        cls.derived = derived
        mp.add_property("npc", relationship(npc))

    def test_join_parent_child(self):
        Parent = self.classes.Parent

        sess = fixture_session()
        self.assert_compile(
            sess.query(Parent)
            .join(Parent.npc)
            .filter(self.derived.c.data == "x"),
            "SELECT parent.id AS parent_id, parent.data AS parent_data "
            "FROM parent JOIN (SELECT child.id AS id, "
            "child.parent_id AS parent_id, "
            "child.data AS data "
            "FROM child) AS anon_1 ON parent.id = anon_1.parent_id "
            "WHERE anon_1.data = :data_1",
        )

    def test_join_parent_child_select_from(self):
        Parent = self.classes.Parent
        npc = self.npc
        sess = fixture_session()
        self.assert_compile(
            sess.query(npc)
            .select_from(Parent)
            .join(Parent.npc)
            .filter(self.derived.c.data == "x"),
            "SELECT anon_1.id AS anon_1_id, anon_1.parent_id "
            "AS anon_1_parent_id, anon_1.data AS anon_1_data "
            "FROM parent JOIN (SELECT child.id AS id, child.parent_id AS "
            "parent_id, child.data AS data FROM child) AS anon_1 ON "
            "parent.id = anon_1.parent_id WHERE anon_1.data = :data_1",
        )

    def test_join_select_parent_child(self):
        Parent = self.classes.Parent
        npc = self.npc
        sess = fixture_session()
        self.assert_compile(
            sess.query(Parent, npc)
            .join(Parent.npc)
            .filter(self.derived.c.data == "x"),
            "SELECT parent.id AS parent_id, parent.data AS parent_data, "
            "anon_1.id AS anon_1_id, anon_1.parent_id AS anon_1_parent_id, "
            "anon_1.data AS anon_1_data FROM parent JOIN "
            "(SELECT child.id AS id, child.parent_id AS parent_id, "
            "child.data AS data FROM child) AS anon_1 ON parent.id = "
            "anon_1.parent_id WHERE anon_1.data = :data_1",
        )


class SelfReferentialTest(fixtures.MappedTest, AssertsCompiledSQL):
    run_setup_mappers = "once"
    run_inserts = "once"
    run_deletes = None
    __dialect__ = "default"

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "nodes",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("parent_id", Integer, ForeignKey("nodes.id")),
            Column("data", String(30)),
        )

    @classmethod
    def setup_classes(cls):
        class Node(cls.Comparable):
            def append(self, node):
                self.children.append(node)

    @classmethod
    def setup_mappers(cls):
        Node, nodes = cls.classes.Node, cls.tables.nodes

        mapper(
            Node,
            nodes,
            properties={
                "children": relationship(
                    Node,
                    lazy="select",
                    join_depth=3,
                    backref=backref("parent", remote_side=[nodes.c.id]),
                )
            },
        )

    @classmethod
    def insert_data(cls, connection):
        Node = cls.classes.Node

        sess = Session(connection)
        n1 = Node(data="n1")
        n1.append(Node(data="n11"))
        n1.append(Node(data="n12"))
        n1.append(Node(data="n13"))
        n1.children[1].append(Node(data="n121"))
        n1.children[1].append(Node(data="n122"))
        n1.children[1].append(Node(data="n123"))
        sess.add(n1)
        sess.flush()
        sess.close()

    def test_join_4_explicit_join(self):
        Node = self.classes.Node
        sess = fixture_session()

        na = aliased(Node)
        na2 = aliased(Node)

        # this one is a great example of how to show how the API changes;
        # while it requires the explicitness of aliased(Node), the whole
        # guesswork of joinpoint / aliased goes away and the whole thing
        # is simpler
        #
        #  .join("parent", aliased=True)
        #  .filter(Node.data == "n12")
        #  .join("parent", aliased=True, from_joinpoint=True)
        #  .filter(Node.data == "n1")
        #
        #  becomes:
        #
        #   na = aliased(Node)
        #   na2 = aliased(Node)
        #
        #   ...
        #   .join(na, Node.parent)
        #   .filter(na.data == "n12")
        #   .join(na2, na.parent)
        #   .filter(na2.data == "n1")
        #
        q = (
            sess.query(Node)
            .filter(Node.data == "n122")
            .join(na, Node.parent)
            .filter(na.data == "n12")
            .join(na2, na.parent)
            .filter(na2.data == "n1")
        )

        self.assert_compile(
            q,
            "SELECT nodes.id AS nodes_id, nodes.parent_id AS nodes_parent_id, "
            "nodes.data AS nodes_data FROM nodes JOIN nodes AS nodes_1 "
            "ON nodes_1.id = nodes.parent_id JOIN nodes AS nodes_2 "
            "ON nodes_2.id = nodes_1.parent_id WHERE nodes.data = :data_1 "
            "AND nodes_1.data = :data_2 AND nodes_2.data = :data_3",
            checkparams={"data_1": "n122", "data_2": "n12", "data_3": "n1"},
        )

        node = q.first()
        eq_(node.data, "n122")

    def test_from_self_inside_excludes_outside(self):
        """test the propagation of aliased() from inside to outside
        on a from_self()..
        """

        Node = self.classes.Node

        sess = fixture_session()

        n1 = aliased(Node)

        # n1 is not inside the from_self(), so all cols must be maintained
        # on the outside

        subq = (
            sess.query(Node)
            .filter(Node.data == "n122")
            .set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)
            .subquery()
        )

        na = aliased(Node, subq)

        self.assert_compile(
            sess.query(n1, na.id),
            "SELECT nodes_1.id AS nodes_1_id, "
            "nodes_1.parent_id AS nodes_1_parent_id, "
            "nodes_1.data AS nodes_1_data, anon_1.nodes_id AS anon_1_nodes_id "
            "FROM nodes AS nodes_1, (SELECT nodes.id AS nodes_id, "
            "nodes.parent_id AS nodes_parent_id, "
            "nodes.data AS nodes_data FROM "
            "nodes WHERE nodes.data = :data_1) AS anon_1",
            use_default_dialect=True,
        )

        parent = aliased(Node)
        grandparent = aliased(Node)
        subq = (
            sess.query(Node, parent, grandparent)
            .join(parent, Node.parent)
            .join(grandparent, parent.parent)
            .filter(Node.data == "n122")
            .filter(parent.data == "n12")
            .filter(grandparent.data == "n1")
            .set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)
            .subquery()
        )
        na = aliased(Node, subq)
        pa = aliased(parent, subq)
        ga = aliased(grandparent, subq)

        q = sess.query(na, pa, ga).limit(1)

        # parent, grandparent *are* inside the from_self(), so they
        # should get aliased to the outside.
        self.assert_compile(
            q,
            "SELECT anon_1.nodes_id AS anon_1_nodes_id, "
            "anon_1.nodes_parent_id AS anon_1_nodes_parent_id, "
            "anon_1.nodes_data AS anon_1_nodes_data, "
            "anon_1.nodes_1_id AS anon_1_nodes_1_id, "
            "anon_1.nodes_1_parent_id AS anon_1_nodes_1_parent_id, "
            "anon_1.nodes_1_data AS anon_1_nodes_1_data, "
            "anon_1.nodes_2_id AS anon_1_nodes_2_id, "
            "anon_1.nodes_2_parent_id AS anon_1_nodes_2_parent_id, "
            "anon_1.nodes_2_data AS anon_1_nodes_2_data "
            "FROM (SELECT nodes.id AS nodes_id, nodes.parent_id "
            "AS nodes_parent_id, nodes.data AS nodes_data, "
            "nodes_1.id AS nodes_1_id, "
            "nodes_1.parent_id AS nodes_1_parent_id, "
            "nodes_1.data AS nodes_1_data, nodes_2.id AS nodes_2_id, "
            "nodes_2.parent_id AS nodes_2_parent_id, nodes_2.data AS "
            "nodes_2_data FROM nodes JOIN nodes AS nodes_1 ON "
            "nodes_1.id = nodes.parent_id JOIN nodes AS nodes_2 "
            "ON nodes_2.id = nodes_1.parent_id "
            "WHERE nodes.data = :data_1 AND nodes_1.data = :data_2 AND "
            "nodes_2.data = :data_3) AS anon_1 LIMIT :param_1",
            {"param_1": 1},
            use_default_dialect=True,
        )

    def test_join_to_self_no_aliases_raises(self):
        Node = self.classes.Node

        s = fixture_session()
        assert_raises_message(
            sa.exc.InvalidRequestError,
            "Can't construct a join from mapped class Node->nodes to mapped "
            "class Node->nodes, they are the same entity",
            s.query(Node).join(Node.children)._compile_context,
        )

    def test_explicit_join_1(self):
        Node = self.classes.Node
        n1 = aliased(Node)
        n2 = aliased(Node)

        self.assert_compile(
            join(Node, n1, "children").join(n2, "children"),
            "nodes JOIN nodes AS nodes_1 ON nodes.id = nodes_1.parent_id "
            "JOIN nodes AS nodes_2 ON nodes_1.id = nodes_2.parent_id",
            use_default_dialect=True,
        )

    def test_explicit_join_2(self):
        Node = self.classes.Node
        n1 = aliased(Node)
        n2 = aliased(Node)

        self.assert_compile(
            join(Node, n1, Node.children).join(n2, n1.children),
            "nodes JOIN nodes AS nodes_1 ON nodes.id = nodes_1.parent_id "
            "JOIN nodes AS nodes_2 ON nodes_1.id = nodes_2.parent_id",
            use_default_dialect=True,
        )

    def test_explicit_join_3(self):
        Node = self.classes.Node
        n1 = aliased(Node)
        n2 = aliased(Node)

        # the join_to_left=False here is unfortunate.   the default on this
        # flag should be False.
        self.assert_compile(
            join(Node, n1, Node.children).join(
                n2, Node.children, join_to_left=False
            ),
            "nodes JOIN nodes AS nodes_1 ON nodes.id = nodes_1.parent_id "
            "JOIN nodes AS nodes_2 ON nodes.id = nodes_2.parent_id",
            use_default_dialect=True,
        )

    def test_explicit_join_4(self):
        Node = self.classes.Node
        sess = fixture_session()
        n1 = aliased(Node)
        n2 = aliased(Node)

        self.assert_compile(
            sess.query(Node).join(n1, Node.children).join(n2, n1.children),
            "SELECT nodes.id AS nodes_id, nodes.parent_id AS nodes_parent_id, "
            "nodes.data AS nodes_data FROM nodes JOIN nodes AS nodes_1 "
            "ON nodes.id = nodes_1.parent_id "
            "JOIN nodes AS nodes_2 ON nodes_1.id = nodes_2.parent_id",
            use_default_dialect=True,
        )

    def test_explicit_join_5(self):
        Node = self.classes.Node
        sess = fixture_session()
        n1 = aliased(Node)
        n2 = aliased(Node)

        self.assert_compile(
            sess.query(Node).join(n1, Node.children).join(n2, Node.children),
            "SELECT nodes.id AS nodes_id, nodes.parent_id AS nodes_parent_id, "
            "nodes.data AS nodes_data FROM nodes JOIN nodes AS nodes_1 "
            "ON nodes.id = nodes_1.parent_id "
            "JOIN nodes AS nodes_2 ON nodes.id = nodes_2.parent_id",
            use_default_dialect=True,
        )

    def test_explicit_join_6(self):
        Node = self.classes.Node
        sess = fixture_session()
        n1 = aliased(Node)

        node = (
            sess.query(Node)
            .select_from(join(Node, n1, "children"))
            .filter(n1.data == "n122")
            .first()
        )
        assert node.data == "n12"

    def test_explicit_join_7(self):
        Node = self.classes.Node
        sess = fixture_session()
        n1 = aliased(Node)
        n2 = aliased(Node)

        node = (
            sess.query(Node)
            .select_from(join(Node, n1, "children").join(n2, "children"))
            .filter(n2.data == "n122")
            .first()
        )
        assert node.data == "n1"

    def test_explicit_join_8(self):
        Node = self.classes.Node
        sess = fixture_session()
        n1 = aliased(Node)
        n2 = aliased(Node)

        # mix explicit and named onclauses
        node = (
            sess.query(Node)
            .select_from(
                join(Node, n1, Node.id == n1.parent_id).join(n2, "children")
            )
            .filter(n2.data == "n122")
            .first()
        )
        assert node.data == "n1"

    def test_explicit_join_9(self):
        Node = self.classes.Node
        sess = fixture_session()
        n1 = aliased(Node)
        n2 = aliased(Node)

        node = (
            sess.query(Node)
            .select_from(join(Node, n1, "parent").join(n2, "parent"))
            .filter(
                and_(Node.data == "n122", n1.data == "n12", n2.data == "n1")
            )
            .first()
        )
        assert node.data == "n122"

    def test_explicit_join_10(self):
        Node = self.classes.Node
        sess = fixture_session()
        n1 = aliased(Node)
        n2 = aliased(Node)

        eq_(
            list(
                sess.query(Node)
                .select_from(join(Node, n1, "parent").join(n2, "parent"))
                .filter(
                    and_(
                        Node.data == "n122", n1.data == "n12", n2.data == "n1"
                    )
                )
                .with_entities(Node.data, n1.data, n2.data)
            ),
            [("n122", "n12", "n1")],
        )

    def test_join_to_nonaliased(self):
        Node = self.classes.Node

        sess = fixture_session()

        n1 = aliased(Node)

        # using 'n1.parent' implicitly joins to unaliased Node
        eq_(
            sess.query(n1).join(n1.parent).filter(Node.data == "n1").all(),
            [
                Node(parent_id=1, data="n11", id=2),
                Node(parent_id=1, data="n12", id=3),
                Node(parent_id=1, data="n13", id=4),
            ],
        )

        # explicit (new syntax)
        eq_(
            sess.query(n1)
            .join(Node, n1.parent)
            .filter(Node.data == "n1")
            .all(),
            [
                Node(parent_id=1, data="n11", id=2),
                Node(parent_id=1, data="n12", id=3),
                Node(parent_id=1, data="n13", id=4),
            ],
        )

    def test_multiple_explicit_entities_one(self):
        Node = self.classes.Node

        sess = fixture_session()

        parent = aliased(Node)
        grandparent = aliased(Node)
        eq_(
            sess.query(Node, parent, grandparent)
            .join(parent, Node.parent)
            .join(grandparent, parent.parent)
            .filter(Node.data == "n122")
            .filter(parent.data == "n12")
            .filter(grandparent.data == "n1")
            .first(),
            (Node(data="n122"), Node(data="n12"), Node(data="n1")),
        )

    def test_multiple_explicit_entities_two(self):
        Node = self.classes.Node

        sess = fixture_session()

        parent = aliased(Node)
        grandparent = aliased(Node)

        subq = (
            sess.query(Node, parent, grandparent)
            .join(parent, Node.parent)
            .join(grandparent, parent.parent)
            .filter(Node.data == "n122")
            .filter(parent.data == "n12")
            .filter(grandparent.data == "n1")
            .subquery()
        )

        na = aliased(Node, subq)
        pa = aliased(parent, subq)
        ga = aliased(grandparent, subq)

        eq_(
            sess.query(na, pa, ga).first(),
            (Node(data="n122"), Node(data="n12"), Node(data="n1")),
        )

    def test_multiple_explicit_entities_three(self):
        Node = self.classes.Node

        sess = fixture_session()

        parent = aliased(Node)
        grandparent = aliased(Node)
        # same, change order around
        subq = (
            sess.query(parent, grandparent, Node)
            .join(parent, Node.parent)
            .join(grandparent, parent.parent)
            .filter(Node.data == "n122")
            .filter(parent.data == "n12")
            .filter(grandparent.data == "n1")
            .subquery()
        )

        na = aliased(Node, subq)
        pa = aliased(parent, subq)
        ga = aliased(grandparent, subq)

        eq_(
            sess.query(pa, ga, na).first(),
            (Node(data="n12"), Node(data="n1"), Node(data="n122")),
        )

    def test_multiple_explicit_entities_four(self):
        Node = self.classes.Node

        sess = fixture_session()

        parent = aliased(Node)
        grandparent = aliased(Node)
        eq_(
            sess.query(Node, parent, grandparent)
            .join(parent, Node.parent)
            .join(grandparent, parent.parent)
            .filter(Node.data == "n122")
            .filter(parent.data == "n12")
            .filter(grandparent.data == "n1")
            .options(joinedload(Node.children))
            .first(),
            (Node(data="n122"), Node(data="n12"), Node(data="n1")),
        )

    def test_multiple_explicit_entities_five(self):
        Node = self.classes.Node

        sess = fixture_session()

        parent = aliased(Node)
        grandparent = aliased(Node)

        subq = (
            sess.query(Node, parent, grandparent)
            .join(parent, Node.parent)
            .join(grandparent, parent.parent)
            .filter(Node.data == "n122")
            .filter(parent.data == "n12")
            .filter(grandparent.data == "n1")
            .subquery()
        )

        na = aliased(Node, subq)
        pa = aliased(parent, subq)
        ga = aliased(grandparent, subq)

        eq_(
            sess.query(na, pa, ga).options(joinedload(na.children)).first(),
            (Node(data="n122"), Node(data="n12"), Node(data="n1")),
        )

    def test_any(self):
        Node = self.classes.Node

        sess = fixture_session()
        eq_(
            sess.query(Node)
            .filter(Node.children.any(Node.data == "n1"))
            .all(),
            [],
        )
        eq_(
            sess.query(Node)
            .filter(Node.children.any(Node.data == "n12"))
            .all(),
            [Node(data="n1")],
        )
        eq_(
            sess.query(Node)
            .filter(~Node.children.any())
            .order_by(Node.id)
            .all(),
            [
                Node(data="n11"),
                Node(data="n13"),
                Node(data="n121"),
                Node(data="n122"),
                Node(data="n123"),
            ],
        )

    def test_has(self):
        Node = self.classes.Node

        sess = fixture_session()

        eq_(
            sess.query(Node)
            .filter(Node.parent.has(Node.data == "n12"))
            .order_by(Node.id)
            .all(),
            [Node(data="n121"), Node(data="n122"), Node(data="n123")],
        )
        eq_(
            sess.query(Node)
            .filter(Node.parent.has(Node.data == "n122"))
            .all(),
            [],
        )
        eq_(
            sess.query(Node).filter(~Node.parent.has()).all(),
            [Node(data="n1")],
        )

    def test_contains(self):
        Node = self.classes.Node

        sess = fixture_session()

        n122 = sess.query(Node).filter(Node.data == "n122").one()
        eq_(
            sess.query(Node).filter(Node.children.contains(n122)).all(),
            [Node(data="n12")],
        )

        n13 = sess.query(Node).filter(Node.data == "n13").one()
        eq_(
            sess.query(Node).filter(Node.children.contains(n13)).all(),
            [Node(data="n1")],
        )

    def test_eq_ne(self):
        Node = self.classes.Node

        sess = fixture_session()

        n12 = sess.query(Node).filter(Node.data == "n12").one()
        eq_(
            sess.query(Node).filter(Node.parent == n12).all(),
            [Node(data="n121"), Node(data="n122"), Node(data="n123")],
        )

        eq_(
            sess.query(Node).filter(Node.parent != n12).all(),
            [
                Node(data="n1"),
                Node(data="n11"),
                Node(data="n12"),
                Node(data="n13"),
            ],
        )


class SelfReferentialM2MTest(fixtures.MappedTest):
    run_setup_mappers = "once"
    run_inserts = "once"
    run_deletes = None

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "nodes",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", String(30)),
        )

        Table(
            "node_to_nodes",
            metadata,
            Column(
                "left_node_id",
                Integer,
                ForeignKey("nodes.id"),
                primary_key=True,
            ),
            Column(
                "right_node_id",
                Integer,
                ForeignKey("nodes.id"),
                primary_key=True,
            ),
        )

    @classmethod
    def setup_classes(cls):
        class Node(cls.Comparable):
            pass

    @classmethod
    def insert_data(cls, connection):
        Node, nodes, node_to_nodes = (
            cls.classes.Node,
            cls.tables.nodes,
            cls.tables.node_to_nodes,
        )

        mapper(
            Node,
            nodes,
            properties={
                "children": relationship(
                    Node,
                    lazy="select",
                    secondary=node_to_nodes,
                    primaryjoin=nodes.c.id == node_to_nodes.c.left_node_id,
                    secondaryjoin=nodes.c.id == node_to_nodes.c.right_node_id,
                )
            },
        )
        sess = Session(connection)
        n1 = Node(data="n1")
        n2 = Node(data="n2")
        n3 = Node(data="n3")
        n4 = Node(data="n4")
        n5 = Node(data="n5")
        n6 = Node(data="n6")
        n7 = Node(data="n7")

        n1.children = [n2, n3, n4]
        n2.children = [n3, n6, n7]
        n3.children = [n5, n4]

        sess.add(n1)
        sess.add(n2)
        sess.add(n3)
        sess.add(n4)
        sess.flush()
        sess.close()

    def test_any(self):
        Node = self.classes.Node

        sess = fixture_session()
        eq_(
            sess.query(Node)
            .filter(Node.children.any(Node.data == "n3"))
            .order_by(Node.data)
            .all(),
            [Node(data="n1"), Node(data="n2")],
        )

    def test_contains(self):
        Node = self.classes.Node

        sess = fixture_session()
        n4 = sess.query(Node).filter_by(data="n4").one()

        eq_(
            sess.query(Node)
            .filter(Node.children.contains(n4))
            .order_by(Node.data)
            .all(),
            [Node(data="n1"), Node(data="n3")],
        )
        eq_(
            sess.query(Node)
            .filter(not_(Node.children.contains(n4)))
            .order_by(Node.data)
            .all(),
            [
                Node(data="n2"),
                Node(data="n4"),
                Node(data="n5"),
                Node(data="n6"),
                Node(data="n7"),
            ],
        )

    def test_explicit_join(self):
        Node = self.classes.Node

        sess = fixture_session()

        n1 = aliased(Node)
        eq_(
            sess.query(Node)
            .select_from(join(Node, n1, "children"))
            .filter(n1.data.in_(["n3", "n7"]))
            .order_by(Node.id)
            .all(),
            [Node(data="n1"), Node(data="n2")],
        )


class JoinLateralTest(fixtures.MappedTest, AssertsCompiledSQL):
    __dialect__ = default.DefaultDialect(supports_native_boolean=True)

    run_setup_bind = None
    run_setup_mappers = "once"

    run_create_tables = None

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "people",
            metadata,
            Column("people_id", Integer, primary_key=True),
            Column("age", Integer),
            Column("name", String(30)),
        )
        Table(
            "bookcases",
            metadata,
            Column("bookcase_id", Integer, primary_key=True),
            Column(
                "bookcase_owner_id", Integer, ForeignKey("people.people_id")
            ),
            Column("bookcase_shelves", Integer),
            Column("bookcase_width", Integer),
        )
        Table(
            "books",
            metadata,
            Column("book_id", Integer, primary_key=True),
            Column(
                "bookcase_id", Integer, ForeignKey("bookcases.bookcase_id")
            ),
            Column("book_owner_id", Integer, ForeignKey("people.people_id")),
            Column("book_weight", Integer),
        )

    @classmethod
    def setup_classes(cls):
        people, bookcases, books = cls.tables("people", "bookcases", "books")

        class Person(cls.Comparable):
            pass

        class Bookcase(cls.Comparable):
            pass

        class Book(cls.Comparable):
            pass

        mapper(Person, people)
        mapper(
            Bookcase,
            bookcases,
            properties={
                "owner": relationship(Person),
                "books": relationship(Book),
            },
        )
        mapper(Book, books)

    def test_select_subquery(self):
        Person, Book = self.classes("Person", "Book")

        s = fixture_session()

        subq = (
            s.query(Book.book_id)
            .correlate(Person)
            .filter(Person.people_id == Book.book_owner_id)
            .subquery()
            .lateral()
        )

        stmt = s.query(Person, subq.c.book_id).join(subq, true())

        self.assert_compile(
            stmt,
            "SELECT people.people_id AS people_people_id, "
            "people.age AS people_age, people.name AS people_name, "
            "anon_1.book_id AS anon_1_book_id "
            "FROM people JOIN LATERAL "
            "(SELECT books.book_id AS book_id FROM books "
            "WHERE people.people_id = books.book_owner_id) AS anon_1 ON true",
        )

    # sef == select_entity_from
    def test_select_subquery_sef_implicit_correlate(self):
        Person, Book = self.classes("Person", "Book")

        s = fixture_session()

        stmt = s.query(Person).subquery()

        subq = (
            s.query(Book.book_id)
            .filter(Person.people_id == Book.book_owner_id)
            .subquery()
            .lateral()
        )

        stmt = (
            s.query(Person, subq.c.book_id)
            .select_entity_from(stmt)
            .join(subq, true())
        )

        self.assert_compile(
            stmt,
            "SELECT anon_1.people_id AS anon_1_people_id, "
            "anon_1.age AS anon_1_age, anon_1.name AS anon_1_name, "
            "anon_2.book_id AS anon_2_book_id "
            "FROM "
            "(SELECT people.people_id AS people_id, people.age AS age, "
            "people.name AS name FROM people) AS anon_1 "
            "JOIN LATERAL "
            "(SELECT books.book_id AS book_id FROM books "
            "WHERE anon_1.people_id = books.book_owner_id) AS anon_2 ON true",
        )

    def test_select_subquery_sef_implicit_correlate_coreonly(self):
        Person, Book = self.classes("Person", "Book")

        s = fixture_session()

        stmt = s.query(Person).subquery()

        subq = (
            select(Book.book_id)
            .where(Person.people_id == Book.book_owner_id)
            .subquery()
            .lateral()
        )

        stmt = (
            s.query(Person, subq.c.book_id)
            .select_entity_from(stmt)
            .join(subq, true())
        )

        self.assert_compile(
            stmt,
            "SELECT anon_1.people_id AS anon_1_people_id, "
            "anon_1.age AS anon_1_age, anon_1.name AS anon_1_name, "
            "anon_2.book_id AS anon_2_book_id "
            "FROM "
            "(SELECT people.people_id AS people_id, people.age AS age, "
            "people.name AS name FROM people) AS anon_1 "
            "JOIN LATERAL "
            "(SELECT books.book_id AS book_id FROM books "
            "WHERE anon_1.people_id = books.book_owner_id) AS anon_2 ON true",
        )

    def test_select_subquery_sef_explicit_correlate_coreonly(self):
        Person, Book = self.classes("Person", "Book")

        s = fixture_session()

        stmt = s.query(Person).subquery()

        subq = (
            select(Book.book_id)
            .correlate(Person)
            .where(Person.people_id == Book.book_owner_id)
            .subquery()
            .lateral()
        )

        stmt = (
            s.query(Person, subq.c.book_id)
            .select_entity_from(stmt)
            .join(subq, true())
        )

        self.assert_compile(
            stmt,
            "SELECT anon_1.people_id AS anon_1_people_id, "
            "anon_1.age AS anon_1_age, anon_1.name AS anon_1_name, "
            "anon_2.book_id AS anon_2_book_id "
            "FROM "
            "(SELECT people.people_id AS people_id, people.age AS age, "
            "people.name AS name FROM people) AS anon_1 "
            "JOIN LATERAL "
            "(SELECT books.book_id AS book_id FROM books "
            "WHERE anon_1.people_id = books.book_owner_id) AS anon_2 ON true",
        )

    def test_select_subquery_sef_explicit_correlate(self):
        Person, Book = self.classes("Person", "Book")

        s = fixture_session()

        stmt = s.query(Person).subquery()

        subq = (
            s.query(Book.book_id)
            .correlate(Person)
            .filter(Person.people_id == Book.book_owner_id)
            .subquery()
            .lateral()
        )

        stmt = (
            s.query(Person, subq.c.book_id)
            .select_entity_from(stmt)
            .join(subq, true())
        )

        self.assert_compile(
            stmt,
            "SELECT anon_1.people_id AS anon_1_people_id, "
            "anon_1.age AS anon_1_age, anon_1.name AS anon_1_name, "
            "anon_2.book_id AS anon_2_book_id "
            "FROM "
            "(SELECT people.people_id AS people_id, people.age AS age, "
            "people.name AS name FROM people) AS anon_1 "
            "JOIN LATERAL "
            "(SELECT books.book_id AS book_id FROM books "
            "WHERE anon_1.people_id = books.book_owner_id) AS anon_2 ON true",
        )

    def test_from_function(self):
        Bookcase = self.classes.Bookcase

        s = fixture_session()

        srf = lateral(func.generate_series(1, Bookcase.bookcase_shelves))

        self.assert_compile(
            s.query(Bookcase).join(srf, true()),
            "SELECT bookcases.bookcase_id AS bookcases_bookcase_id, "
            "bookcases.bookcase_owner_id AS bookcases_bookcase_owner_id, "
            "bookcases.bookcase_shelves AS bookcases_bookcase_shelves, "
            "bookcases.bookcase_width AS bookcases_bookcase_width "
            "FROM bookcases JOIN "
            "LATERAL generate_series(:generate_series_1, "
            "bookcases.bookcase_shelves) AS anon_1 ON true",
        )

    def test_from_function_select_entity_from(self):
        Bookcase = self.classes.Bookcase

        s = fixture_session()

        subq = s.query(Bookcase).subquery()

        srf = lateral(func.generate_series(1, Bookcase.bookcase_shelves))

        self.assert_compile(
            s.query(Bookcase).select_entity_from(subq).join(srf, true()),
            "SELECT anon_1.bookcase_id AS anon_1_bookcase_id, "
            "anon_1.bookcase_owner_id AS anon_1_bookcase_owner_id, "
            "anon_1.bookcase_shelves AS anon_1_bookcase_shelves, "
            "anon_1.bookcase_width AS anon_1_bookcase_width "
            "FROM (SELECT bookcases.bookcase_id AS bookcase_id, "
            "bookcases.bookcase_owner_id AS bookcase_owner_id, "
            "bookcases.bookcase_shelves AS bookcase_shelves, "
            "bookcases.bookcase_width AS bookcase_width FROM bookcases) "
            "AS anon_1 "
            "JOIN LATERAL "
            "generate_series(:generate_series_1, anon_1.bookcase_shelves) "
            "AS anon_2 ON true",
        )


class JoinRawTablesWLegacyTest(QueryTest, AssertsCompiledSQL):
    """test issue 6003 where creating a legacy query with only Core elements
    fails to accommodate for the ORM context thus producing a query
    that ignores the "legacy" joins

    """

    __dialect__ = "default"

    @testing.combinations(
        (
            lambda sess, User, Address: sess.query(User).join(Address),
            "SELECT users.id AS users_id, users.name AS users_name FROM "
            "users JOIN addresses ON users.id = addresses.user_id",
        ),
        (
            lambda sess, user_table, address_table: sess.query(
                user_table
            ).join(address_table),
            "SELECT users.id AS users_id, users.name AS users_name FROM "
            "users JOIN addresses ON users.id = addresses.user_id",
        ),
        (
            lambda sess, User, Address, Order: sess.query(User)
            .outerjoin(Order)
            .join(Address),
            "SELECT users.id AS users_id, users.name AS users_name FROM "
            "users LEFT OUTER JOIN orders ON users.id = orders.user_id "
            "JOIN addresses ON addresses.id = orders.address_id",
        ),
        (
            lambda sess, user_table, address_table, order_table: sess.query(
                user_table
            )
            .outerjoin(order_table)
            .join(address_table),
            "SELECT users.id AS users_id, users.name AS users_name FROM "
            "users LEFT OUTER JOIN orders ON users.id = orders.user_id "
            "JOIN addresses ON addresses.id = orders.address_id",
        ),
    )
    def test_join_render(self, spec, expected):
        User, Address, Order = self.classes("User", "Address", "Order")
        user_table, address_table, order_table = self.tables(
            "users", "addresses", "orders"
        )

        sess = fixture_session()

        q = testing.resolve_lambda(spec, **locals())

        self.assert_compile(q, expected)

        self.assert_compile(
            q.set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL).statement,
            expected,
        )

    def test_core_round_trip(self):
        user_table, address_table = self.tables("users", "addresses")

        sess = fixture_session()

        q = (
            sess.query(user_table)
            .join(address_table)
            .where(address_table.c.email_address.startswith("ed"))
        )
        eq_(q.all(), [(8, "ed"), (8, "ed"), (8, "ed")])
