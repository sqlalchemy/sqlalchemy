from sqlalchemy import bindparam
from sqlalchemy import exc
from sqlalchemy import func
from sqlalchemy import insert
from sqlalchemy import literal_column
from sqlalchemy import or_
from sqlalchemy import select
from sqlalchemy import testing
from sqlalchemy import util
from sqlalchemy.orm import aliased
from sqlalchemy.orm import column_property
from sqlalchemy.orm import contains_eager
from sqlalchemy.orm import join as orm_join
from sqlalchemy.orm import joinedload
from sqlalchemy.orm import mapper
from sqlalchemy.orm import query_expression
from sqlalchemy.orm import relationship
from sqlalchemy.orm import with_expression
from sqlalchemy.orm import with_polymorphic
from sqlalchemy.sql import sqltypes
from sqlalchemy.sql.selectable import Join as core_join
from sqlalchemy.sql.selectable import LABEL_STYLE_DISAMBIGUATE_ONLY
from sqlalchemy.sql.selectable import LABEL_STYLE_TABLENAME_PLUS_COL
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import eq_
from sqlalchemy.testing.fixtures import fixture_session
from sqlalchemy.testing.util import resolve_lambda
from .inheritance import _poly_fixtures
from .test_query import QueryTest

# TODO:
# composites / unions, etc.


class SelectableTest(QueryTest, AssertsCompiledSQL):
    __dialect__ = "default"

    def test_filter_by(self):
        User, Address = self.classes("User", "Address")

        stmt = select(User).filter_by(name="ed")

        self.assert_compile(
            stmt,
            "SELECT users.id, users.name FROM users "
            "WHERE users.name = :name_1",
        )

    def test_froms_single_table(self):
        User, Address = self.classes("User", "Address")

        stmt = select(User).filter_by(name="ed")

        eq_(stmt.froms, [self.tables.users])

    def test_froms_join(self):
        User, Address = self.classes("User", "Address")
        users, addresses = self.tables("users", "addresses")

        stmt = select(User).join(User.addresses)

        assert stmt.froms[0].compare(users.join(addresses))

    @testing.combinations(
        (
            lambda User: (User,),
            lambda User: [
                {
                    "name": "User",
                    "type": User,
                    "aliased": False,
                    "expr": User,
                    "entity": User,
                }
            ],
        ),
        (
            lambda User: (User.id,),
            lambda User: [
                {
                    "name": "id",
                    "type": testing.eq_type_affinity(sqltypes.Integer),
                    "aliased": False,
                    "expr": User.id,
                    "entity": User,
                }
            ],
        ),
        (
            lambda User, Address: (User.id, Address),
            lambda User, Address: [
                {
                    "name": "id",
                    "type": testing.eq_type_affinity(sqltypes.Integer),
                    "aliased": False,
                    "expr": User.id,
                    "entity": User,
                },
                {
                    "name": "Address",
                    "type": Address,
                    "aliased": False,
                    "expr": Address,
                    "entity": Address,
                },
            ],
        ),
    )
    def test_column_descriptions(self, cols, expected):
        User, Address = self.classes("User", "Address")

        cols = testing.resolve_lambda(cols, User=User, Address=Address)
        expected = testing.resolve_lambda(expected, User=User, Address=Address)

        stmt = select(*cols)

        eq_(stmt.column_descriptions, expected)


class JoinTest(QueryTest, AssertsCompiledSQL):
    __dialect__ = "default"

    def test_join_from_no_onclause(self):
        User, Address = self.classes("User", "Address")

        stmt = select(literal_column("1")).join_from(User, Address)
        self.assert_compile(
            stmt,
            "SELECT 1 FROM users JOIN addresses "
            "ON users.id = addresses.user_id",
        )

    def test_join_from_w_relationship(self):
        User, Address = self.classes("User", "Address")

        stmt = select(literal_column("1")).join_from(
            User, Address, User.addresses
        )
        self.assert_compile(
            stmt,
            "SELECT 1 FROM users JOIN addresses "
            "ON users.id = addresses.user_id",
        )

    def test_join_from_alised_w_relationship(self):
        User, Address = self.classes("User", "Address")

        u1 = aliased(User)

        stmt = select(literal_column("1")).join_from(u1, Address, u1.addresses)
        self.assert_compile(
            stmt,
            "SELECT 1 FROM users AS users_1 JOIN addresses "
            "ON users_1.id = addresses.user_id",
        )

    def test_join_conflicting_right_side(self):
        User, Address = self.classes("User", "Address")

        stmt = select(User).join(Address, User.orders)
        assert_raises_message(
            exc.InvalidRequestError,
            "Join target .*Address.* does not correspond to the right side "
            "of join condition User.orders",
            stmt.compile,
        )

    def test_join_from_conflicting_left_side_plain(self):
        User, Address, Order = self.classes("User", "Address", "Order")

        stmt = select(User).join_from(User, Address, Order.address)
        assert_raises_message(
            exc.InvalidRequestError,
            r"explicit from clause .*User.* does not match .* Order.address",
            stmt.compile,
        )

    def test_join_from_conflicting_left_side_mapper_vs_aliased(self):
        User, Address = self.classes("User", "Address")

        u1 = aliased(User)

        stmt = select(User).join_from(User, Address, u1.addresses)
        assert_raises_message(
            exc.InvalidRequestError,
            # the display of the attribute here is not consistent vs.
            # the straight aliased class, should improve this.
            r"explicit from clause .*User.* does not match left side .*"
            r"of relationship attribute AliasedClass_User.addresses",
            stmt.compile,
        )

    def test_join_from_conflicting_left_side_aliased_vs_mapper(self):
        User, Address = self.classes("User", "Address")

        u1 = aliased(User)

        stmt = select(u1).join_from(u1, Address, User.addresses)
        assert_raises_message(
            exc.InvalidRequestError,
            r"explicit from clause aliased\(User\) does not match left "
            "side of relationship attribute User.addresses",
            stmt.compile,
        )

    def test_join_from_we_can_explicitly_tree_joins(self):
        User, Address, Order, Item, Keyword = self.classes(
            "User", "Address", "Order", "Item", "Keyword"
        )

        stmt = (
            select(User)
            .join(User.addresses)
            .join_from(User, Order, User.orders)
            .join(Order.items)
        )
        self.assert_compile(
            stmt,
            "SELECT users.id, users.name FROM users JOIN addresses "
            "ON users.id = addresses.user_id JOIN orders "
            "ON users.id = orders.user_id JOIN order_items AS order_items_1 "
            "ON orders.id = order_items_1.order_id JOIN items "
            "ON items.id = order_items_1.item_id",
        )

    def test_join_from_w_filter_by(self):
        User, Address, Order, Item, Keyword = self.classes(
            "User", "Address", "Order", "Item", "Keyword"
        )

        stmt = (
            select(User)
            .filter_by(name="n1")
            .join(User.addresses)
            .filter_by(email_address="a1")
            .join_from(User, Order, User.orders)
            .filter_by(description="d1")
            .join(Order.items)
            .filter_by(description="d2")
        )
        self.assert_compile(
            stmt,
            "SELECT users.id, users.name FROM users "
            "JOIN addresses ON users.id = addresses.user_id "
            "JOIN orders ON users.id = orders.user_id "
            "JOIN order_items AS order_items_1 "
            "ON orders.id = order_items_1.order_id "
            "JOIN items ON items.id = order_items_1.item_id "
            "WHERE users.name = :name_1 "
            "AND addresses.email_address = :email_address_1 "
            "AND orders.description = :description_1 "
            "AND items.description = :description_2",
            checkparams={
                "name_1": "n1",
                "email_address_1": "a1",
                "description_1": "d1",
                "description_2": "d2",
            },
        )

    @testing.combinations(
        (
            lambda User: select(User).where(User.id == bindparam("foo")),
            "SELECT users.id, users.name FROM users WHERE users.id = :foo",
            {"foo": "bar"},
            {"foo": "bar"},
        ),
        (
            lambda User, Address: select(User)
            .join_from(User, Address)
            .where(User.id == bindparam("foo")),
            "SELECT users.id, users.name FROM users JOIN addresses "
            "ON users.id = addresses.user_id WHERE users.id = :foo",
            {"foo": "bar"},
            {"foo": "bar"},
        ),
        (
            lambda User, Address: select(User)
            .join_from(User, Address, User.addresses)
            .where(User.id == bindparam("foo")),
            "SELECT users.id, users.name FROM users JOIN addresses "
            "ON users.id = addresses.user_id WHERE users.id = :foo",
            {"foo": "bar"},
            {"foo": "bar"},
        ),
        (
            lambda User, Address: select(User)
            .join(User.addresses)
            .where(User.id == bindparam("foo")),
            "SELECT users.id, users.name FROM users JOIN addresses "
            "ON users.id = addresses.user_id WHERE users.id = :foo",
            {"foo": "bar"},
            {"foo": "bar"},
        ),
    )
    def test_params_with_join(
        self, test_case, expected, bindparams, expected_params
    ):
        User, Address = self.classes("User", "Address")

        stmt = resolve_lambda(test_case, **locals())

        stmt = stmt.params(**bindparams)

        self.assert_compile(stmt, expected, checkparams=expected_params)


class LoadersInSubqueriesTest(QueryTest, AssertsCompiledSQL):
    """The Query object calls eanble_eagerloads(False) when you call
    .subquery().  With Core select, we don't have that information, we instead
    have to look at the "toplevel" flag to know where we are.   make sure
    the many different combinations that these two objects and still
    too many flags at the moment work as expected on the outside.

    """

    __dialect__ = "default"

    run_setup_mappers = None

    @testing.fixture
    def joinedload_fixture(self):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        mapper(
            User,
            users,
            properties={"addresses": relationship(Address, lazy="joined")},
        )

        mapper(Address, addresses)

        return User, Address

    def test_no_joinedload_in_subquery_select_rows(self, joinedload_fixture):
        User, Address = joinedload_fixture

        sess = fixture_session()
        stmt1 = sess.query(User).subquery()
        stmt1 = sess.query(stmt1)

        stmt2 = select(User).subquery()

        stmt2 = select(stmt2)

        expected = (
            "SELECT anon_1.id, anon_1.name FROM "
            "(SELECT users.id AS id, users.name AS name "
            "FROM users) AS anon_1"
        )
        self.assert_compile(
            stmt1._final_statement(legacy_query_style=False),
            expected,
        )

        self.assert_compile(stmt2, expected)

    def test_no_joinedload_in_subquery_select_entity(self, joinedload_fixture):
        User, Address = joinedload_fixture

        sess = fixture_session()
        stmt1 = sess.query(User).subquery()
        ua = aliased(User, stmt1)
        stmt1 = sess.query(ua)

        stmt2 = select(User).subquery()

        ua = aliased(User, stmt2)
        stmt2 = select(ua)

        expected = (
            "SELECT anon_1.id, anon_1.name, addresses_1.id AS id_1, "
            "addresses_1.user_id, addresses_1.email_address FROM "
            "(SELECT users.id AS id, users.name AS name FROM users) AS anon_1 "
            "LEFT OUTER JOIN addresses AS addresses_1 "
            "ON anon_1.id = addresses_1.user_id"
        )

        self.assert_compile(
            stmt1._final_statement(legacy_query_style=False),
            expected,
        )

        self.assert_compile(stmt2, expected)

    # TODO: need to test joinedload options, deferred mappings, deferred
    # options.  these are all loader options that should *only* have an
    # effect on the outermost statement, never a subquery.


class ExtraColsTest(QueryTest, AssertsCompiledSQL):
    __dialect__ = "default"

    run_setup_mappers = None

    @testing.fixture
    def query_expression_fixture(self):
        users, User = (
            self.tables.users,
            self.classes.User,
        )

        mapper(
            User,
            users,
            properties=util.OrderedDict([("value", query_expression())]),
        )
        return User

    @testing.fixture
    def column_property_fixture(self):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        mapper(
            User,
            users,
            properties=util.OrderedDict(
                [
                    ("concat", column_property((users.c.id * 2))),
                    (
                        "count",
                        column_property(
                            select(func.count(addresses.c.id))
                            .where(
                                users.c.id == addresses.c.user_id,
                            )
                            .correlate(users)
                            .scalar_subquery()
                        ),
                    ),
                ]
            ),
        )

        mapper(
            Address,
            addresses,
            properties={
                "user": relationship(
                    User,
                )
            },
        )

        return User, Address

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
        )

        mapper(
            Address,
            addresses,
            properties={
                "user": relationship(
                    User,
                )
            },
        )

        return User, Address

    def test_no_joinedload_embedded(self, plain_fixture):
        User, Address = plain_fixture

        stmt = select(Address).options(joinedload(Address.user))

        subq = stmt.subquery()

        s2 = select(subq)

        self.assert_compile(
            s2,
            "SELECT anon_1.id, anon_1.user_id, anon_1.email_address "
            "FROM (SELECT addresses.id AS id, addresses.user_id AS "
            "user_id, addresses.email_address AS email_address "
            "FROM addresses) AS anon_1",
        )

    def test_with_expr_one(self, query_expression_fixture):
        User = query_expression_fixture

        stmt = select(User).options(
            with_expression(User.value, User.name + "foo")
        )

        self.assert_compile(
            stmt,
            "SELECT users.name || :name_1 AS anon_1, users.id, "
            "users.name FROM users",
        )

    def test_with_expr_two(self, query_expression_fixture):
        User = query_expression_fixture

        stmt = select(User.id, User.name, (User.name + "foo").label("foo"))

        subq = stmt.subquery()
        u1 = aliased(User, subq)

        stmt = select(u1).options(with_expression(u1.value, subq.c.foo))

        self.assert_compile(
            stmt,
            "SELECT anon_1.foo, anon_1.id, anon_1.name FROM "
            "(SELECT users.id AS id, users.name AS name, "
            "users.name || :name_1 AS foo FROM users) AS anon_1",
        )

    def test_joinedload_outermost(self, plain_fixture):
        User, Address = plain_fixture

        stmt = select(Address).options(joinedload(Address.user))

        # render joined eager loads with stringify
        self.assert_compile(
            stmt,
            "SELECT addresses.id, addresses.user_id, addresses.email_address, "
            "users_1.id AS id_1, users_1.name FROM addresses "
            "LEFT OUTER JOIN users AS users_1 "
            "ON users_1.id = addresses.user_id",
        )

    def test_contains_eager_outermost(self, plain_fixture):
        User, Address = plain_fixture

        stmt = (
            select(Address)
            .join(Address.user)
            .options(contains_eager(Address.user))
        )

        # render joined eager loads with stringify
        self.assert_compile(
            stmt,
            "SELECT users.id, users.name, addresses.id AS id_1, "
            "addresses.user_id, "
            "addresses.email_address "
            "FROM addresses JOIN users ON users.id = addresses.user_id",
        )

    def test_column_properties(self, column_property_fixture):
        """test querying mappings that reference external columns or
        selectables."""

        User, Address = column_property_fixture

        stmt = select(User)

        self.assert_compile(
            stmt,
            "SELECT users.id * :id_1 AS anon_1, "
            "(SELECT count(addresses.id) AS count_1 FROM addresses "
            "WHERE users.id = addresses.user_id) AS anon_2, users.id, "
            "users.name FROM users",
            checkparams={"id_1": 2},
        )

    def test_column_properties_can_we_use(self, column_property_fixture):
        """test querying mappings that reference external columns or
        selectables."""

        # User, Address = column_property_fixture

        # stmt = select(User)

        # TODO: shouldn't we be able to get at count ?

    # stmt = stmt.where(stmt.selected_columns.count > 5)

    # self.assert_compile(stmt, "")

    def test_column_properties_subquery(self, column_property_fixture):
        """test querying mappings that reference external columns or
        selectables."""

        User, Address = column_property_fixture

        stmt = select(User)

        # here, the subquery needs to export the columns that include
        # the column properties
        stmt = select(stmt.subquery())

        # TODO: shouldnt we be able to get to stmt.subquery().c.count ?
        self.assert_compile(
            stmt,
            "SELECT anon_2.anon_1, anon_2.anon_3, anon_2.id, anon_2.name "
            "FROM (SELECT users.id * :id_1 AS anon_1, "
            "(SELECT count(addresses.id) AS count_1 FROM addresses "
            "WHERE users.id = addresses.user_id) AS anon_3, users.id AS id, "
            "users.name AS name FROM users) AS anon_2",
            checkparams={"id_1": 2},
        )

    def test_column_properties_subquery_two(self, column_property_fixture):
        """test querying mappings that reference external columns or
        selectables."""

        User, Address = column_property_fixture

        # col properties will retain anonymous labels, however will
        # adopt the .key within the subquery collection so they can
        # be addressed.
        stmt = select(
            User.id,
            User.name,
            User.concat,
            User.count,
        )

        subq = stmt.subquery()
        # here, the subquery needs to export the columns that include
        # the column properties
        stmt = select(subq).where(subq.c.concat == "foo")

        self.assert_compile(
            stmt,
            "SELECT anon_1.id, anon_1.name, anon_1.anon_2, anon_1.anon_3 "
            "FROM (SELECT users.id AS id, users.name AS name, "
            "users.id * :id_1 AS anon_2, "
            "(SELECT count(addresses.id) AS count_1 "
            "FROM addresses WHERE users.id = addresses.user_id) AS anon_3 "
            "FROM users) AS anon_1 WHERE anon_1.anon_2 = :param_1",
            checkparams={"id_1": 2, "param_1": "foo"},
        )

    def test_column_properties_aliased_subquery(self, column_property_fixture):
        """test querying mappings that reference external columns or
        selectables."""

        User, Address = column_property_fixture

        u1 = aliased(User)
        stmt = select(u1)

        # here, the subquery needs to export the columns that include
        # the column properties
        stmt = select(stmt.subquery())
        self.assert_compile(
            stmt,
            "SELECT anon_2.anon_1, anon_2.anon_3, anon_2.id, anon_2.name "
            "FROM (SELECT users_1.id * :id_1 AS anon_1, "
            "(SELECT count(addresses.id) AS count_1 FROM addresses "
            "WHERE users_1.id = addresses.user_id) AS anon_3, "
            "users_1.id AS id, users_1.name AS name "
            "FROM users AS users_1) AS anon_2",
            checkparams={"id_1": 2},
        )


class RelationshipNaturalCompileTest(QueryTest, AssertsCompiledSQL):
    """test using core join() with relationship attributes.

    as __clause_element__() produces a workable SQL expression, this should
    be generally possible.

    However, it can't work for many-to-many relationships, as these
    require two joins.    Only the ORM can look at the entities and decide
    that there's a separate "secondary" table to be rendered as a separate
    join.

    """

    __dialect__ = "default"

    def test_of_type_implicit_join(self):
        User, Address = self.classes("User", "Address")

        u1 = aliased(User)
        a1 = aliased(Address)

        stmt1 = select(u1).where(u1.addresses.of_type(a1))
        stmt2 = (
            fixture_session()
            .query(u1)
            .filter(u1.addresses.of_type(a1))
            ._final_statement(legacy_query_style=False)
        )

        expected = (
            "SELECT users_1.id, users_1.name FROM users AS users_1, "
            "addresses AS addresses_1 WHERE users_1.id = addresses_1.user_id"
        )

        self.assert_compile(stmt1, expected)
        self.assert_compile(stmt2, expected)

    def test_of_type_explicit_join(self):
        User, Address = self.classes("User", "Address")

        u1 = aliased(User)
        a1 = aliased(Address)

        stmt = select(u1).join(u1.addresses.of_type(a1))

        self.assert_compile(
            stmt,
            "SELECT users_1.id, users_1.name FROM users AS users_1 "
            "JOIN addresses AS addresses_1 "
            "ON users_1.id = addresses_1.user_id",
        )

    def test_many_to_many_explicit_join(self):
        Item, Keyword = self.classes("Item", "Keyword")

        stmt = select(Item).join(Keyword, Item.keywords)

        self.assert_compile(
            stmt,
            "SELECT items.id, items.description FROM items "
            "JOIN item_keywords AS item_keywords_1 "
            "ON items.id = item_keywords_1.item_id "
            "JOIN keywords ON keywords.id = item_keywords_1.keyword_id",
        )

    def test_many_to_many_implicit_join(self):
        Item, Keyword = self.classes("Item", "Keyword")

        stmt = select(Item).where(Item.keywords)

        # this was the intent of the primary + secondary clauseelement.
        # it can do enough of the right thing in an implicit join
        # context.
        self.assert_compile(
            stmt,
            "SELECT items.id, items.description FROM items, "
            "item_keywords AS item_keywords_1, keywords "
            "WHERE items.id = item_keywords_1.item_id "
            "AND keywords.id = item_keywords_1.keyword_id",
        )


class InheritedTest(_poly_fixtures._Polymorphic):
    run_setup_mappers = "once"


class ExplicitWithPolymorhpicTest(
    _poly_fixtures._PolymorphicUnions, AssertsCompiledSQL
):

    __dialect__ = "default"

    default_punion = (
        "(SELECT pjoin.person_id AS person_id, "
        "pjoin.company_id AS company_id, "
        "pjoin.name AS name, pjoin.type AS type, "
        "pjoin.status AS status, pjoin.engineer_name AS engineer_name, "
        "pjoin.primary_language AS primary_language, "
        "pjoin.manager_name AS manager_name "
        "FROM (SELECT engineers.person_id AS person_id, "
        "people.company_id AS company_id, people.name AS name, "
        "people.type AS type, engineers.status AS status, "
        "engineers.engineer_name AS engineer_name, "
        "engineers.primary_language AS primary_language, "
        "CAST(NULL AS VARCHAR(50)) AS manager_name "
        "FROM people JOIN engineers ON people.person_id = engineers.person_id "
        "UNION ALL SELECT managers.person_id AS person_id, "
        "people.company_id AS company_id, people.name AS name, "
        "people.type AS type, managers.status AS status, "
        "CAST(NULL AS VARCHAR(50)) AS engineer_name, "
        "CAST(NULL AS VARCHAR(50)) AS primary_language, "
        "managers.manager_name AS manager_name FROM people "
        "JOIN managers ON people.person_id = managers.person_id) AS pjoin) "
        "AS anon_1"
    )

    def test_subquery_col_expressions_wpoly_one(self):
        Person, Manager, Engineer = self.classes(
            "Person", "Manager", "Engineer"
        )

        wp1 = with_polymorphic(Person, [Manager, Engineer])

        subq1 = select(wp1).subquery()

        wp2 = with_polymorphic(Person, [Engineer, Manager])
        subq2 = select(wp2).subquery()

        # first thing we see, is that when we go through with_polymorphic,
        # the entities that get placed into the aliased class go through
        # Mapper._mappers_from_spec(), which matches them up to the
        # existing Mapper.self_and_descendants collection, meaning,
        # the order is the same every time.   Assert here that's still
        # happening.  If a future internal change modifies this assumption,
        # that's not necessarily bad, but it would change things.

        eq_(
            subq1.c.keys(),
            [
                "person_id",
                "company_id",
                "name",
                "type",
                "person_id_1",
                "status",
                "engineer_name",
                "primary_language",
                "person_id_1",
                "status_1",
                "manager_name",
            ],
        )
        eq_(
            subq2.c.keys(),
            [
                "person_id",
                "company_id",
                "name",
                "type",
                "person_id_1",
                "status",
                "engineer_name",
                "primary_language",
                "person_id_1",
                "status_1",
                "manager_name",
            ],
        )

    def test_subquery_col_expressions_wpoly_two(self):
        Person, Manager, Engineer = self.classes(
            "Person", "Manager", "Engineer"
        )

        wp1 = with_polymorphic(Person, [Manager, Engineer])

        subq1 = select(wp1).subquery()

        stmt = select(subq1).where(
            or_(
                subq1.c.engineer_name == "dilbert",
                subq1.c.manager_name == "dogbert",
            )
        )

        self.assert_compile(
            stmt,
            "SELECT anon_1.person_id, anon_1.company_id, anon_1.name, "
            "anon_1.type, anon_1.person_id AS person_id_1, anon_1.status, "
            "anon_1.engineer_name, anon_1.primary_language, "
            "anon_1.person_id AS person_id_2, anon_1.status AS status_1, "
            "anon_1.manager_name FROM "
            "%s WHERE "
            "anon_1.engineer_name = :engineer_name_1 "
            "OR anon_1.manager_name = :manager_name_1" % (self.default_punion),
        )


class ImplicitWithPolymorphicTest(
    _poly_fixtures._PolymorphicUnions, AssertsCompiledSQL
):
    """Test a series of mappers with a very awkward with_polymorphic setting,
    that tables and columns are rendered using the selectable in the correct
    contexts.  PolymorphicUnions represent the most awkward and verbose
    polymorphic fixtures you can have.   expressions need to be maximally
    accurate in terms of the mapped selectable in order to produce correct
    queries, which also will be really wrong if that mapped selectable is not
    in use.

    """

    __dialect__ = "default"

    def test_select_columns_where_baseclass(self):
        Person = self.classes.Person

        stmt = (
            select(Person.person_id, Person.name)
            .where(Person.name == "some name")
            .order_by(Person.person_id)
        )

        sess = fixture_session()
        q = (
            sess.query(Person.person_id, Person.name)
            .filter(Person.name == "some name")
            .order_by(Person.person_id)
        )

        expected = (
            "SELECT pjoin.person_id, pjoin.name FROM "
            "(SELECT engineers.person_id AS person_id, people.company_id AS "
            "company_id, people.name AS name, people.type AS type, "
            "engineers.status AS status, engineers.engineer_name AS "
            "engineer_name, engineers.primary_language AS primary_language, "
            "CAST(NULL AS VARCHAR(50)) AS manager_name FROM people "
            "JOIN engineers ON people.person_id = engineers.person_id "
            "UNION ALL SELECT managers.person_id AS person_id, "
            "people.company_id AS company_id, people.name AS name, "
            "people.type AS type, managers.status AS status, "
            "CAST(NULL AS VARCHAR(50)) AS engineer_name, "
            "CAST(NULL AS VARCHAR(50)) AS primary_language, "
            "managers.manager_name AS manager_name FROM people "
            "JOIN managers ON people.person_id = managers.person_id) AS "
            "pjoin WHERE pjoin.name = :name_1 ORDER BY pjoin.person_id"
        )
        self.assert_compile(stmt, expected)

        self.assert_compile(
            q._final_statement(legacy_query_style=False),
            expected,
        )

    def test_select_where_baseclass(self):
        Person = self.classes.Person

        stmt = (
            select(Person)
            .where(Person.name == "some name")
            .order_by(Person.person_id)
        )

        sess = fixture_session()
        q = (
            sess.query(Person)
            .filter(Person.name == "some name")
            .order_by(Person.person_id)
        )

        expected = (
            "SELECT pjoin.person_id, pjoin.company_id, pjoin.name, "
            "pjoin.type, pjoin.status, pjoin.engineer_name, "
            "pjoin.primary_language, pjoin.manager_name FROM "
            "(SELECT engineers.person_id AS person_id, people.company_id "
            "AS company_id, people.name AS name, people.type AS type, "
            "engineers.status AS status, engineers.engineer_name AS "
            "engineer_name, engineers.primary_language AS primary_language, "
            "CAST(NULL AS VARCHAR(50)) AS manager_name FROM people "
            "JOIN engineers ON people.person_id = engineers.person_id "
            "UNION ALL SELECT managers.person_id AS person_id, "
            "people.company_id AS company_id, people.name AS name, "
            "people.type AS type, managers.status AS status, "
            "CAST(NULL AS VARCHAR(50)) AS engineer_name, "
            "CAST(NULL AS VARCHAR(50)) AS primary_language, "
            "managers.manager_name AS manager_name FROM people "
            "JOIN managers ON people.person_id = managers.person_id) AS "
            "pjoin WHERE pjoin.name = :name_1 ORDER BY pjoin.person_id"
        )
        self.assert_compile(stmt, expected)

        self.assert_compile(
            q._final_statement(legacy_query_style=False),
            expected,
        )

    def test_select_where_subclass(self):

        Engineer = self.classes.Engineer

        # what will *not* work with Core, that the ORM does for now,
        # is that if you do where/orderby Person.column, it will de-adapt
        # the Person columns from the polymorphic union

        stmt = (
            select(Engineer)
            .where(Engineer.name == "some name")
            .order_by(Engineer.person_id)
        )

        sess = fixture_session()
        q = (
            sess.query(Engineer)
            .filter(Engineer.name == "some name")
            .order_by(Engineer.person_id)
        )

        plain_expected = (  # noqa
            "SELECT engineers.person_id, people.person_id, people.company_id, "
            "people.name, "
            "people.type, engineers.status, "
            "engineers.engineer_name, engineers.primary_language "
            "FROM people JOIN engineers "
            "ON people.person_id = engineers.person_id "
            "WHERE people.name = :name_1 ORDER BY engineers.person_id"
        )
        # when we have disambiguating labels turned on
        disambiguate_expected = (  # noqa
            "SELECT engineers.person_id, people.person_id AS person_id_1, "
            "people.company_id, "
            "people.name, "
            "people.type, engineers.status, "
            "engineers.engineer_name, engineers.primary_language "
            "FROM people JOIN engineers "
            "ON people.person_id = engineers.person_id "
            "WHERE people.name = :name_1 ORDER BY engineers.person_id"
        )

        # these change based on how we decide to apply labels
        # in context.py
        self.assert_compile(stmt, disambiguate_expected)

        self.assert_compile(
            q._final_statement(legacy_query_style=False),
            disambiguate_expected,
        )

    def test_select_where_columns_subclass(self):

        Engineer = self.classes.Engineer

        # what will *not* work with Core, that the ORM does for now,
        # is that if you do where/orderby Person.column, it will de-adapt
        # the Person columns from the polymorphic union

        # After many attempts to get the JOIN to render, by annotating
        # the columns with the "join" that they come from and trying to
        # get Select() to render out that join, there's no approach
        # that really works without stepping on other assumptions, so
        # add select_from(Engineer) explicitly.   It's still puzzling why the
        # ORM seems to know how to make this decision more effectively
        # when the select() has the same amount of information.
        stmt = (
            select(Engineer.person_id, Engineer.name)
            .where(Engineer.name == "some name")
            .select_from(Engineer)
            .order_by(Engineer.person_id)
        )

        sess = fixture_session()
        q = (
            sess.query(Engineer.person_id, Engineer.name)
            .filter(Engineer.name == "some name")
            .order_by(Engineer.person_id)
        )

        expected = (
            "SELECT engineers.person_id, people.name "
            "FROM people JOIN engineers "
            "ON people.person_id = engineers.person_id "
            "WHERE people.name = :name_1 ORDER BY engineers.person_id"
        )

        self.assert_compile(stmt, expected)
        self.assert_compile(
            q._final_statement(legacy_query_style=False),
            expected,
        )


class RelationshipNaturalInheritedTest(InheritedTest, AssertsCompiledSQL):
    __dialect__ = "default"

    straight_company_to_person_expected = (
        "SELECT companies.company_id, companies.name FROM companies "
        "JOIN people ON companies.company_id = people.company_id"
    )

    default_pjoin = (
        "(people LEFT OUTER "
        "JOIN engineers ON people.person_id = engineers.person_id "
        "LEFT OUTER JOIN managers "
        "ON people.person_id = managers.person_id "
        "LEFT OUTER JOIN boss ON managers.person_id = boss.boss_id) "
        "ON companies.company_id = people.company_id"
    )

    flat_aliased_pjoin = (
        "(people AS people_1 LEFT OUTER JOIN engineers AS "
        "engineers_1 ON people_1.person_id = engineers_1.person_id "
        "LEFT OUTER JOIN managers AS managers_1 "
        "ON people_1.person_id = managers_1.person_id "
        "LEFT OUTER JOIN boss AS boss_1 ON "
        "managers_1.person_id = boss_1.boss_id) "
        "ON companies.company_id = people_1.company_id"
    )

    aliased_pjoin = (
        "(SELECT people.person_id AS people_person_id, people.company_id "
        "AS people_company_id, people.name AS people_name, people.type "
        "AS people_type, engineers.person_id AS engineers_person_id, "
        "engineers.status AS engineers_status, engineers.engineer_name "
        "AS engineers_engineer_name, engineers.primary_language "
        "AS engineers_primary_language, managers.person_id "
        "AS managers_person_id, managers.status AS managers_status, "
        "managers.manager_name AS managers_manager_name, "
        "boss.boss_id AS boss_boss_id, boss.golf_swing AS boss_golf_swing "
        "FROM people LEFT OUTER JOIN engineers ON people.person_id = "
        "engineers.person_id LEFT OUTER JOIN managers ON "
        "people.person_id = managers.person_id LEFT OUTER JOIN boss "
        "ON managers.person_id = boss.boss_id) AS anon_1 "
        "ON companies.company_id = anon_1.people_company_id"
    )

    person_paperwork_expected = (
        "SELECT companies.company_id, companies.name FROM companies "
        "JOIN people ON companies.company_id = people.company_id "
        "JOIN paperwork ON people.person_id = paperwork.person_id"
    )

    c_to_p_whereclause = (
        "SELECT companies.company_id, companies.name FROM companies "
        "JOIN people ON companies.company_id = people.company_id "
        "WHERE people.name = :name_1"
    )

    poly_columns = "SELECT people.person_id FROM people"

    def test_straight(self):
        Company, Person, Manager, Engineer = self.classes(
            "Company", "Person", "Manager", "Engineer"
        )

        stmt1 = select(Company).select_from(
            orm_join(Company, Person, Company.employees)
        )
        stmt2 = select(Company).join(Company.employees)
        stmt3 = (
            fixture_session()
            .query(Company)
            .join(Company.employees)
            ._final_statement(legacy_query_style=False)
        )

        self.assert_compile(stmt1, self.straight_company_to_person_expected)
        self.assert_compile(stmt2, self.straight_company_to_person_expected)
        self.assert_compile(stmt3, self.straight_company_to_person_expected)

    def test_columns(self):
        Company, Person, Manager, Engineer = self.classes(
            "Company", "Person", "Manager", "Engineer"
        )

        stmt = select(Person.person_id)

        self.assert_compile(stmt, self.poly_columns)

    def test_straight_whereclause(self):
        Company, Person, Manager, Engineer = self.classes(
            "Company", "Person", "Manager", "Engineer"
        )

        stmt1 = (
            select(Company)
            .select_from(orm_join(Company, Person, Company.employees))
            .where(Person.name == "ed")
        )

        stmt2 = (
            select(Company).join(Company.employees).where(Person.name == "ed")
        )
        stmt3 = (
            fixture_session()
            .query(Company)
            .join(Company.employees)
            .filter(Person.name == "ed")
            ._final_statement(legacy_query_style=False)
        )

        self.assert_compile(stmt1, self.c_to_p_whereclause)
        self.assert_compile(stmt2, self.c_to_p_whereclause)
        self.assert_compile(stmt3, self.c_to_p_whereclause)

    def test_two_level(self):
        Company, Person, Paperwork = self.classes(
            "Company", "Person", "Paperwork"
        )

        stmt1 = select(Company).select_from(
            orm_join(Company, Person, Company.employees).join(
                Paperwork, Person.paperwork
            )
        )

        stmt2 = select(Company).join(Company.employees).join(Person.paperwork)
        stmt3 = (
            fixture_session()
            .query(Company)
            .join(Company.employees)
            .join(Person.paperwork)
            ._final_statement(legacy_query_style=False)
        )

        self.assert_compile(stmt1, self.person_paperwork_expected)
        self.assert_compile(stmt2, self.person_paperwork_expected)
        self.assert_compile(stmt3, self.person_paperwork_expected)

    def test_wpoly_of_type(self):
        Company, Person, Manager, Engineer = self.classes(
            "Company", "Person", "Manager", "Engineer"
        )

        p1 = with_polymorphic(Person, "*")

        stmt1 = select(Company).select_from(
            orm_join(Company, p1, Company.employees.of_type(p1))
        )

        stmt2 = select(Company).join(Company.employees.of_type(p1))
        stmt3 = (
            fixture_session()
            .query(Company)
            .join(Company.employees.of_type(p1))
            ._final_statement(legacy_query_style=False)
        )
        expected = (
            "SELECT companies.company_id, companies.name "
            "FROM companies JOIN %s" % self.default_pjoin
        )

        self.assert_compile(stmt1, expected)
        self.assert_compile(stmt2, expected)
        self.assert_compile(stmt3, expected)

    def test_wpoly_aliased_of_type(self):
        Company, Person, Manager, Engineer = self.classes(
            "Company", "Person", "Manager", "Engineer"
        )
        s = fixture_session()

        p1 = with_polymorphic(Person, "*", aliased=True)

        stmt1 = select(Company).select_from(
            orm_join(Company, p1, Company.employees.of_type(p1))
        )

        stmt2 = select(Company).join(p1, Company.employees.of_type(p1))

        stmt3 = (
            s.query(Company)
            .join(Company.employees.of_type(p1))
            ._final_statement(legacy_query_style=False)
        )

        expected = (
            "SELECT companies.company_id, companies.name FROM companies "
            "JOIN %s" % self.aliased_pjoin
        )

        self.assert_compile(stmt1, expected)
        self.assert_compile(stmt2, expected)
        self.assert_compile(stmt3, expected)

    def test_wpoly_aliased_flat_of_type(self):
        Company, Person, Manager, Engineer = self.classes(
            "Company", "Person", "Manager", "Engineer"
        )

        p1 = with_polymorphic(Person, "*", aliased=True, flat=True)

        stmt1 = select(Company).select_from(
            orm_join(Company, p1, Company.employees.of_type(p1))
        )

        stmt2 = select(Company).join(p1, Company.employees.of_type(p1))

        stmt3 = (
            fixture_session()
            .query(Company)
            .join(Company.employees.of_type(p1))
            ._final_statement(legacy_query_style=False)
        )

        expected = (
            "SELECT companies.company_id, companies.name FROM companies "
            "JOIN %s" % self.flat_aliased_pjoin
        )

        self.assert_compile(stmt1, expected)
        self.assert_compile(stmt2, expected)
        self.assert_compile(stmt3, expected)


class RelNaturalAliasedJoinsTest(
    _poly_fixtures._PolymorphicAliasedJoins, RelationshipNaturalInheritedTest
):

    # this is the label style for the polymorphic selectable, not the
    # outside query
    label_style = LABEL_STYLE_TABLENAME_PLUS_COL

    straight_company_to_person_expected = (
        "SELECT companies.company_id, companies.name FROM companies "
        "JOIN (SELECT people.person_id AS people_person_id, people.company_id "
        "AS people_company_id, people.name AS people_name, people.type "
        "AS people_type, engineers.person_id AS engineers_person_id, "
        "engineers.status AS engineers_status, engineers.engineer_name "
        "AS engineers_engineer_name, engineers.primary_language AS "
        "engineers_primary_language, managers.person_id AS "
        "managers_person_id, managers.status AS managers_status, "
        "managers.manager_name AS managers_manager_name FROM people "
        "LEFT OUTER JOIN engineers ON people.person_id = "
        "engineers.person_id LEFT OUTER JOIN managers ON people.person_id = "
        "managers.person_id) AS pjoin ON companies.company_id = "
        "pjoin.people_company_id"
    )

    person_paperwork_expected = (
        "SELECT companies.company_id, companies.name FROM companies JOIN "
        "(SELECT people.person_id AS people_person_id, people.company_id "
        "AS people_company_id, people.name AS people_name, people.type "
        "AS people_type, engineers.person_id AS engineers_person_id, "
        "engineers.status AS engineers_status, engineers.engineer_name "
        "AS engineers_engineer_name, engineers.primary_language AS "
        "engineers_primary_language, managers.person_id AS "
        "managers_person_id, managers.status AS managers_status, "
        "managers.manager_name AS managers_manager_name FROM people "
        "LEFT OUTER JOIN engineers ON people.person_id = engineers.person_id "
        "LEFT OUTER JOIN managers ON people.person_id = managers.person_id) "
        "AS pjoin ON companies.company_id = pjoin.people_company_id "
        "JOIN paperwork ON pjoin.people_person_id = paperwork.person_id"
    )

    default_pjoin = (
        "(SELECT people.person_id AS people_person_id, "
        "people.company_id AS people_company_id, people.name AS people_name, "
        "people.type AS people_type, engineers.person_id AS "
        "engineers_person_id, engineers.status AS engineers_status, "
        "engineers.engineer_name AS engineers_engineer_name, "
        "engineers.primary_language AS engineers_primary_language, "
        "managers.person_id AS managers_person_id, managers.status "
        "AS managers_status, managers.manager_name AS managers_manager_name "
        "FROM people LEFT OUTER JOIN engineers ON people.person_id = "
        "engineers.person_id LEFT OUTER JOIN managers "
        "ON people.person_id = managers.person_id) AS pjoin "
        "ON companies.company_id = pjoin.people_company_id"
    )
    flat_aliased_pjoin = (
        "(SELECT people.person_id AS people_person_id, "
        "people.company_id AS people_company_id, people.name AS people_name, "
        "people.type AS people_type, engineers.person_id "
        "AS engineers_person_id, engineers.status AS engineers_status, "
        "engineers.engineer_name AS engineers_engineer_name, "
        "engineers.primary_language AS engineers_primary_language, "
        "managers.person_id AS managers_person_id, "
        "managers.status AS managers_status, managers.manager_name "
        "AS managers_manager_name FROM people "
        "LEFT OUTER JOIN engineers ON people.person_id = engineers.person_id "
        "LEFT OUTER JOIN managers ON people.person_id = managers.person_id) "
        "AS pjoin_1 ON companies.company_id = pjoin_1.people_company_id"
    )

    aliased_pjoin = (
        "(SELECT people.person_id AS people_person_id, people.company_id "
        "AS people_company_id, people.name AS people_name, "
        "people.type AS people_type, engineers.person_id AS "
        "engineers_person_id, engineers.status AS engineers_status, "
        "engineers.engineer_name AS engineers_engineer_name, "
        "engineers.primary_language AS engineers_primary_language, "
        "managers.person_id AS managers_person_id, managers.status "
        "AS managers_status, managers.manager_name AS managers_manager_name "
        "FROM people LEFT OUTER JOIN engineers ON people.person_id = "
        "engineers.person_id LEFT OUTER JOIN managers "
        "ON people.person_id = managers.person_id) AS pjoin_1 "
        "ON companies.company_id = pjoin_1.people_company_id"
    )

    c_to_p_whereclause = (
        "SELECT companies.company_id, companies.name FROM companies "
        "JOIN (SELECT people.person_id AS people_person_id, "
        "people.company_id AS people_company_id, people.name AS people_name, "
        "people.type AS people_type, engineers.person_id AS "
        "engineers_person_id, engineers.status AS engineers_status, "
        "engineers.engineer_name AS engineers_engineer_name, "
        "engineers.primary_language AS engineers_primary_language, "
        "managers.person_id AS managers_person_id, managers.status "
        "AS managers_status, managers.manager_name AS managers_manager_name "
        "FROM people LEFT OUTER JOIN engineers "
        "ON people.person_id = engineers.person_id "
        "LEFT OUTER JOIN managers ON people.person_id = managers.person_id) "
        "AS pjoin ON companies.company_id = pjoin.people_company_id "
        "WHERE pjoin.people_name = :people_name_1"
    )

    poly_columns = (
        "SELECT pjoin.people_person_id FROM (SELECT people.person_id AS "
        "people_person_id, people.company_id AS people_company_id, "
        "people.name AS people_name, people.type AS people_type, "
        "engineers.person_id AS engineers_person_id, engineers.status "
        "AS engineers_status, engineers.engineer_name AS "
        "engineers_engineer_name, engineers.primary_language AS "
        "engineers_primary_language, managers.person_id AS "
        "managers_person_id, managers.status AS managers_status, "
        "managers.manager_name AS managers_manager_name FROM people "
        "LEFT OUTER JOIN engineers ON people.person_id = engineers.person_id "
        "LEFT OUTER JOIN managers ON people.person_id = managers.person_id) "
        "AS pjoin"
    )


class RelNaturalAliasedJoinsDisamTest(
    _poly_fixtures._PolymorphicAliasedJoins, RelationshipNaturalInheritedTest
):
    # this is the label style for the polymorphic selectable, not the
    # outside query
    label_style = LABEL_STYLE_DISAMBIGUATE_ONLY

    straight_company_to_person_expected = (
        "SELECT companies.company_id, companies.name FROM companies JOIN "
        "(SELECT people.person_id AS person_id, "
        "people.company_id AS company_id, people.name AS name, "
        "people.type AS type, engineers.person_id AS person_id_1, "
        "engineers.status AS status, "
        "engineers.engineer_name AS engineer_name, "
        "engineers.primary_language AS primary_language, "
        "managers.person_id AS person_id_2, managers.status AS status_1, "
        "managers.manager_name AS manager_name FROM people "
        "LEFT OUTER JOIN engineers ON people.person_id = engineers.person_id "
        "LEFT OUTER JOIN managers ON people.person_id = managers.person_id) "
        "AS pjoin ON companies.company_id = pjoin.company_id"
    )

    person_paperwork_expected = (
        "SELECT companies.company_id, companies.name FROM companies "
        "JOIN (SELECT people.person_id AS person_id, people.company_id "
        "AS company_id, people.name AS name, people.type AS type, "
        "engineers.person_id AS person_id_1, engineers.status AS status, "
        "engineers.engineer_name AS engineer_name, "
        "engineers.primary_language AS primary_language, managers.person_id "
        "AS person_id_2, managers.status AS status_1, managers.manager_name "
        "AS manager_name FROM people LEFT OUTER JOIN engineers "
        "ON people.person_id = engineers.person_id "
        "LEFT OUTER JOIN managers ON people.person_id = managers.person_id) "
        "AS pjoin ON companies.company_id = pjoin.company_id "
        "JOIN paperwork ON pjoin.person_id = paperwork.person_id"
    )

    default_pjoin = (
        "(SELECT people.person_id AS person_id, people.company_id AS "
        "company_id, people.name AS name, people.type AS type, "
        "engineers.person_id AS person_id_1, engineers.status AS status, "
        "engineers.engineer_name AS engineer_name, engineers.primary_language "
        "AS primary_language, managers.person_id AS person_id_2, "
        "managers.status AS status_1, managers.manager_name AS manager_name "
        "FROM people LEFT OUTER JOIN engineers ON people.person_id = "
        "engineers.person_id LEFT OUTER JOIN managers ON people.person_id = "
        "managers.person_id) AS pjoin "
        "ON companies.company_id = pjoin.company_id"
    )
    flat_aliased_pjoin = (
        "(SELECT people.person_id AS person_id, people.company_id AS "
        "company_id, people.name AS name, people.type AS type, "
        "engineers.person_id AS person_id_1, engineers.status AS status, "
        "engineers.engineer_name AS engineer_name, "
        "engineers.primary_language AS primary_language, "
        "managers.person_id AS person_id_2, managers.status AS status_1, "
        "managers.manager_name AS manager_name FROM people "
        "LEFT OUTER JOIN engineers ON people.person_id = engineers.person_id "
        "LEFT OUTER JOIN managers ON people.person_id = managers.person_id) "
        "AS pjoin_1 ON companies.company_id = pjoin_1.company_id"
    )

    aliased_pjoin = (
        "(SELECT people.person_id AS person_id, people.company_id AS "
        "company_id, people.name AS name, people.type AS type, "
        "engineers.person_id AS person_id_1, engineers.status AS status, "
        "engineers.engineer_name AS engineer_name, engineers.primary_language "
        "AS primary_language, managers.person_id AS person_id_2, "
        "managers.status AS status_1, managers.manager_name AS manager_name "
        "FROM people LEFT OUTER JOIN engineers ON people.person_id = "
        "engineers.person_id LEFT OUTER JOIN managers ON people.person_id = "
        "managers.person_id) AS pjoin_1 "
        "ON companies.company_id = pjoin_1.company_id"
    )

    c_to_p_whereclause = (
        "SELECT companies.company_id, companies.name FROM companies JOIN "
        "(SELECT people.person_id AS person_id, "
        "people.company_id AS company_id, people.name AS name, "
        "people.type AS type, engineers.person_id AS person_id_1, "
        "engineers.status AS status, "
        "engineers.engineer_name AS engineer_name, "
        "engineers.primary_language AS primary_language, "
        "managers.person_id AS person_id_2, managers.status AS status_1, "
        "managers.manager_name AS manager_name FROM people "
        "LEFT OUTER JOIN engineers ON people.person_id = engineers.person_id "
        "LEFT OUTER JOIN managers ON people.person_id = managers.person_id) "
        "AS pjoin ON companies.company_id = pjoin.company_id "
        "WHERE pjoin.name = :name_1"
    )

    poly_columns = (
        "SELECT pjoin.person_id FROM (SELECT people.person_id AS "
        "person_id, people.company_id AS company_id, people.name AS name, "
        "people.type AS type, engineers.person_id AS person_id_1, "
        "engineers.status AS status, "
        "engineers.engineer_name AS engineer_name, "
        "engineers.primary_language AS primary_language, "
        "managers.person_id AS person_id_2, "
        "managers.status AS status_1, managers.manager_name AS manager_name "
        "FROM people LEFT OUTER JOIN engineers "
        "ON people.person_id = engineers.person_id "
        "LEFT OUTER JOIN managers "
        "ON people.person_id = managers.person_id) AS pjoin"
    )


class RawSelectTest(QueryTest, AssertsCompiledSQL):
    """older tests from test_query.   Here, they are converted to use
    future selects with ORM compilation.

    """

    __dialect__ = "default"

    def test_select_from_entity(self):
        User = self.classes.User

        self.assert_compile(
            select(literal_column("*")).select_from(User),
            "SELECT * FROM users",
        )

    def test_where_relationship(self):
        User = self.classes.User

        stmt1 = select(User).where(User.addresses)
        stmt2 = (
            fixture_session()
            .query(User)
            .filter(User.addresses)
            ._final_statement(legacy_query_style=False)
        )

        expected = (
            "SELECT users.id, users.name FROM users, addresses "
            "WHERE users.id = addresses.user_id"
        )

        self.assert_compile(stmt1, expected)
        self.assert_compile(stmt2, expected)

    def test_where_m2m_relationship(self):
        Item = self.classes.Item

        expected = (
            "SELECT items.id, items.description FROM items, "
            "item_keywords AS item_keywords_1, keywords "
            "WHERE items.id = item_keywords_1.item_id "
            "AND keywords.id = item_keywords_1.keyword_id"
        )

        stmt1 = select(Item).where(Item.keywords)
        stmt2 = (
            fixture_session()
            .query(Item)
            .filter(Item.keywords)
            ._final_statement(legacy_query_style=False)
        )
        self.assert_compile(stmt1, expected)
        self.assert_compile(stmt2, expected)

    def test_inline_select_from_entity(self):
        User = self.classes.User

        expected = "SELECT * FROM users"
        stmt1 = select(literal_column("*")).select_from(User)
        stmt2 = (
            fixture_session()
            .query(literal_column("*"))
            .select_from(User)
            ._final_statement(legacy_query_style=False)
        )

        self.assert_compile(stmt1, expected)
        self.assert_compile(stmt2, expected)

    def test_select_from_aliased_entity(self):
        User = self.classes.User
        ua = aliased(User, name="ua")

        stmt1 = select(literal_column("*")).select_from(ua)
        stmt2 = (
            fixture_session()
            .query(literal_column("*"))
            .select_from(ua)
            ._final_statement(legacy_query_style=False)
        )

        expected = "SELECT * FROM users AS ua"

        self.assert_compile(stmt1, expected)
        self.assert_compile(stmt2, expected)

    def test_correlate_entity(self):
        User = self.classes.User
        Address = self.classes.Address

        expected = (
            "SELECT users.name, addresses.id, "
            "(SELECT count(addresses.id) AS count_1 "
            "FROM addresses WHERE users.id = addresses.user_id) AS anon_1 "
            "FROM users, addresses"
        )

        stmt1 = select(
            User.name,
            Address.id,
            select(func.count(Address.id))
            .where(User.id == Address.user_id)
            .correlate(User)
            .scalar_subquery(),
        )
        stmt2 = (
            fixture_session()
            .query(
                User.name,
                Address.id,
                select(func.count(Address.id))
                .where(User.id == Address.user_id)
                .correlate(User)
                .scalar_subquery(),
            )
            ._final_statement(legacy_query_style=False)
        )

        self.assert_compile(stmt1, expected)
        self.assert_compile(stmt2, expected)

    def test_correlate_aliased_entity(self):
        User = self.classes.User
        Address = self.classes.Address
        uu = aliased(User, name="uu")

        stmt1 = select(
            uu.name,
            Address.id,
            select(func.count(Address.id))
            .where(uu.id == Address.user_id)
            .correlate(uu)
            .scalar_subquery(),
        )

        stmt2 = (
            fixture_session()
            .query(
                uu.name,
                Address.id,
                select(func.count(Address.id))
                .where(uu.id == Address.user_id)
                .correlate(uu)
                .scalar_subquery(),
            )
            ._final_statement(legacy_query_style=False)
        )

        expected = (
            "SELECT uu.name, addresses.id, "
            "(SELECT count(addresses.id) AS count_1 "
            "FROM addresses WHERE uu.id = addresses.user_id) AS anon_1 "
            "FROM users AS uu, addresses"
        )

        self.assert_compile(stmt1, expected)
        self.assert_compile(stmt2, expected)

    def test_columns_clause_entity(self):
        User = self.classes.User

        expected = "SELECT users.id, users.name FROM users"

        stmt1 = select(User)
        stmt2 = (
            fixture_session()
            .query(User)
            ._final_statement(legacy_query_style=False)
        )

        self.assert_compile(stmt1, expected)
        self.assert_compile(stmt2, expected)

    def test_columns_clause_columns(self):
        User = self.classes.User

        expected = "SELECT users.id, users.name FROM users"

        stmt1 = select(User.id, User.name)
        stmt2 = (
            fixture_session()
            .query(User.id, User.name)
            ._final_statement(legacy_query_style=False)
        )

        self.assert_compile(stmt1, expected)
        self.assert_compile(stmt2, expected)

    def test_columns_clause_aliased_columns(self):
        User = self.classes.User
        ua = aliased(User, name="ua")

        stmt1 = select(ua.id, ua.name)
        stmt2 = (
            fixture_session()
            .query(ua.id, ua.name)
            ._final_statement(legacy_query_style=False)
        )
        expected = "SELECT ua.id, ua.name FROM users AS ua"

        self.assert_compile(stmt1, expected)
        self.assert_compile(stmt2, expected)

    def test_columns_clause_aliased_entity(self):
        User = self.classes.User
        ua = aliased(User, name="ua")

        stmt1 = select(ua)
        stmt2 = (
            fixture_session()
            .query(ua)
            ._final_statement(legacy_query_style=False)
        )
        expected = "SELECT ua.id, ua.name FROM users AS ua"

        self.assert_compile(stmt1, expected)
        self.assert_compile(stmt2, expected)

    def test_core_join_in_select_from_no_onclause(self):
        User = self.classes.User
        Address = self.classes.Address

        self.assert_compile(
            select(User).select_from(core_join(User, Address)),
            "SELECT users.id, users.name FROM users "
            "JOIN addresses ON users.id = addresses.user_id",
        )

    def test_join_to_entity_no_onclause(self):
        User = self.classes.User
        Address = self.classes.Address

        self.assert_compile(
            select(User).join(Address),
            "SELECT users.id, users.name FROM users "
            "JOIN addresses ON users.id = addresses.user_id",
        )

    def test_insert_from_query(self):
        User = self.classes.User
        Address = self.classes.Address

        s = fixture_session()
        q = s.query(User.id, User.name).filter_by(name="ed")
        self.assert_compile(
            insert(Address).from_select(("id", "email_address"), q),
            "INSERT INTO addresses (id, email_address) "
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users WHERE users.name = :name_1",
        )

    def test_insert_from_query_col_attr(self):
        User = self.classes.User
        Address = self.classes.Address

        s = fixture_session()
        q = s.query(User.id, User.name).filter_by(name="ed")
        self.assert_compile(
            insert(Address).from_select(
                (Address.id, Address.email_address), q
            ),
            "INSERT INTO addresses (id, email_address) "
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users WHERE users.name = :name_1",
        )

    def test_update_from_entity(self):
        from sqlalchemy.sql import update

        User = self.classes.User
        self.assert_compile(
            update(User), "UPDATE users SET id=:id, name=:name"
        )

        self.assert_compile(
            update(User).values(name="ed").where(User.id == 5),
            "UPDATE users SET name=:name WHERE users.id = :id_1",
            checkparams={"id_1": 5, "name": "ed"},
        )

        self.assert_compile(
            update(User).values({User.name: "ed"}).where(User.id == 5),
            "UPDATE users SET name=:name WHERE users.id = :id_1",
            checkparams={"id_1": 5, "name": "ed"},
        )

    def test_delete_from_entity(self):
        from sqlalchemy.sql import delete

        User = self.classes.User
        self.assert_compile(delete(User), "DELETE FROM users")

        self.assert_compile(
            delete(User).where(User.id == 5),
            "DELETE FROM users WHERE users.id = :id_1",
            checkparams={"id_1": 5},
        )

    def test_insert_from_entity(self):
        from sqlalchemy.sql import insert

        User = self.classes.User
        self.assert_compile(
            insert(User), "INSERT INTO users (id, name) VALUES (:id, :name)"
        )

        self.assert_compile(
            insert(User).values(name="ed"),
            "INSERT INTO users (name) VALUES (:name)",
            checkparams={"name": "ed"},
        )

    def test_col_prop_builtin_function(self):
        class Foo(object):
            pass

        mapper(
            Foo,
            self.tables.users,
            properties={
                "foob": column_property(
                    func.coalesce(self.tables.users.c.name)
                )
            },
        )

        stmt1 = select(Foo).where(Foo.foob == "somename").order_by(Foo.foob)
        stmt2 = (
            fixture_session()
            .query(Foo)
            .filter(Foo.foob == "somename")
            .order_by(Foo.foob)
            ._final_statement(legacy_query_style=False)
        )

        expected = (
            "SELECT coalesce(users.name) AS coalesce_1, "
            "users.id, users.name FROM users "
            "WHERE coalesce(users.name) = :param_1 "
            "ORDER BY coalesce_1"
        )
        self.assert_compile(stmt1, expected)
        self.assert_compile(stmt2, expected)
