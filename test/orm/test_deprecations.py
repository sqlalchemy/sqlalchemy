import sqlalchemy as sa
from sqlalchemy import and_
from sqlalchemy import cast
from sqlalchemy import column
from sqlalchemy import desc
from sqlalchemy import event
from sqlalchemy import exc as sa_exc
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import Integer
from sqlalchemy import literal_column
from sqlalchemy import MetaData
from sqlalchemy import or_
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import table
from sqlalchemy import testing
from sqlalchemy import text
from sqlalchemy import true
from sqlalchemy.engine import default
from sqlalchemy.orm import aliased
from sqlalchemy.orm import as_declarative
from sqlalchemy.orm import attributes
from sqlalchemy.orm import backref
from sqlalchemy.orm import collections
from sqlalchemy.orm import column_property
from sqlalchemy.orm import configure_mappers
from sqlalchemy.orm import contains_alias
from sqlalchemy.orm import contains_eager
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import declared_attr
from sqlalchemy.orm import defer
from sqlalchemy.orm import deferred
from sqlalchemy.orm import eagerload
from sqlalchemy.orm import foreign
from sqlalchemy.orm import instrumentation
from sqlalchemy.orm import joinedload
from sqlalchemy.orm import mapper
from sqlalchemy.orm import relation
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session
from sqlalchemy.orm import subqueryload
from sqlalchemy.orm import synonym
from sqlalchemy.orm import undefer
from sqlalchemy.orm import with_loader_criteria
from sqlalchemy.orm import with_parent
from sqlalchemy.orm import with_polymorphic
from sqlalchemy.orm.collections import collection
from sqlalchemy.orm.util import polymorphic_union
from sqlalchemy.sql import elements
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import assertions
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import eq_
from sqlalchemy.testing import eq_ignore_whitespace
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.testing import is_true
from sqlalchemy.testing import mock
from sqlalchemy.testing.assertsql import CompiledSQL
from sqlalchemy.testing.fixtures import ComparableEntity
from sqlalchemy.testing.fixtures import fixture_session
from sqlalchemy.testing.mock import call
from sqlalchemy.testing.mock import Mock
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table
from . import _fixtures
from .inheritance import _poly_fixtures
from .test_bind import GetBindTest as _GetBindTest
from .test_dynamic import _DynamicFixture
from .test_events import _RemoveListeners
from .test_options import PathTest as OptionsPathTest
from .test_query import QueryTest
from .test_transaction import _LocalFixture


join_aliased_dep = (
    r"The ``aliased`` and ``from_joinpoint`` keyword arguments to "
    r"Query.join\(\)"
)

w_polymorphic_dep = (
    r"The Query.with_polymorphic\(\) method is "
    "considered legacy as of the 1.x series"
)

join_chain_dep = (
    r"Passing a chain of multiple join conditions to Query.join\(\)"
)

join_strings_dep = "Using strings to indicate relationship names in Query.join"
join_tuple_form = (
    r"Query.join\(\) will no longer accept tuples as "
    "arguments in SQLAlchemy 2.0."
)


class DeprecatedQueryTest(_fixtures.FixtureTest, AssertsCompiledSQL):
    __dialect__ = "default"

    run_setup_mappers = "once"
    run_inserts = "once"
    run_deletes = None

    @classmethod
    def setup_mappers(cls):
        cls._setup_stock_mapping()

    @classmethod
    def _expect_implicit_subquery(cls):
        return assertions.expect_deprecated(
            "Implicit coercion of SELECT and textual SELECT constructs into "
            r"FROM clauses is deprecated; please call \.subquery\(\) on any "
            "Core select or ORM Query object in order to produce a "
            "subquery object."
        )

    def test_deprecated_negative_slices(self):
        User = self.classes.User

        sess = fixture_session()
        q = sess.query(User).order_by(User.id)

        with testing.expect_deprecated(
            "Support for negative indexes for SQL index / slice operators"
        ):
            eq_(q[-5:-2], [User(id=7), User(id=8)])

        with testing.expect_deprecated(
            "Support for negative indexes for SQL index / slice operators"
        ):
            eq_(q[-1], User(id=10))

        with testing.expect_deprecated(
            "Support for negative indexes for SQL index / slice operators"
        ):
            eq_(q[-2], User(id=9))

        with testing.expect_deprecated(
            "Support for negative indexes for SQL index / slice operators"
        ):
            eq_(q[:-2], [User(id=7), User(id=8)])

        # this doesn't evaluate anything because it's a net-negative
        eq_(q[-2:-5], [])

    def test_deprecated_negative_slices_compile(self):
        User = self.classes.User

        sess = fixture_session()
        q = sess.query(User).order_by(User.id)

        with testing.expect_deprecated(
            "Support for negative indexes for SQL index / slice operators"
        ):
            self.assert_sql(
                testing.db,
                lambda: q[-5:-2],
                [
                    (
                        "SELECT users.id AS users_id, users.name "
                        "AS users_name "
                        "FROM users ORDER BY users.id",
                        {},
                    )
                ],
            )

        with testing.expect_deprecated(
            "Support for negative indexes for SQL index / slice operators"
        ):
            self.assert_sql(
                testing.db,
                lambda: q[-5:],
                [
                    (
                        "SELECT users.id AS users_id, users.name "
                        "AS users_name "
                        "FROM users ORDER BY users.id",
                        {},
                    )
                ],
            )

    def test_aliased(self):
        User = self.classes.User

        s = fixture_session()

        with testing.expect_deprecated_20(join_aliased_dep):
            q1 = s.query(User).join(User.addresses, aliased=True)

        self.assert_compile(
            q1,
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users JOIN addresses AS addresses_1 "
            "ON users.id = addresses_1.user_id",
        )

    def test_from_joinpoint(self):
        User = self.classes.User
        Address = self.classes.Address

        s = fixture_session()

        u1 = aliased(User)

        with testing.expect_deprecated_20(join_aliased_dep):
            q1 = (
                s.query(u1)
                .join(u1.addresses)
                .join(Address.user, from_joinpoint=True)
            )

        self.assert_compile(
            q1,
            "SELECT users_1.id AS users_1_id, users_1.name AS users_1_name "
            "FROM users AS users_1 JOIN addresses ON users_1.id = "
            "addresses.user_id JOIN users ON users.id = addresses.user_id",
        )

    def test_multiple_joins(self):
        User = self.classes.User
        Address = self.classes.Address

        s = fixture_session()

        u1 = aliased(User)

        with testing.expect_deprecated_20(join_chain_dep):
            q1 = s.query(u1).join(u1.addresses, Address.user)

        self.assert_compile(
            q1,
            "SELECT users_1.id AS users_1_id, users_1.name AS users_1_name "
            "FROM users AS users_1 JOIN addresses ON users_1.id = "
            "addresses.user_id JOIN users ON users.id = addresses.user_id",
        )

    def test_str_join_target(self):
        User = self.classes.User

        s = fixture_session()

        with testing.expect_deprecated_20(join_strings_dep):
            q1 = s.query(User).join("addresses")

        self.assert_compile(
            q1,
            "SELECT users.id AS users_id, users.name AS "
            "users_name FROM users JOIN addresses "
            "ON users.id = addresses.user_id",
        )

    def test_str_rel_loader_opt(self):
        User = self.classes.User

        s = fixture_session()

        q1 = s.query(User).options(joinedload("addresses"))

        with testing.expect_deprecated_20(
            "Using strings to indicate column or relationship "
            "paths in loader options"
        ):
            self.assert_compile(
                q1,
                "SELECT users.id AS users_id, users.name AS users_name, "
                "addresses_1.id AS addresses_1_id, addresses_1.user_id "
                "AS addresses_1_user_id, addresses_1.email_address AS "
                "addresses_1_email_address FROM users LEFT OUTER JOIN "
                "addresses AS addresses_1 ON users.id = addresses_1.user_id "
                "ORDER BY addresses_1.id",
            )

    def test_dotted_options(self):
        User = self.classes.User

        sess = fixture_session()

        with testing.expect_deprecated_20(
            "Using strings to indicate column or relationship "
            "paths in loader options"
        ):
            q2 = (
                sess.query(User)
                .order_by(User.id)
                .options(sa.orm.joinedload("orders"))
                .options(sa.orm.joinedload("orders.items"))
                .options(sa.orm.joinedload("orders.items.keywords"))
            )
            u = q2.all()

        def go():
            u[0].orders[1].items[0].keywords[1]

        self.sql_count_(0, go)

    def test_str_col_loader_opt(self):
        User = self.classes.User

        s = fixture_session()

        q1 = s.query(User).options(defer("name"))

        with testing.expect_deprecated_20(
            "Using strings to indicate column or relationship "
            "paths in loader options"
        ):
            self.assert_compile(q1, "SELECT users.id AS users_id FROM users")

    def test_str_with_parent(self):
        User = self.classes.User
        Address = self.classes.Address

        s = fixture_session()

        u1 = User(id=1)

        with testing.expect_deprecated_20(
            r"Using strings to indicate relationship names in the ORM "
            r"with_parent\(\)",
        ):
            q1 = s.query(Address).filter(with_parent(u1, "addresses"))

        self.assert_compile(
            q1,
            "SELECT addresses.id AS addresses_id, addresses.user_id "
            "AS addresses_user_id, addresses.email_address "
            "AS addresses_email_address FROM addresses "
            "WHERE :param_1 = addresses.user_id",
        )

        with testing.expect_deprecated_20(
            r"Using strings to indicate relationship names in the ORM "
            r"with_parent\(\)",
        ):
            q1 = s.query(Address).with_parent(u1, "addresses")

        self.assert_compile(
            q1,
            "SELECT addresses.id AS addresses_id, addresses.user_id "
            "AS addresses_user_id, addresses.email_address "
            "AS addresses_email_address FROM addresses "
            "WHERE :param_1 = addresses.user_id",
        )

    def test_invalid_column(self):
        User = self.classes.User

        s = fixture_session()
        q = s.query(User.id)

        with testing.expect_deprecated(r"Query.add_column\(\) is deprecated"):
            q = q.add_column(User.name)

        self.assert_compile(
            q,
            "SELECT users.id AS users_id, users.name AS users_name FROM users",
        )

    def test_via_textasfrom_select_from(self):
        User = self.classes.User
        s = fixture_session()

        with self._expect_implicit_subquery():
            eq_(
                s.query(User)
                .select_entity_from(
                    text("select * from users").columns(User.id, User.name)
                )
                .order_by(User.id)
                .all(),
                [User(id=7), User(id=8), User(id=9), User(id=10)],
            )

    def test_text_as_column(self):
        User = self.classes.User

        s = fixture_session()

        # TODO: this works as of "use rowproxy for ORM keyed tuple"
        # Ieb9085e9bcff564359095b754da9ae0af55679f0
        # but im not sure how this relates to things here
        q = s.query(User.id, text("users.name"))
        self.assert_compile(
            q, "SELECT users.id AS users_id, users.name FROM users"
        )
        eq_(q.all(), [(7, "jack"), (8, "ed"), (9, "fred"), (10, "chuck")])

        # same here, this was "passing string names to Query.columns"
        # deprecation message, that's gone here?
        assert_raises_message(
            sa.exc.ArgumentError,
            "Textual column expression 'name' should be explicitly",
            s.query,
            User.id,
            "name",
        )

    def test_query_as_scalar(self):
        User = self.classes.User

        s = fixture_session()
        with assertions.expect_deprecated(
            r"The Query.as_scalar\(\) method is deprecated and will "
            "be removed in a future release."
        ):
            s.query(User).as_scalar()

    def test_select_entity_from_crit(self):
        User, users = self.classes.User, self.tables.users

        sel = users.select()
        sess = fixture_session()

        with self._expect_implicit_subquery():
            eq_(
                sess.query(User)
                .select_entity_from(sel)
                .filter(User.id.in_([7, 8]))
                .all(),
                [User(name="jack", id=7), User(name="ed", id=8)],
            )

    def test_select_entity_from_select(self):
        User, users = self.classes.User, self.tables.users

        sess = fixture_session()
        with self._expect_implicit_subquery():
            self.assert_compile(
                sess.query(User.name).select_entity_from(
                    users.select().where(users.c.id > 5)
                ),
                "SELECT anon_1.name AS anon_1_name FROM "
                "(SELECT users.id AS id, users.name AS name FROM users "
                "WHERE users.id > :id_1) AS anon_1",
            )

    def test_select_entity_from_q_statement(self):
        User = self.classes.User

        sess = fixture_session()

        q = sess.query(User)
        with self._expect_implicit_subquery():
            q = sess.query(User).select_entity_from(q.statement)
        self.assert_compile(
            q.filter(User.name == "ed"),
            "SELECT anon_1.id AS anon_1_id, anon_1.name AS anon_1_name "
            "FROM (SELECT users.id AS id, users.name AS name FROM "
            "users) AS anon_1 WHERE anon_1.name = :name_1",
        )

    def test_select_from_q_statement_no_aliasing(self):
        User = self.classes.User
        sess = fixture_session()

        q = sess.query(User)
        with self._expect_implicit_subquery():
            q = sess.query(User).select_from(q.statement)
        self.assert_compile(
            q.filter(User.name == "ed"),
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users, (SELECT users.id AS id, users.name AS name FROM "
            "users) AS anon_1 WHERE users.name = :name_1",
        )

    def test_from_alias_three(self):
        User, addresses, users = (
            self.classes.User,
            self.tables.addresses,
            self.tables.users,
        )

        query = (
            users.select(users.c.id == 7)
            .union(users.select(users.c.id > 7))
            .alias("ulist")
            .outerjoin(addresses)
            .select(order_by=[text("ulist.id"), addresses.c.id])
        )
        sess = fixture_session()

        # better way.  use select_entity_from()
        def go():
            with self._expect_implicit_subquery():
                result = (
                    sess.query(User)
                    .select_entity_from(query)
                    .options(contains_eager("addresses"))
                    .all()
                )
            assert self.static.user_address_result == result

        self.assert_sql_count(testing.db, go, 1)

    def test_from_alias_four(self):
        User, addresses, users = (
            self.classes.User,
            self.tables.addresses,
            self.tables.users,
        )

        sess = fixture_session()

        # same thing, but alias addresses, so that the adapter
        # generated by select_entity_from() is wrapped within
        # the adapter created by contains_eager()
        adalias = addresses.alias()
        query = (
            users.select(users.c.id == 7)
            .union(users.select(users.c.id > 7))
            .alias("ulist")
            .outerjoin(adalias)
            .select(order_by=[text("ulist.id"), adalias.c.id])
        )

        def go():
            with self._expect_implicit_subquery():
                result = (
                    sess.query(User)
                    .select_entity_from(query)
                    .options(contains_eager("addresses", alias=adalias))
                    .all()
                )
            assert self.static.user_address_result == result

        self.assert_sql_count(testing.db, go, 1)

    def test_select(self):
        users = self.tables.users

        sess = fixture_session()

        with self._expect_implicit_subquery():
            stmt = sess.query(users).select_entity_from(users.select())

        with testing.expect_deprecated_20(r"The Query.with_labels\(\)"):
            stmt = stmt.apply_labels().statement
        self.assert_compile(
            stmt,
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users, "
            "(SELECT users.id AS id, users.name AS name FROM users) "
            "AS anon_1",
        )

    def test_apply_labels(self):
        User = self.classes.User

        with testing.expect_deprecated_20(
            r"The Query.with_labels\(\) and Query.apply_labels\(\) "
            "method is considered legacy"
        ):
            q = fixture_session().query(User).apply_labels().statement

        self.assert_compile(
            q,
            "SELECT users.id AS users_id, users.name AS users_name FROM users",
        )

    def test_with_labels(self):
        User = self.classes.User

        with testing.expect_deprecated_20(
            r"The Query.with_labels\(\) and Query.apply_labels\(\) "
            "method is considered legacy"
        ):
            q = fixture_session().query(User).with_labels().statement

        self.assert_compile(
            q,
            "SELECT users.id AS users_id, users.name AS users_name FROM users",
        )

    def test_join(self):
        users, Address, User = (
            self.tables.users,
            self.classes.Address,
            self.classes.User,
        )

        # mapper(User, users, properties={"addresses": relationship(Address)})
        # mapper(Address, addresses)

        sel = users.select(users.c.id.in_([7, 8]))
        sess = fixture_session()

        with self._expect_implicit_subquery():
            result = (
                sess.query(User)
                .select_entity_from(sel)
                .join("addresses")
                .add_entity(Address)
                .order_by(User.id)
                .order_by(Address.id)
                .all()
            )

        eq_(
            result,
            [
                (
                    User(name="jack", id=7),
                    Address(user_id=7, email_address="jack@bean.com", id=1),
                ),
                (
                    User(name="ed", id=8),
                    Address(user_id=8, email_address="ed@wood.com", id=2),
                ),
                (
                    User(name="ed", id=8),
                    Address(user_id=8, email_address="ed@bettyboop.com", id=3),
                ),
                (
                    User(name="ed", id=8),
                    Address(user_id=8, email_address="ed@lala.com", id=4),
                ),
            ],
        )

        adalias = aliased(Address)
        with self._expect_implicit_subquery():
            result = (
                sess.query(User)
                .select_entity_from(sel)
                .join(adalias, "addresses")
                .add_entity(adalias)
                .order_by(User.id)
                .order_by(adalias.id)
                .all()
            )
        eq_(
            result,
            [
                (
                    User(name="jack", id=7),
                    Address(user_id=7, email_address="jack@bean.com", id=1),
                ),
                (
                    User(name="ed", id=8),
                    Address(user_id=8, email_address="ed@wood.com", id=2),
                ),
                (
                    User(name="ed", id=8),
                    Address(user_id=8, email_address="ed@bettyboop.com", id=3),
                ),
                (
                    User(name="ed", id=8),
                    Address(user_id=8, email_address="ed@lala.com", id=4),
                ),
            ],
        )

    def test_more_joins(self):
        (users, Keyword, User) = (
            self.tables.users,
            self.classes.Keyword,
            self.classes.User,
        )

        sess = fixture_session()
        sel = users.select(users.c.id.in_([7, 8]))

        with self._expect_implicit_subquery():
            eq_(
                sess.query(User)
                .select_entity_from(sel)
                .join("orders", "items", "keywords")
                .filter(Keyword.name.in_(["red", "big", "round"]))
                .all(),
                [User(name="jack", id=7)],
            )

        with self._expect_implicit_subquery():
            eq_(
                sess.query(User)
                .select_entity_from(sel)
                .join("orders", "items", "keywords", aliased=True)
                .filter(Keyword.name.in_(["red", "big", "round"]))
                .all(),
                [User(name="jack", id=7)],
            )

    def test_join_no_order_by(self):
        User, users = self.classes.User, self.tables.users

        sel = users.select(users.c.id.in_([7, 8]))
        sess = fixture_session()

        with self._expect_implicit_subquery():
            eq_(
                sess.query(User).select_entity_from(sel).all(),
                [User(name="jack", id=7), User(name="ed", id=8)],
            )

    def test_replace_with_eager(self):
        users, Address, User = (
            self.tables.users,
            self.classes.Address,
            self.classes.User,
        )

        sel = users.select(users.c.id.in_([7, 8]))
        sess = fixture_session()

        def go():
            with self._expect_implicit_subquery():
                eq_(
                    sess.query(User)
                    .options(joinedload("addresses"))
                    .select_entity_from(sel)
                    .order_by(User.id)
                    .all(),
                    [
                        User(id=7, addresses=[Address(id=1)]),
                        User(
                            id=8,
                            addresses=[
                                Address(id=2),
                                Address(id=3),
                                Address(id=4),
                            ],
                        ),
                    ],
                )

        self.assert_sql_count(testing.db, go, 1)
        sess.expunge_all()

        def go():
            with self._expect_implicit_subquery():
                eq_(
                    sess.query(User)
                    .options(joinedload("addresses"))
                    .select_entity_from(sel)
                    .filter(User.id == 8)
                    .order_by(User.id)
                    .all(),
                    [
                        User(
                            id=8,
                            addresses=[
                                Address(id=2),
                                Address(id=3),
                                Address(id=4),
                            ],
                        )
                    ],
                )

        self.assert_sql_count(testing.db, go, 1)
        sess.expunge_all()

        def go():
            with self._expect_implicit_subquery():
                eq_(
                    sess.query(User)
                    .options(joinedload("addresses"))
                    .select_entity_from(sel)
                    .order_by(User.id)[1],
                    User(
                        id=8,
                        addresses=[
                            Address(id=2),
                            Address(id=3),
                            Address(id=4),
                        ],
                    ),
                )

        self.assert_sql_count(testing.db, go, 1)

    def test_onclause_conditional_adaption(self):
        Item, Order, orders, order_items, User = (
            self.classes.Item,
            self.classes.Order,
            self.tables.orders,
            self.tables.order_items,
            self.classes.User,
        )

        sess = fixture_session()

        oalias = orders.select()

        with self._expect_implicit_subquery():
            self.assert_compile(
                sess.query(User)
                .join(oalias, User.orders)
                .join(
                    Item,
                    and_(
                        Order.id == order_items.c.order_id,
                        order_items.c.item_id == Item.id,
                    ),
                    from_joinpoint=True,
                ),
                "SELECT users.id AS users_id, users.name AS users_name "
                "FROM users JOIN "
                "(SELECT orders.id AS id, orders.user_id AS user_id, "
                "orders.address_id AS address_id, orders.description "
                "AS description, orders.isopen AS isopen FROM orders) "
                "AS anon_1 ON users.id = anon_1.user_id JOIN items "
                "ON anon_1.id = order_items.order_id "
                "AND order_items.item_id = items.id",
                use_default_dialect=True,
            )


class SelfRefFromSelfTest(fixtures.MappedTest, AssertsCompiledSQL):
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

    def test_from_self_inside_excludes_outside(self):
        """test the propagation of aliased() from inside to outside
        on a from_self()..
        """

        Node = self.classes.Node

        sess = fixture_session()

        n1 = aliased(Node)

        # n1 is not inside the from_self(), so all cols must be maintained
        # on the outside
        with self._from_self_deprecated():
            self.assert_compile(
                sess.query(Node)
                .filter(Node.data == "n122")
                .from_self(n1, Node.id),
                "SELECT nodes_1.id AS nodes_1_id, "
                "nodes_1.parent_id AS nodes_1_parent_id, "
                "nodes_1.data AS nodes_1_data, anon_1.nodes_id "
                "AS anon_1_nodes_id "
                "FROM nodes AS nodes_1, (SELECT nodes.id AS nodes_id, "
                "nodes.parent_id AS nodes_parent_id, "
                "nodes.data AS nodes_data FROM "
                "nodes WHERE nodes.data = :data_1) AS anon_1",
                use_default_dialect=True,
            )

        parent = aliased(Node)
        grandparent = aliased(Node)
        with self._from_self_deprecated():
            q = (
                sess.query(Node, parent, grandparent)
                .join(parent, Node.parent)
                .join(grandparent, parent.parent)
                .filter(Node.data == "n122")
                .filter(parent.data == "n12")
                .filter(grandparent.data == "n1")
                .from_self()
                .limit(1)
            )

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

    def test_multiple_explicit_entities_two(self):
        Node = self.classes.Node

        sess = fixture_session()

        parent = aliased(Node)
        grandparent = aliased(Node)
        with self._from_self_deprecated():
            eq_(
                sess.query(Node, parent, grandparent)
                .join(parent, Node.parent)
                .join(grandparent, parent.parent)
                .filter(Node.data == "n122")
                .filter(parent.data == "n12")
                .filter(grandparent.data == "n1")
                .from_self()
                .first(),
                (Node(data="n122"), Node(data="n12"), Node(data="n1")),
            )

    def test_multiple_explicit_entities_three(self):
        Node = self.classes.Node

        sess = fixture_session()

        parent = aliased(Node)
        grandparent = aliased(Node)
        # same, change order around
        with self._from_self_deprecated():
            eq_(
                sess.query(parent, grandparent, Node)
                .join(parent, Node.parent)
                .join(grandparent, parent.parent)
                .filter(Node.data == "n122")
                .filter(parent.data == "n12")
                .filter(grandparent.data == "n1")
                .from_self()
                .first(),
                (Node(data="n12"), Node(data="n1"), Node(data="n122")),
            )

    def test_multiple_explicit_entities_five(self):
        Node = self.classes.Node

        sess = fixture_session()

        parent = aliased(Node)
        grandparent = aliased(Node)
        with self._from_self_deprecated():
            eq_(
                sess.query(Node, parent, grandparent)
                .join(parent, Node.parent)
                .join(grandparent, parent.parent)
                .filter(Node.data == "n122")
                .filter(parent.data == "n12")
                .filter(grandparent.data == "n1")
                .from_self()
                .options(joinedload(Node.children))
                .first(),
                (Node(data="n122"), Node(data="n12"), Node(data="n1")),
            )

    def _from_self_deprecated(self):
        return testing.expect_deprecated_20(r"The Query.from_self\(\) method")


class SelfReferentialEagerTest(fixtures.MappedTest):
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

    def test_eager_loading_with_deferred(self):
        nodes = self.tables.nodes

        class Node(fixtures.ComparableEntity):
            def append(self, node):
                self.children.append(node)

        mapper(
            Node,
            nodes,
            properties={
                "children": relationship(
                    Node, lazy="joined", join_depth=3, order_by=nodes.c.id
                ),
                "data": deferred(nodes.c.data),
            },
        )
        sess = fixture_session()
        n1 = Node(data="n1")
        n1.append(Node(data="n11"))
        n1.append(Node(data="n12"))
        sess.add(n1)
        sess.flush()
        sess.expunge_all()

        def go():
            with assertions.expect_deprecated_20(
                "Using strings to indicate column or relationship paths"
            ):
                eq_(
                    Node(
                        data="n1",
                        children=[Node(data="n11"), Node(data="n12")],
                    ),
                    sess.query(Node)
                    .options(undefer("data"), undefer("children.data"))
                    .first(),
                )

        self.assert_sql_count(testing.db, go, 1)


class LazyLoadOptSpecificityTest(fixtures.DeclarativeMappedTest):
    """test for [ticket:3963]"""

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class A(Base):
            __tablename__ = "a"
            id = Column(Integer, primary_key=True)
            bs = relationship("B")

        class B(Base):
            __tablename__ = "b"
            id = Column(Integer, primary_key=True)
            a_id = Column(ForeignKey("a.id"))
            cs = relationship("C")

        class C(Base):
            __tablename__ = "c"
            id = Column(Integer, primary_key=True)
            b_id = Column(ForeignKey("b.id"))

    @classmethod
    def insert_data(cls, connection):
        A, B, C = cls.classes("A", "B", "C")
        s = Session(connection)
        s.add(A(id=1, bs=[B(cs=[C()])]))
        s.add(A(id=2))
        s.commit()

    def _run_tests(self, query, expected):
        def go():
            for a, _ in query:
                for b in a.bs:
                    b.cs

        self.assert_sql_count(testing.db, go, expected)

    def test_string_options_aliased_whatever(self):
        A, B, C = self.classes("A", "B", "C")
        s = fixture_session()
        aa = aliased(A)
        q = (
            s.query(aa, A)
            .filter(aa.id == 1)
            .filter(A.id == 2)
            .filter(aa.id != A.id)
            .options(joinedload("bs").joinedload("cs"))
        )
        with assertions.expect_deprecated_20(
            "Using strings to indicate column or relationship paths"
        ):
            self._run_tests(q, 1)

    def test_string_options_unaliased_whatever(self):
        A, B, C = self.classes("A", "B", "C")
        s = fixture_session()
        aa = aliased(A)
        q = (
            s.query(A, aa)
            .filter(aa.id == 2)
            .filter(A.id == 1)
            .filter(aa.id != A.id)
            .options(joinedload("bs").joinedload("cs"))
        )
        with assertions.expect_deprecated_20(
            "Using strings to indicate column or relationship paths"
        ):
            self._run_tests(q, 1)


class DynamicTest(_DynamicFixture, _fixtures.FixtureTest):
    def test_negative_slice_access_raises(self):
        User, Address = self._user_address_fixture()
        sess = fixture_session()
        u1 = sess.get(User, 8)

        with testing.expect_deprecated_20(
            "Support for negative indexes for SQL index / slice"
        ):
            eq_(u1.addresses[-1], Address(id=4))

        with testing.expect_deprecated_20(
            "Support for negative indexes for SQL index / slice"
        ):
            eq_(u1.addresses[-5:-2], [Address(id=2)])

        with testing.expect_deprecated_20(
            "Support for negative indexes for SQL index / slice"
        ):
            eq_(u1.addresses[-2], Address(id=3))

        with testing.expect_deprecated_20(
            "Support for negative indexes for SQL index / slice"
        ):
            eq_(u1.addresses[:-2], [Address(id=2)])


class FromSelfTest(QueryTest, AssertsCompiledSQL):
    __dialect__ = "default"

    def _from_self_deprecated(self):
        return testing.expect_deprecated_20(r"The Query.from_self\(\) method")

    def test_illegal_operations(self):

        User = self.classes.User

        s = fixture_session()

        with self._from_self_deprecated():
            q = s.query(User).from_self()
        assert_raises_message(
            sa.exc.InvalidRequestError,
            r"Can't call Query.update\(\) or Query.delete\(\)",
            q.update,
            {},
        )

        assert_raises_message(
            sa.exc.InvalidRequestError,
            r"Can't call Query.update\(\) or Query.delete\(\)",
            q.delete,
            {},
        )

    def test_columns_augmented_distinct_on(self):
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()

        with self._from_self_deprecated():
            q = (
                sess.query(
                    User.id,
                    User.name.label("foo"),
                    Address.id,
                    Address.email_address,
                )
                .distinct(Address.email_address)
                .order_by(User.id, User.name, Address.email_address)
                .from_self(User.id, User.name.label("foo"), Address.id)
            )

        # Address.email_address is added because of DISTINCT,
        # however User.id, User.name are not b.c. they're already there,
        # even though User.name is labeled
        self.assert_compile(
            q,
            "SELECT anon_1.users_id AS anon_1_users_id, anon_1.foo AS foo, "
            "anon_1.addresses_id AS anon_1_addresses_id "
            "FROM ("
            "SELECT DISTINCT ON (addresses.email_address) "
            "users.id AS users_id, users.name AS foo, "
            "addresses.id AS addresses_id, addresses.email_address AS "
            "addresses_email_address FROM users, addresses ORDER BY "
            "users.id, users.name, addresses.email_address"
            ") AS anon_1",
            dialect="postgresql",
        )

    def test_columns_augmented_roundtrip_one_from_self(self):
        """Test workaround for legacy style DISTINCT on extra column.

        See #5134

        """
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()
        with self._from_self_deprecated():
            q = (
                sess.query(User, Address.email_address)
                .join("addresses")
                .distinct()
                .from_self(User)
                .order_by(desc(Address.email_address))
            )

        eq_([User(id=7), User(id=9), User(id=8)], q.all())

    def test_columns_augmented_roundtrip_three_from_self(self):
        """Test workaround for legacy style DISTINCT on extra column.

        See #5134

        """

        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()

        with self._from_self_deprecated():
            q = (
                sess.query(
                    User.id,
                    User.name.label("foo"),
                    Address.id,
                    Address.email_address,
                )
                .join(Address, true())
                .filter(User.name == "jack")
                .filter(User.id + Address.user_id > 0)
                .distinct()
                .from_self(User.id, User.name.label("foo"), Address.id)
                .order_by(User.id, User.name, Address.email_address)
            )

        eq_(
            q.all(),
            [
                (7, "jack", 3),
                (7, "jack", 4),
                (7, "jack", 2),
                (7, "jack", 5),
                (7, "jack", 1),
            ],
        )
        for row in q:
            eq_(row._mapping.keys(), ["id", "foo", "id"])

    def test_clause_onclause(self):
        Order, User = (
            self.classes.Order,
            self.classes.User,
        )

        sess = fixture_session()
        # explicit onclause with from_self(), means
        # the onclause must be aliased against the query's custom
        # FROM object
        with self._from_self_deprecated():
            eq_(
                sess.query(User)
                .order_by(User.id)
                .offset(2)
                .from_self()
                .join(Order, User.id == Order.user_id)
                .all(),
                [User(name="fred")],
            )

    def test_from_self_resets_joinpaths(self):
        """test a join from from_self() doesn't confuse joins inside the subquery
        with the outside.
        """

        Item, Keyword = self.classes.Item, self.classes.Keyword

        sess = fixture_session()

        with self._from_self_deprecated():
            self.assert_compile(
                sess.query(Item)
                .join(Item.keywords)
                .from_self(Keyword)
                .join(Item.keywords),
                "SELECT keywords.id AS keywords_id, "
                "keywords.name AS keywords_name "
                "FROM (SELECT items.id AS items_id, "
                "items.description AS items_description "
                "FROM items JOIN item_keywords AS item_keywords_1 "
                "ON items.id = "
                "item_keywords_1.item_id JOIN keywords "
                "ON keywords.id = item_keywords_1.keyword_id) "
                "AS anon_1 JOIN item_keywords AS item_keywords_2 ON "
                "anon_1.items_id = item_keywords_2.item_id "
                "JOIN keywords ON "
                "keywords.id = item_keywords_2.keyword_id",
                use_default_dialect=True,
            )

    def test_single_prop_9(self):
        User = self.classes.User

        sess = fixture_session()
        with self._from_self_deprecated():
            self.assert_compile(
                sess.query(User)
                .filter(User.name == "ed")
                .from_self()
                .join(User.orders),
                "SELECT anon_1.users_id AS anon_1_users_id, "
                "anon_1.users_name AS anon_1_users_name "
                "FROM (SELECT users.id AS users_id, users.name AS users_name "
                "FROM users "
                "WHERE users.name = :name_1) AS anon_1 JOIN orders "
                "ON anon_1.users_id = orders.user_id",
            )

    def test_anonymous_expression_from_self_twice_oldstyle(self):
        # relies upon _orm_only_from_obj_alias setting

        sess = fixture_session()
        c1, c2 = column("c1"), column("c2")
        q1 = sess.query(c1, c2).filter(c1 == "dog")
        with self._from_self_deprecated():
            q1 = q1.from_self().from_self()
        self.assert_compile(
            q1.order_by(c1),
            "SELECT anon_1.anon_2_c1 AS anon_1_anon_2_c1, anon_1.anon_2_c2 AS "
            "anon_1_anon_2_c2 FROM (SELECT anon_2.c1 AS anon_2_c1, anon_2.c2 "
            "AS anon_2_c2 "
            "FROM (SELECT c1, c2 WHERE c1 = :c1_1) AS "
            "anon_2) AS anon_1 ORDER BY anon_1.anon_2_c1",
        )

    def test_anonymous_expression_plus_flag_aliased_join(self):
        """test that the 'dont alias non-ORM' rule remains for other
        kinds of aliasing when _from_selectable() is used."""

        User = self.classes.User
        Address = self.classes.Address
        addresses = self.tables.addresses

        sess = fixture_session()
        q1 = sess.query(User.id).filter(User.id > 5)
        with self._from_self_deprecated():
            q1 = q1.from_self()

        with testing.expect_deprecated_20(join_aliased_dep):
            q1 = q1.join(User.addresses, aliased=True).order_by(
                User.id, Address.id, addresses.c.id
            )

        self.assert_compile(
            q1,
            "SELECT anon_1.users_id AS anon_1_users_id "
            "FROM (SELECT users.id AS users_id FROM users "
            "WHERE users.id > :id_1) AS anon_1 JOIN addresses AS addresses_1 "
            "ON anon_1.users_id = addresses_1.user_id "
            "ORDER BY anon_1.users_id, addresses_1.id, addresses.id",
        )

    def test_anonymous_expression_plus_explicit_aliased_join(self):
        """test that the 'dont alias non-ORM' rule remains for other
        kinds of aliasing when _from_selectable() is used."""

        User = self.classes.User
        Address = self.classes.Address
        addresses = self.tables.addresses

        sess = fixture_session()
        q1 = sess.query(User.id).filter(User.id > 5)
        with self._from_self_deprecated():
            q1 = q1.from_self()

        aa = aliased(Address)
        q1 = q1.join(aa, User.addresses).order_by(
            User.id, aa.id, addresses.c.id
        )
        self.assert_compile(
            q1,
            "SELECT anon_1.users_id AS anon_1_users_id "
            "FROM (SELECT users.id AS users_id FROM users "
            "WHERE users.id > :id_1) AS anon_1 JOIN addresses AS addresses_1 "
            "ON anon_1.users_id = addresses_1.user_id "
            "ORDER BY anon_1.users_id, addresses_1.id, addresses.id",
        )

    def test_table_anonymous_expression_from_self_twice_oldstyle(self):
        # relies upon _orm_only_from_obj_alias setting
        from sqlalchemy.sql import column

        sess = fixture_session()
        t1 = table("t1", column("c1"), column("c2"))
        q1 = sess.query(t1.c.c1, t1.c.c2).filter(t1.c.c1 == "dog")
        with self._from_self_deprecated():
            q1 = q1.from_self().from_self()
        self.assert_compile(
            q1.order_by(t1.c.c1),
            "SELECT anon_1.anon_2_t1_c1 "
            "AS anon_1_anon_2_t1_c1, anon_1.anon_2_t1_c2 "
            "AS anon_1_anon_2_t1_c2 "
            "FROM (SELECT anon_2.t1_c1 AS anon_2_t1_c1, "
            "anon_2.t1_c2 AS anon_2_t1_c2 FROM (SELECT t1.c1 AS t1_c1, t1.c2 "
            "AS t1_c2 FROM t1 WHERE t1.c1 = :c1_1) AS anon_2) AS anon_1 "
            "ORDER BY anon_1.anon_2_t1_c1",
        )

    def test_self_referential(self):
        Order = self.classes.Order

        sess = fixture_session()
        oalias = aliased(Order)

        with self._from_self_deprecated():
            for q in [
                sess.query(Order, oalias)
                .filter(Order.user_id == oalias.user_id)
                .filter(Order.user_id == 7)
                .filter(Order.id > oalias.id)
                .order_by(Order.id, oalias.id),
                sess.query(Order, oalias)
                .filter(Order.id > oalias.id)
                .from_self()
                .filter(Order.user_id == oalias.user_id)
                .filter(Order.user_id == 7)
                .order_by(Order.id, oalias.id),
                # same thing, but reversed.
                sess.query(oalias, Order)
                .filter(Order.id < oalias.id)
                .from_self()
                .filter(oalias.user_id == Order.user_id)
                .filter(oalias.user_id == 7)
                .order_by(oalias.id, Order.id),
                # here we go....two layers of aliasing
                sess.query(Order, oalias)
                .filter(Order.user_id == oalias.user_id)
                .filter(Order.user_id == 7)
                .filter(Order.id > oalias.id)
                .from_self()
                .order_by(Order.id, oalias.id)
                .limit(10)
                .options(joinedload(Order.items)),
                # gratuitous four layers
                sess.query(Order, oalias)
                .filter(Order.user_id == oalias.user_id)
                .filter(Order.user_id == 7)
                .filter(Order.id > oalias.id)
                .from_self()
                .from_self()
                .from_self()
                .order_by(Order.id, oalias.id)
                .limit(10)
                .options(joinedload(Order.items)),
            ]:

                eq_(
                    q.all(),
                    [
                        (
                            Order(
                                address_id=1,
                                description="order 3",
                                isopen=1,
                                user_id=7,
                                id=3,
                            ),
                            Order(
                                address_id=1,
                                description="order 1",
                                isopen=0,
                                user_id=7,
                                id=1,
                            ),
                        ),
                        (
                            Order(
                                address_id=None,
                                description="order 5",
                                isopen=0,
                                user_id=7,
                                id=5,
                            ),
                            Order(
                                address_id=1,
                                description="order 1",
                                isopen=0,
                                user_id=7,
                                id=1,
                            ),
                        ),
                        (
                            Order(
                                address_id=None,
                                description="order 5",
                                isopen=0,
                                user_id=7,
                                id=5,
                            ),
                            Order(
                                address_id=1,
                                description="order 3",
                                isopen=1,
                                user_id=7,
                                id=3,
                            ),
                        ),
                    ],
                )

    def test_from_self_internal_literals_oldstyle(self):
        # relies upon _orm_only_from_obj_alias setting
        Order = self.classes.Order

        sess = fixture_session()

        # ensure column expressions are taken from inside the subquery, not
        # restated at the top
        with self._from_self_deprecated():
            q = (
                sess.query(
                    Order.id,
                    Order.description,
                    literal_column("'q'").label("foo"),
                )
                .filter(Order.description == "order 3")
                .from_self()
            )
        self.assert_compile(
            q,
            "SELECT anon_1.orders_id AS "
            "anon_1_orders_id, "
            "anon_1.orders_description AS anon_1_orders_description, "
            "anon_1.foo AS anon_1_foo FROM (SELECT "
            "orders.id AS orders_id, "
            "orders.description AS orders_description, "
            "'q' AS foo FROM orders WHERE "
            "orders.description = :description_1) AS "
            "anon_1",
        )
        eq_(q.all(), [(3, "order 3", "q")])

    def test_column_access_from_self(self):
        User = self.classes.User
        sess = fixture_session()

        with self._from_self_deprecated():
            q = sess.query(User).from_self()
        self.assert_compile(
            q.filter(User.name == "ed"),
            "SELECT anon_1.users_id AS anon_1_users_id, anon_1.users_name AS "
            "anon_1_users_name FROM (SELECT users.id AS users_id, users.name "
            "AS users_name FROM users) AS anon_1 WHERE anon_1.users_name = "
            ":name_1",
        )

    def test_column_access_from_self_twice(self):
        User = self.classes.User
        sess = fixture_session()

        with self._from_self_deprecated():
            q = sess.query(User).from_self(User.id, User.name).from_self()
        self.assert_compile(
            q.filter(User.name == "ed"),
            "SELECT anon_1.anon_2_users_id AS anon_1_anon_2_users_id, "
            "anon_1.anon_2_users_name AS anon_1_anon_2_users_name FROM "
            "(SELECT anon_2.users_id AS anon_2_users_id, anon_2.users_name "
            "AS anon_2_users_name FROM (SELECT users.id AS users_id, "
            "users.name AS users_name FROM users) AS anon_2) AS anon_1 "
            "WHERE anon_1.anon_2_users_name = :name_1",
        )

    def test_column_queries_nine(self):
        Address, User = (
            self.classes.Address,
            self.classes.User,
        )

        sess = fixture_session()

        adalias = aliased(Address)
        # select from aliasing + explicit aliasing
        with self._from_self_deprecated():
            eq_(
                sess.query(User, adalias.email_address, adalias.id)
                .outerjoin(adalias, User.addresses)
                .from_self(User, adalias.email_address)
                .order_by(User.id, adalias.id)
                .all(),
                [
                    (User(name="jack", id=7), "jack@bean.com"),
                    (User(name="ed", id=8), "ed@wood.com"),
                    (User(name="ed", id=8), "ed@bettyboop.com"),
                    (User(name="ed", id=8), "ed@lala.com"),
                    (User(name="fred", id=9), "fred@fred.com"),
                    (User(name="chuck", id=10), None),
                ],
            )

    def test_column_queries_ten(self):
        Address, User = (
            self.classes.Address,
            self.classes.User,
        )

        sess = fixture_session()

        # anon + select from aliasing
        aa = aliased(Address)
        with self._from_self_deprecated():
            eq_(
                sess.query(User)
                .join(aa, User.addresses)
                .filter(aa.email_address.like("%ed%"))
                .from_self()
                .all(),
                [User(name="ed", id=8), User(name="fred", id=9)],
            )

    def test_column_queries_eleven(self):
        Address, User = (
            self.classes.Address,
            self.classes.User,
        )

        sess = fixture_session()

        adalias = aliased(Address)
        # test eager aliasing, with/without select_entity_from aliasing
        with self._from_self_deprecated():
            for q in [
                sess.query(User, adalias.email_address)
                .outerjoin(adalias, User.addresses)
                .options(joinedload(User.addresses))
                .order_by(User.id, adalias.id)
                .limit(10),
                sess.query(User, adalias.email_address, adalias.id)
                .outerjoin(adalias, User.addresses)
                .from_self(User, adalias.email_address)
                .options(joinedload(User.addresses))
                .order_by(User.id, adalias.id)
                .limit(10),
            ]:
                eq_(
                    q.all(),
                    [
                        (
                            User(
                                addresses=[
                                    Address(
                                        user_id=7,
                                        email_address="jack@bean.com",
                                        id=1,
                                    )
                                ],
                                name="jack",
                                id=7,
                            ),
                            "jack@bean.com",
                        ),
                        (
                            User(
                                addresses=[
                                    Address(
                                        user_id=8,
                                        email_address="ed@wood.com",
                                        id=2,
                                    ),
                                    Address(
                                        user_id=8,
                                        email_address="ed@bettyboop.com",
                                        id=3,
                                    ),
                                    Address(
                                        user_id=8,
                                        email_address="ed@lala.com",
                                        id=4,
                                    ),
                                ],
                                name="ed",
                                id=8,
                            ),
                            "ed@wood.com",
                        ),
                        (
                            User(
                                addresses=[
                                    Address(
                                        user_id=8,
                                        email_address="ed@wood.com",
                                        id=2,
                                    ),
                                    Address(
                                        user_id=8,
                                        email_address="ed@bettyboop.com",
                                        id=3,
                                    ),
                                    Address(
                                        user_id=8,
                                        email_address="ed@lala.com",
                                        id=4,
                                    ),
                                ],
                                name="ed",
                                id=8,
                            ),
                            "ed@bettyboop.com",
                        ),
                        (
                            User(
                                addresses=[
                                    Address(
                                        user_id=8,
                                        email_address="ed@wood.com",
                                        id=2,
                                    ),
                                    Address(
                                        user_id=8,
                                        email_address="ed@bettyboop.com",
                                        id=3,
                                    ),
                                    Address(
                                        user_id=8,
                                        email_address="ed@lala.com",
                                        id=4,
                                    ),
                                ],
                                name="ed",
                                id=8,
                            ),
                            "ed@lala.com",
                        ),
                        (
                            User(
                                addresses=[
                                    Address(
                                        user_id=9,
                                        email_address="fred@fred.com",
                                        id=5,
                                    )
                                ],
                                name="fred",
                                id=9,
                            ),
                            "fred@fred.com",
                        ),
                        (User(addresses=[], name="chuck", id=10), None),
                    ],
                )

    def test_filter(self):
        User = self.classes.User

        with self._from_self_deprecated():
            eq_(
                [User(id=8), User(id=9)],
                fixture_session()
                .query(User)
                .filter(User.id.in_([8, 9]))
                .from_self()
                .all(),
            )

        with self._from_self_deprecated():
            eq_(
                [User(id=8), User(id=9)],
                fixture_session()
                .query(User)
                .order_by(User.id)
                .slice(1, 3)
                .from_self()
                .all(),
            )

        with self._from_self_deprecated():
            eq_(
                [User(id=8)],
                list(
                    fixture_session()
                    .query(User)
                    .filter(User.id.in_([8, 9]))
                    .from_self()
                    .order_by(User.id)[0:1]
                ),
            )

    def test_join(self):
        User, Address = self.classes.User, self.classes.Address

        with self._from_self_deprecated():
            eq_(
                [
                    (User(id=8), Address(id=2)),
                    (User(id=8), Address(id=3)),
                    (User(id=8), Address(id=4)),
                    (User(id=9), Address(id=5)),
                ],
                fixture_session()
                .query(User)
                .filter(User.id.in_([8, 9]))
                .from_self()
                .join("addresses")
                .add_entity(Address)
                .order_by(User.id, Address.id)
                .all(),
            )

    def test_group_by(self):
        Address = self.classes.Address

        eq_(
            fixture_session()
            .query(Address.user_id, func.count(Address.id).label("count"))
            .group_by(Address.user_id)
            .order_by(Address.user_id)
            .all(),
            [(7, 1), (8, 3), (9, 1)],
        )

        with self._from_self_deprecated():
            eq_(
                fixture_session()
                .query(Address.user_id, Address.id)
                .from_self(Address.user_id, func.count(Address.id))
                .group_by(Address.user_id)
                .order_by(Address.user_id)
                .all(),
                [(7, 1), (8, 3), (9, 1)],
            )

    def test_having(self):
        User = self.classes.User

        s = fixture_session()

        with self._from_self_deprecated():
            self.assert_compile(
                s.query(User.id)
                .group_by(User.id)
                .having(User.id > 5)
                .from_self(),
                "SELECT anon_1.users_id AS anon_1_users_id FROM "
                "(SELECT users.id AS users_id FROM users GROUP "
                "BY users.id HAVING users.id > :id_1) AS anon_1",
            )

    def test_no_joinedload(self):
        """test that joinedloads are pushed outwards and not rendered in
        subqueries."""

        User = self.classes.User

        s = fixture_session()

        with self._from_self_deprecated():
            q = s.query(User).options(joinedload(User.addresses)).from_self()

        self.assert_compile(
            q.statement,
            "SELECT anon_1.users_id, anon_1.users_name, addresses_1.id, "
            "addresses_1.user_id, addresses_1.email_address FROM "
            "(SELECT users.id AS users_id, users.name AS "
            "users_name FROM users) AS anon_1 LEFT OUTER JOIN "
            "addresses AS addresses_1 ON anon_1.users_id = "
            "addresses_1.user_id ORDER BY addresses_1.id",
        )

    def test_aliases(self):
        """test that aliased objects are accessible externally to a from_self()
        call."""

        User, Address = self.classes.User, self.classes.Address

        s = fixture_session()

        ualias = aliased(User)

        with self._from_self_deprecated():
            eq_(
                s.query(User, ualias)
                .filter(User.id > ualias.id)
                .from_self(User.name, ualias.name)
                .order_by(User.name, ualias.name)
                .all(),
                [
                    ("chuck", "ed"),
                    ("chuck", "fred"),
                    ("chuck", "jack"),
                    ("ed", "jack"),
                    ("fred", "ed"),
                    ("fred", "jack"),
                ],
            )

        with self._from_self_deprecated():
            eq_(
                s.query(User, ualias)
                .filter(User.id > ualias.id)
                .from_self(User.name, ualias.name)
                .filter(ualias.name == "ed")
                .order_by(User.name, ualias.name)
                .all(),
                [("chuck", "ed"), ("fred", "ed")],
            )

        with self._from_self_deprecated():
            eq_(
                s.query(User, ualias)
                .filter(User.id > ualias.id)
                .from_self(ualias.name, Address.email_address)
                .join(ualias.addresses)
                .order_by(ualias.name, Address.email_address)
                .all(),
                [
                    ("ed", "fred@fred.com"),
                    ("jack", "ed@bettyboop.com"),
                    ("jack", "ed@lala.com"),
                    ("jack", "ed@wood.com"),
                    ("jack", "fred@fred.com"),
                ],
            )

    def test_multiple_entities(self):
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()

        with self._from_self_deprecated():
            eq_(
                sess.query(User, Address)
                .filter(User.id == Address.user_id)
                .filter(Address.id.in_([2, 5]))
                .from_self()
                .all(),
                [(User(id=8), Address(id=2)), (User(id=9), Address(id=5))],
            )

        with self._from_self_deprecated():
            eq_(
                sess.query(User, Address)
                .filter(User.id == Address.user_id)
                .filter(Address.id.in_([2, 5]))
                .from_self()
                .options(joinedload("addresses"))
                .first(),
                (
                    User(id=8, addresses=[Address(), Address(), Address()]),
                    Address(id=2),
                ),
            )

    def test_multiple_with_column_entities_oldstyle(self):
        # relies upon _orm_only_from_obj_alias setting
        User = self.classes.User

        sess = fixture_session()

        with self._from_self_deprecated():
            eq_(
                sess.query(User.id)
                .from_self()
                .add_columns(func.count().label("foo"))
                .group_by(User.id)
                .order_by(User.id)
                .from_self()
                .all(),
                [(7, 1), (8, 1), (9, 1), (10, 1)],
            )


class SubqRelationsFromSelfTest(fixtures.DeclarativeMappedTest):
    def _from_self_deprecated(self):
        return testing.expect_deprecated_20(r"The Query.from_self\(\) method")

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class A(Base, ComparableEntity):
            __tablename__ = "a"

            id = Column(Integer, primary_key=True)
            cs = relationship("C", order_by="C.id")

        class B(Base, ComparableEntity):
            __tablename__ = "b"
            id = Column(Integer, primary_key=True)
            a_id = Column(ForeignKey("a.id"))
            a = relationship("A")
            ds = relationship("D", order_by="D.id")

        class C(Base, ComparableEntity):
            __tablename__ = "c"
            id = Column(Integer, primary_key=True)
            a_id = Column(ForeignKey("a.id"))

        class D(Base, ComparableEntity):
            __tablename__ = "d"
            id = Column(Integer, primary_key=True)
            b_id = Column(ForeignKey("b.id"))

    @classmethod
    def insert_data(cls, connection):
        A, B, C, D = cls.classes("A", "B", "C", "D")

        s = Session(connection)

        as_ = [
            A(
                id=i,
                cs=[C(), C()],
            )
            for i in range(1, 5)
        ]

        s.add_all(
            [
                B(a=as_[0], ds=[D()]),
                B(a=as_[1], ds=[D()]),
                B(a=as_[2]),
                B(a=as_[3]),
            ]
        )

        s.commit()

    def test_subq_w_from_self_one(self):
        A, B, C = self.classes("A", "B", "C")

        s = fixture_session()

        cache = {}

        for i in range(3):
            with self._from_self_deprecated():
                q = (
                    s.query(B)
                    .execution_options(compiled_cache=cache)
                    .join(B.a)
                    .filter(B.id < 4)
                    .filter(A.id > 1)
                    .from_self()
                    .options(subqueryload(B.a).subqueryload(A.cs))
                    .from_self()
                )

            def go():
                results = q.all()
                eq_(
                    results,
                    [
                        B(
                            a=A(cs=[C(a_id=2, id=3), C(a_id=2, id=4)], id=2),
                            a_id=2,
                            id=2,
                        ),
                        B(
                            a=A(cs=[C(a_id=3, id=5), C(a_id=3, id=6)], id=3),
                            a_id=3,
                            id=3,
                        ),
                    ],
                )

            self.assert_sql_execution(
                testing.db,
                go,
                CompiledSQL(
                    "SELECT anon_1.anon_2_b_id AS anon_1_anon_2_b_id, "
                    "anon_1.anon_2_b_a_id AS anon_1_anon_2_b_a_id FROM "
                    "(SELECT anon_2.b_id AS anon_2_b_id, anon_2.b_a_id "
                    "AS anon_2_b_a_id FROM (SELECT b.id AS b_id, b.a_id "
                    "AS b_a_id FROM b JOIN a ON a.id = b.a_id "
                    "WHERE b.id < :id_1 AND a.id > :id_2) AS anon_2) AS anon_1"
                ),
                CompiledSQL(
                    "SELECT a.id AS a_id, anon_1.anon_2_anon_3_b_a_id AS "
                    "anon_1_anon_2_anon_3_b_a_id FROM (SELECT DISTINCT "
                    "anon_2.anon_3_b_a_id AS anon_2_anon_3_b_a_id FROM "
                    "(SELECT anon_3.b_id AS anon_3_b_id, anon_3.b_a_id "
                    "AS anon_3_b_a_id FROM (SELECT b.id AS b_id, b.a_id "
                    "AS b_a_id FROM b JOIN a ON a.id = b.a_id "
                    "WHERE b.id < :id_1 AND a.id > :id_2) AS anon_3) "
                    "AS anon_2) AS anon_1 JOIN a "
                    "ON a.id = anon_1.anon_2_anon_3_b_a_id"
                ),
                CompiledSQL(
                    "SELECT c.id AS c_id, c.a_id AS c_a_id, a_1.id "
                    "AS a_1_id FROM (SELECT DISTINCT anon_2.anon_3_b_a_id AS "
                    "anon_2_anon_3_b_a_id FROM "
                    "(SELECT anon_3.b_id AS anon_3_b_id, anon_3.b_a_id "
                    "AS anon_3_b_a_id FROM (SELECT b.id AS b_id, b.a_id "
                    "AS b_a_id FROM b JOIN a ON a.id = b.a_id "
                    "WHERE b.id < :id_1 AND a.id > :id_2) AS anon_3) "
                    "AS anon_2) AS anon_1 JOIN a AS a_1 ON a_1.id = "
                    "anon_1.anon_2_anon_3_b_a_id JOIN c ON a_1.id = c.a_id "
                    "ORDER BY c.id"
                ),
            )

            s.close()

    def test_subq_w_from_self_two(self):

        A, B, C = self.classes("A", "B", "C")

        s = fixture_session()
        cache = {}

        for i in range(3):

            def go():
                with self._from_self_deprecated():
                    q = (
                        s.query(B)
                        .execution_options(compiled_cache=cache)
                        .join(B.a)
                        .from_self()
                    )
                q = q.options(subqueryload(B.ds))

                q.all()

            self.assert_sql_execution(
                testing.db,
                go,
                CompiledSQL(
                    "SELECT anon_1.b_id AS anon_1_b_id, anon_1.b_a_id AS "
                    "anon_1_b_a_id FROM (SELECT b.id AS b_id, b.a_id "
                    "AS b_a_id FROM b JOIN a ON a.id = b.a_id) AS anon_1"
                ),
                CompiledSQL(
                    "SELECT d.id AS d_id, d.b_id AS d_b_id, "
                    "anon_1.anon_2_b_id AS anon_1_anon_2_b_id "
                    "FROM (SELECT anon_2.b_id AS anon_2_b_id FROM "
                    "(SELECT b.id AS b_id, b.a_id AS b_a_id FROM b "
                    "JOIN a ON a.id = b.a_id) AS anon_2) AS anon_1 "
                    "JOIN d ON anon_1.anon_2_b_id = d.b_id ORDER BY d.id"
                ),
            )
            s.close()


class SessionTest(fixtures.RemovesEvents, _LocalFixture):
    def test_transaction_attr(self):
        s1 = Session(testing.db)

        with testing.expect_deprecated_20(
            "The Session.transaction attribute is considered legacy as "
            "of the 1.x series"
        ):
            s1.transaction

    def test_textual_execute(self, connection):
        """test that Session.execute() converts to text()"""

        users = self.tables.users

        with Session(bind=connection) as sess:
            sess.execute(users.insert(), dict(id=7, name="jack"))

            with testing.expect_deprecated_20(
                "Using plain strings to indicate SQL statements "
                "without using the text"
            ):
                # use :bindparam style
                eq_(
                    sess.execute(
                        "select * from users where id=:id", {"id": 7}
                    ).fetchall(),
                    [(7, "jack")],
                )

            with testing.expect_deprecated_20(
                "Using plain strings to indicate SQL statements "
                "without using the text"
            ):
                # use :bindparam style
                eq_(
                    sess.scalar(
                        "select id from users where id=:id", {"id": 7}
                    ),
                    7,
                )

    def test_session_str(self):
        s1 = Session(testing.db)
        str(s1)

    def test_subtransactions_deprecated(self):
        s1 = Session(testing.db)
        s1.begin()

        with testing.expect_deprecated_20(
            "The Session.begin.subtransactions flag is deprecated "
            "and will be removed in SQLAlchemy version 2.0."
        ):
            s1.begin(subtransactions=True)

        s1.close()

    def test_autocommit_deprecated(Self):
        with testing.expect_deprecated_20(
            "The Session.autocommit parameter is deprecated "
            "and will be removed in SQLAlchemy version 2.0."
        ):
            Session(autocommit=True)

    @testing.combinations(
        {"mapper": None},
        {"clause": None},
        {"bind_arguments": {"mapper": None}, "clause": None},
        {"bind_arguments": {}, "clause": None},
    )
    def test_bind_kwarg_deprecated(self, kw):
        s1 = Session(testing.db)

        for meth in s1.execute, s1.scalar:
            m1 = mock.Mock(side_effect=s1.get_bind)
            with mock.patch.object(s1, "get_bind", m1):
                expr = text("select 1")

                with testing.expect_deprecated_20(
                    r"Passing bind arguments to Session.execute\(\) as "
                    "keyword "
                    "arguments is deprecated and will be removed SQLAlchemy "
                    "2.0"
                ):
                    meth(expr, **kw)

                bind_arguments = kw.pop("bind_arguments", None)
                if bind_arguments:
                    bind_arguments.update(kw)

                    if "clause" not in kw:
                        bind_arguments["clause"] = expr
                    eq_(m1.mock_calls, [call(**bind_arguments)])
                else:
                    if "clause" not in kw:
                        kw["clause"] = expr
                    eq_(m1.mock_calls, [call(**kw)])

    @testing.requires.independent_connections
    @testing.emits_warning(".*previous exception")
    def test_failed_rollback_deactivates_transaction_ctx_integration(self):
        # test #4050 in the same context as that of oslo.db

        User = self.classes.User

        with testing.expect_deprecated_20(
            "The Session.autocommit parameter is deprecated"
        ):
            session = Session(bind=testing.db, autocommit=True)

        evented_exceptions = []
        caught_exceptions = []

        def canary(context):
            evented_exceptions.append(context.original_exception)

        rollback_error = testing.db.dialect.dbapi.InterfaceError(
            "Can't roll back to savepoint"
        )

        def prevent_savepoint_rollback(
            cursor, statement, parameters, context=None
        ):
            if (
                context is not None
                and context.compiled
                and isinstance(
                    context.compiled.statement,
                    elements.RollbackToSavepointClause,
                )
            ):
                raise rollback_error

        self.event_listen(testing.db, "handle_error", canary, retval=True)
        self.event_listen(
            testing.db.dialect, "do_execute", prevent_savepoint_rollback
        )

        with session.begin():
            session.add(User(id=1, name="x"))

        try:
            with session.begin():
                try:
                    with session.begin_nested():
                        # raises IntegrityError on flush
                        session.add(User(id=1, name="x"))

                # outermost is the failed SAVEPOINT rollback
                # from the "with session.begin_nested()"
                except sa_exc.DBAPIError as dbe_inner:
                    caught_exceptions.append(dbe_inner.orig)
                    raise
        except sa_exc.DBAPIError as dbe_outer:
            caught_exceptions.append(dbe_outer.orig)

        is_true(
            isinstance(
                evented_exceptions[0], testing.db.dialect.dbapi.IntegrityError
            )
        )
        eq_(evented_exceptions[1], rollback_error)
        eq_(len(evented_exceptions), 2)
        eq_(caught_exceptions, [rollback_error, rollback_error])

    def test_contextmanager_commit(self):
        User = self.classes.User

        with testing.expect_deprecated_20(
            "The Session.autocommit parameter is deprecated"
        ):
            sess = Session(testing.db, autocommit=True)
        with sess.begin():
            sess.add(User(name="u1"))

        sess.rollback()
        eq_(sess.query(User).count(), 1)

    def test_contextmanager_rollback(self):
        User = self.classes.User

        with testing.expect_deprecated_20(
            "The Session.autocommit parameter is deprecated"
        ):
            sess = Session(testing.db, autocommit=True)

        def go():
            with sess.begin():
                sess.add(User())  # name can't be null

        assert_raises(sa_exc.DBAPIError, go)

        eq_(sess.query(User).count(), 0)

        with sess.begin():
            sess.add(User(name="u1"))
        eq_(sess.query(User).count(), 1)


class AutocommitClosesOnFailTest(fixtures.MappedTest):
    __requires__ = ("deferrable_fks",)

    @classmethod
    def define_tables(cls, metadata):
        Table("t1", metadata, Column("id", Integer, primary_key=True))

        Table(
            "t2",
            metadata,
            Column("id", Integer, primary_key=True),
            Column(
                "t1_id",
                Integer,
                ForeignKey("t1.id", deferrable=True, initially="deferred"),
            ),
        )

    @classmethod
    def setup_classes(cls):
        class T1(cls.Comparable):
            pass

        class T2(cls.Comparable):
            pass

    @classmethod
    def setup_mappers(cls):
        T2, T1, t2, t1 = (
            cls.classes.T2,
            cls.classes.T1,
            cls.tables.t2,
            cls.tables.t1,
        )

        mapper(T1, t1)
        mapper(T2, t2)

    def test_close_transaction_on_commit_fail(self):
        T2 = self.classes.T2

        session = Session(testing.db, autocommit=True)

        # with a deferred constraint, this fails at COMMIT time instead
        # of at INSERT time.
        session.add(T2(id=1, t1_id=123))

        assert_raises(
            (sa.exc.IntegrityError, sa.exc.DatabaseError), session.flush
        )

        assert session._legacy_transaction() is None


class DeprecatedInhTest(_poly_fixtures._Polymorphic):
    def test_with_polymorphic(self):
        Person = _poly_fixtures.Person
        Engineer = _poly_fixtures.Engineer

        with DeprecatedQueryTest._expect_implicit_subquery():
            p_poly = with_polymorphic(Person, [Engineer], select(Person))

        is_true(
            sa.inspect(p_poly).selectable.compare(select(Person).subquery())
        )

    def test_multiple_adaption(self):
        """test that multiple filter() adapters get chained together "
        and work correctly within a multiple-entry join()."""

        Company = _poly_fixtures.Company
        Machine = _poly_fixtures.Machine
        Engineer = _poly_fixtures.Engineer

        people = self.tables.people
        engineers = self.tables.engineers
        machines = self.tables.machines

        sess = fixture_session()

        mach_alias = machines.select()
        with DeprecatedQueryTest._expect_implicit_subquery():
            self.assert_compile(
                sess.query(Company)
                .join(people.join(engineers), Company.employees)
                .join(mach_alias, Engineer.machines, from_joinpoint=True)
                .filter(Engineer.name == "dilbert")
                .filter(Machine.name == "foo"),
                "SELECT companies.company_id AS companies_company_id, "
                "companies.name AS companies_name "
                "FROM companies JOIN (people "
                "JOIN engineers ON people.person_id = "
                "engineers.person_id) ON companies.company_id = "
                "people.company_id JOIN "
                "(SELECT machines.machine_id AS machine_id, "
                "machines.name AS name, "
                "machines.engineer_id AS engineer_id "
                "FROM machines) AS anon_1 "
                "ON engineers.person_id = anon_1.engineer_id "
                "WHERE people.name = :name_1 AND anon_1.name = :name_2",
                use_default_dialect=True,
            )


class DeprecatedMapperTest(_fixtures.FixtureTest, AssertsCompiledSQL):
    __dialect__ = "default"

    def test_deferred_scalar_loader_name_change(self):
        class Foo(object):
            pass

        def myloader(*arg, **kw):
            pass

        instrumentation.register_class(Foo)
        manager = instrumentation.manager_of_class(Foo)

        with testing.expect_deprecated(
            "The ClassManager.deferred_scalar_loader attribute is now named "
            "expired_attribute_loader"
        ):
            manager.deferred_scalar_loader = myloader

        is_(manager.expired_attribute_loader, myloader)

        with testing.expect_deprecated(
            "The ClassManager.deferred_scalar_loader attribute is now named "
            "expired_attribute_loader"
        ):
            is_(manager.deferred_scalar_loader, myloader)

    def test_polymorphic_union_w_select(self):
        users, addresses = self.tables.users, self.tables.addresses

        with DeprecatedQueryTest._expect_implicit_subquery():
            dep = polymorphic_union(
                {"u": users.select(), "a": addresses.select()},
                "type",
                "bcjoin",
            )

        subq_version = polymorphic_union(
            {
                "u": users.select().subquery(),
                "a": addresses.select().subquery(),
            },
            "type",
            "bcjoin",
        )
        is_true(dep.compare(subq_version))

    def test_comparable_column(self):
        users, User = self.tables.users, self.classes.User

        class MyComparator(sa.orm.properties.ColumnProperty.Comparator):
            __hash__ = None

            def __eq__(self, other):
                # lower case comparison
                return func.lower(self.__clause_element__()) == func.lower(
                    other
                )

            def intersects(self, other):
                # non-standard comparator
                return self.__clause_element__().op("&=")(other)

        mapper(
            User,
            users,
            properties={
                "name": sa.orm.column_property(
                    users.c.name, comparator_factory=MyComparator
                )
            },
        )

        assert_raises_message(
            AttributeError,
            "Neither 'InstrumentedAttribute' object nor "
            "'MyComparator' object associated with User.name has "
            "an attribute 'nonexistent'",
            getattr,
            User.name,
            "nonexistent",
        )

        eq_(
            str(
                (User.name == "ed").compile(
                    dialect=sa.engine.default.DefaultDialect()
                )
            ),
            "lower(users.name) = lower(:lower_1)",
        )
        eq_(
            str(
                (User.name.intersects("ed")).compile(
                    dialect=sa.engine.default.DefaultDialect()
                )
            ),
            "users.name &= :name_1",
        )

    def test_add_property(self):
        users = self.tables.users

        assert_col = []

        class User(fixtures.ComparableEntity):
            def _get_name(self):
                assert_col.append(("get", self._name))
                return self._name

            def _set_name(self, name):
                assert_col.append(("set", name))
                self._name = name

            name = property(_get_name, _set_name)

        m = mapper(User, users)

        m.add_property("_name", deferred(users.c.name))
        m.add_property("name", synonym("_name"))

        sess = fixture_session(autocommit=False)
        assert sess.query(User).get(7)

        u = sess.query(User).filter_by(name="jack").one()

        def go():
            eq_(u.name, "jack")
            eq_(assert_col, [("get", "jack")], str(assert_col))

        self.sql_count_(1, go)


class DeprecatedOptionAllTest(OptionsPathTest, _fixtures.FixtureTest):
    run_inserts = "once"
    run_deletes = None

    def _mapper_fixture_one(self):
        users, User, addresses, Address, orders, Order = (
            self.tables.users,
            self.classes.User,
            self.tables.addresses,
            self.classes.Address,
            self.tables.orders,
            self.classes.Order,
        )
        keywords, items, item_keywords, Keyword, Item = (
            self.tables.keywords,
            self.tables.items,
            self.tables.item_keywords,
            self.classes.Keyword,
            self.classes.Item,
        )
        mapper(
            User,
            users,
            properties={
                "addresses": relationship(Address),
                "orders": relationship(Order),
            },
        )
        mapper(Address, addresses)
        mapper(
            Order,
            orders,
            properties={
                "items": relationship(Item, secondary=self.tables.order_items)
            },
        )
        mapper(
            Keyword,
            keywords,
            properties={
                "keywords": column_property(keywords.c.name + "some keyword")
            },
        )
        mapper(
            Item,
            items,
            properties=dict(
                keywords=relationship(Keyword, secondary=item_keywords)
            ),
        )

    def _assert_eager_with_entity_exception(
        self, entity_list, options, message
    ):
        assert_raises_message(
            sa.exc.ArgumentError,
            message,
            fixture_session()
            .query(*entity_list)
            .options(*options)
            ._compile_context,
        )

    def test_defer_addtl_attrs(self):
        users, User, Address, addresses = (
            self.tables.users,
            self.classes.User,
            self.classes.Address,
            self.tables.addresses,
        )

        mapper(Address, addresses)
        mapper(
            User,
            users,
            properties={
                "addresses": relationship(
                    Address, lazy="selectin", order_by=addresses.c.id
                )
            },
        )

        sess = fixture_session()

        with testing.expect_deprecated(
            r"The \*addl_attrs on orm.defer is deprecated.  "
            "Please use method chaining"
        ):
            sess.query(User).options(defer("addresses", "email_address"))

        with testing.expect_deprecated(
            r"The \*addl_attrs on orm.undefer is deprecated.  "
            "Please use method chaining"
        ):
            sess.query(User).options(undefer("addresses", "email_address"))


class InstrumentationTest(fixtures.ORMTest):
    def test_dict_subclass4(self):
        # tests #2654
        with testing.expect_deprecated(
            r"The collection.converter\(\) handler is deprecated and will "
            "be removed in a future release.  Please refer to the "
            "AttributeEvents"
        ):

            class MyDict(collections.MappedCollection):
                def __init__(self):
                    super(MyDict, self).__init__(lambda value: "k%d" % value)

                @collection.converter
                def _convert(self, dictlike):
                    for key, value in dictlike.items():
                        yield value + 5

        class Foo(object):
            pass

        instrumentation.register_class(Foo)
        attributes.register_attribute(
            Foo, "attr", uselist=True, typecallable=MyDict, useobject=True
        )

        f = Foo()
        f.attr = {"k1": 1, "k2": 2}

        eq_(f.attr, {"k7": 7, "k6": 6})

    def test_name_setup(self):
        with testing.expect_deprecated(
            r"The collection.converter\(\) handler is deprecated and will "
            "be removed in a future release.  Please refer to the "
            "AttributeEvents"
        ):

            class Base(object):
                @collection.iterator
                def base_iterate(self, x):
                    return "base_iterate"

                @collection.appender
                def base_append(self, x):
                    return "base_append"

                @collection.converter
                def base_convert(self, x):
                    return "base_convert"

                @collection.remover
                def base_remove(self, x):
                    return "base_remove"

        from sqlalchemy.orm.collections import _instrument_class

        _instrument_class(Base)

        eq_(Base._sa_remover(Base(), 5), "base_remove")
        eq_(Base._sa_appender(Base(), 5), "base_append")
        eq_(Base._sa_iterator(Base(), 5), "base_iterate")
        eq_(Base._sa_converter(Base(), 5), "base_convert")

        with testing.expect_deprecated(
            r"The collection.converter\(\) handler is deprecated and will "
            "be removed in a future release.  Please refer to the "
            "AttributeEvents"
        ):

            class Sub(Base):
                @collection.converter
                def base_convert(self, x):
                    return "sub_convert"

                @collection.remover
                def sub_remove(self, x):
                    return "sub_remove"

        _instrument_class(Sub)

        eq_(Sub._sa_appender(Sub(), 5), "base_append")
        eq_(Sub._sa_remover(Sub(), 5), "sub_remove")
        eq_(Sub._sa_iterator(Sub(), 5), "base_iterate")
        eq_(Sub._sa_converter(Sub(), 5), "sub_convert")


class NonPrimaryRelationshipLoaderTest(_fixtures.FixtureTest):
    run_inserts = "once"
    run_deletes = None

    def test_selectload(self):
        """tests lazy loading with two relationships simultaneously,
        from the same table, using aliases."""

        users, orders, User, Address, Order, addresses = (
            self.tables.users,
            self.tables.orders,
            self.classes.User,
            self.classes.Address,
            self.classes.Order,
            self.tables.addresses,
        )

        openorders = sa.alias(orders, "openorders")
        closedorders = sa.alias(orders, "closedorders")

        mapper(Address, addresses)

        mapper(Order, orders)

        with testing.expect_deprecated(
            "The mapper.non_primary parameter is deprecated"
        ):
            open_mapper = mapper(Order, openorders, non_primary=True)
            closed_mapper = mapper(Order, closedorders, non_primary=True)
        mapper(
            User,
            users,
            properties=dict(
                addresses=relationship(Address, lazy=True),
                open_orders=relationship(
                    open_mapper,
                    primaryjoin=sa.and_(
                        openorders.c.isopen == 1,
                        users.c.id == openorders.c.user_id,
                    ),
                    lazy="select",
                ),
                closed_orders=relationship(
                    closed_mapper,
                    primaryjoin=sa.and_(
                        closedorders.c.isopen == 0,
                        users.c.id == closedorders.c.user_id,
                    ),
                    lazy="select",
                ),
            ),
        )

        self._run_double_test(10)

    def test_joinedload(self):
        """Eager loading with two relationships simultaneously,
        from the same table, using aliases."""

        users, orders, User, Address, Order, addresses = (
            self.tables.users,
            self.tables.orders,
            self.classes.User,
            self.classes.Address,
            self.classes.Order,
            self.tables.addresses,
        )

        openorders = sa.alias(orders, "openorders")
        closedorders = sa.alias(orders, "closedorders")

        mapper(Address, addresses)
        mapper(Order, orders)

        with testing.expect_deprecated(
            "The mapper.non_primary parameter is deprecated"
        ):
            open_mapper = mapper(Order, openorders, non_primary=True)
            closed_mapper = mapper(Order, closedorders, non_primary=True)

        mapper(
            User,
            users,
            properties=dict(
                addresses=relationship(
                    Address, lazy="joined", order_by=addresses.c.id
                ),
                open_orders=relationship(
                    open_mapper,
                    primaryjoin=sa.and_(
                        openorders.c.isopen == 1,
                        users.c.id == openorders.c.user_id,
                    ),
                    lazy="joined",
                    order_by=openorders.c.id,
                ),
                closed_orders=relationship(
                    closed_mapper,
                    primaryjoin=sa.and_(
                        closedorders.c.isopen == 0,
                        users.c.id == closedorders.c.user_id,
                    ),
                    lazy="joined",
                    order_by=closedorders.c.id,
                ),
            ),
        )
        self._run_double_test(1)

    def test_selectin(self):

        users, orders, User, Address, Order, addresses = (
            self.tables.users,
            self.tables.orders,
            self.classes.User,
            self.classes.Address,
            self.classes.Order,
            self.tables.addresses,
        )

        openorders = sa.alias(orders, "openorders")
        closedorders = sa.alias(orders, "closedorders")

        mapper(Address, addresses)
        mapper(Order, orders)

        with testing.expect_deprecated(
            "The mapper.non_primary parameter is deprecated"
        ):
            open_mapper = mapper(Order, openorders, non_primary=True)
            closed_mapper = mapper(Order, closedorders, non_primary=True)

        mapper(
            User,
            users,
            properties=dict(
                addresses=relationship(
                    Address, lazy="selectin", order_by=addresses.c.id
                ),
                open_orders=relationship(
                    open_mapper,
                    primaryjoin=sa.and_(
                        openorders.c.isopen == 1,
                        users.c.id == openorders.c.user_id,
                    ),
                    lazy="selectin",
                    order_by=openorders.c.id,
                ),
                closed_orders=relationship(
                    closed_mapper,
                    primaryjoin=sa.and_(
                        closedorders.c.isopen == 0,
                        users.c.id == closedorders.c.user_id,
                    ),
                    lazy="selectin",
                    order_by=closedorders.c.id,
                ),
            ),
        )

        self._run_double_test(4)

    def test_subqueryload(self):

        users, orders, User, Address, Order, addresses = (
            self.tables.users,
            self.tables.orders,
            self.classes.User,
            self.classes.Address,
            self.classes.Order,
            self.tables.addresses,
        )

        openorders = sa.alias(orders, "openorders")
        closedorders = sa.alias(orders, "closedorders")

        mapper(Address, addresses)
        mapper(Order, orders)

        with testing.expect_deprecated(
            "The mapper.non_primary parameter is deprecated"
        ):
            open_mapper = mapper(Order, openorders, non_primary=True)
            closed_mapper = mapper(Order, closedorders, non_primary=True)

        mapper(
            User,
            users,
            properties=dict(
                addresses=relationship(
                    Address, lazy="subquery", order_by=addresses.c.id
                ),
                open_orders=relationship(
                    open_mapper,
                    primaryjoin=sa.and_(
                        openorders.c.isopen == 1,
                        users.c.id == openorders.c.user_id,
                    ),
                    lazy="subquery",
                    order_by=openorders.c.id,
                ),
                closed_orders=relationship(
                    closed_mapper,
                    primaryjoin=sa.and_(
                        closedorders.c.isopen == 0,
                        users.c.id == closedorders.c.user_id,
                    ),
                    lazy="subquery",
                    order_by=closedorders.c.id,
                ),
            ),
        )

        self._run_double_test(4)

    def _run_double_test(self, count):
        User, Address, Order, Item = self.classes(
            "User", "Address", "Order", "Item"
        )
        q = fixture_session().query(User).order_by(User.id)

        def go():
            eq_(
                [
                    User(
                        id=7,
                        addresses=[Address(id=1)],
                        open_orders=[Order(id=3)],
                        closed_orders=[Order(id=1), Order(id=5)],
                    ),
                    User(
                        id=8,
                        addresses=[
                            Address(id=2),
                            Address(id=3),
                            Address(id=4),
                        ],
                        open_orders=[],
                        closed_orders=[],
                    ),
                    User(
                        id=9,
                        addresses=[Address(id=5)],
                        open_orders=[Order(id=4)],
                        closed_orders=[Order(id=2)],
                    ),
                    User(id=10),
                ],
                q.all(),
            )

        self.assert_sql_count(testing.db, go, count)

        sess = fixture_session()
        user = sess.query(User).get(7)

        closed_mapper = User.closed_orders.entity
        open_mapper = User.open_orders.entity
        eq_(
            [Order(id=1), Order(id=5)],
            fixture_session()
            .query(closed_mapper)
            .with_parent(user, property="closed_orders")
            .all(),
        )
        eq_(
            [Order(id=3)],
            fixture_session()
            .query(open_mapper)
            .with_parent(user, property="open_orders")
            .all(),
        )


class ViewonlyFlagWarningTest(fixtures.MappedTest):
    """test for #4993.

    In 1.4, this moves to test/orm/test_cascade, deprecation warnings
    become errors, will then be for #4994.

    """

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "users",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(30)),
        )
        Table(
            "orders",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("user_id", Integer),
            Column("description", String(30)),
        )

    @classmethod
    def setup_classes(cls):
        class User(cls.Comparable):
            pass

        class Order(cls.Comparable):
            pass

    @testing.combinations(
        ("passive_deletes", True),
        ("passive_updates", False),
        ("enable_typechecks", False),
        ("active_history", True),
        ("cascade_backrefs", False),
    )
    def test_viewonly_warning(self, flag, value):
        Order = self.classes.Order

        with testing.expect_warnings(
            r"Setting %s on relationship\(\) while also setting "
            "viewonly=True does not make sense" % flag
        ):
            kw = {
                "viewonly": True,
                "primaryjoin": self.tables.users.c.id
                == foreign(self.tables.orders.c.user_id),
            }
            kw[flag] = value
            rel = relationship(Order, **kw)

            eq_(getattr(rel, flag), value)


class NonPrimaryMapperTest(_fixtures.FixtureTest, AssertsCompiledSQL):
    __dialect__ = "default"

    def test_non_primary_identity_class(self):
        User = self.classes.User
        users, addresses = self.tables.users, self.tables.addresses

        class AddressUser(User):
            pass

        mapper(User, users, polymorphic_identity="user")
        m2 = mapper(
            AddressUser,
            addresses,
            inherits=User,
            polymorphic_identity="address",
            properties={"address_id": addresses.c.id},
        )
        with testing.expect_deprecated(
            "The mapper.non_primary parameter is deprecated"
        ):
            m3 = mapper(AddressUser, addresses, non_primary=True)
        assert m3._identity_class is m2._identity_class
        eq_(
            m2.identity_key_from_instance(AddressUser()),
            m3.identity_key_from_instance(AddressUser()),
        )

    def test_illegal_non_primary(self):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        mapper(User, users)
        mapper(Address, addresses)
        with testing.expect_deprecated(
            "The mapper.non_primary parameter is deprecated"
        ):
            m = mapper(  # noqa F841
                User,
                users,
                non_primary=True,
                properties={"addresses": relationship(Address)},
            )
        assert_raises_message(
            sa.exc.ArgumentError,
            "Attempting to assign a new relationship 'addresses' "
            "to a non-primary mapper on class 'User'",
            configure_mappers,
        )

    def test_illegal_non_primary_2(self):
        User, users = self.classes.User, self.tables.users

        with testing.expect_deprecated(
            "The mapper.non_primary parameter is deprecated"
        ):
            assert_raises_message(
                sa.exc.InvalidRequestError,
                "Configure a primary mapper first",
                mapper,
                User,
                users,
                non_primary=True,
            )

    def test_illegal_non_primary_3(self):
        users, addresses = self.tables.users, self.tables.addresses

        class Base(object):
            pass

        class Sub(Base):
            pass

        mapper(Base, users)
        with testing.expect_deprecated(
            "The mapper.non_primary parameter is deprecated"
        ):
            assert_raises_message(
                sa.exc.InvalidRequestError,
                "Configure a primary mapper first",
                mapper,
                Sub,
                addresses,
                non_primary=True,
            )


class InstancesTest(QueryTest, AssertsCompiledSQL):
    @testing.fails(
        "ORM refactor not allowing this yet, "
        "we may just abandon this use case"
    )
    def test_from_alias_one(self):
        User, addresses, users = (
            self.classes.User,
            self.tables.addresses,
            self.tables.users,
        )

        query = (
            users.select(users.c.id == 7)
            .union(users.select(users.c.id > 7))
            .alias("ulist")
            .outerjoin(addresses)
            .select(order_by=[text("ulist.id"), addresses.c.id])
        )
        sess = fixture_session()
        q = sess.query(User)

        # note this has multiple problems because we aren't giving Query
        # the statement where it would be able to create an adapter
        def go():
            with testing.expect_deprecated(
                r"Using the Query.instances\(\) method without a context",
                "Retrieving row values using Column objects with only "
                "matching names",
            ):
                result = list(
                    q.options(
                        contains_alias("ulist"), contains_eager("addresses")
                    ).instances(query.execute())
                )
            assert self.static.user_address_result == result

        self.assert_sql_count(testing.db, go, 1)

    def test_from_alias_two_old_way(self):
        User, addresses, users = (
            self.classes.User,
            self.tables.addresses,
            self.tables.users,
        )

        query = (
            users.select(users.c.id == 7)
            .union(users.select(users.c.id > 7))
            .alias("ulist")
            .outerjoin(addresses)
            .select(order_by=[text("ulist.id"), addresses.c.id])
        )
        sess = fixture_session()
        q = sess.query(User)

        def go():
            with testing.expect_deprecated(
                "The AliasOption is not necessary for entities to be "
                "matched up to a query"
            ):
                result = (
                    q.options(
                        contains_alias("ulist"), contains_eager("addresses")
                    )
                    .from_statement(query)
                    .all()
                )
            assert self.static.user_address_result == result

        self.assert_sql_count(testing.db, go, 1)

    def test_contains_eager(self):
        users, addresses, User = (
            self.tables.users,
            self.tables.addresses,
            self.classes.User,
        )

        sess = fixture_session()

        selectquery = users.outerjoin(addresses).select(
            users.c.id < 10,
            order_by=[users.c.id, addresses.c.id],
        )
        q = sess.query(User)

        def go():
            with testing.expect_deprecated(
                r"Using the Query.instances\(\) method without a context"
            ):
                result = list(
                    q.options(contains_eager("addresses")).instances(
                        sess.execute(selectquery)
                    )
                )
            assert self.static.user_address_result[0:3] == result

        self.assert_sql_count(testing.db, go, 1)

        sess.expunge_all()

        def go():
            with testing.expect_deprecated(
                r"Using the Query.instances\(\) method without a context"
            ):
                result = list(
                    q.options(contains_eager(User.addresses)).instances(
                        sess.connection().execute(selectquery)
                    )
                )
            assert self.static.user_address_result[0:3] == result

        self.assert_sql_count(testing.db, go, 1)

    def test_contains_eager_string_alias(self):
        addresses, users, User = (
            self.tables.addresses,
            self.tables.users,
            self.classes.User,
        )

        sess = fixture_session()
        q = sess.query(User)

        adalias = addresses.alias("adalias")
        selectquery = users.outerjoin(adalias).select(
            order_by=[users.c.id, adalias.c.id]
        )

        # note this has multiple problems because we aren't giving Query
        # the statement where it would be able to create an adapter
        def go():
            with testing.expect_deprecated(
                r"Using the Query.instances\(\) method without a context",
                r"Passing a string name for the 'alias' argument to "
                r"'contains_eager\(\)` is deprecated",
                "Retrieving row values using Column objects with only "
                "matching names",
            ):
                result = list(
                    q.options(
                        contains_eager("addresses", alias="adalias")
                    ).instances(sess.connection().execute(selectquery))
                )
            assert self.static.user_address_result == result

        self.assert_sql_count(testing.db, go, 1)

    def test_contains_eager_aliased_instances(self):
        addresses, users, User = (
            self.tables.addresses,
            self.tables.users,
            self.classes.User,
        )

        sess = fixture_session()
        q = sess.query(User)

        adalias = addresses.alias("adalias")
        selectquery = users.outerjoin(adalias).select(
            order_by=[users.c.id, adalias.c.id]
        )

        # note this has multiple problems because we aren't giving Query
        # the statement where it would be able to create an adapter
        def go():
            with testing.expect_deprecated(
                r"Using the Query.instances\(\) method without a context"
            ):
                result = list(
                    q.options(
                        contains_eager("addresses", alias=adalias)
                    ).instances(sess.connection().execute(selectquery))
                )
            assert self.static.user_address_result == result

        self.assert_sql_count(testing.db, go, 1)

    def test_contains_eager_multi_string_alias(self):
        orders, items, users, order_items, User = (
            self.tables.orders,
            self.tables.items,
            self.tables.users,
            self.tables.order_items,
            self.classes.User,
        )

        sess = fixture_session()
        q = sess.query(User)

        oalias = orders.alias("o1")
        ialias = items.alias("i1")
        query = (
            users.outerjoin(oalias)
            .outerjoin(order_items)
            .outerjoin(ialias)
            .select()
            .order_by(users.c.id, oalias.c.id, ialias.c.id)
        )

        # test using string alias with more than one level deep
        def go():
            with testing.expect_deprecated(
                r"Using the Query.instances\(\) method without a context",
                r"Passing a string name for the 'alias' argument to "
                r"'contains_eager\(\)` is deprecated",
                "Retrieving row values using Column objects with only "
                "matching names",
            ):
                result = list(
                    q.options(
                        contains_eager("orders", alias="o1"),
                        contains_eager("orders.items", alias="i1"),
                    ).instances(sess.connection().execute(query))
                )
            assert self.static.user_order_result == result

        self.assert_sql_count(testing.db, go, 1)

    def test_contains_eager_multi_alias(self):
        orders, items, users, order_items, User = (
            self.tables.orders,
            self.tables.items,
            self.tables.users,
            self.tables.order_items,
            self.classes.User,
        )

        sess = fixture_session()
        q = sess.query(User)

        oalias = orders.alias("o1")
        ialias = items.alias("i1")
        query = (
            users.outerjoin(oalias)
            .outerjoin(order_items)
            .outerjoin(ialias)
            .select()
            .order_by(users.c.id, oalias.c.id, ialias.c.id)
        )

        # test using Alias with more than one level deep

        # new way:
        # from sqlalchemy.orm.strategy_options import Load
        # opt = Load(User).contains_eager('orders', alias=oalias).
        #     contains_eager('items', alias=ialias)

        def go():
            with testing.expect_deprecated(
                r"Using the Query.instances\(\) method without a context"
            ):
                result = list(
                    q.options(
                        contains_eager("orders", alias=oalias),
                        contains_eager("orders.items", alias=ialias),
                    ).instances(sess.connection().execute(query))
                )
            assert self.static.user_order_result == result

        self.assert_sql_count(testing.db, go, 1)


class TestDeprecation20(fixtures.TestBase):
    def test_relation(self):
        with testing.expect_deprecated_20(".*relationship"):
            relation("foo")

    def test_eagerloading(self):
        with testing.expect_deprecated_20(".*joinedload"):
            eagerload("foo")


class DistinctOrderByImplicitTest(QueryTest, AssertsCompiledSQL):
    __dialect__ = "default"

    def test_columns_augmented_roundtrip_one(self):
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()
        with testing.expect_deprecated_20(join_strings_dep):
            q = (
                sess.query(User)
                .join("addresses")
                .distinct()
                .order_by(desc(Address.email_address))
            )
        with testing.expect_deprecated(
            "ORDER BY columns added implicitly due to "
        ):
            eq_([User(id=7), User(id=9), User(id=8)], q.all())

    def test_columns_augmented_roundtrip_two(self):
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()
        with testing.expect_deprecated_20(join_strings_dep):
            q = (
                sess.query(User)
                .join("addresses")
                .distinct()
                .order_by(desc(Address.email_address).label("foo"))
            )
        with testing.expect_deprecated(
            "ORDER BY columns added implicitly due to "
        ):
            eq_([User(id=7), User(id=9), User(id=8)], q.all())

    def test_columns_augmented_roundtrip_three(self):
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()

        q = (
            sess.query(User.id, User.name.label("foo"), Address.id)
            .join(Address, true())
            .filter(User.name == "jack")
            .filter(User.id + Address.user_id > 0)
            .distinct()
            .order_by(User.id, User.name, Address.email_address)
        )

        # even though columns are added, they aren't in the result
        with testing.expect_deprecated(
            "ORDER BY columns added implicitly due to "
        ):
            eq_(
                q.all(),
                [
                    (7, "jack", 3),
                    (7, "jack", 4),
                    (7, "jack", 2),
                    (7, "jack", 5),
                    (7, "jack", 1),
                ],
            )
            for row in q:
                eq_(row._mapping.keys(), ["id", "foo", "id"])

    def test_columns_augmented_sql_one(self):
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()

        q = (
            sess.query(User.id, User.name.label("foo"), Address.id)
            .distinct()
            .order_by(User.id, User.name, Address.email_address)
        )

        # Address.email_address is added because of DISTINCT,
        # however User.id, User.name are not b.c. they're already there,
        # even though User.name is labeled
        with testing.expect_deprecated(
            "ORDER BY columns added implicitly due to "
        ):
            self.assert_compile(
                q,
                "SELECT DISTINCT users.id AS users_id, users.name AS foo, "
                "addresses.id AS addresses_id, addresses.email_address AS "
                "addresses_email_address FROM users, addresses "
                "ORDER BY users.id, users.name, addresses.email_address",
            )


class SessionEventsTest(_RemoveListeners, _fixtures.FixtureTest):
    run_inserts = None

    def test_on_bulk_update_hook(self):
        User, users = self.classes.User, self.tables.users

        sess = fixture_session()
        canary = Mock()

        event.listen(sess, "after_bulk_update", canary.after_bulk_update)

        def legacy(ses, qry, ctx, res):
            canary.after_bulk_update_legacy(ses, qry, ctx, res)

        event.listen(sess, "after_bulk_update", legacy)

        mapper(User, users)

        with testing.expect_deprecated(
            'The argument signature for the "SessionEvents.after_bulk_update" '
            "event listener"
        ):
            sess.query(User).update({"name": "foo"})

        eq_(canary.after_bulk_update.call_count, 1)

        upd = canary.after_bulk_update.mock_calls[0][1][0]
        eq_(upd.session, sess)
        eq_(
            canary.after_bulk_update_legacy.mock_calls,
            [call(sess, upd.query, None, upd.result)],
        )

    def test_on_bulk_delete_hook(self):
        User, users = self.classes.User, self.tables.users

        sess = fixture_session()
        canary = Mock()

        event.listen(sess, "after_bulk_delete", canary.after_bulk_delete)

        def legacy(ses, qry, ctx, res):
            canary.after_bulk_delete_legacy(ses, qry, ctx, res)

        event.listen(sess, "after_bulk_delete", legacy)

        mapper(User, users)

        with testing.expect_deprecated(
            'The argument signature for the "SessionEvents.after_bulk_delete" '
            "event listener"
        ):
            sess.query(User).delete()

        eq_(canary.after_bulk_delete.call_count, 1)

        upd = canary.after_bulk_delete.mock_calls[0][1][0]
        eq_(upd.session, sess)
        eq_(
            canary.after_bulk_delete_legacy.mock_calls,
            [call(sess, upd.query, None, upd.result)],
        )


class ImmediateTest(_fixtures.FixtureTest):
    run_inserts = "once"
    run_deletes = None

    @classmethod
    def setup_mappers(cls):
        Address, addresses, users, User = (
            cls.classes.Address,
            cls.tables.addresses,
            cls.tables.users,
            cls.classes.User,
        )

        mapper(Address, addresses)

        mapper(User, users, properties=dict(addresses=relationship(Address)))

    def test_value(self):
        User = self.classes.User

        sess = fixture_session()

        with testing.expect_deprecated(r"Query.value\(\) is deprecated"):
            eq_(sess.query(User).filter_by(id=7).value(User.id), 7)
        with testing.expect_deprecated(r"Query.value\(\) is deprecated"):
            eq_(
                sess.query(User.id, User.name).filter_by(id=7).value(User.id),
                7,
            )
        with testing.expect_deprecated(r"Query.value\(\) is deprecated"):
            eq_(sess.query(User).filter_by(id=0).value(User.id), None)

        sess.bind = testing.db
        with testing.expect_deprecated(r"Query.value\(\) is deprecated"):
            eq_(sess.query().value(sa.literal_column("1").label("x")), 1)

    def test_value_cancels_loader_opts(self):
        User = self.classes.User

        sess = fixture_session()

        q = (
            sess.query(User)
            .filter(User.name == "ed")
            .options(joinedload(User.addresses))
        )

        with testing.expect_deprecated(r"Query.value\(\) is deprecated"):
            q = q.value(func.count(literal_column("*")))


class MixedEntitiesTest(QueryTest, AssertsCompiledSQL):
    __dialect__ = "default"

    def test_values(self):
        Address, users, User = (
            self.classes.Address,
            self.tables.users,
            self.classes.User,
        )

        sess = fixture_session()

        with testing.expect_deprecated(r"Query.values?\(\) is deprecated"):
            assert list(sess.query(User).values()) == list()

        sel = users.select(User.id.in_([7, 8])).alias()
        q = sess.query(User)
        with testing.expect_deprecated(r"Query.values?\(\) is deprecated"):
            q2 = q.select_entity_from(sel).values(User.name)
        eq_(list(q2), [("jack",), ("ed",)])

        q = sess.query(User)

        with testing.expect_deprecated(r"Query.values?\(\) is deprecated"):
            q2 = q.order_by(User.id).values(
                User.name, User.name + " " + cast(User.id, String(50))
            )
        eq_(
            list(q2),
            [
                ("jack", "jack 7"),
                ("ed", "ed 8"),
                ("fred", "fred 9"),
                ("chuck", "chuck 10"),
            ],
        )

        with testing.expect_deprecated(r"Query.values?\(\) is deprecated"):
            q2 = (
                q.join("addresses")
                .filter(User.name.like("%e%"))
                .order_by(User.id, Address.id)
                .values(User.name, Address.email_address)
            )
        eq_(
            list(q2),
            [
                ("ed", "ed@wood.com"),
                ("ed", "ed@bettyboop.com"),
                ("ed", "ed@lala.com"),
                ("fred", "fred@fred.com"),
            ],
        )

        with testing.expect_deprecated(r"Query.values?\(\) is deprecated"):
            q2 = (
                q.join("addresses")
                .filter(User.name.like("%e%"))
                .order_by(desc(Address.email_address))
                .slice(1, 3)
                .values(User.name, Address.email_address)
            )
        eq_(list(q2), [("ed", "ed@wood.com"), ("ed", "ed@lala.com")])

        adalias = aliased(Address)
        with testing.expect_deprecated(r"Query.values?\(\) is deprecated"):
            q2 = (
                q.join(adalias, "addresses")
                .filter(User.name.like("%e%"))
                .order_by(adalias.email_address)
                .values(User.name, adalias.email_address)
            )
        eq_(
            list(q2),
            [
                ("ed", "ed@bettyboop.com"),
                ("ed", "ed@lala.com"),
                ("ed", "ed@wood.com"),
                ("fred", "fred@fred.com"),
            ],
        )

        with testing.expect_deprecated(r"Query.values?\(\) is deprecated"):
            q2 = q.values(func.count(User.name))
        assert next(q2) == (4,)

        with testing.expect_deprecated(r"Query.values?\(\) is deprecated"):
            q2 = (
                q.select_entity_from(sel)
                .filter(User.id == 8)
                .values(User.name, sel.c.name, User.name)
            )
        eq_(list(q2), [("ed", "ed", "ed")])

        # using User.xxx is alised against "sel", so this query returns nothing
        with testing.expect_deprecated(r"Query.values?\(\) is deprecated"):
            q2 = (
                q.select_entity_from(sel)
                .filter(User.id == 8)
                .filter(User.id > sel.c.id)
                .values(User.name, sel.c.name, User.name)
            )
        eq_(list(q2), [])

        # whereas this uses users.c.xxx, is not aliased and creates a new join
        with testing.expect_deprecated(r"Query.values?\(\) is deprecated"):
            q2 = (
                q.select_entity_from(sel)
                .filter(users.c.id == 8)
                .filter(users.c.id > sel.c.id)
                .values(users.c.name, sel.c.name, User.name)
            )
            eq_(list(q2), [("ed", "jack", "jack")])

    @testing.fails_on("mssql", "FIXME: unknown")
    def test_values_specific_order_by(self):
        users, User = self.tables.users, self.classes.User

        sess = fixture_session()

        with testing.expect_deprecated(r"Query.values?\(\) is deprecated"):
            assert list(sess.query(User).values()) == list()

        sel = users.select(User.id.in_([7, 8])).alias()
        q = sess.query(User)
        u2 = aliased(User)
        with testing.expect_deprecated(r"Query.values?\(\) is deprecated"):
            q2 = (
                q.select_entity_from(sel)
                .filter(u2.id > 1)
                .filter(or_(u2.id == User.id, u2.id != User.id))
                .order_by(User.id, sel.c.id, u2.id)
                .values(User.name, sel.c.name, u2.name)
            )
        eq_(
            list(q2),
            [
                ("jack", "jack", "jack"),
                ("jack", "jack", "ed"),
                ("jack", "jack", "fred"),
                ("jack", "jack", "chuck"),
                ("ed", "ed", "jack"),
                ("ed", "ed", "ed"),
                ("ed", "ed", "fred"),
                ("ed", "ed", "chuck"),
            ],
        )

    @testing.fails_on("mssql", "FIXME: unknown")
    @testing.fails_on(
        "oracle", "Oracle doesn't support boolean expressions as " "columns"
    )
    @testing.fails_on(
        "postgresql+pg8000",
        "pg8000 parses the SQL itself before passing on "
        "to PG, doesn't parse this",
    )
    @testing.fails_on(
        "postgresql+asyncpg",
        "Asyncpg uses preprated statements that are not compatible with how "
        "sqlalchemy passes the query. Fails with "
        'ERROR:  column "users.name" must appear in the GROUP BY clause'
        " or be used in an aggregate function",
    )
    @testing.fails_on("firebird", "unknown")
    def test_values_with_boolean_selects(self):
        """Tests a values clause that works with select boolean
        evaluations"""

        User = self.classes.User

        sess = fixture_session()

        q = sess.query(User)
        with testing.expect_deprecated(r"Query.values?\(\) is deprecated"):
            q2 = (
                q.group_by(User.name.like("%j%"))
                .order_by(desc(User.name.like("%j%")))
                .values(
                    User.name.like("%j%"), func.count(User.name.like("%j%"))
                )
            )
        eq_(list(q2), [(True, 1), (False, 3)])

        with testing.expect_deprecated(r"Query.values?\(\) is deprecated"):
            q2 = q.order_by(desc(User.name.like("%j%"))).values(
                User.name.like("%j%")
            )
        eq_(list(q2), [(True,), (False,), (False,), (False,)])


class DeclarativeBind(fixtures.TestBase):
    def test_declarative_base(self):
        with testing.expect_deprecated_20(
            "The ``bind`` argument to declarative_base is "
            "deprecated and will be removed in SQLAlchemy 2.0.",
        ):
            Base = declarative_base(bind=testing.db)

        is_true(Base.metadata.bind is testing.db)

    def test_as_declarative(self):
        with testing.expect_deprecated_20(
            "The ``bind`` argument to as_declarative is "
            "deprecated and will be removed in SQLAlchemy 2.0.",
        ):

            @as_declarative(bind=testing.db)
            class Base(object):
                @declared_attr
                def __tablename__(cls):
                    return cls.__name__.lower()

                id = Column(Integer, primary_key=True)

        is_true(Base.metadata.bind is testing.db)


class JoinTest(QueryTest, AssertsCompiledSQL):
    __dialect__ = "default"

    def test_implicit_joins_from_aliases(self):
        Item, User, Order = (
            self.classes.Item,
            self.classes.User,
            self.classes.Order,
        )

        sess = fixture_session()
        OrderAlias = aliased(Order)

        with testing.expect_deprecated_20(join_strings_dep):
            eq_(
                sess.query(OrderAlias)
                .join("items")
                .filter_by(description="item 3")
                .order_by(OrderAlias.id)
                .all(),
                [
                    Order(
                        address_id=1,
                        description="order 1",
                        isopen=0,
                        user_id=7,
                        id=1,
                    ),
                    Order(
                        address_id=4,
                        description="order 2",
                        isopen=0,
                        user_id=9,
                        id=2,
                    ),
                    Order(
                        address_id=1,
                        description="order 3",
                        isopen=1,
                        user_id=7,
                        id=3,
                    ),
                ],
            )

        with testing.expect_deprecated_20(join_strings_dep, join_aliased_dep):
            eq_(
                sess.query(User, OrderAlias, Item.description)
                .join(OrderAlias, "orders")
                .join("items", from_joinpoint=True)
                .filter_by(description="item 3")
                .order_by(User.id, OrderAlias.id)
                .all(),
                [
                    (
                        User(name="jack", id=7),
                        Order(
                            address_id=1,
                            description="order 1",
                            isopen=0,
                            user_id=7,
                            id=1,
                        ),
                        "item 3",
                    ),
                    (
                        User(name="jack", id=7),
                        Order(
                            address_id=1,
                            description="order 3",
                            isopen=1,
                            user_id=7,
                            id=3,
                        ),
                        "item 3",
                    ),
                    (
                        User(name="fred", id=9),
                        Order(
                            address_id=4,
                            description="order 2",
                            isopen=0,
                            user_id=9,
                            id=2,
                        ),
                        "item 3",
                    ),
                ],
            )

    def test_orderby_arg_bug(self):
        User, users, Order = (
            self.classes.User,
            self.tables.users,
            self.classes.Order,
        )

        sess = fixture_session()
        # no arg error
        with testing.expect_deprecated_20(join_aliased_dep):
            (
                sess.query(User)
                .join("orders", aliased=True)
                .order_by(Order.id)
                .reset_joinpoint()
                .order_by(users.c.id)
                .all()
            )

    def test_aliased(self):
        """test automatic generation of aliased joins."""

        Item, Order, User, Address = (
            self.classes.Item,
            self.classes.Order,
            self.classes.User,
            self.classes.Address,
        )

        sess = fixture_session()

        # test a basic aliasized path
        with testing.expect_deprecated(join_aliased_dep, join_strings_dep):
            q = (
                sess.query(User)
                .join("addresses", aliased=True)
                .filter_by(email_address="jack@bean.com")
            )
        assert [User(id=7)] == q.all()

        with testing.expect_deprecated(join_aliased_dep, join_strings_dep):
            q = (
                sess.query(User)
                .join("addresses", aliased=True)
                .filter(Address.email_address == "jack@bean.com")
            )
        assert [User(id=7)] == q.all()

        with testing.expect_deprecated(join_aliased_dep, join_strings_dep):
            q = (
                sess.query(User)
                .join("addresses", aliased=True)
                .filter(
                    or_(
                        Address.email_address == "jack@bean.com",
                        Address.email_address == "fred@fred.com",
                    )
                )
            )
        assert [User(id=7), User(id=9)] == q.all()

        # test two aliasized paths, one to 'orders' and the other to
        # 'orders','items'. one row is returned because user 7 has order 3 and
        # also has order 1 which has item 1
        # this tests a o2m join and a m2m join.
        with testing.expect_deprecated(
            join_aliased_dep, join_strings_dep, join_chain_dep
        ):
            q = (
                sess.query(User)
                .join("orders", aliased=True)
                .filter(Order.description == "order 3")
                .join("orders", "items", aliased=True)
                .filter(Item.description == "item 1")
            )
        assert q.count() == 1
        assert [User(id=7)] == q.all()

        with testing.expect_deprecated(join_strings_dep, join_chain_dep):
            # test the control version - same joins but not aliased. rows are
            # not returned because order 3 does not have item 1
            q = (
                sess.query(User)
                .join("orders")
                .filter(Order.description == "order 3")
                .join("orders", "items")
                .filter(Item.description == "item 1")
            )
        assert [] == q.all()
        assert q.count() == 0

        # the left half of the join condition of the any() is aliased.
        with testing.expect_deprecated(join_aliased_dep, join_strings_dep):
            q = (
                sess.query(User)
                .join("orders", aliased=True)
                .filter(Order.items.any(Item.description == "item 4"))
            )
        assert [User(id=7)] == q.all()

        # test that aliasing gets reset when join() is called
        with testing.expect_deprecated(join_aliased_dep, join_strings_dep):
            q = (
                sess.query(User)
                .join("orders", aliased=True)
                .filter(Order.description == "order 3")
                .join("orders", aliased=True)
                .filter(Order.description == "order 5")
            )
        assert q.count() == 1
        assert [User(id=7)] == q.all()

    def test_overlapping_paths_two(self):
        User = self.classes.User

        sess = fixture_session()

        # test overlapping paths.   User->orders is used by both joins, but
        # rendered once.
        with testing.expect_deprecated_20(join_strings_dep, join_chain_dep):
            self.assert_compile(
                sess.query(User)
                .join("orders", "items")
                .join("orders", "address"),
                "SELECT users.id AS users_id, users.name AS users_name "
                "FROM users "
                "JOIN orders "
                "ON users.id = orders.user_id "
                "JOIN order_items AS order_items_1 "
                "ON orders.id = order_items_1.order_id "
                "JOIN items ON items.id = order_items_1.item_id JOIN "
                "addresses "
                "ON addresses.id = orders.address_id",
            )

    def test_overlapping_paths_three(self):
        User = self.classes.User

        for aliased_ in (True, False):
            # load a user who has an order that contains item id 3 and address
            # id 1 (order 3, owned by jack)

            warnings = (join_strings_dep, join_chain_dep)
            if aliased_:
                warnings += (join_aliased_dep,)

            with testing.expect_deprecated_20(*warnings):
                result = (
                    fixture_session()
                    .query(User)
                    .join("orders", "items", aliased=aliased_)
                    .filter_by(id=3)
                    .join("orders", "address", aliased=aliased_)
                    .filter_by(id=1)
                    .all()
                )
            assert [User(id=7, name="jack")] == result

    def test_overlapping_paths_multilevel(self):
        User = self.classes.User

        s = fixture_session()

        with testing.expect_deprecated_20(join_strings_dep, join_chain_dep):
            q = (
                s.query(User)
                .join("orders")
                .join("addresses")
                .join("orders", "items")
                .join("addresses", "dingaling")
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

    def test_from_joinpoint(self):
        Item, User, Order = (
            self.classes.Item,
            self.classes.User,
            self.classes.Order,
        )

        sess = fixture_session()

        for oalias, ialias in [
            (True, True),
            (False, False),
            (True, False),
            (False, True),
        ]:
            with testing.expect_deprecated(join_aliased_dep, join_strings_dep):
                eq_(
                    sess.query(User)
                    .join("orders", aliased=oalias)
                    .join("items", from_joinpoint=True, aliased=ialias)
                    .filter(Item.description == "item 4")
                    .all(),
                    [User(name="jack")],
                )

            # use middle criterion
            with testing.expect_deprecated(join_aliased_dep, join_strings_dep):
                eq_(
                    sess.query(User)
                    .join("orders", aliased=oalias)
                    .filter(Order.user_id == 9)
                    .join("items", from_joinpoint=True, aliased=ialias)
                    .filter(Item.description == "item 4")
                    .all(),
                    [],
                )

        orderalias = aliased(Order)
        itemalias = aliased(Item)
        with testing.expect_deprecated(join_aliased_dep, join_strings_dep):
            eq_(
                sess.query(User)
                .join(orderalias, "orders")
                .join(itemalias, "items", from_joinpoint=True)
                .filter(itemalias.description == "item 4")
                .all(),
                [User(name="jack")],
            )
        with testing.expect_deprecated(join_aliased_dep, join_strings_dep):
            eq_(
                sess.query(User)
                .join(orderalias, "orders")
                .join(itemalias, "items", from_joinpoint=True)
                .filter(orderalias.user_id == 9)
                .filter(itemalias.description == "item 4")
                .all(),
                [],
            )

    def test_multi_tuple_form_legacy_one(self):
        """test the 'tuple' form of join, now superseded
        by the two-element join() form.


        """

        Order, User = (
            self.classes.Order,
            self.classes.User,
        )

        sess = fixture_session()

        with testing.expect_deprecated(join_tuple_form):
            q = (
                sess.query(User)
                .join((Order, User.id == Order.user_id))
                .filter_by(description="foo")
            )
        self.assert_compile(
            q,
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users JOIN orders ON users.id = orders.user_id "
            "WHERE orders.description = :description_1",
        )

    def test_multi_tuple_form_legacy_two(self):
        """test the 'tuple' form of join, now superseded
        by the two-element join() form.


        """

        Item, Order, User = (
            self.classes.Item,
            self.classes.Order,
            self.classes.User,
        )

        sess = fixture_session()

        with testing.expect_deprecated_20(join_tuple_form):
            q = (
                sess.query(User)
                .join((Order, User.id == Order.user_id), (Item, Order.items))
                .filter_by(description="foo")
            )
        self.assert_compile(
            q,
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users JOIN orders ON users.id = orders.user_id "
            "JOIN order_items AS order_items_1 ON orders.id = "
            "order_items_1.order_id JOIN items ON items.id = "
            "order_items_1.item_id WHERE items.description = :description_1",
        )

    def test_multi_tuple_form_legacy_three(self):
        """test the 'tuple' form of join, now superseded
        by the two-element join() form.


        """

        Order, User = (
            self.classes.Order,
            self.classes.User,
        )

        sess = fixture_session()

        # the old "backwards" form
        with testing.expect_deprecated_20(join_tuple_form, join_strings_dep):
            q = (
                sess.query(User)
                .join(("orders", Order))
                .filter_by(description="foo")
            )
        self.assert_compile(
            q,
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users JOIN orders ON users.id = orders.user_id "
            "WHERE orders.description = :description_1",
        )

    def test_multi_tuple_form_legacy_three_point_five(self):
        """test the 'tuple' form of join, now superseded
        by the two-element join() form.


        """

        Order, User = (
            self.classes.Order,
            self.classes.User,
        )

        sess = fixture_session()

        with testing.expect_deprecated_20(join_strings_dep):
            q = (
                sess.query(User)
                .join(Order, "orders")
                .filter_by(description="foo")
            )
        self.assert_compile(
            q,
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users JOIN orders ON users.id = orders.user_id "
            "WHERE orders.description = :description_1",
        )

    def test_multi_tuple_form_legacy_four(self):
        User, Order, Item, Keyword = self.classes(
            "User", "Order", "Item", "Keyword"
        )

        sess = fixture_session()

        # ensure when the tokens are broken up that from_joinpoint
        # is set between them

        expected = (
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users JOIN orders ON users.id = orders.user_id "
            "JOIN order_items AS order_items_1 ON orders.id = "
            "order_items_1.order_id JOIN items ON items.id = "
            "order_items_1.item_id JOIN item_keywords AS item_keywords_1 "
            "ON items.id = item_keywords_1.item_id "
            "JOIN keywords ON keywords.id = item_keywords_1.keyword_id"
        )

        with testing.expect_deprecated_20(join_tuple_form, join_strings_dep):
            q = sess.query(User).join(
                (Order, "orders"), (Item, "items"), (Keyword, "keywords")
            )
        self.assert_compile(q, expected)

        with testing.expect_deprecated_20(join_strings_dep):
            q = sess.query(User).join("orders", "items", "keywords")
        self.assert_compile(q, expected)

    def test_single_name(self):
        User = self.classes.User

        sess = fixture_session()

        with testing.expect_deprecated_20(join_strings_dep):
            self.assert_compile(
                sess.query(User).join("orders"),
                "SELECT users.id AS users_id, users.name AS users_name "
                "FROM users JOIN orders ON users.id = orders.user_id",
            )

        with testing.expect_deprecated_20(join_strings_dep):
            assert_raises(
                sa_exc.InvalidRequestError,
                sess.query(User).join("user")._compile_context,
            )

        with testing.expect_deprecated_20(join_strings_dep, join_chain_dep):
            self.assert_compile(
                sess.query(User).join("orders", "items"),
                "SELECT users.id AS users_id, users.name AS users_name "
                "FROM users "
                "JOIN orders ON users.id = orders.user_id "
                "JOIN order_items AS order_items_1 "
                "ON orders.id = order_items_1.order_id JOIN items "
                "ON items.id = order_items_1.item_id",
            )

        # test overlapping paths.   User->orders is used by both joins, but
        # rendered once.
        with testing.expect_deprecated_20(join_strings_dep, join_chain_dep):
            self.assert_compile(
                sess.query(User)
                .join("orders", "items")
                .join("orders", "address"),
                "SELECT users.id AS users_id, users.name AS users_name "
                "FROM users "
                "JOIN orders "
                "ON users.id = orders.user_id "
                "JOIN order_items AS order_items_1 "
                "ON orders.id = order_items_1.order_id "
                "JOIN items ON items.id = order_items_1.item_id JOIN "
                "addresses "
                "ON addresses.id = orders.address_id",
            )

    def test_single_prop_5(self):
        (
            Order,
            User,
        ) = (self.classes.Order, self.classes.User)

        sess = fixture_session()
        with testing.expect_deprecated_20(join_chain_dep):
            self.assert_compile(
                sess.query(User).join(User.orders, Order.items),
                "SELECT users.id AS users_id, users.name AS users_name "
                "FROM users "
                "JOIN orders ON users.id = orders.user_id "
                "JOIN order_items AS order_items_1 "
                "ON orders.id = order_items_1.order_id JOIN items "
                "ON items.id = order_items_1.item_id",
            )

    def test_single_prop_7(self):
        Order, User = (self.classes.Order, self.classes.User)

        sess = fixture_session()
        # this query is somewhat nonsensical.  the old system didn't render a
        # correct query for this. In this case its the most faithful to what
        # was asked - there's no linkage between User.orders and "oalias",
        # so two FROM elements are generated.
        oalias = aliased(Order)
        with testing.expect_deprecated_20(join_chain_dep):
            self.assert_compile(
                sess.query(User).join(User.orders, oalias.items),
                "SELECT users.id AS users_id, users.name AS users_name "
                "FROM users "
                "JOIN orders ON users.id = orders.user_id, "
                "orders AS orders_1 JOIN order_items AS order_items_1 "
                "ON orders_1.id = order_items_1.order_id "
                "JOIN items ON items.id = order_items_1.item_id",
            )

    def test_single_prop_8(self):
        (
            Order,
            User,
        ) = (self.classes.Order, self.classes.User)

        sess = fixture_session()
        # same as before using an aliased() for User as well
        ualias = aliased(User)
        oalias = aliased(Order)
        with testing.expect_deprecated_20(join_chain_dep):
            self.assert_compile(
                sess.query(ualias).join(ualias.orders, oalias.items),
                "SELECT users_1.id AS users_1_id, users_1.name "
                "AS users_1_name "
                "FROM users AS users_1 "
                "JOIN orders ON users_1.id = orders.user_id, "
                "orders AS orders_1 JOIN order_items AS order_items_1 "
                "ON orders_1.id = order_items_1.order_id "
                "JOIN items ON items.id = order_items_1.item_id",
            )

    def test_single_prop_10(self):
        User, Address = (self.classes.User, self.classes.Address)

        sess = fixture_session()
        with testing.expect_deprecated_20(join_aliased_dep):
            self.assert_compile(
                sess.query(User)
                .join(User.addresses, aliased=True)
                .filter(Address.email_address == "foo"),
                "SELECT users.id AS users_id, users.name AS users_name "
                "FROM users JOIN addresses AS addresses_1 "
                "ON users.id = addresses_1.user_id "
                "WHERE addresses_1.email_address = :email_address_1",
            )

    def test_single_prop_11(self):
        Item, Order, User, = (
            self.classes.Item,
            self.classes.Order,
            self.classes.User,
        )

        sess = fixture_session()
        with testing.expect_deprecated_20(join_aliased_dep, join_chain_dep):
            self.assert_compile(
                sess.query(User)
                .join(User.orders, Order.items, aliased=True)
                .filter(Item.id == 10),
                "SELECT users.id AS users_id, users.name AS users_name "
                "FROM users JOIN orders AS orders_1 "
                "ON users.id = orders_1.user_id "
                "JOIN order_items AS order_items_1 "
                "ON orders_1.id = order_items_1.order_id "
                "JOIN items AS items_1 ON items_1.id = order_items_1.item_id "
                "WHERE items_1.id = :id_1",
            )

    def test_multiple_adaption(self):
        Item, Order, User = (
            self.classes.Item,
            self.classes.Order,
            self.classes.User,
        )

        sess = fixture_session()

        with testing.expect_deprecated_20(join_chain_dep, join_aliased_dep):
            self.assert_compile(
                sess.query(User)
                .join(User.orders, Order.items, aliased=True)
                .filter(Order.id == 7)
                .filter(Item.id == 8),
                "SELECT users.id AS users_id, users.name AS users_name "
                "FROM users "
                "JOIN orders AS orders_1 "
                "ON users.id = orders_1.user_id JOIN order_items AS "
                "order_items_1 "
                "ON orders_1.id = order_items_1.order_id "
                "JOIN items AS items_1 ON items_1.id = order_items_1.item_id "
                "WHERE orders_1.id = :id_1 AND items_1.id = :id_2",
                use_default_dialect=True,
            )

    def test_onclause_conditional_adaption(self):
        Item, Order, orders, order_items, User = (
            self.classes.Item,
            self.classes.Order,
            self.tables.orders,
            self.tables.order_items,
            self.classes.User,
        )

        sess = fixture_session()

        # this is now a very weird test, nobody should really
        # be using the aliased flag in this way.
        with testing.expect_deprecated_20(join_aliased_dep):
            self.assert_compile(
                sess.query(User)
                .join(User.orders, aliased=True)
                .join(
                    Item,
                    and_(
                        Order.id == order_items.c.order_id,
                        order_items.c.item_id == Item.id,
                    ),
                    from_joinpoint=True,
                    aliased=True,
                ),
                "SELECT users.id AS users_id, users.name AS users_name "
                "FROM users "
                "JOIN orders AS orders_1 ON users.id = orders_1.user_id "
                "JOIN items AS items_1 "
                "ON orders_1.id = order_items.order_id "
                "AND order_items.item_id = items_1.id",
                use_default_dialect=True,
            )

        # nothing is deprecated here but it is comparing to the above
        # that nothing is adapted.
        oalias = aliased(Order, orders.select().subquery())
        self.assert_compile(
            sess.query(User)
            .join(oalias, User.orders)
            .join(
                Item,
                and_(
                    oalias.id == order_items.c.order_id,
                    order_items.c.item_id == Item.id,
                ),
            ),
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users JOIN "
            "(SELECT orders.id AS id, orders.user_id AS user_id, "
            "orders.address_id AS address_id, orders.description "
            "AS description, orders.isopen AS isopen FROM orders) AS anon_1 "
            "ON users.id = anon_1.user_id JOIN items "
            "ON anon_1.id = order_items.order_id "
            "AND order_items.item_id = items.id",
            use_default_dialect=True,
        )

        # query.join(<stuff>, aliased=True).join(target, sql_expression)
        # or: query.join(path_to_some_joined_table_mapper).join(target,
        # sql_expression)

    def test_overlap_with_aliases(self):
        orders, User, users = (
            self.tables.orders,
            self.classes.User,
            self.tables.users,
        )

        oalias = orders.alias("oalias")

        with testing.expect_deprecated_20(join_strings_dep, join_chain_dep):
            result = (
                fixture_session()
                .query(User)
                .select_from(users.join(oalias))
                .filter(
                    oalias.c.description.in_(["order 1", "order 2", "order 3"])
                )
                .join("orders", "items")
                .order_by(User.id)
                .all()
            )
        assert [User(id=7, name="jack"), User(id=9, name="fred")] == result

        with testing.expect_deprecated_20(join_strings_dep, join_chain_dep):
            result = (
                fixture_session()
                .query(User)
                .select_from(users.join(oalias))
                .filter(
                    oalias.c.description.in_(["order 1", "order 2", "order 3"])
                )
                .join("orders", "items")
                .filter_by(id=4)
                .all()
            )
        assert [User(id=7, name="jack")] == result

    def test_reset_joinpoint(self):
        User = self.classes.User

        for aliased_ in (True, False):
            warnings = (
                join_strings_dep,
                join_chain_dep,
            )
            if aliased_:
                warnings += (join_aliased_dep,)
            # load a user who has an order that contains item id 3 and address
            # id 1 (order 3, owned by jack)

            with fixture_session() as sess:
                with testing.expect_deprecated_20(*warnings):
                    result = (
                        sess.query(User)
                        .join("orders", "items", aliased=aliased_)
                        .filter_by(id=3)
                        .reset_joinpoint()
                        .join("orders", "address", aliased=aliased_)
                        .filter_by(id=1)
                        .all()
                    )
                assert [User(id=7, name="jack")] == result

            with fixture_session() as sess:
                with testing.expect_deprecated_20(*warnings):
                    result = (
                        sess.query(User)
                        .join(
                            "orders", "items", aliased=aliased_, isouter=True
                        )
                        .filter_by(id=3)
                        .reset_joinpoint()
                        .join(
                            "orders", "address", aliased=aliased_, isouter=True
                        )
                        .filter_by(id=1)
                        .all()
                    )
                assert [User(id=7, name="jack")] == result

            with fixture_session() as sess:
                with testing.expect_deprecated_20(*warnings):
                    result = (
                        sess.query(User)
                        .outerjoin("orders", "items", aliased=aliased_)
                        .filter_by(id=3)
                        .reset_joinpoint()
                        .outerjoin("orders", "address", aliased=aliased_)
                        .filter_by(id=1)
                        .all()
                    )
                assert [User(id=7, name="jack")] == result


class AliasFromCorrectLeftTest(
    fixtures.DeclarativeMappedTest, AssertsCompiledSQL
):
    run_create_tables = None
    __dialect__ = "default"

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class Object(Base):
            __tablename__ = "object"

            type = Column(String(30))
            __mapper_args__ = {
                "polymorphic_identity": "object",
                "polymorphic_on": type,
            }

            id = Column(Integer, primary_key=True)
            name = Column(String(256))

        class A(Object):
            __tablename__ = "a"

            __mapper_args__ = {"polymorphic_identity": "a"}

            id = Column(Integer, ForeignKey("object.id"), primary_key=True)

            b_list = relationship(
                "B", secondary="a_b_association", backref="a_list"
            )

        class B(Object):
            __tablename__ = "b"

            __mapper_args__ = {"polymorphic_identity": "b"}

            id = Column(Integer, ForeignKey("object.id"), primary_key=True)

        class ABAssociation(Base):
            __tablename__ = "a_b_association"

            a_id = Column(Integer, ForeignKey("a.id"), primary_key=True)
            b_id = Column(Integer, ForeignKey("b.id"), primary_key=True)

        class X(Base):
            __tablename__ = "x"

            id = Column(Integer, primary_key=True)
            name = Column(String(30))

            obj_id = Column(Integer, ForeignKey("object.id"))
            obj = relationship("Object", backref="x_list")

    def test_join_prop_to_string(self):
        A, B, X = self.classes("A", "B", "X")

        s = fixture_session()

        with testing.expect_deprecated_20(join_strings_dep):
            q = s.query(B).join(B.a_list, "x_list").filter(X.name == "x1")

        self.assert_compile(
            q,
            "SELECT object.type AS object_type, b.id AS b_id, "
            "object.id AS object_id, object.name AS object_name "
            "FROM object JOIN b ON object.id = b.id "
            "JOIN a_b_association AS a_b_association_1 "
            "ON b.id = a_b_association_1.b_id "
            "JOIN ("
            "object AS object_1 "
            "JOIN a AS a_1 ON object_1.id = a_1.id"
            ") ON a_1.id = a_b_association_1.a_id "
            "JOIN x ON object_1.id = x.obj_id WHERE x.name = :name_1",
        )

    def test_join_prop_to_prop(self):
        A, B, X = self.classes("A", "B", "X")

        s = fixture_session()

        # B -> A, but both are Object.  So when we say A.x_list, make sure
        # we pick the correct right side
        with testing.expect_deprecated_20(join_chain_dep):
            q = s.query(B).join(B.a_list, A.x_list).filter(X.name == "x1")

        self.assert_compile(
            q,
            "SELECT object.type AS object_type, b.id AS b_id, "
            "object.id AS object_id, object.name AS object_name "
            "FROM object JOIN b ON object.id = b.id "
            "JOIN a_b_association AS a_b_association_1 "
            "ON b.id = a_b_association_1.b_id "
            "JOIN ("
            "object AS object_1 "
            "JOIN a AS a_1 ON object_1.id = a_1.id"
            ") ON a_1.id = a_b_association_1.a_id "
            "JOIN x ON object_1.id = x.obj_id WHERE x.name = :name_1",
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

    def test_join_1(self):
        Node = self.classes.Node
        sess = fixture_session()

        with testing.expect_deprecated_20(join_strings_dep, join_aliased_dep):
            node = (
                sess.query(Node)
                .join("children", aliased=True)
                .filter_by(data="n122")
                .first()
            )
        assert node.data == "n12"

    def test_join_2(self):
        Node = self.classes.Node
        sess = fixture_session()
        with testing.expect_deprecated_20(join_aliased_dep):
            ret = (
                sess.query(Node.data)
                .join(Node.children, aliased=True)
                .filter_by(data="n122")
                .all()
            )
        assert ret == [("n12",)]

    def test_join_3_filter_by(self):
        Node = self.classes.Node
        sess = fixture_session()
        with testing.expect_deprecated_20(
            join_strings_dep, join_aliased_dep, join_chain_dep
        ):
            q = (
                sess.query(Node)
                .join("children", "children", aliased=True)
                .filter_by(data="n122")
            )
        self.assert_compile(
            q,
            "SELECT nodes.id AS nodes_id, nodes.parent_id AS nodes_parent_id, "
            "nodes.data AS nodes_data FROM nodes JOIN nodes AS nodes_1 "
            "ON nodes.id = nodes_1.parent_id JOIN nodes AS nodes_2 "
            "ON nodes_1.id = nodes_2.parent_id WHERE nodes_2.data = :data_1",
            checkparams={"data_1": "n122"},
        )
        node = q.first()
        eq_(node.data, "n1")

    def test_join_3_filter(self):
        Node = self.classes.Node
        sess = fixture_session()
        with testing.expect_deprecated_20(
            join_strings_dep, join_aliased_dep, join_chain_dep
        ):
            q = (
                sess.query(Node)
                .join("children", "children", aliased=True)
                .filter(Node.data == "n122")
            )
        self.assert_compile(
            q,
            "SELECT nodes.id AS nodes_id, nodes.parent_id AS nodes_parent_id, "
            "nodes.data AS nodes_data FROM nodes JOIN nodes AS nodes_1 "
            "ON nodes.id = nodes_1.parent_id JOIN nodes AS nodes_2 "
            "ON nodes_1.id = nodes_2.parent_id WHERE nodes_2.data = :data_1",
            checkparams={"data_1": "n122"},
        )
        node = q.first()
        eq_(node.data, "n1")

    def test_join_4_filter_by(self):
        Node = self.classes.Node
        sess = fixture_session()

        with testing.expect_deprecated_20(join_strings_dep, join_aliased_dep):
            q = (
                sess.query(Node)
                .filter_by(data="n122")
                .join("parent", aliased=True)
                .filter_by(data="n12")
                .join("parent", aliased=True, from_joinpoint=True)
                .filter_by(data="n1")
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

    def test_join_4_filter(self):
        Node = self.classes.Node
        sess = fixture_session()

        with testing.expect_deprecated_20(join_strings_dep, join_aliased_dep):
            q = (
                sess.query(Node)
                .filter(Node.data == "n122")
                .join("parent", aliased=True)
                .filter(Node.data == "n12")
                .join("parent", aliased=True, from_joinpoint=True)
                .filter(Node.data == "n1")
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

    def test_string_or_prop_aliased_one(self):
        """test that join('foo') behaves the same as join(Cls.foo) in a self
        referential scenario.

        """

        Node = self.classes.Node

        sess = fixture_session()
        nalias = aliased(
            Node, sess.query(Node).filter_by(data="n1").subquery()
        )

        with testing.expect_deprecated_20(join_aliased_dep):
            q1 = (
                sess.query(nalias)
                .join(nalias.children, aliased=True)
                .join(Node.children, from_joinpoint=True)
                .filter(Node.data == "n1")
            )

        with testing.expect_deprecated_20(join_aliased_dep, join_strings_dep):
            q2 = (
                sess.query(nalias)
                .join(nalias.children, aliased=True)
                .join("children", from_joinpoint=True)
                .filter(Node.data == "n1")
            )

        for q in (q1, q2):
            self.assert_compile(
                q,
                "SELECT anon_1.id AS anon_1_id, anon_1.parent_id AS "
                "anon_1_parent_id, anon_1.data AS anon_1_data FROM "
                "(SELECT nodes.id AS id, nodes.parent_id AS parent_id, "
                "nodes.data AS data FROM nodes WHERE nodes.data = :data_1) "
                "AS anon_1 JOIN nodes AS nodes_1 ON anon_1.id = "
                "nodes_1.parent_id JOIN nodes "
                "ON nodes_1.id = nodes.parent_id "
                "WHERE nodes_1.data = :data_2",
                use_default_dialect=True,
                checkparams={"data_1": "n1", "data_2": "n1"},
            )

    def test_string_or_prop_aliased_two(self):
        Node = self.classes.Node

        sess = fixture_session()
        nalias = aliased(
            Node, sess.query(Node).filter_by(data="n1").subquery()
        )

        with testing.expect_deprecated_20(join_aliased_dep):
            q1 = (
                sess.query(Node)
                .filter(Node.data == "n1")
                .join(nalias.children, aliased=True)
                .filter(nalias.data == "n2")
                .join(Node.children, aliased=True, from_joinpoint=True)
                .filter(Node.data == "n3")
                .join(Node.children, from_joinpoint=True)
                .filter(Node.data == "n4")
            )

        with testing.expect_deprecated_20(join_aliased_dep, join_strings_dep):
            q2 = (
                sess.query(Node)
                .filter(Node.data == "n1")
                .join(nalias.children, aliased=True)
                .filter(nalias.data == "n2")
                .join("children", aliased=True, from_joinpoint=True)
                .filter(Node.data == "n3")
                .join("children", from_joinpoint=True)
                .filter(Node.data == "n4")
            )

        for q in (q1, q2):
            self.assert_compile(
                q,
                "SELECT nodes.id AS nodes_id, nodes.parent_id "
                "AS nodes_parent_id, nodes.data AS nodes_data "
                "FROM (SELECT nodes.id AS id, nodes.parent_id AS parent_id, "
                "nodes.data AS data FROM nodes WHERE nodes.data = :data_1) "
                "AS anon_1 JOIN nodes AS nodes_1 "
                "ON anon_1.id = nodes_1.parent_id JOIN nodes AS nodes_2 "
                "ON nodes_1.id = nodes_2.parent_id JOIN nodes "
                "ON nodes_2.id = nodes.parent_id WHERE nodes.data = :data_2 "
                "AND anon_1.data = :data_3 AND nodes_2.data = :data_4 "
                "AND nodes_2.data = :data_5",
                use_default_dialect=True,
                checkparams={
                    "data_1": "n1",
                    "data_2": "n1",
                    "data_3": "n2",
                    "data_4": "n3",
                    "data_5": "n4",
                },
            )


class InheritedJoinTest(_poly_fixtures._Polymorphic, AssertsCompiledSQL):
    run_setup_mappers = "once"

    def test_multiple_adaption(self):
        """test that multiple filter() adapters get chained together "
        and work correctly within a multiple-entry join()."""

        people, Company, Machine, engineers, machines, Engineer = (
            self.tables.people,
            self.classes.Company,
            self.classes.Machine,
            self.tables.engineers,
            self.tables.machines,
            self.classes.Engineer,
        )

        sess = fixture_session()

        mach_alias = aliased(Machine, machines.select().subquery())

        with testing.expect_deprecated_20(join_aliased_dep):
            self.assert_compile(
                sess.query(Company)
                .join(people.join(engineers), Company.employees)
                .join(mach_alias, Engineer.machines, from_joinpoint=True)
                .filter(Engineer.name == "dilbert")
                .filter(mach_alias.name == "foo"),
                "SELECT companies.company_id AS companies_company_id, "
                "companies.name AS companies_name "
                "FROM companies JOIN (people "
                "JOIN engineers ON people.person_id = "
                "engineers.person_id) ON companies.company_id = "
                "people.company_id JOIN "
                "(SELECT machines.machine_id AS machine_id, "
                "machines.name AS name, "
                "machines.engineer_id AS engineer_id "
                "FROM machines) AS anon_1 "
                "ON engineers.person_id = anon_1.engineer_id "
                "WHERE people.name = :name_1 AND anon_1.name = :name_2",
                use_default_dialect=True,
            )

    def test_prop_with_polymorphic_1(self):
        Person, Manager, Paperwork = (
            self.classes.Person,
            self.classes.Manager,
            self.classes.Paperwork,
        )

        sess = fixture_session()

        with testing.expect_deprecated_20(join_strings_dep, w_polymorphic_dep):
            self.assert_compile(
                sess.query(Person)
                .with_polymorphic(Manager)
                .order_by(Person.person_id)
                .join("paperwork")
                .filter(Paperwork.description.like("%review%")),
                "SELECT people.person_id AS people_person_id, "
                "people.company_id AS"
                " people_company_id, "
                "people.name AS people_name, people.type AS people_type, "
                "managers.person_id AS managers_person_id, "
                "managers.status AS managers_status, managers.manager_name AS "
                "managers_manager_name FROM people "
                "LEFT OUTER JOIN managers "
                "ON people.person_id = managers.person_id "
                "JOIN paperwork "
                "ON people.person_id = paperwork.person_id "
                "WHERE paperwork.description LIKE :description_1 "
                "ORDER BY people.person_id",
                use_default_dialect=True,
            )

    def test_prop_with_polymorphic_2(self):
        Person, Manager, Paperwork = (
            self.classes.Person,
            self.classes.Manager,
            self.classes.Paperwork,
        )

        sess = fixture_session()

        with testing.expect_deprecated_20(
            join_strings_dep, w_polymorphic_dep, join_aliased_dep
        ):
            self.assert_compile(
                sess.query(Person)
                .with_polymorphic(Manager)
                .order_by(Person.person_id)
                .join("paperwork", aliased=True)
                .filter(Paperwork.description.like("%review%")),
                "SELECT people.person_id AS people_person_id, "
                "people.company_id AS people_company_id, "
                "people.name AS people_name, people.type AS people_type, "
                "managers.person_id AS managers_person_id, "
                "managers.status AS managers_status, "
                "managers.manager_name AS managers_manager_name "
                "FROM people LEFT OUTER JOIN managers "
                "ON people.person_id = managers.person_id "
                "JOIN paperwork AS paperwork_1 "
                "ON people.person_id = paperwork_1.person_id "
                "WHERE paperwork_1.description "
                "LIKE :description_1 ORDER BY people.person_id",
                use_default_dialect=True,
            )

    def test_with_poly_loader_criteria_warning(self):
        Person, Manager = (
            self.classes.Person,
            self.classes.Manager,
        )

        sess = fixture_session()

        with testing.expect_deprecated_20(w_polymorphic_dep):
            q = (
                sess.query(Person)
                .with_polymorphic(Manager)
                .options(with_loader_criteria(Person, Person.person_id == 1))
            )

        with testing.expect_warnings(
            r"The with_loader_criteria\(\) function may not work "
            r"correctly with the legacy Query.with_polymorphic\(\)"
        ):
            str(q)


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

    def test_mapped_to_select_implicit_left_w_aliased(self):
        T1, T2 = self.classes.T1, self.classes.T2

        sess = fixture_session()
        subq = (
            sess.query(T2.t1_id, func.count(T2.id).label("count"))
            .group_by(T2.t1_id)
            .subquery()
        )

        with testing.expect_deprecated_20(join_aliased_dep):
            assert_raises_message(
                sa_exc.InvalidRequestError,
                r"The aliased=True parameter on query.join\(\) only works "
                "with "
                "an ORM entity, not a plain selectable, as the target.",
                # this doesn't work, so have it raise an error
                sess.query(T1.id)
                .join(subq, subq.c.t1_id == T1.id, aliased=True)
                ._compile_context,
            )


class MultiplePathTest(fixtures.MappedTest, AssertsCompiledSQL):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "t1",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", String(30)),
        )
        Table(
            "t2",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", String(30)),
        )

        Table(
            "t1t2_1",
            metadata,
            Column("t1id", Integer, ForeignKey("t1.id")),
            Column("t2id", Integer, ForeignKey("t2.id")),
        )

        Table(
            "t1t2_2",
            metadata,
            Column("t1id", Integer, ForeignKey("t1.id")),
            Column("t2id", Integer, ForeignKey("t2.id")),
        )

    def test_basic(self):
        t2, t1t2_1, t1t2_2, t1 = (
            self.tables.t2,
            self.tables.t1t2_1,
            self.tables.t1t2_2,
            self.tables.t1,
        )

        class T1(object):
            pass

        class T2(object):
            pass

        mapper(
            T1,
            t1,
            properties={
                "t2s_1": relationship(T2, secondary=t1t2_1),
                "t2s_2": relationship(T2, secondary=t1t2_2),
            },
        )
        mapper(T2, t2)

        with testing.expect_deprecated_20(join_strings_dep):
            q = (
                fixture_session()
                .query(T1)
                .join("t2s_1")
                .filter(t2.c.id == 5)
                .reset_joinpoint()
                .join("t2s_2")
            )
        self.assert_compile(
            q,
            "SELECT t1.id AS t1_id, t1.data AS t1_data FROM t1 "
            "JOIN t1t2_1 AS t1t2_1_1 "
            "ON t1.id = t1t2_1_1.t1id JOIN t2 ON t2.id = t1t2_1_1.t2id "
            "JOIN t1t2_2 AS t1t2_2_1 "
            "ON t1.id = t1t2_2_1.t1id JOIN t2 ON t2.id = t1t2_2_1.t2id "
            "WHERE t2.id = :id_1",
            use_default_dialect=True,
        )


class BindSensitiveStringifyTest(fixtures.TestBase):
    def _fixture(self):
        # building a totally separate metadata /mapping here
        # because we need to control if the MetaData is bound or not

        class User(object):
            pass

        m = MetaData()
        user_table = Table(
            "users",
            m,
            Column("id", Integer, primary_key=True),
            Column("name", String(50)),
        )

        mapper(User, user_table)
        return User

    def _dialect_fixture(self):
        class MyDialect(default.DefaultDialect):
            default_paramstyle = "qmark"

        from sqlalchemy.engine import base

        return base.Engine(mock.Mock(), MyDialect(), mock.Mock())

    def _test(self, bound_session, session_present, expect_bound):
        if bound_session:
            eng = self._dialect_fixture()
        else:
            eng = None

        User = self._fixture()

        s = Session(eng if bound_session else None)
        q = s.query(User).filter(User.id == 7)
        if not session_present:
            q = q.with_session(None)

        eq_ignore_whitespace(
            str(q),
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users WHERE users.id = ?"
            if expect_bound
            else "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users WHERE users.id = :id_1",
        )

    def test_query_bound_session(self):
        self._test(True, True, True)

    def test_query_no_session(self):
        self._test(False, False, False)

    def test_query_unbound_session(self):
        self._test(False, True, False)


class GetBindTest(_GetBindTest):
    @classmethod
    def define_tables(cls, metadata):
        super(GetBindTest, cls).define_tables(metadata)
        metadata.bind = testing.db

    def test_fallback_table_metadata(self):
        session = self._fixture({})
        is_(session.get_bind(self.classes.BaseClass), testing.db)

    def test_bind_base_table_concrete_sub_class(self):
        base_class_bind = Mock()
        session = self._fixture({self.tables.base_table: base_class_bind})

        is_(session.get_bind(self.classes.ConcreteSubClass), testing.db)
