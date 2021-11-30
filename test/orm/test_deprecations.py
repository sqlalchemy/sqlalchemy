import sqlalchemy as sa
from sqlalchemy import and_
from sqlalchemy import cast
from sqlalchemy import column
from sqlalchemy import desc
from sqlalchemy import event
from sqlalchemy import exc as sa_exc
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import inspect
from sqlalchemy import Integer
from sqlalchemy import join
from sqlalchemy import LABEL_STYLE_TABLENAME_PLUS_COL
from sqlalchemy import lateral
from sqlalchemy import literal_column
from sqlalchemy import MetaData
from sqlalchemy import or_
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import table
from sqlalchemy import testing
from sqlalchemy import text
from sqlalchemy import true
from sqlalchemy import util
from sqlalchemy.engine import default
from sqlalchemy.engine import result_tuple
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm import aliased
from sqlalchemy.orm import as_declarative
from sqlalchemy.orm import attributes
from sqlalchemy.orm import backref
from sqlalchemy.orm import clear_mappers
from sqlalchemy.orm import collections
from sqlalchemy.orm import column_property
from sqlalchemy.orm import configure_mappers
from sqlalchemy.orm import contains_alias
from sqlalchemy.orm import contains_eager
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import declared_attr
from sqlalchemy.orm import defaultload
from sqlalchemy.orm import defer
from sqlalchemy.orm import deferred
from sqlalchemy.orm import eagerload
from sqlalchemy.orm import exc as orm_exc
from sqlalchemy.orm import foreign
from sqlalchemy.orm import instrumentation
from sqlalchemy.orm import joinedload
from sqlalchemy.orm import Load
from sqlalchemy.orm import load_only
from sqlalchemy.orm import mapper
from sqlalchemy.orm import relation
from sqlalchemy.orm import relationship
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import selectinload
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import strategy_options
from sqlalchemy.orm import subqueryload
from sqlalchemy.orm import synonym
from sqlalchemy.orm import undefer
from sqlalchemy.orm import with_loader_criteria
from sqlalchemy.orm import with_parent
from sqlalchemy.orm import with_polymorphic
from sqlalchemy.orm.collections import attribute_mapped_collection
from sqlalchemy.orm.collections import collection
from sqlalchemy.orm.util import polymorphic_union
from sqlalchemy.sql import elements
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import assertions
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import eq_
from sqlalchemy.testing import eq_ignore_whitespace
from sqlalchemy.testing import expect_deprecated
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
from sqlalchemy.testing.util import resolve_lambda
from sqlalchemy.util import pickle
from . import _fixtures
from .inheritance import _poly_fixtures
from .inheritance._poly_fixtures import _Polymorphic
from .inheritance._poly_fixtures import Company
from .inheritance._poly_fixtures import Engineer
from .inheritance._poly_fixtures import Manager
from .inheritance._poly_fixtures import Person
from .test_ac_relationships import PartitionByFixture
from .test_bind import GetBindTest as _GetBindTest
from .test_default_strategies import (
    DefaultStrategyOptionsTest as _DefaultStrategyOptionsTest,
)
from .test_deferred import InheritanceTest as _deferred_InheritanceTest
from .test_dynamic import _DynamicFixture
from .test_events import _RemoveListeners
from .test_options import PathTest as OptionsPathTest
from .test_options import PathTest
from .test_options import QueryTest as OptionsQueryTest
from .test_query import QueryTest
from .test_transaction import _LocalFixture
from ..sql.test_compare import CacheKeyFixture


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

undefer_needs_chaining = (
    r"The \*addl_attrs on orm.(?:un)?defer is deprecated.  "
    "Please use method chaining"
)

join_strings_dep = "Using strings to indicate relationship names in Query.join"
join_tuple_form = (
    r"Query.join\(\) will no longer accept tuples as "
    "arguments in SQLAlchemy 2.0."
)

autocommit_dep = (
    "The Session.autocommit parameter is deprecated "
    "and will be removed in SQLAlchemy version 2.0."
)

subtransactions_dep = (
    "The Session.begin.subtransactions flag is deprecated "
    "and will be removed in SQLAlchemy version 2.0."
)
opt_strings_dep = (
    "Using strings to indicate column or relationship "
    "paths in loader options"
)

wparent_strings_dep = (
    r"Using strings to indicate relationship names "
    r"in the ORM with_parent\(\) function"
)

query_wparent_dep = (
    r"The Query.with_parent\(\) method is considered legacy as of the 1.x"
)

query_get_dep = r"The Query.get\(\) method is considered legacy as of the 1.x"

sef_dep = (
    r"The Query.select_entity_from\(\) method is considered "
    "legacy as of the 1.x"
)

with_polymorphic_dep = (
    r"The Query.with_polymorphic\(\) method is considered legacy as of "
    r"the 1.x series of SQLAlchemy and will be removed in 2.0"
)

merge_result_dep = (
    r"The merge_result\(\) function is considered legacy as of the 1.x series"
)

dep_exc_wildcard = (
    r"The undocumented `.{WILDCARD}` format is deprecated and will be removed "
    r"in a future version as it is believed to be unused. If you have been "
    r"using this functionality, please comment on Issue #4390 on the "
    r"SQLAlchemy project tracker."
)


def _aliased_join_warning(arg=None):
    return testing.expect_warnings(
        "An alias is being generated automatically against joined entity "
        "mapped class " + (arg if arg else "")
    )


def _aliased_join_deprecation(arg=None):
    return testing.expect_deprecated(
        "An alias is being generated automatically against joined entity "
        "mapped class " + (arg if arg else "")
    )


class GetTest(QueryTest):
    def test_get(self):
        User = self.classes.User

        s = fixture_session()
        with assertions.expect_deprecated_20(query_get_dep):
            assert s.query(User).get(19) is None
        with assertions.expect_deprecated_20(query_get_dep):
            u = s.query(User).get(7)
        with assertions.expect_deprecated_20(query_get_dep):
            u2 = s.query(User).get(7)
        assert u is u2
        s.expunge_all()
        with assertions.expect_deprecated_20(query_get_dep):
            u2 = s.query(User).get(7)
        assert u is not u2

    def test_loader_options(self):
        User = self.classes.User

        s = fixture_session()

        with assertions.expect_deprecated_20(query_get_dep):
            u1 = s.query(User).options(joinedload(User.addresses)).get(8)
        eq_(len(u1.__dict__["addresses"]), 3)

    def test_no_criterion_when_already_loaded(self):
        """test that get()/load() does not use preexisting filter/etc.
        criterion, even when we're only using the identity map."""

        User, Address = self.classes.User, self.classes.Address

        s = fixture_session()

        s.get(User, 7)

        q = s.query(User).join(User.addresses).filter(Address.user_id == 8)
        with assertions.expect_deprecated_20(query_get_dep):
            with assertions.expect_raises_message(
                sa_exc.InvalidRequestError,
                r"Query.get\(\) being called on a Query with existing "
                "criterion.",
            ):
                q.get(7)

    def test_no_criterion(self):
        """test that get()/load() does not use preexisting filter/etc.
        criterion"""

        User, Address = self.classes.User, self.classes.Address

        s = fixture_session()

        q = s.query(User).join(User.addresses).filter(Address.user_id == 8)

        with assertions.expect_deprecated_20(query_get_dep):
            with assertions.expect_raises_message(
                sa_exc.InvalidRequestError,
                r"Query.get\(\) being called on a Query with existing "
                "criterion.",
            ):
                q.get(7)

        with assertions.expect_deprecated_20(query_get_dep):
            with assertions.expect_raises_message(
                sa_exc.InvalidRequestError,
                r"Query.get\(\) being called on a Query with existing "
                "criterion.",
            ):
                s.query(User).filter(User.id == 7).get(19)

        # order_by()/get() doesn't raise
        with assertions.expect_deprecated_20(query_get_dep):
            s.query(User).order_by(User.id).get(8)

    def test_get_against_col(self):
        User = self.classes.User

        s = fixture_session()

        with assertions.expect_deprecated_20(query_get_dep):
            with assertions.expect_raises_message(
                sa_exc.InvalidRequestError,
                r"get\(\) can only be used against a single mapped class.",
            ):
                s.query(User.id).get(5)

    def test_only_full_mapper_zero(self):
        User, Address = self.classes.User, self.classes.Address

        s = fixture_session()
        q = s.query(User, Address)

        with assertions.expect_deprecated_20(query_get_dep):
            with assertions.expect_raises_message(
                sa_exc.InvalidRequestError,
                r"get\(\) can only be used against a single mapped class.",
            ):
                q.get(5)


class CustomJoinTest(QueryTest):
    run_setup_mappers = None

    def test_double_same_mappers_flag_alias(self):
        """test aliasing of joins with a custom join condition"""

        (
            addresses,
            items,
            order_items,
            orders,
            Item,
            User,
            Address,
            Order,
            users,
        ) = (
            self.tables.addresses,
            self.tables.items,
            self.tables.order_items,
            self.tables.orders,
            self.classes.Item,
            self.classes.User,
            self.classes.Address,
            self.classes.Order,
            self.tables.users,
        )

        self.mapper_registry.map_imperatively(Address, addresses)
        self.mapper_registry.map_imperatively(
            Order,
            orders,
            properties={
                "items": relationship(
                    Item,
                    secondary=order_items,
                    lazy="select",
                    order_by=items.c.id,
                )
            },
        )
        self.mapper_registry.map_imperatively(Item, items)
        self.mapper_registry.map_imperatively(
            User,
            users,
            properties=dict(
                addresses=relationship(Address, lazy="select"),
                open_orders=relationship(
                    Order,
                    primaryjoin=and_(
                        orders.c.isopen == 1, users.c.id == orders.c.user_id
                    ),
                    lazy="select",
                    viewonly=True,
                ),
                closed_orders=relationship(
                    Order,
                    primaryjoin=and_(
                        orders.c.isopen == 0, users.c.id == orders.c.user_id
                    ),
                    lazy="select",
                    viewonly=True,
                ),
            ),
        )
        q = fixture_session().query(User)

        with assertions.expect_deprecated_20(
            join_aliased_dep,
            join_strings_dep,
            join_chain_dep,
            raise_on_any_unexpected=True,
        ):
            eq_(
                q.join("open_orders", "items", aliased=True)
                .filter(Item.id == 4)
                .join("closed_orders", "items", aliased=True)
                .filter(Item.id == 3)
                .all(),
                [User(id=7)],
            )


class PickleTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "users",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("name", String(30), nullable=False),
            test_needs_acid=True,
            test_needs_fk=True,
        )

        Table(
            "addresses",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("user_id", None, ForeignKey("users.id")),
            Column("email_address", String(50), nullable=False),
            test_needs_acid=True,
            test_needs_fk=True,
        )
        Table(
            "orders",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("user_id", None, ForeignKey("users.id")),
            Column("address_id", None, ForeignKey("addresses.id")),
            Column("description", String(30)),
            Column("isopen", Integer),
            test_needs_acid=True,
            test_needs_fk=True,
        )
        Table(
            "dingalings",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("address_id", None, ForeignKey("addresses.id")),
            Column("data", String(30)),
            test_needs_acid=True,
            test_needs_fk=True,
        )

    def _option_test_fixture(self):
        users, addresses, dingalings = (
            self.tables.users,
            self.tables.addresses,
            self.tables.dingalings,
        )

        # these must be module level for pickling
        from .test_pickled import User, Address, Dingaling

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties={"addresses": relationship(Address, backref="user")},
        )
        self.mapper_registry.map_imperatively(
            Address,
            addresses,
            properties={"dingaling": relationship(Dingaling)},
        )
        self.mapper_registry.map_imperatively(Dingaling, dingalings)
        sess = fixture_session()
        u1 = User(name="ed")
        u1.addresses.append(Address(email_address="ed@bar.com"))
        sess.add(u1)
        sess.flush()
        sess.expunge_all()
        return sess, User, Address, Dingaling

    @testing.requires.non_broken_pickle
    def test_became_bound_options(self):
        sess, User, Address, Dingaling = self._option_test_fixture()

        for opt in [
            sa.orm.joinedload("addresses"),
            sa.orm.defer("name"),
            sa.orm.joinedload("addresses").joinedload(Address.dingaling),
        ]:
            with assertions.expect_deprecated_20(opt_strings_dep):
                context = sess.query(User).options(opt)._compile_context()
            opt = [
                v
                for v in context.attributes.values()
                if isinstance(v, sa.orm.Load)
            ][0]

            opt2 = pickle.loads(pickle.dumps(opt))
            eq_(opt.path, opt2.path)
            eq_(opt.local_opts, opt2.local_opts)

        u1 = sess.query(User).options(opt).first()
        pickle.loads(pickle.dumps(u1))

    @testing.requires.non_broken_pickle
    @testing.combinations(
        lambda: sa.orm.joinedload("addresses"),
        lambda: sa.orm.defer("name"),
        lambda Address: sa.orm.joinedload("addresses").joinedload(
            Address.dingaling
        ),
        lambda: sa.orm.joinedload("addresses").raiseload("*"),
    )
    def test_unbound_options(self, test_case):
        sess, User, Address, Dingaling = self._option_test_fixture()

        opt = testing.resolve_lambda(test_case, User=User, Address=Address)
        opt2 = pickle.loads(pickle.dumps(opt))
        eq_(opt.path, opt2.path)

        with assertions.expect_deprecated_20(opt_strings_dep):
            u1 = sess.query(User).options(opt).first()
        pickle.loads(pickle.dumps(u1))

    @testing.requires.non_broken_pickle
    @testing.combinations(
        lambda User: sa.orm.Load(User).joinedload("addresses"),
        lambda User: sa.orm.Load(User).joinedload("addresses").raiseload("*"),
        lambda User: sa.orm.Load(User).defer("name"),
        lambda User, Address: sa.orm.Load(User)
        .joinedload("addresses")
        .joinedload(Address.dingaling),
        lambda User, Address: sa.orm.Load(User)
        .joinedload("addresses", innerjoin=True)
        .joinedload(Address.dingaling),
    )
    def test_bound_options(self, test_case):
        sess, User, Address, Dingaling = self._option_test_fixture()

        with assertions.expect_deprecated_20(opt_strings_dep):
            opt = testing.resolve_lambda(test_case, User=User, Address=Address)

        opt2 = pickle.loads(pickle.dumps(opt))
        eq_(opt.path, opt2.path)
        eq_(opt.context.keys(), opt2.context.keys())
        eq_(opt.local_opts, opt2.local_opts)

        u1 = sess.query(User).options(opt).first()
        pickle.loads(pickle.dumps(u1))


class SynonymTest(QueryTest, AssertsCompiledSQL):
    __dialect__ = "default"

    @classmethod
    def setup_mappers(cls):
        (
            users,
            Keyword,
            items,
            order_items,
            orders,
            Item,
            User,
            Address,
            keywords,
            Order,
            item_keywords,
            addresses,
        ) = (
            cls.tables.users,
            cls.classes.Keyword,
            cls.tables.items,
            cls.tables.order_items,
            cls.tables.orders,
            cls.classes.Item,
            cls.classes.User,
            cls.classes.Address,
            cls.tables.keywords,
            cls.classes.Order,
            cls.tables.item_keywords,
            cls.tables.addresses,
        )

        cls.mapper_registry.map_imperatively(
            User,
            users,
            properties={
                "name_syn": synonym("name"),
                "addresses": relationship(Address),
                "orders": relationship(
                    Order, backref="user", order_by=orders.c.id
                ),  # o2m, m2o
                "orders_syn": synonym("orders"),
                "orders_syn_2": synonym("orders_syn"),
            },
        )
        cls.mapper_registry.map_imperatively(Address, addresses)
        cls.mapper_registry.map_imperatively(
            Order,
            orders,
            properties={
                "items": relationship(Item, secondary=order_items),  # m2m
                "address": relationship(Address),  # m2o
                "items_syn": synonym("items"),
            },
        )
        cls.mapper_registry.map_imperatively(
            Item,
            items,
            properties={
                "keywords": relationship(
                    Keyword, secondary=item_keywords
                )  # m2m
            },
        )
        cls.mapper_registry.map_imperatively(Keyword, keywords)

    def test_options_syn_of_syn_string(self):
        User, Order = self.classes.User, self.classes.Order

        s = fixture_session()

        def go():
            with testing.expect_deprecated_20(opt_strings_dep):
                result = (
                    s.query(User)
                    .filter_by(name="jack")
                    .options(joinedload("orders_syn_2"))
                    .all()
                )
            eq_(
                result,
                [
                    User(
                        id=7,
                        name="jack",
                        orders=[
                            Order(description="order 1"),
                            Order(description="order 3"),
                            Order(description="order 5"),
                        ],
                    )
                ],
            )

        self.assert_sql_count(testing.db, go, 1)


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

    def test_multiple_entities(self):
        User = self.classes.User
        Address = self.classes.Address
        Dingaling = self.classes.Dingaling

        s = fixture_session()

        u1 = aliased(User)

        with testing.expect_deprecated_20(join_chain_dep):
            q1 = s.query(u1).join(Address, User)

        self.assert_compile(
            q1,
            "SELECT users_1.id AS users_1_id, users_1.name AS users_1_name "
            "FROM users AS users_1 JOIN addresses ON users_1.id = "
            "addresses.user_id JOIN users ON users.id = addresses.user_id",
        )

        with testing.expect_deprecated_20(join_chain_dep):
            q1 = s.query(u1).join(Address, User, Dingaling)

        self.assert_compile(
            q1,
            "SELECT users_1.id AS users_1_id, users_1.name AS users_1_name "
            "FROM users AS users_1 JOIN addresses "
            "ON users_1.id = addresses.user_id "
            "JOIN users ON users.id = addresses.user_id "
            "JOIN dingalings ON addresses.id = dingalings.address_id",
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

        with testing.expect_deprecated_20(opt_strings_dep):
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
            users.select()
            .where(users.c.id == 7)
            .union(users.select().where(users.c.id > 7))
            .alias("ulist")
            .outerjoin(addresses)
            .select()
            .order_by(text("ulist.id"), addresses.c.id)
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
            users.select()
            .where(users.c.id == 7)
            .union(users.select().where(users.c.id > 7))
            .alias("ulist")
            .outerjoin(adalias)
            .select()
            .order_by(text("ulist.id"), adalias.c.id)
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

        sel = users.select().where(users.c.id.in_([7, 8]))
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
        sel = users.select().where(users.c.id.in_([7, 8]))

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

        sel = users.select().where(users.c.id.in_([7, 8]))
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

        sel = users.select().where(users.c.id.in_([7, 8]))
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

        with self._expect_implicit_subquery(), _aliased_join_warning():
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

        cls.mapper_registry.map_imperatively(
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

        self.mapper_registry.map_imperatively(
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

    def test_basic_filter_by(self):
        """test #7239"""

        User = self.classes.User

        s = fixture_session()

        with self._from_self_deprecated():
            q = s.query(User).from_self()

        self.assert_compile(
            q.filter_by(id=5),
            "SELECT anon_1.users_id AS anon_1_users_id, anon_1.users_name "
            "AS anon_1_users_name FROM (SELECT users.id AS users_id, "
            "users.name AS users_name FROM users) AS anon_1 "
            "WHERE anon_1.users_id = :id_1",
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

        with testing.expect_deprecated_20(subtransactions_dep):
            s1.begin(subtransactions=True)

        s1.close()

    def test_autocommit_deprecated(Self):
        with testing.expect_deprecated_20(autocommit_dep):
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


class TransScopingTest(_fixtures.FixtureTest):
    run_inserts = None
    __prefer_requires__ = ("independent_connections",)

    @testing.combinations((True,), (False,), argnames="begin")
    @testing.combinations((True,), (False,), argnames="expire_on_commit")
    @testing.combinations((True,), (False,), argnames="modify_unconditional")
    @testing.combinations(
        ("nothing",), ("modify",), ("add",), ("delete",), argnames="case_"
    )
    def test_autobegin_attr_change(
        self, case_, begin, modify_unconditional, expire_on_commit
    ):
        """test :ticket:`6360`"""

        autocommit = True
        User, users = self.classes.User, self.tables.users

        self.mapper_registry.map_imperatively(User, users)

        with testing.expect_deprecated_20(autocommit_dep):
            s = Session(
                testing.db,
                autocommit=autocommit,
                expire_on_commit=expire_on_commit,
            )

        u = User(name="x")
        u2 = User(name="d")
        u3 = User(name="e")
        s.add_all([u, u2, u3])

        if autocommit:
            s.flush()
        else:
            s.commit()

        if begin:
            s.begin()

        if case_ == "add":
            # this autobegins
            s.add(User(name="q"))
        elif case_ == "delete":
            # this autobegins
            s.delete(u2)
        elif case_ == "modify":
            # this autobegins
            u3.name = "m"

        if case_ == "nothing" and not begin:
            assert not s._transaction
            expect_expire = expire_on_commit
        elif autocommit and not begin:
            assert not s._transaction
            expect_expire = expire_on_commit
        else:
            assert s._transaction
            expect_expire = True

        if modify_unconditional:
            # this autobegins
            u.name = "y"
            expect_expire = True

        if not expect_expire:
            assert not s._transaction

        # test is that state is consistent after rollback()
        s.rollback()

        if autocommit and not begin and modify_unconditional:
            eq_(u.name, "y")
        else:
            if not expect_expire:
                assert "name" in u.__dict__
            else:
                assert "name" not in u.__dict__
            eq_(u.name, "x")

    def test_no_autoflush_or_commit_in_expire_w_autocommit(self):
        """test second part of :ticket:`6233`.

        Here we test that the "autoflush on unexpire" feature added
        in :ticket:`5226` is turned off for a legacy autocommit session.

        """

        with testing.expect_deprecated_20(autocommit_dep):
            s = Session(
                testing.db,
                autocommit=True,
                expire_on_commit=True,
                autoflush=True,
            )

        User, users = self.classes.User, self.tables.users

        self.mapper_registry.map_imperatively(User, users)

        u1 = User(name="u1")
        s.add(u1)
        s.flush()  # this commits

        u1.name = "u2"  # this does not commit

        assert "id" not in u1.__dict__
        u1.id  # this unexpires

        # never expired
        eq_(u1.__dict__["name"], "u2")

        eq_(u1.name, "u2")

        # still in dirty collection
        assert u1 in s.dirty


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

        cls.mapper_registry.map_imperatively(T1, t1)
        cls.mapper_registry.map_imperatively(T2, t2)

    def test_close_transaction_on_commit_fail(self):
        T2 = self.classes.T2

        with testing.expect_deprecated_20(autocommit_dep):
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

        # note python 2 does not allow parens here; reformat in py3 only
        with DeprecatedQueryTest._expect_implicit_subquery(), _aliased_join_warning(  # noqa E501
            "Person->people"
        ):
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

        self.mapper_registry.map_imperatively(
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

        m = self.mapper_registry.map_imperatively(User, users)

        m.add_property("_name", deferred(users.c.name))
        m.add_property("name", synonym("_name"))

        sess = fixture_session(autocommit=False)
        assert sess.get(User, 7)

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
        self.mapper_registry.map_imperatively(
            User,
            users,
            properties={
                "addresses": relationship(Address),
                "orders": relationship(Order),
            },
        )
        self.mapper_registry.map_imperatively(Address, addresses)
        self.mapper_registry.map_imperatively(
            Order,
            orders,
            properties={
                "items": relationship(Item, secondary=self.tables.order_items)
            },
        )
        self.mapper_registry.map_imperatively(
            Keyword,
            keywords,
            properties={
                "keywords": column_property(keywords.c.name + "some keyword")
            },
        )
        self.mapper_registry.map_imperatively(
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

        self.mapper_registry.map_imperatively(Address, addresses)
        self.mapper_registry.map_imperatively(
            User,
            users,
            properties={
                "addresses": relationship(
                    Address, lazy="selectin", order_by=addresses.c.id
                )
            },
        )

        sess = fixture_session()

        with testing.expect_deprecated(undefer_needs_chaining):
            sess.query(User).options(defer("addresses", "email_address"))

        with testing.expect_deprecated(undefer_needs_chaining):
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

        self.mapper_registry.map_imperatively(Address, addresses)

        self.mapper_registry.map_imperatively(Order, orders)

        with testing.expect_deprecated(
            "The mapper.non_primary parameter is deprecated"
        ):
            open_mapper = self.mapper_registry.map_imperatively(
                Order, openorders, non_primary=True
            )
            closed_mapper = self.mapper_registry.map_imperatively(
                Order, closedorders, non_primary=True
            )
        self.mapper_registry.map_imperatively(
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

        self.mapper_registry.map_imperatively(Address, addresses)
        self.mapper_registry.map_imperatively(Order, orders)

        with testing.expect_deprecated(
            "The mapper.non_primary parameter is deprecated"
        ):
            open_mapper = self.mapper_registry.map_imperatively(
                Order, openorders, non_primary=True
            )
            closed_mapper = self.mapper_registry.map_imperatively(
                Order, closedorders, non_primary=True
            )

        self.mapper_registry.map_imperatively(
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

        self.mapper_registry.map_imperatively(Address, addresses)
        self.mapper_registry.map_imperatively(Order, orders)

        with testing.expect_deprecated(
            "The mapper.non_primary parameter is deprecated"
        ):
            open_mapper = self.mapper_registry.map_imperatively(
                Order, openorders, non_primary=True
            )
            closed_mapper = self.mapper_registry.map_imperatively(
                Order, closedorders, non_primary=True
            )

        self.mapper_registry.map_imperatively(
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

        self.mapper_registry.map_imperatively(Address, addresses)
        self.mapper_registry.map_imperatively(Order, orders)

        with testing.expect_deprecated(
            "The mapper.non_primary parameter is deprecated"
        ):
            open_mapper = self.mapper_registry.map_imperatively(
                Order, openorders, non_primary=True
            )
            closed_mapper = self.mapper_registry.map_imperatively(
                Order, closedorders, non_primary=True
            )

        self.mapper_registry.map_imperatively(
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
        user = sess.get(User, 7)

        closed_mapper = User.closed_orders.entity
        open_mapper = User.open_orders.entity
        with testing.expect_deprecated_20(wparent_strings_dep):
            eq_(
                [Order(id=1), Order(id=5)],
                fixture_session()
                .query(closed_mapper)
                .with_parent(user, property="closed_orders")
                .all(),
            )
        with testing.expect_deprecated_20(wparent_strings_dep):
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

    def teardown_test(self):
        clear_mappers()

    def test_non_primary_identity_class(self):
        User = self.classes.User
        users, addresses = self.tables.users, self.tables.addresses

        class AddressUser(User):
            pass

        self.mapper_registry.map_imperatively(
            User, users, polymorphic_identity="user"
        )
        m2 = self.mapper_registry.map_imperatively(
            AddressUser,
            addresses,
            inherits=User,
            polymorphic_identity="address",
            properties={"address_id": addresses.c.id},
        )
        with testing.expect_deprecated(
            "The mapper.non_primary parameter is deprecated"
        ):
            m3 = self.mapper_registry.map_imperatively(
                AddressUser, addresses, non_primary=True
            )
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

        self.mapper_registry.map_imperatively(User, users)
        self.mapper_registry.map_imperatively(Address, addresses)
        with testing.expect_deprecated(
            "The mapper.non_primary parameter is deprecated"
        ):
            m = self.mapper_registry.map_imperatively(  # noqa F841
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

        assert_raises_message(
            sa.exc.InvalidRequestError,
            "Configure a primary mapper first",
            self.mapper_registry.map_imperatively,
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

        self.mapper_registry.map_imperatively(Base, users)
        assert_raises_message(
            sa.exc.InvalidRequestError,
            "Configure a primary mapper first",
            self.mapper_registry.map_imperatively,
            Sub,
            addresses,
            non_primary=True,
        )

    def test_illegal_non_primary_legacy(self):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        with testing.expect_deprecated(
            "Calling the mapper.* function directly outside of a declarative "
        ):
            mapper(User, users)
        with testing.expect_deprecated(
            "Calling the mapper.* function directly outside of a declarative "
        ):
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

    def test_illegal_non_primary_2_legacy(self):
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

    def test_illegal_non_primary_3_legacy(self):
        users, addresses = self.tables.users, self.tables.addresses

        class Base(object):
            pass

        class Sub(Base):
            pass

        with testing.expect_deprecated(
            "Calling the mapper.* function directly outside of a declarative "
        ):
            mapper(Base, users)
        with testing.expect_deprecated(
            "The mapper.non_primary parameter is deprecated",
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
            users.select()
            .where(users.c.id == 7)
            .union(users.select().where(users.c.id > 7))
            .alias("ulist")
            .outerjoin(addresses)
            .select()
            .order_by(text("ulist.id"), addresses.c.id)
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

        selectquery = (
            users.outerjoin(addresses)
            .select()
            .where(users.c.id < 10)
            .order_by(users.c.id, addresses.c.id)
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
        selectquery = (
            users.outerjoin(adalias)
            .select()
            .order_by(users.c.id, adalias.c.id)
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
        selectquery = (
            users.outerjoin(adalias)
            .select()
            .order_by(users.c.id, adalias.c.id)
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


class TextTest(QueryTest):
    def test_via_textasfrom_select_from(self):
        User = self.classes.User
        s = fixture_session()

        with assertions.expect_deprecated_20(sef_dep):
            eq_(
                s.query(User)
                .select_entity_from(
                    text("select * from users")
                    .columns(User.id, User.name)
                    .subquery()
                )
                .order_by(User.id)
                .all(),
                [User(id=7), User(id=8), User(id=9), User(id=10)],
            )


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


class AutoCommitTest(_LocalFixture):
    __backend__ = True

    def test_begin_nested_requires_trans(self):
        with assertions.expect_deprecated_20(autocommit_dep):
            sess = fixture_session(autocommit=True)
        assert_raises(sa_exc.InvalidRequestError, sess.begin_nested)

    def test_begin_preflush(self):
        User = self.classes.User
        with assertions.expect_deprecated_20(autocommit_dep):
            sess = fixture_session(autocommit=True)

        u1 = User(name="ed")
        sess.add(u1)

        sess.begin()
        u2 = User(name="some other user")
        sess.add(u2)
        sess.rollback()
        assert u2 not in sess
        assert u1 in sess
        assert sess.query(User).filter_by(name="ed").one() is u1

    def test_accounting_commit_fails_add(self):
        User = self.classes.User
        with assertions.expect_deprecated_20(autocommit_dep):
            sess = fixture_session(autocommit=True)

        fail = False

        def fail_fn(*arg, **kw):
            if fail:
                raise Exception("commit fails")

        event.listen(sess, "after_flush_postexec", fail_fn)
        u1 = User(name="ed")
        sess.add(u1)

        fail = True
        assert_raises(Exception, sess.flush)
        fail = False

        assert u1 not in sess
        u1new = User(id=2, name="fred")
        sess.add(u1new)
        sess.add(u1)
        sess.flush()
        assert u1 in sess
        eq_(
            sess.query(User.name).order_by(User.name).all(),
            [("ed",), ("fred",)],
        )

    def test_accounting_commit_fails_delete(self):
        User = self.classes.User
        with assertions.expect_deprecated_20(autocommit_dep):
            sess = fixture_session(autocommit=True)

        fail = False

        def fail_fn(*arg, **kw):
            if fail:
                raise Exception("commit fails")

        event.listen(sess, "after_flush_postexec", fail_fn)
        u1 = User(name="ed")
        sess.add(u1)
        sess.flush()

        sess.delete(u1)
        fail = True
        assert_raises(Exception, sess.flush)
        fail = False

        assert u1 in sess
        assert u1 not in sess.deleted
        sess.delete(u1)
        sess.flush()
        assert u1 not in sess
        eq_(sess.query(User.name).order_by(User.name).all(), [])

    @testing.requires.updateable_autoincrement_pks
    def test_accounting_no_select_needed(self):
        """test that flush accounting works on non-expired instances
        when autocommit=True/expire_on_commit=True."""

        User = self.classes.User
        with assertions.expect_deprecated_20(autocommit_dep):
            sess = fixture_session(autocommit=True, expire_on_commit=True)

        u1 = User(id=1, name="ed")
        sess.add(u1)
        sess.flush()

        u1.id = 3
        u1.name = "fred"
        self.assert_sql_count(testing.db, sess.flush, 1)
        assert "id" not in u1.__dict__
        eq_(u1.id, 3)


class SessionStateTest(_fixtures.FixtureTest):
    run_inserts = None

    __prefer_requires__ = ("independent_connections",)

    def test_autocommit_doesnt_raise_on_pending(self):
        User, users = self.classes.User, self.tables.users

        self.mapper_registry.map_imperatively(User, users)
        with assertions.expect_deprecated_20(autocommit_dep):
            session = Session(testing.db, autocommit=True)

        session.add(User(name="ed"))

        session.begin()
        session.flush()
        session.commit()


class SessionTransactionTest(fixtures.RemovesEvents, _fixtures.FixtureTest):
    run_inserts = None
    __backend__ = True

    @testing.fixture
    def conn(self):
        with testing.db.connect() as conn:
            yield conn

    @testing.fixture
    def future_conn(self):

        engine = Engine._future_facade(testing.db)
        with engine.connect() as conn:
            yield conn

    def test_deactive_status_check(self):
        sess = fixture_session()
        trans = sess.begin()

        with assertions.expect_deprecated_20(subtransactions_dep):
            trans2 = sess.begin(subtransactions=True)
        trans2.rollback()
        assert_raises_message(
            sa_exc.InvalidRequestError,
            "This session is in 'inactive' state, due to the SQL transaction "
            "being rolled back; no further SQL can be emitted within this "
            "transaction.",
            trans.commit,
        )

    def test_deactive_status_check_w_exception(self):
        sess = fixture_session()
        trans = sess.begin()
        with assertions.expect_deprecated_20(subtransactions_dep):
            trans2 = sess.begin(subtransactions=True)
        try:
            raise Exception("test")
        except Exception:
            trans2.rollback(_capture_exception=True)
        assert_raises_message(
            sa_exc.PendingRollbackError,
            r"This Session's transaction has been rolled back due to a "
            r"previous exception during flush. To begin a new transaction "
            r"with this Session, first issue Session.rollback\(\). "
            r"Original exception was: test",
            trans.commit,
        )

    def test_error_on_using_inactive_session_commands(self):
        users, User = self.tables.users, self.classes.User

        self.mapper_registry.map_imperatively(User, users)
        with assertions.expect_deprecated_20(autocommit_dep):
            sess = fixture_session(autocommit=True)
        sess.begin()
        with assertions.expect_deprecated_20(subtransactions_dep):
            sess.begin(subtransactions=True)
        sess.add(User(name="u1"))
        sess.flush()
        sess.rollback()
        with assertions.expect_deprecated_20(subtransactions_dep):
            assert_raises_message(
                sa_exc.InvalidRequestError,
                "This session is in 'inactive' state, due to the SQL "
                "transaction "
                "being rolled back; no further SQL can be emitted within this "
                "transaction.",
                sess.begin,
                subtransactions=True,
            )
        sess.close()

    def test_subtransaction_on_external_subtrans(self, conn):
        users, User = self.tables.users, self.classes.User

        self.mapper_registry.map_imperatively(User, users)

        trans = conn.begin()
        sess = Session(bind=conn, autocommit=False, autoflush=True)
        with assertions.expect_deprecated_20(subtransactions_dep):
            sess.begin(subtransactions=True)
        u = User(name="ed")
        sess.add(u)
        sess.flush()
        sess.commit()  # commit does nothing
        trans.rollback()  # rolls back
        assert len(sess.query(User).all()) == 0
        sess.close()

    def test_subtransaction_on_noautocommit(self):
        User, users = self.classes.User, self.tables.users

        self.mapper_registry.map_imperatively(User, users)
        sess = fixture_session(autocommit=False, autoflush=True)
        with assertions.expect_deprecated_20(subtransactions_dep):
            sess.begin(subtransactions=True)
        u = User(name="u1")
        sess.add(u)
        sess.flush()
        sess.commit()  # commit does nothing
        sess.rollback()  # rolls back
        assert len(sess.query(User).all()) == 0
        sess.close()

    @testing.requires.savepoints
    def test_heavy_nesting(self):
        users = self.tables.users

        session = fixture_session()
        session.begin()
        session.connection().execute(users.insert().values(name="user1"))
        with assertions.expect_deprecated_20(subtransactions_dep):
            session.begin(subtransactions=True)
        session.begin_nested()
        session.connection().execute(users.insert().values(name="user2"))
        assert (
            session.connection()
            .exec_driver_sql("select count(1) from users")
            .scalar()
            == 2
        )
        session.rollback()
        assert (
            session.connection()
            .exec_driver_sql("select count(1) from users")
            .scalar()
            == 1
        )
        session.connection().execute(users.insert().values(name="user3"))
        session.commit()
        assert (
            session.connection()
            .exec_driver_sql("select count(1) from users")
            .scalar()
            == 2
        )

    @testing.requires.savepoints
    def test_heavy_nesting_future(self):
        users = self.tables.users

        from sqlalchemy.future import Engine

        engine = Engine._future_facade(testing.db)
        with Session(engine, autocommit=False) as session:
            session.begin()
            session.connection().execute(users.insert().values(name="user1"))
            with assertions.expect_deprecated_20(subtransactions_dep):
                session.begin(subtransactions=True)
            session.begin_nested()
            session.connection().execute(users.insert().values(name="user2"))
            assert (
                session.connection()
                .exec_driver_sql("select count(1) from users")
                .scalar()
                == 2
            )
            session.rollback()
            assert (
                session.connection()
                .exec_driver_sql("select count(1) from users")
                .scalar()
                == 1
            )
            session.connection().execute(users.insert().values(name="user3"))
            session.commit()
            assert (
                session.connection()
                .exec_driver_sql("select count(1) from users")
                .scalar()
                == 2
            )

    @testing.requires.savepoints
    def test_mixed_transaction_control(self):
        users, User = self.tables.users, self.classes.User

        self.mapper_registry.map_imperatively(User, users)

        with assertions.expect_deprecated_20(autocommit_dep):
            sess = fixture_session(autocommit=True)

        sess.begin()
        sess.begin_nested()
        with assertions.expect_deprecated_20(subtransactions_dep):
            transaction = sess.begin(subtransactions=True)

        sess.add(User(name="u1"))

        transaction.commit()
        sess.commit()
        sess.commit()

        sess.close()

        eq_(len(sess.query(User).all()), 1)

        t1 = sess.begin()
        t2 = sess.begin_nested()

        sess.add(User(name="u2"))

        t2.commit()
        assert sess._legacy_transaction() is t1

        sess.close()

    @testing.requires.savepoints
    def test_nested_transaction_connection_add_autocommit(self):
        users, User = self.tables.users, self.classes.User

        self.mapper_registry.map_imperatively(User, users)

        with assertions.expect_deprecated_20(autocommit_dep):
            sess = fixture_session(autocommit=True)

        sess.begin()
        sess.begin_nested()

        u1 = User(name="u1")
        sess.add(u1)
        sess.flush()

        sess.rollback()

        u2 = User(name="u2")
        sess.add(u2)

        sess.commit()

        eq_(set(sess.query(User).all()), set([u2]))

        sess.begin()
        sess.begin_nested()

        u3 = User(name="u3")
        sess.add(u3)
        sess.commit()  # commit the nested transaction
        sess.rollback()

        eq_(set(sess.query(User).all()), set([u2]))

        sess.close()

    def test_active_flag_autocommit(self):
        with assertions.expect_deprecated_20(autocommit_dep):
            sess = Session(bind=testing.db, autocommit=True)
        assert not sess.is_active
        sess.begin()
        assert sess.is_active
        sess.rollback()
        assert not sess.is_active


class SessionEventsTest(_RemoveListeners, _fixtures.FixtureTest):
    run_inserts = None

    def _listener_fixture(self, **kw):
        canary = []

        def listener(name):
            def go(*arg, **kw):
                canary.append(name)

            return go

        sess = fixture_session(**kw)

        for evt in [
            "after_transaction_create",
            "after_transaction_end",
            "before_commit",
            "after_commit",
            "after_rollback",
            "after_soft_rollback",
            "before_flush",
            "after_flush",
            "after_flush_postexec",
            "after_begin",
            "before_attach",
            "after_attach",
            "after_bulk_update",
            "after_bulk_delete",
        ]:
            event.listen(sess, evt, listener(evt))

        return sess, canary

    def test_flush_autocommit_hook(self):
        User, users = self.classes.User, self.tables.users

        self.mapper_registry.map_imperatively(User, users)

        with assertions.expect_deprecated_20(autocommit_dep):
            sess, canary = self._listener_fixture(
                autoflush=False, autocommit=True, expire_on_commit=False
            )

        u = User(name="u1")
        sess.add(u)
        sess.flush()
        eq_(
            canary,
            [
                "before_attach",
                "after_attach",
                "before_flush",
                "after_transaction_create",
                "after_begin",
                "after_flush",
                "after_flush_postexec",
                "before_commit",
                "after_commit",
                "after_transaction_end",
            ],
        )

    def test_on_bulk_update_hook(self):
        User, users = self.classes.User, self.tables.users

        sess = fixture_session()
        canary = Mock()

        event.listen(sess, "after_bulk_update", canary.after_bulk_update)

        def legacy(ses, qry, ctx, res):
            canary.after_bulk_update_legacy(ses, qry, ctx, res)

        event.listen(sess, "after_bulk_update", legacy)

        self.mapper_registry.map_imperatively(User, users)

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

        self.mapper_registry.map_imperatively(User, users)

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

        cls.mapper_registry.map_imperatively(Address, addresses)

        cls.mapper_registry.map_imperatively(
            User, users, properties=dict(addresses=relationship(Address))
        )

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

        sel = users.select().where(User.id.in_([7, 8])).alias()
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

        sel = users.select().where(User.id.in_([7, 8])).alias()
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

    @testing.combinations(
        "string_relationship",
        "string_relationship_only",
    )
    def test_filter_by_from_join(self, onclause_type):
        User, Address = self.classes("User", "Address")
        (address_table,) = self.tables("addresses")
        (user_table,) = self.tables("users")

        sess = fixture_session()
        q = sess.query(User)

        with assertions.expect_deprecated_20(join_strings_dep):
            if onclause_type == "string_relationship":
                q = q.join(Address, "addresses")
            elif onclause_type == "string_relationship_only":
                q = q.join("addresses")
            else:
                assert False

        q2 = q.filter_by(email_address="foo")

        self.assert_compile(
            q2,
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users JOIN addresses ON users.id = addresses.user_id "
            "WHERE addresses.email_address = :email_address_1",
        )

        q2 = q.reset_joinpoint().filter_by(name="user")
        self.assert_compile(
            q2,
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users JOIN addresses ON users.id = addresses.user_id "
            "WHERE users.name = :name_1",
        )

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

    def test_does_filter_aliasing_work(self):
        User, Address = self.classes("User", "Address")

        s = fixture_session()

        # aliased=True is to be deprecated, other filter lambdas
        # that go into effect include polymorphic filtering.
        with testing.expect_deprecated(join_aliased_dep):
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

        with _aliased_join_warning():
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

        with _aliased_join_warning():
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

        cls.mapper_registry.map_imperatively(
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


class InheritedJoinTest(
    fixtures.NoCache,
    _poly_fixtures._Polymorphic,
    _poly_fixtures._PolymorphicFixtureBase,
    AssertsCompiledSQL,
):
    run_setup_mappers = "once"
    __dialect__ = "default"

    def test_load_only_alias_subclass(self):
        Manager = self.classes.Manager

        s = fixture_session()
        m1 = aliased(Manager, flat=True)
        q = (
            s.query(m1)
            .order_by(m1.person_id)
            .options(load_only("status", "manager_name"))
        )
        with assertions.expect_deprecated_20(opt_strings_dep):
            self.assert_compile(
                q,
                "SELECT managers_1.person_id AS managers_1_person_id, "
                "people_1.person_id AS people_1_person_id, "
                "people_1.type AS people_1_type, "
                "managers_1.status AS managers_1_status, "
                "managers_1.manager_name AS managers_1_manager_name "
                "FROM people AS people_1 JOIN managers AS "
                "managers_1 ON people_1.person_id = managers_1.person_id "
                "ORDER BY managers_1.person_id",
            )

    def test_load_only_alias_subclass_bound(self):
        Manager = self.classes.Manager

        s = fixture_session()
        m1 = aliased(Manager, flat=True)
        with assertions.expect_deprecated_20(opt_strings_dep):
            q = (
                s.query(m1)
                .order_by(m1.person_id)
                .options(Load(m1).load_only("status", "manager_name"))
            )
        self.assert_compile(
            q,
            "SELECT managers_1.person_id AS managers_1_person_id, "
            "people_1.person_id AS people_1_person_id, "
            "people_1.type AS people_1_type, "
            "managers_1.status AS managers_1_status, "
            "managers_1.manager_name AS managers_1_manager_name "
            "FROM people AS people_1 JOIN managers AS "
            "managers_1 ON people_1.person_id = managers_1.person_id "
            "ORDER BY managers_1.person_id",
        )

    def test_load_only_of_type_with_polymorphic(self):
        Company, Person, Manager = self.classes("Company", "Person", "Manager")
        s = fixture_session()

        wp = with_polymorphic(Person, [Manager], flat=True)

        # needs to be explicit, we don't currently dig onto all the
        # sub-entities in the wp
        with assertions.expect_deprecated_20(opt_strings_dep):
            assert_raises_message(
                sa.exc.ArgumentError,
                r'Can\'t find property named "status" on '
                r"with_polymorphic\(Person, \[Manager\]\) in this Query.",
                s.query(Company)
                .options(
                    joinedload(Company.employees.of_type(wp)).load_only(
                        "status"
                    )
                )
                ._compile_context,
            )

    def test_join_to_selectable(self):
        people, Company, engineers, Engineer = (
            self.tables.people,
            self.classes.Company,
            self.tables.engineers,
            self.classes.Engineer,
        )

        sess = fixture_session()

        with _aliased_join_deprecation():
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

        with testing.expect_deprecated_20(
            join_aliased_dep
        ), _aliased_join_deprecation():
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

    def test_join_to_subclass_selectable_auto_alias(self):
        Company, Engineer = self.classes("Company", "Engineer")
        people, engineers = self.tables("people", "engineers")

        sess = fixture_session()

        with _aliased_join_deprecation():
            eq_(
                sess.query(Company)
                .join(people.join(engineers), "employees")
                .filter(Engineer.primary_language == "java")
                .all(),
                [self.c1],
            )

        # occurs for 2.0 style query also
        with _aliased_join_deprecation():
            stmt = (
                select(Company)
                .join(people.join(engineers), Company.employees)
                .filter(Engineer.primary_language == "java")
            )
            results = sess.scalars(stmt)
            eq_(results.all(), [self.c1])

    def test_join_to_subclass_two(self):
        Company, Engineer = self.classes("Company", "Engineer")
        people, engineers = self.tables("people", "engineers")

        sess = fixture_session()

        with _aliased_join_deprecation():
            eq_(
                sess.query(Company)
                .join(people.join(engineers), "employees")
                .filter(Engineer.primary_language == "java")
                .all(),
                [self.c1],
            )

    def test_join_to_subclass_six_selectable_auto_alias(self):
        Company, Engineer = self.classes("Company", "Engineer")
        people, engineers = self.tables("people", "engineers")

        sess = fixture_session()

        with _aliased_join_deprecation():
            eq_(
                sess.query(Company)
                .join(people.join(engineers), "employees")
                .join(Engineer.machines)
                .all(),
                [self.c1, self.c2],
            )

    def test_join_to_subclass_six_point_five_selectable_auto_alias(self):
        Company, Engineer = self.classes("Company", "Engineer")
        people, engineers = self.tables("people", "engineers")

        sess = fixture_session()

        with _aliased_join_deprecation():
            eq_(
                sess.query(Company)
                .join(people.join(engineers), "employees")
                .join(Engineer.machines)
                .filter(Engineer.name == "dilbert")
                .all(),
                [self.c1],
            )

    def test_join_to_subclass_seven_selectable_auto_alias(self):
        Company, Engineer, Machine = self.classes(
            "Company", "Engineer", "Machine"
        )
        people, engineers = self.tables("people", "engineers")

        sess = fixture_session()

        with _aliased_join_deprecation():
            eq_(
                sess.query(Company)
                .join(people.join(engineers), "employees")
                .join(Engineer.machines)
                .filter(Machine.name.ilike("%thinkpad%"))
                .all(),
                [self.c1],
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
        class T1(cls.Comparable):
            pass

        class T2(cls.Comparable):
            pass

    @classmethod
    def setup_mappers(cls):
        T1, T2 = cls.classes("T1", "T2")
        table1, table2 = cls.tables.table1, cls.tables.table2
        cls.mapper_registry.map_imperatively(T1, table1)
        cls.mapper_registry.map_imperatively(T2, table2)

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

        self.mapper_registry.map_imperatively(
            T1,
            t1,
            properties={
                "t2s_1": relationship(T2, secondary=t1t2_1),
                "t2s_2": relationship(T2, secondary=t1t2_2),
            },
        )
        self.mapper_registry.map_imperatively(T2, t2)

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


class BindSensitiveStringifyTest(fixtures.MappedTest):
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

        clear_mappers()
        self.mapper_registry.map_imperatively(User, user_table)
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
        with testing.expect_deprecated_20(
            "This Session located a target engine via bound metadata"
        ):
            is_(session.get_bind(self.classes.BaseClass), testing.db)

    def test_bind_base_table_concrete_sub_class(self):
        base_class_bind = Mock()
        session = self._fixture({self.tables.base_table: base_class_bind})

        with testing.expect_deprecated_20(
            "This Session located a target engine via bound metadata"
        ):
            is_(session.get_bind(self.classes.ConcreteSubClass), testing.db)


class DeprecationScopedSessionTest(fixtures.MappedTest):
    def test_config_errors(self):
        sm = sessionmaker()

        def go():
            s = sm()
            s._is_asyncio = True
            return s

        Session = scoped_session(go)

        with expect_deprecated(
            "Using `scoped_session` with asyncio is deprecated and "
            "will raise an error in a future version. "
            "Please use `async_scoped_session` instead."
        ):
            Session()
        Session.remove()


@testing.combinations(
    (
        "inline",
        True,
    ),
    (
        "separate",
        False,
    ),
    argnames="inline",
    id_="sa",
)
@testing.combinations(
    (
        "string",
        True,
    ),
    (
        "literal",
        False,
    ),
    argnames="stringbased",
    id_="sa",
)
class ExplicitJoinTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        global User, Address
        Base = declarative_base(metadata=metadata)

        class User(Base, fixtures.ComparableEntity):

            __tablename__ = "users"
            id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )
            name = Column(String(50))

        class Address(Base, fixtures.ComparableEntity):

            __tablename__ = "addresses"
            id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )
            email = Column(String(50))
            user_id = Column(Integer, ForeignKey("users.id"))
            if cls.inline:
                if cls.stringbased:
                    user = relationship(
                        "User",
                        primaryjoin="User.id==Address.user_id",
                        backref="addresses",
                    )
                else:
                    user = relationship(
                        User,
                        primaryjoin=User.id == user_id,
                        backref="addresses",
                    )

        if not cls.inline:
            configure_mappers()
            if cls.stringbased:
                Address.user = relationship(
                    "User",
                    primaryjoin="User.id==Address.user_id",
                    backref="addresses",
                )
            else:
                Address.user = relationship(
                    User,
                    primaryjoin=User.id == Address.user_id,
                    backref="addresses",
                )

    @classmethod
    def insert_data(cls, connection):
        params = [
            dict(list(zip(("id", "name"), column_values)))
            for column_values in [
                (7, "jack"),
                (8, "ed"),
                (9, "fred"),
                (10, "chuck"),
            ]
        ]

        connection.execute(User.__table__.insert(), params)
        connection.execute(
            Address.__table__.insert(),
            [
                dict(list(zip(("id", "user_id", "email"), column_values)))
                for column_values in [
                    (1, 7, "jack@bean.com"),
                    (2, 8, "ed@wood.com"),
                    (3, 8, "ed@bettyboop.com"),
                    (4, 8, "ed@lala.com"),
                    (5, 9, "fred@fred.com"),
                ]
            ],
        )

    def test_aliased_join(self):

        # this query will screw up if the aliasing enabled in
        # query.join() gets applied to the right half of the join
        # condition inside the any(). the join condition inside of
        # any() comes from the "primaryjoin" of the relationship,
        # and should not be annotated with _orm_adapt.
        # PropertyLoader.Comparator will annotate the left side with
        # _orm_adapt, though.

        sess = fixture_session()

        with testing.expect_deprecated_20(join_aliased_dep):
            eq_(
                sess.query(User)
                .join(User.addresses, aliased=True)
                .filter(Address.email == "ed@wood.com")
                .filter(User.addresses.any(Address.email == "jack@bean.com"))
                .all(),
                [],
            )


class RequirementsTest(fixtures.MappedTest):

    """Tests the contract for user classes."""

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "ht1",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("value", String(10)),
        )
        Table(
            "ht2",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("ht1_id", Integer, ForeignKey("ht1.id")),
            Column("value", String(10)),
        )
        Table(
            "ht3",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("value", String(10)),
        )
        Table(
            "ht4",
            metadata,
            Column("ht1_id", Integer, ForeignKey("ht1.id"), primary_key=True),
            Column("ht3_id", Integer, ForeignKey("ht3.id"), primary_key=True),
        )
        Table(
            "ht5",
            metadata,
            Column("ht1_id", Integer, ForeignKey("ht1.id"), primary_key=True),
        )
        Table(
            "ht6",
            metadata,
            Column("ht1a_id", Integer, ForeignKey("ht1.id"), primary_key=True),
            Column("ht1b_id", Integer, ForeignKey("ht1.id"), primary_key=True),
            Column("value", String(10)),
        )

    if util.py2k:

        def test_baseclass_map_imperatively(self):
            ht1 = self.tables.ht1

            class OldStyle:
                pass

            assert_raises(
                sa.exc.ArgumentError,
                self.mapper_registry.map_imperatively,
                OldStyle,
                ht1,
            )

            assert_raises(
                sa.exc.ArgumentError,
                self.mapper_registry.map_imperatively,
                123,
            )

        def test_baseclass_legacy_mapper(self):
            ht1 = self.tables.ht1

            class OldStyle:
                pass

            assert_raises(
                sa.exc.ArgumentError,
                mapper,
                OldStyle,
                ht1,
            )

            assert_raises(
                sa.exc.ArgumentError,
                mapper,
                123,
            )

            class NoWeakrefSupport(str):
                pass

            # TODO: is weakref support detectable without an instance?
            # self.assertRaises(
            #  sa.exc.ArgumentError, mapper, NoWeakrefSupport, t2)


class DeferredOptionsTest(AssertsCompiledSQL, _fixtures.FixtureTest):
    __dialect__ = "default"

    def test_load_only_synonym(self):
        orders, Order = self.tables.orders, self.classes.Order

        self.mapper_registry.map_imperatively(
            Order,
            orders,
            properties={"desc": synonym("description")},
        )

        opt = load_only("isopen", "desc")

        sess = fixture_session()
        q = sess.query(Order).options(opt)
        with assertions.expect_deprecated_20(opt_strings_dep):
            self.assert_compile(
                q,
                "SELECT orders.id AS orders_id, orders.description "
                "AS orders_description, orders.isopen AS orders_isopen "
                "FROM orders",
            )

    def test_deep_options(self):
        users, items, order_items, Order, Item, User, orders = (
            self.tables.users,
            self.tables.items,
            self.tables.order_items,
            self.classes.Order,
            self.classes.Item,
            self.classes.User,
            self.tables.orders,
        )

        self.mapper_registry.map_imperatively(
            Item,
            items,
            properties=dict(description=deferred(items.c.description)),
        )
        self.mapper_registry.map_imperatively(
            Order,
            orders,
            properties=dict(items=relationship(Item, secondary=order_items)),
        )
        self.mapper_registry.map_imperatively(
            User,
            users,
            properties=dict(orders=relationship(Order, order_by=orders.c.id)),
        )

        sess = fixture_session()
        q = sess.query(User).order_by(User.id)
        result = q.all()
        item = result[0].orders[1].items[1]

        def go():
            eq_(item.description, "item 4")

        self.sql_count_(1, go)
        eq_(item.description, "item 4")

        sess.expunge_all()
        with assertions.expect_deprecated(undefer_needs_chaining):
            result = q.options(
                undefer(User.orders, Order.items, Item.description)
            ).all()
        item = result[0].orders[1].items[1]

        def go():
            eq_(item.description, "item 4")

        self.sql_count_(0, go)
        eq_(item.description, "item 4")


class OptionsTest(PathTest, OptionsQueryTest):
    def _option_fixture(self, *arg):
        return strategy_options._UnboundLoad._from_keys(
            strategy_options._UnboundLoad.joinedload, arg, True, {}
        )

    def test_chained(self):
        User = self.classes.User
        Order = self.classes.Order
        sess = fixture_session()
        q = sess.query(User)
        opt = self._option_fixture(User.orders).joinedload("items")
        with assertions.expect_deprecated_20(opt_strings_dep):
            self._assert_path_result(
                opt, q, [(User, "orders"), (User, "orders", Order, "items")]
            )

    def test_chained_plus_dotted(self):
        User = self.classes.User
        Order = self.classes.Order
        Item = self.classes.Item
        sess = fixture_session()
        q = sess.query(User)
        opt = self._option_fixture("orders.items").joinedload("keywords")
        with assertions.expect_deprecated_20(opt_strings_dep):
            self._assert_path_result(
                opt,
                q,
                [
                    (User, "orders"),
                    (User, "orders", Order, "items"),
                    (User, "orders", Order, "items", Item, "keywords"),
                ],
            )

    def test_with_current_matching_string(self):
        Item, User, Order = (
            self.classes.Item,
            self.classes.User,
            self.classes.Order,
        )

        sess = fixture_session()
        q = sess.query(Item)._with_current_path(
            self._make_path_registry([User, "orders", Order, "items"])
        )

        opt = self._option_fixture("orders.items.keywords")
        with assertions.expect_deprecated_20(opt_strings_dep):
            self._assert_path_result(opt, q, [(Item, "keywords")])

    def test_with_current_nonmatching_string(self):
        Item, User, Order = (
            self.classes.Item,
            self.classes.User,
            self.classes.Order,
        )

        sess = fixture_session()
        q = sess.query(Item)._with_current_path(
            self._make_path_registry([User, "orders", Order, "items"])
        )

        opt = self._option_fixture("keywords")
        self._assert_path_result(opt, q, [])

        opt = self._option_fixture("items.keywords")
        self._assert_path_result(opt, q, [])

    def test_path_multilevel_string(self):
        Item, User, Order = (
            self.classes.Item,
            self.classes.User,
            self.classes.Order,
        )

        sess = fixture_session()
        q = sess.query(User)

        opt = self._option_fixture("orders.items.keywords")
        with assertions.expect_deprecated_20(opt_strings_dep):
            self._assert_path_result(
                opt,
                q,
                [
                    (User, "orders"),
                    (User, "orders", Order, "items"),
                    (User, "orders", Order, "items", Item, "keywords"),
                ],
            )

    def test_chained_plus_multi(self):
        User = self.classes.User
        Order = self.classes.Order
        Item = self.classes.Item
        sess = fixture_session()
        q = sess.query(User)
        opt = self._option_fixture(User.orders, Order.items).joinedload(
            "keywords"
        )
        with assertions.expect_deprecated_20(opt_strings_dep):
            self._assert_path_result(
                opt,
                q,
                [
                    (User, "orders"),
                    (User, "orders", Order, "items"),
                    (User, "orders", Order, "items", Item, "keywords"),
                ],
            )

    def test_multi_entity_opt_on_string(self):
        Item = self.classes.Item
        Order = self.classes.Order
        opt = self._option_fixture("items")
        sess = fixture_session()
        q = sess.query(Item, Order)
        with assertions.expect_deprecated_20(opt_strings_dep):
            self._assert_path_result(opt, q, [])

    def test_get_path_one_level_string(self):
        User = self.classes.User

        sess = fixture_session()
        q = sess.query(User)

        opt = self._option_fixture("addresses")
        with assertions.expect_deprecated_20(opt_strings_dep):
            self._assert_path_result(opt, q, [(User, "addresses")])

    def test_get_path_one_level_with_unrelated(self):
        Order = self.classes.Order

        sess = fixture_session()
        q = sess.query(Order)
        opt = self._option_fixture("addresses")
        with assertions.expect_deprecated_20(opt_strings_dep):
            self._assert_path_result(opt, q, [])


class SubOptionsTest(PathTest, OptionsQueryTest):
    run_create_tables = False
    run_inserts = None
    run_deletes = None

    def _assert_opts(self, q, sub_opt, non_sub_opts):
        attr_a = {}

        for val in sub_opt._to_bind:
            val._bind_loader(
                [
                    ent.entity_zero
                    for ent in q._compile_state()._lead_mapper_entities
                ],
                q._compile_options._current_path,
                attr_a,
                False,
            )

        attr_b = {}

        for opt in non_sub_opts:
            for val in opt._to_bind:
                val._bind_loader(
                    [
                        ent.entity_zero
                        for ent in q._compile_state()._lead_mapper_entities
                    ],
                    q._compile_options._current_path,
                    attr_b,
                    False,
                )

        for k, l in attr_b.items():
            if not l.strategy:
                del attr_b[k]

        def strat_as_tuple(strat):
            return (
                strat.strategy,
                strat.local_opts,
                strat.propagate_to_loaders,
                strat._of_type,
                strat.is_class_strategy,
                strat.is_opts_only,
            )

        eq_(
            {path: strat_as_tuple(load) for path, load in attr_a.items()},
            {path: strat_as_tuple(load) for path, load in attr_b.items()},
        )

    def test_invalid_two(self):
        User, Address, Order, Item, SubItem = self.classes(
            "User", "Address", "Order", "Item", "SubItem"
        )

        # these options are "invalid", in that User.orders -> Item.keywords
        # is not a path.  However, the "normal" option is not generating
        # an error for now, which is bad, but we're testing here only that
        # it works the same way, so there you go.   If and when we make this
        # case raise, then both cases should raise in the same way.
        sub_opt = joinedload("orders").options(
            joinedload("keywords"), joinedload("items")
        )
        non_sub_opts = [
            joinedload(User.orders).joinedload(Item.keywords),
            defaultload(User.orders).joinedload(Order.items),
        ]
        sess = fixture_session()
        with assertions.expect_deprecated_20(opt_strings_dep):
            self._assert_opts(sess.query(User), sub_opt, non_sub_opts)

    def test_four_strings(self):
        User, Address, Order, Item, SubItem, Keyword = self.classes(
            "User", "Address", "Order", "Item", "SubItem", "Keyword"
        )
        sub_opt = joinedload("orders").options(
            defer("description"),
            joinedload("items").options(
                joinedload("keywords").options(defer("name")),
                defer("description"),
            ),
        )
        non_sub_opts = [
            joinedload(User.orders),
            defaultload(User.orders).defer(Order.description),
            defaultload(User.orders).joinedload(Order.items),
            defaultload(User.orders)
            .defaultload(Order.items)
            .joinedload(Item.keywords),
            defaultload(User.orders)
            .defaultload(Order.items)
            .defer(Item.description),
            defaultload(User.orders)
            .defaultload(Order.items)
            .defaultload(Item.keywords)
            .defer(Keyword.name),
        ]
        sess = fixture_session()
        with assertions.expect_deprecated_20(opt_strings_dep):
            self._assert_opts(sess.query(User), sub_opt, non_sub_opts)

    def test_five_strings(self):
        User, Address, Order, Item, SubItem, Keyword = self.classes(
            "User", "Address", "Order", "Item", "SubItem", "Keyword"
        )
        sub_opt = joinedload("orders").options(load_only("description"))
        non_sub_opts = [
            joinedload(User.orders),
            defaultload(User.orders).load_only(Order.description),
        ]
        sess = fixture_session()
        with assertions.expect_deprecated_20(opt_strings_dep):
            self._assert_opts(sess.query(User), sub_opt, non_sub_opts)


class OptionsNoPropTest(_fixtures.FixtureTest):
    """test the error messages emitted when using property
    options in conjunction with column-only entities, or
    for not existing options

    """

    run_create_tables = False
    run_inserts = None
    run_deletes = None

    def test_option_with_column_basestring(self):
        Item = self.classes.Item

        message = (
            "Query has only expression-based entities - can't "
            'find property named "keywords".'
        )
        self._assert_eager_with_just_column_exception(
            Item.id, "keywords", message
        )

    def test_option_against_nonexistent_basestring(self):
        Item = self.classes.Item
        with assertions.expect_deprecated_20(opt_strings_dep):
            self._assert_eager_with_entity_exception(
                [Item],
                (joinedload("foo"),),
                'Can\'t find property named "foo" on mapped class '
                "Item->items in this Query.",
            )

    def test_option_against_nonexistent_twolevel_basestring(self):
        Item = self.classes.Item
        with assertions.expect_deprecated_20(opt_strings_dep):
            self._assert_eager_with_entity_exception(
                [Item],
                (joinedload("keywords.foo"),),
                'Can\'t find property named "foo" on mapped class '
                "Keyword->keywords in this Query.",
            )

    def test_option_against_nonexistent_twolevel_chained(self):
        Item = self.classes.Item
        with assertions.expect_deprecated_20(opt_strings_dep):
            self._assert_eager_with_entity_exception(
                [Item],
                (joinedload("keywords").joinedload("foo"),),
                'Can\'t find property named "foo" on mapped class '
                "Keyword->keywords in this Query.",
            )

    @testing.fails_if(
        lambda: True,
        "PropertyOption doesn't yet check for relation/column on end result",
    )
    def test_option_against_non_relation_basestring(self):
        Item = self.classes.Item
        Keyword = self.classes.Keyword
        self._assert_eager_with_entity_exception(
            [Keyword, Item],
            (joinedload("keywords"),),
            r"Attribute 'keywords' of entity 'Mapper\|Keyword\|keywords' "
            "does not refer to a mapped entity",
        )

    @testing.fails_if(
        lambda: True,
        "PropertyOption doesn't yet check for relation/column on end result",
    )
    def test_option_against_multi_non_relation_basestring(self):
        Item = self.classes.Item
        Keyword = self.classes.Keyword
        self._assert_eager_with_entity_exception(
            [Keyword, Item],
            (joinedload("keywords"),),
            r"Attribute 'keywords' of entity 'Mapper\|Keyword\|keywords' "
            "does not refer to a mapped entity",
        )

    def test_option_against_wrong_entity_type_basestring(self):
        Item = self.classes.Item
        with assertions.expect_deprecated_20(opt_strings_dep):
            self._assert_loader_strategy_exception(
                [Item],
                (joinedload("id").joinedload("keywords"),),
                'Can\'t apply "joined loader" strategy to property "Item.id", '
                'which is a "column property"; this loader strategy is '
                'intended to be used with a "relationship property".',
            )

    def test_col_option_against_relationship_basestring(self):
        Item = self.classes.Item
        with assertions.expect_deprecated_20(opt_strings_dep):
            self._assert_loader_strategy_exception(
                [Item],
                (load_only("keywords"),),
                'Can\'t apply "column loader" strategy to property '
                '"Item.keywords", which is a "relationship property"; this '
                "loader strategy is intended to be used with a "
                '"column property".',
            )

    def test_option_against_multi_non_relation_twolevel_basestring(self):
        Item = self.classes.Item
        Keyword = self.classes.Keyword
        with assertions.expect_deprecated_20(opt_strings_dep):
            self._assert_loader_strategy_exception(
                [Keyword, Item],
                (joinedload("id").joinedload("keywords"),),
                'Can\'t apply "joined loader" strategy to property '
                '"Keyword.id", '
                'which is a "column property"; this loader strategy is '
                "intended "
                'to be used with a "relationship property".',
            )

    def test_option_against_multi_nonexistent_basestring(self):
        Item = self.classes.Item
        Keyword = self.classes.Keyword
        with assertions.expect_deprecated_20(opt_strings_dep):
            self._assert_eager_with_entity_exception(
                [Keyword, Item],
                (joinedload("description"),),
                'Can\'t find property named "description" on mapped class '
                "Keyword->keywords in this Query.",
            )

    def test_option_against_multi_no_entities_basestring(self):
        Item = self.classes.Item
        Keyword = self.classes.Keyword
        self._assert_eager_with_entity_exception(
            [Keyword.id, Item.id],
            (joinedload("keywords"),),
            r"Query has only expression-based entities - can't find property "
            'named "keywords".',
        )

    def test_option_with_mapper_then_column_basestring(self):
        Item = self.classes.Item

        with assertions.expect_deprecated_20(opt_strings_dep):
            self._assert_option([Item, Item.id], "keywords")

    def test_option_with_mapper_basestring(self):
        Item = self.classes.Item

        with assertions.expect_deprecated_20(opt_strings_dep):
            self._assert_option([Item], "keywords")

    def test_option_with_column_then_mapper_basestring(self):
        Item = self.classes.Item

        with assertions.expect_deprecated_20(opt_strings_dep):
            self._assert_option([Item.id, Item], "keywords")

    @classmethod
    def setup_mappers(cls):
        users, User, addresses, Address, orders, Order = (
            cls.tables.users,
            cls.classes.User,
            cls.tables.addresses,
            cls.classes.Address,
            cls.tables.orders,
            cls.classes.Order,
        )
        cls.mapper_registry.map_imperatively(
            User,
            users,
            properties={
                "addresses": relationship(Address),
                "orders": relationship(Order),
            },
        )
        cls.mapper_registry.map_imperatively(Address, addresses)
        cls.mapper_registry.map_imperatively(Order, orders)
        keywords, items, item_keywords, Keyword, Item = (
            cls.tables.keywords,
            cls.tables.items,
            cls.tables.item_keywords,
            cls.classes.Keyword,
            cls.classes.Item,
        )
        cls.mapper_registry.map_imperatively(
            Keyword,
            keywords,
            properties={
                "keywords": column_property(keywords.c.name + "some keyword")
            },
        )
        cls.mapper_registry.map_imperatively(
            Item,
            items,
            properties=dict(
                keywords=relationship(Keyword, secondary=item_keywords)
            ),
        )

        class OrderWProp(cls.classes.Order):
            @property
            def some_attr(self):
                return "hi"

        cls.mapper_registry.map_imperatively(
            OrderWProp, None, inherits=cls.classes.Order
        )

    def _assert_option(self, entity_list, option):
        Item = self.classes.Item

        context = (
            fixture_session()
            .query(*entity_list)
            .options(joinedload(option))
            ._compile_state()
        )
        key = ("loader", (inspect(Item), inspect(Item).attrs.keywords))
        assert key in context.attributes

    def _assert_loader_strategy_exception(self, entity_list, options, message):
        assert_raises_message(
            orm_exc.LoaderStrategyException,
            message,
            fixture_session()
            .query(*entity_list)
            .options(*options)
            ._compile_state,
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
            ._compile_state,
        )

    def _assert_eager_with_just_column_exception(
        self, column, eager_option, message
    ):
        assert_raises_message(
            sa.exc.ArgumentError,
            message,
            fixture_session()
            .query(column)
            .options(joinedload(eager_option))
            ._compile_state,
        )


class OptionsNoPropTestInh(_Polymorphic):
    def test_missing_str_attr_of_type_subclass(self):
        s = fixture_session()

        with assertions.expect_deprecated_20(opt_strings_dep):
            assert_raises_message(
                sa.exc.ArgumentError,
                r'Can\'t find property named "manager_name" on '
                r"mapped class Engineer->engineers in this Query.$",
                s.query(Company)
                .options(
                    joinedload(Company.employees.of_type(Engineer)).load_only(
                        "manager_name"
                    )
                )
                ._compile_state,
            )


class CacheKeyTest(CacheKeyFixture, _fixtures.FixtureTest):
    """In these tests we've moved / adapted all the tests from
    test_cache_key that make use of string options or string join().  Because
    we are ensuring cache keys are distinct we still keep a lot of the
    non-deprecated cases in the lists that we are testing.

    """

    run_setup_mappers = "once"
    run_inserts = None
    run_deletes = None

    @classmethod
    def setup_mappers(cls):
        cls._setup_stock_mapping()

    def _stmt_20(self, *elements):
        return tuple(
            elem._statement_20() if isinstance(elem, sa.orm.Query) else elem
            for elem in elements
        )

    def _deprecated_opt(self, fn):
        with assertions.expect_deprecated_20(
            opt_strings_dep, raise_on_any_unexpected=True
        ):
            return fn()

    def _deprecated_join(self, fn):
        with assertions.expect_deprecated_20(
            join_strings_dep, raise_on_any_unexpected=True
        ):
            return fn()

    def _deprecated_join_w_aliased(self, fn):
        with assertions.expect_deprecated_20(
            join_strings_dep, join_aliased_dep, raise_on_any_unexpected=True
        ):
            return fn()

    def test_bound_options(self):
        User, Address, Keyword, Order, Item = self.classes(
            "User", "Address", "Keyword", "Order", "Item"
        )

        self._run_cache_key_fixture(
            lambda: (
                Load(User).joinedload(User.addresses),
                Load(User).joinedload(
                    User.addresses.of_type(aliased(Address))
                ),
                Load(User).joinedload(User.orders),
                self._deprecated_opt(
                    lambda: Load(User).subqueryload("addresses")
                ),
                self._deprecated_opt(lambda: Load(Address).defer("id")),
                Load(Address).defer("*"),
                self._deprecated_opt(
                    lambda: Load(aliased(Address)).defer("id")
                ),
                Load(User).joinedload(User.addresses).defer(Address.id),
                Load(User).joinedload(User.orders).joinedload(Order.items),
                Load(User).joinedload(User.orders).subqueryload(Order.items),
                Load(User).subqueryload(User.orders).subqueryload(Order.items),
                Load(Address).raiseload("*"),
                self._deprecated_opt(lambda: Load(Address).raiseload("user")),
            ),
            compare_values=True,
        )

    def test_bound_options_equiv_on_strname(self):
        """Bound loader options resolve on string name so test that the cache
        key for the string version matches the resolved version.

        """
        User, Address, Keyword, Order, Item = self.classes(
            "User", "Address", "Keyword", "Order", "Item"
        )

        for left, right in [
            (
                Load(User).defer(User.id),
                self._deprecated_opt(lambda: Load(User).defer("id")),
            ),
            (
                Load(User).joinedload(User.addresses),
                self._deprecated_opt(
                    lambda: Load(User).joinedload("addresses")
                ),
            ),
            (
                Load(User).joinedload(User.orders).joinedload(Order.items),
                self._deprecated_opt(
                    lambda: Load(User).joinedload("orders").joinedload("items")
                ),
            ),
        ]:
            eq_(left._generate_cache_key(), right._generate_cache_key())

    def test_orm_query_w_orm_joins(self):

        User, Address, Keyword, Order, Item = self.classes(
            "User", "Address", "Keyword", "Order", "Item"
        )

        a1 = aliased(Address)

        self._run_cache_key_fixture(
            lambda: self._stmt_20(
                fixture_session().query(User).join(User.addresses),
                fixture_session().query(User).join(User.orders),
                fixture_session()
                .query(User)
                .join(User.addresses)
                .join(User.orders),
                self._deprecated_join_w_aliased(
                    lambda: fixture_session()
                    .query(User)
                    .join("addresses")
                    .join("dingalings", from_joinpoint=True)
                ),
                self._deprecated_join(
                    lambda: fixture_session().query(User).join("addresses")
                ),
                self._deprecated_join(
                    lambda: fixture_session().query(User).join("orders")
                ),
                self._deprecated_join(
                    lambda: fixture_session()
                    .query(User)
                    .join("addresses")
                    .join("orders")
                ),
                fixture_session().query(User).join(Address, User.addresses),
                self._deprecated_join(
                    lambda: fixture_session().query(User).join(a1, "addresses")
                ),
                self._deprecated_join_w_aliased(
                    lambda: fixture_session()
                    .query(User)
                    .join(a1, "addresses", aliased=True)
                ),
                fixture_session().query(User).join(User.addresses.of_type(a1)),
            ),
            compare_values=True,
        )

    def test_unbound_options(self):
        User, Address, Keyword, Order, Item = self.classes(
            "User", "Address", "Keyword", "Order", "Item"
        )

        # unbound options dont emit a deprecation warning during cache
        # key generation
        self._run_cache_key_fixture(
            lambda: (
                joinedload(User.addresses),
                joinedload(User.addresses.of_type(aliased(Address))),
                joinedload("addresses"),
                joinedload(User.orders),
                joinedload(User.orders.and_(Order.id != 5)),
                joinedload(User.orders).selectinload("items"),
                joinedload(User.orders).selectinload(Order.items),
                defer(User.id),
                defer("id"),
                defer("*"),
                defer(Address.id),
                joinedload(User.addresses).defer(Address.id),
                joinedload(User.addresses).defer("id"),
                subqueryload(User.orders)
                .subqueryload(Order.items)
                .defer(Item.description),
                defaultload(User.orders).defaultload(Order.items),
                defaultload(User.orders),
            ),
            compare_values=True,
        )

    def test_unbound_sub_options(self):
        """test #6869"""

        User, Address, Keyword, Order, Item = self.classes(
            "User", "Address", "Keyword", "Order", "Item"
        )

        self._run_cache_key_fixture(
            lambda: (
                joinedload(User.addresses).options(
                    joinedload(Address.dingaling)
                ),
                joinedload(User.addresses).options(
                    joinedload(Address.dingaling).options(load_only("name"))
                ),
                joinedload(User.orders).options(
                    joinedload(Order.items).options(joinedload(Item.keywords))
                ),
            ),
            compare_values=True,
        )


class PolyCacheKeyTest(CacheKeyFixture, _poly_fixtures._Polymorphic):
    run_setup_mappers = "once"
    run_inserts = None
    run_deletes = None

    def _stmt_20(self, *elements):
        return tuple(
            elem._statement_20() if isinstance(elem, sa.orm.Query) else elem
            for elem in elements
        )

    def test_wp_queries(self):
        Person, Manager, Engineer, Boss = self.classes(
            "Person", "Manager", "Engineer", "Boss"
        )

        def one():
            with assertions.expect_deprecated_20(w_polymorphic_dep):
                return (
                    fixture_session()
                    .query(Person)
                    .with_polymorphic([Manager, Engineer])
                )

        def two():
            wp = with_polymorphic(Person, [Manager, Engineer])

            return fixture_session().query(wp)

        def three():
            wp = with_polymorphic(Person, [Manager, Engineer])

            return fixture_session().query(wp).filter(wp.name == "asdfo")

        def three_a():
            wp = with_polymorphic(Person, [Manager, Engineer], flat=True)

            return fixture_session().query(wp).filter(wp.name == "asdfo")

        def four():
            with assertions.expect_deprecated_20(w_polymorphic_dep):
                return (
                    fixture_session()
                    .query(Person)
                    .with_polymorphic([Manager, Engineer])
                    .filter(Person.name == "asdf")
                )

        def five():
            subq = (
                select(Person)
                .outerjoin(Manager)
                .outerjoin(Engineer)
                .subquery()
            )
            wp = with_polymorphic(Person, [Manager, Engineer], subq)

            return fixture_session().query(wp).filter(wp.name == "asdfo")

        def six():
            subq = (
                select(Person)
                .outerjoin(Manager)
                .outerjoin(Engineer)
                .subquery()
            )

            with assertions.expect_deprecated_20(w_polymorphic_dep):
                return (
                    fixture_session()
                    .query(Person)
                    .with_polymorphic([Manager, Engineer], subq)
                    .filter(Person.name == "asdfo")
                )

        self._run_cache_key_fixture(
            lambda: self._stmt_20(
                one(), two(), three(), three_a(), four(), five(), six()
            ),
            compare_values=True,
        )


class AliasedClassRelationshipTest(
    PartitionByFixture, testing.AssertsCompiledSQL
):
    __requires__ = ("window_functions",)
    __dialect__ = "default"

    def test_selectinload_w_joinedload_after(self):
        """test has been enhanced to also test #7224"""

        A, B, C = self.classes("A", "B", "C")

        s = Session(testing.db)

        opt = selectinload(A.partitioned_bs).joinedload("cs")

        def go():
            for a1 in s.query(A).options(opt):
                for b in a1.partitioned_bs:
                    eq_(len(b.cs), 2)

        with assertions.expect_deprecated_20(opt_strings_dep):
            self.assert_sql_count(testing.db, go, 2)


class ColumnAccessTest(QueryTest, AssertsCompiledSQL):
    """test access of columns after _from_selectable has been applied"""

    __dialect__ = "default"

    def test_select_entity_from(self):
        User = self.classes.User
        sess = fixture_session()

        q = sess.query(User)
        with assertions.expect_deprecated_20(sef_dep):
            q = sess.query(User).select_entity_from(q.statement.subquery())
        self.assert_compile(
            q.filter(User.name == "ed"),
            "SELECT anon_1.id AS anon_1_id, anon_1.name AS anon_1_name "
            "FROM (SELECT users.id AS id, users.name AS name FROM "
            "users) AS anon_1 WHERE anon_1.name = :name_1",
        )

    def test_select_entity_from_no_entities(self):
        User = self.classes.User
        sess = fixture_session()

        with assertions.expect_deprecated_20(sef_dep):
            assert_raises_message(
                sa.exc.ArgumentError,
                r"A selectable \(FromClause\) instance is "
                "expected when the base alias is being set",
                sess.query(User).select_entity_from(User)._compile_context,
            )


class SelectFromTest(QueryTest, AssertsCompiledSQL):
    run_setup_mappers = None
    __dialect__ = "default"

    def test_aliased_class_vs_nonaliased(self):
        User, users = self.classes.User, self.tables.users
        self.mapper_registry.map_imperatively(User, users)

        sess = fixture_session()
        with assertions.expect_deprecated_20(sef_dep):
            self.assert_compile(
                sess.query(User.name).select_entity_from(
                    users.select().where(users.c.id > 5).subquery()
                ),
                "SELECT anon_1.name AS anon_1_name FROM "
                "(SELECT users.id AS id, "
                "users.name AS name FROM users WHERE users.id > :id_1) "
                "AS anon_1",
            )

    @testing.combinations(
        (
            lambda users: users.select().where(users.c.id.in_([7, 8])),
            "SELECT anon_1.id AS anon_1_id, anon_1.name AS anon_1_name "
            "FROM (SELECT users.id AS id, users.name AS name "
            "FROM users WHERE users.id IN (__[POSTCOMPILE_id_1])) AS anon_1 "
            "WHERE anon_1.name = :name_1",
        ),
        (
            lambda users: users.select()
            .where(users.c.id.in_([7, 8]))
            .set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL),
            "SELECT anon_1.users_id AS anon_1_users_id, anon_1.users_name "
            "AS anon_1_users_name FROM (SELECT users.id AS users_id, "
            "users.name AS users_name FROM users "
            "WHERE users.id IN (__[POSTCOMPILE_id_1])) AS anon_1 "
            "WHERE anon_1.users_name = :name_1",
        ),
        (
            lambda User, sess: sess.query(User).where(User.id.in_([7, 8])),
            "SELECT anon_1.id AS anon_1_id, anon_1.name AS anon_1_name "
            "FROM (SELECT users.id AS id, users.name AS name "
            "FROM users WHERE users.id IN (__[POSTCOMPILE_id_1])) AS anon_1 "
            "WHERE anon_1.name = :name_1",
        ),
    )
    def test_filter_by(self, query_fn, expected):
        """test #7239"""

        User = self.classes.User
        sess = fixture_session()

        User, users = self.classes.User, self.tables.users

        self.mapper_registry.map_imperatively(User, users)

        sel = resolve_lambda(query_fn, User=User, users=users, sess=sess)

        sess = fixture_session()

        with assertions.expect_deprecated_20(sef_dep):
            q = sess.query(User).select_entity_from(sel.subquery())

        self.assert_compile(q.filter_by(name="ed"), expected)
        eq_(q.filter_by(name="ed").all(), [User(name="ed")])

    def test_join_no_order_by(self):
        User, users = self.classes.User, self.tables.users

        self.mapper_registry.map_imperatively(User, users)

        sel = users.select().where(users.c.id.in_([7, 8]))
        sess = fixture_session()

        with assertions.expect_deprecated_20(sef_dep):
            eq_(
                sess.query(User).select_entity_from(sel.subquery()).all(),
                [User(name="jack", id=7), User(name="ed", id=8)],
            )

    def test_join(self):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        self.mapper_registry.map_imperatively(
            User, users, properties={"addresses": relationship(Address)}
        )
        self.mapper_registry.map_imperatively(Address, addresses)

        sel = users.select().where(users.c.id.in_([7, 8]))
        sess = fixture_session()

        with assertions.expect_deprecated_20(sef_dep):
            eq_(
                sess.query(User)
                .select_entity_from(sel.subquery())
                .join("addresses")
                .add_entity(Address)
                .order_by(User.id)
                .order_by(Address.id)
                .all(),
                [
                    (
                        User(name="jack", id=7),
                        Address(
                            user_id=7, email_address="jack@bean.com", id=1
                        ),
                    ),
                    (
                        User(name="ed", id=8),
                        Address(user_id=8, email_address="ed@wood.com", id=2),
                    ),
                    (
                        User(name="ed", id=8),
                        Address(
                            user_id=8, email_address="ed@bettyboop.com", id=3
                        ),
                    ),
                    (
                        User(name="ed", id=8),
                        Address(user_id=8, email_address="ed@lala.com", id=4),
                    ),
                ],
            )

        adalias = aliased(Address)
        with assertions.expect_deprecated_20(sef_dep):
            eq_(
                sess.query(User)
                .select_entity_from(sel.subquery())
                .join(adalias, "addresses")
                .add_entity(adalias)
                .order_by(User.id)
                .order_by(adalias.id)
                .all(),
                [
                    (
                        User(name="jack", id=7),
                        Address(
                            user_id=7, email_address="jack@bean.com", id=1
                        ),
                    ),
                    (
                        User(name="ed", id=8),
                        Address(user_id=8, email_address="ed@wood.com", id=2),
                    ),
                    (
                        User(name="ed", id=8),
                        Address(
                            user_id=8, email_address="ed@bettyboop.com", id=3
                        ),
                    ),
                    (
                        User(name="ed", id=8),
                        Address(user_id=8, email_address="ed@lala.com", id=4),
                    ),
                ],
            )

    def test_more_joins(self):
        (
            users,
            Keyword,
            orders,
            items,
            order_items,
            Order,
            Item,
            User,
            keywords,
            item_keywords,
        ) = (
            self.tables.users,
            self.classes.Keyword,
            self.tables.orders,
            self.tables.items,
            self.tables.order_items,
            self.classes.Order,
            self.classes.Item,
            self.classes.User,
            self.tables.keywords,
            self.tables.item_keywords,
        )

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties={"orders": relationship(Order, backref="user")},
        )  # o2m, m2o
        self.mapper_registry.map_imperatively(
            Order,
            orders,
            properties={
                "items": relationship(
                    Item, secondary=order_items, order_by=items.c.id
                )
            },
        )  # m2m

        self.mapper_registry.map_imperatively(
            Item,
            items,
            properties={
                "keywords": relationship(
                    Keyword, secondary=item_keywords, order_by=keywords.c.id
                )
            },
        )  # m2m
        self.mapper_registry.map_imperatively(Keyword, keywords)

        sess = fixture_session()
        sel = users.select().where(users.c.id.in_([7, 8]))

        with assertions.expect_deprecated_20(sef_dep):
            eq_(
                sess.query(User)
                .select_entity_from(sel.subquery())
                .join(User.orders, Order.items, Item.keywords)
                .filter(Keyword.name.in_(["red", "big", "round"]))
                .all(),
                [User(name="jack", id=7)],
            )

    def test_very_nested_joins_with_joinedload(self):
        (
            users,
            Keyword,
            orders,
            items,
            order_items,
            Order,
            Item,
            User,
            keywords,
            item_keywords,
        ) = (
            self.tables.users,
            self.classes.Keyword,
            self.tables.orders,
            self.tables.items,
            self.tables.order_items,
            self.classes.Order,
            self.classes.Item,
            self.classes.User,
            self.tables.keywords,
            self.tables.item_keywords,
        )

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties={"orders": relationship(Order, backref="user")},
        )  # o2m, m2o
        self.mapper_registry.map_imperatively(
            Order,
            orders,
            properties={
                "items": relationship(
                    Item, secondary=order_items, order_by=items.c.id
                )
            },
        )  # m2m
        self.mapper_registry.map_imperatively(
            Item,
            items,
            properties={
                "keywords": relationship(
                    Keyword, secondary=item_keywords, order_by=keywords.c.id
                )
            },
        )  # m2m
        self.mapper_registry.map_imperatively(Keyword, keywords)

        sess = fixture_session()

        sel = users.select().where(users.c.id.in_([7, 8]))

        def go():
            with assertions.expect_deprecated_20(sef_dep):
                eq_(
                    sess.query(User)
                    .select_entity_from(sel.subquery())
                    .options(
                        joinedload("orders")
                        .joinedload("items")
                        .joinedload("keywords")
                    )
                    .join(User.orders, Order.items, Item.keywords)
                    .filter(Keyword.name.in_(["red", "big", "round"]))
                    .all(),
                    [
                        User(
                            name="jack",
                            orders=[
                                Order(
                                    description="order 1",
                                    items=[
                                        Item(
                                            description="item 1",
                                            keywords=[
                                                Keyword(name="red"),
                                                Keyword(name="big"),
                                                Keyword(name="round"),
                                            ],
                                        ),
                                        Item(
                                            description="item 2",
                                            keywords=[
                                                Keyword(name="red", id=2),
                                                Keyword(name="small", id=5),
                                                Keyword(name="square"),
                                            ],
                                        ),
                                        Item(
                                            description="item 3",
                                            keywords=[
                                                Keyword(name="green", id=3),
                                                Keyword(name="big", id=4),
                                                Keyword(name="round", id=6),
                                            ],
                                        ),
                                    ],
                                ),
                                Order(
                                    description="order 3",
                                    items=[
                                        Item(
                                            description="item 3",
                                            keywords=[
                                                Keyword(name="green", id=3),
                                                Keyword(name="big", id=4),
                                                Keyword(name="round", id=6),
                                            ],
                                        ),
                                        Item(
                                            description="item 4",
                                            keywords=[],
                                            id=4,
                                        ),
                                        Item(
                                            description="item 5",
                                            keywords=[],
                                            id=5,
                                        ),
                                    ],
                                ),
                                Order(
                                    description="order 5",
                                    items=[
                                        Item(description="item 5", keywords=[])
                                    ],
                                ),
                            ],
                        )
                    ],
                )

        self.assert_sql_count(testing.db, go, 1)

        sess.expunge_all()
        sel2 = orders.select().where(orders.c.id.in_([1, 2, 3]))
        with assertions.expect_deprecated_20(sef_dep):
            eq_(
                sess.query(Order)
                .select_entity_from(sel2.subquery())
                .join(Order.items)
                .join(Item.keywords)
                .filter(Keyword.name == "red")
                .order_by(Order.id)
                .all(),
                [
                    Order(description="order 1", id=1),
                    Order(description="order 2", id=2),
                ],
            )

    def test_replace_with_eager(self):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties={
                "addresses": relationship(Address, order_by=addresses.c.id)
            },
        )
        self.mapper_registry.map_imperatively(Address, addresses)

        sel = users.select().where(users.c.id.in_([7, 8]))
        sess = fixture_session()

        def go():
            with assertions.expect_deprecated_20(sef_dep):
                eq_(
                    sess.query(User)
                    .options(joinedload("addresses"))
                    .select_entity_from(sel.subquery())
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
            with assertions.expect_deprecated_20(sef_dep):
                eq_(
                    sess.query(User)
                    .options(joinedload("addresses"))
                    .select_entity_from(sel.subquery())
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
            with assertions.expect_deprecated_20(sef_dep):
                eq_(
                    sess.query(User)
                    .options(joinedload("addresses"))
                    .select_entity_from(sel.subquery())
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

    def test_select_from_aliased_one(self):
        User, users = self.classes.User, self.tables.users

        self.mapper_registry.map_imperatively(User, users)

        sess = fixture_session()

        not_users = table("users", column("id"), column("name"))
        ua = aliased(User, select(not_users).alias(), adapt_on_names=True)

        with assertions.expect_deprecated_20(sef_dep):
            q = (
                sess.query(User.name)
                .select_entity_from(ua)
                .order_by(User.name)
            )
        self.assert_compile(
            q,
            "SELECT anon_1.name AS anon_1_name FROM (SELECT users.id AS id, "
            "users.name AS name FROM users) AS anon_1 ORDER BY anon_1.name",
        )
        eq_(q.all(), [("chuck",), ("ed",), ("fred",), ("jack",)])

    def test_select_from_aliased_two(self):
        User, users = self.classes.User, self.tables.users

        self.mapper_registry.map_imperatively(User, users)

        sess = fixture_session()

        ua = aliased(User)

        with assertions.expect_deprecated_20(sef_dep):
            q = (
                sess.query(User.name)
                .select_entity_from(ua)
                .order_by(User.name)
            )
        self.assert_compile(
            q,
            "SELECT users_1.name AS users_1_name FROM users AS users_1 "
            "ORDER BY users_1.name",
        )
        eq_(q.all(), [("chuck",), ("ed",), ("fred",), ("jack",)])

    def test_select_from_core_alias_one(self):
        User, users = self.classes.User, self.tables.users

        self.mapper_registry.map_imperatively(User, users)

        sess = fixture_session()

        ua = users.alias()

        with assertions.expect_deprecated_20(sef_dep):
            q = (
                sess.query(User.name)
                .select_entity_from(ua)
                .order_by(User.name)
            )
        self.assert_compile(
            q,
            "SELECT users_1.name AS users_1_name FROM users AS users_1 "
            "ORDER BY users_1.name",
        )
        eq_(q.all(), [("chuck",), ("ed",), ("fred",), ("jack",)])

    def test_differentiate_self_external(self):
        """test some different combinations of joining a table to a subquery of
        itself."""

        users, User = self.tables.users, self.classes.User

        self.mapper_registry.map_imperatively(User, users)

        sess = fixture_session()

        sel = sess.query(User).filter(User.id.in_([7, 8])).subquery()
        ualias = aliased(User)

        with assertions.expect_deprecated_20(sef_dep):
            self.assert_compile(
                sess.query(ualias)
                .select_entity_from(sel)
                .filter(ualias.id > sel.c.id),
                "SELECT users_1.id AS users_1_id, "
                "users_1.name AS users_1_name "
                "FROM users AS users_1, ("
                "SELECT users.id AS id, users.name AS name FROM users "
                "WHERE users.id IN (__[POSTCOMPILE_id_1])) AS anon_1 "
                "WHERE users_1.id > anon_1.id",
                check_post_param={"id_1": [7, 8]},
            )

        with assertions.expect_deprecated_20(sef_dep):
            self.assert_compile(
                sess.query(ualias)
                .select_entity_from(sel)
                .join(ualias, ualias.id > sel.c.id),
                "SELECT users_1.id AS users_1_id, "
                "users_1.name AS users_1_name "
                "FROM (SELECT users.id AS id, users.name AS name "
                "FROM users WHERE users.id IN "
                "(__[POSTCOMPILE_id_1])) AS anon_1 "
                "JOIN users AS users_1 ON users_1.id > anon_1.id",
                check_post_param={"id_1": [7, 8]},
            )

        with assertions.expect_deprecated_20(sef_dep):
            self.assert_compile(
                sess.query(ualias)
                .select_entity_from(sel)
                .join(ualias, ualias.id > User.id),
                "SELECT users_1.id AS users_1_id, "
                "users_1.name AS users_1_name "
                "FROM (SELECT users.id AS id, users.name AS name FROM "
                "users WHERE users.id IN (__[POSTCOMPILE_id_1])) AS anon_1 "
                "JOIN users AS users_1 ON users_1.id > anon_1.id",
                check_post_param={"id_1": [7, 8]},
            )

        with assertions.expect_deprecated_20(sef_dep):
            self.assert_compile(
                sess.query(ualias).select_entity_from(
                    join(sel, ualias, ualias.id > sel.c.id)
                ),
                "SELECT users_1.id AS users_1_id, "
                "users_1.name AS users_1_name "
                "FROM "
                "(SELECT users.id AS id, users.name AS name "
                "FROM users WHERE users.id "
                "IN (__[POSTCOMPILE_id_1])) AS anon_1 "
                "JOIN users AS users_1 ON users_1.id > anon_1.id",
                check_post_param={"id_1": [7, 8]},
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
        class Person(cls.Comparable):
            pass

        class Bookcase(cls.Comparable):
            pass

        class Book(cls.Comparable):
            pass

    @classmethod
    def setup_mappers(cls):
        Person, Bookcase, Book = cls.classes("Person", "Bookcase", "Book")
        people, bookcases, books = cls.tables("people", "bookcases", "books")
        cls.mapper_registry.map_imperatively(Person, people)
        cls.mapper_registry.map_imperatively(
            Bookcase,
            bookcases,
            properties={
                "owner": relationship(Person),
                "books": relationship(Book),
            },
        )
        cls.mapper_registry.map_imperatively(Book, books)

    # "sef" == "select entity from"
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

        with assertions.expect_deprecated_20(sef_dep):
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

        with assertions.expect_deprecated_20(sef_dep):
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

        with assertions.expect_deprecated_20(sef_dep):
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

        with assertions.expect_deprecated_20(sef_dep):
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

    def test_from_function_sef(self):
        Bookcase = self.classes.Bookcase

        s = fixture_session()

        subq = s.query(Bookcase).subquery()

        srf = lateral(func.generate_series(1, Bookcase.bookcase_shelves))

        with assertions.expect_deprecated_20(sef_dep):
            q = s.query(Bookcase).select_entity_from(subq).join(srf, true())

        self.assert_compile(
            q,
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


class ParentTest(QueryTest, AssertsCompiledSQL):
    __dialect__ = "default"

    def test_o2m(self):
        User, orders, Order = (
            self.classes.User,
            self.tables.orders,
            self.classes.Order,
        )

        sess = fixture_session()
        q = sess.query(User)

        u1 = q.filter_by(name="jack").one()

        # test auto-lookup of property
        with assertions.expect_deprecated_20(query_wparent_dep):
            o = sess.query(Order).with_parent(u1).all()
        assert [
            Order(description="order 1"),
            Order(description="order 3"),
            Order(description="order 5"),
        ] == o

        # test with explicit property
        with assertions.expect_deprecated_20(query_wparent_dep):
            o = sess.query(Order).with_parent(u1, property=User.orders).all()
        assert [
            Order(description="order 1"),
            Order(description="order 3"),
            Order(description="order 5"),
        ] == o

        with assertions.expect_deprecated_20(query_wparent_dep):
            # test generative criterion
            o = sess.query(Order).with_parent(u1).filter(orders.c.id > 2).all()
        assert [
            Order(description="order 3"),
            Order(description="order 5"),
        ] == o

    def test_select_from(self):
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()
        u1 = sess.get(User, 7)
        with assertions.expect_deprecated_20(query_wparent_dep):
            q = sess.query(Address).select_from(Address).with_parent(u1)
        self.assert_compile(
            q,
            "SELECT addresses.id AS addresses_id, "
            "addresses.user_id AS addresses_user_id, "
            "addresses.email_address AS addresses_email_address "
            "FROM addresses WHERE :param_1 = addresses.user_id",
            {"param_1": 7},
        )

    def test_from_entity_query_entity(self):
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()
        u1 = sess.get(User, 7)
        with assertions.expect_deprecated_20(query_wparent_dep):
            q = sess.query(User, Address).with_parent(
                u1, User.addresses, from_entity=Address
            )
        self.assert_compile(
            q,
            "SELECT users.id AS users_id, users.name AS users_name, "
            "addresses.id AS addresses_id, addresses.user_id "
            "AS addresses_user_id, "
            "addresses.email_address AS addresses_email_address "
            "FROM users, addresses "
            "WHERE :param_1 = addresses.user_id",
            {"param_1": 7},
        )

    def test_select_from_alias(self):
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()
        u1 = sess.get(User, 7)
        a1 = aliased(Address)
        with assertions.expect_deprecated_20(query_wparent_dep):
            q = sess.query(a1).with_parent(u1)
        self.assert_compile(
            q,
            "SELECT addresses_1.id AS addresses_1_id, "
            "addresses_1.user_id AS addresses_1_user_id, "
            "addresses_1.email_address AS addresses_1_email_address "
            "FROM addresses AS addresses_1 "
            "WHERE :param_1 = addresses_1.user_id",
            {"param_1": 7},
        )

    def test_select_from_alias_explicit_prop(self):
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()
        u1 = sess.get(User, 7)
        a1 = aliased(Address)
        with assertions.expect_deprecated_20(query_wparent_dep):
            q = sess.query(a1).with_parent(u1, User.addresses)
        self.assert_compile(
            q,
            "SELECT addresses_1.id AS addresses_1_id, "
            "addresses_1.user_id AS addresses_1_user_id, "
            "addresses_1.email_address AS addresses_1_email_address "
            "FROM addresses AS addresses_1 "
            "WHERE :param_1 = addresses_1.user_id",
            {"param_1": 7},
        )

    def test_select_from_alias_from_entity(self):
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()
        u1 = sess.get(User, 7)
        a1 = aliased(Address)
        a2 = aliased(Address)
        with assertions.expect_deprecated_20(query_wparent_dep):
            q = sess.query(a1, a2).with_parent(
                u1, User.addresses, from_entity=a2
            )
        self.assert_compile(
            q,
            "SELECT addresses_1.id AS addresses_1_id, "
            "addresses_1.user_id AS addresses_1_user_id, "
            "addresses_1.email_address AS addresses_1_email_address, "
            "addresses_2.id AS addresses_2_id, "
            "addresses_2.user_id AS addresses_2_user_id, "
            "addresses_2.email_address AS addresses_2_email_address "
            "FROM addresses AS addresses_1, "
            "addresses AS addresses_2 WHERE :param_1 = addresses_2.user_id",
            {"param_1": 7},
        )

    def test_select_from_alias_of_type(self):
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()
        u1 = sess.get(User, 7)
        a1 = aliased(Address)
        a2 = aliased(Address)
        with assertions.expect_deprecated_20(query_wparent_dep):
            q = sess.query(a1, a2).with_parent(u1, User.addresses.of_type(a2))
        self.assert_compile(
            q,
            "SELECT addresses_1.id AS addresses_1_id, "
            "addresses_1.user_id AS addresses_1_user_id, "
            "addresses_1.email_address AS addresses_1_email_address, "
            "addresses_2.id AS addresses_2_id, "
            "addresses_2.user_id AS addresses_2_user_id, "
            "addresses_2.email_address AS addresses_2_email_address "
            "FROM addresses AS addresses_1, "
            "addresses AS addresses_2 WHERE :param_1 = addresses_2.user_id",
            {"param_1": 7},
        )

    def test_noparent(self):
        Item, User = self.classes.Item, self.classes.User

        sess = fixture_session()
        q = sess.query(User)

        u1 = q.filter_by(name="jack").one()

        with assertions.expect_deprecated_20(query_wparent_dep):
            with assertions.expect_raises_message(
                sa_exc.InvalidRequestError,
                "Could not locate a property which relates "
                "instances of class 'Item' to instances of class 'User'",
            ):
                q = sess.query(Item).with_parent(u1)

    def test_m2m(self):
        Item, Keyword = self.classes.Item, self.classes.Keyword

        sess = fixture_session()
        i1 = sess.query(Item).filter_by(id=2).one()
        with assertions.expect_deprecated_20(query_wparent_dep):
            k = sess.query(Keyword).with_parent(i1).all()
        assert [
            Keyword(name="red"),
            Keyword(name="small"),
            Keyword(name="square"),
        ] == k

    def test_with_transient(self):
        User, Order = self.classes.User, self.classes.Order

        sess = fixture_session()

        q = sess.query(User)
        u1 = q.filter_by(name="jack").one()
        utrans = User(id=u1.id)
        with assertions.expect_deprecated_20(query_wparent_dep):
            o = sess.query(Order).with_parent(utrans, User.orders)
        eq_(
            [
                Order(description="order 1"),
                Order(description="order 3"),
                Order(description="order 5"),
            ],
            o.all(),
        )

    def test_with_pending_autoflush(self):
        Order, User = self.classes.Order, self.classes.User

        sess = fixture_session()

        o1 = sess.query(Order).first()
        opending = Order(id=20, user_id=o1.user_id)
        sess.add(opending)
        with assertions.expect_deprecated_20(query_wparent_dep):
            eq_(
                sess.query(User).with_parent(opending, Order.user).one(),
                User(id=o1.user_id),
            )

    def test_with_pending_no_autoflush(self):
        Order, User = self.classes.Order, self.classes.User

        sess = fixture_session(autoflush=False)

        o1 = sess.query(Order).first()
        opending = Order(user_id=o1.user_id)
        sess.add(opending)
        with assertions.expect_deprecated_20(query_wparent_dep):
            eq_(
                sess.query(User).with_parent(opending, Order.user).one(),
                User(id=o1.user_id),
            )

    def test_unique_binds_union(self):
        """bindparams used in the 'parent' query are unique"""
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()
        u1, u2 = sess.query(User).order_by(User.id)[0:2]

        with assertions.expect_deprecated_20(query_wparent_dep):
            q1 = sess.query(Address).with_parent(u1, User.addresses)
        with assertions.expect_deprecated_20(query_wparent_dep):
            q2 = sess.query(Address).with_parent(u2, User.addresses)

        self.assert_compile(
            q1.union(q2),
            "SELECT anon_1.addresses_id AS anon_1_addresses_id, "
            "anon_1.addresses_user_id AS anon_1_addresses_user_id, "
            "anon_1.addresses_email_address AS "
            "anon_1_addresses_email_address FROM (SELECT addresses.id AS "
            "addresses_id, addresses.user_id AS addresses_user_id, "
            "addresses.email_address AS addresses_email_address FROM "
            "addresses WHERE :param_1 = addresses.user_id UNION SELECT "
            "addresses.id AS addresses_id, addresses.user_id AS "
            "addresses_user_id, addresses.email_address "
            "AS addresses_email_address "
            "FROM addresses WHERE :param_2 = addresses.user_id) AS anon_1",
            checkparams={"param_1": 7, "param_2": 8},
        )


class CollectionCascadesDespiteBackrefTest(fixtures.TestBase):
    """test old cascade_backrefs behavior

    see test/orm/test_cascade.py::class CollectionCascadesNoBackrefTest
    for the future version

    """

    @testing.fixture
    def cascade_fixture(self, registry):
        def go(collection_class):
            @registry.mapped
            class A(object):
                __tablename__ = "a"

                id = Column(Integer, primary_key=True)
                bs = relationship(
                    "B", backref="a", collection_class=collection_class
                )

            @registry.mapped
            class B(object):
                __tablename__ = "b_"
                id = Column(Integer, primary_key=True)
                a_id = Column(ForeignKey("a.id"))
                key = Column(String)

            return A, B

        yield go

    @testing.combinations(
        (set, "add"),
        (list, "append"),
        (attribute_mapped_collection("key"), "__setitem__"),
        (attribute_mapped_collection("key"), "setdefault"),
        (attribute_mapped_collection("key"), "update_dict"),
        (attribute_mapped_collection("key"), "update_kw"),
        argnames="collection_class,methname",
    )
    @testing.combinations((True,), (False,), argnames="future")
    def test_cascades_on_collection(
        self, cascade_fixture, collection_class, methname, future
    ):
        A, B = cascade_fixture(collection_class)

        s = Session(future=future)

        a1 = A()
        s.add(a1)

        b1 = B(key="b1")
        b2 = B(key="b2")
        b3 = B(key="b3")

        if future:
            dep_ctx = util.nullcontext
        else:

            def dep_ctx():
                return assertions.expect_deprecated_20(
                    '"B" object is being merged into a Session along the '
                    'backref cascade path for relationship "A.bs"'
                )

        with dep_ctx():
            b1.a = a1
        with dep_ctx():
            b3.a = a1

        if future:
            assert b1 not in s
            assert b3 not in s
        else:
            assert b1 in s
            assert b3 in s

        if methname == "__setitem__":
            meth = getattr(a1.bs, methname)
            meth(b1.key, b1)
            meth(b2.key, b2)
        elif methname == "setdefault":
            meth = getattr(a1.bs, methname)
            meth(b1.key, b1)
            meth(b2.key, b2)
        elif methname == "update_dict" and isinstance(a1.bs, dict):
            a1.bs.update({b1.key: b1, b2.key: b2})
        elif methname == "update_kw" and isinstance(a1.bs, dict):
            a1.bs.update(b1=b1, b2=b2)
        else:
            meth = getattr(a1.bs, methname)
            meth(b1)
            meth(b2)

        assert b1 in s
        assert b2 in s

        # future version:
        if future:
            assert b3 not in s  # the event never triggers from reverse
        else:
            # old behavior
            assert b3 in s


class LoadOnFKsTest(fixtures.DeclarativeMappedTest):
    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class Parent(Base):
            __tablename__ = "parent"
            __table_args__ = {"mysql_engine": "InnoDB"}

            id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )

        class Child(Base):
            __tablename__ = "child"
            __table_args__ = {"mysql_engine": "InnoDB"}

            id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )
            parent_id = Column(Integer, ForeignKey("parent.id"))

            parent = relationship(Parent, backref=backref("children"))

    @testing.fixture
    def parent_fixture(self, connection):
        Parent, Child = self.classes("Parent", "Child")

        sess = fixture_session(bind=connection, autoflush=False)
        p1 = Parent()
        p2 = Parent()
        c1, c2 = Child(), Child()
        c1.parent = p1
        sess.add_all([p1, p2])
        assert c1 in sess

        yield sess, p1, p2, c1, c2

        sess.close()

    def test_enable_rel_loading_on_persistent_allows_backref_event(
        self, parent_fixture
    ):
        sess, p1, p2, c1, c2 = parent_fixture
        Parent, Child = self.classes("Parent", "Child")

        c3 = Child()
        sess.enable_relationship_loading(c3)
        c3.parent_id = p1.id
        with assertions.expect_deprecated_20(
            '"Child" object is being merged into a Session along the '
            'backref cascade path for relationship "Parent.children"'
        ):
            c3.parent = p1

        # backref fired off when c3.parent was set,
        # because the "old" value was None
        # change as of [ticket:3708]
        assert c3 in p1.children

    def test_enable_rel_loading_allows_backref_event(self, parent_fixture):
        sess, p1, p2, c1, c2 = parent_fixture
        Parent, Child = self.classes("Parent", "Child")

        c3 = Child()
        sess.enable_relationship_loading(c3)
        c3.parent_id = p1.id

        with assertions.expect_deprecated_20(
            '"Child" object is being merged into a Session along the '
            'backref cascade path for relationship "Parent.children"'
        ):
            c3.parent = p1

        # backref fired off when c3.parent was set,
        # because the "old" value was None
        # change as of [ticket:3708]
        assert c3 in p1.children


class LazyTest(_fixtures.FixtureTest):
    run_inserts = "once"
    run_deletes = None

    def test_backrefs_dont_lazyload(self):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User,
        )

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties={"addresses": relationship(Address, backref="user")},
        )
        self.mapper_registry.map_imperatively(Address, addresses)
        sess = fixture_session(autoflush=False)
        ad = sess.query(Address).filter_by(id=1).one()
        assert ad.user.id == 7

        def go():
            ad.user = None
            assert ad.user is None

        self.assert_sql_count(testing.db, go, 0)

        u1 = sess.query(User).filter_by(id=7).one()

        def go():
            assert ad not in u1.addresses

        self.assert_sql_count(testing.db, go, 1)

        sess.expire(u1, ["addresses"])

        def go():
            assert ad in u1.addresses

        self.assert_sql_count(testing.db, go, 1)

        sess.expire(u1, ["addresses"])
        ad2 = Address()

        def go():
            with assertions.expect_deprecated_20(
                ".* object is being merged into a Session along the "
                "backref cascade path for relationship "
            ):
                ad2.user = u1
            assert ad2.user is u1

        self.assert_sql_count(testing.db, go, 0)

        def go():
            assert ad2 in u1.addresses

        self.assert_sql_count(testing.db, go, 1)


class BindIntegrationTest(_fixtures.FixtureTest):
    run_inserts = None

    def test_bound_connection_transactional(self):
        User, users = self.classes.User, self.tables.users

        self.mapper_registry.map_imperatively(User, users)

        with testing.db.connect() as c:
            trans = c.begin()

            with assertions.expect_deprecated_20(autocommit_dep):
                sess = Session(bind=c, autocommit=True)
            u = User(name="u3")
            sess.add(u)
            sess.flush()
            assert c.in_transaction()
            trans.commit()
            assert not c.in_transaction()
            assert (
                c.exec_driver_sql("select count(1) from users").scalar() == 1
            )


class MergeResultTest(_fixtures.FixtureTest):
    run_setup_mappers = "once"
    run_inserts = "once"
    run_deletes = None

    @classmethod
    def setup_mappers(cls):
        cls._setup_stock_mapping()

    def _fixture(self):
        User = self.classes.User

        s = fixture_session()
        u1, u2, u3, u4 = (
            User(id=1, name="u1"),
            User(id=2, name="u2"),
            User(id=7, name="u3"),
            User(id=8, name="u4"),
        )
        s.query(User).filter(User.id.in_([7, 8])).all()
        s.close()
        return s, [u1, u2, u3, u4]

    def test_single_entity(self):
        s, (u1, u2, u3, u4) = self._fixture()
        User = self.classes.User

        q = s.query(User)
        collection = [u1, u2, u3, u4]

        with assertions.expect_deprecated_20(merge_result_dep):
            it = q.merge_result(collection)
        eq_([x.id for x in it], [1, 2, 7, 8])

    def test_single_column(self):
        User = self.classes.User

        s = fixture_session()

        q = s.query(User.id)
        collection = [(1,), (2,), (7,), (8,)]
        with assertions.expect_deprecated_20(merge_result_dep):
            it = q.merge_result(collection)
        eq_(list(it), [(1,), (2,), (7,), (8,)])

    def test_entity_col_mix_plain_tuple(self):
        s, (u1, u2, u3, u4) = self._fixture()
        User = self.classes.User

        q = s.query(User, User.id)
        collection = [(u1, 1), (u2, 2), (u3, 7), (u4, 8)]
        with assertions.expect_deprecated_20(merge_result_dep):
            it = q.merge_result(collection)
        it = list(it)
        eq_([(x.id, y) for x, y in it], [(1, 1), (2, 2), (7, 7), (8, 8)])
        eq_(list(it[0]._mapping.keys()), ["User", "id"])

    def test_entity_col_mix_keyed_tuple(self):
        s, (u1, u2, u3, u4) = self._fixture()
        User = self.classes.User

        q = s.query(User, User.id)

        row = result_tuple(["User", "id"])

        def kt(*x):
            return row(x)

        collection = [kt(u1, 1), kt(u2, 2), kt(u3, 7), kt(u4, 8)]
        with assertions.expect_deprecated_20(merge_result_dep):
            it = q.merge_result(collection)
        it = list(it)
        eq_([(x.id, y) for x, y in it], [(1, 1), (2, 2), (7, 7), (8, 8)])
        eq_(list(it[0]._mapping.keys()), ["User", "id"])

    def test_none_entity(self):
        s, (u1, u2, u3, u4) = self._fixture()
        User = self.classes.User

        ua = aliased(User)
        q = s.query(User, ua)

        row = result_tuple(["User", "useralias"])

        def kt(*x):
            return row(x)

        collection = [kt(u1, u2), kt(u1, None), kt(u2, u3)]
        with assertions.expect_deprecated_20(merge_result_dep):
            it = q.merge_result(collection)
        eq_(
            [(x and x.id or None, y and y.id or None) for x, y in it],
            [(u1.id, u2.id), (u1.id, None), (u2.id, u3.id)],
        )


class DefaultStrategyOptionsTest(_DefaultStrategyOptionsTest):
    def test_joined_path_wildcards(self):
        sess = self._upgrade_fixture()
        users = []

        User, Order, Item = self.classes("User", "Order", "Item")

        # test upgrade all to joined: 1 sql
        def go():
            users[:] = (
                sess.query(User)
                .options(joinedload(".*"))
                .options(defaultload(User.addresses).joinedload("*"))
                .options(defaultload(User.orders).joinedload("*"))
                .options(
                    defaultload(User.orders)
                    .defaultload(Order.items)
                    .joinedload("*")
                )
                .order_by(self.classes.User.id)
                .all()
            )

        with assertions.expect_deprecated(dep_exc_wildcard):
            self.assert_sql_count(testing.db, go, 1)
            self._assert_fully_loaded(users)

    def test_subquery_path_wildcards(self):
        sess = self._upgrade_fixture()
        users = []

        User, Order = self.classes("User", "Order")

        # test upgrade all to subquery: 1 sql + 4 relationships = 5
        def go():
            users[:] = (
                sess.query(User)
                .options(subqueryload(".*"))
                .options(defaultload(User.addresses).subqueryload("*"))
                .options(defaultload(User.orders).subqueryload("*"))
                .options(
                    defaultload(User.orders)
                    .defaultload(Order.items)
                    .subqueryload("*")
                )
                .order_by(User.id)
                .all()
            )

        with assertions.expect_deprecated(dep_exc_wildcard):
            self.assert_sql_count(testing.db, go, 5)

            # verify everything loaded, with no additional sql needed
            self._assert_fully_loaded(users)


class Deferred_InheritanceTest(_deferred_InheritanceTest):
    def test_defer_on_wildcard_subclass(self):
        # pretty much the same as load_only except doesn't
        # exclude the primary key

        # what is ".*"?  this is not documented anywhere, how did this
        # get implemented without docs ?  see #4390
        s = fixture_session()
        with assertions.expect_deprecated(dep_exc_wildcard):
            q = (
                s.query(Manager)
                .order_by(Person.person_id)
                .options(defer(".*"), undefer(Manager.status))
            )
        self.assert_compile(
            q,
            "SELECT managers.status AS managers_status "
            "FROM people JOIN managers ON "
            "people.person_id = managers.person_id ORDER BY people.person_id",
        )
        # note this doesn't apply to "bound" loaders since they don't seem
        # to have this ".*" featue.
