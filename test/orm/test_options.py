import pickle

import sqlalchemy as sa
from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import inspect
from sqlalchemy import Integer
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import testing
from sqlalchemy.orm import aliased
from sqlalchemy.orm import attributes
from sqlalchemy.orm import column_property
from sqlalchemy.orm import contains_eager
from sqlalchemy.orm import defaultload
from sqlalchemy.orm import defer
from sqlalchemy.orm import exc as orm_exc
from sqlalchemy.orm import joinedload
from sqlalchemy.orm import Load
from sqlalchemy.orm import load_only
from sqlalchemy.orm import loading
from sqlalchemy.orm import relationship
from sqlalchemy.orm import strategy_options
from sqlalchemy.orm import subqueryload
from sqlalchemy.orm import synonym
from sqlalchemy.orm import undefer
from sqlalchemy.orm import util as orm_util
from sqlalchemy.orm import with_polymorphic
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_not
from sqlalchemy.testing.assertions import assert_raises_message
from sqlalchemy.testing.assertions import AssertsCompiledSQL
from sqlalchemy.testing.assertions import emits_warning
from sqlalchemy.testing.assertions import eq_
from sqlalchemy.testing.assertions import expect_raises_message
from sqlalchemy.testing.fixtures import fixture_session
from sqlalchemy.testing.pickleable import Address
from sqlalchemy.testing.pickleable import User
from test.orm import _fixtures
from .inheritance._poly_fixtures import _Polymorphic
from .inheritance._poly_fixtures import Company
from .inheritance._poly_fixtures import Engineer
from .inheritance._poly_fixtures import Manager
from .inheritance._poly_fixtures import Person


class QueryTest(_fixtures.FixtureTest):
    run_setup_mappers = "once"
    run_inserts = "once"
    run_deletes = None

    @classmethod
    def setup_mappers(cls):
        cls._setup_stock_mapping()

        class SubItem(cls.classes.Item):
            pass

        cls.mapper_registry.map_imperatively(
            SubItem,
            None,
            inherits=cls.classes.Item,
            properties={
                "extra_keywords": relationship(
                    cls.classes.Keyword,
                    viewonly=True,
                    secondary=cls.tables.item_keywords,
                )
            },
        )

        class OrderWProp(cls.classes.Order):
            @property
            def some_attr(self):
                return "hi"

        cls.mapper_registry.map_imperatively(
            OrderWProp, None, inherits=cls.classes.Order
        )


class PathTest:
    def _make_path(self, path):
        r = []
        for i, item in enumerate(path):
            if i % 2 == 0:
                item = inspect(item)
            else:
                if isinstance(item, str):
                    item = inspect(r[-1]).mapper.attrs[item]
            r.append(item)
        return tuple(r)

    def _make_path_registry(self, path):
        return orm_util.PathRegistry.coerce(self._make_path(path))

    def _assert_path_result(self, opt, q, paths):
        attr = {}

        compile_state = q._compile_state()
        compile_state.attributes = attr = {}
        opt.process_compile_state(compile_state)

        assert_paths = [k[1] for k in attr]
        eq_(
            {p for p in assert_paths},
            {self._make_path(p) for p in paths},
        )


class LoadTest(PathTest, QueryTest):
    def test_str(self):
        User = self.classes.User
        result = Load(User)
        eq_(
            str(result),
            "Load(Mapper[User(users)])",
        )

        result = Load(aliased(User))
        eq_(
            str(result),
            "Load(aliased(User))",
        )

    def test_gen_path_attr_entity(self):
        User = self.classes.User
        Address = self.classes.Address

        ll = Load(User)

        eq_(
            strategy_options._AttributeStrategyLoad.create(
                ll.path,
                User.addresses,
                ("strategy", True),
                "relationship",
                {},
                True,
            ).path,
            self._make_path_registry([User, "addresses", Address]),
        )

    def test_gen_path_attr_column(self):
        User = self.classes.User

        ll = Load(User)
        eq_(
            strategy_options._AttributeStrategyLoad.create(
                ll.path,
                User.name,
                ("strategy", True),
                "column",
                {},
                True,
            ).path,
            self._make_path_registry([User, "name"]),
        )

    def test_gen_path_invalid_from_col(self):
        User = self.classes.User

        result = Load(User)
        result.path = self._make_path_registry([User, "name"])
        assert_raises_message(
            sa.exc.ArgumentError,
            "Attribute 'name' of entity 'Mapper|User|users' does "
            "not refer to a mapped entity",
            result._clone_for_bind_strategy,
            (User.addresses,),
            None,
            "relationship",
        )

    def test_gen_path_attr_entity_invalid_raiseerr(self):
        User = self.classes.User
        Order = self.classes.Order

        result = Load(User)

        assert_raises_message(
            sa.exc.ArgumentError,
            "Attribute 'Order.items' does not link from element "
            "'Mapper|User|users'",
            result._clone_for_bind_strategy,
            (Order.items,),
            None,
            "relationship",
        )

    def test_gen_path_attr_entity_invalid_noraiseerr(self):
        User = self.classes.User
        Order = self.classes.Order

        ll = Load(User)

        eq_(
            strategy_options._AttributeStrategyLoad.create(
                ll.path,
                Order.items,
                ("strategy", True),
                "relationship",
                {},
                True,
                raiseerr=False,
            ),
            None,
        )

    def test_set_strat_ent(self):
        User = self.classes.User

        l1 = Load(User)
        l2 = l1.joinedload(User.addresses)

        s = fixture_session()
        q1 = s.query(User).options(l2)
        attr = q1._compile_context().attributes

        eq_(
            attr[("loader", self._make_path([User, "addresses"]))],
            l2.context[0],
        )

    def test_set_strat_col(self):
        User = self.classes.User

        l1 = Load(User)
        l2 = l1.defer(User.name)
        s = fixture_session()
        q1 = s.query(User).options(l2)
        attr = q1._compile_context().attributes

        eq_(attr[("loader", self._make_path([User, "name"]))], l2.context[0])


class OfTypePathingTest(PathTest, QueryTest):
    def _fixture(self):
        User, Address = self.classes.User, self.classes.Address
        Dingaling = self.classes.Dingaling
        address_table = self.tables.addresses

        class SubAddr(Address):
            pass

        self.mapper_registry.map_imperatively(
            SubAddr,
            inherits=Address,
            properties={
                "sub_attr": column_property(address_table.c.email_address),
                "dings": relationship(Dingaling, viewonly=True),
            },
        )

        return User, Address, SubAddr

    @emits_warning("This declarative base already contains a class")
    def test_oftype_only_col_attr_unbound(self):
        User, Address, SubAddr = self._fixture()

        l1 = defaultload(User.addresses.of_type(SubAddr)).defer(
            SubAddr.sub_attr
        )

        sess = fixture_session()
        q = sess.query(User)
        self._assert_path_result(
            l1,
            q,
            [(User, "addresses"), (User, "addresses", SubAddr, "sub_attr")],
        )

    @emits_warning("This declarative base already contains a class")
    def test_oftype_only_col_attr_bound(self):
        User, Address, SubAddr = self._fixture()

        l1 = (
            Load(User)
            .defaultload(User.addresses.of_type(SubAddr))
            .defer(SubAddr.sub_attr)
        )

        sess = fixture_session()
        q = sess.query(User)
        self._assert_path_result(
            l1,
            q,
            [(User, "addresses"), (User, "addresses", SubAddr, "sub_attr")],
        )

    @emits_warning("This declarative base already contains a class")
    def test_oftype_only_rel_attr_unbound(self):
        User, Address, SubAddr = self._fixture()

        l1 = defaultload(User.addresses.of_type(SubAddr)).joinedload(
            SubAddr.dings
        )

        sess = fixture_session()
        q = sess.query(User)
        self._assert_path_result(
            l1, q, [(User, "addresses"), (User, "addresses", SubAddr, "dings")]
        )

    @emits_warning("This declarative base already contains a class")
    def test_oftype_only_rel_attr_bound(self):
        User, Address, SubAddr = self._fixture()

        l1 = (
            Load(User)
            .defaultload(User.addresses.of_type(SubAddr))
            .joinedload(SubAddr.dings)
        )

        sess = fixture_session()
        q = sess.query(User)
        self._assert_path_result(
            l1, q, [(User, "addresses"), (User, "addresses", SubAddr, "dings")]
        )


class WithEntitiesTest(QueryTest, AssertsCompiledSQL):
    __dialect__ = "default"

    def test_options_legacy_with_entities_onelevel(self):
        """test issue #6253 (part of #6503)"""

        User = self.classes.User
        sess = fixture_session()

        q = (
            sess.query(User)
            .options(joinedload(User.addresses))
            .with_entities(User.id)
        )
        self.assert_compile(q, "SELECT users.id AS users_id FROM users")

    def test_options_with_only_cols_onelevel(self):
        """test issue #6253 (part of #6503)"""

        User = self.classes.User

        q = (
            select(User)
            .options(joinedload(User.addresses))
            .with_only_columns(User.id)
        )
        self.assert_compile(q, "SELECT users.id FROM users")

    def test_options_entities_replaced_with_equivs_one(self):
        User = self.classes.User
        Address = self.classes.Address

        q = (
            select(User, Address)
            .options(joinedload(User.addresses))
            .with_only_columns(User)
        )
        self.assert_compile(
            q,
            "SELECT users.id, users.name, addresses_1.id AS id_1, "
            "addresses_1.user_id, addresses_1.email_address FROM users "
            "LEFT OUTER JOIN addresses AS addresses_1 "
            "ON users.id = addresses_1.user_id ORDER BY addresses_1.id",
        )

    def test_options_entities_replaced_with_equivs_two(self):
        User = self.classes.User
        Address = self.classes.Address

        q = (
            select(User, Address)
            .options(joinedload(User.addresses), joinedload(Address.dingaling))
            .with_only_columns(User)
        )
        self.assert_compile(
            q,
            "SELECT users.id, users.name, addresses_1.id AS id_1, "
            "addresses_1.user_id, addresses_1.email_address FROM users "
            "LEFT OUTER JOIN addresses AS addresses_1 "
            "ON users.id = addresses_1.user_id ORDER BY addresses_1.id",
        )

    def test_options_entities_replaced_with_equivs_three(self):
        User = self.classes.User
        Address = self.classes.Address

        q = (
            select(User)
            .options(joinedload(User.addresses))
            .with_only_columns(User, Address)
            .options(joinedload(Address.dingaling))
            .join_from(User, Address)
        )
        self.assert_compile(
            q,
            "SELECT users.id, users.name, addresses.id AS id_1, "
            "addresses.user_id, addresses.email_address, "
            "addresses_1.id AS id_2, addresses_1.user_id AS user_id_1, "
            "addresses_1.email_address AS email_address_1, "
            "dingalings_1.id AS id_3, dingalings_1.address_id, "
            "dingalings_1.data "
            "FROM users JOIN addresses ON users.id = addresses.user_id "
            "LEFT OUTER JOIN addresses AS addresses_1 "
            "ON users.id = addresses_1.user_id "
            "LEFT OUTER JOIN dingalings AS dingalings_1 "
            "ON addresses.id = dingalings_1.address_id "
            "ORDER BY addresses_1.id",
        )


class OptionsTest(PathTest, QueryTest):
    def _option_fixture(self, *arg):
        # note contains_eager() passes chained=True to _from_keys,
        # which means an expression like contains_eager(a, b, c)
        # is expected to produce
        # contains_eager(a).contains_eager(b).contains_eager(c).  no other
        # loader option works this way right now; the rest all use
        # defaultload() for the "chain" elements
        return strategy_options._generate_from_keys(
            strategy_options.Load.contains_eager,
            arg,
            True,
            dict(_propagate_to_loaders=True),
        )

    @testing.combinations(
        lambda: joinedload("addresses"),
        lambda: defer("name"),
        lambda Address: joinedload("addresses").joinedload(Address.dingaling),
        lambda: joinedload("addresses"),
    )
    def test_error_for_string_names_unbound(self, test_case):
        User, Address = self.classes("User", "Address")

        with expect_raises_message(
            sa.exc.ArgumentError,
            "Strings are not accepted for attribute names in loader "
            "options; please use class-bound attributes directly.",
        ):
            testing.resolve_lambda(test_case, User=User, Address=Address)

    @testing.combinations(
        lambda User: Load(User).joinedload("addresses"),
        lambda User: Load(User).defer("name"),
        lambda User, Address: Load(User)
        .joinedload("addresses")
        .joinedload(Address.dingaling),
        lambda User: Load(User).joinedload("addresses"),
    )
    def test_error_for_string_names_bound(self, test_case):
        User, Address = self.classes("User", "Address")

        with expect_raises_message(
            sa.exc.ArgumentError,
            "Strings are not accepted for attribute names in loader "
            "options; please use class-bound attributes directly.",
        ):
            testing.resolve_lambda(test_case, User=User, Address=Address)

    def test_get_path_one_level_attribute(self):
        User = self.classes.User

        sess = fixture_session()
        q = sess.query(User)

        opt = self._option_fixture(User.addresses)
        self._assert_path_result(opt, q, [(User, "addresses")])

    def test_get_path_one_level_with_unrelated(self):
        Order = self.classes.Order
        User = self.classes.User

        sess = fixture_session()
        q = sess.query(Order)
        opt = self._option_fixture(User.addresses)

        with expect_raises_message(
            sa.exc.ArgumentError,
            r"Mapped class Mapper\[User\(users\)\] does not apply to any "
            "of the root entities in this query",
        ):
            self._assert_path_result(opt, q, [])

    def test_path_multilevel_attribute(self):
        Item, User, Order = (
            self.classes.Item,
            self.classes.User,
            self.classes.Order,
        )

        sess = fixture_session()
        q = sess.query(User)

        opt = self._option_fixture(User.orders, Order.items, Item.keywords)
        self._assert_path_result(
            opt,
            q,
            [
                (User, "orders"),
                (User, "orders", Order, "items"),
                (User, "orders", Order, "items", Item, "keywords"),
            ],
        )

    def test_with_current_matching_attribute(self):
        Item, User, Order = (
            self.classes.Item,
            self.classes.User,
            self.classes.Order,
        )

        sess = fixture_session()
        q = sess.query(Item)._with_current_path(
            self._make_path_registry([User, "orders", Order, "items"])
        )

        opt = self._option_fixture(User.orders, Order.items, Item.keywords)
        self._assert_path_result(opt, q, [(Item, "keywords")])

    def test_with_current_nonmatching_attribute(self):
        Item, User, Order = (
            self.classes.Item,
            self.classes.User,
            self.classes.Order,
        )

        sess = fixture_session()
        q = sess.query(Item)._with_current_path(
            self._make_path_registry([User, "orders", Order, "items"])
        )

        opt = self._option_fixture(Item.keywords)
        self._assert_path_result(opt, q, [])

        opt = self._option_fixture(Order.items, Item.keywords)
        self._assert_path_result(opt, q, [])

    def test_with_current_nonmatching_entity(self):
        Item, User, Order = (
            self.classes.Item,
            self.classes.User,
            self.classes.Order,
        )

        sess = fixture_session()
        q = sess.query(Item)._with_current_path(
            self._make_path_registry(
                [inspect(aliased(User)), "orders", Order, "items"]
            )
        )

        opt = self._option_fixture(User.orders)
        self._assert_path_result(opt, q, [])

        opt = self._option_fixture(User.orders, Order.items, Item.keywords)
        self._assert_path_result(opt, q, [])

        q = sess.query(Item)._with_current_path(
            self._make_path_registry([User, "orders", Order, "items"])
        )

        ac = aliased(User)

        opt = self._option_fixture(ac.orders)
        self._assert_path_result(opt, q, [])

        opt = self._option_fixture(ac.orders, Order.items, Item.keywords)
        self._assert_path_result(opt, q, [])

    def test_with_current_match_aliased_classes(self):
        Item, User, Order = (
            self.classes.Item,
            self.classes.User,
            self.classes.Order,
        )

        ac = aliased(User)
        sess = fixture_session()
        q = sess.query(Item)._with_current_path(
            self._make_path_registry([inspect(ac), "orders", Order, "items"])
        )

        opt = self._option_fixture(ac.orders, Order.items, Item.keywords)
        self._assert_path_result(opt, q, [(Item, "keywords")])

        opt = self._option_fixture(ac.orders, Order.items)
        self._assert_path_result(opt, q, [])

    @emits_warning("This declarative base already contains a class")
    def test_from_base_to_subclass_attr(self):
        Dingaling, Address = self.classes.Dingaling, self.classes.Address

        sess = fixture_session()

        class SubAddr(Address):
            pass

        self.mapper_registry.map_imperatively(
            SubAddr,
            inherits=Address,
            properties={"flub": relationship(Dingaling, viewonly=True)},
        )

        q = sess.query(Address)
        opt = self._option_fixture(SubAddr.flub)

        self._assert_path_result(opt, q, [(SubAddr, "flub")])

    @emits_warning("This declarative base already contains a class")
    def test_from_subclass_to_subclass_attr(self):
        Dingaling, Address = self.classes.Dingaling, self.classes.Address

        sess = fixture_session()

        class SubAddr(Address):
            pass

        self.mapper_registry.map_imperatively(
            SubAddr,
            inherits=Address,
            properties={"flub": relationship(Dingaling, viewonly=True)},
        )

        q = sess.query(SubAddr)
        opt = self._option_fixture(SubAddr.flub)

        self._assert_path_result(opt, q, [(SubAddr, "flub")])

    def test_from_base_to_base_attr_via_subclass(self):
        Dingaling, Address = self.classes.Dingaling, self.classes.Address

        sess = fixture_session()

        class SubAddr(Address):
            pass

        self.mapper_registry.map_imperatively(
            SubAddr,
            inherits=Address,
            properties={"flub": relationship(Dingaling, viewonly=True)},
        )

        q = sess.query(Address)
        opt = self._option_fixture(SubAddr.user)

        self._assert_path_result(
            opt, q, [(Address, inspect(Address).attrs.user)]
        )

    @emits_warning("This declarative base already contains a class")
    def test_of_type(self):
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()

        class SubAddr(Address):
            pass

        self.mapper_registry.map_imperatively(SubAddr, inherits=Address)

        q = sess.query(User)
        opt = self._option_fixture(
            User.addresses.of_type(SubAddr), SubAddr.user
        )

        u_mapper = inspect(User)
        a_mapper = inspect(Address)
        self._assert_path_result(
            opt,
            q,
            [
                (u_mapper, u_mapper.attrs.addresses),
                (
                    u_mapper,
                    u_mapper.attrs.addresses,
                    a_mapper,
                    a_mapper.attrs.user,
                ),
            ],
        )

    @emits_warning("This declarative base already contains a class")
    def test_of_type_plus_level(self):
        Dingaling, User, Address = (
            self.classes.Dingaling,
            self.classes.User,
            self.classes.Address,
        )

        sess = fixture_session()

        class SubAddr(Address):
            pass

        self.mapper_registry.map_imperatively(
            SubAddr,
            inherits=Address,
            properties={"flub": relationship(Dingaling, viewonly=True)},
        )

        q = sess.query(User)
        opt = self._option_fixture(
            User.addresses.of_type(SubAddr), SubAddr.flub
        )

        u_mapper = inspect(User)
        sa_mapper = inspect(SubAddr)
        self._assert_path_result(
            opt,
            q,
            [
                (u_mapper, u_mapper.attrs.addresses),
                (
                    u_mapper,
                    u_mapper.attrs.addresses,
                    sa_mapper,
                    sa_mapper.attrs.flub,
                ),
            ],
        )

    def test_aliased_single(self):
        User = self.classes.User

        sess = fixture_session()
        ualias = aliased(User)
        q = sess.query(ualias)
        opt = self._option_fixture(ualias.addresses)
        self._assert_path_result(opt, q, [(inspect(ualias), "addresses")])

    def test_with_current_aliased_single(self):
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()
        ualias = aliased(User)
        q = sess.query(Address)._with_current_path(
            self._make_path_registry([Address, "user"])
        )
        opt = self._option_fixture(
            Address.user.of_type(ualias), ualias.addresses
        )
        self._assert_path_result(opt, q, [(inspect(ualias), "addresses")])

    def test_with_current_aliased_single_nonmatching_option(self):
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()
        ualias = aliased(User)
        q = sess.query(User)._with_current_path(
            self._make_path_registry([User, "addresses", Address, "user"])
        )
        opt = self._option_fixture(
            Address.user.of_type(ualias), ualias.addresses
        )
        self._assert_path_result(opt, q, [])

    def test_with_current_aliased_single_nonmatching_entity(self):
        User, Address = self.classes.User, self.classes.Address

        sess = fixture_session()
        ualias = aliased(User)
        q = sess.query(ualias)._with_current_path(
            # this was:
            # self._make_path_registry([Address, "user"])
            # .. which seems like an impossible "current_path"
            #
            # this one makes a little more sense
            self._make_path_registry([ualias, "addresses", Address, "user"])
        )
        opt = self._option_fixture(Address.user, User.addresses)
        self._assert_path_result(opt, q, [])

    def test_multi_entity_opt_on_second(self):
        Item = self.classes.Item
        Order = self.classes.Order
        opt = self._option_fixture(Order.items)
        sess = fixture_session()
        q = sess.query(Item, Order)
        self._assert_path_result(opt, q, [(Order, "items")])

    def test_path_exhausted(self):
        User = self.classes.User
        Item = self.classes.Item
        Order = self.classes.Order
        opt = self._option_fixture(User.orders)
        sess = fixture_session()
        q = sess.query(Item)._with_current_path(
            self._make_path_registry([User, "orders", Order, "items"])
        )
        self._assert_path_result(opt, q, [])

    def test_chained(self):
        User = self.classes.User
        Order = self.classes.Order
        sess = fixture_session()
        q = sess.query(User)
        opt = self._option_fixture(User.orders).joinedload(Order.items)
        self._assert_path_result(
            opt, q, [(User, "orders"), (User, "orders", Order, "items")]
        )

    def test_chained_plus_multi(self):
        User = self.classes.User
        Order = self.classes.Order
        Item = self.classes.Item
        sess = fixture_session()
        q = sess.query(User)
        opt = self._option_fixture(User.orders, Order.items).joinedload(
            Item.keywords
        )

        self._assert_path_result(
            opt,
            q,
            [
                (User, "orders"),
                (User, "orders", Order, "items"),
                (User, "orders", Order, "items", Item, "keywords"),
            ],
        )


class FromSubclassOptionsTest(PathTest, fixtures.DeclarativeMappedTest):
    # test for regression to #3963
    run_setup_mappers = "once"
    run_inserts = "once"
    run_deletes = None

    @classmethod
    def setup_mappers(cls):
        Base = cls.DeclarativeBasic

        class BaseCls(Base):
            __tablename__ = "basecls"
            id = Column(Integer, primary_key=True)

            type = Column(String(30))
            related_id = Column(ForeignKey("related.id"))
            related = relationship("Related")

        class SubClass(BaseCls):
            __tablename__ = "subcls"
            id = Column(ForeignKey("basecls.id"), primary_key=True)

        class Related(Base):
            __tablename__ = "related"
            id = Column(Integer, primary_key=True)

            sub_related_id = Column(ForeignKey("sub_related.id"))
            sub_related = relationship("SubRelated")

        class SubRelated(Base):
            __tablename__ = "sub_related"
            id = Column(Integer, primary_key=True)

    def test_with_current_nonmatching_entity_subclasses(self):
        BaseCls, SubClass, Related, SubRelated = self.classes(
            "BaseCls", "SubClass", "Related", "SubRelated"
        )
        sess = fixture_session()

        q = sess.query(Related)._with_current_path(
            self._make_path_registry([inspect(SubClass), "related"])
        )

        opt = subqueryload(SubClass.related).subqueryload(Related.sub_related)
        self._assert_path_result(opt, q, [(Related, "sub_related")])


class OptionsNoPropTest(_fixtures.FixtureTest):
    """test the error messages emitted when using property
    options in conjunction with column-only entities, or
    for not existing options

    """

    run_create_tables = False
    run_inserts = None
    run_deletes = None

    def test_option_with_mapper_PropCompatator(self):
        Item = self.classes.Item

        self._assert_option([Item], Item.keywords)

    def test_option_with_mapper_then_column_PropComparator(self):
        Item = self.classes.Item

        self._assert_option([Item, Item.id], Item.keywords)

    def test_option_with_column_then_mapper_PropComparator(self):
        Item = self.classes.Item

        self._assert_option([Item.id, Item], Item.keywords)

    def test_option_with_column_PropComparator(self):
        Item = self.classes.Item

        self._assert_eager_with_just_column_exception(
            Item.id,
            Item.keywords,
            r"Query has only expression-based entities; attribute loader "
            r"options for Mapper\[Item\(items\)\] can't be applied here.",
        )

    def test_option_against_nonexistent_PropComparator(self):
        Item = self.classes.Item
        Keyword = self.classes.Keyword
        self._assert_eager_with_entity_exception(
            [Keyword],
            (joinedload(Item.keywords),),
            r"Mapped class Mapper\[Item\(items\)\] does not apply to any of "
            "the root entities in this query, e.g. "
            r"Mapper\[Keyword\(keywords\)\]. "
            "Please specify the full path from one of "
            "the root entities to the target attribute. ",
        )

    def test_load_only_against_multi_entity_attr(self):
        User = self.classes.User
        Item = self.classes.Item
        self._assert_eager_with_entity_exception(
            [User, Item],
            lambda: (load_only(User.id, Item.id),),
            r"Can't apply wildcard \('\*'\) or load_only\(\) loader option "
            r"to multiple entities in the same option. Use separate options "
            "per entity.",
        )

    def test_col_option_against_relationship_attr(self):
        Item = self.classes.Item
        self._assert_loader_strategy_exception(
            [Item],
            lambda: (load_only(Item.keywords),),
            'Can\'t apply "column loader" strategy to property '
            '"Item.keywords", which is a "relationship"; this '
            'loader strategy is intended to be used with a "column property".',
        )

    def test_option_against_wrong_multi_entity_type_attr_one(self):
        Item = self.classes.Item
        Keyword = self.classes.Keyword
        self._assert_loader_strategy_exception(
            [Keyword, Item],
            lambda: (joinedload(Keyword.id).joinedload(Item.keywords),),
            'Can\'t apply "joined loader" strategy to property "Keyword.id", '
            'which is a "column property"; this loader strategy is intended '
            'to be used with a "relationship property".',
        )

    def test_option_against_wrong_multi_entity_type_attr_two(self):
        Item = self.classes.Item
        Keyword = self.classes.Keyword
        self._assert_loader_strategy_exception(
            [Keyword, Item],
            lambda: (joinedload(Keyword.keywords).joinedload(Item.keywords),),
            'Can\'t apply "joined loader" strategy to property '
            '"Keyword.keywords", which is a "mapped sql expression"; '
            "this loader "
            'strategy is intended to be used with a "relationship property".',
        )

    def test_option_against_wrong_multi_entity_type_attr_three(self):
        Item = self.classes.Item
        Keyword = self.classes.Keyword
        self._assert_eager_with_entity_exception(
            [Keyword.id, Item.id],
            lambda: (joinedload(Keyword.keywords),),
            r"Query has only expression-based entities; attribute loader "
            r"options for Mapper\[Keyword\(keywords\)\] can't be applied "
            "here.",
        )

    @testing.combinations(True, False, argnames="first_element")
    def test_wrong_type_in_option_cls(self, first_element):
        Item = self.classes.Item
        Keyword = self.classes.Keyword
        self._assert_eager_with_entity_exception(
            [Item],
            lambda: (
                (joinedload(Keyword),)
                if first_element
                else (Load(Item).joinedload(Keyword),)
            ),
            "expected ORM mapped attribute for loader strategy argument",
        )

    @testing.combinations(
        (15,), (object(),), (type,), ({"foo": "bar"},), argnames="rando"
    )
    @testing.combinations(True, False, argnames="first_element")
    def test_wrong_type_in_option_any_random_type(self, rando, first_element):
        Item = self.classes.Item
        self._assert_eager_with_entity_exception(
            [Item],
            lambda: (
                (joinedload(rando),)
                if first_element
                else (Load(Item).joinedload(rando))
            ),
            "expected ORM mapped attribute for loader strategy argument",
        )

    @testing.combinations(True, False, argnames="first_element")
    def test_wrong_type_in_option_descriptor(self, first_element):
        OrderWProp = self.classes.OrderWProp

        self._assert_eager_with_entity_exception(
            [OrderWProp],
            lambda: (
                (joinedload(OrderWProp.some_attr),)
                if first_element
                else (Load(OrderWProp).joinedload(OrderWProp.some_attr),)
            ),
            "expected ORM mapped attribute for loader strategy argument",
        )

    def test_non_contiguous_all_option(self):
        User = self.classes.User
        self._assert_eager_with_entity_exception(
            [User],
            lambda: (joinedload(User.addresses).joinedload(User.orders),),
            r'ORM mapped entity or attribute "User.orders" does not link '
            r'from relationship "User.addresses"',
        )

    def test_non_contiguous_all_option_of_type(self):
        User = self.classes.User
        Order = self.classes.Order
        self._assert_eager_with_entity_exception(
            [User],
            lambda: (
                joinedload(User.addresses).joinedload(
                    User.orders.of_type(Order)
                ),
            ),
            r'ORM mapped entity or attribute "User.orders" does not link '
            r'from relationship "User.addresses"',
        )

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
        sess = fixture_session()
        with expect_raises_message(orm_exc.LoaderStrategyException, message):
            # accommodate Load() objects that will raise
            # on construction
            if callable(options):
                options = options()

            # accommodate UnboundLoad objects that will raise
            # only when compile state is set up
            sess.query(*entity_list).options(*options)._compile_state()

    def _assert_eager_with_entity_exception(
        self, entity_list, options, message
    ):
        sess = fixture_session()
        with expect_raises_message(sa.exc.ArgumentError, message):
            # accommodate Load() objects that will raise
            # on construction
            if callable(options):
                options = options()

            # accommodate UnboundLoad objects that will raise
            # only when compile state is set up
            sess.query(*entity_list).options(*options)._compile_state()

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
    def test_missing_attr_wpoly_subclasss(self):
        s = fixture_session()

        wp = with_polymorphic(Person, [Manager], flat=True)

        assert_raises_message(
            sa.exc.ArgumentError,
            r"Mapped class Mapper\[Manager\(managers\)\] does not apply to "
            "any of "
            r"the root entities in this query, e.g. "
            r"with_polymorphic\(Person, \[Manager\]\).",
            s.query(wp).options(load_only(Manager.status))._compile_state,
        )

    def test_missing_attr_of_type_subclass_one(self):
        s = fixture_session()

        e1 = with_polymorphic(Person, [Engineer])
        assert_raises_message(
            sa.exc.ArgumentError,
            r'ORM mapped entity or attribute "Manager.manager_name" does '
            r"not link from "
            r'relationship "Company.employees.'
            r'of_type\(with_polymorphic\(Person, \[Engineer\]\)\)".$',
            lambda: s.query(Company)
            .options(
                joinedload(Company.employees.of_type(e1)).load_only(
                    Manager.manager_name
                )
            )
            ._compile_state(),
        )

    def test_missing_attr_of_type_subclass_two(self):
        s = fixture_session()

        assert_raises_message(
            sa.exc.ArgumentError,
            r'ORM mapped entity or attribute "Manager.manager_name" does '
            r"not link from "
            r'relationship "Company.employees.'
            r'of_type\(Mapper\[Engineer\(engineers\)\]\)".$',
            lambda: s.query(Company)
            .options(
                joinedload(Company.employees.of_type(Engineer)).load_only(
                    Manager.manager_name
                )
            )
            ._compile_state(),
        )

    def test_missing_attr_of_type_subclass_name_matches(self):
        s = fixture_session()

        # the name "status" is present on Engineer also, make sure
        # that doesn't get mixed up here
        assert_raises_message(
            sa.exc.ArgumentError,
            r'ORM mapped entity or attribute "Manager.status" does '
            r"not link from "
            r'relationship "Company.employees.'
            r'of_type\(Mapper\[Engineer\(engineers\)\]\)".$',
            lambda: s.query(Company)
            .options(
                joinedload(Company.employees.of_type(Engineer)).load_only(
                    Manager.status
                )
            )
            ._compile_state(),
        )

    def test_missing_attr_of_type_wpoly_subclass(self):
        s = fixture_session()

        wp = with_polymorphic(Person, [Manager], flat=True)

        assert_raises_message(
            sa.exc.ArgumentError,
            r'ORM mapped entity or attribute "Manager.manager_name" does '
            r"not link from "
            r'relationship "Company.employees.'
            r'of_type\(with_polymorphic\(Person, \[Manager\]\)\)".$',
            lambda: s.query(Company)
            .options(
                joinedload(Company.employees.of_type(wp)).load_only(
                    Manager.manager_name
                )
            )
            ._compile_state(),
        )

    @testing.variation("use_options", [True, False])
    def test_missing_attr_is_missing_of_type_for_subtype(self, use_options):
        s = fixture_session()

        with expect_raises_message(
            sa.exc.ArgumentError,
            r"ORM mapped entity or attribute "
            r'(?:"Mapper\[Engineer\(engineers\)\]"|"Engineer.engineer_name") '
            r'does not link from relationship "Company.employees".  Did you '
            r'mean to use "Company.employees.of_type\(Engineer\)" '
            r'or "loadopt.options'
            r'\(selectin_polymorphic\(Person, \[Engineer\]\), ...\)" \?',
        ):
            if use_options:
                s.query(Company).options(
                    joinedload(Company.employees).options(
                        defer(Engineer.engineer_name)
                    )
                )._compile_state()
            else:
                s.query(Company).options(
                    joinedload(Company.employees).defer(Engineer.engineer_name)
                )._compile_state()


class PickleTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "users",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(30), nullable=False),
        )
        Table(
            "addresses",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("user_id", None, ForeignKey("users.id")),
            Column("email_address", String(50), nullable=False),
        )

    @testing.fixture
    def user_address_fixture(self, registry):
        registry.map_imperatively(
            User,
            self.tables.users,
            properties={"addresses": relationship(Address)},
        )
        registry.map_imperatively(Address, self.tables.addresses)

        return User, Address

    def test_slots(self, user_address_fixture):
        User, Address = user_address_fixture

        opt = joinedload(User.addresses)

        assert not hasattr(opt, "__dict__")
        assert not hasattr(opt.context[0], "__dict__")

    def test_pickle_relationship_loader(self, user_address_fixture):
        User, Address = user_address_fixture

        opt = joinedload(User.addresses)

        pickled = pickle.dumps(opt)

        opt2 = pickle.loads(pickled)

        is_not(opt, opt2)
        assert isinstance(opt, Load)
        assert isinstance(opt2, Load)

        for k in opt.__slots__:
            eq_(getattr(opt, k), getattr(opt2, k))


class LocalOptsTest(PathTest, QueryTest):
    @classmethod
    def setup_test_class(cls):
        def some_col_opt_only(self, key, opts):
            return self._set_column_strategy((key,), None, opts)

        strategy_options._AbstractLoad.some_col_opt_only = some_col_opt_only

        def some_col_opt_strategy(loadopt, key, opts):
            return loadopt._set_column_strategy(
                (key,), {"deferred": True, "instrument": True}, opts
            )

        strategy_options._AbstractLoad.some_col_opt_strategy = (
            some_col_opt_strategy
        )

    def _assert_attrs(self, opts, expected):
        User = self.classes.User

        s = fixture_session()
        q1 = s.query(User).options(*opts)
        attr = q1._compile_context().attributes

        key = (
            "loader",
            tuple(inspect(User)._path_registry[User.name.property]),
        )
        eq_(attr[key].local_opts, expected)

    def test_single_opt_only(self):
        User = self.classes.User

        opt = strategy_options.Load(User).some_col_opt_only(
            User.name, {"foo": "bar"}
        )
        self._assert_attrs([opt], {"foo": "bar"})

    def test_bound_multiple_opt_only(self):
        User = self.classes.User
        opts = [
            Load(User)
            .some_col_opt_only(User.name, {"foo": "bar"})
            .some_col_opt_only(User.name, {"bat": "hoho"})
        ]
        self._assert_attrs(opts, {"foo": "bar", "bat": "hoho"})

    def test_bound_strat_opt_recvs_from_optonly(self):
        User = self.classes.User
        opts = [
            Load(User)
            .some_col_opt_only(User.name, {"foo": "bar"})
            .some_col_opt_strategy(User.name, {"bat": "hoho"})
        ]
        self._assert_attrs(opts, {"foo": "bar", "bat": "hoho"})

    def test_bound_opt_only_adds_to_strat(self):
        User = self.classes.User
        opts = [
            Load(User)
            .some_col_opt_strategy(User.name, {"bat": "hoho"})
            .some_col_opt_only(User.name, {"foo": "bar"})
        ]
        self._assert_attrs(opts, {"foo": "bar", "bat": "hoho"})


class SubOptionsTest(PathTest, QueryTest):
    run_create_tables = False
    run_inserts = None
    run_deletes = None

    def _assert_opts(self, q, sub_opt, non_sub_opts):
        attr_a = {}

        q1 = q.options(sub_opt)._compile_context()
        q2 = q.options(*non_sub_opts)._compile_context()

        attr_a = {
            k: v
            for k, v in q1.attributes.items()
            if isinstance(k, tuple) and k[0] == "loader"
        }
        attr_b = {
            k: v
            for k, v in q2.attributes.items()
            if isinstance(k, tuple) and k[0] == "loader"
        }

        def strat_as_tuple(strat):
            return (
                strat.strategy,
                strat.local_opts,
                getattr(strat, "_of_type", None),
                strat.is_class_strategy,
                strat.is_opts_only,
            )

        eq_(
            {path: strat_as_tuple(load) for path, load in attr_a.items()},
            {path: strat_as_tuple(load) for path, load in attr_b.items()},
        )

    def test_one(self):
        User, Address, Order, Item, SubItem = self.classes(
            "User", "Address", "Order", "Item", "SubItem"
        )
        sub_opt = joinedload(User.orders).options(
            joinedload(Order.items).options(defer(Item.description)),
            defer(Order.description),
        )
        non_sub_opts = [
            joinedload(User.orders),
            defaultload(User.orders)
            .joinedload(Order.items)
            .defer(Item.description),
            defaultload(User.orders).defer(Order.description),
        ]

        sess = fixture_session()
        self._assert_opts(sess.query(User), sub_opt, non_sub_opts)

    def test_two(self):
        User, Address, Order, Item, SubItem = self.classes(
            "User", "Address", "Order", "Item", "SubItem"
        )

        sub_opt = defaultload(User.orders).options(
            joinedload(Order.items),
            defaultload(Order.items).options(subqueryload(Item.keywords)),
            defer(Order.description),
        )
        non_sub_opts = [
            defaultload(User.orders)
            .joinedload(Order.items)
            .subqueryload(Item.keywords),
            defaultload(User.orders).defer(Order.description),
        ]

        sess = fixture_session()
        self._assert_opts(sess.query(User), sub_opt, non_sub_opts)

    def test_three(self):
        User, Address, Order, Item, SubItem = self.classes(
            "User", "Address", "Order", "Item", "SubItem"
        )
        sub_opt = defaultload(User.orders).options(defer("*"))
        non_sub_opts = [defaultload(User.orders).defer("*")]
        sess = fixture_session()
        self._assert_opts(sess.query(User), sub_opt, non_sub_opts)

    def test_four(self):
        User, Address, Order, Item, SubItem, Keyword = self.classes(
            "User", "Address", "Order", "Item", "SubItem", "Keyword"
        )
        sub_opt = joinedload(User.orders).options(
            defer(Order.description),
            joinedload(Order.items).options(
                joinedload(Item.keywords).options(defer(Keyword.name)),
                defer(Item.description),
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
        self._assert_opts(sess.query(User), sub_opt, non_sub_opts)

    def test_five(self):
        User, Address, Order, Item, SubItem, Keyword = self.classes(
            "User", "Address", "Order", "Item", "SubItem", "Keyword"
        )
        sub_opt = joinedload(User.orders).options(load_only(Order.description))
        non_sub_opts = [
            joinedload(User.orders),
            defaultload(User.orders).load_only(Order.description),
        ]
        sess = fixture_session()
        self._assert_opts(sess.query(User), sub_opt, non_sub_opts)

    def test_invalid_one(self):
        User, Address, Order, Item, SubItem = self.classes(
            "User", "Address", "Order", "Item", "SubItem"
        )

        with expect_raises_message(
            sa.exc.ArgumentError,
            r'ORM mapped entity or attribute "Item.keywords" does '
            r"not link from "
            r'relationship "User.orders"',
        ):
            [
                joinedload(User.orders).joinedload(Item.keywords),
                defaultload(User.orders).joinedload(Order.items),
            ]
        with expect_raises_message(
            sa.exc.ArgumentError,
            r'ORM mapped entity or attribute "Item.keywords" does '
            r"not link from "
            r'relationship "User.orders"',
        ):
            joinedload(User.orders).options(
                joinedload(Item.keywords), joinedload(Order.items)
            )


class MapperOptionsTest(_fixtures.FixtureTest):
    def test_synonym_options(self):
        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User,
        )

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties=dict(
                addresses=relationship(
                    self.mapper_registry.map_imperatively(Address, addresses),
                    lazy="select",
                    order_by=addresses.c.id,
                ),
                adlist=synonym("addresses"),
            ),
        )

        def go():
            sess = fixture_session()
            u = (
                sess.query(User)
                .order_by(User.id)
                .options(sa.orm.joinedload(User.adlist))
                .filter_by(name="jack")
            ).one()
            eq_(u.adlist, [self.static.user_address_result[0].addresses[0]])

        self.assert_sql_count(testing.db, go, 1)

    def test_eager_options(self):
        """A lazy relationship can be upgraded to an eager relationship."""

        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User,
        )

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties=dict(
                addresses=relationship(
                    self.mapper_registry.map_imperatively(Address, addresses),
                    order_by=addresses.c.id,
                )
            ),
        )

        sess = fixture_session()
        result = (
            sess.query(User)
            .order_by(User.id)
            .options(sa.orm.joinedload(User.addresses))
        ).all()

        def go():
            eq_(result, self.static.user_address_result)

        self.sql_count_(0, go)

    def test_eager_options_with_limit(self):
        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User,
        )

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties=dict(
                addresses=relationship(
                    self.mapper_registry.map_imperatively(Address, addresses),
                    lazy="select",
                )
            ),
        )

        sess = fixture_session()
        u = (
            sess.query(User)
            .options(sa.orm.joinedload(User.addresses))
            .filter_by(id=8)
        ).one()

        def go():
            eq_(u.id, 8)
            eq_(len(u.addresses), 3)

        self.sql_count_(0, go)

        sess.expunge_all()

        u = sess.query(User).filter_by(id=8).one()
        eq_(u.id, 8)
        eq_(len(u.addresses), 3)

    def test_lazy_options_with_limit(self):
        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User,
        )

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties=dict(
                addresses=relationship(
                    self.mapper_registry.map_imperatively(Address, addresses),
                    lazy="joined",
                )
            ),
        )

        sess = fixture_session()
        u = (
            sess.query(User)
            .options(sa.orm.lazyload(User.addresses))
            .filter_by(id=8)
        ).one()

        def go():
            eq_(u.id, 8)
            eq_(len(u.addresses), 3)

        self.sql_count_(1, go)

    def test_eager_degrade(self):
        """An eager relationship automatically degrades to a lazy relationship
        if eager columns are not available"""

        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User,
        )

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties=dict(
                addresses=relationship(
                    self.mapper_registry.map_imperatively(Address, addresses),
                    lazy="joined",
                    order_by=addresses.c.id,
                )
            ),
        )

        sess = fixture_session()
        # first test straight eager load, 1 statement

        def go():
            result = sess.query(User).order_by(User.id).all()
            eq_(result, self.static.user_address_result)

        self.sql_count_(1, go)

        sess.expunge_all()

        # then select just from users.  run it into instances.
        # then assert the data, which will launch 3 more lazy loads
        # (previous users in session fell out of scope and were removed from
        # session's identity map)
        r = sess.connection().execute(users.select().order_by(users.c.id))

        ctx = sess.query(User)._compile_context()

        def go():
            result = loading.instances(r, ctx).scalars().unique()
            result = list(result)
            eq_(result, self.static.user_address_result)

        self.sql_count_(4, go)

    def test_eager_degrade_deep(self):
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
            item_keywords,
            Order,
            addresses,
        ) = (
            self.tables.users,
            self.classes.Keyword,
            self.tables.items,
            self.tables.order_items,
            self.tables.orders,
            self.classes.Item,
            self.classes.User,
            self.classes.Address,
            self.tables.keywords,
            self.tables.item_keywords,
            self.classes.Order,
            self.tables.addresses,
        )

        # test with a deeper set of eager loads.  when we first load the three
        # users, they will have no addresses or orders.  the number of lazy
        # loads when traversing the whole thing will be three for the
        # addresses and three for the orders.
        self.mapper_registry.map_imperatively(Address, addresses)

        self.mapper_registry.map_imperatively(Keyword, keywords)

        self.mapper_registry.map_imperatively(
            Item,
            items,
            properties=dict(
                keywords=relationship(
                    Keyword,
                    secondary=item_keywords,
                    lazy="joined",
                    order_by=item_keywords.c.keyword_id,
                )
            ),
        )

        self.mapper_registry.map_imperatively(
            Order,
            orders,
            properties=dict(
                items=relationship(
                    Item,
                    secondary=order_items,
                    lazy="joined",
                    order_by=order_items.c.item_id,
                )
            ),
        )

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties=dict(
                addresses=relationship(
                    Address, lazy="joined", order_by=addresses.c.id
                ),
                orders=relationship(
                    Order, lazy="joined", order_by=orders.c.id
                ),
            ),
        )

        sess = fixture_session()

        # first test straight eager load, 1 statement
        def go():
            result = sess.query(User).order_by(User.id).all()
            eq_(result, self.static.user_all_result)

        self.assert_sql_count(testing.db, go, 1)

        sess.expunge_all()

        # then select just from users.  run it into instances.
        # then assert the data, which will launch 6 more lazy loads
        r = sess.connection().execute(users.select())

        ctx = sess.query(User)._compile_context()

        def go():
            result = loading.instances(r, ctx).scalars().unique()
            result = list(result)
            eq_(result, self.static.user_all_result)

        self.assert_sql_count(testing.db, go, 6)

    def test_lazy_options(self):
        """An eager relationship can be upgraded to a lazy relationship."""

        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User,
        )

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties=dict(
                addresses=relationship(
                    self.mapper_registry.map_imperatively(Address, addresses),
                    lazy="joined",
                )
            ),
        )

        sess = fixture_session()
        result = (
            sess.query(User)
            .order_by(User.id)
            .options(sa.orm.lazyload(User.addresses))
        ).all()

        def go():
            eq_(result, self.static.user_address_result)

        self.sql_count_(4, go)

    def test_option_propagate(self):
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
            User, users, properties=dict(orders=relationship(Order))
        )
        self.mapper_registry.map_imperatively(
            Order,
            orders,
            properties=dict(items=relationship(Item, secondary=order_items)),
        )
        self.mapper_registry.map_imperatively(Item, items)

        sess = fixture_session()

        oalias = aliased(Order)

        # this one is *really weird*
        # here's what the test originally had.  note two different strategies
        # for Order.items
        #
        # opt1 = sa.orm.joinedload(User.orders, Order.items)
        # opt2 = sa.orm.contains_eager(User.orders, Order.items, alias=oalias)

        # here's how it would translate.  note that the second
        # contains_eager() for Order.items just got cancelled out,
        # I guess the joinedload() would somehow overrule the contains_eager
        #
        # opt1 = Load(User).defaultload(User.orders).joinedload(Order.items)
        # opt2 = Load(User).contains_eager(User.orders, alias=oalias)

        # setting up the options more specifically works however with
        # both the old way and the new way
        opt1 = sa.orm.joinedload(User.orders, Order.items)
        opt2 = sa.orm.contains_eager(User.orders, alias=oalias)

        u1 = (
            sess.query(User)
            .join(oalias, User.orders)
            .options(opt1, opt2)
            .first()
        )
        ustate = attributes.instance_state(u1)
        assert opt1 in ustate.load_options
        assert opt2 not in ustate.load_options

    @testing.combinations(
        (
            lambda User, Order: (
                joinedload(User.orders),
                contains_eager(User.orders),
            ),
            r"Loader strategies for ORM Path\[Mapper\[User\(users\)\] -> "
            r"User.orders -> Mapper\[Order\(orders\)\]\] conflict",
        ),
        (
            lambda User, Order: (
                joinedload(User.orders),
                joinedload(User.orders).joinedload(Order.items),
            ),
            None,
        ),
        (
            lambda User, Order: (
                joinedload(User.orders),
                joinedload(User.orders, innerjoin=True).joinedload(
                    Order.items
                ),
            ),
            r"Loader strategies for ORM Path\[Mapper\[User\(users\)\] -> "
            r"User.orders -> Mapper\[Order\(orders\)\]\] conflict",
        ),
        (
            lambda User: (defer(User.name), undefer(User.name)),
            r"Loader strategies for ORM Path\[Mapper\[User\(users\)\] -> "
            r"User.name\] conflict",
        ),
    )
    def test_conflicts(self, make_opt, errmsg):
        """introduce a new error for conflicting options in SQLAlchemy 2.0.

        This case seems to be fairly difficult to come up with randomly
        so let's see if we can refuse to guess for this case.

        """
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
            User, users, properties=dict(orders=relationship(Order))
        )
        self.mapper_registry.map_imperatively(
            Order,
            orders,
            properties=dict(items=relationship(Item, secondary=order_items)),
        )
        self.mapper_registry.map_imperatively(Item, items)

        sess = fixture_session()

        opt = testing.resolve_lambda(
            make_opt, User=User, Order=Order, Item=Item
        )

        if errmsg:
            with expect_raises_message(sa.exc.InvalidRequestError, errmsg):
                sess.query(User).options(opt)._compile_context()
        else:
            sess.query(User).options(opt)._compile_context()
