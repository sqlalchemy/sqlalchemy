import sqlalchemy as sa
from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import inspect
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy.orm import aliased
from sqlalchemy.orm import class_mapper
from sqlalchemy.orm import column_property
from sqlalchemy.orm import create_session
from sqlalchemy.orm import defaultload
from sqlalchemy.orm import defer
from sqlalchemy.orm import exc as orm_exc
from sqlalchemy.orm import joinedload
from sqlalchemy.orm import lazyload
from sqlalchemy.orm import Load
from sqlalchemy.orm import load_only
from sqlalchemy.orm import mapper
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session
from sqlalchemy.orm import strategy_options
from sqlalchemy.orm import subqueryload
from sqlalchemy.orm import util as orm_util
from sqlalchemy.orm import with_polymorphic
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.assertions import assert_raises_message
from sqlalchemy.testing.assertions import eq_
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

        mapper(
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


class PathTest(object):
    def _make_path(self, path):
        r = []
        for i, item in enumerate(path):
            if i % 2 == 0:
                if isinstance(item, type):
                    item = class_mapper(item)
            else:
                if isinstance(item, str):
                    item = inspect(r[-1]).mapper.attrs[item]
            r.append(item)
        return tuple(r)

    def _make_path_registry(self, path):
        return orm_util.PathRegistry.coerce(self._make_path(path))

    def _assert_path_result(self, opt, q, paths):
        q._attributes = q._attributes.copy()
        attr = {}

        if isinstance(opt, strategy_options._UnboundLoad):
            for val in opt._to_bind:
                val._bind_loader(
                    [ent.entity_zero for ent in q._mapper_entities],
                    q._current_path,
                    attr,
                    False,
                )
        else:
            opt._process(q, True)
            attr = q._attributes

        assert_paths = [k[1] for k in attr]
        eq_(
            set([p for p in assert_paths]),
            set([self._make_path(p) for p in paths]),
        )


class LoadTest(PathTest, QueryTest):
    def test_str(self):
        User = self.classes.User
        result = Load(User)
        result.strategy = (("deferred", False), ("instrument", True))
        eq_(
            str(result),
            "Load(strategy=(('deferred', False), ('instrument', True)))",
        )

    def test_gen_path_attr_entity(self):
        User = self.classes.User
        Address = self.classes.Address

        result = Load(User)
        eq_(
            result._generate_path(
                inspect(User)._path_registry,
                User.addresses,
                None,
                "relationship",
            ),
            self._make_path_registry([User, "addresses", Address]),
        )

    def test_gen_path_attr_column(self):
        User = self.classes.User

        result = Load(User)
        eq_(
            result._generate_path(
                inspect(User)._path_registry, User.name, None, "column"
            ),
            self._make_path_registry([User, "name"]),
        )

    def test_gen_path_string_entity(self):
        User = self.classes.User
        Address = self.classes.Address

        result = Load(User)
        eq_(
            result._generate_path(
                inspect(User)._path_registry, "addresses", None, "relationship"
            ),
            self._make_path_registry([User, "addresses", Address]),
        )

    def test_gen_path_string_column(self):
        User = self.classes.User

        result = Load(User)
        eq_(
            result._generate_path(
                inspect(User)._path_registry, "name", None, "column"
            ),
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
            result._generate_path,
            result.path,
            User.addresses,
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
            result._generate_path,
            inspect(User)._path_registry,
            Order.items,
            None,
            "relationship",
        )

    def test_gen_path_attr_entity_invalid_noraiseerr(self):
        User = self.classes.User
        Order = self.classes.Order

        result = Load(User)

        eq_(
            result._generate_path(
                inspect(User)._path_registry,
                Order.items,
                None,
                "relationship",
                False,
            ),
            None,
        )

    def test_set_strat_ent(self):
        User = self.classes.User

        l1 = Load(User)
        l2 = l1.joinedload("addresses")
        to_bind = l2.context.values()[0]
        eq_(
            l1.context,
            {("loader", self._make_path([User, "addresses"])): to_bind},
        )

    def test_set_strat_col(self):
        User = self.classes.User

        l1 = Load(User)
        l2 = l1.defer("name")
        l3 = list(l2.context.values())[0]
        eq_(l1.context, {("loader", self._make_path([User, "name"])): l3})


class OfTypePathingTest(PathTest, QueryTest):
    def _fixture(self):
        User, Address = self.classes.User, self.classes.Address
        Dingaling = self.classes.Dingaling
        address_table = self.tables.addresses

        class SubAddr(Address):
            pass

        mapper(
            SubAddr,
            inherits=Address,
            properties={
                "sub_attr": column_property(address_table.c.email_address),
                "dings": relationship(Dingaling),
            },
        )

        return User, Address, SubAddr

    def test_oftype_only_col_attr_unbound(self):
        User, Address, SubAddr = self._fixture()

        l1 = defaultload(User.addresses.of_type(SubAddr)).defer(
            SubAddr.sub_attr
        )

        sess = Session()
        q = sess.query(User)
        self._assert_path_result(
            l1,
            q,
            [(User, "addresses"), (User, "addresses", SubAddr, "sub_attr")],
        )

    def test_oftype_only_col_attr_bound(self):
        User, Address, SubAddr = self._fixture()

        l1 = (
            Load(User)
            .defaultload(User.addresses.of_type(SubAddr))
            .defer(SubAddr.sub_attr)
        )

        sess = Session()
        q = sess.query(User)
        self._assert_path_result(
            l1,
            q,
            [(User, "addresses"), (User, "addresses", SubAddr, "sub_attr")],
        )

    def test_oftype_only_col_attr_string_unbound(self):
        User, Address, SubAddr = self._fixture()

        l1 = defaultload(User.addresses.of_type(SubAddr)).defer("sub_attr")

        sess = Session()
        q = sess.query(User)
        self._assert_path_result(
            l1,
            q,
            [(User, "addresses"), (User, "addresses", SubAddr, "sub_attr")],
        )

    def test_oftype_only_col_attr_string_bound(self):
        User, Address, SubAddr = self._fixture()

        l1 = (
            Load(User)
            .defaultload(User.addresses.of_type(SubAddr))
            .defer("sub_attr")
        )

        sess = Session()
        q = sess.query(User)
        self._assert_path_result(
            l1,
            q,
            [(User, "addresses"), (User, "addresses", SubAddr, "sub_attr")],
        )

    def test_oftype_only_rel_attr_unbound(self):
        User, Address, SubAddr = self._fixture()

        l1 = defaultload(User.addresses.of_type(SubAddr)).joinedload(
            SubAddr.dings
        )

        sess = Session()
        q = sess.query(User)
        self._assert_path_result(
            l1, q, [(User, "addresses"), (User, "addresses", SubAddr, "dings")]
        )

    def test_oftype_only_rel_attr_bound(self):
        User, Address, SubAddr = self._fixture()

        l1 = (
            Load(User)
            .defaultload(User.addresses.of_type(SubAddr))
            .joinedload(SubAddr.dings)
        )

        sess = Session()
        q = sess.query(User)
        self._assert_path_result(
            l1, q, [(User, "addresses"), (User, "addresses", SubAddr, "dings")]
        )

    def test_oftype_only_rel_attr_string_unbound(self):
        User, Address, SubAddr = self._fixture()

        l1 = defaultload(User.addresses.of_type(SubAddr)).joinedload("dings")

        sess = Session()
        q = sess.query(User)
        self._assert_path_result(
            l1, q, [(User, "addresses"), (User, "addresses", SubAddr, "dings")]
        )

    def test_oftype_only_rel_attr_string_bound(self):
        User, Address, SubAddr = self._fixture()

        l1 = (
            Load(User)
            .defaultload(User.addresses.of_type(SubAddr))
            .defer("sub_attr")
        )

        sess = Session()
        q = sess.query(User)
        self._assert_path_result(
            l1,
            q,
            [(User, "addresses"), (User, "addresses", SubAddr, "sub_attr")],
        )


class OptionsTest(PathTest, QueryTest):
    def _option_fixture(self, *arg):
        return strategy_options._UnboundLoad._from_keys(
            strategy_options._UnboundLoad.joinedload, arg, True, {}
        )

    def test_get_path_one_level_string(self):
        User = self.classes.User

        sess = Session()
        q = sess.query(User)

        opt = self._option_fixture("addresses")
        self._assert_path_result(opt, q, [(User, "addresses")])

    def test_get_path_one_level_attribute(self):
        User = self.classes.User

        sess = Session()
        q = sess.query(User)

        opt = self._option_fixture(User.addresses)
        self._assert_path_result(opt, q, [(User, "addresses")])

    def test_path_on_entity_but_doesnt_match_currentpath(self):
        User, Address = self.classes.User, self.classes.Address

        # ensure "current path" is fully consumed before
        # matching against current entities.
        # see [ticket:2098]
        sess = Session()
        q = sess.query(User)
        opt = self._option_fixture("email_address", "id")
        q = sess.query(Address)._with_current_path(
            orm_util.PathRegistry.coerce(
                [inspect(User), inspect(User).attrs.addresses]
            )
        )
        self._assert_path_result(opt, q, [])

    def test_get_path_one_level_with_unrelated(self):
        Order = self.classes.Order

        sess = Session()
        q = sess.query(Order)
        opt = self._option_fixture("addresses")
        self._assert_path_result(opt, q, [])

    def test_path_multilevel_string(self):
        Item, User, Order = (
            self.classes.Item,
            self.classes.User,
            self.classes.Order,
        )

        sess = Session()
        q = sess.query(User)

        opt = self._option_fixture("orders.items.keywords")
        self._assert_path_result(
            opt,
            q,
            [
                (User, "orders"),
                (User, "orders", Order, "items"),
                (User, "orders", Order, "items", Item, "keywords"),
            ],
        )

    def test_path_multilevel_attribute(self):
        Item, User, Order = (
            self.classes.Item,
            self.classes.User,
            self.classes.Order,
        )

        sess = Session()
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

    def test_with_current_matching_string(self):
        Item, User, Order = (
            self.classes.Item,
            self.classes.User,
            self.classes.Order,
        )

        sess = Session()
        q = sess.query(Item)._with_current_path(
            self._make_path_registry([User, "orders", Order, "items"])
        )

        opt = self._option_fixture("orders.items.keywords")
        self._assert_path_result(opt, q, [(Item, "keywords")])

    def test_with_current_matching_attribute(self):
        Item, User, Order = (
            self.classes.Item,
            self.classes.User,
            self.classes.Order,
        )

        sess = Session()
        q = sess.query(Item)._with_current_path(
            self._make_path_registry([User, "orders", Order, "items"])
        )

        opt = self._option_fixture(User.orders, Order.items, Item.keywords)
        self._assert_path_result(opt, q, [(Item, "keywords")])

    def test_with_current_nonmatching_string(self):
        Item, User, Order = (
            self.classes.Item,
            self.classes.User,
            self.classes.Order,
        )

        sess = Session()
        q = sess.query(Item)._with_current_path(
            self._make_path_registry([User, "orders", Order, "items"])
        )

        opt = self._option_fixture("keywords")
        self._assert_path_result(opt, q, [])

        opt = self._option_fixture("items.keywords")
        self._assert_path_result(opt, q, [])

    def test_with_current_nonmatching_attribute(self):
        Item, User, Order = (
            self.classes.Item,
            self.classes.User,
            self.classes.Order,
        )

        sess = Session()
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

        sess = Session()
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
        sess = Session()
        q = sess.query(Item)._with_current_path(
            self._make_path_registry([inspect(ac), "orders", Order, "items"])
        )

        opt = self._option_fixture(ac.orders, Order.items, Item.keywords)
        self._assert_path_result(opt, q, [(Item, "keywords")])

        opt = self._option_fixture(ac.orders, Order.items)
        self._assert_path_result(opt, q, [])

    def test_from_base_to_subclass_attr(self):
        Dingaling, Address = self.classes.Dingaling, self.classes.Address

        sess = Session()

        class SubAddr(Address):
            pass

        mapper(
            SubAddr,
            inherits=Address,
            properties={"flub": relationship(Dingaling)},
        )

        q = sess.query(Address)
        opt = self._option_fixture(SubAddr.flub)

        self._assert_path_result(opt, q, [(SubAddr, "flub")])

    def test_from_subclass_to_subclass_attr(self):
        Dingaling, Address = self.classes.Dingaling, self.classes.Address

        sess = Session()

        class SubAddr(Address):
            pass

        mapper(
            SubAddr,
            inherits=Address,
            properties={"flub": relationship(Dingaling)},
        )

        q = sess.query(SubAddr)
        opt = self._option_fixture(SubAddr.flub)

        self._assert_path_result(opt, q, [(SubAddr, "flub")])

    def test_from_base_to_base_attr_via_subclass(self):
        Dingaling, Address = self.classes.Dingaling, self.classes.Address

        sess = Session()

        class SubAddr(Address):
            pass

        mapper(
            SubAddr,
            inherits=Address,
            properties={"flub": relationship(Dingaling)},
        )

        q = sess.query(Address)
        opt = self._option_fixture(SubAddr.user)

        self._assert_path_result(
            opt, q, [(Address, inspect(Address).attrs.user)]
        )

    def test_of_type(self):
        User, Address = self.classes.User, self.classes.Address

        sess = Session()

        class SubAddr(Address):
            pass

        mapper(SubAddr, inherits=Address)

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

    def test_of_type_string_attr(self):
        User, Address = self.classes.User, self.classes.Address

        sess = Session()

        class SubAddr(Address):
            pass

        mapper(SubAddr, inherits=Address)

        q = sess.query(User)
        opt = self._option_fixture(User.addresses.of_type(SubAddr), "user")

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

    def test_of_type_plus_level(self):
        Dingaling, User, Address = (
            self.classes.Dingaling,
            self.classes.User,
            self.classes.Address,
        )

        sess = Session()

        class SubAddr(Address):
            pass

        mapper(
            SubAddr,
            inherits=Address,
            properties={"flub": relationship(Dingaling)},
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

        sess = Session()
        ualias = aliased(User)
        q = sess.query(ualias)
        opt = self._option_fixture(ualias.addresses)
        self._assert_path_result(opt, q, [(inspect(ualias), "addresses")])

    def test_with_current_aliased_single(self):
        User, Address = self.classes.User, self.classes.Address

        sess = Session()
        ualias = aliased(User)
        q = sess.query(ualias)._with_current_path(
            self._make_path_registry([Address, "user"])
        )
        opt = self._option_fixture(Address.user, ualias.addresses)
        self._assert_path_result(opt, q, [(inspect(ualias), "addresses")])

    def test_with_current_aliased_single_nonmatching_option(self):
        User, Address = self.classes.User, self.classes.Address

        sess = Session()
        ualias = aliased(User)
        q = sess.query(User)._with_current_path(
            self._make_path_registry([Address, "user"])
        )
        opt = self._option_fixture(Address.user, ualias.addresses)
        self._assert_path_result(opt, q, [])

    def test_with_current_aliased_single_nonmatching_entity(self):
        User, Address = self.classes.User, self.classes.Address

        sess = Session()
        ualias = aliased(User)
        q = sess.query(ualias)._with_current_path(
            self._make_path_registry([Address, "user"])
        )
        opt = self._option_fixture(Address.user, User.addresses)
        self._assert_path_result(opt, q, [])

    def test_multi_entity_opt_on_second(self):
        Item = self.classes.Item
        Order = self.classes.Order
        opt = self._option_fixture(Order.items)
        sess = Session()
        q = sess.query(Item, Order)
        self._assert_path_result(opt, q, [(Order, "items")])

    def test_multi_entity_opt_on_string(self):
        Item = self.classes.Item
        Order = self.classes.Order
        opt = self._option_fixture("items")
        sess = Session()
        q = sess.query(Item, Order)
        self._assert_path_result(opt, q, [])

    def test_multi_entity_no_mapped_entities(self):
        Item = self.classes.Item
        Order = self.classes.Order
        opt = self._option_fixture("items")
        sess = Session()
        q = sess.query(Item.id, Order.id)
        self._assert_path_result(opt, q, [])

    def test_path_exhausted(self):
        User = self.classes.User
        Item = self.classes.Item
        Order = self.classes.Order
        opt = self._option_fixture(User.orders)
        sess = Session()
        q = sess.query(Item)._with_current_path(
            self._make_path_registry([User, "orders", Order, "items"])
        )
        self._assert_path_result(opt, q, [])

    def test_chained(self):
        User = self.classes.User
        Order = self.classes.Order
        sess = Session()
        q = sess.query(User)
        opt = self._option_fixture(User.orders).joinedload("items")
        self._assert_path_result(
            opt, q, [(User, "orders"), (User, "orders", Order, "items")]
        )

    def test_chained_plus_dotted(self):
        User = self.classes.User
        Order = self.classes.Order
        Item = self.classes.Item
        sess = Session()
        q = sess.query(User)
        opt = self._option_fixture("orders.items").joinedload("keywords")
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
        sess = Session()
        q = sess.query(User)
        opt = self._option_fixture(User.orders, Order.items).joinedload(
            "keywords"
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
        sess = Session()

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

    def test_option_with_mapper_basestring(self):
        Item = self.classes.Item

        self._assert_option([Item], "keywords")

    def test_option_with_mapper_PropCompatator(self):
        Item = self.classes.Item

        self._assert_option([Item], Item.keywords)

    def test_option_with_mapper_then_column_basestring(self):
        Item = self.classes.Item

        self._assert_option([Item, Item.id], "keywords")

    def test_option_with_mapper_then_column_PropComparator(self):
        Item = self.classes.Item

        self._assert_option([Item, Item.id], Item.keywords)

    def test_option_with_column_then_mapper_basestring(self):
        Item = self.classes.Item

        self._assert_option([Item.id, Item], "keywords")

    def test_option_with_column_then_mapper_PropComparator(self):
        Item = self.classes.Item

        self._assert_option([Item.id, Item], Item.keywords)

    def test_option_with_column_basestring(self):
        Item = self.classes.Item

        message = (
            "Query has only expression-based entities - can't "
            'find property named "keywords".'
        )
        self._assert_eager_with_just_column_exception(
            Item.id, "keywords", message
        )

    def test_option_with_column_PropComparator(self):
        Item = self.classes.Item

        self._assert_eager_with_just_column_exception(
            Item.id,
            Item.keywords,
            "Query has only expression-based entities, which do not apply "
            'to relationship property "Item.keywords"',
        )

    def test_option_against_nonexistent_PropComparator(self):
        Item = self.classes.Item
        Keyword = self.classes.Keyword
        self._assert_eager_with_entity_exception(
            [Keyword],
            (joinedload(Item.keywords),),
            'Mapped attribute "Item.keywords" does not apply to any of the '
            "root entities in this query, e.g. mapped class "
            "Keyword->keywords. Please specify the full path from one of "
            "the root entities to the target attribute. ",
        )

    def test_option_against_nonexistent_basestring(self):
        Item = self.classes.Item
        self._assert_eager_with_entity_exception(
            [Item],
            (joinedload("foo"),),
            'Can\'t find property named "foo" on mapped class '
            "Item->items in this Query.",
        )

    def test_option_against_nonexistent_twolevel_basestring(self):
        Item = self.classes.Item
        self._assert_eager_with_entity_exception(
            [Item],
            (joinedload("keywords.foo"),),
            'Can\'t find property named "foo" on mapped class '
            "Keyword->keywords in this Query.",
        )

    def test_option_against_nonexistent_twolevel_chained(self):
        Item = self.classes.Item
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
        self._assert_loader_strategy_exception(
            [Item],
            (joinedload("id").joinedload("keywords"),),
            'Can\'t apply "joined loader" strategy to property "Item.id", '
            'which is a "column property"; this loader strategy is '
            'intended to be used with a "relationship property".',
        )

    def test_col_option_against_relationship_basestring(self):
        Item = self.classes.Item
        self._assert_loader_strategy_exception(
            [Item],
            (load_only("keywords"),),
            'Can\'t apply "column loader" strategy to property '
            '"Item.keywords", which is a "relationship property"; this '
            'loader strategy is intended to be used with a "column property".',
        )

    def test_load_only_against_multi_entity_attr(self):
        User = self.classes.User
        Item = self.classes.Item
        self._assert_eager_with_entity_exception(
            [User, Item],
            (load_only(User.id, Item.id),),
            r"Can't apply wildcard \('\*'\) or load_only\(\) loader option "
            "to multiple entities mapped class User->users, mapped class "
            "Item->items. Specify loader options for each entity "
            "individually, such as "
            r"Load\(mapped class User->users\).some_option\('\*'\), "
            r"Load\(mapped class Item->items\).some_option\('\*'\).",
        )

    def test_col_option_against_relationship_attr(self):
        Item = self.classes.Item
        self._assert_loader_strategy_exception(
            [Item],
            (load_only(Item.keywords),),
            'Can\'t apply "column loader" strategy to property '
            '"Item.keywords", which is a "relationship property"; this '
            'loader strategy is intended to be used with a "column property".',
        )

    def test_option_against_multi_non_relation_twolevel_basestring(self):
        Item = self.classes.Item
        Keyword = self.classes.Keyword
        self._assert_loader_strategy_exception(
            [Keyword, Item],
            (joinedload("id").joinedload("keywords"),),
            'Can\'t apply "joined loader" strategy to property "Keyword.id", '
            'which is a "column property"; this loader strategy is intended '
            'to be used with a "relationship property".',
        )

    def test_option_against_multi_nonexistent_basestring(self):
        Item = self.classes.Item
        Keyword = self.classes.Keyword
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

    def test_option_against_wrong_multi_entity_type_attr_one(self):
        Item = self.classes.Item
        Keyword = self.classes.Keyword
        self._assert_loader_strategy_exception(
            [Keyword, Item],
            (joinedload(Keyword.id).joinedload(Item.keywords),),
            'Can\'t apply "joined loader" strategy to property "Keyword.id", '
            'which is a "column property"; this loader strategy is intended '
            'to be used with a "relationship property".',
        )

    def test_option_against_wrong_multi_entity_type_attr_two(self):
        Item = self.classes.Item
        Keyword = self.classes.Keyword
        self._assert_loader_strategy_exception(
            [Keyword, Item],
            (joinedload(Keyword.keywords).joinedload(Item.keywords),),
            'Can\'t apply "joined loader" strategy to property '
            '"Keyword.keywords", which is a "column property"; this loader '
            'strategy is intended to be used with a "relationship property".',
        )

    def test_option_against_wrong_multi_entity_type_attr_three(self):
        Item = self.classes.Item
        Keyword = self.classes.Keyword
        self._assert_eager_with_entity_exception(
            [Keyword.id, Item.id],
            (joinedload(Keyword.keywords).joinedload(Item.keywords),),
            "Query has only expression-based entities, which do not apply to "
            'column property "Keyword.keywords"',
        )

    def test_wrong_type_in_option(self):
        Item = self.classes.Item
        Keyword = self.classes.Keyword
        self._assert_eager_with_entity_exception(
            [Item],
            (joinedload(Keyword),),
            r"mapper option expects string key or list of attributes",
        )

    def test_non_contiguous_all_option(self):
        User = self.classes.User
        self._assert_eager_with_entity_exception(
            [User],
            (joinedload(User.addresses).joinedload(User.orders),),
            r"Attribute 'User.orders' does not link "
            "from element 'Mapper|Address|addresses'",
        )

    def test_non_contiguous_all_option_of_type(self):
        User = self.classes.User
        Order = self.classes.Order
        self._assert_eager_with_entity_exception(
            [User],
            (
                joinedload(User.addresses).joinedload(
                    User.orders.of_type(Order)
                ),
            ),
            r"Attribute 'User.orders' does not link "
            "from element 'Mapper|Address|addresses'",
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
        mapper(
            User,
            users,
            properties={
                "addresses": relationship(Address),
                "orders": relationship(Order),
            },
        )
        mapper(Address, addresses)
        mapper(Order, orders)
        keywords, items, item_keywords, Keyword, Item = (
            cls.tables.keywords,
            cls.tables.items,
            cls.tables.item_keywords,
            cls.classes.Keyword,
            cls.classes.Item,
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

    def _assert_option(self, entity_list, option):
        Item = self.classes.Item

        q = create_session().query(*entity_list).options(joinedload(option))
        key = ("loader", (inspect(Item), inspect(Item).attrs.keywords))
        assert key in q._attributes

    def _assert_loader_strategy_exception(self, entity_list, options, message):
        assert_raises_message(
            orm_exc.LoaderStrategyException,
            message,
            create_session().query(*entity_list).options,
            *options
        )

    def _assert_eager_with_entity_exception(
        self, entity_list, options, message
    ):
        assert_raises_message(
            sa.exc.ArgumentError,
            message,
            create_session().query(*entity_list).options,
            *options
        )

    def _assert_eager_with_just_column_exception(
        self, column, eager_option, message
    ):
        assert_raises_message(
            sa.exc.ArgumentError,
            message,
            create_session().query(column).options,
            joinedload(eager_option),
        )


class OptionsNoPropTestInh(_Polymorphic):
    def test_missing_attr_wpoly_subclasss(self):
        s = Session()

        wp = with_polymorphic(Person, [Manager], flat=True)

        assert_raises_message(
            sa.exc.ArgumentError,
            r'Mapped attribute "Manager.status" does not apply to any of '
            r"the root entities in this query, e.g. "
            r"with_polymorphic\(Person, \[Manager\]\).",
            s.query(wp).options,
            load_only(Manager.status),
        )

    def test_missing_attr_of_type_subclass(self):
        s = Session()

        assert_raises_message(
            sa.exc.ArgumentError,
            r'Attribute "Manager.manager_name" does not link from element '
            r'"with_polymorphic\(Person, \[Engineer\]\)".$',
            s.query(Company).options,
            joinedload(Company.employees.of_type(Engineer)).load_only(
                Manager.manager_name
            ),
        )

    def test_missing_attr_of_type_subclass_name_matches(self):
        s = Session()

        # the name "status" is present on Engineer also, make sure
        # that doesn't get mixed up here
        assert_raises_message(
            sa.exc.ArgumentError,
            r'Attribute "Manager.status" does not link from element '
            r'"with_polymorphic\(Person, \[Engineer\]\)".$',
            s.query(Company).options,
            joinedload(Company.employees.of_type(Engineer)).load_only(
                Manager.status
            ),
        )

    def test_missing_str_attr_of_type_subclass(self):
        s = Session()

        assert_raises_message(
            sa.exc.ArgumentError,
            r'Can\'t find property named "manager_name" on '
            r"mapped class Engineer->engineers in this Query.$",
            s.query(Company).options,
            joinedload(Company.employees.of_type(Engineer)).load_only(
                "manager_name"
            ),
        )

    def test_missing_attr_of_type_wpoly_subclass(self):
        s = Session()

        wp = with_polymorphic(Person, [Manager], flat=True)

        assert_raises_message(
            sa.exc.ArgumentError,
            r'Attribute "Manager.manager_name" does not link from '
            r'element "with_polymorphic\(Person, \[Manager\]\)".$',
            s.query(Company).options,
            joinedload(Company.employees.of_type(wp)).load_only(
                Manager.manager_name
            ),
        )

    def test_missing_attr_is_missing_of_type_for_alias(self):
        s = Session()

        pa = aliased(Person)

        assert_raises_message(
            sa.exc.ArgumentError,
            r'Attribute "AliasedClass_Person.name" does not link from '
            r'element "mapped class Person->people".  Did you mean to use '
            r"Company.employees.of_type\(AliasedClass_Person\)\?",
            s.query(Company).options,
            joinedload(Company.employees).load_only(pa.name),
        )

        q = s.query(Company).options(
            joinedload(Company.employees.of_type(pa)).load_only(pa.name)
        )
        orig_path = inspect(Company)._path_registry[
            Company.employees.property
        ][inspect(pa)][pa.name.property]
        key = ("loader", orig_path.natural_path)
        loader = q._attributes[key]
        eq_(loader.path, orig_path)


class PickleTest(PathTest, QueryTest):
    def _option_fixture(self, *arg):
        return strategy_options._UnboundLoad._from_keys(
            strategy_options._UnboundLoad.joinedload, arg, True, {}
        )

    def test_modern_opt_getstate(self):
        User = self.classes.User

        opt = self._option_fixture(User.addresses)
        to_bind = list(opt._to_bind)
        eq_(
            opt.__getstate__(),
            {
                "_is_chain_link": False,
                "local_opts": {},
                "is_class_strategy": False,
                "path": [(User, "addresses", None)],
                "propagate_to_loaders": True,
                "_of_type": None,
                "_to_bind": to_bind,
            },
        )

    def test_modern_opt_setstate(self):
        User = self.classes.User

        inner_opt = strategy_options._UnboundLoad.__new__(
            strategy_options._UnboundLoad
        )
        inner_state = {
            "_is_chain_link": False,
            "local_opts": {},
            "is_class_strategy": False,
            "path": [(User, "addresses", None)],
            "propagate_to_loaders": True,
            "_to_bind": None,
            "strategy": (("lazy", "joined"),),
        }
        inner_opt.__setstate__(inner_state)

        opt = strategy_options._UnboundLoad.__new__(
            strategy_options._UnboundLoad
        )
        state = {
            "_is_chain_link": False,
            "local_opts": {},
            "is_class_strategy": False,
            "path": [(User, "addresses", None)],
            "propagate_to_loaders": True,
            "_to_bind": [inner_opt],
        }

        opt.__setstate__(state)

        query = create_session().query(User)
        attr = {}
        load = opt._bind_loader(
            [ent.entity_zero for ent in query._mapper_entities],
            query._current_path,
            attr,
            False,
        )

        eq_(
            load.path,
            inspect(User)._path_registry[User.addresses.property][
                inspect(self.classes.Address)
            ],
        )

    def test_legacy_opt_setstate(self):
        User = self.classes.User

        opt = strategy_options._UnboundLoad.__new__(
            strategy_options._UnboundLoad
        )
        state = {
            "_is_chain_link": False,
            "local_opts": {},
            "is_class_strategy": False,
            "path": [(User, "addresses")],
            "propagate_to_loaders": True,
            "_to_bind": [opt],
            "strategy": (("lazy", "joined"),),
        }

        opt.__setstate__(state)

        query = create_session().query(User)
        attr = {}
        load = opt._bind_loader(
            [ent.entity_zero for ent in query._mapper_entities],
            query._current_path,
            attr,
            False,
        )

        eq_(
            load.path,
            inspect(User)._path_registry[User.addresses.property][
                inspect(self.classes.Address)
            ],
        )


class LocalOptsTest(PathTest, QueryTest):
    @classmethod
    def setup_class(cls):
        super(LocalOptsTest, cls).setup_class()

        @strategy_options.loader_option()
        def some_col_opt_only(loadopt, key, opts):
            return loadopt.set_column_strategy(
                (key,), None, opts, opts_only=True
            )

        @strategy_options.loader_option()
        def some_col_opt_strategy(loadopt, key, opts):
            return loadopt.set_column_strategy(
                (key,), {"deferred": True, "instrument": True}, opts
            )

        cls.some_col_opt_only = some_col_opt_only
        cls.some_col_opt_strategy = some_col_opt_strategy

    def _assert_attrs(self, opts, expected):
        User = self.classes.User

        query = create_session().query(User)
        attr = {}

        for opt in opts:
            if isinstance(opt, strategy_options._UnboundLoad):
                for tb in opt._to_bind:
                    tb._bind_loader(
                        [ent.entity_zero for ent in query._mapper_entities],
                        query._current_path,
                        attr,
                        False,
                    )
            else:
                attr.update(opt.context)

        key = (
            "loader",
            tuple(inspect(User)._path_registry[User.name.property]),
        )
        eq_(attr[key].local_opts, expected)

    def test_single_opt_only(self):
        opt = strategy_options._UnboundLoad().some_col_opt_only(
            "name", {"foo": "bar"}
        )
        self._assert_attrs([opt], {"foo": "bar"})

    def test_unbound_multiple_opt_only(self):
        opts = [
            strategy_options._UnboundLoad().some_col_opt_only(
                "name", {"foo": "bar"}
            ),
            strategy_options._UnboundLoad().some_col_opt_only(
                "name", {"bat": "hoho"}
            ),
        ]
        self._assert_attrs(opts, {"foo": "bar", "bat": "hoho"})

    def test_bound_multiple_opt_only(self):
        User = self.classes.User
        opts = [
            Load(User)
            .some_col_opt_only("name", {"foo": "bar"})
            .some_col_opt_only("name", {"bat": "hoho"})
        ]
        self._assert_attrs(opts, {"foo": "bar", "bat": "hoho"})

    def test_bound_strat_opt_recvs_from_optonly(self):
        User = self.classes.User
        opts = [
            Load(User)
            .some_col_opt_only("name", {"foo": "bar"})
            .some_col_opt_strategy("name", {"bat": "hoho"})
        ]
        self._assert_attrs(opts, {"foo": "bar", "bat": "hoho"})

    def test_unbound_strat_opt_recvs_from_optonly(self):
        opts = [
            strategy_options._UnboundLoad().some_col_opt_only(
                "name", {"foo": "bar"}
            ),
            strategy_options._UnboundLoad().some_col_opt_strategy(
                "name", {"bat": "hoho"}
            ),
        ]
        self._assert_attrs(opts, {"foo": "bar", "bat": "hoho"})

    def test_unbound_opt_only_adds_to_strat(self):
        opts = [
            strategy_options._UnboundLoad().some_col_opt_strategy(
                "name", {"bat": "hoho"}
            ),
            strategy_options._UnboundLoad().some_col_opt_only(
                "name", {"foo": "bar"}
            ),
        ]
        self._assert_attrs(opts, {"foo": "bar", "bat": "hoho"})

    def test_bound_opt_only_adds_to_strat(self):
        User = self.classes.User
        opts = [
            Load(User)
            .some_col_opt_strategy("name", {"bat": "hoho"})
            .some_col_opt_only("name", {"foo": "bar"})
        ]
        self._assert_attrs(opts, {"foo": "bar", "bat": "hoho"})


class SubOptionsTest(PathTest, QueryTest):
    run_create_tables = False
    run_inserts = None
    run_deletes = None

    def _assert_opts(self, q, sub_opt, non_sub_opts):
        existing_attributes = q._attributes
        q._attributes = q._attributes.copy()
        attr_a = {}

        for val in sub_opt._to_bind:
            val._bind_loader(
                [ent.entity_zero for ent in q._mapper_entities],
                q._current_path,
                attr_a,
                False,
            )

        q._attributes = existing_attributes.copy()

        attr_b = {}

        for opt in non_sub_opts:
            for val in opt._to_bind:
                val._bind_loader(
                    [ent.entity_zero for ent in q._mapper_entities],
                    q._current_path,
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

        sess = Session()
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

        sess = Session()
        self._assert_opts(sess.query(User), sub_opt, non_sub_opts)

    def test_three(self):
        User, Address, Order, Item, SubItem = self.classes(
            "User", "Address", "Order", "Item", "SubItem"
        )
        sub_opt = defaultload(User.orders).options(defer("*"))
        non_sub_opts = [defaultload(User.orders).defer("*")]
        sess = Session()
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
        sess = Session()
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
        sess = Session()
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
        sess = Session()
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
        sess = Session()
        self._assert_opts(sess.query(User), sub_opt, non_sub_opts)

    def test_invalid_one(self):
        User, Address, Order, Item, SubItem = self.classes(
            "User", "Address", "Order", "Item", "SubItem"
        )

        # these options are "invalid", in that User.orders -> Item.keywords
        # is not a path.  However, the "normal" option is not generating
        # an error for now, which is bad, but we're testing here only that
        # it works the same way, so there you go.   If and when we make this
        # case raise, then both cases should raise in the same way.
        sub_opt = joinedload(User.orders).options(
            joinedload(Item.keywords), joinedload(Order.items)
        )
        non_sub_opts = [
            joinedload(User.orders).joinedload(Item.keywords),
            defaultload(User.orders).joinedload(Order.items),
        ]
        sess = Session()
        self._assert_opts(sess.query(User), sub_opt, non_sub_opts)

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
        sess = Session()
        self._assert_opts(sess.query(User), sub_opt, non_sub_opts)

    def test_not_implemented_fromload(self):
        User, Address, Order, Item, SubItem = self.classes(
            "User", "Address", "Order", "Item", "SubItem"
        )

        assert_raises_message(
            NotImplementedError,
            r"The options\(\) method is currently only supported "
            "for 'unbound' loader options",
            Load(User).joinedload(User.orders).options,
            joinedload(Order.items),
        )

    def test_not_implemented_toload(self):
        User, Address, Order, Item, SubItem = self.classes(
            "User", "Address", "Order", "Item", "SubItem"
        )

        assert_raises_message(
            NotImplementedError,
            r"Only 'unbound' loader options may be used with the "
            r"Load.options\(\) method",
            joinedload(User.orders).options,
            Load(Order).joinedload(Order.items),
        )


class CacheKeyTest(PathTest, QueryTest):

    run_create_tables = False
    run_inserts = None
    run_deletes = None

    def test_unbound_cache_key_included_safe(self):
        User, Address, Order, Item, SubItem = self.classes(
            "User", "Address", "Order", "Item", "SubItem"
        )

        query_path = self._make_path_registry([User, "orders"])

        opt = joinedload(User.orders).joinedload(Order.items)
        eq_(
            opt._generate_cache_key(query_path),
            (((Order, "items", Item, ("lazy", "joined")),)),
        )

    def test_unbound_cache_key_included_safe_multipath(self):
        User, Address, Order, Item, SubItem = self.classes(
            "User", "Address", "Order", "Item", "SubItem"
        )

        query_path = self._make_path_registry([User, "orders"])

        base = joinedload(User.orders)
        opt1 = base.joinedload(Order.items)
        opt2 = base.joinedload(Order.address)

        eq_(
            opt1._generate_cache_key(query_path),
            (((Order, "items", Item, ("lazy", "joined")),)),
        )

        eq_(
            opt2._generate_cache_key(query_path),
            (((Order, "address", Address, ("lazy", "joined")),)),
        )

    def test_bound_cache_key_included_safe_multipath(self):
        User, Address, Order, Item, SubItem = self.classes(
            "User", "Address", "Order", "Item", "SubItem"
        )

        query_path = self._make_path_registry([User, "orders"])

        base = Load(User).joinedload(User.orders)
        opt1 = base.joinedload(Order.items)
        opt2 = base.joinedload(Order.address)

        eq_(
            opt1._generate_cache_key(query_path),
            (((Order, "items", Item, ("lazy", "joined")),)),
        )

        eq_(
            opt2._generate_cache_key(query_path),
            (((Order, "address", Address, ("lazy", "joined")),)),
        )

    def test_bound_cache_key_included_safe(self):
        User, Address, Order, Item, SubItem = self.classes(
            "User", "Address", "Order", "Item", "SubItem"
        )

        query_path = self._make_path_registry([User, "orders"])

        opt = Load(User).joinedload(User.orders).joinedload(Order.items)
        eq_(
            opt._generate_cache_key(query_path),
            (((Order, "items", Item, ("lazy", "joined")),)),
        )

    def test_unbound_cache_key_excluded_on_other(self):
        User, Address, Order, Item, SubItem = self.classes(
            "User", "Address", "Order", "Item", "SubItem"
        )

        query_path = self._make_path_registry([User, "addresses"])

        opt = joinedload(User.orders).joinedload(Order.items)
        eq_(opt._generate_cache_key(query_path), None)

    def test_bound_cache_key_excluded_on_other(self):
        User, Address, Order, Item, SubItem = self.classes(
            "User", "Address", "Order", "Item", "SubItem"
        )

        query_path = self._make_path_registry([User, "addresses"])

        opt = Load(User).joinedload(User.orders).joinedload(Order.items)
        eq_(opt._generate_cache_key(query_path), None)

    def test_unbound_cache_key_excluded_on_aliased(self):
        User, Address, Order, Item, SubItem = self.classes(
            "User", "Address", "Order", "Item", "SubItem"
        )

        # query of:
        #
        # query(User).options(
        #       joinedload(aliased(User).orders).joinedload(Order.items))
        #
        # we are lazy loading Order objects from User.orders
        # the path excludes our option so cache key should
        # be None

        query_path = self._make_path_registry([User, "orders"])

        opt = joinedload(aliased(User).orders).joinedload(Order.items)
        eq_(opt._generate_cache_key(query_path), None)

    def test_bound_cache_key_wildcard_one(self):
        # do not change this test, it is testing
        # a specific condition in Load._chop_path().
        User, Address = self.classes("User", "Address")

        query_path = self._make_path_registry([User, "addresses"])

        opt = Load(User).lazyload("*")
        eq_(opt._generate_cache_key(query_path), None)

    def test_unbound_cache_key_wildcard_one(self):
        User, Address = self.classes("User", "Address")

        query_path = self._make_path_registry([User, "addresses"])

        opt = lazyload("*")
        eq_(
            opt._generate_cache_key(query_path),
            (("relationship:_sa_default", ("lazy", "select")),),
        )

    def test_bound_cache_key_wildcard_two(self):
        User, Address, Order, Item, SubItem, Keyword = self.classes(
            "User", "Address", "Order", "Item", "SubItem", "Keyword"
        )

        query_path = self._make_path_registry([User])

        opt = Load(User).lazyload("orders").lazyload("*")
        eq_(
            opt._generate_cache_key(query_path),
            (
                ("orders", Order, ("lazy", "select")),
                ("orders", Order, "relationship:*", ("lazy", "select")),
            ),
        )

    def test_unbound_cache_key_wildcard_two(self):
        User, Address, Order, Item, SubItem, Keyword = self.classes(
            "User", "Address", "Order", "Item", "SubItem", "Keyword"
        )

        query_path = self._make_path_registry([User])

        opt = lazyload("orders").lazyload("*")
        eq_(
            opt._generate_cache_key(query_path),
            (
                ("orders", Order, ("lazy", "select")),
                ("orders", Order, "relationship:*", ("lazy", "select")),
            ),
        )

    def test_unbound_cache_key_of_type_subclass_relationship(self):
        User, Address, Order, Item, SubItem, Keyword = self.classes(
            "User", "Address", "Order", "Item", "SubItem", "Keyword"
        )

        query_path = self._make_path_registry([Order, "items", Item])

        opt = subqueryload(Order.items.of_type(SubItem)).subqueryload(
            SubItem.extra_keywords
        )

        eq_(
            opt._generate_cache_key(query_path),
            (
                (SubItem, ("lazy", "subquery")),
                ("extra_keywords", Keyword, ("lazy", "subquery")),
            ),
        )

    def test_unbound_cache_key_of_type_subclass_relationship_stringattr(self):
        User, Address, Order, Item, SubItem, Keyword = self.classes(
            "User", "Address", "Order", "Item", "SubItem", "Keyword"
        )

        query_path = self._make_path_registry([Order, "items", Item])

        opt = subqueryload(Order.items.of_type(SubItem)).subqueryload(
            "extra_keywords"
        )

        eq_(
            opt._generate_cache_key(query_path),
            (
                (SubItem, ("lazy", "subquery")),
                ("extra_keywords", Keyword, ("lazy", "subquery")),
            ),
        )

    def test_bound_cache_key_of_type_subclass_relationship(self):
        User, Address, Order, Item, SubItem, Keyword = self.classes(
            "User", "Address", "Order", "Item", "SubItem", "Keyword"
        )

        query_path = self._make_path_registry([Order, "items", Item])

        opt = (
            Load(Order)
            .subqueryload(Order.items.of_type(SubItem))
            .subqueryload(SubItem.extra_keywords)
        )

        eq_(
            opt._generate_cache_key(query_path),
            (
                (SubItem, ("lazy", "subquery")),
                ("extra_keywords", Keyword, ("lazy", "subquery")),
            ),
        )

    def test_bound_cache_key_of_type_subclass_string_relationship(self):
        User, Address, Order, Item, SubItem, Keyword = self.classes(
            "User", "Address", "Order", "Item", "SubItem", "Keyword"
        )

        query_path = self._make_path_registry([Order, "items", Item])

        opt = (
            Load(Order)
            .subqueryload(Order.items.of_type(SubItem))
            .subqueryload("extra_keywords")
        )

        eq_(
            opt._generate_cache_key(query_path),
            (
                (SubItem, ("lazy", "subquery")),
                ("extra_keywords", Keyword, ("lazy", "subquery")),
            ),
        )

    def test_unbound_cache_key_excluded_of_type_safe(self):
        User, Address, Order, Item, SubItem = self.classes(
            "User", "Address", "Order", "Item", "SubItem"
        )
        # query of:
        #
        # query(User).options(
        #       subqueryload(User.orders).
        #       subqueryload(Order.items.of_type(SubItem)))
        #
        #
        # we are lazy loading Address objects from User.addresses
        # the path excludes our option so cache key should
        # be None

        query_path = self._make_path_registry([User, "addresses"])

        opt = subqueryload(User.orders).subqueryload(
            Order.items.of_type(SubItem)
        )
        eq_(opt._generate_cache_key(query_path), None)

    def test_unbound_cache_key_excluded_of_type_unsafe(self):
        User, Address, Order, Item, SubItem = self.classes(
            "User", "Address", "Order", "Item", "SubItem"
        )
        # query of:
        #
        # query(User).options(
        #       subqueryload(User.orders).
        #       subqueryload(Order.items.of_type(aliased(SubItem))))
        #
        #
        # we are lazy loading Address objects from User.addresses
        # the path excludes our option so cache key should
        # be None

        query_path = self._make_path_registry([User, "addresses"])

        opt = subqueryload(User.orders).subqueryload(
            Order.items.of_type(aliased(SubItem))
        )
        eq_(opt._generate_cache_key(query_path), None)

    def test_bound_cache_key_excluded_of_type_safe(self):
        User, Address, Order, Item, SubItem = self.classes(
            "User", "Address", "Order", "Item", "SubItem"
        )
        # query of:
        #
        # query(User).options(
        #       subqueryload(User.orders).
        #       subqueryload(Order.items.of_type(SubItem)))
        #
        #
        # we are lazy loading Address objects from User.addresses
        # the path excludes our option so cache key should
        # be None

        query_path = self._make_path_registry([User, "addresses"])

        opt = (
            Load(User)
            .subqueryload(User.orders)
            .subqueryload(Order.items.of_type(SubItem))
        )
        eq_(opt._generate_cache_key(query_path), None)

    def test_bound_cache_key_excluded_of_type_unsafe(self):
        User, Address, Order, Item, SubItem = self.classes(
            "User", "Address", "Order", "Item", "SubItem"
        )
        # query of:
        #
        # query(User).options(
        #       subqueryload(User.orders).
        #       subqueryload(Order.items.of_type(aliased(SubItem))))
        #
        #
        # we are lazy loading Address objects from User.addresses
        # the path excludes our option so cache key should
        # be None

        query_path = self._make_path_registry([User, "addresses"])

        opt = (
            Load(User)
            .subqueryload(User.orders)
            .subqueryload(Order.items.of_type(aliased(SubItem)))
        )
        eq_(opt._generate_cache_key(query_path), None)

    def test_unbound_cache_key_included_of_type_safe(self):
        User, Address, Order, Item, SubItem = self.classes(
            "User", "Address", "Order", "Item", "SubItem"
        )

        query_path = self._make_path_registry([User, "orders"])

        opt = joinedload(User.orders).joinedload(Order.items.of_type(SubItem))
        eq_(
            opt._generate_cache_key(query_path),
            ((Order, "items", SubItem, ("lazy", "joined")),),
        )

    def test_bound_cache_key_included_of_type_safe(self):
        User, Address, Order, Item, SubItem = self.classes(
            "User", "Address", "Order", "Item", "SubItem"
        )

        query_path = self._make_path_registry([User, "orders"])

        opt = (
            Load(User)
            .joinedload(User.orders)
            .joinedload(Order.items.of_type(SubItem))
        )

        eq_(
            opt._generate_cache_key(query_path),
            ((Order, "items", SubItem, ("lazy", "joined")),),
        )

    def test_unbound_cache_key_included_unsafe_option_one(self):
        User, Address, Order, Item, SubItem = self.classes(
            "User", "Address", "Order", "Item", "SubItem"
        )

        query_path = self._make_path_registry([User, "orders"])

        opt = joinedload(User.orders).joinedload(
            Order.items.of_type(aliased(SubItem))
        )
        eq_(opt._generate_cache_key(query_path), False)

    def test_unbound_cache_key_included_unsafe_option_two(self):
        User, Address, Order, Item, SubItem = self.classes(
            "User", "Address", "Order", "Item", "SubItem"
        )

        query_path = self._make_path_registry([User, "orders", Order])

        opt = joinedload(User.orders).joinedload(
            Order.items.of_type(aliased(SubItem))
        )
        eq_(opt._generate_cache_key(query_path), False)

    def test_unbound_cache_key_included_unsafe_option_three(self):
        User, Address, Order, Item, SubItem = self.classes(
            "User", "Address", "Order", "Item", "SubItem"
        )

        query_path = self._make_path_registry([User, "orders", Order, "items"])

        opt = joinedload(User.orders).joinedload(
            Order.items.of_type(aliased(SubItem))
        )
        eq_(opt._generate_cache_key(query_path), False)

    def test_unbound_cache_key_included_unsafe_query(self):
        User, Address, Order, Item, SubItem = self.classes(
            "User", "Address", "Order", "Item", "SubItem"
        )

        au = aliased(User)
        query_path = self._make_path_registry([inspect(au), "orders"])

        opt = joinedload(au.orders).joinedload(Order.items)
        eq_(opt._generate_cache_key(query_path), False)

    def test_unbound_cache_key_included_safe_w_deferred(self):
        User, Address, Order, Item, SubItem = self.classes(
            "User", "Address", "Order", "Item", "SubItem"
        )

        query_path = self._make_path_registry([User, "addresses"])

        opt = (
            joinedload(User.addresses)
            .defer(Address.email_address)
            .defer(Address.user_id)
        )
        eq_(
            opt._generate_cache_key(query_path),
            (
                (
                    Address,
                    "email_address",
                    ("deferred", True),
                    ("instrument", True),
                ),
                (Address, "user_id", ("deferred", True), ("instrument", True)),
            ),
        )

    def test_unbound_cache_key_included_safe_w_deferred_multipath(self):
        User, Address, Order, Item, SubItem = self.classes(
            "User", "Address", "Order", "Item", "SubItem"
        )

        query_path = self._make_path_registry([User, "orders"])

        base = joinedload(User.orders)
        opt1 = base.joinedload(Order.items)
        opt2 = (
            base.joinedload(Order.address)
            .defer(Address.email_address)
            .defer(Address.user_id)
        )

        eq_(
            opt1._generate_cache_key(query_path),
            ((Order, "items", Item, ("lazy", "joined")),),
        )

        eq_(
            opt2._generate_cache_key(query_path),
            (
                (Order, "address", Address, ("lazy", "joined")),
                (
                    Order,
                    "address",
                    Address,
                    "email_address",
                    ("deferred", True),
                    ("instrument", True),
                ),
                (
                    Order,
                    "address",
                    Address,
                    "user_id",
                    ("deferred", True),
                    ("instrument", True),
                ),
            ),
        )

    def test_bound_cache_key_included_safe_w_deferred(self):
        User, Address, Order, Item, SubItem = self.classes(
            "User", "Address", "Order", "Item", "SubItem"
        )

        query_path = self._make_path_registry([User, "addresses"])

        opt = (
            Load(User)
            .joinedload(User.addresses)
            .defer(Address.email_address)
            .defer(Address.user_id)
        )
        eq_(
            opt._generate_cache_key(query_path),
            (
                (
                    Address,
                    "email_address",
                    ("deferred", True),
                    ("instrument", True),
                ),
                (Address, "user_id", ("deferred", True), ("instrument", True)),
            ),
        )

    def test_bound_cache_key_included_safe_w_deferred_multipath(self):
        User, Address, Order, Item, SubItem = self.classes(
            "User", "Address", "Order", "Item", "SubItem"
        )

        query_path = self._make_path_registry([User, "orders"])

        base = Load(User).joinedload(User.orders)
        opt1 = base.joinedload(Order.items)
        opt2 = (
            base.joinedload(Order.address)
            .defer(Address.email_address)
            .defer(Address.user_id)
        )

        eq_(
            opt1._generate_cache_key(query_path),
            ((Order, "items", Item, ("lazy", "joined")),),
        )

        eq_(
            opt2._generate_cache_key(query_path),
            (
                (Order, "address", Address, ("lazy", "joined")),
                (
                    Order,
                    "address",
                    Address,
                    "email_address",
                    ("deferred", True),
                    ("instrument", True),
                ),
                (
                    Order,
                    "address",
                    Address,
                    "user_id",
                    ("deferred", True),
                    ("instrument", True),
                ),
            ),
        )

    def test_unbound_cache_key_included_safe_w_option(self):
        User, Address, Order, Item, SubItem = self.classes(
            "User", "Address", "Order", "Item", "SubItem"
        )

        opt = (
            defaultload("orders")
            .joinedload("items", innerjoin=True)
            .defer("description")
        )
        query_path = self._make_path_registry([User, "orders"])

        eq_(
            opt._generate_cache_key(query_path),
            (
                (
                    Order,
                    "items",
                    Item,
                    ("lazy", "joined"),
                    ("innerjoin", True),
                ),
                (
                    Order,
                    "items",
                    Item,
                    "description",
                    ("deferred", True),
                    ("instrument", True),
                ),
            ),
        )

    def test_bound_cache_key_excluded_on_aliased(self):
        User, Address, Order, Item, SubItem = self.classes(
            "User", "Address", "Order", "Item", "SubItem"
        )

        query_path = self._make_path_registry([User, "orders"])

        au = aliased(User)
        opt = Load(au).joinedload(au.orders).joinedload(Order.items)
        eq_(opt._generate_cache_key(query_path), None)

    def test_bound_cache_key_included_unsafe_option_one(self):
        User, Address, Order, Item, SubItem = self.classes(
            "User", "Address", "Order", "Item", "SubItem"
        )

        query_path = self._make_path_registry([User, "orders"])

        opt = (
            Load(User)
            .joinedload(User.orders)
            .joinedload(Order.items.of_type(aliased(SubItem)))
        )
        eq_(opt._generate_cache_key(query_path), False)

    def test_bound_cache_key_included_unsafe_option_two(self):
        User, Address, Order, Item, SubItem = self.classes(
            "User", "Address", "Order", "Item", "SubItem"
        )

        query_path = self._make_path_registry([User, "orders", Order])

        opt = (
            Load(User)
            .joinedload(User.orders)
            .joinedload(Order.items.of_type(aliased(SubItem)))
        )
        eq_(opt._generate_cache_key(query_path), False)

    def test_bound_cache_key_included_unsafe_option_three(self):
        User, Address, Order, Item, SubItem = self.classes(
            "User", "Address", "Order", "Item", "SubItem"
        )

        query_path = self._make_path_registry([User, "orders", Order, "items"])

        opt = (
            Load(User)
            .joinedload(User.orders)
            .joinedload(Order.items.of_type(aliased(SubItem)))
        )
        eq_(opt._generate_cache_key(query_path), False)

    def test_bound_cache_key_included_unsafe_query(self):
        User, Address, Order, Item, SubItem = self.classes(
            "User", "Address", "Order", "Item", "SubItem"
        )

        au = aliased(User)
        query_path = self._make_path_registry([inspect(au), "orders"])

        opt = Load(au).joinedload(au.orders).joinedload(Order.items)
        eq_(opt._generate_cache_key(query_path), False)

    def test_bound_cache_key_included_safe_w_option(self):
        User, Address, Order, Item, SubItem = self.classes(
            "User", "Address", "Order", "Item", "SubItem"
        )

        opt = (
            Load(User)
            .defaultload("orders")
            .joinedload("items", innerjoin=True)
            .defer("description")
        )
        query_path = self._make_path_registry([User, "orders"])

        eq_(
            opt._generate_cache_key(query_path),
            (
                (
                    Order,
                    "items",
                    Item,
                    ("lazy", "joined"),
                    ("innerjoin", True),
                ),
                (
                    Order,
                    "items",
                    Item,
                    "description",
                    ("deferred", True),
                    ("instrument", True),
                ),
            ),
        )

    def test_unbound_cache_key_included_safe_w_loadonly_strs(self):
        User, Address, Order, Item, SubItem = self.classes(
            "User", "Address", "Order", "Item", "SubItem"
        )

        query_path = self._make_path_registry([User, "addresses"])

        opt = defaultload(User.addresses).load_only("id", "email_address")
        eq_(
            opt._generate_cache_key(query_path),
            (
                (Address, "id", ("deferred", False), ("instrument", True)),
                (
                    Address,
                    "email_address",
                    ("deferred", False),
                    ("instrument", True),
                ),
                (
                    Address,
                    "column:*",
                    ("deferred", True),
                    ("instrument", True),
                    ("undefer_pks", True),
                ),
            ),
        )

    def test_unbound_cache_key_included_safe_w_loadonly_props(self):
        User, Address, Order, Item, SubItem = self.classes(
            "User", "Address", "Order", "Item", "SubItem"
        )

        query_path = self._make_path_registry([User, "addresses"])

        opt = defaultload(User.addresses).load_only(
            Address.id, Address.email_address
        )
        eq_(
            opt._generate_cache_key(query_path),
            (
                (Address, "id", ("deferred", False), ("instrument", True)),
                (
                    Address,
                    "email_address",
                    ("deferred", False),
                    ("instrument", True),
                ),
                (
                    Address,
                    "column:*",
                    ("deferred", True),
                    ("instrument", True),
                    ("undefer_pks", True),
                ),
            ),
        )

    def test_bound_cache_key_included_safe_w_loadonly(self):
        User, Address, Order, Item, SubItem = self.classes(
            "User", "Address", "Order", "Item", "SubItem"
        )

        query_path = self._make_path_registry([User, "addresses"])

        opt = (
            Load(User)
            .defaultload(User.addresses)
            .load_only("id", "email_address")
        )
        eq_(
            opt._generate_cache_key(query_path),
            (
                (Address, "id", ("deferred", False), ("instrument", True)),
                (
                    Address,
                    "email_address",
                    ("deferred", False),
                    ("instrument", True),
                ),
                (
                    Address,
                    "column:*",
                    ("deferred", True),
                    ("instrument", True),
                    ("undefer_pks", True),
                ),
            ),
        )

    def test_unbound_cache_key_undefer_group(self):
        User, Address = self.classes("User", "Address")

        query_path = self._make_path_registry([User, "addresses"])

        opt = defaultload(User.addresses).undefer_group("xyz")

        eq_(
            opt._generate_cache_key(query_path),
            ((Address, "column:*", ("undefer_group_xyz", True)),),
        )

    def test_bound_cache_key_undefer_group(self):
        User, Address = self.classes("User", "Address")

        query_path = self._make_path_registry([User, "addresses"])

        opt = Load(User).defaultload(User.addresses).undefer_group("xyz")

        eq_(
            opt._generate_cache_key(query_path),
            ((Address, "column:*", ("undefer_group_xyz", True)),),
        )
