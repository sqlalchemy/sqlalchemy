from sqlalchemy import inspect
from sqlalchemy.orm import attributes, mapper, relationship, backref, \
    configure_mappers, create_session, synonym, Session, class_mapper, \
    aliased, column_property, joinedload_all, joinedload, Query,\
    util as orm_util, Load
import sqlalchemy as sa
from sqlalchemy import testing
from sqlalchemy.testing.assertions import eq_, assert_raises, assert_raises_message
from test.orm import _fixtures

class QueryTest(_fixtures.FixtureTest):
    run_setup_mappers = 'once'
    run_inserts = 'once'
    run_deletes = None

    @classmethod
    def setup_mappers(cls):
        cls._setup_stock_mapping()

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

        for val in opt._to_bind:
            val._bind_loader(q, attr, False)

        assert_paths = [k[1] for k in attr]
        eq_(
            set([p for p in assert_paths]),
            set([self._make_path(p) for p in paths])
        )

class LoadTest(PathTest, QueryTest):

    def test_gen_path_attr_entity(self):
        User = self.classes.User
        Address = self.classes.Address

        l = Load(User)
        eq_(
            l._generate_path(inspect(User)._path_registry, User.addresses, "relationship"),
            self._make_path_registry([User, "addresses", Address])
        )

    def test_gen_path_attr_column(self):
        User = self.classes.User

        l = Load(User)
        eq_(
            l._generate_path(inspect(User)._path_registry, User.name, "column"),
            self._make_path_registry([User, "name"])
        )

    def test_gen_path_string_entity(self):
        User = self.classes.User
        Address = self.classes.Address

        l = Load(User)
        eq_(
            l._generate_path(inspect(User)._path_registry, "addresses", "relationship"),
            self._make_path_registry([User, "addresses", Address])
        )

    def test_gen_path_string_column(self):
        User = self.classes.User

        l = Load(User)
        eq_(
            l._generate_path(inspect(User)._path_registry, "name", "column"),
            self._make_path_registry([User, "name"])
        )

    def test_gen_path_invalid_from_col(self):
        User = self.classes.User

        l = Load(User)
        l.path = self._make_path_registry([User, "name"])
        assert_raises_message(
            sa.exc.ArgumentError,
            "Attribute 'name' of entity 'Mapper|User|users' does "
                "not refer to a mapped entity",
            l._generate_path, l.path, User.addresses, "relationship"

        )
    def test_gen_path_attr_entity_invalid_raiseerr(self):
        User = self.classes.User
        Order = self.classes.Order

        l = Load(User)

        assert_raises_message(
            sa.exc.ArgumentError,
            "Attribute 'Order.items' does not link from element 'Mapper|User|users'",
            l._generate_path,
            inspect(User)._path_registry, Order.items, "relationship",
        )

    def test_gen_path_attr_entity_invalid_noraiseerr(self):
        User = self.classes.User
        Order = self.classes.Order

        l = Load(User)

        eq_(
            l._generate_path(
                inspect(User)._path_registry, Order.items, "relationship", False
            ),
            None
        )

    def test_set_strat_ent(self):
        User = self.classes.User

        l1 = Load(User)
        l2 = l1.joinedload("addresses")
        eq_(
            l1.context,
            {
                ('loader', self._make_path([User, "addresses"])): l2
            }
        )

    def test_set_strat_col(self):
        User = self.classes.User

        l1 = Load(User)
        l2 = l1.defer("name")
        l3 = list(l2.context.values())[0]
        eq_(
            l1.context,
            {
                ('loader', self._make_path([User, "name"])): l3
            }
        )


class OptionsTest(PathTest, QueryTest):

    def _option_fixture(self, *arg):
        from sqlalchemy.orm import strategy_options

        return strategy_options._UnboundLoad._from_keys(
                    strategy_options._UnboundLoad.joinedload, arg, True, {})



    def test_get_path_one_level_string(self):
        User = self.classes.User

        sess = Session()
        q = sess.query(User)

        opt = self._option_fixture("addresses")
        self._assert_path_result(opt, q, [(User, 'addresses')])

    def test_get_path_one_level_attribute(self):
        User = self.classes.User

        sess = Session()
        q = sess.query(User)

        opt = self._option_fixture(User.addresses)
        self._assert_path_result(opt, q, [(User, 'addresses')])

    def test_path_on_entity_but_doesnt_match_currentpath(self):
        User, Address = self.classes.User, self.classes.Address

        # ensure "current path" is fully consumed before
        # matching against current entities.
        # see [ticket:2098]
        sess = Session()
        q = sess.query(User)
        opt = self._option_fixture('email_address', 'id')
        q = sess.query(Address)._with_current_path(
                orm_util.PathRegistry.coerce([inspect(User),
                        inspect(User).attrs.addresses])
            )
        self._assert_path_result(opt, q, [])

    def test_get_path_one_level_with_unrelated(self):
        Order = self.classes.Order

        sess = Session()
        q = sess.query(Order)
        opt = self._option_fixture("addresses")
        self._assert_path_result(opt, q, [])

    def test_path_multilevel_string(self):
        Item, User, Order = (self.classes.Item,
                                self.classes.User,
                                self.classes.Order)

        sess = Session()
        q = sess.query(User)

        opt = self._option_fixture("orders.items.keywords")
        self._assert_path_result(opt, q, [
            (User, 'orders'),
            (User, 'orders', Order, 'items'),
            (User, 'orders', Order, 'items', Item, 'keywords')
        ])

    def test_path_multilevel_attribute(self):
        Item, User, Order = (self.classes.Item,
                                self.classes.User,
                                self.classes.Order)

        sess = Session()
        q = sess.query(User)

        opt = self._option_fixture(User.orders, Order.items, Item.keywords)
        self._assert_path_result(opt, q, [
            (User, 'orders'),
            (User, 'orders', Order, 'items'),
            (User, 'orders', Order, 'items', Item, 'keywords')
        ])

    def test_with_current_matching_string(self):
        Item, User, Order = (self.classes.Item,
                                self.classes.User,
                                self.classes.Order)

        sess = Session()
        q = sess.query(Item)._with_current_path(
                self._make_path_registry([User, 'orders', Order, 'items'])
            )

        opt = self._option_fixture("orders.items.keywords")
        self._assert_path_result(opt, q, [
            (Item, 'keywords')
        ])

    def test_with_current_matching_attribute(self):
        Item, User, Order = (self.classes.Item,
                                self.classes.User,
                                self.classes.Order)

        sess = Session()
        q = sess.query(Item)._with_current_path(
                self._make_path_registry([User, 'orders', Order, 'items'])
            )

        opt = self._option_fixture(User.orders, Order.items, Item.keywords)
        self._assert_path_result(opt, q, [
            (Item, 'keywords')
        ])

    def test_with_current_nonmatching_string(self):
        Item, User, Order = (self.classes.Item,
                                self.classes.User,
                                self.classes.Order)

        sess = Session()
        q = sess.query(Item)._with_current_path(
                self._make_path_registry([User, 'orders', Order, 'items'])
            )

        opt = self._option_fixture("keywords")
        self._assert_path_result(opt, q, [])

        opt = self._option_fixture("items.keywords")
        self._assert_path_result(opt, q, [])

    def test_with_current_nonmatching_attribute(self):
        Item, User, Order = (self.classes.Item,
                                self.classes.User,
                                self.classes.Order)

        sess = Session()
        q = sess.query(Item)._with_current_path(
                self._make_path_registry([User, 'orders', Order, 'items'])
            )

        opt = self._option_fixture(Item.keywords)
        self._assert_path_result(opt, q, [])

        opt = self._option_fixture(Order.items, Item.keywords)
        self._assert_path_result(opt, q, [])

    def test_from_base_to_subclass_attr(self):
        Dingaling, Address = self.classes.Dingaling, self.classes.Address

        sess = Session()
        class SubAddr(Address):
            pass
        mapper(SubAddr, inherits=Address, properties={
            'flub': relationship(Dingaling)
        })

        q = sess.query(Address)
        opt = self._option_fixture(SubAddr.flub)

        self._assert_path_result(opt, q, [(SubAddr, 'flub')])

    def test_from_subclass_to_subclass_attr(self):
        Dingaling, Address = self.classes.Dingaling, self.classes.Address

        sess = Session()
        class SubAddr(Address):
            pass
        mapper(SubAddr, inherits=Address, properties={
            'flub': relationship(Dingaling)
        })

        q = sess.query(SubAddr)
        opt = self._option_fixture(SubAddr.flub)

        self._assert_path_result(opt, q, [(SubAddr, 'flub')])

    def test_from_base_to_base_attr_via_subclass(self):
        Dingaling, Address = self.classes.Dingaling, self.classes.Address

        sess = Session()
        class SubAddr(Address):
            pass
        mapper(SubAddr, inherits=Address, properties={
            'flub': relationship(Dingaling)
        })

        q = sess.query(Address)
        opt = self._option_fixture(SubAddr.user)

        self._assert_path_result(opt, q,
                [(Address, inspect(Address).attrs.user)])

    def test_of_type(self):
        User, Address = self.classes.User, self.classes.Address

        sess = Session()
        class SubAddr(Address):
            pass
        mapper(SubAddr, inherits=Address)

        q = sess.query(User)
        opt = self._option_fixture(User.addresses.of_type(SubAddr), SubAddr.user)

        u_mapper = inspect(User)
        a_mapper = inspect(Address)
        self._assert_path_result(opt, q, [
            (u_mapper, u_mapper.attrs.addresses),
            (u_mapper, u_mapper.attrs.addresses, a_mapper, a_mapper.attrs.user)
        ])

    def test_of_type_plus_level(self):
        Dingaling, User, Address = (self.classes.Dingaling,
                                self.classes.User,
                                self.classes.Address)

        sess = Session()
        class SubAddr(Address):
            pass
        mapper(SubAddr, inherits=Address, properties={
            'flub': relationship(Dingaling)
        })

        q = sess.query(User)
        opt = self._option_fixture(User.addresses.of_type(SubAddr), SubAddr.flub)

        u_mapper = inspect(User)
        sa_mapper = inspect(SubAddr)
        self._assert_path_result(opt, q, [
            (u_mapper, u_mapper.attrs.addresses),
            (u_mapper, u_mapper.attrs.addresses, sa_mapper, sa_mapper.attrs.flub)
        ])

    def test_aliased_single(self):
        User = self.classes.User

        sess = Session()
        ualias = aliased(User)
        q = sess.query(ualias)
        opt = self._option_fixture(ualias.addresses)
        self._assert_path_result(opt, q, [(inspect(ualias), 'addresses')])

    def test_with_current_aliased_single(self):
        User, Address = self.classes.User, self.classes.Address

        sess = Session()
        ualias = aliased(User)
        q = sess.query(ualias)._with_current_path(
                        self._make_path_registry([Address, 'user'])
                )
        opt = self._option_fixture(Address.user, ualias.addresses)
        self._assert_path_result(opt, q, [(inspect(ualias), 'addresses')])

    def test_with_current_aliased_single_nonmatching_option(self):
        User, Address = self.classes.User, self.classes.Address

        sess = Session()
        ualias = aliased(User)
        q = sess.query(User)._with_current_path(
                        self._make_path_registry([Address, 'user'])
                )
        opt = self._option_fixture(Address.user, ualias.addresses)
        self._assert_path_result(opt, q, [])

    def test_with_current_aliased_single_nonmatching_entity(self):
        User, Address = self.classes.User, self.classes.Address

        sess = Session()
        ualias = aliased(User)
        q = sess.query(ualias)._with_current_path(
                        self._make_path_registry([Address, 'user'])
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
                        self._make_path_registry([User, 'orders', Order, 'items'])
                )
        self._assert_path_result(opt, q, [])

    def test_chained(self):
        User = self.classes.User
        Order = self.classes.Order
        Item = self.classes.Item
        sess = Session()
        q = sess.query(User)
        opt = self._option_fixture(User.orders).joinedload("items")
        self._assert_path_result(opt, q, [
                (User, 'orders'),
                (User, 'orders', Order, "items")
            ])

    def test_chained_plus_dotted(self):
        User = self.classes.User
        Order = self.classes.Order
        Item = self.classes.Item
        sess = Session()
        q = sess.query(User)
        opt = self._option_fixture("orders.items").joinedload("keywords")
        self._assert_path_result(opt, q, [
                (User, 'orders'),
                (User, 'orders', Order, "items"),
                (User, 'orders', Order, "items", Item, "keywords")
            ])

    def test_chained_plus_multi(self):
        User = self.classes.User
        Order = self.classes.Order
        Item = self.classes.Item
        sess = Session()
        q = sess.query(User)
        opt = self._option_fixture(User.orders, Order.items).joinedload("keywords")
        self._assert_path_result(opt, q, [
                (User, 'orders'),
                (User, 'orders', Order, "items"),
                (User, 'orders', Order, "items", Item, "keywords")
            ])


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

        self._assert_option([Item], 'keywords')

    def test_option_with_mapper_PropCompatator(self):
        Item = self.classes.Item

        self._assert_option([Item], Item.keywords)

    def test_option_with_mapper_then_column_basestring(self):
        Item = self.classes.Item

        self._assert_option([Item, Item.id], 'keywords')

    def test_option_with_mapper_then_column_PropComparator(self):
        Item = self.classes.Item

        self._assert_option([Item, Item.id], Item.keywords)

    def test_option_with_column_then_mapper_basestring(self):
        Item = self.classes.Item

        self._assert_option([Item.id, Item], 'keywords')

    def test_option_with_column_then_mapper_PropComparator(self):
        Item = self.classes.Item

        self._assert_option([Item.id, Item], Item.keywords)

    def test_option_with_column_basestring(self):
        Item = self.classes.Item

        message = \
            "Query has only expression-based entities - "\
            "can't find property named 'keywords'."
        self._assert_eager_with_just_column_exception(Item.id,
                'keywords', message)

    def test_option_with_column_PropComparator(self):
        Item = self.classes.Item

        self._assert_eager_with_just_column_exception(Item.id,
                Item.keywords,
                "Query has only expression-based entities "
                "- can't find property named 'keywords'."
                )

    def test_option_against_nonexistent_PropComparator(self):
        Item = self.classes.Item
        Keyword = self.classes.Keyword
        self._assert_eager_with_entity_exception(
            [Keyword],
            (joinedload(Item.keywords), ),
            r"Can't find property 'keywords' on any entity specified "
            r"in this Query.  Note the full path from root "
            r"\(Mapper\|Keyword\|keywords\) to target entity must be specified."
        )

    def test_option_against_nonexistent_basestring(self):
        Item = self.classes.Item
        self._assert_eager_with_entity_exception(
            [Item],
            (joinedload("foo"), ),
            r"Can't find property named 'foo' on the mapped "
            r"entity Mapper\|Item\|items in this Query."
        )

    def test_option_against_nonexistent_twolevel_basestring(self):
        Item = self.classes.Item
        self._assert_eager_with_entity_exception(
            [Item],
            (joinedload("keywords.foo"), ),
            r"Can't find property named 'foo' on the mapped entity "
            r"Mapper\|Keyword\|keywords in this Query."
        )

    def test_option_against_nonexistent_twolevel_all(self):
        Item = self.classes.Item
        self._assert_eager_with_entity_exception(
            [Item],
            (joinedload_all("keywords.foo"), ),
            r"Can't find property named 'foo' on the mapped entity "
            r"Mapper\|Keyword\|keywords in this Query."
        )

    @testing.fails_if(lambda: True,
        "PropertyOption doesn't yet check for relation/column on end result")
    def test_option_against_non_relation_basestring(self):
        Item = self.classes.Item
        Keyword = self.classes.Keyword
        self._assert_eager_with_entity_exception(
            [Keyword, Item],
            (joinedload_all("keywords"), ),
            r"Attribute 'keywords' of entity 'Mapper\|Keyword\|keywords' "
            "does not refer to a mapped entity"
        )

    @testing.fails_if(lambda: True,
            "PropertyOption doesn't yet check for relation/column on end result")
    def test_option_against_multi_non_relation_basestring(self):
        Item = self.classes.Item
        Keyword = self.classes.Keyword
        self._assert_eager_with_entity_exception(
            [Keyword, Item],
            (joinedload_all("keywords"), ),
            r"Attribute 'keywords' of entity 'Mapper\|Keyword\|keywords' "
            "does not refer to a mapped entity"
        )

    def test_option_against_wrong_entity_type_basestring(self):
        Item = self.classes.Item
        self._assert_eager_with_entity_exception(
            [Item],
            (joinedload_all("id", "keywords"), ),
            r"Attribute 'id' of entity 'Mapper\|Item\|items' does not "
            r"refer to a mapped entity"
        )

    def test_option_against_multi_non_relation_twolevel_basestring(self):
        Item = self.classes.Item
        Keyword = self.classes.Keyword
        self._assert_eager_with_entity_exception(
            [Keyword, Item],
            (joinedload_all("id", "keywords"), ),
            r"Attribute 'id' of entity 'Mapper\|Keyword\|keywords' "
            "does not refer to a mapped entity"
        )

    def test_option_against_multi_nonexistent_basestring(self):
        Item = self.classes.Item
        Keyword = self.classes.Keyword
        self._assert_eager_with_entity_exception(
            [Keyword, Item],
            (joinedload_all("description"), ),
            r"Can't find property named 'description' on the mapped "
            r"entity Mapper\|Keyword\|keywords in this Query."
        )

    def test_option_against_multi_no_entities_basestring(self):
        Item = self.classes.Item
        Keyword = self.classes.Keyword
        self._assert_eager_with_entity_exception(
            [Keyword.id, Item.id],
            (joinedload_all("keywords"), ),
            r"Query has only expression-based entities - can't find property "
            "named 'keywords'."
        )

    def test_option_against_wrong_multi_entity_type_attr_one(self):
        Item = self.classes.Item
        Keyword = self.classes.Keyword
        self._assert_eager_with_entity_exception(
            [Keyword, Item],
            (joinedload_all(Keyword.id, Item.keywords), ),
            r"Attribute 'id' of entity 'Mapper\|Keyword\|keywords' "
            "does not refer to a mapped entity"
        )

    def test_option_against_wrong_multi_entity_type_attr_two(self):
        Item = self.classes.Item
        Keyword = self.classes.Keyword
        self._assert_eager_with_entity_exception(
            [Keyword, Item],
            (joinedload_all(Keyword.keywords, Item.keywords), ),
            r"Attribute 'keywords' of entity 'Mapper\|Keyword\|keywords' "
            "does not refer to a mapped entity"
        )

    def test_option_against_wrong_multi_entity_type_attr_three(self):
        Item = self.classes.Item
        Keyword = self.classes.Keyword
        self._assert_eager_with_entity_exception(
            [Keyword.id, Item.id],
            (joinedload_all(Keyword.keywords, Item.keywords), ),
            r"Query has only expression-based entities - "
            "can't find property named 'keywords'."
        )

    def test_wrong_type_in_option(self):
        Item = self.classes.Item
        Keyword = self.classes.Keyword
        self._assert_eager_with_entity_exception(
            [Item],
            (joinedload_all(Keyword), ),
            r"mapper option expects string key or list of attributes"
        )

    def test_non_contiguous_all_option(self):
        User = self.classes.User
        self._assert_eager_with_entity_exception(
            [User],
            (joinedload_all(User.addresses, User.orders), ),
            r"Attribute 'User.orders' does not link "
            "from element 'Mapper|Address|addresses'"
        )

    def test_non_contiguous_all_option_of_type(self):
        User = self.classes.User
        Order = self.classes.Order
        self._assert_eager_with_entity_exception(
            [User],
            (joinedload_all(User.addresses, User.orders.of_type(Order)), ),
            r"Attribute 'User.orders' does not link "
            "from element 'Mapper|Address|addresses'"
        )

    @classmethod
    def setup_mappers(cls):
        users, User, addresses, Address, orders, Order = (
                    cls.tables.users, cls.classes.User,
                    cls.tables.addresses, cls.classes.Address,
                    cls.tables.orders, cls.classes.Order)
        mapper(User, users, properties={
            'addresses': relationship(Address),
            'orders': relationship(Order)
        })
        mapper(Address, addresses)
        mapper(Order, orders)
        keywords, items, item_keywords, Keyword, Item = (cls.tables.keywords,
                                cls.tables.items,
                                cls.tables.item_keywords,
                                cls.classes.Keyword,
                                cls.classes.Item)
        mapper(Keyword, keywords, properties={
            "keywords": column_property(keywords.c.name + "some keyword")
        })
        mapper(Item, items,
               properties=dict(keywords=relationship(Keyword,
               secondary=item_keywords)))

    def _assert_option(self, entity_list, option):
        Item = self.classes.Item

        q = create_session().query(*entity_list).\
                            options(joinedload(option))
        key = ('loader', (inspect(Item), inspect(Item).attrs.keywords))
        assert key in q._attributes

    def _assert_eager_with_entity_exception(self, entity_list, options,
                                message):
        assert_raises_message(sa.exc.ArgumentError,
                                message,
                              create_session().query(*entity_list).options,
                              *options)

    def _assert_eager_with_just_column_exception(self, column,
            eager_option, message):
        assert_raises_message(sa.exc.ArgumentError, message,
                              create_session().query(column).options,
                              joinedload(eager_option))

