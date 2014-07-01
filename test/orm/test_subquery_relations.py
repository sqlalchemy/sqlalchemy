from sqlalchemy.testing import eq_, is_, is_not_
from sqlalchemy import testing
from sqlalchemy.testing.schema import Table, Column
from sqlalchemy import Integer, String, ForeignKey, bindparam, inspect
from sqlalchemy.orm import backref, subqueryload, subqueryload_all, \
    mapper, relationship, clear_mappers, create_session, lazyload, \
    aliased, joinedload, deferred, undefer, eagerload_all,\
    Session
from sqlalchemy.testing import eq_, assert_raises, \
    assert_raises_message
from sqlalchemy.testing.assertsql import CompiledSQL
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.entities import ComparableEntity
from test.orm import _fixtures
import sqlalchemy as sa

from sqlalchemy.orm import with_polymorphic

class EagerTest(_fixtures.FixtureTest, testing.AssertsCompiledSQL):
    run_inserts = 'once'
    run_deletes = None

    def test_basic(self):
        users, Address, addresses, User = (self.tables.users,
                                self.classes.Address,
                                self.tables.addresses,
                                self.classes.User)

        mapper(User, users, properties={
            'addresses':relationship(
                            mapper(Address, addresses),
                            order_by=Address.id)
        })
        sess = create_session()

        q = sess.query(User).options(subqueryload(User.addresses))

        def go():
            eq_(
                    [User(id=7, addresses=[
                            Address(id=1, email_address='jack@bean.com')])],
                    q.filter(User.id==7).all()
            )

        self.assert_sql_count(testing.db, go, 2)

        def go():
            eq_(
                self.static.user_address_result,
                q.order_by(User.id).all()
            )
        self.assert_sql_count(testing.db, go, 2)

    def test_from_aliased(self):
        users, Dingaling, User, dingalings, Address, addresses = (self.tables.users,
                                self.classes.Dingaling,
                                self.classes.User,
                                self.tables.dingalings,
                                self.classes.Address,
                                self.tables.addresses)

        mapper(Dingaling, dingalings)
        mapper(Address, addresses, properties={
            'dingalings':relationship(Dingaling, order_by=Dingaling.id)
        })
        mapper(User, users, properties={
            'addresses':relationship(
                            Address,
                            order_by=Address.id)
        })
        sess = create_session()

        u = aliased(User)

        q = sess.query(u).options(subqueryload(u.addresses))

        def go():
            eq_(
                    [User(id=7, addresses=[
                            Address(id=1, email_address='jack@bean.com')])],
                    q.filter(u.id==7).all()
            )

        self.assert_sql_count(testing.db, go, 2)

        def go():
            eq_(
                self.static.user_address_result,
                q.order_by(u.id).all()
            )
        self.assert_sql_count(testing.db, go, 2)

        q = sess.query(u).\
                        options(subqueryload_all(u.addresses, Address.dingalings))

        def go():
            eq_(
                [
                    User(id=8, addresses=[
                        Address(id=2, email_address='ed@wood.com', dingalings=[Dingaling()]),
                        Address(id=3, email_address='ed@bettyboop.com'),
                        Address(id=4, email_address='ed@lala.com'),
                    ]),
                    User(id=9, addresses=[
                        Address(id=5, dingalings=[Dingaling()])
                    ]),
                ],
                q.filter(u.id.in_([8, 9])).all()
            )
        self.assert_sql_count(testing.db, go, 3)


    def test_from_get(self):
        users, Address, addresses, User = (self.tables.users,
                                self.classes.Address,
                                self.tables.addresses,
                                self.classes.User)

        mapper(User, users, properties={
            'addresses':relationship(
                            mapper(Address, addresses),
                            order_by=Address.id)
        })
        sess = create_session()

        q = sess.query(User).options(subqueryload(User.addresses))
        def go():
            eq_(
                    User(id=7, addresses=[
                            Address(id=1, email_address='jack@bean.com')]),
                    q.get(7)
            )

        self.assert_sql_count(testing.db, go, 2)

    def test_from_params(self):
        users, Address, addresses, User = (self.tables.users,
                                self.classes.Address,
                                self.tables.addresses,
                                self.classes.User)

        mapper(User, users, properties={
            'addresses':relationship(
                            mapper(Address, addresses),
                            order_by=Address.id)
        })
        sess = create_session()

        q = sess.query(User).options(subqueryload(User.addresses))
        def go():
            eq_(
                    User(id=7, addresses=[
                            Address(id=1, email_address='jack@bean.com')]),
                    q.filter(User.id==bindparam('foo')).params(foo=7).one()
            )

        self.assert_sql_count(testing.db, go, 2)

    def test_disable_dynamic(self):
        """test no subquery option on a dynamic."""

        users, Address, addresses, User = (self.tables.users,
                                self.classes.Address,
                                self.tables.addresses,
                                self.classes.User)


        mapper(User, users, properties={
            'addresses':relationship(Address, lazy="dynamic")
        })
        mapper(Address, addresses)
        sess = create_session()

        # previously this would not raise, but would emit
        # the query needlessly and put the result nowhere.
        assert_raises_message(
            sa.exc.InvalidRequestError,
            "User.addresses' does not support object population - eager loading cannot be applied.",
            sess.query(User).options(subqueryload(User.addresses)).first,
        )

    def test_many_to_many_plain(self):
        keywords, items, item_keywords, Keyword, Item = (self.tables.keywords,
                                self.tables.items,
                                self.tables.item_keywords,
                                self.classes.Keyword,
                                self.classes.Item)

        mapper(Keyword, keywords)
        mapper(Item, items, properties = dict(
                keywords = relationship(Keyword, secondary=item_keywords,
                                    lazy='subquery', order_by=keywords.c.id)))

        q = create_session().query(Item).order_by(Item.id)
        def go():
            eq_(self.static.item_keyword_result, q.all())
        self.assert_sql_count(testing.db, go, 2)

    def test_many_to_many_with_join(self):
        keywords, items, item_keywords, Keyword, Item = (self.tables.keywords,
                                self.tables.items,
                                self.tables.item_keywords,
                                self.classes.Keyword,
                                self.classes.Item)

        mapper(Keyword, keywords)
        mapper(Item, items, properties = dict(
                keywords = relationship(Keyword, secondary=item_keywords,
                                    lazy='subquery', order_by=keywords.c.id)))

        q = create_session().query(Item).order_by(Item.id)
        def go():
            eq_(self.static.item_keyword_result[0:2],
                q.join('keywords').filter(Keyword.name == 'red').all())
        self.assert_sql_count(testing.db, go, 2)

    def test_many_to_many_with_join_alias(self):
        keywords, items, item_keywords, Keyword, Item = (self.tables.keywords,
                                self.tables.items,
                                self.tables.item_keywords,
                                self.classes.Keyword,
                                self.classes.Item)

        mapper(Keyword, keywords)
        mapper(Item, items, properties = dict(
                keywords = relationship(Keyword, secondary=item_keywords,
                                    lazy='subquery', order_by=keywords.c.id)))

        q = create_session().query(Item).order_by(Item.id)
        def go():
            eq_(self.static.item_keyword_result[0:2],
                (q.join('keywords', aliased=True).
                 filter(Keyword.name == 'red')).all())
        self.assert_sql_count(testing.db, go, 2)

    def test_orderby(self):
        users, Address, addresses, User = (self.tables.users,
                                self.classes.Address,
                                self.tables.addresses,
                                self.classes.User)

        mapper(User, users, properties = {
            'addresses':relationship(mapper(Address, addresses),
                        lazy='subquery', order_by=addresses.c.email_address),
        })
        q = create_session().query(User)
        eq_([
            User(id=7, addresses=[
                Address(id=1)
            ]),
            User(id=8, addresses=[
                Address(id=3, email_address='ed@bettyboop.com'),
                Address(id=4, email_address='ed@lala.com'),
                Address(id=2, email_address='ed@wood.com')
            ]),
            User(id=9, addresses=[
                Address(id=5)
            ]),
            User(id=10, addresses=[])
        ], q.order_by(User.id).all())

    def test_orderby_multi(self):
        users, Address, addresses, User = (self.tables.users,
                                self.classes.Address,
                                self.tables.addresses,
                                self.classes.User)

        mapper(User, users, properties = {
            'addresses':relationship(mapper(Address, addresses),
                            lazy='subquery',
                            order_by=[
                                    addresses.c.email_address,
                                    addresses.c.id]),
        })
        q = create_session().query(User)
        eq_([
            User(id=7, addresses=[
                Address(id=1)
            ]),
            User(id=8, addresses=[
                Address(id=3, email_address='ed@bettyboop.com'),
                Address(id=4, email_address='ed@lala.com'),
                Address(id=2, email_address='ed@wood.com')
            ]),
            User(id=9, addresses=[
                Address(id=5)
            ]),
            User(id=10, addresses=[])
        ], q.order_by(User.id).all())

    def test_orderby_related(self):
        """A regular mapper select on a single table can
            order by a relationship to a second table"""

        Address, addresses, users, User = (self.classes.Address,
                                self.tables.addresses,
                                self.tables.users,
                                self.classes.User)


        mapper(Address, addresses)
        mapper(User, users, properties = dict(
            addresses = relationship(Address,
                                        lazy='subquery',
                                        order_by=addresses.c.id),
        ))

        q = create_session().query(User)
        l = q.filter(User.id==Address.user_id).\
            order_by(Address.email_address).all()

        eq_([
            User(id=8, addresses=[
                Address(id=2, email_address='ed@wood.com'),
                Address(id=3, email_address='ed@bettyboop.com'),
                Address(id=4, email_address='ed@lala.com'),
            ]),
            User(id=9, addresses=[
                Address(id=5)
            ]),
            User(id=7, addresses=[
                Address(id=1)
            ]),
        ], l)

    def test_orderby_desc(self):
        Address, addresses, users, User = (self.classes.Address,
                                self.tables.addresses,
                                self.tables.users,
                                self.classes.User)

        mapper(Address, addresses)
        mapper(User, users, properties = dict(
            addresses = relationship(Address, lazy='subquery',
                                 order_by=[
                                    sa.desc(addresses.c.email_address)
                                ]),
        ))
        sess = create_session()
        eq_([
            User(id=7, addresses=[
                Address(id=1)
            ]),
            User(id=8, addresses=[
                Address(id=2, email_address='ed@wood.com'),
                Address(id=4, email_address='ed@lala.com'),
                Address(id=3, email_address='ed@bettyboop.com'),
            ]),
            User(id=9, addresses=[
                Address(id=5)
            ]),
            User(id=10, addresses=[])
        ], sess.query(User).order_by(User.id).all())

    _pathing_runs = [
        ( "lazyload", "lazyload", "lazyload", 15 ),
        ("subqueryload", "lazyload", "lazyload", 12),
        ("subqueryload", "subqueryload", "lazyload", 8),
        ("joinedload", "subqueryload", "lazyload", 7),
        ("lazyload", "lazyload", "subqueryload", 12),
        ("subqueryload", "subqueryload", "subqueryload", 4),
        ("subqueryload", "subqueryload", "joinedload", 3),
    ]

    def test_options_pathing(self):
        self._do_options_test(self._pathing_runs)

    def test_mapper_pathing(self):
        self._do_mapper_test(self._pathing_runs)

    def _do_options_test(self, configs):
        users, Keyword, orders, items, order_items, Order, Item, User, keywords, item_keywords = (self.tables.users,
                                self.classes.Keyword,
                                self.tables.orders,
                                self.tables.items,
                                self.tables.order_items,
                                self.classes.Order,
                                self.classes.Item,
                                self.classes.User,
                                self.tables.keywords,
                                self.tables.item_keywords)

        mapper(User, users, properties={
            'orders':relationship(Order, order_by=orders.c.id), # o2m, m2o
        })
        mapper(Order, orders, properties={
            'items':relationship(Item,
                        secondary=order_items, order_by=items.c.id),  #m2m
        })
        mapper(Item, items, properties={
            'keywords':relationship(Keyword,
                                        secondary=item_keywords,
                                        order_by=keywords.c.id) #m2m
        })
        mapper(Keyword, keywords)

        callables = {
                        'joinedload':joinedload,
                    'subqueryload':subqueryload
                }

        for o, i, k, count in configs:
            options = []
            if o in callables:
                options.append(callables[o](User.orders))
            if i in callables:
                options.append(callables[i](User.orders, Order.items))
            if k in callables:
                options.append(callables[k](User.orders, Order.items, Item.keywords))

            self._do_query_tests(options, count)

    def _do_mapper_test(self, configs):
        users, Keyword, orders, items, order_items, Order, Item, User, keywords, item_keywords = (self.tables.users,
                                self.classes.Keyword,
                                self.tables.orders,
                                self.tables.items,
                                self.tables.order_items,
                                self.classes.Order,
                                self.classes.Item,
                                self.classes.User,
                                self.tables.keywords,
                                self.tables.item_keywords)

        opts = {
            'lazyload':'select',
            'joinedload':'joined',
            'subqueryload':'subquery',
        }

        for o, i, k, count in configs:
            mapper(User, users, properties={
                'orders':relationship(Order, lazy=opts[o], order_by=orders.c.id),
            })
            mapper(Order, orders, properties={
                'items':relationship(Item,
                            secondary=order_items, lazy=opts[i], order_by=items.c.id),
            })
            mapper(Item, items, properties={
                'keywords':relationship(Keyword,
                                            lazy=opts[k],
                                            secondary=item_keywords,
                                            order_by=keywords.c.id)
            })
            mapper(Keyword, keywords)

            try:
                self._do_query_tests([], count)
            finally:
                clear_mappers()

    def _do_query_tests(self, opts, count):
        Order, User = self.classes.Order, self.classes.User

        sess = create_session()
        def go():
            eq_(
                sess.query(User).options(*opts).order_by(User.id).all(),
                self.static.user_item_keyword_result
            )
        self.assert_sql_count(testing.db, go, count)

        eq_(
            sess.query(User).options(*opts).filter(User.name=='fred').
                    order_by(User.id).all(),
            self.static.user_item_keyword_result[2:3]
        )

        sess = create_session()
        eq_(
            sess.query(User).options(*opts).join(User.orders).
                    filter(Order.id==3).\
                    order_by(User.id).all(),
            self.static.user_item_keyword_result[0:1]
        )


    def test_cyclical(self):
        """A circular eager relationship breaks the cycle with a lazy loader"""

        Address, addresses, users, User = (self.classes.Address,
                                self.tables.addresses,
                                self.tables.users,
                                self.classes.User)


        mapper(Address, addresses)
        mapper(User, users, properties = dict(
            addresses = relationship(Address, lazy='subquery',
                                 backref=sa.orm.backref('user', lazy='subquery'),
                                            order_by=Address.id)
        ))
        is_(sa.orm.class_mapper(User).get_property('addresses').lazy, 'subquery')
        is_(sa.orm.class_mapper(Address).get_property('user').lazy, 'subquery')

        sess = create_session()
        eq_(self.static.user_address_result, sess.query(User).order_by(User.id).all())

    def test_double(self):
        """Eager loading with two relationships simultaneously,
            from the same table, using aliases."""

        users, orders, User, Address, Order, addresses = (self.tables.users,
                                self.tables.orders,
                                self.classes.User,
                                self.classes.Address,
                                self.classes.Order,
                                self.tables.addresses)


        openorders = sa.alias(orders, 'openorders')
        closedorders = sa.alias(orders, 'closedorders')

        mapper(Address, addresses)
        mapper(Order, orders)

        open_mapper = mapper(Order, openorders, non_primary=True)
        closed_mapper = mapper(Order, closedorders, non_primary=True)

        mapper(User, users, properties = dict(
            addresses = relationship(Address, lazy='subquery',
                                        order_by=addresses.c.id),
            open_orders = relationship(
                open_mapper,
                primaryjoin=sa.and_(openorders.c.isopen == 1,
                                 users.c.id==openorders.c.user_id),
                lazy='subquery', order_by=openorders.c.id),
            closed_orders = relationship(
                closed_mapper,
                primaryjoin=sa.and_(closedorders.c.isopen == 0,
                                 users.c.id==closedorders.c.user_id),
                lazy='subquery', order_by=closedorders.c.id)))

        q = create_session().query(User).order_by(User.id)

        def go():
            eq_([
                User(
                    id=7,
                    addresses=[Address(id=1)],
                    open_orders = [Order(id=3)],
                    closed_orders = [Order(id=1), Order(id=5)]
                ),
                User(
                    id=8,
                    addresses=[Address(id=2), Address(id=3), Address(id=4)],
                    open_orders = [],
                    closed_orders = []
                ),
                User(
                    id=9,
                    addresses=[Address(id=5)],
                    open_orders = [Order(id=4)],
                    closed_orders = [Order(id=2)]
                ),
                User(id=10)

            ], q.all())
        self.assert_sql_count(testing.db, go, 4)

    def test_double_same_mappers(self):
        """Eager loading with two relationships simulatneously,
        from the same table, using aliases."""

        addresses, items, order_items, orders, Item, User, Address, Order, users = (self.tables.addresses,
                                self.tables.items,
                                self.tables.order_items,
                                self.tables.orders,
                                self.classes.Item,
                                self.classes.User,
                                self.classes.Address,
                                self.classes.Order,
                                self.tables.users)


        mapper(Address, addresses)
        mapper(Order, orders, properties={
            'items': relationship(Item, secondary=order_items, lazy='subquery',
                              order_by=items.c.id)})
        mapper(Item, items)
        mapper(User, users, properties=dict(
            addresses=relationship(Address, lazy='subquery', order_by=addresses.c.id),
            open_orders=relationship(
                Order,
                primaryjoin=sa.and_(orders.c.isopen == 1,
                                 users.c.id==orders.c.user_id),
                lazy='subquery', order_by=orders.c.id),
            closed_orders=relationship(
                Order,
                primaryjoin=sa.and_(orders.c.isopen == 0,
                                 users.c.id==orders.c.user_id),
                lazy='subquery', order_by=orders.c.id)))
        q = create_session().query(User).order_by(User.id)

        def go():
            eq_([
                User(id=7,
                     addresses=[
                       Address(id=1)],
                     open_orders=[Order(id=3,
                                        items=[
                                          Item(id=3),
                                          Item(id=4),
                                          Item(id=5)])],
                     closed_orders=[Order(id=1,
                                          items=[
                                            Item(id=1),
                                            Item(id=2),
                                            Item(id=3)]),
                                    Order(id=5,
                                          items=[
                                            Item(id=5)])]),
                User(id=8,
                     addresses=[
                       Address(id=2),
                       Address(id=3),
                       Address(id=4)],
                     open_orders = [],
                     closed_orders = []),
                User(id=9,
                     addresses=[
                       Address(id=5)],
                     open_orders=[
                       Order(id=4,
                             items=[
                               Item(id=1),
                               Item(id=5)])],
                     closed_orders=[
                       Order(id=2,
                             items=[
                               Item(id=1),
                               Item(id=2),
                               Item(id=3)])]),
                User(id=10)
            ], q.all())
        self.assert_sql_count(testing.db, go, 6)

    def test_limit(self):
        """Limit operations combined with lazy-load relationships."""

        users, items, order_items, orders, Item, User, Address, Order, addresses = (self.tables.users,
                                self.tables.items,
                                self.tables.order_items,
                                self.tables.orders,
                                self.classes.Item,
                                self.classes.User,
                                self.classes.Address,
                                self.classes.Order,
                                self.tables.addresses)


        mapper(Item, items)
        mapper(Order, orders, properties={
            'items':relationship(Item, secondary=order_items, lazy='subquery',
                order_by=items.c.id)
        })
        mapper(User, users, properties={
            'addresses':relationship(mapper(Address, addresses),
                            lazy='subquery',
                            order_by=addresses.c.id),
            'orders':relationship(Order, lazy='select', order_by=orders.c.id)
        })

        sess = create_session()
        q = sess.query(User)

        l = q.order_by(User.id).limit(2).offset(1).all()
        eq_(self.static.user_all_result[1:3], l)

        sess = create_session()
        l = q.order_by(sa.desc(User.id)).limit(2).offset(2).all()
        eq_(list(reversed(self.static.user_all_result[0:2])), l)

    def test_mapper_order_by(self):
        users, User, Address, addresses = (self.tables.users,
                                self.classes.User,
                                self.classes.Address,
                                self.tables.addresses)

        mapper(Address, addresses)
        mapper(User, users, properties={
            'addresses':relationship(Address,
                            lazy='subquery',
                            order_by=addresses.c.id),
        },order_by=users.c.id.desc())

        sess = create_session()
        q = sess.query(User)

        l = q.limit(2).all()
        eq_(l, list(reversed(self.static.user_address_result[2:4])))


    def test_one_to_many_scalar(self):
        Address, addresses, users, User = (self.classes.Address,
                                self.tables.addresses,
                                self.tables.users,
                                self.classes.User)

        mapper(User, users, properties = dict(
            address = relationship(mapper(Address, addresses),
                                    lazy='subquery', uselist=False)
        ))
        q = create_session().query(User)

        def go():
            l = q.filter(users.c.id == 7).all()
            eq_([User(id=7, address=Address(id=1))], l)
        self.assert_sql_count(testing.db, go, 2)

    def test_many_to_one(self):
        users, Address, addresses, User = (self.tables.users,
                                self.classes.Address,
                                self.tables.addresses,
                                self.classes.User)

        mapper(Address, addresses, properties = dict(
            user = relationship(mapper(User, users), lazy='subquery')
        ))
        sess = create_session()
        q = sess.query(Address)

        def go():
            a = q.filter(addresses.c.id==1).one()
            is_not_(a.user, None)
            u1 = sess.query(User).get(7)
            is_(a.user, u1)
        self.assert_sql_count(testing.db, go, 2)

    def test_double_with_aggregate(self):
        User, users, orders, Order = (self.classes.User,
                                self.tables.users,
                                self.tables.orders,
                                self.classes.Order)

        max_orders_by_user = sa.select([sa.func.max(orders.c.id).label('order_id')],
                                       group_by=[orders.c.user_id]
                                     ).alias('max_orders_by_user')

        max_orders = orders.select(orders.c.id==max_orders_by_user.c.order_id).\
                                alias('max_orders')

        mapper(Order, orders)
        mapper(User, users, properties={
               'orders':relationship(Order, backref='user', lazy='subquery',
                                            order_by=orders.c.id),
               'max_order':relationship(
                                mapper(Order, max_orders, non_primary=True),
                                lazy='subquery', uselist=False)
               })

        q = create_session().query(User)

        def go():
            eq_([
                User(id=7, orders=[
                        Order(id=1),
                        Order(id=3),
                        Order(id=5),
                    ],
                    max_order=Order(id=5)
                ),
                User(id=8, orders=[]),
                User(id=9, orders=[Order(id=2),Order(id=4)],
                    max_order=Order(id=4)
                ),
                User(id=10),
            ], q.order_by(User.id).all())
        self.assert_sql_count(testing.db, go, 3)

    def test_uselist_false_warning(self):
        """test that multiple rows received by a
        uselist=False raises a warning."""

        User, users, orders, Order = (self.classes.User,
                                self.tables.users,
                                self.tables.orders,
                                self.classes.Order)


        mapper(User, users, properties={
            'order':relationship(Order, uselist=False)
        })
        mapper(Order, orders)
        s = create_session()
        assert_raises(sa.exc.SAWarning,
                s.query(User).options(subqueryload(User.order)).all)

class LoadOnExistingTest(_fixtures.FixtureTest):
    """test that loaders from a base Query fully populate."""

    run_inserts = 'once'
    run_deletes = None

    def _collection_to_scalar_fixture(self):
        User, Address, Dingaling = self.classes.User, \
            self.classes.Address, self.classes.Dingaling
        mapper(User, self.tables.users, properties={
            'addresses':relationship(Address),
        })
        mapper(Address, self.tables.addresses, properties={
            'dingaling':relationship(Dingaling)
        })
        mapper(Dingaling, self.tables.dingalings)

        sess = Session(autoflush=False)
        return User, Address, Dingaling, sess

    def _collection_to_collection_fixture(self):
        User, Order, Item = self.classes.User, \
            self.classes.Order, self.classes.Item
        mapper(User, self.tables.users, properties={
            'orders':relationship(Order),
        })
        mapper(Order, self.tables.orders, properties={
            'items':relationship(Item, secondary=self.tables.order_items),
        })
        mapper(Item, self.tables.items)

        sess = Session(autoflush=False)
        return User, Order, Item, sess

    def _eager_config_fixture(self):
        User, Address = self.classes.User, self.classes.Address
        mapper(User, self.tables.users, properties={
            'addresses':relationship(Address, lazy="subquery"),
        })
        mapper(Address, self.tables.addresses)
        sess = Session(autoflush=False)
        return User, Address, sess

    def _deferred_config_fixture(self):
        User, Address = self.classes.User, self.classes.Address
        mapper(User, self.tables.users, properties={
            'name':deferred(self.tables.users.c.name),
            'addresses':relationship(Address, lazy="subquery"),
        })
        mapper(Address, self.tables.addresses)
        sess = Session(autoflush=False)
        return User, Address, sess

    def test_no_query_on_refresh(self):
        User, Address, sess = self._eager_config_fixture()

        u1 = sess.query(User).get(8)
        assert 'addresses' in u1.__dict__
        sess.expire(u1)
        def go():
            eq_(u1.id, 8)
        self.assert_sql_count(testing.db, go, 1)
        assert 'addresses' not in u1.__dict__

    def test_no_query_on_deferred(self):
        User, Address, sess = self._deferred_config_fixture()
        u1 = sess.query(User).get(8)
        assert 'addresses' in u1.__dict__
        sess.expire(u1, ['addresses'])
        def go():
            eq_(u1.name, 'ed')
        self.assert_sql_count(testing.db, go, 1)
        assert 'addresses' not in u1.__dict__

    def test_populate_existing_propagate(self):
        User, Address, sess = self._eager_config_fixture()
        u1 = sess.query(User).get(8)
        u1.addresses[2].email_address = "foofoo"
        del u1.addresses[1]
        u1 = sess.query(User).populate_existing().filter_by(id=8).one()
        # collection is reverted
        eq_(len(u1.addresses), 3)

        # attributes on related items reverted
        eq_(u1.addresses[2].email_address, "ed@lala.com")

    def test_loads_second_level_collection_to_scalar(self):
        User, Address, Dingaling, sess = self._collection_to_scalar_fixture()

        u1 = sess.query(User).get(8)
        a1 = Address()
        u1.addresses.append(a1)
        a2 = u1.addresses[0]
        a2.email_address = 'foo'
        sess.query(User).options(subqueryload_all("addresses.dingaling")).\
                            filter_by(id=8).all()
        assert u1.addresses[-1] is a1
        for a in u1.addresses:
            if a is not a1:
                assert 'dingaling' in a.__dict__
            else:
                assert 'dingaling' not in a.__dict__
            if a is a2:
                eq_(a2.email_address, 'foo')

    def test_loads_second_level_collection_to_collection(self):
        User, Order, Item, sess = self._collection_to_collection_fixture()

        u1 = sess.query(User).get(7)
        u1.orders
        o1 = Order()
        u1.orders.append(o1)
        sess.query(User).options(subqueryload_all("orders.items")).\
                            filter_by(id=7).all()
        for o in u1.orders:
            if o is not o1:
                assert 'items' in o.__dict__
            else:
                assert 'items' not in o.__dict__

    def test_load_two_levels_collection_to_scalar(self):
        User, Address, Dingaling, sess = self._collection_to_scalar_fixture()

        u1 = sess.query(User).filter_by(id=8).options(subqueryload("addresses")).one()
        sess.query(User).filter_by(id=8).options(subqueryload_all("addresses.dingaling")).first()
        assert 'dingaling' in u1.addresses[0].__dict__

    def test_load_two_levels_collection_to_collection(self):
        User, Order, Item, sess = self._collection_to_collection_fixture()

        u1 = sess.query(User).filter_by(id=7).options(subqueryload("orders")).one()
        sess.query(User).filter_by(id=7).options(subqueryload_all("orders.items")).first()
        assert 'items' in u1.orders[0].__dict__

class OrderBySecondaryTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table('m2m', metadata,
              Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
              Column('aid', Integer, ForeignKey('a.id')),
              Column('bid', Integer, ForeignKey('b.id')))

        Table('a', metadata,
              Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
              Column('data', String(50)))
        Table('b', metadata,
              Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
              Column('data', String(50)))

    @classmethod
    def fixtures(cls):
        return dict(
            a=(('id', 'data'),
               (1, 'a1'),
               (2, 'a2')),

            b=(('id', 'data'),
               (1, 'b1'),
               (2, 'b2'),
               (3, 'b3'),
               (4, 'b4')),

            m2m=(('id', 'aid', 'bid'),
                 (2, 1, 1),
                 (4, 2, 4),
                 (1, 1, 3),
                 (6, 2, 2),
                 (3, 1, 2),
                 (5, 2, 3)))

    def test_ordering(self):
        a, m2m, b = (self.tables.a,
                                self.tables.m2m,
                                self.tables.b)

        class A(fixtures.ComparableEntity):pass
        class B(fixtures.ComparableEntity):pass

        mapper(A, a, properties={
            'bs':relationship(B, secondary=m2m, lazy='subquery', order_by=m2m.c.id)
        })
        mapper(B, b)

        sess = create_session()
        def go():
            eq_(sess.query(A).all(), [
                        A(data='a1', bs=[B(data='b3'), B(data='b1'), B(data='b2')]),
                        A(bs=[B(data='b4'), B(data='b3'), B(data='b2')])
            ])
        self.assert_sql_count(testing.db, go, 2)


from .inheritance._poly_fixtures import _Polymorphic, Person, Engineer, \
                Paperwork, Machine, MachineType, Company

class BaseRelationFromJoinedSubclassTest(_Polymorphic):
    @classmethod
    def define_tables(cls, metadata):
        people = Table('people', metadata,
            Column('person_id', Integer,
                primary_key=True,
                test_needs_autoincrement=True),
            Column('name', String(50)),
            Column('type', String(30)))

        # to test fully, PK of engineers table must be
        # named differently from that of people
        engineers = Table('engineers', metadata,
            Column('engineer_id', Integer,
                ForeignKey('people.person_id'),
                primary_key=True),
            Column('primary_language', String(50)))

        paperwork = Table('paperwork', metadata,
            Column('paperwork_id', Integer,
                primary_key=True,
                test_needs_autoincrement=True),
            Column('description', String(50)),
            Column('person_id', Integer,
                ForeignKey('people.person_id')))

    @classmethod
    def setup_mappers(cls):
        people = cls.tables.people
        engineers = cls.tables.engineers
        paperwork = cls.tables.paperwork

        mapper(Person, people,
            polymorphic_on=people.c.type,
            polymorphic_identity='person',
            properties={
                'paperwork': relationship(
                    Paperwork, order_by=paperwork.c.paperwork_id)})

        mapper(Engineer, engineers,
            inherits=Person,
            polymorphic_identity='engineer')

        mapper(Paperwork, paperwork)

    @classmethod
    def insert_data(cls):

        e1 = Engineer(primary_language="java")
        e2 = Engineer(primary_language="c++")
        e1.paperwork = [Paperwork(description="tps report #1"),
                        Paperwork(description="tps report #2")]
        e2.paperwork = [Paperwork(description="tps report #3")]
        sess = create_session()
        sess.add_all([e1, e2])
        sess.flush()

    def test_correct_subquery_nofrom(self):
        sess = create_session()
        # use Person.paperwork here just to give the least
        # amount of context
        q = sess.query(Engineer).\
                filter(Engineer.primary_language == 'java').\
                options(subqueryload(Person.paperwork))
        def go():
            eq_(q.all()[0].paperwork,
                    [Paperwork(description="tps report #1"),
                    Paperwork(description="tps report #2")],

                )
        self.assert_sql_execution(
                testing.db,
                go,
                CompiledSQL(
                    "SELECT people.person_id AS people_person_id, "
                    "people.name AS people_name, people.type AS people_type, "
                    "engineers.engineer_id AS engineers_engineer_id, "
                    "engineers.primary_language AS engineers_primary_language "
                    "FROM people JOIN engineers ON "
                    "people.person_id = engineers.engineer_id "
                    "WHERE engineers.primary_language = :primary_language_1",
                    {"primary_language_1": "java"}
                ),
                # ensure we get "people JOIN engineer" here, even though
                # primary key "people.person_id" is against "Person"
                # *and* the path comes out as "Person.paperwork", still
                # want to select from "Engineer" entity
                CompiledSQL(
                    "SELECT paperwork.paperwork_id AS paperwork_paperwork_id, "
                    "paperwork.description AS paperwork_description, "
                    "paperwork.person_id AS paperwork_person_id, "
                    "anon_1.people_person_id AS anon_1_people_person_id "
                    "FROM (SELECT people.person_id AS people_person_id "
                        "FROM people JOIN engineers "
                        "ON people.person_id = engineers.engineer_id "
                        "WHERE engineers.primary_language = "
                            ":primary_language_1) AS anon_1 "
                    "JOIN paperwork "
                        "ON anon_1.people_person_id = paperwork.person_id "
                    "ORDER BY anon_1.people_person_id, paperwork.paperwork_id",
                    {"primary_language_1": "java"}
                )
        )

    def test_correct_subquery_existingfrom(self):
        sess = create_session()
        # use Person.paperwork here just to give the least
        # amount of context
        q = sess.query(Engineer).\
                filter(Engineer.primary_language == 'java').\
                join(Engineer.paperwork).\
                filter(Paperwork.description == "tps report #2").\
                options(subqueryload(Person.paperwork))
        def go():
            eq_(q.one().paperwork,
                    [Paperwork(description="tps report #1"),
                    Paperwork(description="tps report #2")],

                )
        self.assert_sql_execution(
            testing.db,
            go,
            CompiledSQL(
                "SELECT people.person_id AS people_person_id, "
                "people.name AS people_name, people.type AS people_type, "
                "engineers.engineer_id AS engineers_engineer_id, "
                "engineers.primary_language AS engineers_primary_language "
                "FROM people JOIN engineers "
                    "ON people.person_id = engineers.engineer_id "
                    "JOIN paperwork ON people.person_id = paperwork.person_id "
                "WHERE engineers.primary_language = :primary_language_1 "
                "AND paperwork.description = :description_1",
                {"primary_language_1": "java",
                    "description_1": "tps report #2"}
            ),
            CompiledSQL(
                "SELECT paperwork.paperwork_id AS paperwork_paperwork_id, "
                "paperwork.description AS paperwork_description, "
                "paperwork.person_id AS paperwork_person_id, "
                "anon_1.people_person_id AS anon_1_people_person_id "
                "FROM (SELECT people.person_id AS people_person_id "
                "FROM people JOIN engineers ON people.person_id = "
                "engineers.engineer_id JOIN paperwork "
                "ON people.person_id = paperwork.person_id "
                "WHERE engineers.primary_language = :primary_language_1 AND "
                "paperwork.description = :description_1) AS anon_1 "
                "JOIN paperwork ON anon_1.people_person_id = "
                "paperwork.person_id "
                "ORDER BY anon_1.people_person_id, paperwork.paperwork_id",
                {"primary_language_1": "java",
                    "description_1": "tps report #2"}
            )
        )

    def test_correct_subquery_with_polymorphic_no_alias(self):
        # test #3106
        sess = create_session()

        wp = with_polymorphic(Person, [Engineer])
        q = sess.query(wp).\
                options(subqueryload(wp.paperwork)).\
                order_by(Engineer.primary_language.desc())

        def go():
            eq_(q.first(),
                Engineer(
                    paperwork=[
                        Paperwork(description="tps report #1"),
                        Paperwork(description="tps report #2")],
                    primary_language='java'
                )

            )
        self.assert_sql_execution(
            testing.db,
            go,
            CompiledSQL(
                "SELECT people.person_id AS people_person_id, "
                "people.name AS people_name, people.type AS people_type, "
                "engineers.engineer_id AS engineers_engineer_id, "
                "engineers.primary_language AS engineers_primary_language "
                "FROM people LEFT OUTER JOIN engineers ON people.person_id = "
                "engineers.engineer_id ORDER BY engineers.primary_language "
                "DESC LIMIT :param_1"),
            CompiledSQL(
                "SELECT paperwork.paperwork_id AS paperwork_paperwork_id, "
                "paperwork.description AS paperwork_description, "
                "paperwork.person_id AS paperwork_person_id, "
                "anon_1.people_person_id AS anon_1_people_person_id FROM "
                "(SELECT people.person_id AS people_person_id FROM people "
                "LEFT OUTER JOIN engineers ON people.person_id = "
                "engineers.engineer_id ORDER BY engineers.primary_language "
                "DESC LIMIT :param_1) AS anon_1 JOIN paperwork "
                "ON anon_1.people_person_id = paperwork.person_id "
                "ORDER BY anon_1.people_person_id, paperwork.paperwork_id")
        )

    def test_correct_subquery_with_polymorphic_alias(self):
        # test #3106
        sess = create_session()

        wp = with_polymorphic(Person, [Engineer], aliased=True)
        q = sess.query(wp).\
                options(subqueryload(wp.paperwork)).\
                order_by(wp.Engineer.primary_language.desc())

        def go():
            eq_(q.first(),
                Engineer(
                    paperwork=[
                        Paperwork(description="tps report #1"),
                        Paperwork(description="tps report #2")],
                    primary_language='java'
                )

            )
        self.assert_sql_execution(
            testing.db,
            go,
            CompiledSQL(
                "SELECT anon_1.people_person_id AS anon_1_people_person_id, "
                "anon_1.people_name AS anon_1_people_name, "
                "anon_1.people_type AS anon_1_people_type, "
                "anon_1.engineers_engineer_id AS anon_1_engineers_engineer_id, "
                "anon_1.engineers_primary_language "
                "AS anon_1_engineers_primary_language FROM "
                "(SELECT people.person_id AS people_person_id, "
                "people.name AS people_name, people.type AS people_type, "
                "engineers.engineer_id AS engineers_engineer_id, "
                "engineers.primary_language AS engineers_primary_language "
                "FROM people LEFT OUTER JOIN engineers ON people.person_id = "
                "engineers.engineer_id) AS anon_1 "
                "ORDER BY anon_1.engineers_primary_language DESC "
                "LIMIT :param_1"),
            CompiledSQL(
                "SELECT paperwork.paperwork_id AS paperwork_paperwork_id, "
                "paperwork.description AS paperwork_description, "
                "paperwork.person_id AS paperwork_person_id, "
                "anon_1.anon_2_people_person_id AS "
                "anon_1_anon_2_people_person_id FROM "
                "(SELECT DISTINCT anon_2.people_person_id AS "
                "anon_2_people_person_id, "
                "anon_2.engineers_primary_language AS "
                "anon_2_engineers_primary_language FROM "
                "(SELECT people.person_id AS people_person_id, "
                "people.name AS people_name, people.type AS people_type, "
                "engineers.engineer_id AS engineers_engineer_id, "
                "engineers.primary_language AS engineers_primary_language "
                "FROM people LEFT OUTER JOIN engineers ON people.person_id = "
                "engineers.engineer_id) AS anon_2 "
                "ORDER BY anon_2.engineers_primary_language "
                "DESC LIMIT :param_1) AS anon_1 "
                "JOIN paperwork "
                "ON anon_1.anon_2_people_person_id = paperwork.person_id "
                "ORDER BY anon_1.anon_2_people_person_id, "
                "paperwork.paperwork_id")
        )

    def test_correct_subquery_with_polymorphic_flat_alias(self):
        # test #3106
        sess = create_session()

        wp = with_polymorphic(Person, [Engineer], aliased=True, flat=True)
        q = sess.query(wp).\
                options(subqueryload(wp.paperwork)).\
                order_by(wp.Engineer.primary_language.desc())

        def go():
            eq_(q.first(),
                Engineer(
                    paperwork=[
                        Paperwork(description="tps report #1"),
                        Paperwork(description="tps report #2")],
                    primary_language='java'
                )

            )
        self.assert_sql_execution(
            testing.db,
            go,
            CompiledSQL(
                "SELECT people_1.person_id AS people_1_person_id, "
                "people_1.name AS people_1_name, "
                "people_1.type AS people_1_type, "
                "engineers_1.engineer_id AS engineers_1_engineer_id, "
                "engineers_1.primary_language AS engineers_1_primary_language "
                "FROM people AS people_1 "
                "LEFT OUTER JOIN engineers AS engineers_1 "
                "ON people_1.person_id = engineers_1.engineer_id "
                "ORDER BY engineers_1.primary_language DESC LIMIT :param_1"),
            CompiledSQL(
                "SELECT paperwork.paperwork_id AS paperwork_paperwork_id, "
                "paperwork.description AS paperwork_description, "
                "paperwork.person_id AS paperwork_person_id, "
                "anon_1.people_1_person_id AS anon_1_people_1_person_id "
                "FROM (SELECT people_1.person_id AS people_1_person_id "
                "FROM people AS people_1 "
                "LEFT OUTER JOIN engineers AS engineers_1 "
                "ON people_1.person_id = engineers_1.engineer_id "
                "ORDER BY engineers_1.primary_language DESC LIMIT :param_1) "
                "AS anon_1 JOIN paperwork ON anon_1.people_1_person_id = "
                "paperwork.person_id ORDER BY anon_1.people_1_person_id, "
                "paperwork.paperwork_id"
            )
        )

class SubRelationFromJoinedSubclassMultiLevelTest(_Polymorphic):
    @classmethod
    def define_tables(cls, metadata):
        Table('companies', metadata,
            Column('company_id', Integer,
                primary_key=True,
                test_needs_autoincrement=True),
            Column('name', String(50)))

        Table('people', metadata,
            Column('person_id', Integer,
                primary_key=True,
                test_needs_autoincrement=True),
            Column('company_id', ForeignKey('companies.company_id')),
            Column('name', String(50)),
            Column('type', String(30)))

        Table('engineers', metadata,
            Column('engineer_id', ForeignKey('people.person_id'),
                            primary_key=True),
            Column('primary_language', String(50)))

        Table('machines', metadata,
            Column('machine_id',
                 Integer, primary_key=True,
                 test_needs_autoincrement=True),
            Column('name', String(50)),
            Column('engineer_id', ForeignKey('engineers.engineer_id')),
            Column('machine_type_id',
                            ForeignKey('machine_type.machine_type_id')))

        Table('machine_type', metadata,
            Column('machine_type_id',
                    Integer, primary_key=True,
                        test_needs_autoincrement=True),
            Column('name', String(50)))

    @classmethod
    def setup_mappers(cls):
        companies = cls.tables.companies
        people = cls.tables.people
        engineers = cls.tables.engineers
        machines = cls.tables.machines
        machine_type = cls.tables.machine_type

        mapper(Company, companies, properties={
                'employees': relationship(Person, order_by=people.c.person_id)
                })
        mapper(Person, people,
            polymorphic_on=people.c.type,
            polymorphic_identity='person',
            with_polymorphic='*')

        mapper(Engineer, engineers,
            inherits=Person,
            polymorphic_identity='engineer', properties={
                'machines': relationship(Machine,
                                        order_by=machines.c.machine_id)
            })

        mapper(Machine, machines, properties={
                'type': relationship(MachineType)
                })
        mapper(MachineType, machine_type)


    @classmethod
    def insert_data(cls):
        c1 = cls._fixture()
        sess = create_session()
        sess.add(c1)
        sess.flush()

    @classmethod
    def _fixture(cls):
        mt1 = MachineType(name='mt1')
        mt2 = MachineType(name='mt2')
        return Company(
                employees=[
                    Engineer(
                        name='e1',
                        machines=[
                            Machine(name='m1', type=mt1),
                            Machine(name='m2', type=mt2)
                        ]
                    ),
                    Engineer(
                        name='e2',
                        machines=[
                            Machine(name='m3', type=mt1),
                            Machine(name='m4', type=mt1)
                        ]
                    )
                ])

    def test_chained_subq_subclass(self):
        s = Session()
        q = s.query(Company).options(
                        subqueryload(Company.employees.of_type(Engineer)).
                            subqueryload(Engineer.machines).
                            subqueryload(Machine.type)
                    )

        def go():
            eq_(
                q.all(),
                [self._fixture()]
            )
        self.assert_sql_count(testing.db, go, 4)


class SelfReferentialTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table('nodes', metadata,
              Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('parent_id', Integer, ForeignKey('nodes.id')),
            Column('data', String(30)))

    def test_basic(self):
        nodes = self.tables.nodes

        class Node(fixtures.ComparableEntity):
            def append(self, node):
                self.children.append(node)

        mapper(Node, nodes, properties={
            'children':relationship(Node,
                                        lazy='subquery',
                                        join_depth=3, order_by=nodes.c.id)
        })
        sess = create_session()
        n1 = Node(data='n1')
        n1.append(Node(data='n11'))
        n1.append(Node(data='n12'))
        n1.append(Node(data='n13'))
        n1.children[1].append(Node(data='n121'))
        n1.children[1].append(Node(data='n122'))
        n1.children[1].append(Node(data='n123'))
        n2 = Node(data='n2')
        n2.append(Node(data='n21'))
        n2.children[0].append(Node(data='n211'))
        n2.children[0].append(Node(data='n212'))

        sess.add(n1)
        sess.add(n2)
        sess.flush()
        sess.expunge_all()
        def go():
            d = sess.query(Node).filter(Node.data.in_(['n1', 'n2'])).\
                            order_by(Node.data).all()
            eq_([Node(data='n1', children=[
                    Node(data='n11'),
                    Node(data='n12', children=[
                        Node(data='n121'),
                        Node(data='n122'),
                        Node(data='n123')
                    ]),
                    Node(data='n13')
                ]),
                Node(data='n2', children=[
                    Node(data='n21', children=[
                        Node(data='n211'),
                        Node(data='n212'),
                    ])
                ])
            ], d)
        self.assert_sql_count(testing.db, go, 4)



    def test_lazy_fallback_doesnt_affect_eager(self):
        nodes = self.tables.nodes

        class Node(fixtures.ComparableEntity):
            def append(self, node):
                self.children.append(node)

        mapper(Node, nodes, properties={
            'children':relationship(Node, lazy='subquery', join_depth=1,
                                    order_by=nodes.c.id)
        })
        sess = create_session()
        n1 = Node(data='n1')
        n1.append(Node(data='n11'))
        n1.append(Node(data='n12'))
        n1.append(Node(data='n13'))
        n1.children[1].append(Node(data='n121'))
        n1.children[1].append(Node(data='n122'))
        n1.children[1].append(Node(data='n123'))
        sess.add(n1)
        sess.flush()
        sess.expunge_all()

        def go():
            allnodes = sess.query(Node).order_by(Node.data).all()
            n12 = allnodes[2]
            eq_(n12.data, 'n12')
            eq_([
                Node(data='n121'),
                Node(data='n122'),
                Node(data='n123')
            ], list(n12.children))
        self.assert_sql_count(testing.db, go, 4)

    def test_with_deferred(self):
        nodes = self.tables.nodes

        class Node(fixtures.ComparableEntity):
            def append(self, node):
                self.children.append(node)

        mapper(Node, nodes, properties={
            'children':relationship(Node, lazy='subquery', join_depth=3,
                                    order_by=nodes.c.id),
            'data':deferred(nodes.c.data)
        })
        sess = create_session()
        n1 = Node(data='n1')
        n1.append(Node(data='n11'))
        n1.append(Node(data='n12'))
        sess.add(n1)
        sess.flush()
        sess.expunge_all()

        def go():
            eq_(
                Node(data='n1', children=[Node(data='n11'), Node(data='n12')]),
                sess.query(Node).order_by(Node.id).first(),
                )
        self.assert_sql_count(testing.db, go, 6)

        sess.expunge_all()

        def go():
            eq_(Node(data='n1', children=[Node(data='n11'), Node(data='n12')]),
                sess.query(Node).options(undefer('data')).order_by(Node.id).first())
        self.assert_sql_count(testing.db, go, 5)

        sess.expunge_all()

        def go():
            eq_(Node(data='n1', children=[Node(data='n11'), Node(data='n12')]),
                sess.query(Node).options(undefer('data'),
                                            undefer('children.data')).first())
        self.assert_sql_count(testing.db, go, 3)


    def test_options(self):
        nodes = self.tables.nodes

        class Node(fixtures.ComparableEntity):
            def append(self, node):
                self.children.append(node)

        mapper(Node, nodes, properties={
            'children':relationship(Node, order_by=nodes.c.id)
        }, order_by=nodes.c.id)
        sess = create_session()
        n1 = Node(data='n1')
        n1.append(Node(data='n11'))
        n1.append(Node(data='n12'))
        n1.append(Node(data='n13'))
        n1.children[1].append(Node(data='n121'))
        n1.children[1].append(Node(data='n122'))
        n1.children[1].append(Node(data='n123'))
        sess.add(n1)
        sess.flush()
        sess.expunge_all()
        def go():
            d = sess.query(Node).filter_by(data='n1').\
                        options(subqueryload_all('children.children')).first()
            eq_(Node(data='n1', children=[
                Node(data='n11'),
                Node(data='n12', children=[
                    Node(data='n121'),
                    Node(data='n122'),
                    Node(data='n123')
                ]),
                Node(data='n13')
            ]), d)
        self.assert_sql_count(testing.db, go, 3)

    def test_no_depth(self):
        """no join depth is set, so no eager loading occurs."""

        nodes = self.tables.nodes

        class Node(fixtures.ComparableEntity):
            def append(self, node):
                self.children.append(node)

        mapper(Node, nodes, properties={
            'children':relationship(Node, lazy='subquery')
        })
        sess = create_session()
        n1 = Node(data='n1')
        n1.append(Node(data='n11'))
        n1.append(Node(data='n12'))
        n1.append(Node(data='n13'))
        n1.children[1].append(Node(data='n121'))
        n1.children[1].append(Node(data='n122'))
        n1.children[1].append(Node(data='n123'))
        n2 = Node(data='n2')
        n2.append(Node(data='n21'))
        sess.add(n1)
        sess.add(n2)
        sess.flush()
        sess.expunge_all()
        def go():
            d = sess.query(Node).filter(Node.data.in_(['n1', 'n2'])).order_by(Node.data).all()
            eq_([
                Node(data='n1', children=[
                    Node(data='n11'),
                    Node(data='n12', children=[
                        Node(data='n121'),
                        Node(data='n122'),
                        Node(data='n123')
                    ]),
                    Node(data='n13')
                ]),
                Node(data='n2', children=[
                    Node(data='n21')
                ])
            ], d)
        self.assert_sql_count(testing.db, go, 4)

class InheritanceToRelatedTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table('foo', metadata,
            Column("id", Integer, primary_key=True),
            Column("type", String(50)),
            Column("related_id", Integer, ForeignKey("related.id"))
        )
        Table("bar", metadata,
            Column("id", Integer, ForeignKey('foo.id'), primary_key=True),
        )
        Table("baz", metadata,
            Column("id", Integer, ForeignKey('foo.id'), primary_key=True),
        )
        Table("related", metadata,
            Column("id", Integer, primary_key=True),
        )

    @classmethod
    def setup_classes(cls):
        class Foo(cls.Comparable):
            pass
        class Bar(Foo):
            pass
        class Baz(Foo):
            pass
        class Related(cls.Comparable):
            pass

    @classmethod
    def fixtures(cls):
        return dict(
            foo=[
                ('id', 'type', 'related_id'),
                (1, 'bar', 1),
                (2, 'bar', 2),
                (3, 'baz', 1),
                (4, 'baz', 2),
            ],
            bar=[
                ('id', ),
                (1,),
                (2,)
            ],
            baz=[
                ('id', ),
                (3,),
                (4,)
            ],
            related=[
                ('id', ),
                (1,),
                (2,)
            ]
        )
    @classmethod
    def setup_mappers(cls):
        mapper(cls.classes.Foo, cls.tables.foo, properties={
            'related': relationship(cls.classes.Related)
        }, polymorphic_on=cls.tables.foo.c.type)
        mapper(cls.classes.Bar, cls.tables.bar, polymorphic_identity='bar',
                    inherits=cls.classes.Foo)
        mapper(cls.classes.Baz, cls.tables.baz, polymorphic_identity='baz',
                    inherits=cls.classes.Foo)
        mapper(cls.classes.Related, cls.tables.related)

    def test_caches_query_per_base_subq(self):
        Foo, Bar, Baz, Related = self.classes.Foo, self.classes.Bar, \
                        self.classes.Baz, self.classes.Related
        s = Session(testing.db)
        def go():
            eq_(
                s.query(Foo).with_polymorphic([Bar, Baz]).\
                            order_by(Foo.id).\
                            options(subqueryload(Foo.related)).all(),
                [
                    Bar(id=1, related=Related(id=1)),
                    Bar(id=2, related=Related(id=2)),
                    Baz(id=3, related=Related(id=1)),
                    Baz(id=4, related=Related(id=2))
                ]
            )
        self.assert_sql_count(testing.db, go, 2)

    def test_caches_query_per_base_joined(self):
        # technically this should be in test_eager_relations
        Foo, Bar, Baz, Related = self.classes.Foo, self.classes.Bar, \
                        self.classes.Baz, self.classes.Related
        s = Session(testing.db)
        def go():
            eq_(
                s.query(Foo).with_polymorphic([Bar, Baz]).\
                            order_by(Foo.id).\
                            options(joinedload(Foo.related)).all(),
                [
                    Bar(id=1, related=Related(id=1)),
                    Bar(id=2, related=Related(id=2)),
                    Baz(id=3, related=Related(id=1)),
                    Baz(id=4, related=Related(id=2))
                ]
            )
        self.assert_sql_count(testing.db, go, 1)

class CyclicalInheritingEagerTestOne(fixtures.MappedTest):

    @classmethod
    def define_tables(cls, metadata):
        Table('t1', metadata,
            Column('c1', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('c2', String(30)),
            Column('type', String(30))
            )

        Table('t2', metadata,
            Column('c1', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('c2', String(30)),
            Column('type', String(30)),
            Column('t1.id', Integer, ForeignKey('t1.c1')))

    def test_basic(self):
        t2, t1 = self.tables.t2, self.tables.t1

        class T(object):
            pass

        class SubT(T):
            pass

        class T2(object):
            pass

        class SubT2(T2):
            pass

        mapper(T, t1, polymorphic_on=t1.c.type, polymorphic_identity='t1')
        mapper(SubT, None, inherits=T, polymorphic_identity='subt1', properties={
            't2s': relationship(SubT2, lazy='subquery',
                    backref=sa.orm.backref('subt', lazy='subquery'))
        })
        mapper(T2, t2, polymorphic_on=t2.c.type, polymorphic_identity='t2')
        mapper(SubT2, None, inherits=T2, polymorphic_identity='subt2')

        # testing a particular endless loop condition in eager load setup
        create_session().query(SubT).all()

class CyclicalInheritingEagerTestTwo(fixtures.DeclarativeMappedTest,
                        testing.AssertsCompiledSQL):
    __dialect__ = 'default'

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic
        class PersistentObject(Base):
            __tablename__ = 'persistent'
            id = Column(Integer, primary_key=True, test_needs_autoincrement=True)

        class Movie(PersistentObject):
            __tablename__ = 'movie'
            id = Column(Integer, ForeignKey('persistent.id'), primary_key=True)
            director_id = Column(Integer, ForeignKey('director.id'))
            title = Column(String(50))

        class Director(PersistentObject):
            __tablename__ = 'director'
            id = Column(Integer, ForeignKey('persistent.id'), primary_key=True)
            movies = relationship("Movie", foreign_keys=Movie.director_id)
            name = Column(String(50))


    def test_from_subclass(self):
        Director = self.classes.Director

        s = create_session()

        ctx = s.query(Director).options(subqueryload('*'))._compile_context()

        q = ctx.attributes[('subquery',
                        (inspect(Director), inspect(Director).attrs.movies))]
        self.assert_compile(q,
            "SELECT movie.id AS movie_id, persistent.id AS persistent_id, "
            "movie.director_id AS movie_director_id, "
            "movie.title AS movie_title, "
            "anon_1.director_id AS anon_1_director_id "
            "FROM (SELECT director.id AS director_id "
                "FROM persistent JOIN director "
                "ON persistent.id = director.id) AS anon_1 "
            "JOIN (persistent JOIN movie ON persistent.id = movie.id) "
            "ON anon_1.director_id = movie.director_id "
            "ORDER BY anon_1.director_id",
            dialect="default"
        )

    def test_integrate(self):
        Director = self.classes.Director
        Movie = self.classes.Movie

        session = Session(testing.db)
        rscott = Director(name="Ridley Scott")
        alien = Movie(title="Alien")
        brunner = Movie(title="Blade Runner")
        rscott.movies.append(brunner)
        rscott.movies.append(alien)
        session.add_all([rscott, alien, brunner])
        session.commit()

        session.close_all()
        d = session.query(Director).options(subqueryload('*')).first()
        assert len(list(session)) == 3


class SubqueryloadDistinctTest(fixtures.DeclarativeMappedTest,
                               testing.AssertsCompiledSQL):
    __dialect__ = 'default'

    run_inserts = 'once'
    run_deletes = None

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class Director(Base):
            __tablename__ = 'director'
            id = Column(Integer, primary_key=True,
                                    test_needs_autoincrement=True)
            name = Column(String(50))

        class DirectorPhoto(Base):
            __tablename__ = 'director_photo'
            id = Column(Integer, primary_key=True,
                                    test_needs_autoincrement=True)
            path = Column(String(255))
            director_id = Column(Integer, ForeignKey('director.id'))
            director = relationship(Director, backref="photos")

        class Movie(Base):
            __tablename__ = 'movie'
            id = Column(Integer, primary_key=True, test_needs_autoincrement=True)
            director_id = Column(Integer, ForeignKey('director.id'))
            director = relationship(Director, backref="movies")
            title = Column(String(50))
            credits = relationship("Credit", backref="movie")

        class Credit(Base):
            __tablename__ = 'credit'
            id = Column(Integer, primary_key=True, test_needs_autoincrement=True)
            movie_id = Column(Integer, ForeignKey('movie.id'))

    @classmethod
    def insert_data(cls):
        Movie = cls.classes.Movie
        Director = cls.classes.Director
        DirectorPhoto = cls.classes.DirectorPhoto
        Credit = cls.classes.Credit

        d = Director(name='Woody Allen')
        d.photos = [DirectorPhoto(path='/1.jpg'),
                    DirectorPhoto(path='/2.jpg')]
        d.movies = [Movie(title='Manhattan', credits=[Credit(), Credit()]),
                    Movie(title='Sweet and Lowdown', credits=[Credit()])]
        sess = create_session()
        sess.add_all([d])
        sess.flush()

    def test_distinct_strategy_opt_m2o(self):
        self._run_test_m2o(True, None)
        self._run_test_m2o(False, None)

    def test_distinct_unrelated_opt_m2o(self):
        self._run_test_m2o(None, True)
        self._run_test_m2o(None, False)

    def _run_test_m2o(self,
            director_strategy_level,
            photo_strategy_level):

        # test where the innermost is m2o, e.g.
        # Movie->director

        Movie = self.classes.Movie
        Director = self.classes.Director

        Movie.director.property.distinct_target_key = director_strategy_level
        Director.photos.property.distinct_target_key = photo_strategy_level

        # the DISTINCT is controlled by
        # only the Movie->director relationship, *not* the
        # Director.photos
        expect_distinct = director_strategy_level in (True, None)

        s = create_session()

        q = (
            s.query(Movie)
            .options(
                subqueryload(Movie.director)
                .subqueryload(Director.photos)
            )
        )
        ctx = q._compile_context()

        q2 = ctx.attributes[
            ('subquery', (inspect(Movie), inspect(Movie).attrs.director))
        ]
        self.assert_compile(
            q2,
            'SELECT director.id AS director_id, '
            'director.name AS director_name, '
            'anon_1.movie_director_id AS anon_1_movie_director_id '
            'FROM (SELECT%s movie.director_id AS movie_director_id '
            'FROM movie) AS anon_1 '
            'JOIN director ON director.id = anon_1.movie_director_id '
            'ORDER BY anon_1.movie_director_id' % (
                    " DISTINCT" if expect_distinct else "")
        )

        ctx2 = q2._compile_context()
        result = s.execute(q2)
        rows = result.fetchall()

        if expect_distinct:
            eq_(rows, [
                (1, 'Woody Allen', 1),
            ])
        else:
            eq_(rows, [
                (1, 'Woody Allen', 1), (1, 'Woody Allen', 1),
            ])

        q3 = ctx2.attributes[
            ('subquery', (inspect(Director), inspect(Director).attrs.photos))
        ]

        self.assert_compile(
            q3,
            'SELECT director_photo.id AS director_photo_id, '
            'director_photo.path AS director_photo_path, '
            'director_photo.director_id AS director_photo_director_id, '
            'director_1.id AS director_1_id '
            'FROM (SELECT%s movie.director_id AS movie_director_id '
            'FROM movie) AS anon_1 '
            'JOIN director AS director_1 ON director_1.id = anon_1.movie_director_id '
            'JOIN director_photo ON director_1.id = director_photo.director_id '
            'ORDER BY director_1.id' % (
                    " DISTINCT" if expect_distinct else "")
        )
        result = s.execute(q3)
        rows = result.fetchall()
        if expect_distinct:
            eq_(set(tuple(t) for t in rows), set([
                (1, '/1.jpg', 1, 1),
                (2, '/2.jpg', 1, 1),
            ]))
        else:
            # oracle might not order the way we expect here
            eq_(set(tuple(t) for t in rows), set([
                (1, '/1.jpg', 1, 1),
                (2, '/2.jpg', 1, 1),
                (1, '/1.jpg', 1, 1),
                (2, '/2.jpg', 1, 1),
            ]))


        movies = q.all()

        # check number of persistent objects in session
        eq_(len(list(s)), 5)

    def test_cant_do_distinct_in_joins(self):
        """the DISTINCT feature here works when the m2o is in the innermost
        mapper, but when we are just joining along relationships outside
        of that, we can still have dupes, and there's no solution to that.

        """
        Movie = self.classes.Movie
        Credit = self.classes.Credit

        s = create_session()

        q = (
            s.query(Credit)
            .options(
                subqueryload(Credit.movie)
                .subqueryload(Movie.director)
            )
        )

        ctx = q._compile_context()

        q2 = ctx.attributes[
            ('subquery', (inspect(Credit), Credit.movie.property))
        ]
        ctx2 = q2._compile_context()
        q3 = ctx2.attributes[
            ('subquery', (inspect(Movie), Movie.director.property))
        ]

        result = s.execute(q3)
        eq_(
            result.fetchall(),
            [
                (1, 'Woody Allen', 1), (1, 'Woody Allen', 1),
            ]
        )


class JoinedNoLoadConflictTest(fixtures.DeclarativeMappedTest):
    """test for [ticket:2887]"""

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class Parent(ComparableEntity, Base):
            __tablename__ = 'parent'

            id = Column(Integer, primary_key=True, test_needs_autoincrement=True)
            name = Column(String(20))

            children = relationship('Child',
                                back_populates='parent',
                                lazy='noload'
                            )

        class Child(ComparableEntity, Base):
            __tablename__ = 'child'

            id = Column(Integer, primary_key=True, test_needs_autoincrement=True)
            name = Column(String(20))
            parent_id = Column(Integer, ForeignKey('parent.id'))

            parent = relationship('Parent', back_populates='children', lazy='joined')

    @classmethod
    def insert_data(cls):
        Parent = cls.classes.Parent
        Child = cls.classes.Child

        s = Session()
        s.add(Parent(name='parent', children=[Child(name='c1')]))
        s.commit()

    def test_subqueryload_on_joined_noload(self):
        Parent = self.classes.Parent
        Child = self.classes.Child

        s = Session()

        # here we have Parent->subqueryload->Child->joinedload->parent->noload->children.
        # the actual subqueryload has to emit *after* we've started populating
        # Parent->subqueryload->child.
        parent = s.query(Parent).options([subqueryload('children')]).first()
        eq_(
            parent.children,
            [Child(name='c1')]
        )

