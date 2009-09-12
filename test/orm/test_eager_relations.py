"""basic tests of eager loaded attributes"""

from sqlalchemy.test.testing import eq_
import sqlalchemy as sa
from sqlalchemy.test import testing
from sqlalchemy.orm import eagerload, deferred, undefer
from sqlalchemy import Integer, String, Date, ForeignKey, and_, select, func
from sqlalchemy.test.schema import Table
from sqlalchemy.test.schema import Column
from sqlalchemy.orm import mapper, relation, create_session, lazyload, aliased
from sqlalchemy.test.testing import eq_
from sqlalchemy.test.assertsql import CompiledSQL
from test.orm import _base, _fixtures
import datetime

class EagerTest(_fixtures.FixtureTest):
    run_inserts = 'once'
    run_deletes = None

    @testing.resolve_artifact_names
    def test_basic(self):
        mapper(User, users, properties={
            'addresses':relation(mapper(Address, addresses), lazy=False, order_by=Address.id)
        })
        sess = create_session()
        q = sess.query(User)

        assert [User(id=7, addresses=[Address(id=1, email_address='jack@bean.com')])] == q.filter(User.id==7).all()
        eq_(self.static.user_address_result, q.order_by(User.id).all())

    @testing.resolve_artifact_names
    def test_late_compile(self):
        m = mapper(User, users)
        sess = create_session()
        sess.query(User).all()
        m.add_property("addresses", relation(mapper(Address, addresses)))
        
        sess.expunge_all()
        def go():
            eq_(
               [User(id=7, addresses=[Address(id=1, email_address='jack@bean.com')])],
               sess.query(User).options(eagerload('addresses')).filter(User.id==7).all()
            )
        self.assert_sql_count(testing.db, go, 1)
            
        
    @testing.resolve_artifact_names
    def test_no_orphan(self):
        """An eagerly loaded child object is not marked as an orphan"""
        mapper(User, users, properties={
            'addresses':relation(Address, cascade="all,delete-orphan", lazy=False)
        })
        mapper(Address, addresses)

        sess = create_session()
        user = sess.query(User).get(7)
        assert getattr(User, 'addresses').hasparent(sa.orm.attributes.instance_state(user.addresses[0]), optimistic=True)
        assert not sa.orm.class_mapper(Address)._is_orphan(sa.orm.attributes.instance_state(user.addresses[0]))

    @testing.resolve_artifact_names
    def test_orderby(self):
        mapper(User, users, properties = {
            'addresses':relation(mapper(Address, addresses), lazy=False, order_by=addresses.c.email_address),
        })
        q = create_session().query(User)
        assert [
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
        ] == q.order_by(User.id).all()

    @testing.resolve_artifact_names
    def test_orderby_multi(self):
        mapper(User, users, properties = {
            'addresses':relation(mapper(Address, addresses), lazy=False, order_by=[addresses.c.email_address, addresses.c.id]),
        })
        q = create_session().query(User)
        assert [
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
        ] == q.order_by(User.id).all()

    @testing.resolve_artifact_names
    def test_orderby_related(self):
        """A regular mapper select on a single table can order by a relation to a second table"""
        mapper(Address, addresses)
        mapper(User, users, properties = dict(
            addresses = relation(Address, lazy=False, order_by=addresses.c.id),
        ))

        q = create_session().query(User)
        l = q.filter(User.id==Address.user_id).order_by(Address.email_address).all()

        assert [
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
        ] == l

    @testing.resolve_artifact_names
    def test_orderby_desc(self):
        mapper(Address, addresses)
        mapper(User, users, properties = dict(
            addresses = relation(Address, lazy=False,
                                 order_by=[sa.desc(addresses.c.email_address)]),
        ))
        sess = create_session()
        assert [
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
        ] == sess.query(User).order_by(User.id).all()

    @testing.resolve_artifact_names
    def test_deferred_fk_col(self):
        User, Address, Dingaling = self.classes.get_all(
            'User', 'Address', 'Dingaling')
        users, addresses, dingalings = self.tables.get_all(
            'users', 'addresses', 'dingalings')

        mapper(Address, addresses, properties={
            'user_id':deferred(addresses.c.user_id),
            'user':relation(User, lazy=False)
        })
        mapper(User, users)

        sess = create_session()

        for q in [
            sess.query(Address).filter(Address.id.in_([1, 4, 5])),
            sess.query(Address).filter(Address.id.in_([1, 4, 5])).limit(3)
        ]:
            sess.expunge_all()
            eq_(q.all(),
                [Address(id=1, user=User(id=7)),
                 Address(id=4, user=User(id=8)),
                 Address(id=5, user=User(id=9))]
            )

        a = sess.query(Address).filter(Address.id==1).first()
        def go():
            eq_(a.user_id, 7)
        # assert that the eager loader added 'user_id' to the row and deferred
        # loading of that col was disabled
        self.assert_sql_count(testing.db, go, 0)

        # do the mapping in reverse
        # (we would have just used an "addresses" backref but the test
        # fixtures then require the whole backref to be set up, lazy loaders
        # trigger, etc.)
        sa.orm.clear_mappers()

        mapper(Address, addresses, properties={
            'user_id':deferred(addresses.c.user_id),
        })
        mapper(User, users, properties={
            'addresses':relation(Address, lazy=False)})

        for q in [
            sess.query(User).filter(User.id==7),
            sess.query(User).filter(User.id==7).limit(1)
        ]:
            sess.expunge_all()
            eq_(q.all(),
                [User(id=7, addresses=[Address(id=1)])]
            )

        sess.expunge_all()
        u = sess.query(User).get(7)
        def go():
            assert u.addresses[0].user_id==7
        # assert that the eager loader didn't have to affect 'user_id' here
        # and that its still deferred
        self.assert_sql_count(testing.db, go, 1)

        sa.orm.clear_mappers()

        mapper(User, users, properties={
            'addresses':relation(Address, lazy=False)})
        mapper(Address, addresses, properties={
            'user_id':deferred(addresses.c.user_id),
            'dingalings':relation(Dingaling, lazy=False)})
        mapper(Dingaling, dingalings, properties={
            'address_id':deferred(dingalings.c.address_id)})
        sess.expunge_all()
        def go():
            u = sess.query(User).get(8)
            eq_(User(id=8,
                     addresses=[Address(id=2, dingalings=[Dingaling(id=1)]),
                                Address(id=3),
                                Address(id=4)]),
                u)
        self.assert_sql_count(testing.db, go, 1)

    @testing.resolve_artifact_names
    def test_many_to_many(self):
        Keyword, Item = self.Keyword, self.Item
        keywords, item_keywords, items = self.tables.get_all(
            'keywords', 'item_keywords', 'items')

        mapper(Keyword, keywords)
        mapper(Item, items, properties = dict(
                keywords = relation(Keyword, secondary=item_keywords,
                                    lazy=False, order_by=keywords.c.id)))

        q = create_session().query(Item).order_by(Item.id)
        def go():
            assert self.static.item_keyword_result == q.all()
        self.assert_sql_count(testing.db, go, 1)

        def go():
            eq_(self.static.item_keyword_result[0:2],
                q.join('keywords').filter(Keyword.name == 'red').all())
        self.assert_sql_count(testing.db, go, 1)

        def go():
            eq_(self.static.item_keyword_result[0:2],
                (q.join('keywords', aliased=True).
                 filter(Keyword.name == 'red')).all())
        self.assert_sql_count(testing.db, go, 1)

    @testing.resolve_artifact_names
    def test_eager_option(self):
        Keyword, Item = self.Keyword, self.Item
        keywords, item_keywords, items = self.tables.get_all(
            'keywords', 'item_keywords', 'items')

        mapper(Keyword, keywords)
        mapper(Item, items, properties = dict(
                keywords = relation(Keyword, secondary=item_keywords, lazy=True,
                                    order_by=keywords.c.id)))

        q = create_session().query(Item)

        def go():
            eq_(self.static.item_keyword_result[0:2],
                (q.options(eagerload('keywords')).
                 join('keywords').filter(keywords.c.name == 'red')).order_by(Item.id).all())

        self.assert_sql_count(testing.db, go, 1)

    @testing.resolve_artifact_names
    def test_cyclical(self):
        """A circular eager relationship breaks the cycle with a lazy loader"""
        User, Address = self.User, self.Address
        users, addresses = self.tables.get_all('users', 'addresses')

        mapper(Address, addresses)
        mapper(User, users, properties = dict(
            addresses = relation(Address, lazy=False,
                                 backref=sa.orm.backref('user', lazy=False), order_by=Address.id)
        ))
        assert sa.orm.class_mapper(User).get_property('addresses').lazy is False
        assert sa.orm.class_mapper(Address).get_property('user').lazy is False

        sess = create_session()
        eq_(self.static.user_address_result, sess.query(User).order_by(User.id).all())

    @testing.resolve_artifact_names
    def test_double(self):
        """Eager loading with two relations simultaneously, from the same table, using aliases."""
        User, Address, Order = self.classes.get_all(
            'User', 'Address', 'Order')
        users, addresses, orders = self.tables.get_all(
            'users', 'addresses', 'orders')

        openorders = sa.alias(orders, 'openorders')
        closedorders = sa.alias(orders, 'closedorders')

        mapper(Address, addresses)
        mapper(Order, orders)
        
        open_mapper = mapper(Order, openorders, non_primary=True)
        closed_mapper = mapper(Order, closedorders, non_primary=True)
        
        mapper(User, users, properties = dict(
            addresses = relation(Address, lazy=False, order_by=addresses.c.id),
            open_orders = relation(
                open_mapper,
                primaryjoin=sa.and_(openorders.c.isopen == 1,
                                 users.c.id==openorders.c.user_id),
                lazy=False, order_by=openorders.c.id),
            closed_orders = relation(
                closed_mapper,
                primaryjoin=sa.and_(closedorders.c.isopen == 0,
                                 users.c.id==closedorders.c.user_id),
                lazy=False, order_by=closedorders.c.id)))

        q = create_session().query(User).order_by(User.id)

        def go():
            assert [
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

            ] == q.all()
        self.assert_sql_count(testing.db, go, 1)

    @testing.resolve_artifact_names
    def test_double_same_mappers(self):
        """Eager loading with two relations simulatneously, from the same table, using aliases."""
        User, Address, Order = self.classes.get_all(
            'User', 'Address', 'Order')
        users, addresses, orders = self.tables.get_all(
            'users', 'addresses', 'orders')

        mapper(Address, addresses)
        mapper(Order, orders, properties={
            'items': relation(Item, secondary=order_items, lazy=False,
                              order_by=items.c.id)})
        mapper(Item, items)
        mapper(User, users, properties=dict(
            addresses=relation(Address, lazy=False, order_by=addresses.c.id),
            open_orders=relation(
                Order,
                primaryjoin=sa.and_(orders.c.isopen == 1,
                                 users.c.id==orders.c.user_id),
                lazy=False, order_by=orders.c.id),
            closed_orders=relation(
                Order,
                primaryjoin=sa.and_(orders.c.isopen == 0,
                                 users.c.id==orders.c.user_id),
                lazy=False, order_by=orders.c.id)))
        q = create_session().query(User).order_by(User.id)

        def go():
            assert [
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
            ] == q.all()
        self.assert_sql_count(testing.db, go, 1)

    @testing.resolve_artifact_names
    def test_no_false_hits(self):
        """Eager loaders don't interpret main table columns as part of their eager load."""
        User, Address, Order = self.classes.get_all(
            'User', 'Address', 'Order')
        users, addresses, orders = self.tables.get_all(
            'users', 'addresses', 'orders')

        mapper(User, users, properties={
            'addresses':relation(Address, lazy=False),
            'orders':relation(Order, lazy=False)
        })
        mapper(Address, addresses)
        mapper(Order, orders)

        allusers = create_session().query(User).all()

        # using a textual select, the columns will be 'id' and 'name'.  the
        # eager loaders have aliases which should not hit on those columns,
        # they should be required to locate only their aliased/fully table
        # qualified column name.
        noeagers = create_session().query(User).from_statement("select * from users").all()
        assert 'orders' not in noeagers[0].__dict__
        assert 'addresses' not in noeagers[0].__dict__

    @testing.fails_on('maxdb', 'FIXME: unknown')
    @testing.resolve_artifact_names
    def test_limit(self):
        """Limit operations combined with lazy-load relationships."""
        User, Item, Address, Order = self.classes.get_all(
            'User', 'Item', 'Address', 'Order')
        users, items, order_items, orders, addresses = self.tables.get_all(
            'users', 'items', 'order_items', 'orders', 'addresses')

        mapper(Item, items)
        mapper(Order, orders, properties={
            'items':relation(Item, secondary=order_items, lazy=False, order_by=items.c.id)
        })
        mapper(User, users, properties={
            'addresses':relation(mapper(Address, addresses), lazy=False, order_by=addresses.c.id),
            'orders':relation(Order, lazy=True)
        })

        sess = create_session()
        q = sess.query(User)

        if testing.against('mysql'):
            l = q.limit(2).all()
            assert self.static.user_all_result[:2] == l
        else:
            l = q.order_by(User.id).limit(2).offset(1).all()
            print self.static.user_all_result[1:3]
            print l
            assert self.static.user_all_result[1:3] == l

    @testing.resolve_artifact_names
    def test_distinct(self):
        # this is an involved 3x union of the users table to get a lot of rows.
        # then see if the "distinct" works its way out.  you actually get the same
        # result with or without the distinct, just via less or more rows.
        u2 = users.alias('u2')
        s = sa.union_all(u2.select(use_labels=True), u2.select(use_labels=True), u2.select(use_labels=True)).alias('u')

        mapper(User, users, properties={
            'addresses':relation(mapper(Address, addresses), lazy=False, order_by=addresses.c.id),
        })

        sess = create_session()
        q = sess.query(User)

        def go():
            l = q.filter(s.c.u2_id==User.id).distinct().order_by(User.id).all()
            eq_(self.static.user_address_result, l)
        self.assert_sql_count(testing.db, go, 1)

    @testing.fails_on('maxdb', 'FIXME: unknown')
    @testing.resolve_artifact_names
    def test_limit_2(self):
        mapper(Keyword, keywords)
        mapper(Item, items, properties = dict(
                keywords = relation(Keyword, secondary=item_keywords, lazy=False, order_by=[keywords.c.id]),
            ))

        sess = create_session()
        q = sess.query(Item)
        l = q.filter((Item.description=='item 2') | (Item.description=='item 5') | (Item.description=='item 3')).\
            order_by(Item.id).limit(2).all()

        assert self.static.item_keyword_result[1:3] == l

    @testing.fails_on('maxdb', 'FIXME: unknown')
    @testing.resolve_artifact_names
    def test_limit_3(self):
        """test that the ORDER BY is propagated from the inner select to the outer select, when using the
        'wrapped' select statement resulting from the combination of eager loading and limit/offset clauses."""

        mapper(Item, items)
        mapper(Order, orders, properties = dict(
                items = relation(Item, secondary=order_items, lazy=False)
        ))

        mapper(Address, addresses)
        mapper(User, users, properties = dict(
            addresses = relation(Address, lazy=False, order_by=addresses.c.id),
            orders = relation(Order, lazy=False, order_by=orders.c.id),
        ))
        sess = create_session()

        q = sess.query(User)

        if not testing.against('maxdb', 'mssql'):
            l = q.join('orders').order_by(Order.user_id.desc()).limit(2).offset(1)
            assert [
                User(id=9,
                    orders=[Order(id=2), Order(id=4)],
                    addresses=[Address(id=5)]
                ),
                User(id=7,
                    orders=[Order(id=1), Order(id=3), Order(id=5)],
                    addresses=[Address(id=1)]
                )
            ] == l.all()

        l = q.join('addresses').order_by(Address.email_address.desc()).limit(1).offset(0)
        assert [
            User(id=7,
                orders=[Order(id=1), Order(id=3), Order(id=5)],
                addresses=[Address(id=1)]
            )
        ] == l.all()

    @testing.resolve_artifact_names
    def test_limit_4(self):
        # tests the LIMIT/OFFSET aliasing on a mapper against a select.   original issue from ticket #904
        sel = sa.select([users, addresses.c.email_address], users.c.id==addresses.c.user_id).alias('useralias')
        mapper(User, sel, properties={
            'orders':relation(Order, primaryjoin=sel.c.id==orders.c.user_id, lazy=False)
        })
        mapper(Order, orders)

        sess = create_session()
        eq_(sess.query(User).first(),
            User(name=u'jack',orders=[
                Order(address_id=1,description=u'order 1',isopen=0,user_id=7,id=1),
                Order(address_id=1,description=u'order 3',isopen=1,user_id=7,id=3),
                Order(address_id=None,description=u'order 5',isopen=0,user_id=7,id=5)],
            email_address=u'jack@bean.com',id=7)
        )

    @testing.resolve_artifact_names
    def test_one_to_many_scalar(self):
        mapper(User, users, properties = dict(
            address = relation(mapper(Address, addresses), lazy=False, uselist=False)
        ))
        q = create_session().query(User)

        def go():
            l = q.filter(users.c.id == 7).all()
            assert [User(id=7, address=Address(id=1))] == l
        self.assert_sql_count(testing.db, go, 1)

    @testing.fails_on('maxdb', 'FIXME: unknown')
    @testing.resolve_artifact_names
    def test_many_to_one(self):
        mapper(Address, addresses, properties = dict(
            user = relation(mapper(User, users), lazy=False)
        ))
        sess = create_session()
        q = sess.query(Address)

        def go():
            a = q.filter(addresses.c.id==1).one()
            assert a.user is not None
            u1 = sess.query(User).get(7)
            assert a.user is u1
        self.assert_sql_count(testing.db, go, 1)

    @testing.resolve_artifact_names
    def test_many_to_one_null(self):
        """test that a many-to-one eager load which loads None does
        not later trigger a lazy load.
        
        """
        
        # use a primaryjoin intended to defeat SA's usage of 
        # query.get() for a many-to-one lazyload
        mapper(Order, orders, properties = dict(
            address = relation(mapper(Address, addresses), 
                primaryjoin=and_(
                    addresses.c.id==orders.c.address_id,
                    addresses.c.email_address != None
                ),
            
            lazy=False)
        ))
        sess = create_session()

        def go():
            o1 = sess.query(Order).options(lazyload('address')).filter(Order.id==5).one()
            eq_(o1.address, None)
        self.assert_sql_count(testing.db, go, 2)
        
        sess.expunge_all()
        def go():
            o1 = sess.query(Order).filter(Order.id==5).one()
            eq_(o1.address, None)
        self.assert_sql_count(testing.db, go, 1)
        
    @testing.resolve_artifact_names
    def test_one_and_many(self):
        """tests eager load for a parent object with a child object that
        contains a many-to-many relationship to a third object."""

        mapper(User, users, properties={
            'orders':relation(Order, lazy=False, order_by=orders.c.id)
        })
        mapper(Item, items)
        mapper(Order, orders, properties = dict(
                items = relation(Item, secondary=order_items, lazy=False, order_by=items.c.id)
            ))

        q = create_session().query(User)

        l = q.filter("users.id in (7, 8, 9)").order_by("users.id")

        def go():
            assert self.static.user_order_result[0:3] == l.all()
        self.assert_sql_count(testing.db, go, 1)

    @testing.resolve_artifact_names
    def test_double_with_aggregate(self):
        max_orders_by_user = sa.select([sa.func.max(orders.c.id).label('order_id')], group_by=[orders.c.user_id]).alias('max_orders_by_user')

        max_orders = orders.select(orders.c.id==max_orders_by_user.c.order_id).alias('max_orders')

        mapper(Order, orders)
        mapper(User, users, properties={
               'orders':relation(Order, backref='user', lazy=False),
               'max_order':relation(mapper(Order, max_orders, non_primary=True), lazy=False, uselist=False)
               })
        q = create_session().query(User)

        def go():
            assert [
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
            ] == q.all()
        self.assert_sql_count(testing.db, go, 1)

    @testing.resolve_artifact_names
    def test_wide(self):
        mapper(Order, orders, properties={'items':relation(Item, secondary=order_items, lazy=False, order_by=items.c.id)})
        mapper(Item, items)
        mapper(User, users, properties = dict(
            addresses = relation(mapper(Address, addresses), lazy = False, order_by=addresses.c.id),
            orders = relation(Order, lazy = False, order_by=orders.c.id),
        ))
        q = create_session().query(User)
        l = q.all()
        assert self.static.user_all_result == q.order_by(User.id).all()

    @testing.resolve_artifact_names
    def test_against_select(self):
        """test eager loading of a mapper which is against a select"""

        s = sa.select([orders], orders.c.isopen==1).alias('openorders')

        mapper(Order, s, properties={
            'user':relation(User, lazy=False)
        })
        mapper(User, users)
        mapper(Item, items)

        q = create_session().query(Order)
        assert [
            Order(id=3, user=User(id=7)),
            Order(id=4, user=User(id=9))
        ] == q.all()

        q = q.select_from(s.join(order_items).join(items)).filter(~Item.id.in_([1, 2, 5]))
        assert [
            Order(id=3, user=User(id=7)),
        ] == q.all()

    @testing.resolve_artifact_names
    def test_aliasing(self):
        """test that eager loading uses aliases to insulate the eager load from regular criterion against those tables."""

        mapper(User, users, properties = dict(
            addresses = relation(mapper(Address, addresses), lazy=False, order_by=addresses.c.id)
        ))
        q = create_session().query(User)
        l = q.filter(addresses.c.email_address == 'ed@lala.com').filter(Address.user_id==User.id).order_by(User.id)
        assert self.static.user_address_result[1:2] == l.all()

class AddEntityTest(_fixtures.FixtureTest):
    run_inserts = 'once'
    run_deletes = None

    @testing.resolve_artifact_names
    def _assert_result(self):
        return [
            (
                User(id=7,
                    addresses=[Address(id=1)]
                ),
                Order(id=1,
                    items=[Item(id=1), Item(id=2), Item(id=3)]
                ),
            ),
            (
                User(id=7,
                    addresses=[Address(id=1)]
                ),
                Order(id=3,
                    items=[Item(id=3), Item(id=4), Item(id=5)]
                ),
            ),
            (
                User(id=7,
                    addresses=[Address(id=1)]
                ),
                Order(id=5,
                    items=[Item(id=5)]
                ),
            ),
            (
                 User(id=9,
                    addresses=[Address(id=5)]
                ),
                 Order(id=2,
                    items=[Item(id=1), Item(id=2), Item(id=3)]
                ),
             ),
             (
                  User(id=9,
                    addresses=[Address(id=5)]
                ),
                  Order(id=4,
                    items=[Item(id=1), Item(id=5)]
                ),
              )
        ]

    @testing.resolve_artifact_names
    def test_mapper_configured(self):
        mapper(User, users, properties={
            'addresses':relation(Address, lazy=False),
            'orders':relation(Order)
        })
        mapper(Address, addresses)
        mapper(Order, orders, properties={
            'items':relation(Item, secondary=order_items, lazy=False, order_by=items.c.id)
        })
        mapper(Item, items)


        sess = create_session()
        oalias = sa.orm.aliased(Order)
        def go():
            ret = sess.query(User, oalias).join(('orders', oalias)).order_by(User.id, oalias.id).all()
            eq_(ret, self._assert_result())
        self.assert_sql_count(testing.db, go, 1)

    @testing.resolve_artifact_names
    def test_options(self):
        mapper(User, users, properties={
            'addresses':relation(Address),
            'orders':relation(Order)
        })
        mapper(Address, addresses)
        mapper(Order, orders, properties={
            'items':relation(Item, secondary=order_items, order_by=items.c.id)
        })
        mapper(Item, items)

        sess = create_session()

        oalias = sa.orm.aliased(Order)
        def go():
            ret = sess.query(User, oalias).options(eagerload('addresses')).join(('orders', oalias)).order_by(User.id, oalias.id).all()
            eq_(ret, self._assert_result())
        self.assert_sql_count(testing.db, go, 6)

        sess.expunge_all()
        def go():
            ret = sess.query(User, oalias).options(eagerload('addresses'), eagerload(oalias.items)).join(('orders', oalias)).order_by(User.id, oalias.id).all()
            eq_(ret, self._assert_result())
        self.assert_sql_count(testing.db, go, 1)

class OrderBySecondaryTest(_base.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table('m2m', metadata,
              Column('id', Integer, primary_key=True),
              Column('aid', Integer, ForeignKey('a.id')),
              Column('bid', Integer, ForeignKey('b.id')))

        Table('a', metadata,
              Column('id', Integer, primary_key=True),
              Column('data', String(50)))
        Table('b', metadata,
              Column('id', Integer, primary_key=True),
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

    @testing.resolve_artifact_names
    def test_ordering(self):
        class A(_base.ComparableEntity):pass
        class B(_base.ComparableEntity):pass

        mapper(A, a, properties={
            'bs':relation(B, secondary=m2m, lazy=False, order_by=m2m.c.id)
        })
        mapper(B, b)

        sess = create_session()
        eq_(sess.query(A).all(), [A(data='a1', bs=[B(data='b3'), B(data='b1'), B(data='b2')]), A(bs=[B(data='b4'), B(data='b3'), B(data='b2')])])


class SelfReferentialEagerTest(_base.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table('nodes', metadata,
              Column('id', Integer, sa.Sequence('node_id_seq', optional=True),
                     primary_key=True),
            Column('parent_id', Integer, ForeignKey('nodes.id')),
            Column('data', String(30)))

    @testing.fails_on('maxdb', 'FIXME: unknown')
    @testing.resolve_artifact_names
    def test_basic(self):
        class Node(_base.ComparableEntity):
            def append(self, node):
                self.children.append(node)

        mapper(Node, nodes, properties={
            'children':relation(Node, lazy=False, join_depth=3, order_by=nodes.c.id)
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
            d = sess.query(Node).filter_by(data='n1').all()[0]
            assert Node(data='n1', children=[
                Node(data='n11'),
                Node(data='n12', children=[
                    Node(data='n121'),
                    Node(data='n122'),
                    Node(data='n123')
                ]),
                Node(data='n13')
            ]) == d
        self.assert_sql_count(testing.db, go, 1)

        sess.expunge_all()
        def go():
            d = sess.query(Node).filter_by(data='n1').first()
            assert Node(data='n1', children=[
                Node(data='n11'),
                Node(data='n12', children=[
                    Node(data='n121'),
                    Node(data='n122'),
                    Node(data='n123')
                ]),
                Node(data='n13')
            ]) == d
        self.assert_sql_count(testing.db, go, 1)


    @testing.resolve_artifact_names
    def test_lazy_fallback_doesnt_affect_eager(self):
        class Node(_base.ComparableEntity):
            def append(self, node):
                self.children.append(node)

        mapper(Node, nodes, properties={
            'children':relation(Node, lazy=False, join_depth=1, order_by=nodes.c.id)
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

        # eager load with join depth 1.  when eager load of 'n1' hits the
        # children of 'n12', no columns are present, eager loader degrades to
        # lazy loader; fine.  but then, 'n12' is *also* in the first level of
        # columns since we're loading the whole table.  when those rows
        # arrive, now we *can* eager load its children and an eager collection
        # should be initialized.  essentially the 'n12' instance is present in
        # not just two different rows but two distinct sets of columns in this
        # result set.
        def go():
            allnodes = sess.query(Node).order_by(Node.data).all()
            n12 = allnodes[2]
            assert n12.data == 'n12'
            assert [
                Node(data='n121'),
                Node(data='n122'),
                Node(data='n123')
            ] == list(n12.children)
        self.assert_sql_count(testing.db, go, 1)

    @testing.resolve_artifact_names
    def test_with_deferred(self):
        class Node(_base.ComparableEntity):
            def append(self, node):
                self.children.append(node)

        mapper(Node, nodes, properties={
            'children':relation(Node, lazy=False, join_depth=3, order_by=nodes.c.id),
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
        self.assert_sql_count(testing.db, go, 4)

        sess.expunge_all()

        def go():
            assert Node(data='n1', children=[Node(data='n11'), Node(data='n12')]) == sess.query(Node).options(undefer('data')).order_by(Node.id).first()
        self.assert_sql_count(testing.db, go, 3)

        sess.expunge_all()

        def go():
            assert Node(data='n1', children=[Node(data='n11'), Node(data='n12')]) == sess.query(Node).options(undefer('data'), undefer('children.data')).first()
        self.assert_sql_count(testing.db, go, 1)


    @testing.resolve_artifact_names
    def test_options(self):
        class Node(_base.ComparableEntity):
            def append(self, node):
                self.children.append(node)

        mapper(Node, nodes, properties={
            'children':relation(Node, lazy=True, order_by=nodes.c.id)
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
            d = sess.query(Node).filter_by(data='n1').options(eagerload('children.children')).first()
            assert Node(data='n1', children=[
                Node(data='n11'),
                Node(data='n12', children=[
                    Node(data='n121'),
                    Node(data='n122'),
                    Node(data='n123')
                ]),
                Node(data='n13')
            ]) == d
        self.assert_sql_count(testing.db, go, 2)

        def go():
            d = sess.query(Node).filter_by(data='n1').options(eagerload('children.children')).first()

        # test that the query isn't wrapping the initial query for eager loading.
        self.assert_sql_execution(testing.db, go, 
            CompiledSQL(
                "SELECT nodes.id AS nodes_id, nodes.parent_id AS nodes_parent_id, nodes.data AS nodes_data FROM nodes "
                "WHERE nodes.data = :data_1 ORDER BY nodes.id  LIMIT 1 OFFSET 0",
                {'data_1': 'n1'}
            )
        )

    @testing.fails_on('maxdb', 'FIXME: unknown')
    @testing.resolve_artifact_names
    def test_no_depth(self):
        class Node(_base.ComparableEntity):
            def append(self, node):
                self.children.append(node)

        mapper(Node, nodes, properties={
            'children':relation(Node, lazy=False)
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
            d = sess.query(Node).filter_by(data='n1').first()
            assert Node(data='n1', children=[
                Node(data='n11'),
                Node(data='n12', children=[
                    Node(data='n121'),
                    Node(data='n122'),
                    Node(data='n123')
                ]),
                Node(data='n13')
            ]) == d
        self.assert_sql_count(testing.db, go, 3)

class MixedSelfReferentialEagerTest(_base.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table('a_table', metadata,
                       Column('id', Integer, primary_key=True)
                       )

        Table('b_table', metadata,
                       Column('id', Integer, primary_key=True),
                       Column('parent_b1_id', Integer, ForeignKey('b_table.id')),
                       Column('parent_a_id', Integer, ForeignKey('a_table.id')),
                       Column('parent_b2_id', Integer, ForeignKey('b_table.id')))


    @classmethod
    @testing.resolve_artifact_names
    def setup_mappers(cls):
        class A(_base.ComparableEntity):
            pass
        class B(_base.ComparableEntity):
            pass
            
        mapper(A,a_table)
        mapper(B,b_table,properties = {
           'parent_b1': relation(B,
                            remote_side = [b_table.c.id],
                            primaryjoin = (b_table.c.parent_b1_id ==b_table.c.id),
                            order_by = b_table.c.id
                            ),
           'parent_z': relation(A,lazy = True),
           'parent_b2': relation(B,
                            remote_side = [b_table.c.id],
                            primaryjoin = (b_table.c.parent_b2_id ==b_table.c.id),
                            order_by = b_table.c.id
                            )
        });
    
    @classmethod
    @testing.resolve_artifact_names
    def insert_data(cls):
        a_table.insert().execute(dict(id=1), dict(id=2), dict(id=3))
        b_table.insert().execute(
            dict(id=1, parent_a_id=2, parent_b1_id=None, parent_b2_id=None),
            dict(id=2, parent_a_id=1, parent_b1_id=1, parent_b2_id=None),
            dict(id=3, parent_a_id=1, parent_b1_id=1, parent_b2_id=2),
            dict(id=4, parent_a_id=3, parent_b1_id=1, parent_b2_id=None),
            dict(id=5, parent_a_id=3, parent_b1_id=None, parent_b2_id=2),
            dict(id=6, parent_a_id=1, parent_b1_id=1, parent_b2_id=3),
            dict(id=7, parent_a_id=2, parent_b1_id=None, parent_b2_id=3),
            dict(id=8, parent_a_id=2, parent_b1_id=1, parent_b2_id=2),
            dict(id=9, parent_a_id=None, parent_b1_id=1, parent_b2_id=None),
            dict(id=10, parent_a_id=3, parent_b1_id=7, parent_b2_id=2),
            dict(id=11, parent_a_id=3, parent_b1_id=1, parent_b2_id=8),
            dict(id=12, parent_a_id=2, parent_b1_id=5, parent_b2_id=2),
            dict(id=13, parent_a_id=3, parent_b1_id=4, parent_b2_id=4),
            dict(id=14, parent_a_id=3, parent_b1_id=7, parent_b2_id=2),
        )
        
    @testing.resolve_artifact_names
    def test_eager_load(self):
        session = create_session()
        def go():
            eq_(
                session.query(B).options(eagerload('parent_b1'),eagerload('parent_b2'),eagerload('parent_z')).
                            filter(B.id.in_([2, 8, 11])).order_by(B.id).all(),
                [
                    B(id=2, parent_z=A(id=1), parent_b1=B(id=1), parent_b2=None),
                    B(id=8, parent_z=A(id=2), parent_b1=B(id=1), parent_b2=B(id=2)),
                    B(id=11, parent_z=A(id=3), parent_b1=B(id=1), parent_b2=B(id=8))
                ]
            )
        self.assert_sql_count(testing.db, go, 1)
        
class SelfReferentialM2MEagerTest(_base.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table('widget', metadata,
            Column('id', Integer, primary_key=True),
            Column('name', sa.Unicode(40), nullable=False, unique=True),
        )

        Table('widget_rel', metadata,
            Column('parent_id', Integer, ForeignKey('widget.id')),
            Column('child_id', Integer, ForeignKey('widget.id')),
            sa.UniqueConstraint('parent_id', 'child_id'),
        )

    @testing.resolve_artifact_names
    def test_basic(self):
        class Widget(_base.ComparableEntity):
            pass

        mapper(Widget, widget, properties={
            'children': relation(Widget, secondary=widget_rel,
                primaryjoin=widget_rel.c.parent_id==widget.c.id,
                secondaryjoin=widget_rel.c.child_id==widget.c.id,
                lazy=False, join_depth=1,
            )
        })

        sess = create_session()
        w1 = Widget(name=u'w1')
        w2 = Widget(name=u'w2')
        w1.children.append(w2)
        sess.add(w1)
        sess.flush()
        sess.expunge_all()

        assert [Widget(name='w1', children=[Widget(name='w2')])] == sess.query(Widget).filter(Widget.name==u'w1').all()

class MixedEntitiesTest(_fixtures.FixtureTest, testing.AssertsCompiledSQL):
    run_setup_mappers = 'once'
    run_inserts = 'once'
    run_deletes = None

    @classmethod
    @testing.resolve_artifact_names
    def setup_mappers(cls):
        mapper(User, users, properties={
            'addresses':relation(Address, backref='user'),
            'orders':relation(Order, backref='user'), # o2m, m2o
        })
        mapper(Address, addresses)
        mapper(Order, orders, properties={
            'items':relation(Item, secondary=order_items, order_by=items.c.id),  #m2m
        })
        mapper(Item, items, properties={
            'keywords':relation(Keyword, secondary=item_keywords) #m2m
        })
        mapper(Keyword, keywords)

    @testing.resolve_artifact_names
    def test_two_entities(self):
        sess = create_session()

        # two FROM clauses
        def go():
            eq_(
                [
                    (User(id=9, addresses=[Address(id=5)]), Order(id=2, items=[Item(id=1), Item(id=2), Item(id=3)])),
                    (User(id=9, addresses=[Address(id=5)]), Order(id=4, items=[Item(id=1), Item(id=5)])),
                ],
                sess.query(User, Order).filter(User.id==Order.user_id).\
                    options(eagerload(User.addresses), eagerload(Order.items)).filter(User.id==9).\
                        order_by(User.id, Order.id).all(),
            )
        self.assert_sql_count(testing.db, go, 1)

        # one FROM clause
        def go():
            eq_(
                [
                    (User(id=9, addresses=[Address(id=5)]), Order(id=2, items=[Item(id=1), Item(id=2), Item(id=3)])),
                    (User(id=9, addresses=[Address(id=5)]), Order(id=4, items=[Item(id=1), Item(id=5)])),
                ],
                sess.query(User, Order).join(User.orders).options(eagerload(User.addresses), eagerload(Order.items)).filter(User.id==9).\
                    order_by(User.id, Order.id).all(),
            )
        self.assert_sql_count(testing.db, go, 1)

    @testing.exclude('sqlite', '>', (0, 0, 0), "sqlite flat out blows it on the multiple JOINs")
    @testing.resolve_artifact_names
    def test_two_entities_with_joins(self):
        sess = create_session()
        
        # two FROM clauses where there's a join on each one
        def go():
            u1 = aliased(User)
            o1 = aliased(Order)
            eq_(
                [
                    (
                        User(addresses=[Address(email_address=u'fred@fred.com')], name=u'fred'), 
                        Order(description=u'order 2', isopen=0, items=[Item(description=u'item 1'), Item(description=u'item 2'), Item(description=u'item 3')]),
                        User(addresses=[Address(email_address=u'jack@bean.com')], name=u'jack'), 
                        Order(description=u'order 3', isopen=1, items=[Item(description=u'item 3'), Item(description=u'item 4'), Item(description=u'item 5')])
                    ), 

                    (
                        User(addresses=[Address(email_address=u'fred@fred.com')], name=u'fred'), 
                        Order(description=u'order 2', isopen=0, items=[Item(description=u'item 1'), Item(description=u'item 2'), Item(description=u'item 3')]),
                        User(addresses=[Address(email_address=u'jack@bean.com')], name=u'jack'), 
                        Order(address_id=None, description=u'order 5', isopen=0, items=[Item(description=u'item 5')])
                    ), 

                    (
                        User(addresses=[Address(email_address=u'fred@fred.com')], name=u'fred'), 
                        Order(description=u'order 4', isopen=1, items=[Item(description=u'item 1'), Item(description=u'item 5')]),
                        User(addresses=[Address(email_address=u'jack@bean.com')], name=u'jack'), 
                        Order(address_id=None, description=u'order 5', isopen=0, items=[Item(description=u'item 5')])
                    ), 
                ],
                sess.query(User, Order, u1, o1).\
                        join((Order, User.orders)).options(eagerload(User.addresses), eagerload(Order.items)).filter(User.id==9).\
                        join((o1, u1.orders)).options(eagerload(u1.addresses), eagerload(o1.items)).filter(u1.id==7).\
                        filter(Order.id<o1.id).\
                        order_by(User.id, Order.id, u1.id, o1.id).all(),
            )
        self.assert_sql_count(testing.db, go, 1)
        
        

    @testing.resolve_artifact_names
    def test_aliased_entity(self):
        sess = create_session()

        oalias = sa.orm.aliased(Order)

        # two FROM clauses
        def go():
            eq_(
                [
                    (User(id=9, addresses=[Address(id=5)]), Order(id=2, items=[Item(id=1), Item(id=2), Item(id=3)])),
                    (User(id=9, addresses=[Address(id=5)]), Order(id=4, items=[Item(id=1), Item(id=5)])),
                ],
                sess.query(User, oalias).filter(User.id==oalias.user_id).\
                    options(eagerload(User.addresses), eagerload(oalias.items)).filter(User.id==9).\
                    order_by(User.id, oalias.id).all(),
            )
        self.assert_sql_count(testing.db, go, 1)

        # one FROM clause
        def go():
            eq_(
                [
                    (User(id=9, addresses=[Address(id=5)]), Order(id=2, items=[Item(id=1), Item(id=2), Item(id=3)])),
                    (User(id=9, addresses=[Address(id=5)]), Order(id=4, items=[Item(id=1), Item(id=5)])),
                ],
                sess.query(User, oalias).join((User.orders, oalias)).options(eagerload(User.addresses), eagerload(oalias.items)).filter(User.id==9).\
                    order_by(User.id, oalias.id).all(),
            )
        self.assert_sql_count(testing.db, go, 1)

        from sqlalchemy.engine.default import DefaultDialect

        # improper setup: oalias in the columns clause but join to usual
        # orders alias.  this should create two FROM clauses even though the
        # query has a from_clause set up via the join
        self.assert_compile(sess.query(User, oalias).join(User.orders).options(eagerload(oalias.items)).with_labels().statement,
        "SELECT users.id AS users_id, users.name AS users_name, orders_1.id AS orders_1_id, "\
        "orders_1.user_id AS orders_1_user_id, orders_1.address_id AS orders_1_address_id, "\
        "orders_1.description AS orders_1_description, orders_1.isopen AS orders_1_isopen, items_1.id AS items_1_id, "\
        "items_1.description AS items_1_description FROM users JOIN orders ON users.id = orders.user_id, "\
        "orders AS orders_1 LEFT OUTER JOIN order_items AS order_items_1 ON orders_1.id = order_items_1.order_id "\
        "LEFT OUTER JOIN items AS items_1 ON items_1.id = order_items_1.item_id ORDER BY items_1.id",
        dialect=DefaultDialect()
        )

class CyclicalInheritingEagerTest(_base.MappedTest):

    @classmethod
    def define_tables(cls, metadata):
        Table('t1', metadata,
            Column('c1', Integer, primary_key=True),
            Column('c2', String(30)),
            Column('type', String(30))
            )

        Table('t2', metadata,
            Column('c1', Integer, primary_key=True),
            Column('c2', String(30)),
            Column('type', String(30)),
            Column('t1.id', Integer, ForeignKey('t1.c1')))

    @testing.resolve_artifact_names
    def test_basic(self):
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
            't2s':relation(SubT2, lazy=False, backref=sa.orm.backref('subt', lazy=False))
        })
        mapper(T2, t2, polymorphic_on=t2.c.type, polymorphic_identity='t2')
        mapper(SubT2, None, inherits=T2, polymorphic_identity='subt2')

        # testing a particular endless loop condition in eager join setup
        create_session().query(SubT).all()

class SubqueryTest(_base.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table('users_table', metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String(16))
        )

        Table('tags_table', metadata,
            Column('id', Integer, primary_key=True),
            Column('user_id', Integer, ForeignKey("users_table.id")),
            Column('score1', sa.Float),
            Column('score2', sa.Float),
        )

    @testing.resolve_artifact_names
    def test_label_anonymizing(self):
        """Eager loading works with subqueries with labels,

        Even if an explicit labelname which conflicts with a label on the
        parent.

        There's not much reason a column_property() would ever need to have a
        label of a specific name (and they don't even need labels these days),
        unless you'd like the name to line up with a name that you may be
        using for a straight textual statement used for loading instances of
        that type.

        """
        class User(_base.ComparableEntity):
            @property
            def prop_score(self):
                return sum([tag.prop_score for tag in self.tags])

        class Tag(_base.ComparableEntity):
            @property
            def prop_score(self):
                return self.score1 * self.score2

        for labeled, labelname in [(True, 'score'), (True, None), (False, None)]:
            sa.orm.clear_mappers()

            tag_score = (tags_table.c.score1 * tags_table.c.score2)
            user_score = sa.select([sa.func.sum(tags_table.c.score1 *
                                                tags_table.c.score2)],
                                   tags_table.c.user_id == users_table.c.id)

            if labeled:
                tag_score = tag_score.label(labelname)
                user_score = user_score.label(labelname)
            else:
                user_score = user_score.as_scalar()

            mapper(Tag, tags_table, properties={
                'query_score': sa.orm.column_property(tag_score),
            })


            mapper(User, users_table, properties={
                'tags': relation(Tag, backref='user', lazy=False),
                'query_score': sa.orm.column_property(user_score),
            })

            session = create_session()
            session.add(User(name='joe', tags=[Tag(score1=5.0, score2=3.0), Tag(score1=55.0, score2=1.0)]))
            session.add(User(name='bar', tags=[Tag(score1=5.0, score2=4.0), Tag(score1=50.0, score2=1.0), Tag(score1=15.0, score2=2.0)]))
            session.flush()
            session.expunge_all()

            for user in session.query(User).all():
                eq_(user.query_score, user.prop_score)

            def go():
                u = session.query(User).filter_by(name='joe').one()
                eq_(u.query_score, u.prop_score)
            self.assert_sql_count(testing.db, go, 1)

            for t in (tags_table, users_table):
                t.delete().execute()

class CorrelatedSubqueryTest(_base.MappedTest):
    """tests for #946, #947, #948.
    
    The "users" table is joined to "stuff", and the relation
    would like to pull only the "stuff" entry with the most recent date.
    
    Exercises a variety of ways to configure this.
    
    """
    
    @classmethod
    def define_tables(cls, metadata):
        users = Table('users', metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String(50))
            )

        stuff = Table('stuff', metadata,
            Column('id', Integer, primary_key=True),
            Column('date', Date),
            Column('user_id', Integer, ForeignKey('users.id')))
    
    @classmethod
    @testing.resolve_artifact_names
    def insert_data(cls):
        users.insert().execute(
            {'id':1, 'name':'user1'},
            {'id':2, 'name':'user2'},
            {'id':3, 'name':'user3'},
        )

        stuff.insert().execute(
            {'id':1, 'user_id':1, 'date':datetime.date(2007, 10, 15)},
            {'id':2, 'user_id':1, 'date':datetime.date(2007, 12, 15)},
            {'id':3, 'user_id':1, 'date':datetime.date(2007, 11, 15)},
            {'id':4, 'user_id':2, 'date':datetime.date(2008, 1, 15)},
            {'id':5, 'user_id':3, 'date':datetime.date(2007, 6, 15)},
            {'id':6, 'user_id':3, 'date':datetime.date(2007, 3, 15)},
        )
        
    
    def test_labeled_on_date_noalias(self):
        self._do_test('label', True, False)

    def test_scalar_on_date_noalias(self):
        self._do_test('scalar', True, False)

    def test_plain_on_date_noalias(self):
        self._do_test('none', True, False)

    def test_labeled_on_limitid_noalias(self):
        self._do_test('label', False, False)

    def test_scalar_on_limitid_noalias(self):
        self._do_test('scalar', False, False)

    def test_plain_on_limitid_noalias(self):
        self._do_test('none', False, False)

    def test_labeled_on_date_alias(self):
        self._do_test('label', True, True)

    def test_scalar_on_date_alias(self):
        self._do_test('scalar', True, True)

    def test_plain_on_date_alias(self):
        self._do_test('none', True, True)

    def test_labeled_on_limitid_alias(self):
        self._do_test('label', False, True)

    def test_scalar_on_limitid_alias(self):
        self._do_test('scalar', False, True)

    def test_plain_on_limitid_alias(self):
        self._do_test('none', False, True)
        
    @testing.resolve_artifact_names
    def _do_test(self, labeled, ondate, aliasstuff):
        class User(_base.ComparableEntity):
            pass

        class Stuff(_base.ComparableEntity):
            pass
        
        mapper(Stuff, stuff)

        if aliasstuff:
            salias = stuff.alias()
        else:
            # if we don't alias the 'stuff' table within the correlated subquery, 
            # it gets aliased in the eager load along with the "stuff" table to "stuff_1".
            # but it's a scalar subquery, and this doesn't actually matter
            salias = stuff

        if ondate:
            # the more 'relational' way to do this, join on the max date
            stuff_view = select([func.max(salias.c.date).label('max_date')]).where(salias.c.user_id==users.c.id).correlate(users)
        else:
            # a common method with the MySQL crowd, which actually might perform better in some
            # cases - subquery does a limit with order by DESC, join on the id
            stuff_view = select([salias.c.id]).where(salias.c.user_id==users.c.id).correlate(users).order_by(salias.c.date.desc()).limit(1)

        if labeled == 'label':
            stuff_view = stuff_view.label('foo')
        elif labeled == 'scalar':
            stuff_view = stuff_view.as_scalar()

        if ondate:
            mapper(User, users, properties={
                'stuff':relation(Stuff, primaryjoin=and_(users.c.id==stuff.c.user_id, stuff.c.date==stuff_view))
            })
        else:
            mapper(User, users, properties={
                'stuff':relation(Stuff, primaryjoin=and_(users.c.id==stuff.c.user_id, stuff.c.id==stuff_view))
            })
            
        sess = create_session()
        def go():
            eq_(
                sess.query(User).order_by(User.name).options(eagerload('stuff')).all(),
                [
                    User(name='user1', stuff=[Stuff(id=2)]),
                    User(name='user2', stuff=[Stuff(id=4)]),
                    User(name='user3', stuff=[Stuff(id=5)])
                ]
            )
        self.assert_sql_count(testing.db, go, 1)
    
        sess = create_session()
        def go():
            eq_(
                sess.query(User).order_by(User.name).first(),
                User(name='user1', stuff=[Stuff(id=2)])
            )
        self.assert_sql_count(testing.db, go, 2)

        sess = create_session()
        def go():
            eq_(
                sess.query(User).order_by(User.name).options(eagerload('stuff')).first(),
                User(name='user1', stuff=[Stuff(id=2)])
            )
        self.assert_sql_count(testing.db, go, 1)

        sess = create_session()
        def go():
            eq_(
                sess.query(User).filter(User.id==2).options(eagerload('stuff')).one(),
                User(name='user2', stuff=[Stuff(id=4)])
            )
        self.assert_sql_count(testing.db, go, 1)


