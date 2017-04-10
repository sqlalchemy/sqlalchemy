"""tests of joined-eager loaded attributes"""

from sqlalchemy.testing import eq_, is_, is_not_, in_
import sqlalchemy as sa
from sqlalchemy import testing
from sqlalchemy.orm import joinedload, deferred, undefer, \
    joinedload_all, backref, Session,\
    defaultload, Load, load_only, contains_eager
from sqlalchemy import Integer, String, Date, ForeignKey, and_, select, \
    func, text
from sqlalchemy.testing.schema import Table, Column
from sqlalchemy.orm import mapper, relationship, create_session, \
    lazyload, aliased, column_property
from sqlalchemy.sql import operators
from sqlalchemy.testing import assert_raises, assert_raises_message
from sqlalchemy.testing.assertsql import CompiledSQL
from sqlalchemy.testing import fixtures, expect_warnings
from test.orm import _fixtures
from sqlalchemy.util import OrderedDict as odict
import datetime


class EagerTest(_fixtures.FixtureTest, testing.AssertsCompiledSQL):
    run_inserts = 'once'
    run_deletes = None
    __dialect__ = 'default'

    def test_basic(self):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User)

        mapper(User, users, properties={
            'addresses': relationship(
                mapper(Address, addresses), lazy='joined', order_by=Address.id)
        })
        sess = create_session()
        q = sess.query(User)

        eq_([User(id=7, addresses=[
            Address(id=1, email_address='jack@bean.com')])],
            q.filter(User.id == 7).all())
        eq_(self.static.user_address_result, q.order_by(User.id).all())

    def test_late_compile(self):
        User, Address, addresses, users = (
            self.classes.User,
            self.classes.Address,
            self.tables.addresses,
            self.tables.users)

        m = mapper(User, users)
        sess = create_session()
        sess.query(User).all()
        m.add_property("addresses", relationship(mapper(Address, addresses)))

        sess.expunge_all()

        def go():
            eq_(
                [User(id=7, addresses=[
                    Address(id=1, email_address='jack@bean.com')])],
                sess.query(User).options(
                    joinedload('addresses')).filter(User.id == 7).all()
            )
        self.assert_sql_count(testing.db, go, 1)

    def test_no_orphan(self):
        """An eagerly loaded child object is not marked as an orphan"""

        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User)

        mapper(User, users, properties={
            'addresses': relationship(
                Address, cascade="all,delete-orphan", lazy='joined')
        })
        mapper(Address, addresses)

        sess = create_session()
        user = sess.query(User).get(7)
        assert getattr(User, 'addresses').\
            hasparent(
                sa.orm.attributes.instance_state(
                    user.addresses[0]), optimistic=True)
        assert not sa.orm.class_mapper(Address).\
            _is_orphan(
                sa.orm.attributes.instance_state(user.addresses[0]))

    def test_orderby(self):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User)

        mapper(User, users, properties={
            'addresses': relationship(
                mapper(Address, addresses),
                lazy='joined', order_by=addresses.c.email_address),
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
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User)

        mapper(User, users, properties={
            'addresses': relationship(
                mapper(Address, addresses),
                lazy='joined',
                order_by=[addresses.c.email_address, addresses.c.id]),
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
        mapper(User, users, properties=dict(
            addresses=relationship(
                Address, lazy='joined', order_by=addresses.c.id),
        ))

        q = create_session().query(User)
        result = q.filter(User.id == Address.user_id).order_by(
            Address.email_address).all()

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
        ], result)

    def test_orderby_desc(self):
        Address, addresses, users, User = (self.classes.Address,
                                           self.tables.addresses,
                                           self.tables.users,
                                           self.classes.User)

        mapper(Address, addresses)
        mapper(User, users, properties=dict(
            addresses=relationship(
                Address, lazy='joined',
                order_by=[sa.desc(addresses.c.email_address)]),
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

    def test_no_ad_hoc_orderby(self):
        """part of #2992; make sure string label references can't
        access an eager loader, else an eager load can corrupt the query.

        """
        Address, addresses, users, User = (self.classes.Address,
                                           self.tables.addresses,
                                           self.tables.users,
                                           self.classes.User)

        mapper(Address, addresses)
        mapper(User, users, properties=dict(
            addresses=relationship(
                Address),
        ))

        sess = create_session()
        q = sess.query(User).\
            join("addresses").\
            options(joinedload("addresses")).\
            order_by("email_address")

        self.assert_compile(
            q,
            "SELECT users.id AS users_id, users.name AS users_name, "
            "addresses_1.id AS addresses_1_id, addresses_1.user_id AS "
            "addresses_1_user_id, addresses_1.email_address AS "
            "addresses_1_email_address FROM users JOIN addresses "
            "ON users.id = addresses.user_id LEFT OUTER JOIN addresses "
            "AS addresses_1 ON users.id = addresses_1.user_id "
            "ORDER BY addresses.email_address"
        )

        q = sess.query(User).options(joinedload("addresses")).\
            order_by("email_address")

        with expect_warnings("Can't resolve label reference 'email_address'"):
            self.assert_compile(
                q,
                "SELECT users.id AS users_id, users.name AS users_name, "
                "addresses_1.id AS addresses_1_id, addresses_1.user_id AS "
                "addresses_1_user_id, addresses_1.email_address AS "
                "addresses_1_email_address FROM users LEFT OUTER JOIN "
                "addresses AS addresses_1 ON users.id = addresses_1.user_id "
                "ORDER BY email_address"
            )

    def test_deferred_fk_col(self):
        users, Dingaling, User, dingalings, Address, addresses = (
            self.tables.users,
            self.classes.Dingaling,
            self.classes.User,
            self.tables.dingalings,
            self.classes.Address,
            self.tables.addresses)

        mapper(Address, addresses, properties={
            'user_id': deferred(addresses.c.user_id),
            'user': relationship(User, lazy='joined')
        })
        mapper(User, users)

        sess = create_session()

        for q in [
            sess.query(Address).filter(
                Address.id.in_([1, 4, 5])
            ).order_by(Address.id),
            sess.query(Address).filter(
                Address.id.in_([1, 4, 5])
            ).order_by(Address.id).limit(3)
        ]:
            sess.expunge_all()
            eq_(q.all(),
                [Address(id=1, user=User(id=7)),
                 Address(id=4, user=User(id=8)),
                 Address(id=5, user=User(id=9))]
                )

        sess.expunge_all()
        a = sess.query(Address).filter(Address.id == 1).all()[0]

        # 1.0 change!  we don't automatically undefer user_id here.
        # if the user wants a column undeferred, add the option.
        def go():
            eq_(a.user_id, 7)
        # self.assert_sql_count(testing.db, go, 0)
        self.assert_sql_count(testing.db, go, 1)

        sess.expunge_all()
        a = sess.query(Address).filter(Address.id == 1).first()

        def go():
            eq_(a.user_id, 7)
        # same, 1.0 doesn't check these
        # self.assert_sql_count(testing.db, go, 0)
        self.assert_sql_count(testing.db, go, 1)

        # do the mapping in reverse
        # (we would have just used an "addresses" backref but the test
        # fixtures then require the whole backref to be set up, lazy loaders
        # trigger, etc.)
        sa.orm.clear_mappers()

        mapper(Address, addresses, properties={
            'user_id': deferred(addresses.c.user_id),
        })
        mapper(User, users, properties={
            'addresses': relationship(Address, lazy='joined')})

        for q in [
            sess.query(User).filter(User.id == 7),
            sess.query(User).filter(User.id == 7).limit(1)
        ]:
            sess.expunge_all()
            eq_(q.all(),
                [User(id=7, addresses=[Address(id=1)])]
                )

        sess.expunge_all()
        u = sess.query(User).get(7)

        def go():
            eq_(u.addresses[0].user_id, 7)
        # assert that the eager loader didn't have to affect 'user_id' here
        # and that its still deferred
        self.assert_sql_count(testing.db, go, 1)

        sa.orm.clear_mappers()

        mapper(User, users, properties={
            'addresses': relationship(Address, lazy='joined',
                                      order_by=addresses.c.id)})
        mapper(Address, addresses, properties={
            'user_id': deferred(addresses.c.user_id),
            'dingalings': relationship(Dingaling, lazy='joined')})
        mapper(Dingaling, dingalings, properties={
            'address_id': deferred(dingalings.c.address_id)})
        sess.expunge_all()

        def go():
            u = sess.query(User).get(8)
            eq_(User(id=8,
                     addresses=[Address(id=2, dingalings=[Dingaling(id=1)]),
                                Address(id=3),
                                Address(id=4)]),
                u)
        self.assert_sql_count(testing.db, go, 1)

    def test_options_pathing(self):
        users, Keyword, orders, items, order_items, \
            Order, Item, User, keywords, item_keywords = (
                self.tables.users,
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
            'orders': relationship(Order, order_by=orders.c.id),  # o2m, m2o
        })
        mapper(Order, orders, properties={
            'items': relationship(
                Item,
                secondary=order_items, order_by=items.c.id),  # m2m
        })
        mapper(Item, items, properties={
            'keywords': relationship(Keyword,
                                     secondary=item_keywords,
                                     order_by=keywords.c.id)  # m2m
        })
        mapper(Keyword, keywords)

        for opt, count in [
            ((
                joinedload(User.orders, Order.items),
            ), 10),
            ((joinedload("orders.items"), ), 10),
            ((
                joinedload(User.orders, ),
                joinedload(User.orders, Order.items),
                joinedload(User.orders, Order.items, Item.keywords),
            ), 1),
            ((
                joinedload(User.orders, Order.items, Item.keywords),
            ), 10),
            ((
                joinedload(User.orders, Order.items),
                joinedload(User.orders, Order.items, Item.keywords),
            ), 5),
        ]:
            sess = create_session()

            def go():
                eq_(
                    sess.query(User).options(*opt).order_by(User.id).all(),
                    self.static.user_item_keyword_result
                )
            self.assert_sql_count(testing.db, go, count)

    def test_disable_dynamic(self):
        """test no joined option on a dynamic."""

        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User)

        mapper(User, users, properties={
            'addresses': relationship(Address, lazy="dynamic")
        })
        mapper(Address, addresses)
        sess = create_session()
        assert_raises_message(
            sa.exc.InvalidRequestError,
            "User.addresses' does not support object "
            "population - eager loading cannot be applied.",
            sess.query(User).options(joinedload(User.addresses)).first,
        )

    def test_many_to_many(self):
        keywords, items, item_keywords, Keyword, Item = (
            self.tables.keywords,
            self.tables.items,
            self.tables.item_keywords,
            self.classes.Keyword,
            self.classes.Item)

        mapper(Keyword, keywords)
        mapper(Item, items, properties=dict(
            keywords=relationship(Keyword, secondary=item_keywords,
                                  lazy='joined', order_by=keywords.c.id)))

        q = create_session().query(Item).order_by(Item.id)

        def go():
            eq_(self.static.item_keyword_result, q.all())
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

    def test_eager_option(self):
        keywords, items, item_keywords, Keyword, Item = (
            self.tables.keywords,
            self.tables.items,
            self.tables.item_keywords,
            self.classes.Keyword,
            self.classes.Item)

        mapper(Keyword, keywords)
        mapper(Item, items, properties=dict(
            keywords=relationship(
                Keyword, secondary=item_keywords, lazy='select',
                order_by=keywords.c.id)))
        q = create_session().query(Item)

        def go():
            eq_(self.static.item_keyword_result[0:2],
                (q.options(
                    joinedload('keywords')
                ).join('keywords').
                    filter(keywords.c.name == 'red')).order_by(Item.id).all())

        self.assert_sql_count(testing.db, go, 1)

    def test_cyclical(self):
        """A circular eager relationship breaks the cycle with a lazy loader"""

        Address, addresses, users, User = (self.classes.Address,
                                           self.tables.addresses,
                                           self.tables.users,
                                           self.classes.User)

        mapper(Address, addresses)
        mapper(User, users, properties=dict(
            addresses=relationship(
                Address, lazy='joined',
                backref=sa.orm.backref('user', lazy='joined'),
                order_by=Address.id)
        ))
        eq_(sa.orm.class_mapper(User).get_property('addresses').lazy, 'joined')
        eq_(sa.orm.class_mapper(Address).get_property('user').lazy, 'joined')

        sess = create_session()
        eq_(
            self.static.user_address_result,
            sess.query(User).order_by(User.id).all())

    def test_double(self):
        """Eager loading with two relationships simultaneously,
            from the same table, using aliases."""

        users, orders, User, Address, Order, addresses = (
            self.tables.users,
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

        mapper(User, users, properties=dict(
            addresses=relationship(
                Address, lazy='joined', order_by=addresses.c.id),
            open_orders=relationship(
                open_mapper,
                primaryjoin=sa.and_(openorders.c.isopen == 1,
                                    users.c.id == openorders.c.user_id),
                lazy='joined', order_by=openorders.c.id),
            closed_orders=relationship(
                closed_mapper,
                primaryjoin=sa.and_(closedorders.c.isopen == 0,
                                    users.c.id == closedorders.c.user_id),
                lazy='joined', order_by=closedorders.c.id)))

        q = create_session().query(User).order_by(User.id)

        def go():
            eq_([
                User(
                    id=7,
                    addresses=[Address(id=1)],
                    open_orders=[Order(id=3)],
                    closed_orders=[Order(id=1), Order(id=5)]
                ),
                User(
                    id=8,
                    addresses=[Address(id=2), Address(id=3), Address(id=4)],
                    open_orders=[],
                    closed_orders=[]
                ),
                User(
                    id=9,
                    addresses=[Address(id=5)],
                    open_orders=[Order(id=4)],
                    closed_orders=[Order(id=2)]
                ),
                User(id=10)

            ], q.all())
        self.assert_sql_count(testing.db, go, 1)

    def test_double_same_mappers(self):
        """Eager loading with two relationships simultaneously,
        from the same table, using aliases."""

        addresses, items, order_items, orders, \
            Item, User, Address, Order, users = (
                self.tables.addresses,
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
            'items': relationship(Item, secondary=order_items, lazy='joined',
                                  order_by=items.c.id)})
        mapper(Item, items)
        mapper(User, users, properties=dict(
            addresses=relationship(
                Address, lazy='joined', order_by=addresses.c.id),
            open_orders=relationship(
                Order,
                primaryjoin=sa.and_(orders.c.isopen == 1,
                                    users.c.id == orders.c.user_id),
                lazy='joined', order_by=orders.c.id),
            closed_orders=relationship(
                Order,
                primaryjoin=sa.and_(orders.c.isopen == 0,
                                    users.c.id == orders.c.user_id),
                lazy='joined', order_by=orders.c.id)))
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
                     open_orders=[],
                     closed_orders=[]),
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
        self.assert_sql_count(testing.db, go, 1)

    def test_no_false_hits(self):
        """Eager loaders don't interpret main table columns as
        part of their eager load."""

        addresses, orders, User, Address, Order, users = (
            self.tables.addresses,
            self.tables.orders,
            self.classes.User,
            self.classes.Address,
            self.classes.Order,
            self.tables.users)

        mapper(User, users, properties={
            'addresses': relationship(Address, lazy='joined'),
            'orders': relationship(Order, lazy='joined')
        })
        mapper(Address, addresses)
        mapper(Order, orders)

        self.allusers = create_session().query(User).all()

        # using a textual select, the columns will be 'id' and 'name'.  the
        # eager loaders have aliases which should not hit on those columns,
        # they should be required to locate only their aliased/fully table
        # qualified column name.
        noeagers = create_session().query(User).\
            from_statement(text("select * from users")).all()
        assert 'orders' not in noeagers[0].__dict__
        assert 'addresses' not in noeagers[0].__dict__

    def test_limit(self):
        """Limit operations combined with lazy-load relationships."""

        users, items, order_items, orders, Item, \
            User, Address, Order, addresses = (
                self.tables.users,
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
            'items': relationship(Item, secondary=order_items, lazy='joined',
                                  order_by=items.c.id)
        })
        mapper(User, users, properties={
            'addresses': relationship(
                mapper(Address, addresses),
                lazy='joined', order_by=addresses.c.id),
            'orders': relationship(Order, lazy='select', order_by=orders.c.id)
        })

        sess = create_session()
        q = sess.query(User)

        result = q.order_by(User.id).limit(2).offset(1).all()
        eq_(self.static.user_all_result[1:3], result)

    def test_distinct(self):
        Address, addresses, users, User = (self.classes.Address,
                                           self.tables.addresses,
                                           self.tables.users,
                                           self.classes.User)

        # this is an involved 3x union of the users table to get a lot of rows.
        # then see if the "distinct" works its way out.  you actually get
        # the same result with or without the distinct, just via less or
        # more rows.
        u2 = users.alias('u2')
        s = sa.union_all(
            u2.select(use_labels=True), u2.select(use_labels=True),
            u2.select(use_labels=True)).alias('u')

        mapper(User, users, properties={
            'addresses': relationship(
                mapper(Address, addresses),
                lazy='joined', order_by=addresses.c.id),
        })

        sess = create_session()
        q = sess.query(User)

        def go():
            result = q.filter(s.c.u2_id == User.id).distinct().\
                order_by(User.id).all()
            eq_(self.static.user_address_result, result)
        self.assert_sql_count(testing.db, go, 1)

    def test_limit_2(self):
        keywords, items, item_keywords, Keyword, Item = (
            self.tables.keywords,
            self.tables.items,
            self.tables.item_keywords,
            self.classes.Keyword,
            self.classes.Item)

        mapper(Keyword, keywords)
        mapper(Item, items, properties=dict(
            keywords=relationship(
                Keyword, secondary=item_keywords,
                lazy='joined', order_by=[keywords.c.id]),
        ))

        sess = create_session()
        q = sess.query(Item)
        result = q.filter((Item.description == 'item 2') |
                          (Item.description == 'item 5') |
                          (Item.description == 'item 3')).\
            order_by(Item.id).limit(2).all()

        eq_(self.static.item_keyword_result[1:3], result)

    def test_limit_3(self):
        """test that the ORDER BY is propagated from the inner
        select to the outer select, when using the
        'wrapped' select statement resulting from the combination of
        eager loading and limit/offset clauses."""

        addresses, items, order_items, orders, \
            Item, User, Address, Order, users = (
                self.tables.addresses,
                self.tables.items,
                self.tables.order_items,
                self.tables.orders,
                self.classes.Item,
                self.classes.User,
                self.classes.Address,
                self.classes.Order,
                self.tables.users)

        mapper(Item, items)
        mapper(Order, orders, properties=dict(
            items=relationship(Item, secondary=order_items, lazy='joined')
        ))

        mapper(Address, addresses)
        mapper(User, users, properties=dict(
            addresses=relationship(
                Address, lazy='joined', order_by=addresses.c.id),
            orders=relationship(Order, lazy='joined', order_by=orders.c.id),
        ))
        sess = create_session()

        q = sess.query(User)

        if not testing.against('mssql'):
            result = q.join('orders').order_by(
                Order.user_id.desc()).limit(2).offset(1)
            eq_([
                User(id=9,
                     orders=[Order(id=2), Order(id=4)],
                     addresses=[Address(id=5)]
                     ),
                User(id=7,
                     orders=[Order(id=1), Order(id=3), Order(id=5)],
                     addresses=[Address(id=1)]
                     )
            ], result.all())

        result = q.join('addresses').order_by(
            Address.email_address.desc()).limit(1).offset(0)
        eq_([
            User(id=7,
                 orders=[Order(id=1), Order(id=3), Order(id=5)],
                 addresses=[Address(id=1)]
                 )
        ], result.all())

    def test_limit_4(self):
        User, Order, addresses, users, orders = (self.classes.User,
                                                 self.classes.Order,
                                                 self.tables.addresses,
                                                 self.tables.users,
                                                 self.tables.orders)

        # tests the LIMIT/OFFSET aliasing on a mapper
        # against a select.   original issue from ticket #904
        sel = sa.select([users, addresses.c.email_address],
                        users.c.id == addresses.c.user_id).alias('useralias')
        mapper(User, sel, properties={
            'orders': relationship(
                Order, primaryjoin=sel.c.id == orders.c.user_id,
                lazy='joined', order_by=orders.c.id)
        })
        mapper(Order, orders)

        sess = create_session()
        eq_(sess.query(User).first(),
            User(name='jack', orders=[
                Order(
                    address_id=1,
                    description='order 1',
                    isopen=0,
                    user_id=7,
                    id=1),
                Order(
                    address_id=1,
                    description='order 3',
                    isopen=1,
                    user_id=7,
                    id=3),
                Order(
                    address_id=None, description='order 5', isopen=0,
                    user_id=7, id=5)],
                 email_address='jack@bean.com', id=7)
            )

    def test_useget_cancels_eager(self):
        """test that a one to many lazyload cancels the unnecessary
        eager many-to-one join on the other side."""

        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User)

        mapper(User, users)
        mapper(Address, addresses, properties={
            'user': relationship(User, lazy='joined', backref='addresses')
        })

        sess = create_session()
        u1 = sess.query(User).filter(User.id == 8).one()

        def go():
            eq_(u1.addresses[0].user, u1)
        self.assert_sql_execution(
            testing.db, go,
            CompiledSQL(
                "SELECT addresses.id AS addresses_id, addresses.user_id AS "
                "addresses_user_id, addresses.email_address AS "
                "addresses_email_address FROM addresses WHERE :param_1 = "
                "addresses.user_id",
                {'param_1': 8})
        )

    def test_useget_cancels_eager_propagated_present(self):
        """test that a one to many lazyload cancels the unnecessary
        eager many-to-one join on the other side, even when a propagated
        option is present."""

        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User)

        mapper(User, users)
        mapper(Address, addresses, properties={
            'user': relationship(User, lazy='joined', backref='addresses')
        })

        from sqlalchemy.orm.interfaces import MapperOption

        class MyBogusOption(MapperOption):
            propagate_to_loaders = True

        sess = create_session()
        u1 = sess.query(User).options(
            MyBogusOption()).filter(User.id == 8).one()

        def go():
            eq_(u1.addresses[0].user, u1)
        self.assert_sql_execution(
            testing.db, go,
            CompiledSQL(
                "SELECT addresses.id AS addresses_id, addresses.user_id AS "
                "addresses_user_id, addresses.email_address AS "
                "addresses_email_address FROM addresses WHERE :param_1 = "
                "addresses.user_id",
                {'param_1': 8})
        )

    def test_manytoone_limit(self):
        """test that the subquery wrapping only occurs with
        limit/offset and m2m or o2m joins present."""

        users, items, order_items, Order, Item, User, \
            Address, orders, addresses = (
                self.tables.users,
                self.tables.items,
                self.tables.order_items,
                self.classes.Order,
                self.classes.Item,
                self.classes.User,
                self.classes.Address,
                self.tables.orders,
                self.tables.addresses)

        mapper(User, users, properties=odict(
            orders=relationship(Order, backref='user')
        ))
        mapper(Order, orders, properties=odict([
            ('items', relationship(Item, secondary=order_items,
                                   backref='orders')),
            ('address', relationship(Address))
        ]))
        mapper(Address, addresses)
        mapper(Item, items)

        sess = create_session()

        self.assert_compile(
            sess.query(User).options(joinedload(User.orders)).limit(10),
            "SELECT anon_1.users_id AS anon_1_users_id, anon_1.users_name "
            "AS anon_1_users_name, orders_1.id AS orders_1_id, "
            "orders_1.user_id AS orders_1_user_id, orders_1.address_id "
            "AS orders_1_address_id, orders_1.description AS "
            "orders_1_description, orders_1.isopen AS orders_1_isopen "
            "FROM (SELECT users.id AS users_id, users.name AS users_name "
            "FROM users "
            "LIMIT :param_1) AS anon_1 LEFT OUTER JOIN orders AS "
            "orders_1 ON anon_1.users_id = orders_1.user_id",
            {'param_1': 10}
        )

        self.assert_compile(
            sess.query(Order).options(joinedload(Order.user)).limit(10),
            "SELECT orders.id AS orders_id, orders.user_id AS orders_user_id, "
            "orders.address_id AS "
            "orders_address_id, orders.description AS orders_description, "
            "orders.isopen AS orders_isopen, "
            "users_1.id AS users_1_id, users_1.name AS users_1_name "
            "FROM orders LEFT OUTER JOIN users AS "
            "users_1 ON users_1.id = orders.user_id LIMIT :param_1",
            {'param_1': 10}
        )

        self.assert_compile(
            sess.query(Order).options(
                joinedload(Order.user, innerjoin=True)).limit(10),
            "SELECT orders.id AS orders_id, orders.user_id AS orders_user_id, "
            "orders.address_id AS "
            "orders_address_id, orders.description AS orders_description, "
            "orders.isopen AS orders_isopen, "
            "users_1.id AS users_1_id, users_1.name AS users_1_name "
            "FROM orders JOIN users AS "
            "users_1 ON users_1.id = orders.user_id LIMIT :param_1",
            {'param_1': 10}
        )

        self.assert_compile(
            sess.query(User).options(
                joinedload_all("orders.address")).limit(10),
            "SELECT anon_1.users_id AS anon_1_users_id, "
            "anon_1.users_name AS anon_1_users_name, "
            "addresses_1.id AS addresses_1_id, "
            "addresses_1.user_id AS addresses_1_user_id, "
            "addresses_1.email_address AS addresses_1_email_address, "
            "orders_1.id AS orders_1_id, "
            "orders_1.user_id AS orders_1_user_id, "
            "orders_1.address_id AS orders_1_address_id, "
            "orders_1.description AS orders_1_description, "
            "orders_1.isopen AS orders_1_isopen FROM "
            "(SELECT users.id AS users_id, users.name AS users_name "
            "FROM users LIMIT :param_1) AS anon_1 "
            "LEFT OUTER JOIN orders AS orders_1 "
            "ON anon_1.users_id = orders_1.user_id LEFT OUTER JOIN "
            "addresses AS addresses_1 ON addresses_1.id = orders_1.address_id",
            {'param_1': 10}
        )

        self.assert_compile(
            sess.query(User).options(joinedload_all("orders.items"),
                                     joinedload("orders.address")),
            "SELECT users.id AS users_id, users.name AS users_name, "
            "items_1.id AS items_1_id, "
            "items_1.description AS items_1_description, "
            "addresses_1.id AS addresses_1_id, "
            "addresses_1.user_id AS addresses_1_user_id, "
            "addresses_1.email_address AS "
            "addresses_1_email_address, orders_1.id AS orders_1_id, "
            "orders_1.user_id AS "
            "orders_1_user_id, orders_1.address_id AS orders_1_address_id, "
            "orders_1.description "
            "AS orders_1_description, orders_1.isopen AS orders_1_isopen "
            "FROM users LEFT OUTER JOIN orders AS orders_1 "
            "ON users.id = orders_1.user_id "
            "LEFT OUTER JOIN (order_items AS order_items_1 "
            "JOIN items AS items_1 ON items_1.id = order_items_1.item_id) "
            "ON orders_1.id = order_items_1.order_id "
            "LEFT OUTER JOIN addresses AS addresses_1 "
            "ON addresses_1.id = orders_1.address_id"
        )

        self.assert_compile(
            sess.query(User).options(
                joinedload("orders"),
                joinedload(
                    "orders.address",
                    innerjoin=True)).limit(10),
            "SELECT anon_1.users_id AS anon_1_users_id, anon_1.users_name "
            "AS anon_1_users_name, addresses_1.id AS addresses_1_id, "
            "addresses_1.user_id AS addresses_1_user_id, "
            "addresses_1.email_address AS addresses_1_email_address, "
            "orders_1.id AS orders_1_id, orders_1.user_id AS "
            "orders_1_user_id, orders_1.address_id AS orders_1_address_id, "
            "orders_1.description AS orders_1_description, "
            "orders_1.isopen AS orders_1_isopen "
            "FROM (SELECT users.id AS users_id, users.name AS users_name "
            "FROM users"
            " LIMIT :param_1) AS anon_1 LEFT OUTER JOIN "
            "(orders AS orders_1 JOIN addresses AS addresses_1 "
            "ON addresses_1.id = orders_1.address_id) ON "
            "anon_1.users_id = orders_1.user_id",
            {'param_1': 10}
        )

        self.assert_compile(
            sess.query(User).options(
                joinedload("orders", innerjoin=True),
                joinedload("orders.address", innerjoin=True)).limit(10),
            "SELECT anon_1.users_id AS anon_1_users_id, "
            "anon_1.users_name AS anon_1_users_name, "
            "addresses_1.id AS addresses_1_id, "
            "addresses_1.user_id AS addresses_1_user_id, "
            "addresses_1.email_address AS addresses_1_email_address, "
            "orders_1.id AS orders_1_id, "
            "orders_1.user_id AS orders_1_user_id, "
            "orders_1.address_id AS orders_1_address_id, "
            "orders_1.description AS orders_1_description, "
            "orders_1.isopen AS orders_1_isopen "
            "FROM (SELECT users.id AS users_id, users.name AS users_name "
            "FROM users "
            "LIMIT :param_1) AS anon_1 JOIN orders "
            "AS orders_1 ON anon_1.users_id = "
            "orders_1.user_id JOIN addresses AS addresses_1 "
            "ON addresses_1.id = orders_1.address_id",
            {'param_1': 10}
        )

    def test_one_to_many_scalar(self):
        Address, addresses, users, User = (self.classes.Address,
                                           self.tables.addresses,
                                           self.tables.users,
                                           self.classes.User)

        mapper(User, users, properties=dict(
            address=relationship(mapper(Address, addresses),
                                 lazy='joined', uselist=False)
        ))
        q = create_session().query(User)

        def go():
            result = q.filter(users.c.id == 7).all()
            eq_([User(id=7, address=Address(id=1))], result)
        self.assert_sql_count(testing.db, go, 1)

    def test_one_to_many_scalar_subq_wrapping(self):
        Address, addresses, users, User = (self.classes.Address,
                                           self.tables.addresses,
                                           self.tables.users,
                                           self.classes.User)

        mapper(User, users, properties=dict(
            address=relationship(mapper(Address, addresses),
                                 lazy='joined', uselist=False)
        ))
        q = create_session().query(User)
        q = q.filter(users.c.id == 7).limit(1)

        self.assert_compile(
            q,
            "SELECT users.id AS users_id, users.name AS users_name, "
            "addresses_1.id AS addresses_1_id, "
            "addresses_1.user_id AS addresses_1_user_id, "
            "addresses_1.email_address AS addresses_1_email_address "
            "FROM users LEFT OUTER JOIN addresses AS addresses_1 "
            "ON users.id = addresses_1.user_id "
            "WHERE users.id = :id_1 "
            "LIMIT :param_1",
            checkparams={'id_1': 7, 'param_1': 1}
        )

    def test_many_to_one(self):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User)

        mapper(Address, addresses, properties=dict(
            user=relationship(mapper(User, users), lazy='joined')
        ))
        sess = create_session()
        q = sess.query(Address)

        def go():
            a = q.filter(addresses.c.id == 1).one()
            is_not_(a.user, None)
            u1 = sess.query(User).get(7)
            is_(a.user, u1)
        self.assert_sql_count(testing.db, go, 1)

    def test_many_to_one_null(self):
        """test that a many-to-one eager load which loads None does
        not later trigger a lazy load.

        """

        Order, Address, addresses, orders = (self.classes.Order,
                                             self.classes.Address,
                                             self.tables.addresses,
                                             self.tables.orders)

        # use a primaryjoin intended to defeat SA's usage of
        # query.get() for a many-to-one lazyload
        mapper(Order, orders, properties=dict(
            address=relationship(
                mapper(Address, addresses),
                primaryjoin=and_(
                    addresses.c.id == orders.c.address_id,
                    addresses.c.email_address != None  # noqa
                ),

                lazy='joined')
        ))
        sess = create_session()

        def go():
            o1 = sess.query(Order).options(
                lazyload('address')).filter(
                Order.id == 5).one()
            eq_(o1.address, None)
        self.assert_sql_count(testing.db, go, 2)

        sess.expunge_all()

        def go():
            o1 = sess.query(Order).filter(Order.id == 5).one()
            eq_(o1.address, None)
        self.assert_sql_count(testing.db, go, 1)

    def test_one_and_many(self):
        """tests eager load for a parent object with a child object that
        contains a many-to-many relationship to a third object."""

        users, items, order_items, orders, Item, User, Order = (
            self.tables.users,
            self.tables.items,
            self.tables.order_items,
            self.tables.orders,
            self.classes.Item,
            self.classes.User,
            self.classes.Order)

        mapper(User, users, properties={
            'orders': relationship(Order, lazy='joined', order_by=orders.c.id)
        })
        mapper(Item, items)
        mapper(Order, orders, properties=dict(
            items=relationship(
                Item,
                secondary=order_items,
                lazy='joined',
                order_by=items.c.id)
        ))

        q = create_session().query(User)

        result = q.filter(text("users.id in (7, 8, 9)")).order_by(
            text("users.id"))

        def go():
            eq_(self.static.user_order_result[0:3], result.all())
        self.assert_sql_count(testing.db, go, 1)

    def test_double_with_aggregate(self):
        User, users, orders, Order = (self.classes.User,
                                      self.tables.users,
                                      self.tables.orders,
                                      self.classes.Order)

        max_orders_by_user = sa.select([
            sa.func.max(orders.c.id).label('order_id')],
            group_by=[orders.c.user_id]
        ).alias('max_orders_by_user')

        max_orders = orders.select(
            orders.c.id == max_orders_by_user.c.order_id).\
            alias('max_orders')

        mapper(Order, orders)
        mapper(User, users, properties={
               'orders': relationship(Order, backref='user', lazy='joined',
                                      order_by=orders.c.id),
               'max_order': relationship(
                   mapper(Order, max_orders, non_primary=True),
                   lazy='joined', uselist=False)
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
                User(id=9, orders=[Order(id=2), Order(id=4)],
                     max_order=Order(id=4)
                     ),
                User(id=10),
            ], q.order_by(User.id).all())
        self.assert_sql_count(testing.db, go, 1)

    def test_uselist_false_warning(self):
        """test that multiple rows received by a
        uselist=False raises a warning."""

        User, users, orders, Order = (self.classes.User,
                                      self.tables.users,
                                      self.tables.orders,
                                      self.classes.Order)

        mapper(User, users, properties={
            'order': relationship(Order, uselist=False)
        })
        mapper(Order, orders)
        s = create_session()
        assert_raises(sa.exc.SAWarning,
                      s.query(User).options(joinedload(User.order)).all)

    def test_wide(self):
        users, items, order_items, Order, Item, \
            User, Address, orders, addresses = (
                self.tables.users,
                self.tables.items,
                self.tables.order_items,
                self.classes.Order,
                self.classes.Item,
                self.classes.User,
                self.classes.Address,
                self.tables.orders,
                self.tables.addresses)

        mapper(
            Order, orders, properties={
                'items': relationship(
                    Item, secondary=order_items, lazy='joined',
                    order_by=items.c.id)})
        mapper(Item, items)
        mapper(User, users, properties=dict(
            addresses=relationship(
                mapper(
                    Address,
                    addresses),
                lazy=False,
                order_by=addresses.c.id),
            orders=relationship(Order, lazy=False, order_by=orders.c.id),
        ))
        q = create_session().query(User)

        def go():
            eq_(self.static.user_all_result, q.order_by(User.id).all())
        self.assert_sql_count(testing.db, go, 1)

    def test_against_select(self):
        """test eager loading of a mapper which is against a select"""

        users, items, order_items, orders, Item, User, Order = (
            self.tables.users,
            self.tables.items,
            self.tables.order_items,
            self.tables.orders,
            self.classes.Item,
            self.classes.User,
            self.classes.Order)

        s = sa.select([orders], orders.c.isopen == 1).alias('openorders')

        mapper(Order, s, properties={
            'user': relationship(User, lazy='joined')
        })
        mapper(User, users)
        mapper(Item, items)

        q = create_session().query(Order)
        eq_([
            Order(id=3, user=User(id=7)),
            Order(id=4, user=User(id=9))
        ], q.all())

        q = q.select_from(s.join(order_items).join(items)).filter(
            ~Item.id.in_([1, 2, 5]))
        eq_([
            Order(id=3, user=User(id=7)),
        ], q.all())

    def test_aliasing(self):
        """test that eager loading uses aliases to insulate the eager
        load from regular criterion against those tables."""

        Address, addresses, users, User = (self.classes.Address,
                                           self.tables.addresses,
                                           self.tables.users,
                                           self.classes.User)

        mapper(User, users, properties=dict(
            addresses=relationship(mapper(Address, addresses),
                                   lazy='joined', order_by=addresses.c.id)
        ))
        q = create_session().query(User)
        result = q.filter(addresses.c.email_address == 'ed@lala.com').filter(
            Address.user_id == User.id).order_by(User.id)
        eq_(self.static.user_address_result[1:2], result.all())

    def test_inner_join(self):
        Address, addresses, users, User = (self.classes.Address,
                                           self.tables.addresses,
                                           self.tables.users,
                                           self.classes.User)

        mapper(User, users, properties=dict(
            addresses=relationship(mapper(Address, addresses), lazy='joined',
                                   innerjoin=True, order_by=addresses.c.id)
        ))
        sess = create_session()
        eq_(
            [User(id=7, addresses=[Address(id=1)]),
             User(id=8,
                  addresses=[Address(id=2, email_address='ed@wood.com'),
                             Address(id=3, email_address='ed@bettyboop.com'),
                             Address(id=4, email_address='ed@lala.com'), ]),
             User(id=9, addresses=[Address(id=5)])], sess.query(User).all()
        )
        self.assert_compile(
            sess.query(User),
            "SELECT users.id AS users_id, users.name AS users_name, "
            "addresses_1.id AS addresses_1_id, "
            "addresses_1.user_id AS addresses_1_user_id, "
            "addresses_1.email_address AS addresses_1_email_address "
            "FROM users JOIN "
            "addresses AS addresses_1 ON users.id = addresses_1.user_id "
            "ORDER BY addresses_1.id")

    def test_inner_join_unnested_chaining_options(self):
        users, items, order_items, Order, Item, User, orders = (
            self.tables.users,
            self.tables.items,
            self.tables.order_items,
            self.classes.Order,
            self.classes.Item,
            self.classes.User,
            self.tables.orders)

        mapper(User, users, properties=dict(
            orders=relationship(Order, innerjoin="unnested",
                                lazy=False)
        ))
        mapper(Order, orders, properties=dict(
            items=relationship(Item, secondary=order_items, lazy=False,
                               innerjoin="unnested")
        ))
        mapper(Item, items)

        sess = create_session()
        self.assert_compile(
            sess.query(User),
            "SELECT users.id AS users_id, users.name AS users_name, "
            "items_1.id AS "
            "items_1_id, items_1.description AS items_1_description, "
            "orders_1.id AS "
            "orders_1_id, orders_1.user_id AS orders_1_user_id, "
            "orders_1.address_id AS "
            "orders_1_address_id, orders_1.description "
            "AS orders_1_description, "
            "orders_1.isopen AS orders_1_isopen FROM users "
            "JOIN orders AS orders_1 ON "
            "users.id = orders_1.user_id JOIN order_items AS order_items_1 "
            "ON orders_1.id = "
            "order_items_1.order_id JOIN items AS items_1 ON items_1.id = "
            "order_items_1.item_id"
        )

        self.assert_compile(
            sess.query(User).options(joinedload(User.orders, innerjoin=False)),
            "SELECT users.id AS users_id, users.name AS users_name, "
            "items_1.id AS "
            "items_1_id, items_1.description AS items_1_description, "
            "orders_1.id AS "
            "orders_1_id, orders_1.user_id AS orders_1_user_id, "
            "orders_1.address_id AS "
            "orders_1_address_id, orders_1.description "
            "AS orders_1_description, "
            "orders_1.isopen AS orders_1_isopen "
            "FROM users LEFT OUTER JOIN orders AS orders_1 "
            "ON users.id = orders_1.user_id "
            "LEFT OUTER JOIN (order_items AS order_items_1 "
            "JOIN items AS items_1 ON items_1.id = order_items_1.item_id) "
            "ON orders_1.id = order_items_1.order_id"
        )

        self.assert_compile(
            sess.query(User).options(
                joinedload(
                    User.orders,
                    Order.items,
                    innerjoin=False)),
            "SELECT users.id AS users_id, users.name AS users_name, "
            "items_1.id AS "
            "items_1_id, items_1.description AS items_1_description, "
            "orders_1.id AS "
            "orders_1_id, orders_1.user_id AS orders_1_user_id, "
            "orders_1.address_id AS "
            "orders_1_address_id, "
            "orders_1.description AS orders_1_description, "
            "orders_1.isopen AS orders_1_isopen "
            "FROM users JOIN orders AS orders_1 ON "
            "users.id = orders_1.user_id "
            "LEFT OUTER JOIN (order_items AS order_items_1 "
            "JOIN items AS items_1 ON items_1.id = order_items_1.item_id) "
            "ON orders_1.id = order_items_1.order_id"

        )

    def test_inner_join_nested_chaining_negative_options(self):
        users, items, order_items, Order, Item, User, orders = (
            self.tables.users,
            self.tables.items,
            self.tables.order_items,
            self.classes.Order,
            self.classes.Item,
            self.classes.User,
            self.tables.orders)

        mapper(User, users, properties=dict(
            orders=relationship(Order, innerjoin=True,
                                lazy=False, order_by=orders.c.id)
        ))
        mapper(Order, orders, properties=dict(
            items=relationship(Item, secondary=order_items, lazy=False,
                               innerjoin=True, order_by=items.c.id)
        ))
        mapper(Item, items)

        sess = create_session()
        self.assert_compile(
            sess.query(User),
            "SELECT users.id AS users_id, users.name AS users_name, "
            "items_1.id AS "
            "items_1_id, items_1.description AS items_1_description, "
            "orders_1.id AS "
            "orders_1_id, orders_1.user_id AS orders_1_user_id, "
            "orders_1.address_id AS "
            "orders_1_address_id, orders_1.description "
            "AS orders_1_description, "
            "orders_1.isopen AS orders_1_isopen FROM users "
            "JOIN orders AS orders_1 ON "
            "users.id = orders_1.user_id JOIN order_items "
            "AS order_items_1 ON orders_1.id = "
            "order_items_1.order_id JOIN items AS items_1 ON items_1.id = "
            "order_items_1.item_id ORDER BY orders_1.id, items_1.id"
        )

        q = sess.query(User).options(joinedload(User.orders, innerjoin=False))
        self.assert_compile(
            q,
            "SELECT users.id AS users_id, users.name AS users_name, "
            "items_1.id AS "
            "items_1_id, items_1.description AS items_1_description, "
            "orders_1.id AS "
            "orders_1_id, orders_1.user_id AS orders_1_user_id, "
            "orders_1.address_id AS "
            "orders_1_address_id, orders_1.description "
            "AS orders_1_description, "
            "orders_1.isopen AS orders_1_isopen "
            "FROM users LEFT OUTER JOIN "
            "(orders AS orders_1 JOIN order_items AS order_items_1 "
            "ON orders_1.id = order_items_1.order_id "
            "JOIN items AS items_1 ON items_1.id = order_items_1.item_id) "
            "ON users.id = orders_1.user_id ORDER BY orders_1.id, items_1.id"
        )

        eq_(
            [
                User(id=7,
                     orders=[
                         Order(
                             id=1, items=[
                                 Item(
                                     id=1), Item(
                                     id=2), Item(
                                     id=3)]),
                         Order(
                             id=3, items=[
                                 Item(
                                     id=3), Item(
                                     id=4), Item(
                                     id=5)]),
                         Order(id=5, items=[Item(id=5)])]),
                User(id=8, orders=[]),
                User(id=9, orders=[
                    Order(id=2, items=[Item(id=1), Item(id=2), Item(id=3)]),
                    Order(id=4, items=[Item(id=1), Item(id=5)])
                ]
                ),
                User(id=10, orders=[])
            ],
            q.order_by(User.id).all()
        )

        self.assert_compile(
            sess.query(User).options(
                joinedload(
                    User.orders,
                    Order.items,
                    innerjoin=False)),
            "SELECT users.id AS users_id, users.name AS users_name, "
            "items_1.id AS "
            "items_1_id, items_1.description AS items_1_description, "
            "orders_1.id AS "
            "orders_1_id, orders_1.user_id AS orders_1_user_id, "
            "orders_1.address_id AS "
            "orders_1_address_id, orders_1.description AS "
            "orders_1_description, "
            "orders_1.isopen AS orders_1_isopen "
            "FROM users JOIN orders AS orders_1 ON users.id = "
            "orders_1.user_id "
            "LEFT OUTER JOIN (order_items AS order_items_1 "
            "JOIN items AS items_1 ON items_1.id = order_items_1.item_id) "
            "ON orders_1.id = order_items_1.order_id ORDER BY "
            "orders_1.id, items_1.id"

        )

    def test_inner_join_nested_chaining_positive_options(self):
        users, items, order_items, Order, Item, User, orders = (
            self.tables.users,
            self.tables.items,
            self.tables.order_items,
            self.classes.Order,
            self.classes.Item,
            self.classes.User,
            self.tables.orders)

        mapper(User, users, properties=dict(
            orders=relationship(Order, order_by=orders.c.id)
        ))
        mapper(Order, orders, properties=dict(
            items=relationship(
                Item,
                secondary=order_items,
                order_by=items.c.id)
        ))
        mapper(Item, items)

        sess = create_session()
        q = sess.query(User).options(
            joinedload("orders", innerjoin=False).
            joinedload("items", innerjoin=True)
        )

        self.assert_compile(
            q,
            "SELECT users.id AS users_id, users.name AS users_name, "
            "items_1.id AS items_1_id, items_1.description "
            "AS items_1_description, "
            "orders_1.id AS orders_1_id, orders_1.user_id "
            "AS orders_1_user_id, "
            "orders_1.address_id AS orders_1_address_id, "
            "orders_1.description AS "
            "orders_1_description, orders_1.isopen AS orders_1_isopen "
            "FROM users LEFT OUTER JOIN (orders AS orders_1 "
            "JOIN order_items AS "
            "order_items_1 ON orders_1.id = order_items_1.order_id "
            "JOIN items AS "
            "items_1 ON items_1.id = order_items_1.item_id) "
            "ON users.id = orders_1.user_id "
            "ORDER BY orders_1.id, items_1.id"
        )

        eq_(
            [
                User(id=7,
                     orders=[
                         Order(
                             id=1, items=[
                                 Item(
                                     id=1), Item(
                                     id=2), Item(
                                     id=3)]),
                         Order(
                             id=3, items=[
                                 Item(
                                     id=3), Item(
                                     id=4), Item(
                                     id=5)]),
                         Order(id=5, items=[Item(id=5)])]),
                User(id=8, orders=[]),
                User(id=9, orders=[
                    Order(id=2, items=[Item(id=1), Item(id=2), Item(id=3)]),
                    Order(id=4, items=[Item(id=1), Item(id=5)])
                ]
                ),
                User(id=10, orders=[])
            ],
            q.order_by(User.id).all()
        )

    def test_unnested_outerjoin_propagation_only_on_correct_path(self):
        # test #3131

        User, users = self.classes.User, self.tables.users
        Order, orders = self.classes.Order, self.tables.orders
        Address, addresses = self.classes.Address, self.tables.addresses

        mapper(User, users, properties=odict([
            ('orders', relationship(Order)),
            ('addresses', relationship(Address))
        ]))
        mapper(Order, orders)
        mapper(Address, addresses)

        sess = create_session()
        q = sess.query(User).options(
            joinedload("orders"),
            joinedload("addresses", innerjoin="unnested"),
        )

        self.assert_compile(
            q,
            "SELECT users.id AS users_id, users.name AS users_name, "
            "orders_1.id AS orders_1_id, "
            "orders_1.user_id AS orders_1_user_id, "
            "orders_1.address_id AS orders_1_address_id, "
            "orders_1.description AS orders_1_description, "
            "orders_1.isopen AS orders_1_isopen, "
            "addresses_1.id AS addresses_1_id, "
            "addresses_1.user_id AS addresses_1_user_id, "
            "addresses_1.email_address AS addresses_1_email_address "
            "FROM users LEFT OUTER JOIN orders AS orders_1 "
            "ON users.id = orders_1.user_id JOIN addresses AS addresses_1 "
            "ON users.id = addresses_1.user_id"
        )

    def test_nested_outerjoin_propagation_only_on_correct_path(self):
        # test #3131

        User, users = self.classes.User, self.tables.users
        Order, orders = self.classes.Order, self.tables.orders
        Address, addresses = self.classes.Address, self.tables.addresses

        mapper(User, users, properties=odict([
            ('orders', relationship(Order)),
            ('addresses', relationship(Address))
        ]))
        mapper(Order, orders)
        mapper(Address, addresses)

        sess = create_session()
        q = sess.query(User).options(
            joinedload("orders"),
            joinedload("addresses", innerjoin=True),
        )

        self.assert_compile(
            q,
            "SELECT users.id AS users_id, users.name AS users_name, "
            "orders_1.id AS orders_1_id, "
            "orders_1.user_id AS orders_1_user_id, "
            "orders_1.address_id AS orders_1_address_id, "
            "orders_1.description AS orders_1_description, "
            "orders_1.isopen AS orders_1_isopen, "
            "addresses_1.id AS addresses_1_id, "
            "addresses_1.user_id AS addresses_1_user_id, "
            "addresses_1.email_address AS addresses_1_email_address "
            "FROM users LEFT OUTER JOIN orders AS orders_1 "
            "ON users.id = orders_1.user_id JOIN addresses AS addresses_1 "
            "ON users.id = addresses_1.user_id"
        )

    def test_catch_the_right_target(self):
        # test eager join chaining to the "nested" join on the left,
        # a new feature as of [ticket:2369]

        users, Keyword, orders, items, order_items, Order, Item, \
            User, keywords, item_keywords = (
                self.tables.users,
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
            'orders': relationship(Order, backref='user'),  # o2m, m2o
        })
        mapper(Order, orders, properties={
            'items': relationship(Item, secondary=order_items,
                                  order_by=items.c.id),  # m2m
        })
        mapper(Item, items, properties={
            'keywords': relationship(Keyword, secondary=item_keywords,
                                     order_by=keywords.c.id)  # m2m
        })
        mapper(Keyword, keywords)

        sess = create_session()
        q = sess.query(User).join(User.orders).join(Order.items).\
            options(joinedload_all("orders.items.keywords"))

        # here, the eager join for keywords can catch onto
        # join(Order.items) or the nested (orders LEFT OUTER JOIN items),
        # it should catch the latter
        self.assert_compile(
            q,
            "SELECT users.id AS users_id, users.name AS users_name, "
            "keywords_1.id AS keywords_1_id, keywords_1.name "
            "AS keywords_1_name, "
            "items_1.id AS items_1_id, items_1.description AS "
            "items_1_description, "
            "orders_1.id AS orders_1_id, orders_1.user_id AS "
            "orders_1_user_id, "
            "orders_1.address_id AS orders_1_address_id, "
            "orders_1.description AS orders_1_description, "
            "orders_1.isopen AS orders_1_isopen "
            "FROM users JOIN orders ON users.id = orders.user_id "
            "JOIN order_items AS order_items_1 ON orders.id = "
            "order_items_1.order_id "
            "JOIN items ON items.id = order_items_1.item_id "
            "LEFT OUTER JOIN orders AS orders_1 ON users.id = "
            "orders_1.user_id "
            "LEFT OUTER JOIN (order_items AS order_items_2 "
            "JOIN items AS items_1 ON items_1.id = order_items_2.item_id) "
            "ON orders_1.id = order_items_2.order_id "
            "LEFT OUTER JOIN (item_keywords AS item_keywords_1 "
            "JOIN keywords AS keywords_1 ON keywords_1.id = "
            "item_keywords_1.keyword_id) "
            "ON items_1.id = item_keywords_1.item_id "
            "ORDER BY items_1.id, keywords_1.id"
        )

    def test_inner_join_unnested_chaining_fixed(self):
        users, items, order_items, Order, Item, User, orders = (
            self.tables.users,
            self.tables.items,
            self.tables.order_items,
            self.classes.Order,
            self.classes.Item,
            self.classes.User,
            self.tables.orders)

        mapper(User, users, properties=dict(
            orders=relationship(Order, lazy=False)
        ))
        mapper(Order, orders, properties=dict(
            items=relationship(Item, secondary=order_items, lazy=False,
                               innerjoin="unnested")
        ))
        mapper(Item, items)

        sess = create_session()

        # joining from user, its all LEFT OUTER JOINs
        self.assert_compile(
            sess.query(User),
            "SELECT users.id AS users_id, users.name AS users_name, "
            "items_1.id AS "
            "items_1_id, items_1.description AS items_1_description, "
            "orders_1.id AS "
            "orders_1_id, orders_1.user_id AS orders_1_user_id, "
            "orders_1.address_id AS "
            "orders_1_address_id, orders_1.description AS "
            "orders_1_description, "
            "orders_1.isopen AS orders_1_isopen FROM users LEFT OUTER JOIN "
            "orders AS orders_1 ON "
            "users.id = orders_1.user_id LEFT OUTER JOIN "
            "(order_items AS order_items_1 JOIN items AS items_1 ON "
            "items_1.id = "
            "order_items_1.item_id) ON orders_1.id = "
            "order_items_1.order_id"
        )

        # joining just from Order, innerjoin=True can be respected
        self.assert_compile(
            sess.query(Order),
            "SELECT orders.id AS orders_id, orders.user_id AS orders_user_id, "
            "orders.address_id AS orders_address_id, orders.description AS "
            "orders_description, orders.isopen AS orders_isopen, items_1.id "
            "AS items_1_id, items_1.description AS items_1_description FROM "
            "orders JOIN order_items AS order_items_1 ON orders.id = "
            "order_items_1.order_id JOIN items AS items_1 ON items_1.id = "
            "order_items_1.item_id"
        )

    def test_inner_join_nested_chaining_fixed(self):
        users, items, order_items, Order, Item, User, orders = (
            self.tables.users,
            self.tables.items,
            self.tables.order_items,
            self.classes.Order,
            self.classes.Item,
            self.classes.User,
            self.tables.orders)

        mapper(User, users, properties=dict(
            orders=relationship(Order, lazy=False)
        ))
        mapper(Order, orders, properties=dict(
            items=relationship(Item, secondary=order_items, lazy=False,
                               innerjoin='nested')
        ))
        mapper(Item, items)

        sess = create_session()

        self.assert_compile(
            sess.query(User),
            "SELECT users.id AS users_id, users.name AS users_name, "
            "items_1.id AS "
            "items_1_id, items_1.description AS items_1_description, "
            "orders_1.id AS "
            "orders_1_id, orders_1.user_id AS orders_1_user_id, "
            "orders_1.address_id AS "
            "orders_1_address_id, orders_1.description AS "
            "orders_1_description, "
            "orders_1.isopen AS orders_1_isopen "
            "FROM users LEFT OUTER JOIN "
            "(orders AS orders_1 JOIN order_items AS order_items_1 "
            "ON orders_1.id = order_items_1.order_id "
            "JOIN items AS items_1 ON items_1.id = order_items_1.item_id) "
            "ON users.id = orders_1.user_id"
        )

    def test_inner_join_options(self):
        users, items, order_items, Order, Item, User, orders = (
            self.tables.users,
            self.tables.items,
            self.tables.order_items,
            self.classes.Order,
            self.classes.Item,
            self.classes.User,
            self.tables.orders)

        mapper(User, users, properties=dict(
            orders=relationship(Order, backref=backref('user', innerjoin=True),
                                order_by=orders.c.id)
        ))
        mapper(Order, orders, properties=dict(
            items=relationship(
                Item,
                secondary=order_items,
                order_by=items.c.id)
        ))
        mapper(Item, items)
        sess = create_session()
        self.assert_compile(
            sess.query(User).options(joinedload(User.orders, innerjoin=True)),
            "SELECT users.id AS users_id, users.name AS users_name, "
            "orders_1.id AS orders_1_id, "
            "orders_1.user_id AS orders_1_user_id, orders_1.address_id AS "
            "orders_1_address_id, "
            "orders_1.description AS orders_1_description, orders_1.isopen "
            "AS orders_1_isopen "
            "FROM users JOIN orders AS orders_1 ON users.id = "
            "orders_1.user_id ORDER BY orders_1.id")

        self.assert_compile(
            sess.query(User).options(
                joinedload_all(User.orders, Order.items, innerjoin=True)),
            "SELECT users.id AS users_id, users.name AS users_name, "
            "items_1.id AS items_1_id, "
            "items_1.description AS items_1_description, "
            "orders_1.id AS orders_1_id, "
            "orders_1.user_id AS orders_1_user_id, orders_1.address_id "
            "AS orders_1_address_id, "
            "orders_1.description AS orders_1_description, orders_1.isopen "
            "AS orders_1_isopen "
            "FROM users JOIN orders AS orders_1 ON users.id = "
            "orders_1.user_id JOIN order_items AS "
            "order_items_1 ON orders_1.id = order_items_1.order_id "
            "JOIN items AS items_1 ON "
            "items_1.id = order_items_1.item_id ORDER BY orders_1.id, "
            "items_1.id")

        def go():
            eq_(
                sess.query(User).options(
                    joinedload(User.orders, innerjoin=True),
                    joinedload(User.orders, Order.items, innerjoin=True)).
                order_by(User.id).all(),

                [User(id=7,
                      orders=[
                          Order(
                              id=1, items=[
                                  Item(
                                      id=1), Item(
                                      id=2), Item(
                                      id=3)]),
                          Order(
                              id=3, items=[
                                  Item(
                                      id=3), Item(
                                      id=4), Item(
                                      id=5)]),
                          Order(id=5, items=[Item(id=5)])]),
                    User(id=9, orders=[
                        Order(
                            id=2, items=[
                                Item(
                                    id=1), Item(
                                    id=2), Item(
                                    id=3)]),
                        Order(id=4, items=[Item(id=1), Item(id=5)])])
                 ]
            )
        self.assert_sql_count(testing.db, go, 1)

        # test that default innerjoin setting is used for options
        self.assert_compile(
            sess.query(Order).options(
                joinedload(
                    Order.user)).filter(
                Order.description == 'foo'),
            "SELECT orders.id AS orders_id, orders.user_id AS orders_user_id, "
            "orders.address_id AS "
            "orders_address_id, orders.description AS orders_description, "
            "orders.isopen AS "
            "orders_isopen, users_1.id AS users_1_id, users_1.name "
            "AS users_1_name "
            "FROM orders JOIN users AS users_1 ON users_1.id = orders.user_id "
            "WHERE orders.description = :description_1"
        )

    def test_propagated_lazyload_wildcard_unbound(self):
        self._test_propagated_lazyload_wildcard(False)

    def test_propagated_lazyload_wildcard_bound(self):
        self._test_propagated_lazyload_wildcard(True)

    def _test_propagated_lazyload_wildcard(self, use_load):
        users, items, order_items, Order, Item, User, orders = (
            self.tables.users,
            self.tables.items,
            self.tables.order_items,
            self.classes.Order,
            self.classes.Item,
            self.classes.User,
            self.tables.orders)

        mapper(User, users, properties=dict(
            orders=relationship(Order, lazy="select")
        ))
        mapper(Order, orders, properties=dict(
            items=relationship(Item, secondary=order_items, lazy="joined")
        ))
        mapper(Item, items)

        sess = create_session()

        if use_load:
            opt = Load(User).defaultload("orders").lazyload("*")
        else:
            opt = defaultload("orders").lazyload("*")

        q = sess.query(User).filter(User.id == 7).options(opt)

        def go():
            for u in q:
                u.orders

        self.sql_eq_(go, [
            ("SELECT users.id AS users_id, users.name AS users_name "
                "FROM users WHERE users.id = :id_1", {"id_1": 7}),
            ("SELECT orders.id AS orders_id, "
             "orders.user_id AS orders_user_id, "
             "orders.address_id AS orders_address_id, "
             "orders.description AS orders_description, "
             "orders.isopen AS orders_isopen FROM orders "
             "WHERE :param_1 = orders.user_id", {"param_1": 7}),
        ])


class InnerJoinSplicingTest(fixtures.MappedTest, testing.AssertsCompiledSQL):
    __dialect__ = 'default'
    __backend__ = True  # exercise hardcore join nesting on backends

    @classmethod
    def define_tables(cls, metadata):
        Table('a', metadata,
              Column('id', Integer, primary_key=True)
              )

        Table('b', metadata,
              Column('id', Integer, primary_key=True),
              Column('a_id', Integer, ForeignKey('a.id')),
              Column('value', String(10)),
              )
        Table('c1', metadata,
              Column('id', Integer, primary_key=True),
              Column('b_id', Integer, ForeignKey('b.id')),
              Column('value', String(10)),
              )
        Table('c2', metadata,
              Column('id', Integer, primary_key=True),
              Column('b_id', Integer, ForeignKey('b.id')),
              Column('value', String(10)),
              )
        Table('d1', metadata,
              Column('id', Integer, primary_key=True),
              Column('c1_id', Integer, ForeignKey('c1.id')),
              Column('value', String(10)),
              )
        Table('d2', metadata,
              Column('id', Integer, primary_key=True),
              Column('c2_id', Integer, ForeignKey('c2.id')),
              Column('value', String(10)),
              )
        Table('e1', metadata,
              Column('id', Integer, primary_key=True),
              Column('d1_id', Integer, ForeignKey('d1.id')),
              Column('value', String(10)),
              )

    @classmethod
    def setup_classes(cls):

        class A(cls.Comparable):
            pass

        class B(cls.Comparable):
            pass

        class C1(cls.Comparable):
            pass

        class C2(cls.Comparable):
            pass

        class D1(cls.Comparable):
            pass

        class D2(cls.Comparable):
            pass

        class E1(cls.Comparable):
            pass

    @classmethod
    def setup_mappers(cls):
        A, B, C1, C2, D1, D2, E1 = (
            cls.classes.A, cls.classes.B, cls.classes.C1,
            cls.classes.C2, cls.classes.D1, cls.classes.D2, cls.classes.E1)
        mapper(A, cls.tables.a, properties={
            'bs': relationship(B)
        })
        mapper(B, cls.tables.b, properties=odict([
            ('c1s', relationship(C1, order_by=cls.tables.c1.c.id)),
            ('c2s', relationship(C2, order_by=cls.tables.c2.c.id))
        ]))
        mapper(C1, cls.tables.c1, properties={
            'd1s': relationship(D1, order_by=cls.tables.d1.c.id)
        })
        mapper(C2, cls.tables.c2, properties={
            'd2s': relationship(D2, order_by=cls.tables.d2.c.id)
        })
        mapper(D1, cls.tables.d1, properties={
            'e1s': relationship(E1, order_by=cls.tables.e1.c.id)
        })
        mapper(D2, cls.tables.d2)
        mapper(E1, cls.tables.e1)

    @classmethod
    def _fixture_data(cls):
        A, B, C1, C2, D1, D2, E1 = (
            cls.classes.A, cls.classes.B, cls.classes.C1,
            cls.classes.C2, cls.classes.D1, cls.classes.D2, cls.classes.E1)
        return [
            A(id=1, bs=[
                B(
                    id=1,
                    c1s=[C1(
                        id=1, value='C11',
                        d1s=[
                            D1(id=1, e1s=[E1(id=1)]), D1(id=2, e1s=[E1(id=2)])
                        ]
                    )
                    ],
                    c2s=[C2(id=1, value='C21', d2s=[D2(id=3)]),
                         C2(id=2, value='C22', d2s=[D2(id=4)])]
                ),
                B(
                    id=2,
                    c1s=[
                        C1(
                            id=4, value='C14',
                            d1s=[D1(
                                id=3, e1s=[
                                    E1(id=3, value='E13'),
                                    E1(id=4, value="E14")
                                ]),
                                D1(id=4, e1s=[E1(id=5)])
                            ]
                        )
                    ],
                    c2s=[C2(id=4, value='C24', d2s=[])]
                ),
            ]),
            A(id=2, bs=[
                B(
                    id=3,
                    c1s=[
                        C1(
                            id=8,
                            d1s=[D1(id=5, value='D15', e1s=[E1(id=6)])]
                        )
                    ],
                    c2s=[C2(id=8, d2s=[D2(id=6, value='D26')])]
                )
            ])
        ]

    @classmethod
    def insert_data(cls):
        s = Session(testing.db)
        s.add_all(cls._fixture_data())
        s.commit()

    def _assert_result(self, query):
        eq_(
            query.all(),
            self._fixture_data()
        )

    def test_nested_innerjoin_propagation_multiple_paths_one(self):
        A, B, C1, C2 = (
            self.classes.A, self.classes.B, self.classes.C1,
            self.classes.C2)

        s = Session()

        q = s.query(A).options(
            joinedload(A.bs, innerjoin=False).
            joinedload(B.c1s, innerjoin=True).
            joinedload(C1.d1s, innerjoin=True),
            defaultload(A.bs).joinedload(B.c2s, innerjoin=True).
            joinedload(C2.d2s, innerjoin=False)
        )
        self.assert_compile(
            q,
            "SELECT a.id AS a_id, d1_1.id AS d1_1_id, "
            "d1_1.c1_id AS d1_1_c1_id, d1_1.value AS d1_1_value, "
            "c1_1.id AS c1_1_id, c1_1.b_id AS c1_1_b_id, "
            "c1_1.value AS c1_1_value, d2_1.id AS d2_1_id, "
            "d2_1.c2_id AS d2_1_c2_id, d2_1.value AS d2_1_value, "
            "c2_1.id AS c2_1_id, c2_1.b_id AS c2_1_b_id, "
            "c2_1.value AS c2_1_value, b_1.id AS b_1_id, "
            "b_1.a_id AS b_1_a_id, b_1.value AS b_1_value "
            "FROM a "
            "LEFT OUTER JOIN "
            "(b AS b_1 JOIN c2 AS c2_1 ON b_1.id = c2_1.b_id "
            "JOIN c1 AS c1_1 ON b_1.id = c1_1.b_id "
            "JOIN d1 AS d1_1 ON c1_1.id = d1_1.c1_id) ON a.id = b_1.a_id "
            "LEFT OUTER JOIN d2 AS d2_1 ON c2_1.id = d2_1.c2_id "
            "ORDER BY c1_1.id, d1_1.id, c2_1.id, d2_1.id"
        )
        self._assert_result(q)

    def test_nested_innerjoin_propagation_multiple_paths_two(self):
        # test #3447
        A = self.classes.A

        s = Session()

        q = s.query(A).options(
            joinedload('bs'),
            joinedload('bs.c2s', innerjoin=True),
            joinedload('bs.c1s', innerjoin=True),
            joinedload('bs.c1s.d1s')
        )
        self.assert_compile(
            q,
            "SELECT a.id AS a_id, d1_1.id AS d1_1_id, "
            "d1_1.c1_id AS d1_1_c1_id, d1_1.value AS d1_1_value, "
            "c1_1.id AS c1_1_id, c1_1.b_id AS c1_1_b_id, "
            "c1_1.value AS c1_1_value, c2_1.id AS c2_1_id, "
            "c2_1.b_id AS c2_1_b_id, c2_1.value AS c2_1_value, "
            "b_1.id AS b_1_id, b_1.a_id AS b_1_a_id, "
            "b_1.value AS b_1_value "
            "FROM a LEFT OUTER JOIN "
            "(b AS b_1 JOIN c2 AS c2_1 ON b_1.id = c2_1.b_id "
            "JOIN c1 AS c1_1 ON b_1.id = c1_1.b_id) ON a.id = b_1.a_id "
            "LEFT OUTER JOIN d1 AS d1_1 ON c1_1.id = d1_1.c1_id "
            "ORDER BY c1_1.id, d1_1.id, c2_1.id"
        )
        self._assert_result(q)

    def test_multiple_splice_points(self):
        A = self.classes.A

        s = Session()

        q = s.query(A).options(
            joinedload('bs', innerjoin=False),
            joinedload('bs.c1s', innerjoin=True),
            joinedload('bs.c2s', innerjoin=True),
            joinedload('bs.c1s.d1s', innerjoin=False),
            joinedload('bs.c2s.d2s'),
            joinedload('bs.c1s.d1s.e1s', innerjoin=True)
        )

        self.assert_compile(
            q,
            "SELECT a.id AS a_id, e1_1.id AS e1_1_id, "
            "e1_1.d1_id AS e1_1_d1_id, e1_1.value AS e1_1_value, "
            "d1_1.id AS d1_1_id, d1_1.c1_id AS d1_1_c1_id, "
            "d1_1.value AS d1_1_value, c1_1.id AS c1_1_id, "
            "c1_1.b_id AS c1_1_b_id, c1_1.value AS c1_1_value, "
            "d2_1.id AS d2_1_id, d2_1.c2_id AS d2_1_c2_id, "
            "d2_1.value AS d2_1_value, c2_1.id AS c2_1_id, "
            "c2_1.b_id AS c2_1_b_id, c2_1.value AS c2_1_value, "
            "b_1.id AS b_1_id, b_1.a_id AS b_1_a_id, b_1.value AS b_1_value "
            "FROM a LEFT OUTER JOIN "
            "(b AS b_1 JOIN c2 AS c2_1 ON b_1.id = c2_1.b_id "
            "JOIN c1 AS c1_1 ON b_1.id = c1_1.b_id) ON a.id = b_1.a_id "
            "LEFT OUTER JOIN ("
            "d1 AS d1_1 JOIN e1 AS e1_1 ON d1_1.id = e1_1.d1_id) "
            "ON c1_1.id = d1_1.c1_id "
            "LEFT OUTER JOIN d2 AS d2_1 ON c2_1.id = d2_1.c2_id "
            "ORDER BY c1_1.id, d1_1.id, e1_1.id, c2_1.id, d2_1.id"
        )
        self._assert_result(q)

    def test_splice_onto_np_mapper(self):
        A = self.classes.A
        B = self.classes.B
        C1 = self.classes.C1
        b_table = self.tables.b
        c1_table = self.tables.c1

        from sqlalchemy import inspect

        weird_selectable = b_table.outerjoin(c1_table)

        b_np = mapper(
            B, weird_selectable, non_primary=True, properties=odict([
                # note we need to make this fixed with lazy=False until
                # [ticket:3348] is resolved
                ('c1s', relationship(C1, lazy=False, innerjoin=True)),
                ('c_id', c1_table.c.id),
                ('b_value', b_table.c.value),
            ])
        )

        a_mapper = inspect(A)
        a_mapper.add_property(
            "bs_np", relationship(b_np)
        )

        s = Session()

        q = s.query(A).options(
            joinedload('bs_np', innerjoin=False)
        )
        self.assert_compile(
            q,
            "SELECT a.id AS a_id, c1_1.id AS c1_1_id, c1_1.b_id AS c1_1_b_id, "
            "c1_1.value AS c1_1_value, c1_2.id AS c1_2_id, "
            "b_1.value AS b_1_value, b_1.id AS b_1_id, "
            "b_1.a_id AS b_1_a_id, c1_2.b_id AS c1_2_b_id, "
            "c1_2.value AS c1_2_value "
            "FROM a LEFT OUTER JOIN "
            "(b AS b_1 LEFT OUTER JOIN c1 AS c1_2 ON b_1.id = c1_2.b_id "
            "JOIN c1 AS c1_1 ON b_1.id = c1_1.b_id) ON a.id = b_1.a_id"
        )


class InnerJoinSplicingWSecondaryTest(
        fixtures.MappedTest, testing.AssertsCompiledSQL):
    __dialect__ = 'default'
    __backend__ = True  # exercise hardcore join nesting on backends

    @classmethod
    def define_tables(cls, metadata):
        Table(
            'a', metadata,
            Column('id', Integer, primary_key=True),
            Column('bid', ForeignKey('b.id'))
        )

        Table(
            'b', metadata,
            Column('id', Integer, primary_key=True),
            Column('cid', ForeignKey('c.id'))
        )

        Table(
            'c', metadata,
            Column('id', Integer, primary_key=True),
        )

        Table('ctod', metadata,
              Column('cid', ForeignKey('c.id'), primary_key=True),
              Column('did', ForeignKey('d.id'), primary_key=True),
              )
        Table('d', metadata,
              Column('id', Integer, primary_key=True),
              )

    @classmethod
    def setup_classes(cls):

        class A(cls.Comparable):
            pass

        class B(cls.Comparable):
            pass

        class C(cls.Comparable):
            pass

        class D(cls.Comparable):
            pass

    @classmethod
    def setup_mappers(cls):
        A, B, C, D = (
            cls.classes.A, cls.classes.B, cls.classes.C,
            cls.classes.D)
        mapper(A, cls.tables.a, properties={
            'b': relationship(B)
        })
        mapper(B, cls.tables.b, properties=odict([
            ('c', relationship(C)),
        ]))
        mapper(C, cls.tables.c, properties=odict([
            ('ds', relationship(D, secondary=cls.tables.ctod,
                                order_by=cls.tables.d.c.id)),
        ]))
        mapper(D, cls.tables.d)

    @classmethod
    def _fixture_data(cls):
        A, B, C, D = (
            cls.classes.A, cls.classes.B, cls.classes.C,
            cls.classes.D)

        d1, d2, d3 = D(id=1), D(id=2), D(id=3)
        return [
            A(
                id=1,
                b=B(
                    id=1,
                    c=C(
                        id=1,
                        ds=[d1, d2]
                    )
                )
            ),
            A(
                id=2,
                b=B(
                    id=2,
                    c=C(
                        id=2,
                        ds=[d2, d3]
                    )
                )
            )
        ]

    @classmethod
    def insert_data(cls):
        s = Session(testing.db)
        s.add_all(cls._fixture_data())
        s.commit()

    def _assert_result(self, query):
        def go():
            eq_(
                query.all(),
                self._fixture_data()
            )

        self.assert_sql_count(
            testing.db,
            go,
            1
        )

    def test_joined_across(self):
        A = self.classes.A

        s = Session()
        q = s.query(A) \
            .options(
                joinedload('b').
                joinedload('c', innerjoin=True).
                joinedload('ds', innerjoin=True))
        self.assert_compile(
            q,
            "SELECT a.id AS a_id, a.bid AS a_bid, d_1.id AS d_1_id, "
            "c_1.id AS c_1_id, b_1.id AS b_1_id, b_1.cid AS b_1_cid "
            "FROM a LEFT OUTER JOIN "
            "(b AS b_1 JOIN "
            "(c AS c_1 JOIN ctod AS ctod_1 ON c_1.id = ctod_1.cid) "
            "ON c_1.id = b_1.cid "
            "JOIN d AS d_1 ON d_1.id = ctod_1.did) ON b_1.id = a.bid "
            "ORDER BY d_1.id"
        )
        self._assert_result(q)


class SubqueryAliasingTest(fixtures.MappedTest, testing.AssertsCompiledSQL):

    """test #2188"""

    __dialect__ = 'default'
    run_create_tables = None

    @classmethod
    def define_tables(cls, metadata):
        Table('a', metadata,
              Column('id', Integer, primary_key=True)
              )

        Table('b', metadata,
              Column('id', Integer, primary_key=True),
              Column('a_id', Integer, ForeignKey('a.id')),
              Column('value', Integer),
              )

    @classmethod
    def setup_classes(cls):

        class A(cls.Comparable):
            pass

        class B(cls.Comparable):
            pass

    def _fixture(self, props):
        A, B = self.classes.A, self.classes.B
        b_table, a_table = self.tables.b, self.tables.a
        mapper(A, a_table, properties=props)
        mapper(B, b_table, properties={
            'a': relationship(A, backref="bs")
        })

    def test_column_property(self):
        A = self.classes.A
        b_table, a_table = self.tables.b, self.tables.a
        cp = select([func.sum(b_table.c.value)]).\
            where(b_table.c.a_id == a_table.c.id)

        self._fixture({
            'summation': column_property(cp)
        })
        self.assert_compile(
            create_session().query(A).options(joinedload_all('bs')).
            order_by(A.summation).
            limit(50),
            "SELECT anon_1.anon_2 AS anon_1_anon_2, anon_1.a_id "
            "AS anon_1_a_id, b_1.id AS b_1_id, b_1.a_id AS "
            "b_1_a_id, b_1.value AS b_1_value FROM (SELECT "
            "(SELECT sum(b.value) AS sum_1 FROM b WHERE b.a_id = a.id) "
            "AS anon_2, a.id AS a_id FROM a ORDER BY anon_2 "
            "LIMIT :param_1) AS anon_1 LEFT OUTER JOIN b AS b_1 ON "
            "anon_1.a_id = b_1.a_id ORDER BY anon_1.anon_2"
        )

    def test_column_property_desc(self):
        A = self.classes.A
        b_table, a_table = self.tables.b, self.tables.a
        cp = select([func.sum(b_table.c.value)]).\
            where(b_table.c.a_id == a_table.c.id)

        self._fixture({
            'summation': column_property(cp)
        })
        self.assert_compile(
            create_session().query(A).options(joinedload_all('bs')).
            order_by(A.summation.desc()).
            limit(50),
            "SELECT anon_1.anon_2 AS anon_1_anon_2, anon_1.a_id "
            "AS anon_1_a_id, b_1.id AS b_1_id, b_1.a_id AS "
            "b_1_a_id, b_1.value AS b_1_value FROM (SELECT "
            "(SELECT sum(b.value) AS sum_1 FROM b WHERE b.a_id = a.id) "
            "AS anon_2, a.id AS a_id FROM a ORDER BY anon_2 DESC "
            "LIMIT :param_1) AS anon_1 LEFT OUTER JOIN b AS b_1 ON "
            "anon_1.a_id = b_1.a_id ORDER BY anon_1.anon_2 DESC"
        )

    def test_column_property_correlated(self):
        A = self.classes.A
        b_table, a_table = self.tables.b, self.tables.a
        cp = select([func.sum(b_table.c.value)]).\
            where(b_table.c.a_id == a_table.c.id).\
            correlate(a_table)

        self._fixture({
            'summation': column_property(cp)
        })
        self.assert_compile(
            create_session().query(A).options(joinedload_all('bs')).
            order_by(A.summation).
            limit(50),
            "SELECT anon_1.anon_2 AS anon_1_anon_2, anon_1.a_id "
            "AS anon_1_a_id, b_1.id AS b_1_id, b_1.a_id AS "
            "b_1_a_id, b_1.value AS b_1_value FROM (SELECT "
            "(SELECT sum(b.value) AS sum_1 FROM b WHERE b.a_id = a.id) "
            "AS anon_2, a.id AS a_id FROM a ORDER BY anon_2 "
            "LIMIT :param_1) AS anon_1 LEFT OUTER JOIN b AS b_1 ON "
            "anon_1.a_id = b_1.a_id ORDER BY anon_1.anon_2"
        )

    def test_standalone_subquery_unlabeled(self):
        A = self.classes.A
        b_table, a_table = self.tables.b, self.tables.a
        self._fixture({})
        cp = select([func.sum(b_table.c.value)]).\
            where(b_table.c.a_id == a_table.c.id).\
            correlate(a_table).as_scalar()

        # up until 0.8, this was ordering by a new subquery.
        # the removal of a separate _make_proxy() from ScalarSelect
        # fixed that.
        self.assert_compile(
            create_session().query(A).options(joinedload_all('bs')).
            order_by(cp).
            limit(50),
            "SELECT anon_1.a_id AS anon_1_a_id, anon_1.anon_2 "
            "AS anon_1_anon_2, b_1.id AS b_1_id, b_1.a_id AS "
            "b_1_a_id, b_1.value AS b_1_value FROM (SELECT a.id "
            "AS a_id, (SELECT sum(b.value) AS sum_1 FROM b WHERE "
            "b.a_id = a.id) AS anon_2 FROM a ORDER BY (SELECT "
            "sum(b.value) AS sum_1 FROM b WHERE b.a_id = a.id) "
            "LIMIT :param_1) AS anon_1 LEFT OUTER JOIN b AS b_1 "
            "ON anon_1.a_id = b_1.a_id ORDER BY anon_1.anon_2"
        )

    def test_standalone_subquery_labeled(self):
        A = self.classes.A
        b_table, a_table = self.tables.b, self.tables.a
        self._fixture({})
        cp = select([func.sum(b_table.c.value)]).\
            where(b_table.c.a_id == a_table.c.id).\
            correlate(a_table).as_scalar().label('foo')
        self.assert_compile(
            create_session().query(A).options(joinedload_all('bs')).
            order_by(cp).
            limit(50),
            "SELECT anon_1.a_id AS anon_1_a_id, anon_1.foo "
            "AS anon_1_foo, b_1.id AS b_1_id, b_1.a_id AS "
            "b_1_a_id, b_1.value AS b_1_value FROM (SELECT a.id "
            "AS a_id, (SELECT sum(b.value) AS sum_1 FROM b WHERE "
            "b.a_id = a.id) AS foo FROM a ORDER BY foo "
            "LIMIT :param_1) AS anon_1 LEFT OUTER JOIN b AS b_1 "
            "ON anon_1.a_id = b_1.a_id ORDER BY "
            "anon_1.foo"
        )

    def test_standalone_negated(self):
        A = self.classes.A
        b_table, a_table = self.tables.b, self.tables.a
        self._fixture({})
        cp = select([func.sum(b_table.c.value)]).\
            where(b_table.c.a_id == a_table.c.id).\
            correlate(a_table).\
            as_scalar()
        # test a different unary operator
        self.assert_compile(
            create_session().query(A).options(joinedload_all('bs')).
            order_by(~cp).
            limit(50),
            "SELECT anon_1.a_id AS anon_1_a_id, anon_1.anon_2 "
            "AS anon_1_anon_2, b_1.id AS b_1_id, b_1.a_id AS "
            "b_1_a_id, b_1.value AS b_1_value FROM (SELECT a.id "
            "AS a_id, NOT (SELECT sum(b.value) AS sum_1 FROM b "
            "WHERE b.a_id = a.id) FROM a ORDER BY NOT (SELECT "
            "sum(b.value) AS sum_1 FROM b WHERE b.a_id = a.id) "
            "LIMIT :param_1) AS anon_1 LEFT OUTER JOIN b AS b_1 "
            "ON anon_1.a_id = b_1.a_id ORDER BY anon_1.anon_2"
        )


class LoadOnExistingTest(_fixtures.FixtureTest):

    """test that loaders from a base Query fully populate."""

    run_inserts = 'once'
    run_deletes = None

    def _collection_to_scalar_fixture(self):
        User, Address, Dingaling = self.classes.User, \
            self.classes.Address, self.classes.Dingaling
        mapper(User, self.tables.users, properties={
            'addresses': relationship(Address),
        })
        mapper(Address, self.tables.addresses, properties={
            'dingaling': relationship(Dingaling)
        })
        mapper(Dingaling, self.tables.dingalings)

        sess = Session(autoflush=False)
        return User, Address, Dingaling, sess

    def _collection_to_collection_fixture(self):
        User, Order, Item = self.classes.User, \
            self.classes.Order, self.classes.Item
        mapper(User, self.tables.users, properties={
            'orders': relationship(Order),
        })
        mapper(Order, self.tables.orders, properties={
            'items': relationship(Item, secondary=self.tables.order_items),
        })
        mapper(Item, self.tables.items)

        sess = Session(autoflush=False)
        return User, Order, Item, sess

    def _eager_config_fixture(self):
        User, Address = self.classes.User, self.classes.Address
        mapper(User, self.tables.users, properties={
            'addresses': relationship(Address, lazy="joined"),
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

    def test_loads_second_level_collection_to_scalar(self):
        User, Address, Dingaling, sess = self._collection_to_scalar_fixture()

        u1 = sess.query(User).get(8)
        a1 = Address()
        u1.addresses.append(a1)
        a2 = u1.addresses[0]
        a2.email_address = 'foo'
        sess.query(User).options(joinedload_all("addresses.dingaling")).\
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
        sess.query(User).options(joinedload_all("orders.items")).\
            filter_by(id=7).all()
        for o in u1.orders:
            if o is not o1:
                assert 'items' in o.__dict__
            else:
                assert 'items' not in o.__dict__

    def test_load_two_levels_collection_to_scalar(self):
        User, Address, Dingaling, sess = self._collection_to_scalar_fixture()

        u1 = sess.query(User).filter_by(
            id=8).options(
            joinedload("addresses")).one()
        sess.query(User).filter_by(
            id=8).options(
            joinedload_all("addresses.dingaling")).first()
        assert 'dingaling' in u1.addresses[0].__dict__

    def test_load_two_levels_collection_to_collection(self):
        User, Order, Item, sess = self._collection_to_collection_fixture()

        u1 = sess.query(User).filter_by(
            id=7).options(
            joinedload("orders")).one()
        sess.query(User).filter_by(
            id=7).options(
            joinedload_all("orders.items")).first()
        assert 'items' in u1.orders[0].__dict__


class AddEntityTest(_fixtures.FixtureTest):
    run_inserts = 'once'
    run_deletes = None

    def _assert_result(self):
        Item, Address, Order, User = (self.classes.Item,
                                      self.classes.Address,
                                      self.classes.Order,
                                      self.classes.User)

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

    def test_mapper_configured(self):
        users, items, order_items, Order, \
            Item, User, Address, orders, addresses = (
                self.tables.users,
                self.tables.items,
                self.tables.order_items,
                self.classes.Order,
                self.classes.Item,
                self.classes.User,
                self.classes.Address,
                self.tables.orders,
                self.tables.addresses)

        mapper(User, users, properties={
            'addresses': relationship(Address, lazy='joined'),
            'orders': relationship(Order)
        })
        mapper(Address, addresses)
        mapper(Order, orders, properties={
            'items': relationship(
                Item, secondary=order_items, lazy='joined',
                order_by=items.c.id)
        })
        mapper(Item, items)

        sess = create_session()
        oalias = sa.orm.aliased(Order)

        def go():
            ret = sess.query(User, oalias).join(oalias, 'orders').\
                order_by(User.id, oalias.id).all()
            eq_(ret, self._assert_result())
        self.assert_sql_count(testing.db, go, 1)

    def test_options(self):
        users, items, order_items, Order,\
            Item, User, Address, orders, addresses = (
                self.tables.users,
                self.tables.items,
                self.tables.order_items,
                self.classes.Order,
                self.classes.Item,
                self.classes.User,
                self.classes.Address,
                self.tables.orders,
                self.tables.addresses)

        mapper(User, users, properties={
            'addresses': relationship(Address),
            'orders': relationship(Order)
        })
        mapper(Address, addresses)
        mapper(Order, orders, properties={
            'items': relationship(
                Item, secondary=order_items, order_by=items.c.id)
        })
        mapper(Item, items)

        sess = create_session()

        oalias = sa.orm.aliased(Order)

        def go():
            ret = sess.query(User, oalias).options(joinedload('addresses')).\
                join(oalias, 'orders').\
                order_by(User.id, oalias.id).all()
            eq_(ret, self._assert_result())
        self.assert_sql_count(testing.db, go, 6)

        sess.expunge_all()

        def go():
            ret = sess.query(User, oalias).\
                options(joinedload('addresses'),
                        joinedload(oalias.items)).\
                join(oalias, 'orders').\
                order_by(User.id, oalias.id).all()
            eq_(ret, self._assert_result())
        self.assert_sql_count(testing.db, go, 1)


class OrderBySecondaryTest(fixtures.MappedTest):

    @classmethod
    def define_tables(cls, metadata):
        Table('m2m', metadata,
              Column(
                  'id',
                  Integer,
                  primary_key=True,
                  test_needs_autoincrement=True),
              Column('aid', Integer, ForeignKey('a.id')),
              Column('bid', Integer, ForeignKey('b.id')))

        Table('a', metadata,
              Column(
                  'id',
                  Integer,
                  primary_key=True,
                  test_needs_autoincrement=True),
              Column('data', String(50)))
        Table('b', metadata,
              Column(
                  'id',
                  Integer,
                  primary_key=True,
                  test_needs_autoincrement=True),
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
        a, m2m, b = (
            self.tables.a,
            self.tables.m2m,
            self.tables.b)

        class A(fixtures.ComparableEntity):
            pass

        class B(fixtures.ComparableEntity):
            pass

        mapper(A, a, properties={
            'bs': relationship(
                B, secondary=m2m, lazy='joined', order_by=m2m.c.id)
        })
        mapper(B, b)

        sess = create_session()
        eq_(sess.query(A).all(),
            [
                A(data='a1', bs=[B(data='b3'), B(data='b1'), B(data='b2')]),
                A(bs=[B(data='b4'), B(data='b3'), B(data='b2')])
        ])


class SelfReferentialEagerTest(fixtures.MappedTest):

    @classmethod
    def define_tables(cls, metadata):
        Table('nodes', metadata,
              Column(
                  'id',
                  Integer,
                  primary_key=True,
                  test_needs_autoincrement=True),
              Column('parent_id', Integer, ForeignKey('nodes.id')),
              Column('data', String(30)))

    def test_basic(self):
        nodes = self.tables.nodes

        class Node(fixtures.ComparableEntity):

            def append(self, node):
                self.children.append(node)

        mapper(Node, nodes, properties={
            'children': relationship(Node,
                                     lazy='joined',
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
        sess.add(n1)
        sess.flush()
        sess.expunge_all()

        def go():
            d = sess.query(Node).filter_by(data='n1').all()[0]
            eq_(Node(data='n1', children=[
                Node(data='n11'),
                Node(data='n12', children=[
                    Node(data='n121'),
                    Node(data='n122'),
                    Node(data='n123')
                ]),
                Node(data='n13')
            ]), d)
        self.assert_sql_count(testing.db, go, 1)

        sess.expunge_all()

        def go():
            d = sess.query(Node).filter_by(data='n1').first()
            eq_(Node(data='n1', children=[
                Node(data='n11'),
                Node(data='n12', children=[
                    Node(data='n121'),
                    Node(data='n122'),
                    Node(data='n123')
                ]),
                Node(data='n13')
            ]), d)
        self.assert_sql_count(testing.db, go, 1)

    def test_lazy_fallback_doesnt_affect_eager(self):
        nodes = self.tables.nodes

        class Node(fixtures.ComparableEntity):

            def append(self, node):
                self.children.append(node)

        mapper(Node, nodes, properties={
            'children': relationship(Node, lazy='joined', join_depth=1,
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
            eq_(n12.data, 'n12')
            eq_([
                Node(data='n121'),
                Node(data='n122'),
                Node(data='n123')
            ], list(n12.children))
        self.assert_sql_count(testing.db, go, 1)

    def test_with_deferred(self):
        nodes = self.tables.nodes

        class Node(fixtures.ComparableEntity):

            def append(self, node):
                self.children.append(node)

        mapper(Node, nodes, properties={
            'children': relationship(Node, lazy='joined', join_depth=3,
                                     order_by=nodes.c.id),
            'data': deferred(nodes.c.data)
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
            eq_(Node(data='n1', children=[Node(data='n11'), Node(data='n12')]),
                sess.query(Node).
                options(undefer('data')).order_by(Node.id).first())
        self.assert_sql_count(testing.db, go, 3)

        sess.expunge_all()

        def go():
            eq_(Node(data='n1', children=[Node(data='n11'), Node(data='n12')]),
                sess.query(Node).options(undefer('data'),
                                         undefer('children.data')).first())
        self.assert_sql_count(testing.db, go, 1)

    def test_options(self):
        nodes = self.tables.nodes

        class Node(fixtures.ComparableEntity):

            def append(self, node):
                self.children.append(node)

        mapper(Node, nodes, properties={
            'children': relationship(Node, lazy='select', order_by=nodes.c.id)
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
            d = sess.query(Node).filter_by(data='n1').\
                order_by(Node.id).\
                options(joinedload('children.children')).first()
            eq_(Node(data='n1', children=[
                Node(data='n11'),
                Node(data='n12', children=[
                    Node(data='n121'),
                    Node(data='n122'),
                    Node(data='n123')
                ]),
                Node(data='n13')
            ]), d)
        self.assert_sql_count(testing.db, go, 2)

        def go():
            sess.query(Node).order_by(Node.id).filter_by(data='n1').\
                options(joinedload('children.children')).first()

        # test that the query isn't wrapping the initial query for eager
        # loading.
        self.assert_sql_execution(
            testing.db, go,
            CompiledSQL(
                "SELECT nodes.id AS nodes_id, nodes.parent_id AS "
                "nodes_parent_id, nodes.data AS nodes_data FROM nodes "
                "WHERE nodes.data = :data_1 ORDER BY nodes.id LIMIT :param_1",
                {'data_1': 'n1'}
            )
        )

    def test_no_depth(self):
        nodes = self.tables.nodes

        class Node(fixtures.ComparableEntity):

            def append(self, node):
                self.children.append(node)

        mapper(Node, nodes, properties={
            'children': relationship(Node, lazy='joined')
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


class MixedSelfReferentialEagerTest(fixtures.MappedTest):

    @classmethod
    def define_tables(cls, metadata):
        Table('a_table', metadata,
              Column(
                  'id',
                  Integer,
                  primary_key=True,
                  test_needs_autoincrement=True)
              )

        Table('b_table', metadata,
              Column(
                  'id',
                  Integer,
                  primary_key=True,
                  test_needs_autoincrement=True),
              Column('parent_b1_id', Integer, ForeignKey('b_table.id')),
              Column('parent_a_id', Integer, ForeignKey('a_table.id')),
              Column('parent_b2_id', Integer, ForeignKey('b_table.id')))

    @classmethod
    def setup_mappers(cls):
        b_table, a_table = cls.tables.b_table, cls.tables.a_table

        class A(cls.Comparable):
            pass

        class B(cls.Comparable):
            pass

        mapper(A, a_table)
        mapper(B, b_table, properties={
            'parent_b1': relationship(
                B,
                remote_side=[b_table.c.id],
                primaryjoin=(b_table.c.parent_b1_id == b_table.c.id),
                order_by=b_table.c.id
            ),
            'parent_z': relationship(A, lazy=True),
            'parent_b2': relationship(
                B,
                remote_side=[b_table.c.id],
                primaryjoin=(b_table.c.parent_b2_id == b_table.c.id),
                order_by=b_table.c.id
            )
        })

    @classmethod
    def insert_data(cls):
        b_table, a_table = cls.tables.b_table, cls.tables.a_table

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

    def test_eager_load(self):
        A, B = self.classes.A, self.classes.B

        session = create_session()

        def go():
            eq_(
                session.query(B).
                options(
                    joinedload('parent_b1'),
                    joinedload('parent_b2'),
                    joinedload('parent_z')
                ).
                filter(B.id.in_([2, 8, 11])).order_by(B.id).all(),
                [
                    B(id=2,
                      parent_z=A(id=1),
                        parent_b1=B(id=1),
                        parent_b2=None),
                    B(id=8,
                      parent_z=A(id=2),
                        parent_b1=B(id=1),
                        parent_b2=B(id=2)),
                    B(id=11,
                      parent_z=A(id=3),
                        parent_b1=B(id=1),
                        parent_b2=B(id=8))
                ]
            )
        self.assert_sql_count(testing.db, go, 1)


class SelfReferentialM2MEagerTest(fixtures.MappedTest):

    @classmethod
    def define_tables(cls, metadata):
        Table('widget', metadata,
              Column(
                  'id',
                  Integer,
                  primary_key=True,
                  test_needs_autoincrement=True),
              Column('name', sa.String(40), nullable=False, unique=True),
              )

        Table('widget_rel', metadata,
              Column('parent_id', Integer, ForeignKey('widget.id')),
              Column('child_id', Integer, ForeignKey('widget.id')),
              sa.UniqueConstraint('parent_id', 'child_id'),
              )

    def test_basic(self):
        widget, widget_rel = self.tables.widget, self.tables.widget_rel

        class Widget(fixtures.ComparableEntity):
            pass

        mapper(Widget, widget, properties={
            'children': relationship(
                Widget, secondary=widget_rel,
                primaryjoin=widget_rel.c.parent_id == widget.c.id,
                secondaryjoin=widget_rel.c.child_id == widget.c.id,
                lazy='joined', join_depth=1,
            )
        })

        sess = create_session()
        w1 = Widget(name='w1')
        w2 = Widget(name='w2')
        w1.children.append(w2)
        sess.add(w1)
        sess.flush()
        sess.expunge_all()

        eq_([Widget(name='w1', children=[Widget(name='w2')])],
            sess.query(Widget).filter(Widget.name == 'w1').all())


class MixedEntitiesTest(_fixtures.FixtureTest, testing.AssertsCompiledSQL):
    run_setup_mappers = 'once'
    run_inserts = 'once'
    run_deletes = None
    __dialect__ = 'default'

    __prefer_backends__ = ('postgresql', 'mysql', 'oracle')

    @classmethod
    def setup_mappers(cls):
        users, Keyword, items, order_items, orders, \
            Item, User, Address, keywords, Order, \
            item_keywords, addresses = (
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
                cls.tables.addresses)

        mapper(User, users, properties={
            'addresses': relationship(Address, backref='user'),
            'orders': relationship(Order, backref='user'),  # o2m, m2o
        })
        mapper(Address, addresses)
        mapper(Order, orders, properties={
            'items': relationship(
                Item, secondary=order_items, order_by=items.c.id),  # m2m
        })
        mapper(Item, items, properties={
            'keywords': relationship(Keyword, secondary=item_keywords)  # m2m
        })
        mapper(Keyword, keywords)

    def test_two_entities(self):
        Item, Order, User, Address = (self.classes.Item,
                                      self.classes.Order,
                                      self.classes.User,
                                      self.classes.Address)

        sess = create_session()

        # two FROM clauses
        def go():
            eq_(
                [
                    (User(id=9, addresses=[Address(id=5)]),
                        Order(id=2, items=[
                            Item(id=1), Item(id=2), Item(id=3)])),
                    (User(id=9, addresses=[Address(id=5)]),
                        Order(id=4, items=[
                            Item(id=1), Item(id=5)])),
                ],
                sess.query(User, Order).filter(User.id == Order.user_id).
                options(joinedload(User.addresses), joinedload(Order.items)).
                filter(User.id == 9).
                order_by(User.id, Order.id).all(),
            )
        self.assert_sql_count(testing.db, go, 1)

        # one FROM clause
        def go():
            eq_(
                [
                    (User(id=9, addresses=[Address(id=5)]),
                        Order(id=2, items=[
                            Item(id=1), Item(id=2), Item(id=3)])),
                    (User(id=9, addresses=[Address(id=5)]),
                        Order(id=4, items=[
                            Item(id=1), Item(id=5)])),
                ],
                sess.query(User, Order).join(User.orders).
                options(joinedload(User.addresses), joinedload(Order.items)).
                filter(User.id == 9).
                order_by(User.id, Order.id).all(),
            )
        self.assert_sql_count(testing.db, go, 1)

    @testing.exclude(
        'sqlite', '>', (0, ), "sqlite flat out blows it on the multiple JOINs")
    def test_two_entities_with_joins(self):
        Item, Order, User, Address = (self.classes.Item,
                                      self.classes.Order,
                                      self.classes.User,
                                      self.classes.Address)

        sess = create_session()

        # two FROM clauses where there's a join on each one
        def go():
            u1 = aliased(User)
            o1 = aliased(Order)
            eq_(
                [
                    (
                        User(addresses=[
                             Address(email_address='fred@fred.com')],
                             name='fred'),
                        Order(description='order 2', isopen=0,
                              items=[
                                  Item(description='item 1'),
                                  Item(description='item 2'),
                                  Item(description='item 3')]),
                        User(addresses=[
                             Address(email_address='jack@bean.com')],
                             name='jack'),
                        Order(description='order 3', isopen=1,
                              items=[
                                  Item(description='item 3'),
                                  Item(description='item 4'),
                                  Item(description='item 5')])
                    ),

                    (
                        User(
                            addresses=[
                                Address(
                                    email_address='fred@fred.com')],
                            name='fred'),
                        Order(
                            description='order 2', isopen=0, items=[
                                Item(
                                    description='item 1'), Item(
                                    description='item 2'), Item(
                                    description='item 3')]),
                        User(
                            addresses=[
                                Address(
                                    email_address='jack@bean.com')],
                            name='jack'),
                        Order(
                            address_id=None,
                            description='order 5',
                            isopen=0,
                            items=[
                                Item(
                                    description='item 5')])
                    ),

                    (
                        User(
                            addresses=[
                                Address(
                                    email_address='fred@fred.com')],
                            name='fred'),
                        Order(
                            description='order 4', isopen=1, items=[
                                Item(
                                    description='item 1'), Item(
                                    description='item 5')]),
                        User(
                            addresses=[
                                Address(
                                    email_address='jack@bean.com')],
                            name='jack'),
                        Order(
                            address_id=None,
                            description='order 5',
                            isopen=0,
                            items=[
                                Item(
                                    description='item 5')])
                    ),
                ],
                sess.query(User, Order, u1, o1).
                join(Order, User.orders).
                options(joinedload(User.addresses),
                        joinedload(Order.items)).filter(User.id == 9).
                join(o1, u1.orders).
                options(joinedload(u1.addresses),
                        joinedload(o1.items)).filter(u1.id == 7).
                filter(Order.id < o1.id).
                order_by(User.id, Order.id, u1.id, o1.id).all(),
            )
        self.assert_sql_count(testing.db, go, 1)

    def test_aliased_entity_one(self):
        Item, Order, User, Address = (self.classes.Item,
                                      self.classes.Order,
                                      self.classes.User,
                                      self.classes.Address)

        sess = create_session()

        oalias = sa.orm.aliased(Order)

        # two FROM clauses
        def go():
            eq_(
                [
                    (
                        User(
                            id=9, addresses=[
                                Address(
                                    id=5)]), Order(
                            id=2, items=[
                                Item(
                                    id=1), Item(
                                    id=2), Item(
                                    id=3)])),
                    (User(id=9, addresses=[Address(id=5)]), Order(
                        id=4, items=[Item(id=1), Item(id=5)])),
                ],
                sess.query(User, oalias).filter(User.id == oalias.user_id).
                options(
                    joinedload(User.addresses),
                    joinedload(oalias.items)).filter(User.id == 9).
                order_by(User.id, oalias.id).all(),
            )
        self.assert_sql_count(testing.db, go, 1)

    def test_aliased_entity_two(self):
        Item, Order, User, Address = (self.classes.Item,
                                      self.classes.Order,
                                      self.classes.User,
                                      self.classes.Address)

        sess = create_session()

        oalias = sa.orm.aliased(Order)

        # one FROM clause
        def go():
            eq_(
                [
                    (
                        User(
                            id=9, addresses=[
                                Address(
                                    id=5)]), Order(
                            id=2, items=[
                                Item(
                                    id=1), Item(
                                    id=2), Item(
                                    id=3)])),
                    (User(id=9, addresses=[Address(id=5)]), Order(
                        id=4, items=[Item(id=1), Item(id=5)])),
                ],
                sess.query(User, oalias).join(oalias, User.orders).
                options(joinedload(User.addresses),
                        joinedload(oalias.items)).
                filter(User.id == 9).
                order_by(User.id, oalias.id).all(),
            )
        self.assert_sql_count(testing.db, go, 1)

    def test_aliased_entity_three(self):
        Order, User = (
            self.classes.Order,
            self.classes.User)

        sess = create_session()

        oalias = sa.orm.aliased(Order)

        # improper setup: oalias in the columns clause but join to usual
        # orders alias.  this should create two FROM clauses even though the
        # query has a from_clause set up via the join
        self.assert_compile(
            sess.query(User, oalias).join(User.orders).
            options(joinedload(oalias.items)).with_labels().statement,
            "SELECT users.id AS users_id, users.name AS users_name, "
            "orders_1.id AS orders_1_id, "
            "orders_1.user_id AS orders_1_user_id, "
            "orders_1.address_id AS orders_1_address_id, "
            "orders_1.description AS orders_1_description, "
            "orders_1.isopen AS orders_1_isopen, items_1.id AS items_1_id, "
            "items_1.description AS items_1_description FROM users "
            "JOIN orders ON users.id = orders.user_id, "
            "orders AS orders_1 LEFT OUTER JOIN (order_items AS order_items_1 "
            "JOIN items AS items_1 ON items_1.id = order_items_1.item_id) "
            "ON orders_1.id = order_items_1.order_id ORDER BY items_1.id"
        )


class SubqueryTest(fixtures.MappedTest):

    @classmethod
    def define_tables(cls, metadata):
        Table('users_table', metadata,
              Column(
                  'id',
                  Integer,
                  primary_key=True,
                  test_needs_autoincrement=True),
              Column('name', String(16))
              )

        Table('tags_table', metadata,
              Column(
                  'id',
                  Integer,
                  primary_key=True,
                  test_needs_autoincrement=True),
              Column('user_id', Integer, ForeignKey("users_table.id")),
              Column('score1', sa.Float),
              Column('score2', sa.Float),
              )

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

        tags_table, users_table = self.tables.tags_table, \
            self.tables.users_table

        class User(fixtures.ComparableEntity):

            @property
            def prop_score(self):
                return sum([tag.prop_score for tag in self.tags])

        class Tag(fixtures.ComparableEntity):

            @property
            def prop_score(self):
                return self.score1 * self.score2

        for labeled, labelname in [(True, 'score'), (True, None),
                                   (False, None)]:
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
                'tags': relationship(Tag, backref='user', lazy='joined'),
                'query_score': sa.orm.column_property(user_score),
            })

            session = create_session()
            session.add(User(name='joe', tags=[Tag(score1=5.0, score2=3.0),
                                               Tag(score1=55.0, score2=1.0)]))
            session.add(User(name='bar', tags=[Tag(score1=5.0, score2=4.0),
                                               Tag(score1=50.0, score2=1.0),
                                               Tag(score1=15.0, score2=2.0)]))
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


class CorrelatedSubqueryTest(fixtures.MappedTest):

    """tests for #946, #947, #948.

    The "users" table is joined to "stuff", and the relationship
    would like to pull only the "stuff" entry with the most recent date.

    Exercises a variety of ways to configure this.

    """

    # another argument for joinedload learning about inner joins

    __requires__ = ('correlated_outer_joins', )

    @classmethod
    def define_tables(cls, metadata):
        Table(
            'users', metadata,
            Column(
                'id',
                Integer,
                primary_key=True,
                test_needs_autoincrement=True),
            Column('name', String(50))
        )

        Table(
            'stuff', metadata,
            Column(
                'id',
                Integer,
                primary_key=True,
                test_needs_autoincrement=True),
            Column('date', Date),
            Column('user_id', Integer, ForeignKey('users.id')))

    @classmethod
    def insert_data(cls):
        stuff, users = cls.tables.stuff, cls.tables.users

        users.insert().execute(
            {'id': 1, 'name': 'user1'},
            {'id': 2, 'name': 'user2'},
            {'id': 3, 'name': 'user3'},
        )

        stuff.insert().execute(
            {'id': 1, 'user_id': 1, 'date': datetime.date(2007, 10, 15)},
            {'id': 2, 'user_id': 1, 'date': datetime.date(2007, 12, 15)},
            {'id': 3, 'user_id': 1, 'date': datetime.date(2007, 11, 15)},
            {'id': 4, 'user_id': 2, 'date': datetime.date(2008, 1, 15)},
            {'id': 5, 'user_id': 3, 'date': datetime.date(2007, 6, 15)},
            {'id': 6, 'user_id': 3, 'date': datetime.date(2007, 3, 15)},
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

    def _do_test(self, labeled, ondate, aliasstuff):
        stuff, users = self.tables.stuff, self.tables.users

        class User(fixtures.ComparableEntity):
            pass

        class Stuff(fixtures.ComparableEntity):
            pass

        mapper(Stuff, stuff)

        if aliasstuff:
            salias = stuff.alias()
        else:
            # if we don't alias the 'stuff' table within the correlated
            # subquery,
            # it gets aliased in the eager load along with the "stuff" table
            # to "stuff_1".
            # but it's a scalar subquery, and this doesn't actually matter
            salias = stuff

        if ondate:
            # the more 'relational' way to do this, join on the max date
            stuff_view = select([func.max(salias.c.date).label('max_date')]).\
                where(salias.c.user_id == users.c.id).correlate(users)
        else:
            # a common method with the MySQL crowd, which actually might
            # perform better in some
            # cases - subquery does a limit with order by DESC, join on the id
            stuff_view = select([salias.c.id]).\
                where(salias.c.user_id == users.c.id).\
                correlate(users).order_by(salias.c.date.desc()).limit(1)

        # can't win on this one
        if testing.against("mssql"):
            operator = operators.in_op
        else:
            operator = operators.eq

        if labeled == 'label':
            stuff_view = stuff_view.label('foo')
            operator = operators.eq
        elif labeled == 'scalar':
            stuff_view = stuff_view.as_scalar()

        if ondate:
            mapper(User, users, properties={
                'stuff': relationship(
                    Stuff,
                    primaryjoin=and_(users.c.id == stuff.c.user_id,
                                     operator(stuff.c.date, stuff_view)))
            })
        else:
            mapper(User, users, properties={
                'stuff': relationship(
                    Stuff,
                    primaryjoin=and_(users.c.id == stuff.c.user_id,
                                     operator(stuff.c.id, stuff_view)))
            })

        sess = create_session()

        def go():
            eq_(
                sess.query(User).order_by(User.name).options(
                    joinedload('stuff')).all(),
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
                sess.query(User).order_by(User.name).options(
                    joinedload('stuff')).first(),
                User(name='user1', stuff=[Stuff(id=2)])
            )
        self.assert_sql_count(testing.db, go, 1)

        sess = create_session()

        def go():
            eq_(
                sess.query(User).filter(User.id == 2).options(
                    joinedload('stuff')).one(),
                User(name='user2', stuff=[Stuff(id=4)])
            )
        self.assert_sql_count(testing.db, go, 1)


class CyclicalInheritingEagerTestOne(fixtures.MappedTest):

    @classmethod
    def define_tables(cls, metadata):
        Table(
            't1', metadata,
            Column(
                'c1', Integer, primary_key=True,
                test_needs_autoincrement=True),
            Column('c2', String(30)),
            Column('type', String(30))
        )

        Table('t2', metadata,
              Column('c1', Integer, primary_key=True,
                     test_needs_autoincrement=True),
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
        mapper(
            SubT, None, inherits=T, polymorphic_identity='subt1',
            properties={
                't2s': relationship(
                    SubT2, lazy='joined',
                    backref=sa.orm.backref('subt', lazy='joined'))
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
            id = Column(Integer, primary_key=True,
                        test_needs_autoincrement=True)

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

        self.assert_compile(
            s.query(Director).options(joinedload('*')),
            "SELECT director.id AS director_id, "
            "persistent.id AS persistent_id, "
            "director.name AS director_name, movie_1.id AS movie_1_id, "
            "persistent_1.id AS persistent_1_id, "
            "movie_1.director_id AS movie_1_director_id, "
            "movie_1.title AS movie_1_title "
            "FROM persistent JOIN director ON persistent.id = director.id "
            "LEFT OUTER JOIN "
            "(persistent AS persistent_1 JOIN movie AS movie_1 "
            "ON persistent_1.id = movie_1.id) "
            "ON director.id = movie_1.director_id"
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
        self.d = session.query(Director).options(joinedload('*')).first()
        assert len(list(session)) == 3


class CyclicalInheritingEagerTestThree(fixtures.DeclarativeMappedTest,
                                       testing.AssertsCompiledSQL):
    __dialect__ = 'default'
    run_create_tables = None

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class PersistentObject(Base):
            __tablename__ = 'persistent'
            id = Column(Integer, primary_key=True,
                        test_needs_autoincrement=True)

            __mapper_args__ = {'with_polymorphic': "*"}

        class Director(PersistentObject):
            __tablename__ = 'director'
            id = Column(Integer, ForeignKey('persistent.id'), primary_key=True)
            other_id = Column(Integer, ForeignKey('persistent.id'))
            name = Column(String(50))
            other = relationship(PersistentObject,
                                 primaryjoin=other_id == PersistentObject.id,
                                 lazy=False)
            __mapper_args__ = {"inherit_condition": id == PersistentObject.id}

    def test_gen_query_nodepth(self):
        PersistentObject = self.classes.PersistentObject
        sess = create_session()
        self.assert_compile(
            sess.query(PersistentObject),
            "SELECT persistent.id AS persistent_id, "
            "director.id AS director_id,"
            " director.other_id AS director_other_id, "
            "director.name AS director_name FROM persistent "
            "LEFT OUTER JOIN director ON director.id = persistent.id"
        )

    def test_gen_query_depth(self):
        PersistentObject = self.classes.PersistentObject
        Director = self.classes.Director
        sess = create_session()
        self.assert_compile(
            sess.query(PersistentObject).options(joinedload(Director.other)),
            "SELECT persistent.id AS persistent_id, "
            "director.id AS director_id, "
            "director.other_id AS director_other_id, "
            "director.name AS director_name, persistent_1.id AS "
            "persistent_1_id, director_1.id AS director_1_id, "
            "director_1.other_id AS director_1_other_id, "
            "director_1.name AS director_1_name "
            "FROM persistent LEFT OUTER JOIN director "
            "ON director.id = persistent.id "
            "LEFT OUTER JOIN (persistent AS persistent_1 "
            "LEFT OUTER JOIN director AS director_1 ON "
            "director_1.id = persistent_1.id) "
            "ON director.other_id = persistent_1.id"
        )


class EnsureColumnsAddedTest(
        fixtures.DeclarativeMappedTest, testing.AssertsCompiledSQL):
    __dialect__ = 'default'
    run_create_tables = None

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class Parent(Base):
            __tablename__ = 'parent'
            id = Column(Integer, primary_key=True,
                        test_needs_autoincrement=True)
            arb = Column(Integer, unique=True)
            data = Column(Integer)
            o2mchild = relationship("O2MChild")
            m2mchild = relationship("M2MChild", secondary=Table(
                'parent_to_m2m', Base.metadata,
                Column('parent_id', ForeignKey('parent.arb')),
                Column('child_id', ForeignKey('m2mchild.id'))
            ))

        class O2MChild(Base):
            __tablename__ = 'o2mchild'
            id = Column(Integer, primary_key=True,
                        test_needs_autoincrement=True)
            parent_id = Column(ForeignKey('parent.arb'))

        class M2MChild(Base):
            __tablename__ = 'm2mchild'
            id = Column(Integer, primary_key=True,
                        test_needs_autoincrement=True)

    def test_joinedload_defered_pk_limit_o2m(self):
        Parent = self.classes.Parent

        s = Session()

        self.assert_compile(
            s.query(Parent).options(
                load_only('data'),
                joinedload(Parent.o2mchild)).limit(10),
            "SELECT anon_1.parent_id AS anon_1_parent_id, "
            "anon_1.parent_data AS anon_1_parent_data, "
            "anon_1.parent_arb AS anon_1_parent_arb, "
            "o2mchild_1.id AS o2mchild_1_id, "
            "o2mchild_1.parent_id AS o2mchild_1_parent_id "
            "FROM (SELECT parent.id AS parent_id, parent.data AS parent_data, "
            "parent.arb AS parent_arb FROM parent LIMIT :param_1) AS anon_1 "
            "LEFT OUTER JOIN o2mchild AS o2mchild_1 "
            "ON anon_1.parent_arb = o2mchild_1.parent_id"
        )

    def test_joinedload_defered_pk_limit_m2m(self):
        Parent = self.classes.Parent

        s = Session()

        self.assert_compile(
            s.query(Parent).options(
                load_only('data'),
                joinedload(Parent.m2mchild)).limit(10),
            "SELECT anon_1.parent_id AS anon_1_parent_id, "
            "anon_1.parent_data AS anon_1_parent_data, "
            "anon_1.parent_arb AS anon_1_parent_arb, "
            "m2mchild_1.id AS m2mchild_1_id "
            "FROM (SELECT parent.id AS parent_id, "
            "parent.data AS parent_data, parent.arb AS parent_arb "
            "FROM parent LIMIT :param_1) AS anon_1 "
            "LEFT OUTER JOIN (parent_to_m2m AS parent_to_m2m_1 "
            "JOIN m2mchild AS m2mchild_1 "
            "ON m2mchild_1.id = parent_to_m2m_1.child_id) "
            "ON anon_1.parent_arb = parent_to_m2m_1.parent_id"
        )

    def test_joinedload_defered_pk_o2m(self):
        Parent = self.classes.Parent

        s = Session()

        self.assert_compile(
            s.query(Parent).options(
                load_only('data'),
                joinedload(Parent.o2mchild)),
            "SELECT parent.id AS parent_id, parent.data AS parent_data, "
            "parent.arb AS parent_arb, o2mchild_1.id AS o2mchild_1_id, "
            "o2mchild_1.parent_id AS o2mchild_1_parent_id "
            "FROM parent LEFT OUTER JOIN o2mchild AS o2mchild_1 "
            "ON parent.arb = o2mchild_1.parent_id"
        )

    def test_joinedload_defered_pk_m2m(self):
        Parent = self.classes.Parent

        s = Session()

        self.assert_compile(
            s.query(Parent).options(
                load_only('data'),
                joinedload(Parent.m2mchild)),
            "SELECT parent.id AS parent_id, parent.data AS parent_data, "
            "parent.arb AS parent_arb, m2mchild_1.id AS m2mchild_1_id "
            "FROM parent LEFT OUTER JOIN (parent_to_m2m AS parent_to_m2m_1 "
            "JOIN m2mchild AS m2mchild_1 "
            "ON m2mchild_1.id = parent_to_m2m_1.child_id) "
            "ON parent.arb = parent_to_m2m_1.parent_id"
        )


class EntityViaMultiplePathTestOne(fixtures.DeclarativeMappedTest):
    """test for [ticket:3431]"""

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class A(Base):
            __tablename__ = 'a'
            id = Column(Integer, primary_key=True)
            b_id = Column(ForeignKey('b.id'))
            c_id = Column(ForeignKey('c.id'))

            b = relationship("B")
            c = relationship("C")

        class B(Base):
            __tablename__ = 'b'
            id = Column(Integer, primary_key=True)
            c_id = Column(ForeignKey('c.id'))

            c = relationship("C")

        class C(Base):
            __tablename__ = 'c'
            id = Column(Integer, primary_key=True)
            d_id = Column(ForeignKey('d.id'))
            d = relationship("D")

        class D(Base):
            __tablename__ = 'd'
            id = Column(Integer, primary_key=True)

            @classmethod
            def define_tables(cls, metadata):
                Table(
                    'a', metadata,
                    Column('id', Integer, primary_key=True),
                    Column('bid', ForeignKey('b.id'))
                )

    def test_multi_path_load(self):
        A, B, C, D = self.classes('A', 'B', 'C', 'D')

        s = Session()

        c = C(d=D())

        s.add(
            A(b=B(c=c), c=c)
        )
        s.commit()

        c_alias_1 = aliased(C)
        c_alias_2 = aliased(C)

        q = s.query(A)
        q = q.join(A.b).join(c_alias_1, B.c).join(c_alias_1.d)
        q = q.options(
            contains_eager(A.b).
            contains_eager(B.c, alias=c_alias_1).
            contains_eager(C.d))
        q = q.join(c_alias_2, A.c)
        q = q.options(contains_eager(A.c, alias=c_alias_2))

        a1 = q.all()[0]

        # ensure 'd' key was populated in dict.  Varies based on
        # PYTHONHASHSEED
        in_('d', a1.c.__dict__)


class EntityViaMultiplePathTestTwo(fixtures.DeclarativeMappedTest):
    """test for [ticket:3431]"""

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class User(Base):
            __tablename__ = 'cs_user'

            id = Column(Integer, primary_key=True)
            data = Column(Integer)

        class LD(Base):
            """Child.  The column we reference 'A' with is an integer."""

            __tablename__ = 'cs_ld'

            id = Column(Integer, primary_key=True)
            user_id = Column(Integer, ForeignKey('cs_user.id'))
            user = relationship(User, primaryjoin=user_id == User.id)

        class A(Base):
            """Child.  The column we reference 'A' with is an integer."""

            __tablename__ = 'cs_a'

            id = Column(Integer, primary_key=True)
            ld_id = Column(Integer, ForeignKey('cs_ld.id'))
            ld = relationship(LD, primaryjoin=ld_id == LD.id)

        class LDA(Base):
            """Child.  The column we reference 'A' with is an integer."""

            __tablename__ = 'cs_lda'

            id = Column(Integer, primary_key=True)
            ld_id = Column(Integer, ForeignKey('cs_ld.id'))
            a_id = Column(Integer, ForeignKey('cs_a.id'))
            a = relationship(A, primaryjoin=a_id == A.id)
            ld = relationship(LD, primaryjoin=ld_id == LD.id)

    def test_multi_path_load(self):
        User, LD, A, LDA = self.classes('User', 'LD', 'A', 'LDA')

        s = Session()

        u0 = User(data=42)
        l0 = LD(user=u0)
        z0 = A(ld=l0)
        lz0 = LDA(ld=l0, a=z0)
        s.add_all([
            u0, l0, z0, lz0
        ])
        s.commit()

        l_ac = aliased(LD)
        u_ac = aliased(User)

        lz_test = (s.query(LDA)
                   .join('ld')
                   .options(contains_eager('ld'))
                   .join('a', (l_ac, 'ld'), (u_ac, 'user'))
                   .options(contains_eager('a')
                            .contains_eager('ld', alias=l_ac)
                            .contains_eager('user', alias=u_ac))
                   .first())

        in_(
            'user', lz_test.a.ld.__dict__
        )


class LazyLoadOptSpecificityTest(fixtures.DeclarativeMappedTest):
    """test for [ticket:3963]"""

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class A(Base):
            __tablename__ = 'a'
            id = Column(Integer, primary_key=True)
            bs = relationship("B")

        class B(Base):
            __tablename__ = 'b'
            id = Column(Integer, primary_key=True)
            a_id = Column(ForeignKey('a.id'))
            cs = relationship("C")

        class C(Base):
            __tablename__ = 'c'
            id = Column(Integer, primary_key=True)
            b_id = Column(ForeignKey('b.id'))

    @classmethod
    def insert_data(cls):
        A, B, C = cls.classes("A", "B", "C")
        s = Session()
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
        s = Session()
        aa = aliased(A)
        q = s.query(aa, A).filter(
            aa.id == 1).filter(A.id == 2).options(
            joinedload("bs").joinedload("cs"))
        self._run_tests(q, 1)

    def test_string_options_unaliased_whatever(self):
        A, B, C = self.classes("A", "B", "C")
        s = Session()
        aa = aliased(A)
        q = s.query(A, aa).filter(
            aa.id == 2).filter(A.id == 1).options(
            joinedload("bs").joinedload("cs"))
        self._run_tests(q, 1)

    def test_lazyload_aliased_abs_bcs_one(self):
        A, B, C = self.classes("A", "B", "C")
        s = Session()
        aa = aliased(A)
        q = s.query(aa, A).filter(
            aa.id == 1).filter(A.id == 2).options(
            joinedload(A.bs).joinedload(B.cs))
        self._run_tests(q, 3)

    def test_lazyload_aliased_abs_bcs_two(self):
        A, B, C = self.classes("A", "B", "C")
        s = Session()
        aa = aliased(A)
        q = s.query(aa, A).filter(
            aa.id == 1).filter(A.id == 2).options(
            defaultload(A.bs).joinedload(B.cs))
        self._run_tests(q, 3)

    def test_pathed_lazyload_aliased_abs_bcs(self):
        A, B, C = self.classes("A", "B", "C")
        s = Session()
        aa = aliased(A)
        opt = Load(A).joinedload(A.bs).joinedload(B.cs)

        q = s.query(aa, A).filter(
            aa.id == 1).filter(A.id == 2).options(opt)
        self._run_tests(q, 3)

    def test_pathed_lazyload_plus_joined_aliased_abs_bcs(self):
        A, B, C = self.classes("A", "B", "C")
        s = Session()
        aa = aliased(A)
        opt = Load(aa).defaultload(aa.bs).joinedload(B.cs)

        q = s.query(aa, A).filter(
            aa.id == 1).filter(A.id == 2).options(opt)
        self._run_tests(q, 2)

    def test_pathed_joinedload_aliased_abs_bcs(self):
        A, B, C = self.classes("A", "B", "C")
        s = Session()
        aa = aliased(A)
        opt = Load(aa).joinedload(aa.bs).joinedload(B.cs)

        q = s.query(aa, A).filter(
            aa.id == 1).filter(A.id == 2).options(opt)
        self._run_tests(q, 1)

    def test_lazyload_plus_joined_aliased_abs_bcs(self):
        A, B, C = self.classes("A", "B", "C")
        s = Session()
        aa = aliased(A)
        q = s.query(aa, A).filter(
            aa.id == 1).filter(A.id == 2).options(
            defaultload(aa.bs).joinedload(B.cs))
        self._run_tests(q, 2)

    def test_joinedload_aliased_abs_bcs(self):
        A, B, C = self.classes("A", "B", "C")
        s = Session()
        aa = aliased(A)
        q = s.query(aa, A).filter(
            aa.id == 1).filter(A.id == 2).options(
            joinedload(aa.bs).joinedload(B.cs))
        self._run_tests(q, 1)

    def test_lazyload_unaliased_abs_bcs_one(self):
        A, B, C = self.classes("A", "B", "C")
        s = Session()
        aa = aliased(A)
        q = s.query(A, aa).filter(
            aa.id == 2).filter(A.id == 1).options(
            joinedload(aa.bs).joinedload(B.cs))
        self._run_tests(q, 3)

    def test_lazyload_unaliased_abs_bcs_two(self):
        A, B, C = self.classes("A", "B", "C")
        s = Session()
        aa = aliased(A)
        q = s.query(A, aa).filter(
            aa.id == 2).filter(A.id == 1).options(
            defaultload(aa.bs).joinedload(B.cs))
        self._run_tests(q, 3)

    def test_lazyload_plus_joined_unaliased_abs_bcs(self):
        A, B, C = self.classes("A", "B", "C")
        s = Session()
        aa = aliased(A)
        q = s.query(A, aa).filter(
            aa.id == 2).filter(A.id == 1).options(
            defaultload(A.bs).joinedload(B.cs))
        self._run_tests(q, 2)

    def test_joinedload_unaliased_abs_bcs(self):
        A, B, C = self.classes("A", "B", "C")
        s = Session()
        aa = aliased(A)
        q = s.query(A, aa).filter(
            aa.id == 2).filter(A.id == 1).options(
            joinedload(A.bs).joinedload(B.cs))
        self._run_tests(q, 1)


class EntityViaMultiplePathTestThree(fixtures.DeclarativeMappedTest):
    """test for [ticket:3811] continuing on [ticket:3431]"""

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class A(Base):
            __tablename__ = 'a'

            id = Column(Integer, primary_key=True)
            parent_id = Column(Integer, ForeignKey('a.id'))
            parent = relationship("A", remote_side=id, lazy="raise")

    def test_multi_path_load_lazy_none(self):
        A = self.classes.A
        s = Session()
        s.add_all([
            A(id=1, parent_id=None),
            A(id=2, parent_id=2),
            A(id=4, parent_id=None),
            A(id=3, parent_id=4),
        ])
        s.commit()

        q1 = s.query(A).order_by(A.id).\
            filter(A.id.in_([1, 2])).options(joinedload(A.parent))

        def go():
            for a in q1:
                if a.id == 1:
                    assert a.parent is None
                else:
                    assert a.parent is not None

        self.assert_sql_count(testing.db, go, 1)

        q1 = s.query(A).order_by(A.id).\
            filter(A.id.in_([3, 4])).options(joinedload(A.parent))

        def go():
            for a in q1:
                if a.id == 4:
                    assert a.parent is None
                else:
                    assert a.parent is not None

        self.assert_sql_count(testing.db, go, 1)
