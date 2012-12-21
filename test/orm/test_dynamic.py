from sqlalchemy.testing import eq_
from sqlalchemy.orm import backref, configure_mappers
from sqlalchemy import testing
from sqlalchemy import desc, select, func, exc
from sqlalchemy.orm import mapper, relationship, create_session, Query, \
                    attributes, exc as orm_exc
from sqlalchemy.orm.dynamic import AppenderMixin
from sqlalchemy.testing import AssertsCompiledSQL, \
        assert_raises_message, assert_raises
from test.orm import _fixtures


class _DynamicFixture(object):
    def _user_address_fixture(self, addresses_args={}):
        users, Address, addresses, User = (self.tables.users,
                                self.classes.Address,
                                self.tables.addresses,
                                self.classes.User)

        mapper(User, users, properties={
            'addresses': relationship(Address, lazy="dynamic",
                                        **addresses_args)
        })
        mapper(Address, addresses)
        return User, Address

class DynamicTest(_DynamicFixture, _fixtures.FixtureTest, AssertsCompiledSQL):

    def test_basic(self):
        User, Address = self._user_address_fixture()
        sess = create_session()
        q = sess.query(User)

        eq_([User(id=7,
                  addresses=[Address(id=1, email_address='jack@bean.com')])],
            q.filter(User.id == 7).all())
        eq_(self.static.user_address_result, q.all())

    def test_statement(self):
        """test that the .statement accessor returns the actual statement that
        would render, without any _clones called."""

        User, Address = self._user_address_fixture()
        sess = create_session()
        q = sess.query(User)

        u = q.filter(User.id == 7).first()
        self.assert_compile(
            u.addresses.statement,
            "SELECT addresses.id, addresses.user_id, addresses.email_address "
            "FROM "
            "addresses WHERE :param_1 = addresses.user_id",
            use_default_dialect=True
        )

    def test_detached_raise(self):
        User, Address = self._user_address_fixture()
        sess = create_session()
        u = sess.query(User).get(8)
        sess.expunge(u)
        assert_raises(
            orm_exc.DetachedInstanceError,
            u.addresses.filter_by,
            email_address='e'
        )

    def test_no_uselist_false(self):
        User, Address = self._user_address_fixture(
                                    addresses_args={"uselist": False})
        assert_raises_message(
            exc.InvalidRequestError,
            "On relationship User.addresses, 'dynamic' loaders cannot be "
            "used with many-to-one/one-to-one relationships and/or "
            "uselist=False.",
            configure_mappers
        )

    def test_no_m2o(self):
        users, Address, addresses, User = (self.tables.users,
                                self.classes.Address,
                                self.tables.addresses,
                                self.classes.User)
        mapper(Address, addresses, properties={
                'user': relationship(User, lazy='dynamic')
            })
        mapper(User, users)
        assert_raises_message(
            exc.InvalidRequestError,
            "On relationship Address.user, 'dynamic' loaders cannot be "
            "used with many-to-one/one-to-one relationships and/or "
            "uselist=False.",
            configure_mappers
        )

    def test_order_by(self):
        User, Address = self._user_address_fixture()
        sess = create_session()
        u = sess.query(User).get(8)
        eq_(
            list(u.addresses.order_by(desc(Address.email_address))),
             [
                Address(email_address=u'ed@wood.com'),
                Address(email_address=u'ed@lala.com'),
                Address(email_address=u'ed@bettyboop.com')
            ]
        )

    def test_configured_order_by(self):
        addresses = self.tables.addresses
        User, Address = self._user_address_fixture(
                                    addresses_args={
                                        "order_by":
                                            addresses.c.email_address.desc()})

        sess = create_session()
        u = sess.query(User).get(8)
        eq_(
            list(u.addresses),
            [
                Address(email_address=u'ed@wood.com'),
                Address(email_address=u'ed@lala.com'),
                Address(email_address=u'ed@bettyboop.com')
            ]
        )

        # test cancellation of None, replacement with something else
        eq_(
            list(u.addresses.order_by(None).order_by(Address.email_address)),
            [
                Address(email_address=u'ed@bettyboop.com'),
                Address(email_address=u'ed@lala.com'),
                Address(email_address=u'ed@wood.com')
            ]
        )

        # test cancellation of None, replacement with nothing
        eq_(
            set(u.addresses.order_by(None)),
            set([
                Address(email_address=u'ed@bettyboop.com'),
                Address(email_address=u'ed@lala.com'),
                Address(email_address=u'ed@wood.com')
            ])
        )

    def test_count(self):
        User, Address = self._user_address_fixture()
        sess = create_session()
        u = sess.query(User).first()
        eq_(u.addresses.count(), 1)

    def test_dynamic_on_backref(self):
        users, Address, addresses, User = (self.tables.users,
                                self.classes.Address,
                                self.tables.addresses,
                                self.classes.User)

        mapper(Address, addresses, properties={
            'user': relationship(User,
                                backref=backref('addresses', lazy='dynamic'))
        })
        mapper(User, users)

        sess = create_session()
        ad = sess.query(Address).get(1)
        def go():
            ad.user = None
        self.assert_sql_count(testing.db, go, 0)
        sess.flush()
        u = sess.query(User).get(7)
        assert ad not in u.addresses

    def test_no_count(self):
        User, Address = self._user_address_fixture()
        sess = create_session()
        q = sess.query(User)

        # dynamic collection cannot implement __len__() (at least one that
        # returns a live database result), else additional count() queries are
        # issued when evaluating in a list context
        def go():
            eq_(
                q.filter(User.id == 7).all(),
                [
                    User(id=7,
                      addresses=[
                        Address(id=1, email_address='jack@bean.com')
                    ])
                ]
            )
        self.assert_sql_count(testing.db, go, 2)

    def test_no_populate(self):
        User, Address = self._user_address_fixture()
        u1 = User()
        assert_raises_message(
            NotImplementedError,
            "Dynamic attributes don't support collection population.",
            attributes.set_committed_value, u1, 'addresses', []
        )

    def test_m2m(self):
        items, Order, orders, order_items, Item = (self.tables.items,
                                self.classes.Order,
                                self.tables.orders,
                                self.tables.order_items,
                                self.classes.Item)

        mapper(Order, orders, properties={
            'items': relationship(Item,
                            secondary=order_items,
                            lazy="dynamic",
                            backref=backref('orders', lazy="dynamic")
                        )
        })
        mapper(Item, items)

        sess = create_session()
        o1 = Order(id=15, description="order 10")
        i1 = Item(id=10, description="item 8")
        o1.items.append(i1)
        sess.add(o1)
        sess.flush()

        assert o1 in i1.orders.all()
        assert i1 in o1.items.all()

    @testing.exclude('mysql', 'between',
            ((5, 1, 49), (5, 1, 52)),
            'https://bugs.launchpad.net/ubuntu/+source/mysql-5.1/+bug/706988')
    def test_association_nonaliased(self):
        items, Order, orders, order_items, Item = (self.tables.items,
                                self.classes.Order,
                                self.tables.orders,
                                self.tables.order_items,
                                self.classes.Item)

        mapper(Order, orders, properties={
            'items': relationship(Item,
                                secondary=order_items,
                                lazy="dynamic",
                                order_by=order_items.c.item_id)
        })
        mapper(Item, items)

        sess = create_session()
        o = sess.query(Order).first()

        self.assert_compile(
            o.items,
            "SELECT items.id AS items_id, items.description AS "
            "items_description FROM items,"
            " order_items WHERE :param_1 = order_items.order_id AND "
            "items.id = order_items.item_id"
            " ORDER BY order_items.item_id",
            use_default_dialect=True
        )

        # filter criterion against the secondary table
        # works
        eq_(
            o.items.filter(order_items.c.item_id == 2).all(),
            [Item(id=2)]
        )

    def test_transient_count(self):
        User, Address = self._user_address_fixture()
        u1 = User()
        u1.addresses.append(Address())
        eq_(u1.addresses.count(), 1)

    def test_transient_access(self):
        User, Address = self._user_address_fixture()
        u1 = User()
        u1.addresses.append(Address())
        eq_(u1.addresses[0], Address())

    def test_custom_query(self):
        class MyQuery(Query):
            pass
        User, Address = self._user_address_fixture(
                                addresses_args={"query_class": MyQuery})

        sess = create_session()
        u = User()
        sess.add(u)

        col = u.addresses
        assert isinstance(col, Query)
        assert isinstance(col, MyQuery)
        assert hasattr(col, 'append')
        eq_(type(col).__name__, 'AppenderMyQuery')

        q = col.limit(1)
        assert isinstance(q, Query)
        assert isinstance(q, MyQuery)
        assert not hasattr(q, 'append')
        eq_(type(q).__name__, 'MyQuery')

    def test_custom_query_with_custom_mixin(self):
        class MyAppenderMixin(AppenderMixin):
            def add(self, items):
                if isinstance(items, list):
                    for item in items:
                        self.append(item)
                else:
                    self.append(items)

        class MyQuery(Query):
            pass

        class MyAppenderQuery(MyAppenderMixin, MyQuery):
            query_class = MyQuery

        User, Address = self._user_address_fixture(
                                addresses_args={
                                    "query_class": MyAppenderQuery})


        sess = create_session()
        u = User()
        sess.add(u)

        col = u.addresses
        assert isinstance(col, Query)
        assert isinstance(col, MyQuery)
        assert hasattr(col, 'append')
        assert hasattr(col, 'add')
        eq_(type(col).__name__, 'MyAppenderQuery')

        q = col.limit(1)
        assert isinstance(q, Query)
        assert isinstance(q, MyQuery)
        assert not hasattr(q, 'append')
        assert not hasattr(q, 'add')
        eq_(type(q).__name__, 'MyQuery')


class UOWTest(_DynamicFixture, _fixtures.FixtureTest):
    run_inserts = None

    def test_persistence(self):
        addresses = self.tables.addresses
        User, Address = self._user_address_fixture()

        sess = create_session()
        u1 = User(name='jack')
        a1 = Address(email_address='foo')
        sess.add_all([u1, a1])
        sess.flush()

        eq_(
            testing.db.scalar(
                select([func.count(1)]).where(addresses.c.user_id != None)
            ),
            0
        )
        u1 = sess.query(User).get(u1.id)
        u1.addresses.append(a1)
        sess.flush()

        eq_(
            testing.db.execute(
                    select([addresses]).where(addresses.c.user_id != None)
            ).fetchall(),
            [(a1.id, u1.id, 'foo')]
        )

        u1.addresses.remove(a1)
        sess.flush()
        eq_(
            testing.db.scalar(
                select([func.count(1)]).where(addresses.c.user_id != None)
            ),
            0
        )

        u1.addresses.append(a1)
        sess.flush()
        eq_(
            testing.db.execute(
                select([addresses]).where(addresses.c.user_id != None)
            ).fetchall(),
            [(a1.id, u1.id, 'foo')]
        )

        a2 = Address(email_address='bar')
        u1.addresses.remove(a1)
        u1.addresses.append(a2)
        sess.flush()
        eq_(
            testing.db.execute(
                select([addresses]).where(addresses.c.user_id != None)
            ).fetchall(),
            [(a2.id, u1.id, 'bar')]
        )


    def test_merge(self):
        addresses = self.tables.addresses
        User, Address = self._user_address_fixture(
                                    addresses_args={
                                        "order_by": addresses.c.email_address})
        sess = create_session()
        u1 = User(name='jack')
        a1 = Address(email_address='a1')
        a2 = Address(email_address='a2')
        a3 = Address(email_address='a3')

        u1.addresses.append(a2)
        u1.addresses.append(a3)

        sess.add_all([u1, a1])
        sess.flush()

        u1 = User(id=u1.id, name='jack')
        u1.addresses.append(a1)
        u1.addresses.append(a3)
        u1 = sess.merge(u1)
        eq_(attributes.get_history(u1, 'addresses'), (
            [a1],
            [a3],
            [a2]
        ))

        sess.flush()

        eq_(
            list(u1.addresses),
            [a1, a3]
        )

    def test_hasattr(self):
        User, Address = self._user_address_fixture()

        u1 = User(name='jack')

        assert 'addresses' not in u1.__dict__
        u1.addresses = [Address(email_address='test')]
        assert 'addresses' in u1.__dict__

    def test_collection_set(self):
        addresses = self.tables.addresses
        User, Address = self._user_address_fixture(
                                    addresses_args={
                                        "order_by": addresses.c.email_address})
        sess = create_session(autoflush=True, autocommit=False)
        u1 = User(name='jack')
        a1 = Address(email_address='a1')
        a2 = Address(email_address='a2')
        a3 = Address(email_address='a3')
        a4 = Address(email_address='a4')

        sess.add(u1)
        u1.addresses = [a1, a3]
        eq_(list(u1.addresses), [a1, a3])
        u1.addresses = [a1, a2, a4]
        eq_(list(u1.addresses), [a1, a2, a4])
        u1.addresses = [a2, a3]
        eq_(list(u1.addresses), [a2, a3])
        u1.addresses = []
        eq_(list(u1.addresses), [])

    def test_rollback(self):
        User, Address = self._user_address_fixture()
        sess = create_session(
                    expire_on_commit=False, autocommit=False, autoflush=True)
        u1 = User(name='jack')
        u1.addresses.append(Address(email_address='lala@hoho.com'))
        sess.add(u1)
        sess.flush()
        sess.commit()
        u1.addresses.append(Address(email_address='foo@bar.com'))
        eq_(
            u1.addresses.order_by(Address.id).all(),
            [
                Address(email_address='lala@hoho.com'),
                Address(email_address='foo@bar.com')
            ]
        )
        sess.rollback()
        eq_(
            u1.addresses.all(),
            [Address(email_address='lala@hoho.com')]
        )

    def _test_delete_cascade(self, expected):
        addresses = self.tables.addresses
        User, Address = self._user_address_fixture(addresses_args={
                            "order_by": addresses.c.id,
                            "backref": "user",
                            "cascade": "save-update" if expected \
                                            else "all, delete"
            })

        sess = create_session(autoflush=True, autocommit=False)
        u = User(name='ed')
        u.addresses.extend(
            [Address(email_address=letter) for letter in 'abcdef']
        )
        sess.add(u)
        sess.commit()
        eq_(testing.db.scalar(
                addresses.count(addresses.c.user_id == None)), 0)
        eq_(testing.db.scalar(
                addresses.count(addresses.c.user_id != None)), 6)

        sess.delete(u)

        sess.commit()

        if expected:
            eq_(testing.db.scalar(
                    addresses.count(addresses.c.user_id == None)), 6)
            eq_(testing.db.scalar(
                    addresses.count(addresses.c.user_id != None)), 0)
        else:
            eq_(testing.db.scalar(addresses.count()), 0)

    def test_delete_nocascade(self):
        self._test_delete_cascade(True)

    def test_delete_cascade(self):
        self._test_delete_cascade(False)

    def test_remove_orphans(self):
        addresses = self.tables.addresses
        User, Address = self._user_address_fixture(addresses_args={
                            "order_by": addresses.c.id,
                            "backref": "user",
                            "cascade": "all, delete-orphan"
            })

        sess = create_session(autoflush=True, autocommit=False)
        u = User(name='ed')
        u.addresses.extend(
            [Address(email_address=letter) for letter in 'abcdef']
        )
        sess.add(u)

        for a in u.addresses.filter(
                Address.email_address.in_(['c', 'e', 'f'])):
            u.addresses.remove(a)

        eq_(
            set(ad for ad, in sess.query(Address.email_address)),
            set(['a', 'b', 'd'])
        )

    def _backref_test(self, autoflush, saveuser):
        User, Address = self._user_address_fixture(addresses_args={
                            "backref": "user",
            })
        sess = create_session(autoflush=autoflush, autocommit=False)

        u = User(name='buffy')

        a = Address(email_address='foo@bar.com')
        a.user = u

        if saveuser:
            sess.add(u)
        else:
            sess.add(a)

        if not autoflush:
            sess.flush()

        assert u in sess
        assert a in sess

        eq_(list(u.addresses), [a])

        a.user = None
        if not autoflush:
            eq_(list(u.addresses), [a])

        if not autoflush:
            sess.flush()
        eq_(list(u.addresses), [])

    def test_backref_autoflush_saveuser(self):
        self._backref_test(True, True)

    def test_backref_autoflush_savead(self):
        self._backref_test(True, False)

    def test_backref_saveuser(self):
        self._backref_test(False, True)

    def test_backref_savead(self):
        self._backref_test(False, False)

    def test_no_deref(self):
        User, Address = self._user_address_fixture(addresses_args={
                            "backref": "user",
            })

        session = create_session()
        user = User()
        user.name = 'joe'
        user.fullname = 'Joe User'
        user.password = 'Joe\'s secret'
        address = Address()
        address.email_address = 'joe@joesdomain.example'
        address.user = user
        session.add(user)
        session.flush()
        session.expunge_all()

        def query1():
            session = create_session(testing.db)
            user = session.query(User).first()
            return user.addresses.all()

        def query2():
            session = create_session(testing.db)
            return session.query(User).first().addresses.all()

        def query3():
            session = create_session(testing.db)
            user = session.query(User).first()
            return session.query(User).first().addresses.all()

        eq_(query1(), [Address(email_address='joe@joesdomain.example')])
        eq_(query2(), [Address(email_address='joe@joesdomain.example')])
        eq_(query3(), [Address(email_address='joe@joesdomain.example')])


