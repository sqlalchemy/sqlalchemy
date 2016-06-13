from sqlalchemy import testing, desc, select, func, exc, cast, Integer
from sqlalchemy.orm import (
    mapper, relationship, create_session, Query, attributes, exc as orm_exc,
    Session, backref, configure_mappers)
from sqlalchemy.orm.dynamic import AppenderMixin
from sqlalchemy.testing import (
    AssertsCompiledSQL, assert_raises_message, assert_raises, eq_, is_)
from test.orm import _fixtures
from sqlalchemy.testing.assertsql import CompiledSQL


class _DynamicFixture(object):
    def _user_address_fixture(self, addresses_args={}):
        users, Address, addresses, User = (self.tables.users,
                                self.classes.Address,
                                self.tables.addresses,
                                self.classes.User)

        mapper(
            User, users, properties={
                'addresses': relationship(
                    Address, lazy="dynamic", **addresses_args)})
        mapper(Address, addresses)
        return User, Address

    def _order_item_fixture(self, items_args={}):
        items, Order, orders, order_items, Item = (self.tables.items,
                                self.classes.Order,
                                self.tables.orders,
                                self.tables.order_items,
                                self.classes.Item)

        mapper(
            Order, orders, properties={
                'items': relationship(
                    Item, secondary=order_items, lazy="dynamic",
                    **items_args)})
        mapper(Item, items)
        return Order, Item


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
        mapper(
            Address, addresses, properties={
                'user': relationship(User, lazy='dynamic')})
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
                Address(email_address='ed@wood.com'),
                Address(email_address='ed@lala.com'),
                Address(email_address='ed@bettyboop.com')
            ]
        )

    def test_configured_order_by(self):
        addresses = self.tables.addresses
        User, Address = self._user_address_fixture(
            addresses_args={"order_by": addresses.c.email_address.desc()})

        sess = create_session()
        u = sess.query(User).get(8)
        eq_(
            list(u.addresses),
            [
                Address(email_address='ed@wood.com'),
                Address(email_address='ed@lala.com'),
                Address(email_address='ed@bettyboop.com')
            ]
        )

        # test cancellation of None, replacement with something else
        eq_(
            list(u.addresses.order_by(None).order_by(Address.email_address)),
            [
                Address(email_address='ed@bettyboop.com'),
                Address(email_address='ed@lala.com'),
                Address(email_address='ed@wood.com')
            ]
        )

        # test cancellation of None, replacement with nothing
        eq_(
            set(u.addresses.order_by(None)),
            set([
                Address(email_address='ed@bettyboop.com'),
                Address(email_address='ed@lala.com'),
                Address(email_address='ed@wood.com')
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
                    User(
                        id=7, addresses=[
                            Address(id=1, email_address='jack@bean.com')])])
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
        Order, Item = self._order_item_fixture(
            items_args={"backref": backref("orders", lazy="dynamic")})

        sess = create_session()
        o1 = Order(id=15, description="order 10")
        i1 = Item(id=10, description="item 8")
        o1.items.append(i1)
        sess.add(o1)
        sess.flush()

        assert o1 in i1.orders.all()
        assert i1 in o1.items.all()

    @testing.exclude(
        'mysql', 'between', ((5, 1, 49), (5, 1, 52)),
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
            addresses_args={"query_class": MyAppenderQuery})

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


class UOWTest(
        _DynamicFixture, _fixtures.FixtureTest,
        testing.AssertsExecutionResults):

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
                select(
                    [func.count(cast(1, Integer))]).
                where(addresses.c.user_id != None)),
            0)
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
                select(
                    [func.count(cast(1, Integer))]).
                where(addresses.c.user_id != None)),
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
            addresses_args={"order_by": addresses.c.email_address})
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
            addresses_args={"order_by": addresses.c.email_address})
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

    def test_noload_append(self):
        # test that a load of User.addresses is not emitted
        # when flushing an append
        User, Address = self._user_address_fixture()

        sess = Session()
        u1 = User(name="jack", addresses=[Address(email_address="a1")])
        sess.add(u1)
        sess.commit()

        u1_id = u1.id
        sess.expire_all()

        u1.addresses.append(Address(email_address='a2'))

        self.assert_sql_execution(
            testing.db,
            sess.flush,
            CompiledSQL(
                "SELECT users.id AS users_id, users.name AS users_name "
                "FROM users WHERE users.id = :param_1",
                lambda ctx: [{"param_1": u1_id}]),
            CompiledSQL(
                "INSERT INTO addresses (user_id, email_address) "
                "VALUES (:user_id, :email_address)",
                lambda ctx: [{'email_address': 'a2', 'user_id': u1_id}]
            )
        )

    def test_noload_remove(self):
        # test that a load of User.addresses is not emitted
        # when flushing a remove
        User, Address = self._user_address_fixture()

        sess = Session()
        u1 = User(name="jack", addresses=[Address(email_address="a1")])
        a2 = Address(email_address='a2')
        u1.addresses.append(a2)
        sess.add(u1)
        sess.commit()

        u1_id = u1.id
        a2_id = a2.id
        sess.expire_all()

        u1.addresses.remove(a2)

        self.assert_sql_execution(
            testing.db,
            sess.flush,
            CompiledSQL(
                "SELECT addresses.id AS addresses_id, addresses.email_address "
                "AS addresses_email_address FROM addresses "
                "WHERE addresses.id = :param_1",
                lambda ctx: [{'param_1': a2_id}]
            ),
            CompiledSQL(
                "UPDATE addresses SET user_id=:user_id WHERE addresses.id = "
                ":addresses_id",
                lambda ctx: [{'addresses_id': a2_id, 'user_id': None}]
            ),
            CompiledSQL(
                "SELECT users.id AS users_id, users.name AS users_name "
                "FROM users WHERE users.id = :param_1",
                lambda ctx: [{"param_1": u1_id}]),
        )

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
        User, Address = self._user_address_fixture(
            addresses_args={
                "order_by": addresses.c.id,
                "backref": "user",
                "cascade": "save-update" if expected else "all, delete"})

        sess = create_session(autoflush=True, autocommit=False)
        u = User(name='ed')
        u.addresses.extend(
            [Address(email_address=letter) for letter in 'abcdef']
        )
        sess.add(u)
        sess.commit()
        eq_(
            testing.db.scalar(
                select([func.count('*')]).where(addresses.c.user_id == None)),
            0)
        eq_(
            testing.db.scalar(
                select([func.count('*')]).where(addresses.c.user_id != None)),
            6)

        sess.delete(u)

        sess.commit()

        if expected:
            eq_(
                testing.db.scalar(
                    select([func.count('*')]).where(
                        addresses.c.user_id == None
                    )
                ),
                6
            )
            eq_(
                testing.db.scalar(
                    select([func.count('*')]).where(
                        addresses.c.user_id != None
                    )
                ),
                0
            )
        else:
            eq_(
                testing.db.scalar(
                    select([func.count('*')]).select_from(addresses)
                ),
                0)

    def test_delete_nocascade(self):
        self._test_delete_cascade(True)

    def test_delete_cascade(self):
        self._test_delete_cascade(False)

    def test_self_referential(self):
        Node, nodes = self.classes.Node, self.tables.nodes

        mapper(
            Node, nodes, properties={
                'children': relationship(
                    Node, lazy="dynamic", order_by=nodes.c.id)})

        sess = Session()
        n2, n3 = Node(), Node()
        n1 = Node(children=[n2, n3])
        sess.add(n1)
        sess.commit()

        eq_(n1.children.all(), [n2, n3])

    def test_remove_orphans(self):
        addresses = self.tables.addresses
        User, Address = self._user_address_fixture(
            addresses_args={
                "order_by": addresses.c.id,
                "backref": "user",
                "cascade": "all, delete-orphan"})

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
        User, Address = self._user_address_fixture(
            addresses_args={"backref": "user"})
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

    def test_backref_events(self):
        User, Address = self._user_address_fixture(
            addresses_args={"backref": "user"})

        u1 = User()
        a1 = Address()
        u1.addresses.append(a1)
        is_(a1.user, u1)

    def test_no_deref(self):
        User, Address = self._user_address_fixture(
            addresses_args={"backref": "user", })

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
            return session.query(User).first().addresses.all()

        eq_(query1(), [Address(email_address='joe@joesdomain.example')])
        eq_(query2(), [Address(email_address='joe@joesdomain.example')])
        eq_(query3(), [Address(email_address='joe@joesdomain.example')])


class HistoryTest(_DynamicFixture, _fixtures.FixtureTest):
    run_inserts = None

    def _transient_fixture(self, addresses_args={}):
        User, Address = self._user_address_fixture(
            addresses_args=addresses_args)

        u1 = User()
        a1 = Address()
        return u1, a1

    def _persistent_fixture(self, autoflush=True, addresses_args={}):
        User, Address = self._user_address_fixture(
            addresses_args=addresses_args)

        u1 = User(name='u1')
        a1 = Address(email_address='a1')
        s = Session(autoflush=autoflush)
        s.add(u1)
        s.flush()
        return u1, a1, s

    def _persistent_m2m_fixture(self, autoflush=True, items_args={}):
        Order, Item = self._order_item_fixture(items_args=items_args)

        o1 = Order()
        i1 = Item(description="i1")
        s = Session(autoflush=autoflush)
        s.add(o1)
        s.flush()
        return o1, i1, s

    def _assert_history(self, obj, compare, compare_passive=None):
        if isinstance(obj, self.classes.User):
            attrname = "addresses"
        elif isinstance(obj, self.classes.Order):
            attrname = "items"

        eq_(
            attributes.get_history(obj, attrname),
            compare
        )

        if compare_passive is None:
            compare_passive = compare

        eq_(
            attributes.get_history(obj, attrname,
                        attributes.LOAD_AGAINST_COMMITTED),
            compare_passive
        )

    def test_append_transient(self):
        u1, a1 = self._transient_fixture()
        u1.addresses.append(a1)

        self._assert_history(u1,
            ([a1], [], [])
        )

    def test_append_persistent(self):
        u1, a1, s = self._persistent_fixture()
        u1.addresses.append(a1)

        self._assert_history(u1,
            ([a1], [], [])
        )

    def test_remove_transient(self):
        u1, a1 = self._transient_fixture()
        u1.addresses.append(a1)
        u1.addresses.remove(a1)

        self._assert_history(u1,
            ([], [], [])
        )

    def test_backref_pop_transient(self):
        u1, a1 = self._transient_fixture(addresses_args={"backref": "user"})
        u1.addresses.append(a1)

        self._assert_history(u1,
            ([a1], [], []),
        )

        a1.user = None

        # removed from added
        self._assert_history(u1,
            ([], [], []),
        )

    def test_remove_persistent(self):
        u1, a1, s = self._persistent_fixture()
        u1.addresses.append(a1)
        s.flush()
        s.expire_all()

        u1.addresses.remove(a1)

        self._assert_history(u1,
            ([], [], [a1])
        )

    def test_backref_pop_persistent_autoflush_o2m_active_hist(self):
        u1, a1, s = self._persistent_fixture(
            addresses_args={"backref": backref("user", active_history=True)})
        u1.addresses.append(a1)
        s.flush()
        s.expire_all()

        a1.user = None

        self._assert_history(u1,
            ([], [], [a1]),
        )

    def test_backref_pop_persistent_autoflush_m2m(self):
        o1, i1, s = self._persistent_m2m_fixture(
            items_args={"backref": "orders"})
        o1.items.append(i1)
        s.flush()
        s.expire_all()

        i1.orders.remove(o1)

        self._assert_history(o1,
            ([], [], [i1]),
        )

    def test_backref_pop_persistent_noflush_m2m(self):
        o1, i1, s = self._persistent_m2m_fixture(
            items_args={"backref": "orders"}, autoflush=False)
        o1.items.append(i1)
        s.flush()
        s.expire_all()

        i1.orders.remove(o1)

        self._assert_history(o1,
            ([], [], [i1]),
        )

    def test_unchanged_persistent(self):
        Address = self.classes.Address

        u1, a1, s = self._persistent_fixture()
        a2, a3 = Address(email_address='a2'), Address(email_address='a3')

        u1.addresses.append(a1)
        u1.addresses.append(a2)
        s.flush()

        u1.addresses.append(a3)
        u1.addresses.remove(a2)

        self._assert_history(u1,
            ([a3], [a1], [a2]),
            compare_passive=([a3], [], [a2])
        )

    def test_replace_transient(self):
        Address = self.classes.Address

        u1, a1 = self._transient_fixture()
        a2, a3, a4, a5 = Address(email_address='a2'), \
            Address(email_address='a3'), Address(email_address='a4'), \
            Address(email_address='a5')

        u1.addresses = [a1, a2]
        u1.addresses = [a2, a3, a4, a5]

        self._assert_history(u1,
            ([a2, a3, a4, a5], [], [])
        )

    def test_replace_persistent_noflush(self):
        Address = self.classes.Address

        u1, a1, s = self._persistent_fixture(autoflush=False)
        a2, a3, a4, a5 = Address(email_address='a2'), \
            Address(email_address='a3'), Address(email_address='a4'), \
            Address(email_address='a5')

        u1.addresses = [a1, a2]
        u1.addresses = [a2, a3, a4, a5]

        self._assert_history(u1,
            ([a2, a3, a4, a5], [], [])
        )

    def test_replace_persistent_autoflush(self):
        Address = self.classes.Address

        u1, a1, s = self._persistent_fixture(autoflush=True)
        a2, a3, a4, a5 = Address(email_address='a2'), \
            Address(email_address='a3'), Address(email_address='a4'), \
            Address(email_address='a5')

        u1.addresses = [a1, a2]
        u1.addresses = [a2, a3, a4, a5]

        self._assert_history(u1,
            ([a3, a4, a5], [a2], [a1]),
            compare_passive=([a3, a4, a5], [], [a1])
        )

    def test_persistent_but_readded_noflush(self):
        u1, a1, s = self._persistent_fixture(autoflush=False)
        u1.addresses.append(a1)
        s.flush()

        u1.addresses.append(a1)

        self._assert_history(u1,
            ([], [a1], []),
            compare_passive=([a1], [], [])
        )

    def test_persistent_but_readded_autoflush(self):
        u1, a1, s = self._persistent_fixture(autoflush=True)
        u1.addresses.append(a1)
        s.flush()

        u1.addresses.append(a1)

        self._assert_history(u1,
            ([], [a1], []),
            compare_passive=([a1], [], [])
        )

    def test_missing_but_removed_noflush(self):
        u1, a1, s = self._persistent_fixture(autoflush=False)

        u1.addresses.remove(a1)

        self._assert_history(u1, ([], [], []), compare_passive=([], [], [a1]))
