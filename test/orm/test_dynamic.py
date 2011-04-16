from test.lib.testing import eq_, ne_
import operator
from sqlalchemy.orm import dynamic_loader, backref
from test.lib import testing
from sqlalchemy import Integer, String, ForeignKey, desc, select, func
from test.lib.schema import Table, Column
from sqlalchemy.orm import mapper, relationship, create_session, Query, attributes
from sqlalchemy.orm.dynamic import AppenderMixin
from test.lib.testing import eq_, AssertsCompiledSQL, assert_raises_message, assert_raises
from test.lib import fixtures
from test.orm import _fixtures


class DynamicTest(_fixtures.FixtureTest, AssertsCompiledSQL):
    def test_basic(self):
        users, Address, addresses, User = (self.tables.users,
                                self.classes.Address,
                                self.tables.addresses,
                                self.classes.User)

        mapper(User, users, properties={
            'addresses':dynamic_loader(mapper(Address, addresses))
        })
        sess = create_session()
        q = sess.query(User)

        u = q.filter(User.id==7).first()

        eq_([User(id=7,
                  addresses=[Address(id=1, email_address='jack@bean.com')])],
            q.filter(User.id==7).all())
        eq_(self.static.user_address_result, q.all())

    def test_statement(self):
        """test that the .statement accessor returns the actual statement that
        would render, without any _clones called."""

        users, Address, addresses, User = (self.tables.users,
                                self.classes.Address,
                                self.tables.addresses,
                                self.classes.User)


        mapper(User, users, properties={
            'addresses':dynamic_loader(mapper(Address, addresses))
        })
        sess = create_session()
        q = sess.query(User)

        u = q.filter(User.id==7).first()
        self.assert_compile(
            u.addresses.statement, 
            "SELECT addresses.id, addresses.user_id, addresses.email_address FROM "
            "addresses WHERE :param_1 = addresses.user_id",
            use_default_dialect=True
        )

    def test_order_by(self):
        users, Address, addresses, User = (self.tables.users,
                                self.classes.Address,
                                self.tables.addresses,
                                self.classes.User)

        mapper(User, users, properties={
            'addresses':dynamic_loader(mapper(Address, addresses))
        })
        sess = create_session()
        u = sess.query(User).get(8)
        eq_(
            list(u.addresses.order_by(desc(Address.email_address))),
             [Address(email_address=u'ed@wood.com'), Address(email_address=u'ed@lala.com'), 
              Address(email_address=u'ed@bettyboop.com')]
            )

    def test_configured_order_by(self):
        users, Address, addresses, User = (self.tables.users,
                                self.classes.Address,
                                self.tables.addresses,
                                self.classes.User)

        mapper(User, users, properties={
            'addresses':dynamic_loader(mapper(Address, addresses), order_by=desc(Address.email_address))
        })
        sess = create_session()
        u = sess.query(User).get(8)
        eq_(list(u.addresses), [Address(email_address=u'ed@wood.com'), Address(email_address=u'ed@lala.com'), Address(email_address=u'ed@bettyboop.com')])

        # test cancellation of None, replacement with something else
        eq_(
            list(u.addresses.order_by(None).order_by(Address.email_address)),
            [Address(email_address=u'ed@bettyboop.com'), Address(email_address=u'ed@lala.com'), Address(email_address=u'ed@wood.com')]
        )

        # test cancellation of None, replacement with nothing
        eq_(
            set(u.addresses.order_by(None)),
            set([Address(email_address=u'ed@bettyboop.com'), Address(email_address=u'ed@lala.com'), Address(email_address=u'ed@wood.com')])
        )

    def test_count(self):
        users, Address, addresses, User = (self.tables.users,
                                self.classes.Address,
                                self.tables.addresses,
                                self.classes.User)

        mapper(User, users, properties={
            'addresses':dynamic_loader(mapper(Address, addresses))
        })
        sess = create_session()
        u = sess.query(User).first()
        eq_(u.addresses.count(), 1)

    def test_backref(self):
        users, Address, addresses, User = (self.tables.users,
                                self.classes.Address,
                                self.tables.addresses,
                                self.classes.User)

        mapper(Address, addresses, properties={
            'user':relationship(User, backref=backref('addresses', lazy='dynamic'))
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
        users, Address, addresses, User = (self.tables.users,
                                self.classes.Address,
                                self.tables.addresses,
                                self.classes.User)

        mapper(User, users, properties={
            'addresses':dynamic_loader(mapper(Address, addresses))
        })
        sess = create_session()
        q = sess.query(User)

        # dynamic collection cannot implement __len__() (at least one that
        # returns a live database result), else additional count() queries are
        # issued when evaluating in a list context
        def go():
            eq_([User(id=7,
                      addresses=[Address(id=1,
                                         email_address='jack@bean.com')])],
                q.filter(User.id==7).all())
        self.assert_sql_count(testing.db, go, 2)

    def test_no_populate(self):
        users, Address, addresses, User = (self.tables.users,
                                self.classes.Address,
                                self.tables.addresses,
                                self.classes.User)

        mapper(User, users, properties={
            'addresses':dynamic_loader(mapper(Address, addresses))
        })
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
            'items':relationship(Item, secondary=order_items, lazy="dynamic",
                             backref=backref('orders', lazy="dynamic"))
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
            ((5, 1,49), (5, 1, 52)),
            'https://bugs.launchpad.net/ubuntu/+source/mysql-5.1/+bug/706988')
    def test_association_nonaliased(self):
        items, Order, orders, order_items, Item = (self.tables.items,
                                self.classes.Order,
                                self.tables.orders,
                                self.tables.order_items,
                                self.classes.Item)

        mapper(Order, orders, properties={
            'items':relationship(Item, secondary=order_items, 
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
            o.items.filter(order_items.c.item_id==2).all(),
            [Item(id=2)]
        )


    def test_transient_detached(self):
        users, Address, addresses, User = (self.tables.users,
                                self.classes.Address,
                                self.tables.addresses,
                                self.classes.User)

        mapper(User, users, properties={
            'addresses':dynamic_loader(mapper(Address, addresses))
        })
        sess = create_session()
        u1 = User()
        u1.addresses.append(Address())
        eq_(u1.addresses.count(), 1)
        eq_(u1.addresses[0], Address())

    def test_custom_query(self):
        users, Address, addresses, User = (self.tables.users,
                                self.classes.Address,
                                self.tables.addresses,
                                self.classes.User)

        class MyQuery(Query):
            pass

        mapper(User, users, properties={
            'addresses':dynamic_loader(mapper(Address, addresses),
                                       query_class=MyQuery)
        })
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
        users, Address, addresses, User = (self.tables.users,
                                self.classes.Address,
                                self.tables.addresses,
                                self.classes.User)

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

        mapper(User, users, properties={
            'addresses':dynamic_loader(mapper(Address, addresses),
                                       query_class=MyAppenderQuery)
        })
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


class SessionTest(_fixtures.FixtureTest):
    run_inserts = None

    def test_events(self):
        users, Address, addresses, User = (self.tables.users,
                                self.classes.Address,
                                self.tables.addresses,
                                self.classes.User)

        mapper(User, users, properties={
            'addresses':dynamic_loader(mapper(Address, addresses))
        })
        sess = create_session()
        u1 = User(name='jack')
        a1 = Address(email_address='foo')
        sess.add_all([u1, a1])
        sess.flush()

        eq_(testing.db.scalar(select([func.count(1)]).where(addresses.c.user_id!=None)), 0)
        u1 = sess.query(User).get(u1.id)
        u1.addresses.append(a1)
        sess.flush()

        eq_(testing.db.execute(select([addresses]).where(addresses.c.user_id!=None)).fetchall(),
            [(a1.id, u1.id, 'foo')])

        u1.addresses.remove(a1)
        sess.flush()
        eq_(testing.db.scalar(select([func.count(1)]).where(addresses.c.user_id!=None)), 0)

        u1.addresses.append(a1)
        sess.flush()
        eq_(testing.db.execute(select([addresses]).where(addresses.c.user_id!=None)).fetchall(),
            [(a1.id, u1.id, 'foo')])

        a2 = Address(email_address='bar')
        u1.addresses.remove(a1)
        u1.addresses.append(a2)
        sess.flush()
        eq_(testing.db.execute(select([addresses]).where(addresses.c.user_id!=None)).fetchall(),
            [(a2.id, u1.id, 'bar')])


    def test_merge(self):
        users, Address, addresses, User = (self.tables.users,
                                self.classes.Address,
                                self.tables.addresses,
                                self.classes.User)

        mapper(User, users, properties={
            'addresses':dynamic_loader(mapper(Address, addresses), order_by=addresses.c.email_address)
        })
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

    def test_flush(self):
        users, Address, addresses, User = (self.tables.users,
                                self.classes.Address,
                                self.tables.addresses,
                                self.classes.User)

        mapper(User, users, properties={
            'addresses':dynamic_loader(mapper(Address, addresses))
        })
        sess = create_session()
        u1 = User(name='jack')
        u2 = User(name='ed')
        u2.addresses.append(Address(email_address='foo@bar.com'))
        u1.addresses.append(Address(email_address='lala@hoho.com'))
        sess.add_all((u1, u2))
        sess.flush()

        from sqlalchemy.orm import attributes
        eq_(attributes.get_history(u1, 'addresses'), ([], [Address(email_address='lala@hoho.com')], []))

        sess.expunge_all()

        # test the test fixture a little bit
        ne_(User(name='jack', addresses=[Address(email_address='wrong')]),
            sess.query(User).first())
        eq_(User(name='jack', addresses=[Address(email_address='lala@hoho.com')]),
            sess.query(User).first())

        eq_([
            User(name='jack', addresses=[Address(email_address='lala@hoho.com')]),
            User(name='ed', addresses=[Address(email_address='foo@bar.com')])
            ],
            sess.query(User).all())

    def test_hasattr(self):
        users, Address, addresses, User = (self.tables.users,
                                self.classes.Address,
                                self.tables.addresses,
                                self.classes.User)

        mapper(User, users, properties={
            'addresses':dynamic_loader(mapper(Address, addresses))
        })
        u1 = User(name='jack')

        assert 'addresses' not in u1.__dict__.keys()
        u1.addresses = [Address(email_address='test')]
        assert 'addresses' in dir(u1)

    def test_collection_set(self):
        users, Address, addresses, User = (self.tables.users,
                                self.classes.Address,
                                self.tables.addresses,
                                self.classes.User)

        mapper(User, users, properties={
            'addresses':dynamic_loader(mapper(Address, addresses), order_by=addresses.c.email_address)
        })
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
        users, Address, addresses, User = (self.tables.users,
                                self.classes.Address,
                                self.tables.addresses,
                                self.classes.User)

        mapper(User, users, properties={
            'addresses':dynamic_loader(mapper(Address, addresses))
        })
        sess = create_session(expire_on_commit=False, autocommit=False, autoflush=True)
        u1 = User(name='jack')
        u1.addresses.append(Address(email_address='lala@hoho.com'))
        sess.add(u1)
        sess.flush()
        sess.commit()
        u1.addresses.append(Address(email_address='foo@bar.com'))
        eq_(u1.addresses.order_by(Address.id).all(), 
                 [Address(email_address='lala@hoho.com'), Address(email_address='foo@bar.com')])
        sess.rollback()
        eq_(u1.addresses.all(), [Address(email_address='lala@hoho.com')])

    @testing.fails_on('maxdb', 'FIXME: unknown')
    def test_delete_nocascade(self):
        users, Address, addresses, User = (self.tables.users,
                                self.classes.Address,
                                self.tables.addresses,
                                self.classes.User)

        mapper(User, users, properties={
            'addresses':dynamic_loader(mapper(Address, addresses), order_by=Address.id,
                                       backref='user')
        })
        sess = create_session(autoflush=True, autocommit=False)
        u = User(name='ed')
        u.addresses.append(Address(email_address='a'))
        u.addresses.append(Address(email_address='b'))
        u.addresses.append(Address(email_address='c'))
        u.addresses.append(Address(email_address='d'))
        u.addresses.append(Address(email_address='e'))
        u.addresses.append(Address(email_address='f'))
        sess.add(u)

        eq_(Address(email_address='c'), u.addresses[2])
        sess.delete(u.addresses[2])
        sess.delete(u.addresses[4])
        sess.delete(u.addresses[3])
        eq_([Address(email_address='a'), Address(email_address='b'), Address(email_address='d')],
            list(u.addresses))

        sess.expunge_all()
        u = sess.query(User).get(u.id)

        sess.delete(u)

        # u.addresses relationship will have to force the load
        # of all addresses so that they can be updated
        sess.flush()
        sess.close()

        eq_(testing.db.scalar(addresses.count(addresses.c.user_id != None)), 0)

    @testing.fails_on('maxdb', 'FIXME: unknown')
    def test_delete_cascade(self):
        users, Address, addresses, User = (self.tables.users,
                                self.classes.Address,
                                self.tables.addresses,
                                self.classes.User)

        mapper(User, users, properties={
            'addresses':dynamic_loader(mapper(Address, addresses), order_by=Address.id,
                                       backref='user', cascade="all, delete-orphan")
        })
        sess = create_session(autoflush=True, autocommit=False)
        u = User(name='ed')
        u.addresses.append(Address(email_address='a'))
        u.addresses.append(Address(email_address='b'))
        u.addresses.append(Address(email_address='c'))
        u.addresses.append(Address(email_address='d'))
        u.addresses.append(Address(email_address='e'))
        u.addresses.append(Address(email_address='f'))
        sess.add(u)

        eq_(Address(email_address='c'), u.addresses[2])
        sess.delete(u.addresses[2])
        sess.delete(u.addresses[4])
        sess.delete(u.addresses[3])
        eq_([Address(email_address='a'), Address(email_address='b'), Address(email_address='d')],
            list(u.addresses))

        sess.expunge_all()
        u = sess.query(User).get(u.id)

        sess.delete(u)

        # u.addresses relationship will have to force the load
        # of all addresses so that they can be updated
        sess.flush()
        sess.close()

        eq_(testing.db.scalar(addresses.count()), 0)

    @testing.fails_on('maxdb', 'FIXME: unknown')
    def test_remove_orphans(self):
        users, Address, addresses, User = (self.tables.users,
                                self.classes.Address,
                                self.tables.addresses,
                                self.classes.User)

        mapper(User, users, properties={
            'addresses':dynamic_loader(mapper(Address, addresses), order_by=Address.id,
                                       cascade="all, delete-orphan", backref='user')
        })
        sess = create_session(autoflush=True, autocommit=False)
        u = User(name='ed')
        u.addresses.append(Address(email_address='a'))
        u.addresses.append(Address(email_address='b'))
        u.addresses.append(Address(email_address='c'))
        u.addresses.append(Address(email_address='d'))
        u.addresses.append(Address(email_address='e'))
        u.addresses.append(Address(email_address='f'))
        sess.add(u)

        eq_([Address(email_address='a'), Address(email_address='b'), Address(email_address='c'),
             Address(email_address='d'), Address(email_address='e'), Address(email_address='f')],
            sess.query(Address).all())

        eq_(Address(email_address='c'), u.addresses[2])

        def go():
            del u.addresses[3]
        assert_raises(
            TypeError,
            go
        )

        for a in u.addresses.filter(Address.email_address.in_(['c', 'e', 'f'])):
            u.addresses.remove(a)

        eq_([Address(email_address='a'), Address(email_address='b'), Address(email_address='d')],
            list(u.addresses))

        eq_([Address(email_address='a'), Address(email_address='b'), Address(email_address='d')],
            sess.query(Address).all())

        sess.delete(u)
        sess.close()

    def _backref_test(self, autoflush, saveuser):
        users, Address, addresses, User = (self.tables.users,
                                self.classes.Address,
                                self.tables.addresses,
                                self.classes.User)

        mapper(User, users, properties={
            'addresses':dynamic_loader(mapper(Address, addresses), backref='user')
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

class DontDereferenceTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table('users', metadata,
              Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
              Column('name', String(40)),
              Column('fullname', String(100)),
              Column('password', String(15)))

        Table('addresses', metadata,
              Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
              Column('email_address', String(100), nullable=False),
              Column('user_id', Integer, ForeignKey('users.id')))

    @classmethod
    def setup_mappers(cls):
        users, addresses = cls.tables.users, cls.tables.addresses

        class User(cls.Comparable):
            pass

        class Address(cls.Comparable):
            pass

        mapper(User, users, properties={
            'addresses': relationship(Address, backref='user', lazy='dynamic')
            })
        mapper(Address, addresses)

    def test_no_deref(self):
        User, Address = self.classes.User, self.classes.Address

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


