"""basic tests of lazy loaded attributes"""

from sqlalchemy.testing import assert_raises
import datetime
from sqlalchemy.orm import attributes, exc as orm_exc, configure_mappers
import sqlalchemy as sa
from sqlalchemy import testing, and_, bindparam
from sqlalchemy import Integer, String, ForeignKey, SmallInteger, Boolean
from sqlalchemy import ForeignKeyConstraint
from sqlalchemy.types import TypeDecorator
from sqlalchemy.testing.schema import Table
from sqlalchemy.testing.schema import Column
from sqlalchemy import orm
from sqlalchemy.orm import mapper, relationship, create_session, Session
from sqlalchemy.testing import eq_, is_true, is_false
from sqlalchemy.testing import fixtures
from test.orm import _fixtures
from sqlalchemy.testing.assertsql import CompiledSQL


class LazyTest(_fixtures.FixtureTest):
    run_inserts = 'once'
    run_deletes = None

    def test_basic(self):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User)

        mapper(User, users, properties={
            'addresses': relationship(
                mapper(Address, addresses), lazy='select')
        })
        sess = create_session()
        q = sess.query(User)
        eq_(
            [User(id=7,
                  addresses=[Address(id=1, email_address='jack@bean.com')])],
            q.filter(users.c.id == 7).all()
        )

    def test_needs_parent(self):
        """test the error raised when parent object is not bound."""

        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User)

        mapper(User, users, properties={
            'addresses': relationship(
                mapper(Address, addresses), lazy='select')
        })
        sess = create_session()
        q = sess.query(User)
        u = q.filter(users.c.id == 7).first()
        sess.expunge(u)
        assert_raises(orm_exc.DetachedInstanceError, getattr, u, 'addresses')

    def test_orderby(self):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User)

        mapper(User, users, properties={
            'addresses': relationship(
                mapper(Address, addresses),
                lazy='select', order_by=addresses.c.email_address),
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
        ] == q.all()

    def test_orderby_secondary(self):
        """tests that a regular mapper select on a single table can
        order by a relationship to a second table"""

        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User)

        mapper(Address, addresses)

        mapper(User, users, properties=dict(
            addresses=relationship(Address, lazy='select'),
        ))
        q = create_session().query(User)
        result = q.filter(users.c.id == addresses.c.user_id).\
            order_by(addresses.c.email_address).all()
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
        ] == result

    def test_orderby_desc(self):
        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User)

        mapper(Address, addresses)

        mapper(User, users, properties=dict(
            addresses=relationship(
                Address, lazy='select',
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
        ] == sess.query(User).all()

    def test_no_orphan(self):
        """test that a lazily loaded child object is not marked as an orphan"""

        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User)

        mapper(User, users, properties={
            'addresses': relationship(
                Address, cascade="all,delete-orphan", lazy='select')
        })
        mapper(Address, addresses)

        sess = create_session()
        user = sess.query(User).get(7)
        assert getattr(User, 'addresses').hasparent(
            attributes.instance_state(user.addresses[0]), optimistic=True)
        assert not sa.orm.class_mapper(Address)._is_orphan(
            attributes.instance_state(user.addresses[0]))

    def test_limit(self):
        """test limit operations combined with lazy-load relationships."""

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
            'items': relationship(Item, secondary=order_items, lazy='select')
        })
        mapper(User, users, properties={
            'addresses': relationship(
                mapper(Address, addresses), lazy='select'),
            'orders': relationship(Order, lazy='select')
        })

        sess = create_session()
        q = sess.query(User)

        if testing.against('mssql'):
            result = q.limit(2).all()
            assert self.static.user_all_result[:2] == result
        else:
            result = q.limit(2).offset(1).all()
            assert self.static.user_all_result[1:3] == result

    def test_distinct(self):
        users, items, order_items, orders, \
            Item, User, Address, Order, addresses = (
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
            'items': relationship(Item, secondary=order_items, lazy='select')
        })
        mapper(User, users, properties={
            'addresses': relationship(
                mapper(Address, addresses), lazy='select'),
            'orders': relationship(Order, lazy='select')
        })

        sess = create_session()
        q = sess.query(User)

        # use a union all to get a lot of rows to join against
        u2 = users.alias('u2')
        s = sa.union_all(
            u2.select(use_labels=True),
            u2.select(use_labels=True), u2.select(use_labels=True)).alias('u')
        result = q.filter(s.c.u2_id == User.id).order_by(User.id).distinct() \
            .all()
        eq_(self.static.user_all_result, result)

    def test_uselist_false_warning(self):
        """test that multiple rows received by a
        uselist=False raises a warning."""

        User, users, orders, Order = (
            self.classes.User,
            self.tables.users,
            self.tables.orders,
            self.classes.Order)

        mapper(User, users, properties={
            'order': relationship(Order, uselist=False)
        })
        mapper(Order, orders)
        s = create_session()
        u1 = s.query(User).filter(User.id == 7).one()
        assert_raises(sa.exc.SAWarning, getattr, u1, 'order')

    def test_callable_bind(self):
        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User)

        mapper(User, users, properties=dict(
            addresses=relationship(
                mapper(Address, addresses),
                lazy='select',
                primaryjoin=and_(
                    users.c.id == addresses.c.user_id,
                    users.c.name == bindparam("name", callable_=lambda: "ed")
                )
            )
        ))

        s = Session()
        ed = s.query(User).filter_by(name='ed').one()
        eq_(ed.addresses, [
            Address(id=2, user_id=8),
            Address(id=3, user_id=8),
            Address(id=4, user_id=8)
        ])

        fred = s.query(User).filter_by(name='fred').one()
        eq_(fred.addresses, [])  # fred is missing

    def test_one_to_many_scalar(self):
        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User)

        mapper(User, users, properties=dict(
            address=relationship(
                mapper(Address, addresses), lazy='select', uselist=False)
        ))
        q = create_session().query(User)
        result = q.filter(users.c.id == 7).all()
        assert [User(id=7, address=Address(id=1))] == result

    def test_many_to_one_binds(self):
        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User)

        mapper(Address, addresses,
               primary_key=[addresses.c.user_id, addresses.c.email_address])

        mapper(User, users, properties=dict(
            address=relationship(
                Address, uselist=False,
                primaryjoin=sa.and_(
                    users.c.id == addresses.c.user_id,
                    addresses.c.email_address == 'ed@bettyboop.com'))
        ))
        q = create_session().query(User)
        eq_(
            [
                User(id=7, address=None),
                User(id=8, address=Address(id=3)),
                User(id=9, address=None),
                User(id=10, address=None),
            ],
            list(q)
        )

    def test_double(self):
        """tests lazy loading with two relationships simultaneously,
        from the same table, using aliases.  """

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
            addresses=relationship(Address, lazy=True),
            open_orders=relationship(
                open_mapper,
                primaryjoin=sa.and_(
                    openorders.c.isopen == 1,
                    users.c.id == openorders.c.user_id), lazy='select'),
            closed_orders=relationship(
                closed_mapper,
                primaryjoin=sa.and_(
                    closedorders.c.isopen == 0,
                    users.c.id == closedorders.c.user_id), lazy='select')
        ))
        q = create_session().query(User)

        assert [
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

        ] == q.all()

        sess = create_session()
        user = sess.query(User).get(7)
        eq_(
            [Order(id=1), Order(id=5)],
            create_session().query(closed_mapper).with_parent(
                user, property='closed_orders').all()
        )
        eq_(
            [Order(id=3)],
            create_session().query(open_mapper).
            with_parent(user, property='open_orders').all()
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
            keywords=relationship(
                Keyword, secondary=item_keywords, lazy='select'),
        ))

        q = create_session().query(Item)
        assert self.static.item_keyword_result == q.all()

        eq_(
            self.static.item_keyword_result[0:2],
            q.join('keywords').filter(keywords.c.name == 'red').all()
        )

    def test_uses_get(self):
        """test that a simple many-to-one lazyload optimizes
        to use query.get()."""

        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User)

        for pj in (
            None,
            users.c.id == addresses.c.user_id,
            addresses.c.user_id == users.c.id
        ):
            mapper(Address, addresses, properties=dict(
                user=relationship(
                    mapper(User, users), lazy='select', primaryjoin=pj)
            ))

            sess = create_session()

            # load address
            a1 = sess.query(Address).\
                filter_by(email_address="ed@wood.com").one()

            # load user that is attached to the address
            u1 = sess.query(User).get(8)

            def go():
                # lazy load of a1.user should get it from the session
                assert a1.user is u1
            self.assert_sql_count(testing.db, go, 0)
            sa.orm.clear_mappers()

    def test_uses_get_compatible_types(self):
        """test the use_get optimization with compatible
        but non-identical types"""

        User, Address = self.classes.User, self.classes.Address

        class IntDecorator(TypeDecorator):
            impl = Integer

        class SmallintDecorator(TypeDecorator):
            impl = SmallInteger

        class SomeDBInteger(sa.Integer):
            pass

        for tt in [
            Integer,
            SmallInteger,
            IntDecorator,
            SmallintDecorator,
            SomeDBInteger,
        ]:
            m = sa.MetaData()
            users = Table(
                'users', m,
                Column(
                    'id', Integer, primary_key=True,
                    test_needs_autoincrement=True),
                Column('name', String(30), nullable=False),
            )
            addresses = Table(
                'addresses', m,
                Column(
                    'id', Integer, primary_key=True,
                    test_needs_autoincrement=True),
                Column('user_id', tt, ForeignKey('users.id')),
                Column('email_address', String(50), nullable=False),
            )

            mapper(Address, addresses, properties=dict(
                user=relationship(mapper(User, users))
            ))

            sess = create_session(bind=testing.db)

            # load address
            a1 = sess.query(Address).\
                filter_by(email_address="ed@wood.com").one()

            # load user that is attached to the address
            u1 = sess.query(User).get(8)

            def go():
                # lazy load of a1.user should get it from the session
                assert a1.user is u1
            self.assert_sql_count(testing.db, go, 0)
            sa.orm.clear_mappers()

    def test_many_to_one(self):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User)

        mapper(Address, addresses, properties=dict(
            user=relationship(mapper(User, users), lazy='select')
        ))
        sess = create_session()
        q = sess.query(Address)
        a = q.filter(addresses.c.id == 1).one()

        assert a.user is not None

        u1 = sess.query(User).get(7)

        assert a.user is u1

    def test_backrefs_dont_lazyload(self):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User)

        mapper(User, users, properties={
            'addresses': relationship(Address, backref='user')
        })
        mapper(Address, addresses)
        sess = create_session()
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

        sess.expire(u1, ['addresses'])

        def go():
            assert ad in u1.addresses
        self.assert_sql_count(testing.db, go, 1)

        sess.expire(u1, ['addresses'])
        ad2 = Address()

        def go():
            ad2.user = u1
            assert ad2.user is u1
        self.assert_sql_count(testing.db, go, 0)

        def go():
            assert ad2 in u1.addresses
        self.assert_sql_count(testing.db, go, 1)


class GetterStateTest(_fixtures.FixtureTest):

    """test lazyloader on non-existent attribute returns
    expected attribute symbols, maintain expected state"""

    run_inserts = None

    def _unhashable_fixture(self, metadata, load_on_pending=False):
        class MyHashType(sa.TypeDecorator):
            impl = sa.String(100)

            def process_bind_param(self, value, dialect):
                return ";".join(
                    "%s=%s" % (k, v)
                       for k, v in
                       sorted(value.items(), key=lambda key: key[0]))

            def process_result_value(self, value, dialect):
                return dict(elem.split("=", 1) for elem in value.split(";"))

        category = Table(
            'category', metadata,
            Column('id', Integer, primary_key=True),
            Column('data', MyHashType())
        )
        article = Table(
            'article', metadata,
            Column('id', Integer, primary_key=True),
            Column('data', MyHashType())
        )

        class Category(fixtures.ComparableEntity):
            pass

        class Article(fixtures.ComparableEntity):
            pass

        mapper(Category, category)
        mapper(Article, article, properties={
            "category": relationship(
                Category,
                primaryjoin=orm.foreign(article.c.data) == category.c.data,
                load_on_pending=load_on_pending
            )
        })

        metadata.create_all()
        sess = Session(autoflush=False)
        data = {"im": "unhashable"}
        a1 = Article(id=1, data=data)
        c1 = Category(id=1, data=data)
        if load_on_pending:
            sess.add(c1)
        else:
            sess.add_all([c1, a1])
        sess.flush()
        if load_on_pending:
            sess.add(a1)
        return Category, Article, sess, a1, c1

    def _u_ad_fixture(self, populate_user, dont_use_get=False):
        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User)

        mapper(User, users, properties={
            'addresses': relationship(Address, back_populates='user')
        })
        mapper(Address, addresses, properties={
            'user': relationship(
                User,
                primaryjoin=and_(
                    users.c.id == addresses.c.user_id, users.c.id != 27)
                if dont_use_get else None,
                back_populates='addresses'
            )
        })

        sess = create_session()
        a1 = Address(email_address='a1')
        sess.add(a1)
        if populate_user:
            a1.user = User(name='ed')
        sess.flush()
        if populate_user:
            sess.expire_all()
        return User, Address, sess, a1

    def test_no_use_get_params_missing(self):
        User, Address, sess, a1 = self._u_ad_fixture(False, True)

        def go():
            eq_(a1.user, None)

        # doesn't emit SQL
        self.assert_sql_count(
            testing.db,
            go,
            0
        )

    @testing.provide_metadata
    def test_no_use_get_params_not_hashable(self):
        Category, Article, sess, a1, c1 = \
            self._unhashable_fixture(self.metadata)

        def go():
            eq_(a1.category, c1)

        self.assert_sql_count(
            testing.db,
            go,
            1
        )

    @testing.provide_metadata
    def test_no_use_get_params_not_hashable_on_pending(self):
        Category, Article, sess, a1, c1 = \
            self._unhashable_fixture(self.metadata, load_on_pending=True)

        def go():
            eq_(a1.category, c1)

        self.assert_sql_count(
            testing.db,
            go,
            1
        )

    def test_get_empty_passive_return_never_set(self):
        User, Address, sess, a1 = self._u_ad_fixture(False)
        eq_(
            Address.user.impl.get(
                attributes.instance_state(a1),
                attributes.instance_dict(a1),
                passive=attributes.PASSIVE_RETURN_NEVER_SET),
            attributes.NEVER_SET
        )
        assert 'user_id' not in a1.__dict__
        assert 'user' not in a1.__dict__

    def test_history_empty_passive_return_never_set(self):
        User, Address, sess, a1 = self._u_ad_fixture(False)
        eq_(
            Address.user.impl.get_history(
                attributes.instance_state(a1),
                attributes.instance_dict(a1),
                passive=attributes.PASSIVE_RETURN_NEVER_SET),
            ((), (), ())
        )
        assert 'user_id' not in a1.__dict__
        assert 'user' not in a1.__dict__

    def test_get_empty_passive_no_initialize(self):
        User, Address, sess, a1 = self._u_ad_fixture(False)
        eq_(
            Address.user.impl.get(
                attributes.instance_state(a1),
                attributes.instance_dict(a1),
                passive=attributes.PASSIVE_NO_INITIALIZE),
            attributes.PASSIVE_NO_RESULT
        )
        assert 'user_id' not in a1.__dict__
        assert 'user' not in a1.__dict__

    def test_history_empty_passive_no_initialize(self):
        User, Address, sess, a1 = self._u_ad_fixture(False)
        eq_(
            Address.user.impl.get_history(
                attributes.instance_state(a1),
                attributes.instance_dict(a1),
                passive=attributes.PASSIVE_NO_INITIALIZE),
            attributes.HISTORY_BLANK
        )
        assert 'user_id' not in a1.__dict__
        assert 'user' not in a1.__dict__

    def test_get_populated_passive_no_initialize(self):
        User, Address, sess, a1 = self._u_ad_fixture(True)
        eq_(
            Address.user.impl.get(
                attributes.instance_state(a1),
                attributes.instance_dict(a1),
                passive=attributes.PASSIVE_NO_INITIALIZE),
            attributes.PASSIVE_NO_RESULT
        )
        assert 'user_id' not in a1.__dict__
        assert 'user' not in a1.__dict__

    def test_history_populated_passive_no_initialize(self):
        User, Address, sess, a1 = self._u_ad_fixture(True)
        eq_(
            Address.user.impl.get_history(
                attributes.instance_state(a1),
                attributes.instance_dict(a1),
                passive=attributes.PASSIVE_NO_INITIALIZE),
            attributes.HISTORY_BLANK
        )
        assert 'user_id' not in a1.__dict__
        assert 'user' not in a1.__dict__

    def test_get_populated_passive_return_never_set(self):
        User, Address, sess, a1 = self._u_ad_fixture(True)
        eq_(
            Address.user.impl.get(
                attributes.instance_state(a1),
                attributes.instance_dict(a1),
                passive=attributes.PASSIVE_RETURN_NEVER_SET),
            User(name='ed')
        )

    def test_history_populated_passive_return_never_set(self):
        User, Address, sess, a1 = self._u_ad_fixture(True)
        eq_(
            Address.user.impl.get_history(
                attributes.instance_state(a1),
                attributes.instance_dict(a1),
                passive=attributes.PASSIVE_RETURN_NEVER_SET),
            ((), [User(name='ed'), ], ())
        )


class M2OGetTest(_fixtures.FixtureTest):
    run_inserts = 'once'
    run_deletes = None

    def test_m2o_noload(self):
        """test that a NULL foreign key doesn't trigger a lazy load"""

        users, Address, addresses, User = (
            self.tables.users,
            self.classes.Address,
            self.tables.addresses,
            self.classes.User)

        mapper(User, users)

        mapper(Address, addresses, properties={
            'user': relationship(User)
        })

        sess = create_session()
        ad1 = Address(email_address='somenewaddress', id=12)
        sess.add(ad1)
        sess.flush()
        sess.expunge_all()

        ad2 = sess.query(Address).get(1)
        ad3 = sess.query(Address).get(ad1.id)

        def go():
            # one lazy load
            assert ad2.user.name == 'jack'
            # no lazy load
            assert ad3.user is None
        self.assert_sql_count(testing.db, go, 1)


class CorrelatedTest(fixtures.MappedTest):

    @classmethod
    def define_tables(self, meta):
        Table('user_t', meta,
              Column('id', Integer, primary_key=True),
              Column('name', String(50)))

        Table('stuff', meta,
              Column('id', Integer, primary_key=True),
              Column('date', sa.Date),
              Column('user_id', Integer, ForeignKey('user_t.id')))

    @classmethod
    def insert_data(cls):
        stuff, user_t = cls.tables.stuff, cls.tables.user_t

        user_t.insert().execute(
            {'id': 1, 'name': 'user1'},
            {'id': 2, 'name': 'user2'},
            {'id': 3, 'name': 'user3'})

        stuff.insert().execute(
            {'id': 1, 'user_id': 1, 'date': datetime.date(2007, 10, 15)},
            {'id': 2, 'user_id': 1, 'date': datetime.date(2007, 12, 15)},
            {'id': 3, 'user_id': 1, 'date': datetime.date(2007, 11, 15)},
            {'id': 4, 'user_id': 2, 'date': datetime.date(2008, 1, 15)},
            {'id': 5, 'user_id': 3, 'date': datetime.date(2007, 6, 15)})

    def test_correlated_lazyload(self):
        stuff, user_t = self.tables.stuff, self.tables.user_t

        class User(fixtures.ComparableEntity):
            pass

        class Stuff(fixtures.ComparableEntity):
            pass

        mapper(Stuff, stuff)

        stuff_view = sa.select([stuff.c.id]).\
            where(stuff.c.user_id == user_t.c.id).correlate(user_t).\
            order_by(sa.desc(stuff.c.date)).limit(1)

        mapper(User, user_t, properties={
            'stuff': relationship(
                Stuff,
                primaryjoin=sa.and_(
                    user_t.c.id == stuff.c.user_id,
                    stuff.c.id == (stuff_view.as_scalar())))
        })

        sess = create_session()

        eq_(
            sess.query(User).all(),
            [
                User(
                    name='user1',
                    stuff=[Stuff(date=datetime.date(2007, 12, 15), id=2)]),
                User(
                    name='user2',
                    stuff=[Stuff(id=4, date=datetime.date(2008, 1, 15))]),
                User(
                    name='user3',
                    stuff=[Stuff(id=5, date=datetime.date(2007, 6, 15))])
            ]
        )


class O2MWOSideFixedTest(fixtures.MappedTest):
    # test #2948 - o2m backref with a "m2o does/does not count"
    # criteria doesn't scan the "o" table

    @classmethod
    def define_tables(self, meta):
        Table('city', meta,
              Column('id', Integer, primary_key=True),
              Column('deleted', Boolean),
              )
        Table('person', meta,
              Column('id', Integer, primary_key=True),
              Column('city_id', ForeignKey('city.id'))
              )

    @classmethod
    def setup_classes(cls):
        class Person(cls.Basic):
            pass

        class City(cls.Basic):
            pass

    @classmethod
    def setup_mappers(cls):
        Person, City = cls.classes.Person, cls.classes.City
        city, person = cls.tables.city, cls.tables.person

        mapper(Person, person, properties={
            'city': relationship(City,
                                 primaryjoin=and_(
                                     person.c.city_id == city.c.id,
                                     city.c.deleted == False),  # noqa
                                 backref='people')
        })
        mapper(City, city)

    def _fixture(self, include_other):
        city, person = self.tables.city, self.tables.person

        if include_other:
            city.insert().execute(
                {"id": 1, "deleted": False},
            )

            person.insert().execute(
                {"id": 1, "city_id": 1},
                {"id": 2, "city_id": 1},
            )

        city.insert().execute(
            {"id": 2, "deleted": True},
        )

        person.insert().execute(
            {"id": 3, "city_id": 2},
            {"id": 4, "city_id": 2},
        )

    def test_lazyload_assert_expected_sql(self):
        self._fixture(True)
        City = self.classes.City
        sess = Session(testing.db)
        c1, c2 = sess.query(City).order_by(City.id).all()

        def go():
            eq_(
                [p.id for p in c2.people],
                []
            )

        self.assert_sql_execution(
            testing.db,
            go,
            CompiledSQL(
                "SELECT person.id AS person_id, person.city_id AS "
                "person_city_id FROM person "
                "WHERE person.city_id = :param_1 AND :param_2 = 0",
                {"param_1": 2, "param_2": 1}
            )
        )

    def test_lazyload_people_other_exists(self):
        self._fixture(True)
        City = self.classes.City
        sess = Session(testing.db)
        c1, c2 = sess.query(City).order_by(City.id).all()
        eq_(
            [p.id for p in c1.people],
            [1, 2]
        )

        eq_(
            [p.id for p in c2.people],
            []
        )

    def test_lazyload_people_no_other_exists(self):
        # note that if we revert #2948, *this still passes!*
        # e.g. due to the scan of the "o" table, whether or not *another*
        # row exists determines if this works.

        self._fixture(False)
        City = self.classes.City
        sess = Session(testing.db)
        c2, = sess.query(City).order_by(City.id).all()

        eq_(
            [p.id for p in c2.people],
            []
        )


class RefersToSelfLazyLoadInterferenceTest(fixtures.MappedTest):
    """Test [issue:3145].

    This involves an object that refers to itself, which isn't
    entirely a supported use case.   Here, we're able to fix it,
    but long term it's not clear if future needs will affect this.
    The use case is not super-critical.

    """

    @classmethod
    def define_tables(cls, metadata):
        Table(
            'a', metadata,
            Column('a_id', Integer, primary_key=True),
            Column('b_id', ForeignKey('b.b_id')),
        )

        Table(
            'b', metadata,
            Column('b_id', Integer, primary_key=True),
            Column('parent_id', ForeignKey('b.b_id')),
        )

        Table(
            'c', metadata,
            Column('c_id', Integer, primary_key=True),
            Column('b_id', ForeignKey('b.b_id')),
        )

    @classmethod
    def setup_classes(cls):
        class A(cls.Basic):
            pass

        class B(cls.Basic):
            pass

        class C(cls.Basic):
            pass

    @classmethod
    def setup_mappers(cls):
        mapper(cls.classes.A, cls.tables.a, properties={
            "b": relationship(cls.classes.B)
        })
        bm = mapper(cls.classes.B, cls.tables.b, properties={
            "parent": relationship(
                cls.classes.B, remote_side=cls.tables.b.c.b_id),
            "zc": relationship(cls.classes.C)
        })
        mapper(cls.classes.C, cls.tables.c)

        bmp = bm._props
        configure_mappers()
        # Bug is order-dependent, must sort the "zc" property to the end
        bmp.sort()

    def test_lazy_doesnt_interfere(self):
        A, B, C = self.classes("A", "B", "C")

        session = Session()
        b = B()
        session.add(b)
        session.flush()

        b.parent_id = b.b_id

        b.zc.append(C())
        b.zc.append(C())
        session.commit()

        # If the bug is here, the next line throws an exception
        session.query(B).options(
            sa.orm.joinedload('parent').joinedload('zc')).all()


class TypeCoerceTest(fixtures.MappedTest, testing.AssertsExecutionResults,):
    """ORM-level test for [ticket:3531]"""

    # mysql is having a recursion issue in the bind_expression
    __only_on__ = ('sqlite', 'postgresql')

    class StringAsInt(TypeDecorator):
        impl = String(50)

        def column_expression(self, col):
            return sa.cast(col, Integer)

        def bind_expression(self, col):
            return sa.cast(col, String)

    @classmethod
    def define_tables(cls, metadata):
        Table(
            'person', metadata,
            Column("id", cls.StringAsInt, primary_key=True),
        )
        Table(
            "pets", metadata,
            Column("id", Integer, primary_key=True),
            Column("person_id", Integer),
        )

    @classmethod
    def setup_classes(cls):
        class Person(cls.Basic):
            pass

        class Pet(cls.Basic):
            pass

    @classmethod
    def setup_mappers(cls):
        mapper(cls.classes.Person, cls.tables.person, properties=dict(
            pets=relationship(
                cls.classes.Pet, primaryjoin=(
                    orm.foreign(cls.tables.pets.c.person_id) ==
                    sa.cast(
                        sa.type_coerce(cls.tables.person.c.id, Integer),
                        Integer
                    )
                )
            )
        ))

        mapper(cls.classes.Pet, cls.tables.pets)

    def test_lazyload_singlecast(self):
        Person = self.classes.Person
        Pet = self.classes.Pet

        s = Session()
        s.add_all([
            Person(id=5), Pet(id=1, person_id=5)
        ])
        s.commit()

        p1 = s.query(Person).first()

        with self.sql_execution_asserter() as asserter:
            p1.pets

        asserter.assert_(
            CompiledSQL(
                "SELECT pets.id AS pets_id, pets.person_id "
                "AS pets_person_id FROM pets "
                "WHERE pets.person_id = CAST(:param_1 AS INTEGER)",
                [{'param_1': 5}]
            )
        )


class CompositeSimpleM2OTest(fixtures.MappedTest):
    """ORM-level test for [ticket:3788]"""

    @classmethod
    def define_tables(cls, metadata):
        Table(
            'a', metadata,
            Column("id1", Integer, primary_key=True),
            Column("id2", Integer, primary_key=True),
        )

        Table(
            "b_sameorder", metadata,
            Column("id", Integer, primary_key=True),
            Column('a_id1', Integer),
            Column('a_id2', Integer),
            ForeignKeyConstraint(['a_id1', 'a_id2'], ['a.id1', 'a.id2'])
        )

        Table(
            "b_differentorder", metadata,
            Column("id", Integer, primary_key=True),
            Column('a_id1', Integer),
            Column('a_id2', Integer),
            ForeignKeyConstraint(['a_id1', 'a_id2'], ['a.id1', 'a.id2'])
        )

    @classmethod
    def setup_classes(cls):
        class A(cls.Basic):
            pass

        class B(cls.Basic):
            pass

    def test_use_get_sameorder(self):
        mapper(self.classes.A, self.tables.a)
        m_b = mapper(self.classes.B, self.tables.b_sameorder, properties={
            'a': relationship(self.classes.A)
        })

        configure_mappers()
        is_true(m_b.relationships.a.strategy.use_get)

    def test_use_get_reverseorder(self):
        mapper(self.classes.A, self.tables.a)
        m_b = mapper(self.classes.B, self.tables.b_differentorder, properties={
            'a': relationship(self.classes.A)
        })

        configure_mappers()
        is_true(m_b.relationships.a.strategy.use_get)

    def test_dont_use_get_pj_is_different(self):
        mapper(self.classes.A, self.tables.a)
        m_b = mapper(self.classes.B, self.tables.b_sameorder, properties={
            'a': relationship(self.classes.A, primaryjoin=and_(
                self.tables.a.c.id1 == self.tables.b_sameorder.c.a_id1,
                self.tables.a.c.id2 == 12
            ))
        })

        configure_mappers()
        is_false(m_b.relationships.a.strategy.use_get)
