import testenv; testenv.configure_for_tests()
import operator
from sqlalchemy import *
from sqlalchemy.orm import *
from testlib import *
from testlib.fixtures import *

from query import QueryTest

class DynamicTest(FixtureTest):
    keep_mappers = False
    refresh_data = True

    def test_basic(self):
        mapper(User, users, properties={
            'addresses':dynamic_loader(mapper(Address, addresses))
        })
        sess = create_session()
        q = sess.query(User)

        print q.filter(User.id==7).all()
        u = q.filter(User.id==7).first()
        print list(u.addresses)
        assert [User(id=7, addresses=[Address(id=1, email_address='jack@bean.com')])] == q.filter(User.id==7).all()
        assert fixtures.user_address_result == q.all()
    
    def test_order_by(self):
        mapper(User, users, properties={
            'addresses':dynamic_loader(mapper(Address, addresses))
        })
        sess = create_session()
        u = sess.query(User).get(8)
        self.assertEquals(list(u.addresses.order_by(desc(Address.email_address))), [Address(email_address=u'ed@wood.com'), Address(email_address=u'ed@lala.com'), Address(email_address=u'ed@bettyboop.com')])

    def test_configured_order_by(self):
        mapper(User, users, properties={
            'addresses':dynamic_loader(mapper(Address, addresses), order_by=desc(Address.email_address))
        })
        sess = create_session()
        u = sess.query(User).get(8)
        self.assertEquals(list(u.addresses), [Address(email_address=u'ed@wood.com'), Address(email_address=u'ed@lala.com'), Address(email_address=u'ed@bettyboop.com')])
        
    def test_count(self):
        mapper(User, users, properties={
            'addresses':dynamic_loader(mapper(Address, addresses))
        })
        sess = create_session()
        u = sess.query(User).first()
        assert u.addresses.count() == 1, u.addresses.count()

    def test_backref(self):
        mapper(Address, addresses, properties={
            'user':relation(User, backref=backref('addresses', lazy='dynamic'))
        })
        mapper(User, users)

        sess = create_session()
        ad = sess.query(Address).get(1)
        def go():
            ad.user = None
        self.assert_sql_count(testing.db, go, 1)
        sess.flush()
        u = sess.query(User).get(7)
        assert ad not in u.addresses

    def test_no_count(self):
        mapper(User, users, properties={
            'addresses':dynamic_loader(mapper(Address, addresses))
        })
        sess = create_session()
        q = sess.query(User)

        # dynamic collection cannot implement __len__() (at least one that returns a live database
        # result), else additional count() queries are issued when evaluating in a list context
        def go():
            assert [User(id=7, addresses=[Address(id=1, email_address='jack@bean.com')])] == q.filter(User.id==7).all()
        self.assert_sql_count(testing.db, go, 2)

    def test_m2m(self):
        mapper(Order, orders, properties={
            'items':relation(Item, secondary=order_items, lazy="dynamic", backref=backref('orders', lazy="dynamic"))
        })
        mapper(Item, items)

        sess = create_session()
        o1 = Order(id=15, description="order 10")
        i1 = Item(id=10, description="item 8")
        o1.items.append(i1)
        sess.save(o1)
        sess.flush()

        assert o1 in i1.orders.all()
        assert i1 in o1.items.all()
    
    def test_transient_detached(self):
        mapper(User, users, properties={
            'addresses':dynamic_loader(mapper(Address, addresses))
        })
        sess = create_session()
        u1 = User()
        u1.addresses.append(Address())
        assert u1.addresses.count() == 1
        assert u1.addresses[0] == Address()
        
class FlushTest(FixtureTest):
    def test_basic(self):
        class Fixture(Base):
            pass

        mapper(User, users, properties={
            'addresses':dynamic_loader(mapper(Address, addresses))
        })
        sess = create_session()
        u1 = User(name='jack')
        u2 = User(name='ed')
        u2.addresses.append(Address(email_address='foo@bar.com'))
        u1.addresses.append(Address(email_address='lala@hoho.com'))
        sess.save(u1)
        sess.save(u2)
        sess.flush()
        
        from sqlalchemy.orm import attributes
        self.assertEquals(attributes.get_history(u1._state, 'addresses'), ([], [Address(email_address='lala@hoho.com')], []))
        
        sess.clear()

        # test the test fixture a little bit
        assert User(name='jack', addresses=[Address(email_address='wrong')]) != sess.query(User).first()
        assert User(name='jack', addresses=[Address(email_address='lala@hoho.com')]) == sess.query(User).first()

        assert [
            User(name='jack', addresses=[Address(email_address='lala@hoho.com')]),
            User(name='ed', addresses=[Address(email_address='foo@bar.com')])
        ] == sess.query(User).all()
    
    def test_hasattr(self):
        mapper(User, users, properties={
            'addresses':dynamic_loader(mapper(Address, addresses))
        })
        u1 = User(name='jack')
        
        assert 'addresses' not in u1.__dict__.keys()
        u1.addresses = [Address(email_address='test')]
        assert 'addresses' in dir(u1)
        
    def test_rollback(self):
        mapper(User, users, properties={
            'addresses':dynamic_loader(mapper(Address, addresses))
        })
        sess = create_session(transactional=True, autoflush=True)
        u1 = User(name='jack')
        u1.addresses.append(Address(email_address='lala@hoho.com'))
        sess.save(u1)
        sess.flush()
        sess.commit()
        u1.addresses.append(Address(email_address='foo@bar.com'))
        self.assertEquals(u1.addresses.all(), [Address(email_address='lala@hoho.com'), Address(email_address='foo@bar.com')])
        sess.rollback()
        self.assertEquals(u1.addresses.all(), [Address(email_address='lala@hoho.com')])

    @testing.fails_on('maxdb')
    def test_delete_nocascade(self):
        mapper(User, users, properties={
            'addresses':dynamic_loader(mapper(Address, addresses), backref='user')
        })
        sess = create_session(autoflush=True)
        u = User(name='ed')
        u.addresses.append(Address(email_address='a'))
        u.addresses.append(Address(email_address='b'))
        u.addresses.append(Address(email_address='c'))
        u.addresses.append(Address(email_address='d'))
        u.addresses.append(Address(email_address='e'))
        u.addresses.append(Address(email_address='f'))
        sess.save(u)

        assert Address(email_address='c') == u.addresses[2]
        sess.delete(u.addresses[2])
        sess.delete(u.addresses[4])
        sess.delete(u.addresses[3])
        assert [Address(email_address='a'), Address(email_address='b'), Address(email_address='d')] == list(u.addresses)

        sess.clear()
        u = sess.query(User).get(u.id)

        sess.delete(u)

        # u.addresses relation will have to force the load
        # of all addresses so that they can be updated
        sess.flush()
        sess.close()

        assert testing.db.scalar(addresses.count(addresses.c.user_id != None)) ==0

    @testing.fails_on('maxdb')
    def test_delete_cascade(self):
        mapper(User, users, properties={
            'addresses':dynamic_loader(mapper(Address, addresses), backref='user', cascade="all, delete-orphan")
        })
        sess = create_session(autoflush=True)
        u = User(name='ed')
        u.addresses.append(Address(email_address='a'))
        u.addresses.append(Address(email_address='b'))
        u.addresses.append(Address(email_address='c'))
        u.addresses.append(Address(email_address='d'))
        u.addresses.append(Address(email_address='e'))
        u.addresses.append(Address(email_address='f'))
        sess.save(u)

        assert Address(email_address='c') == u.addresses[2]
        sess.delete(u.addresses[2])
        sess.delete(u.addresses[4])
        sess.delete(u.addresses[3])
        assert [Address(email_address='a'), Address(email_address='b'), Address(email_address='d')] == list(u.addresses)
        
        sess.clear()
        u = sess.query(User).get(u.id)
        
        sess.delete(u)

        # u.addresses relation will have to force the load
        # of all addresses so that they can be updated
        sess.flush()
        sess.close()

        assert testing.db.scalar(addresses.count()) ==0

    @testing.fails_on('maxdb')
    def test_remove_orphans(self):
        mapper(User, users, properties={
            'addresses':dynamic_loader(mapper(Address, addresses), cascade="all, delete-orphan", backref='user')
        })
        sess = create_session(autoflush=True)
        u = User(name='ed')
        u.addresses.append(Address(email_address='a'))
        u.addresses.append(Address(email_address='b'))
        u.addresses.append(Address(email_address='c'))
        u.addresses.append(Address(email_address='d'))
        u.addresses.append(Address(email_address='e'))
        u.addresses.append(Address(email_address='f'))
        sess.save(u)

        assert [Address(email_address='a'), Address(email_address='b'), Address(email_address='c'),
            Address(email_address='d'), Address(email_address='e'), Address(email_address='f')] == sess.query(Address).all()

        assert Address(email_address='c') == u.addresses[2]

        try:
            del u.addresses[3]
            assert False
        except TypeError, e:
            assert "doesn't support item deletion" in str(e), str(e)

        for a in u.addresses.filter(Address.email_address.in_(['c', 'e', 'f'])):
            u.addresses.remove(a)

        assert [Address(email_address='a'), Address(email_address='b'), Address(email_address='d')] == list(u.addresses)

        assert [Address(email_address='a'), Address(email_address='b'), Address(email_address='d')] == sess.query(Address).all()

        sess.delete(u)
        sess.close()


def create_backref_test(autoflush, saveuser):
    def test_backref(self):
        mapper(User, users, properties={
            'addresses':dynamic_loader(mapper(Address, addresses), backref='user')
        })
        sess = create_session(autoflush=autoflush)

        u = User(name='buffy')

        a = Address(email_address='foo@bar.com')
        a.user = u

        if saveuser:
            sess.save(u)
        else:
            sess.save(a)

        if not autoflush:
            sess.flush()

        assert u in sess
        assert a in sess

        self.assert_(list(u.addresses) == [a])

        a.user = None
        if not autoflush:
            self.assert_(list(u.addresses) == [a])

        if not autoflush:
            sess.flush()
        self.assert_(list(u.addresses) == [])

    test_backref = _function_named(
        test_backref, "test%s%s" % ((autoflush and "_autoflush" or ""),
                                    (saveuser and "_saveuser" or "_savead")))
    setattr(FlushTest, test_backref.__name__, test_backref)

for autoflush in (False, True):
    for saveuser in (False, True):
        create_backref_test(autoflush, saveuser)

class DontDereferenceTest(ORMTest):
    def define_tables(self, metadata):
        global users_table, addresses_table

        users_table = Table('users', metadata,
                           Column('id', Integer, primary_key=True),
                           Column('name', String(40)),
                           Column('fullname', String(100)),
                           Column('password', String(15)))

        addresses_table = Table('addresses', metadata,
                                Column('id', Integer, primary_key=True),
                                Column('email_address', String(100), nullable=False),
                                Column('user_id', Integer, ForeignKey('users.id')))
    def test_no_deref(self):
        mapper(User, users_table, properties={
            'addresses': relation(Address, backref='user', lazy='dynamic')
            })

        mapper(Address, addresses_table)

        session = create_session()
        user = User()
        user.name = 'joe'
        user.fullname = 'Joe User'
        user.password = 'Joe\'s secret'
        address = Address()
        address.email_address = 'joe@joesdomain.example'
        address.user = user
        session.save(user)
        session.flush()
        session.clear()

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

        self.assertEquals(query1(), [Address(email_address='joe@joesdomain.example')]  )
        self.assertEquals(query2(), [Address(email_address='joe@joesdomain.example')]  )
        self.assertEquals(query3(), [Address(email_address='joe@joesdomain.example')]  )


if __name__ == '__main__':
    testenv.main()
