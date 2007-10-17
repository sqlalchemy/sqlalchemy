import testbase
import operator
from sqlalchemy import *
from sqlalchemy.orm import *
from testlib import *
from testlib.fixtures import *

from query import QueryTest

class DynamicTest(QueryTest):
    keep_mappers = False
    
    def setup_mappers(self):
        pass

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
        self.assert_sql_count(testbase.db, go, 2)
        
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
        
        sess.clear()

        # test the test fixture a little bit
        assert User(name='jack', addresses=[Address(email_address='wrong')]) != sess.query(User).first()
        assert User(name='jack', addresses=[Address(email_address='lala@hoho.com')]) == sess.query(User).first()
        
        assert [
            User(name='jack', addresses=[Address(email_address='lala@hoho.com')]),
            User(name='ed', addresses=[Address(email_address='foo@bar.com')])
        ] == sess.query(User).all()

    def test_delete(self):
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
        
        sess.delete(u)
        sess.close()

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

    test_backref.__name__ = "test%s%s" % (
        (autoflush and "_autoflush" or ""),
        (saveuser and "_saveuser" or "_savead"),
    )
    setattr(FlushTest, test_backref.__name__, test_backref)

for autoflush in (False, True):
    for saveuser in (False, True):   
        create_backref_test(autoflush, saveuser) 

if __name__ == '__main__':
    testbase.main()
    
