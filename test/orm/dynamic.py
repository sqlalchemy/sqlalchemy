import testbase
import operator
from sqlalchemy import *
from sqlalchemy import ansisql
from sqlalchemy.orm import *
from testlib import *
from fixtures import *

from query import QueryTest

class DynamicTest(QueryTest):
    keep_mappers = False
    
    def setup_mappers(self):
        pass

    def test_basic(self):
        mapper(User, users, properties={
            'addresses':relation(mapper(Address, addresses), lazy='dynamic')
        })
        sess = create_session()
        q = sess.query(User)

        print q.filter(User.id==7).all()
        u = q.filter(User.id==7).first()
        print list(u.addresses)
        assert [User(id=7, addresses=[Address(id=1, email_address='jack@bean.com')])] == q.filter(User.id==7).all()
        assert fixtures.user_address_result == q.all()

class FlushTest(FixtureTest):
    def test_basic(self):
        mapper(User, users, properties={
            'addresses':relation(mapper(Address, addresses), lazy='dynamic')
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
        
        def go():
            assert [
                User(name='jack', addresses=[Address(email_address='lala@hoho.com')]),
                User(name='ed', addresses=[Address(email_address='foo@bar.com')])
            ] == sess.query(User).all()

        # one query for the query(User).all(), one query for each address iter(),
        # also one query for a count() on each address (the count() is an artifact of the
        # fixtures.Base class, its not intrinsic to the property)
        self.assert_sql_count(testbase.db, go, 5)
        
if __name__ == '__main__':
    testbase.main()
    