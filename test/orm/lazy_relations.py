from sqlalchemy import *
from sqlalchemy.orm import *
import testbase

from fixtures import *
from query import QueryTest

class LazyTest(QueryTest):
    keep_mappers = False

    def setup_mappers(self):
        pass
        
    def test_basic(self):
        mapper(User, users, properties={
            'addresses':relation(mapper(Address, addresses), lazy=True)
        })
        sess = create_session()
        q = sess.query(User)
        assert [User(id=7, addresses=[Address(id=1, email_address='jack@bean.com')])] == q.filter(users.c.id == 7).all()

    def test_bindstosession(self):
        """test that lazy loaders use the mapper's contextual session if the parent instance
        is not in a session, and that an error is raised if no contextual session"""
        
        from sqlalchemy.ext.sessioncontext import SessionContext
        ctx = SessionContext(create_session)
        m = mapper(User, users, properties = dict(
            addresses = relation(mapper(Address, addresses, extension=ctx.mapper_extension), lazy=True)
        ), extension=ctx.mapper_extension)
        q = ctx.current.query(m)
        u = q.filter(users.c.id == 7).first()
        ctx.current.expunge(u)
        assert User(id=7, addresses=[Address(id=1, email_address='jack@bean.com')]) == u

        clear_mappers()

        mapper(User, users, properties={
            'addresses':relation(mapper(Address, addresses), lazy=True)
        })
        try:
            sess = create_session()
            q = sess.query(User)
            u = q.filter(users.c.id == 7).first()
            sess.expunge(u)
            assert User(id=7, addresses=[Address(id=1, email_address='jack@bean.com')]) == u
        except exceptions.InvalidRequestError, err:
            assert "not bound to a Session, and no contextual session" in str(err)

    def test_orderby(self):
        mapper(User, users, properties = {
            'addresses':relation(mapper(Address, addresses), lazy=True, order_by=addresses.c.email_address),
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
        """tests that a regular mapper select on a single table can order by a relation to a second table"""
        
        mapper(Address, addresses)

        mapper(User, users, properties = dict(
            addresses = relation(Address, lazy=True),
        ))
        q = create_session().query(User)
        l = q.filter(users.c.id==addresses.c.user_id).order_by(addresses.c.email_address).all()
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

    def test_orderby_desc(self):
        mapper(Address, addresses)

        mapper(User, users, properties = dict(
            addresses = relation(Address, lazy=True,  order_by=[desc(addresses.c.email_address)]),
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

        mapper(User, users, properties={
            'addresses':relation(Address, cascade="all,delete-orphan", lazy=True)
        })
        mapper(Address, addresses)

        sess = create_session()
        user = sess.query(User).get(7)
        assert getattr(User, 'addresses').hasparent(user.addresses[0], optimistic=True)
        assert not class_mapper(Address)._is_orphan(user.addresses[0])

    def test_limit(self):
        """test limit operations combined with lazy-load relationships."""
        
        mapper(Item, items)
        mapper(Order, orders, properties={
            'items':relation(Item, secondary=order_items, lazy=True)
        })
        mapper(User, users, properties={
            'addresses':relation(mapper(Address, addresses), lazy=True),
            'orders':relation(Order, lazy=True)
        })

        sess = create_session()
        q = sess.query(User)

        if testbase.db.engine.name == 'mssql':
            l = q.limit(2).all()
            assert self.user_all_result[:2] == l
        else:        
            l = q.limit(2).offset(1).all()
            print l
            print self.user_all_result[1:3]
            assert self.user_all_result[1:3] == l

    def test_distinct(self):
        mapper(Item, items)
        mapper(Order, orders, properties={
            'items':relation(Item, secondary=order_items, lazy=True)
        })
        mapper(User, users, properties={
            'addresses':relation(mapper(Address, addresses), lazy=True),
            'orders':relation(Order, lazy=True)
        })

        sess = create_session()
        q = sess.query(User)

        # use a union all to get a lot of rows to join against
        u2 = users.alias('u2')
        s = union_all(u2.select(use_labels=True), u2.select(use_labels=True), u2.select(use_labels=True)).alias('u')
        print [key for key in s.c.keys()]
        l = q.filter(s.c.u2_id==User.c.id).distinct().all()
        assert self.user_all_result == l

    def test_onetoone(self):
        mapper(User, users, properties = dict(
            address = relation(mapper(Address, addresses), lazy=True, uselist=False)
        ))
        q = create_session().query(User)
        l = q.filter(users.c.id == 7).all()
        assert [User(id=7, address=Address(id=1))] == l

if __name__ == '__main__':
    testbase.main()
