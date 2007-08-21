"""basic tests of lazy loaded attributes"""

import testbase
from sqlalchemy import *
from sqlalchemy import exceptions
from sqlalchemy.orm import *
from testlib import *
from testlib.fixtures import *
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
            assert False
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
            assert fixtures.user_all_result[:2] == l
        else:        
            l = q.limit(2).offset(1).all()
            assert fixtures.user_all_result[1:3] == l

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
        assert fixtures.user_all_result == l

    def test_one_to_many_scalar(self):
        mapper(User, users, properties = dict(
            address = relation(mapper(Address, addresses), lazy=True, uselist=False)
        ))
        q = create_session().query(User)
        l = q.filter(users.c.id == 7).all()
        assert [User(id=7, address=Address(id=1))] == l

    def test_double(self):
        """tests lazy loading with two relations simulatneously, from the same table, using aliases.  """
        openorders = alias(orders, 'openorders')
        closedorders = alias(orders, 'closedorders')

        mapper(Address, addresses)
        
        mapper(User, users, properties = dict(
            addresses = relation(Address, lazy = True),
            open_orders = relation(mapper(Order, openorders, entity_name='open'), primaryjoin = and_(openorders.c.isopen == 1, users.c.id==openorders.c.user_id), lazy=True),
            closed_orders = relation(mapper(Order, closedorders,entity_name='closed'), primaryjoin = and_(closedorders.c.isopen == 0, users.c.id==closedorders.c.user_id), lazy=True)
        ))
        q = create_session().query(User)

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
        
        sess = create_session()
        user = sess.query(User).get(7)
        assert [Order(id=1), Order(id=5)] == create_session().query(Order, entity_name='closed').with_parent(user, property='closed_orders').all()
        assert [Order(id=3)] == create_session().query(Order, entity_name='open').with_parent(user, property='open_orders').all()

    def test_many_to_many(self):

        mapper(Keyword, keywords)
        mapper(Item, items, properties = dict(
                keywords = relation(Keyword, secondary=item_keywords, lazy=True),
        ))
        
        q = create_session().query(Item)
        assert fixtures.item_keyword_result == q.all()

        assert fixtures.item_keyword_result[0:2] == q.join('keywords').filter(keywords.c.name == 'red').all()

    def test_uses_get(self):
        """test that a simple many-to-one lazyload optimizes to use query.get()."""

        for pj in (
            None,
            users.c.id==addresses.c.user_id,
            addresses.c.user_id==users.c.id
        ):
            mapper(Address, addresses, properties = dict(
                user = relation(mapper(User, users), lazy=True, primaryjoin=pj)
            ))
        
            sess = create_session()
        
            # load address
            a1 = sess.query(Address).filter_by(email_address="ed@wood.com").one()
        
            # load user that is attached to the address
            u1 = sess.query(User).get(8)
        
            def go():
                # lazy load of a1.user should get it from the session
                assert a1.user is u1
            self.assert_sql_count(testbase.db, go, 0)
            clear_mappers()
        
    def test_many_to_one(self):
        mapper(Address, addresses, properties = dict(
            user = relation(mapper(User, users), lazy=True)
        ))
        sess = create_session()
        q = sess.query(Address)
        a = q.filter(addresses.c.id==1).one()

        assert a.user is not None
        
        u1 = sess.query(User).get(7)
        
        assert a.user is u1

if __name__ == '__main__':
    testbase.main()
