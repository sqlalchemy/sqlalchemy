"""basic tests of eager loaded attributes"""

from sqlalchemy import *
from sqlalchemy.orm import *
import testbase

from fixtures import *
from query import QueryTest

class EagerTest(QueryTest):
    keep_mappers = False

    def setup_mappers(self):
        pass

    def test_basic(self):
        mapper(User, users, properties={
            'addresses':relation(mapper(Address, addresses), lazy=False)
        })
        sess = create_session()
        q = sess.query(User)

        assert [User(id=7, addresses=[Address(id=1, email_address='jack@bean.com')])] == q.filter(users.c.id == 7).all()
        assert fixtures.user_address_result == q.all()

    def test_no_orphan(self):
        """test that an eagerly loaded child object is not marked as an orphan"""

        mapper(User, users, properties={
            'addresses':relation(Address, cascade="all,delete-orphan", lazy=False)
        })
        mapper(Address, addresses)

        sess = create_session()
        user = sess.query(User).get(7)
        assert getattr(User, 'addresses').hasparent(user.addresses[0], optimistic=True)
        assert not class_mapper(Address)._is_orphan(user.addresses[0])

    def test_orderby(self):
        mapper(User, users, properties = {
            'addresses':relation(mapper(Address, addresses), lazy=False, order_by=addresses.c.email_address),
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
            addresses = relation(Address, lazy=False),
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
            addresses = relation(Address, lazy=False, order_by=[desc(addresses.c.email_address)]),
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

    def test_many_to_many(self):

        mapper(Keyword, keywords)
        mapper(Item, items, properties = dict(
                keywords = relation(Keyword, secondary=item_keywords, lazy=False, order_by=keywords.c.id),
        ))

        q = create_session().query(Item)
        def go():
            assert fixtures.item_keyword_result == q.all()
        self.assert_sql_count(testbase.db, go, 1)
        
        def go():
            assert fixtures.item_keyword_result[0:2] == q.join('keywords').filter(keywords.c.name == 'red').all()
        self.assert_sql_count(testbase.db, go, 1)


    def test_eager_option(self):
        mapper(Keyword, keywords)
        mapper(Item, items, properties = dict(
                keywords = relation(Keyword, secondary=item_keywords, lazy=True),
        ))

        q = create_session().query(Item)

        def go():
            assert fixtures.item_keyword_result[0:2] == q.options(eagerload('keywords')).join('keywords').filter(keywords.c.name == 'red').all()
            
        self.assert_sql_count(testbase.db, go, 1)

    def test_cyclical(self):
        """test that a circular eager relationship breaks the cycle with a lazy loader"""
        
        mapper(Address, addresses)
        mapper(User, users, properties = dict(
            addresses = relation(Address, lazy=False, backref=backref('user', lazy=False))
        ))
        assert class_mapper(User).get_property('addresses').lazy is False
        assert class_mapper(Address).get_property('user').lazy is False
        
        sess = create_session()
        assert fixtures.user_address_result == sess.query(User).all()
        
    def test_double(self):
        """tests lazy loading with two relations simulatneously, from the same table, using aliases.  """
        openorders = alias(orders, 'openorders')
        closedorders = alias(orders, 'closedorders')

        mapper(Address, addresses)

        mapper(User, users, properties = dict(
            addresses = relation(Address, lazy=False),
            open_orders = relation(mapper(Order, openorders, entity_name='open'), primaryjoin = and_(openorders.c.isopen == 1, users.c.id==openorders.c.user_id), lazy=False),
            closed_orders = relation(mapper(Order, closedorders,entity_name='closed'), primaryjoin = and_(closedorders.c.isopen == 0, users.c.id==closedorders.c.user_id), lazy=False)
        ))
        q = create_session().query(User)

        def go():
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
        self.assert_sql_count(testbase.db, go, 1)
        
    def test_limit(self):
        """test limit operations combined with lazy-load relationships."""

        mapper(Item, items)
        mapper(Order, orders, properties={
            'items':relation(Item, secondary=order_items, lazy=False)
        })
        mapper(User, users, properties={
            'addresses':relation(mapper(Address, addresses), lazy=False),
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
        # this is an involved 3x union of the users table to get a lot of rows.
        # then see if the "distinct" works its way out.  you actually get the same
        # result with or without the distinct, just via less or more rows.
        u2 = users.alias('u2')
        s = union_all(u2.select(use_labels=True), u2.select(use_labels=True), u2.select(use_labels=True)).alias('u')

        mapper(User, users, properties={
            'addresses':relation(mapper(Address, addresses), lazy=False),
        })

        sess = create_session()
        q = sess.query(User)

        def go():
            l = q.filter(s.c.u2_id==User.c.id).distinct().all()
            assert fixtures.user_address_result == l
        self.assert_sql_count(testbase.db, go, 1)
        
    def test_limit_2(self):
        mapper(Keyword, keywords)
        mapper(Item, items, properties = dict(
                keywords = relation(Keyword, secondary=item_keywords, lazy=False, order_by=[keywords.c.id]),
            ))
            
        sess = create_session()
        q = sess.query(Item)
        l = q.filter((Item.c.description=='item 2') | (Item.c.description=='item 5') | (Item.c.description=='item 3')).\
            order_by(Item.c.id).limit(2).all()

        assert fixtures.item_keyword_result[1:3] == l
        
    def test_limit_3(self):
        """test that the ORDER BY is propigated from the inner select to the outer select, when using the 
        'wrapped' select statement resulting from the combination of eager loading and limit/offset clauses."""
        
        mapper(Item, items)
        mapper(Order, orders, properties = dict(
                items = relation(Item, secondary=order_items, lazy=False)
        ))

        mapper(Address, addresses)
        mapper(User, users, properties = dict(
            addresses = relation(Address, lazy=False),
            orders = relation(Order, lazy=False),
        ))
        sess = create_session()
        
        q = sess.query(User)

        if testbase.db.engine.name != 'mssql':
            l = q.join('orders').order_by(desc(orders.c.user_id)).limit(2).offset(1)
            assert [
                User(id=9, 
                    orders=[Order(id=2), Order(id=4)],
                    addresses=[Address(id=5)]
                ),
                User(id=7, 
                    orders=[Order(id=1), Order(id=3), Order(id=5)],
                    addresses=[Address(id=1)]
                )
            ] == l.all()

        l = q.join('addresses').order_by(desc(addresses.c.email_address)).limit(1).offset(0)
        assert [
            User(id=7, 
                orders=[Order(id=1), Order(id=3), Order(id=5)],
                addresses=[Address(id=1)]
            )
        ] == l.all()

    def test_one_to_many_scalar(self):
        mapper(User, users, properties = dict(
            address = relation(mapper(Address, addresses), lazy=False, uselist=False)
        ))
        q = create_session().query(User)
        
        def go():
            l = q.filter(users.c.id == 7).all()
            assert [User(id=7, address=Address(id=1))] == l
        self.assert_sql_count(testbase.db, go, 1)

    def test_many_to_one(self):
        mapper(Address, addresses, properties = dict(
            user = relation(mapper(User, users), lazy=False)
        ))
        sess = create_session()
        q = sess.query(Address)
        
        def go():
            a = q.filter(addresses.c.id==1).one()
            assert a.user is not None
            u1 = sess.query(User).get(7)
            assert a.user is u1
        self.assert_sql_count(testbase.db, go, 1)
        

    def test_one_and_many(self):
        """tests eager load for a parent object with a child object that 
        contains a many-to-many relationship to a third object."""
        
        mapper(User, users, properties={
            'orders':relation(Order, lazy=False)
        })
        mapper(Item, items) 
        mapper(Order, orders, properties = dict(
                items = relation(Item, secondary=order_items, lazy=False, order_by=items.c.id)
            ))
            
        q = create_session().query(User)
        
        l = q.filter("users.id in (7, 8, 9)")
        
        def go():
            assert fixtures.user_order_result[0:3] == l.all()
        self.assert_sql_count(testbase.db, go, 1)

    def test_double_with_aggregate(self):

        max_orders_by_user = select([func.max(orders.c.id).label('order_id')], group_by=[orders.c.user_id]).alias('max_orders_by_user')
        
        max_orders = orders.select(orders.c.id==max_orders_by_user.c.order_id).alias('max_orders')
        
        mapper(Order, orders)
        mapper(User, users, properties={
               'orders':relation(Order, backref='user', lazy=False),
               'max_order':relation(mapper(Order, max_orders, non_primary=True), lazy=False, uselist=False)
               })
        q = create_session().query(User)
        
        def go():
            assert [
                User(id=7, orders=[
                        Order(id=1),
                        Order(id=3),
                        Order(id=5),
                    ], 
                    max_order=Order(id=5)
                ),
                User(id=8, orders=[]),
                User(id=9, orders=[Order(id=2),Order(id=4)], 
                    max_order=Order(id=4)
                ),
                User(id=10),
            ] == q.all()
        self.assert_sql_count(testbase.db, go, 1)

    def test_wide(self):
        mapper(Order, orders, properties={'items':relation(Item, secondary=order_items, lazy=False, order_by=items.c.id)})
        mapper(Item, items)
        mapper(User, users, properties = dict(
            addresses = relation(mapper(Address, addresses), lazy = False),
            orders = relation(Order, lazy = False),
        ))
        q = create_session().query(User)
        l = q.select()
        assert fixtures.user_all_result == q.all()

    def test_against_select(self):
        """test eager loading of a mapper which is against a select"""

        s = select([orders], orders.c.isopen==1).alias('openorders')
        
        mapper(Order, s, properties={
            'user':relation(User, lazy=False)
        })
        mapper(User, users)

        q = create_session().query(Order)
        assert [
            Order(id=3, user=User(id=7)),
            Order(id=4, user=User(id=9))
        ] == q.all()
        
        q = q.select_from(s.join(order_items).join(items)).filter(~items.c.id.in_(1, 2, 5))
        assert [
            Order(id=3, user=User(id=7)),
        ] == q.all()

    def test_aliasing(self):
        """test that eager loading uses aliases to insulate the eager load from regular criterion against those tables."""
        
        mapper(User, users, properties = dict(
            addresses = relation(mapper(Address, addresses), lazy=False)
        ))
        q = create_session().query(User)
        l = q.filter(addresses.c.email_address == 'ed@lala.com').filter(addresses.c.user_id==users.c.id)
        assert fixtures.user_address_result[1:2] == l.all()

if __name__ == '__main__':
    testbase.main()
