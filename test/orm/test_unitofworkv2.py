from sqlalchemy.test.testing import eq_, assert_raises, assert_raises_message
from sqlalchemy.test import testing
from test.orm import _fixtures
from sqlalchemy.orm import mapper, relationship, backref, create_session
from sqlalchemy.test.assertsql import AllOf, CompiledSQL

from test.orm._fixtures import keywords, addresses, Base, Keyword,  \
           Dingaling, item_keywords, dingalings, User, items,\
           orders, Address, users, nodes, \
            order_items, Item, Order, Node, \
            composite_pk_table, CompositePk

class UOWTest(_fixtures.FixtureTest, testing.AssertsExecutionResults):
    run_inserts = None

class RudimentaryFlushTest(UOWTest):

    def test_one_to_many_save(self):
        mapper(User, users, properties={
            'addresses':relationship(Address),
        })
        mapper(Address, addresses)
        sess = create_session()

        a1, a2 = Address(email_address='a1'), Address(email_address='a2')
        u1 = User(name='u1', addresses=[a1, a2])
        sess.add(u1)
    
        self.assert_sql_execution(
                testing.db,
                sess.flush,
                CompiledSQL(
                    "INSERT INTO users (name) VALUES (:name)",
                    {'name': 'u1'} 
                ),
                CompiledSQL(
                    "INSERT INTO addresses (user_id, email_address) "
                    "VALUES (:user_id, :email_address)",
                    lambda ctx: {'email_address': 'a1', 'user_id':u1.id} 
                ),
                CompiledSQL(
                    "INSERT INTO addresses (user_id, email_address) "
                    "VALUES (:user_id, :email_address)",
                    lambda ctx: {'email_address': 'a2', 'user_id':u1.id} 
                ),
            )

    def test_one_to_many_delete_all(self):
        mapper(User, users, properties={
            'addresses':relationship(Address),
        })
        mapper(Address, addresses)
        sess = create_session()
        a1, a2 = Address(email_address='a1'), Address(email_address='a2')
        u1 = User(name='u1', addresses=[a1, a2])
        sess.add(u1)
        sess.flush()
        
        sess.delete(u1)
        sess.delete(a1)
        sess.delete(a2)
        self.assert_sql_execution(
                testing.db,
                sess.flush,
                CompiledSQL(
                    "DELETE FROM addresses WHERE addresses.id = :id",
                    [{'id':a1.id},{'id':a2.id}]
                ),
                CompiledSQL(
                    "DELETE FROM users WHERE users.id = :id",
                    {'id':u1.id}
                ),
        )

    def test_one_to_many_delete_parent(self):
        mapper(User, users, properties={
            'addresses':relationship(Address),
        })
        mapper(Address, addresses)
        sess = create_session()
        a1, a2 = Address(email_address='a1'), Address(email_address='a2')
        u1 = User(name='u1', addresses=[a1, a2])
        sess.add(u1)
        sess.flush()

        sess.delete(u1)
        self.assert_sql_execution(
                testing.db,
                sess.flush,
                CompiledSQL(
                    "UPDATE addresses SET user_id=:user_id WHERE addresses.id = :addresses_id",
                    lambda ctx: [{u'addresses_id': a1.id, 'user_id': None}]
                ),
                CompiledSQL(
                    "UPDATE addresses SET user_id=:user_id WHERE addresses.id = :addresses_id",
                    lambda ctx: [{u'addresses_id': a2.id, 'user_id': None}]
                ),
                CompiledSQL(
                    "DELETE FROM users WHERE users.id = :id",
                    {'id':u1.id}
                ),
        )
        
    def test_many_to_one_save(self):
        
        mapper(User, users)
        mapper(Address, addresses, properties={
            'user':relationship(User)
        })
        sess = create_session()

        u1 = User(name='u1')
        a1, a2 = Address(email_address='a1', user=u1), \
                    Address(email_address='a2', user=u1)
        sess.add_all([a1, a2])
    
        self.assert_sql_execution(
                testing.db,
                sess.flush,
                CompiledSQL(
                    "INSERT INTO users (name) VALUES (:name)",
                    {'name': 'u1'} 
                ),
                CompiledSQL(
                    "INSERT INTO addresses (user_id, email_address) "
                    "VALUES (:user_id, :email_address)",
                    lambda ctx: {'email_address': 'a1', 'user_id':u1.id} 
                ),
                CompiledSQL(
                    "INSERT INTO addresses (user_id, email_address) "
                    "VALUES (:user_id, :email_address)",
                    lambda ctx: {'email_address': 'a2', 'user_id':u1.id} 
                ),
            )

    def test_many_to_one_delete_all(self):
        mapper(User, users)
        mapper(Address, addresses, properties={
            'user':relationship(User)
        })
        sess = create_session()

        u1 = User(name='u1')
        a1, a2 = Address(email_address='a1', user=u1), \
                    Address(email_address='a2', user=u1)
        sess.add_all([a1, a2])
        sess.flush()
        
        sess.delete(u1)
        sess.delete(a1)
        sess.delete(a2)
        self.assert_sql_execution(
                testing.db,
                sess.flush,
                CompiledSQL(
                    "DELETE FROM addresses WHERE addresses.id = :id",
                    [{'id':a1.id},{'id':a2.id}]
                ),
                CompiledSQL(
                    "DELETE FROM users WHERE users.id = :id",
                    {'id':u1.id}
                ),
        )

    def test_many_to_one_delete_target(self):
        mapper(User, users)
        mapper(Address, addresses, properties={
            'user':relationship(User)
        })
        sess = create_session()

        u1 = User(name='u1')
        a1, a2 = Address(email_address='a1', user=u1), \
                    Address(email_address='a2', user=u1)
        sess.add_all([a1, a2])
        sess.flush()

        sess.delete(u1)
        a1.user = a2.user = None
        self.assert_sql_execution(
                testing.db,
                sess.flush,
                CompiledSQL(
                    "UPDATE addresses SET user_id=:user_id WHERE addresses.id = :addresses_id",
                    lambda ctx: [{u'addresses_id': a1.id, 'user_id': None}]
                ),
                CompiledSQL(
                    "UPDATE addresses SET user_id=:user_id WHERE addresses.id = :addresses_id",
                    lambda ctx: [{u'addresses_id': a2.id, 'user_id': None}]
                ),
                CompiledSQL(
                    "DELETE FROM users WHERE users.id = :id",
                    {'id':u1.id}
                ),
        )

class SingleCycleTest(UOWTest):
    def test_one_to_many_save(self):
        mapper(Node, nodes, properties={
            'children':relationship(Node)
        })
        sess = create_session()

        n2, n3 = Node(data='n2'), Node(data='n3')
        n1 = Node(data='n1', children=[n2, n3])
        
        sess.add(n1)
    
        self.assert_sql_execution(
                testing.db,
                sess.flush,
                
                CompiledSQL(
                    "INSERT INTO nodes (parent_id, data) VALUES (:parent_id, :data)",
                    {'parent_id': None, 'data': 'n1'}
                ),
                AllOf(
                CompiledSQL(
                    "INSERT INTO nodes (parent_id, data) VALUES (:parent_id, :data)",
                    lambda ctx: {'parent_id': n1.id, 'data': 'n2'}
                ),
                CompiledSQL(
                    "INSERT INTO nodes (parent_id, data) VALUES (:parent_id, :data)",
                    lambda ctx: {'parent_id': n1.id, 'data': 'n3'}
                ),
                )
            )

    def test_one_to_many_delete_all(self):
        mapper(Node, nodes, properties={
            'children':relationship(Node)
        })
        sess = create_session()

        n2, n3 = Node(data='n2', children=[]), Node(data='n3', children=[])
        n1 = Node(data='n1', children=[n2, n3])

        sess.add(n1)
        sess.flush()
        
        sess.delete(n1)
        sess.delete(n2)
        sess.delete(n3)
        self.assert_sql_execution(
                testing.db,
                sess.flush,
                AllOf(
                    CompiledSQL("DELETE FROM nodes WHERE nodes.id = :id", 
                            lambda ctx:{'id':n3.id}),
                    CompiledSQL("DELETE FROM nodes WHERE nodes.id = :id", 
                            lambda ctx: {'id':n2.id}),
                ),
                CompiledSQL("DELETE FROM nodes WHERE nodes.id = :id", 
                        lambda ctx: {'id':n1.id})
        )

    def test_one_to_many_delete_parent(self):
        mapper(Node, nodes, properties={
            'children':relationship(Node)
        })
        sess = create_session()

        n2, n3 = Node(data='n2', children=[]), Node(data='n3', children=[])
        n1 = Node(data='n1', children=[n2, n3])

        sess.add(n1)
        sess.flush()

        sess.delete(n1)
        self.assert_sql_execution(
                testing.db,
                sess.flush,
                AllOf(
                    CompiledSQL("UPDATE nodes SET parent_id=:parent_id WHERE nodes.id = :nodes_id", 
                        lambda ctx: {'nodes_id':n3.id, 'parent_id':None}),
                    CompiledSQL("UPDATE nodes SET parent_id=:parent_id WHERE nodes.id = :nodes_id", 
                        lambda ctx: {'nodes_id':n2.id, 'parent_id':None}),
                ),
                CompiledSQL("DELETE FROM nodes WHERE nodes.id = :id", 
                    lambda ctx:{'id':n1.id})
        )
    
    def test_many_to_one_save(self):
        mapper(Node, nodes, properties={
            'parent':relationship(Node, remote_side=nodes.c.id)
        })
        sess = create_session()

        n1 = Node(data='n1')
        n2, n3 = Node(data='n2', parent=n1), Node(data='n3', parent=n1)

        sess.add_all([n2, n3])

        self.assert_sql_execution(
                testing.db,
                sess.flush,

                CompiledSQL(
                    "INSERT INTO nodes (parent_id, data) VALUES (:parent_id, :data)",
                    {'parent_id': None, 'data': 'n1'}
                ),
                AllOf(
                CompiledSQL(
                    "INSERT INTO nodes (parent_id, data) VALUES (:parent_id, :data)",
                    lambda ctx: {'parent_id': n1.id, 'data': 'n2'}
                ),
                CompiledSQL(
                    "INSERT INTO nodes (parent_id, data) VALUES (:parent_id, :data)",
                    lambda ctx: {'parent_id': n1.id, 'data': 'n3'}
                ),
                )
            )

    def test_many_to_one_delete_all(self):
        mapper(Node, nodes, properties={
            'parent':relationship(Node, remote_side=nodes.c.id)
        })
        sess = create_session()

        n1 = Node(data='n1')
        n2, n3 = Node(data='n2', parent=n1), Node(data='n3', parent=n1)

        sess.add_all([n2, n3])
        sess.flush()

        sess.delete(n1)
        sess.delete(n2)
        sess.delete(n3)
        self.assert_sql_execution(
                testing.db,
                sess.flush,
                AllOf(
                    CompiledSQL("DELETE FROM nodes WHERE nodes.id = :id", 
                            lambda ctx:{'id':n3.id}),
                    CompiledSQL("DELETE FROM nodes WHERE nodes.id = :id", 
                            lambda ctx: {'id':n2.id}),
                ),
                CompiledSQL("DELETE FROM nodes WHERE nodes.id = :id", 
                        lambda ctx: {'id':n1.id})
        )

    def test_bidirectional_mutations_one(self):
        mapper(Node, nodes, properties={
            'children':relationship(Node, backref=backref('parent', remote_side=nodes.c.id))
        })
        sess = create_session()

        n2, n3 = Node(data='n2', children=[]), Node(data='n3', children=[])
        n1 = Node(data='n1', children=[n2])
        sess.add(n1)
        sess.flush()
        sess.delete(n2)
        n1.children.append(n3)
        sess.flush()
        
        sess.delete(n1)
        sess.delete(n3)
        sess.flush()