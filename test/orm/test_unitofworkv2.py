from sqlalchemy.test.testing import eq_, assert_raises, assert_raises_message
from sqlalchemy.test import testing
from sqlalchemy.test.schema import Table, Column
from sqlalchemy import Integer, String, ForeignKey
from test.orm import _fixtures, _base
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
                CompiledSQL("DELETE FROM nodes WHERE nodes.id = :id", 
                        lambda ctx:[{'id':n2.id}, {'id':n3.id}]),
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
                CompiledSQL("DELETE FROM nodes WHERE nodes.id = :id", 
                        lambda ctx:[{'id':n2.id},{'id':n3.id}]),
                CompiledSQL("DELETE FROM nodes WHERE nodes.id = :id", 
                        lambda ctx: {'id':n1.id})
        )
    
    def test_cycle_rowswitch(self):
        mapper(Node, nodes, properties={
            'children':relationship(Node)
        })
        sess = create_session()

        n2, n3 = Node(data='n2', children=[]), Node(data='n3', children=[])
        n1 = Node(data='n1', children=[n2])

        sess.add(n1)
        sess.flush()
        sess.delete(n2)
        n3.id = n2.id
        n1.children.append(n3)
        sess.flush()
        
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
        
    def test_bidirectional_multilevel_save(self):
        mapper(Node, nodes, properties={
            'children':relationship(Node, 
                backref=backref('parent', remote_side=nodes.c.id)
            )
        })
        sess = create_session()
        n1 = Node(data='n1')
        n1.children.append(Node(data='n11'))
        n1.children.append(Node(data='n12'))
        n1.children.append(Node(data='n13'))
        n1.children[1].children.append(Node(data='n121'))
        n1.children[1].children.append(Node(data='n122'))
        n1.children[1].children.append(Node(data='n123'))
        sess.add(n1)
        sess.flush()
#        self.assert_sql_execution(
#                testing.db,
 #               sess.flush,
 #       )

class SingleCycleM2MTest(_base.MappedTest, testing.AssertsExecutionResults):

    @classmethod
    def define_tables(cls, metadata):
        nodes = Table('nodes', metadata,
            Column('id', Integer, 
                            primary_key=True, 
                            test_needs_autoincrement=True),
            Column('data', String(30)),
            Column('favorite_node_id', Integer, ForeignKey('nodes.id'))
        )
        
        node_to_nodes =Table('node_to_nodes', metadata,
            Column('left_node_id', Integer, 
                            ForeignKey('nodes.id'),primary_key=True),
            Column('right_node_id', Integer, 
                            ForeignKey('nodes.id'),primary_key=True),
            )
    
    @testing.resolve_artifact_names
    def test_many_to_many_one(self):
        class Node(Base):
            pass
        
        mapper(Node, nodes, properties={
            'children':relationship(Node, secondary=node_to_nodes,
                primaryjoin=nodes.c.id==node_to_nodes.c.left_node_id,
                secondaryjoin=nodes.c.id==node_to_nodes.c.right_node_id,
                backref='parents'
            ),
            'favorite':relationship(Node, remote_side=nodes.c.id)
        })
        
        sess = create_session()
        n1 = Node(data='n1')
        n2 = Node(data='n2')
        n3 = Node(data='n3')
        n4 = Node(data='n4')
        n5 = Node(data='n5')
        
        n4.favorite = n3
        n1.favorite = n5
        n5.favorite = n2
        
        n1.children = [n2, n3, n4]
        n2.children = [n3, n5]
        n3.children = [n5, n4]
        
        sess.add_all([n1, n2, n3, n4, n5])
        self.assert_sql_execution(
                testing.db,
                sess.flush,

                CompiledSQL(
                    "INSERT INTO nodes (data, favorite_node_id) "
                    "VALUES (:data, :favorite_node_id)",
                    {'data': 'n2', 'favorite_node_id': None}
                ),
                CompiledSQL(
                    "INSERT INTO nodes (data, favorite_node_id) "
                    "VALUES (:data, :favorite_node_id)", 
                    {'data': 'n3', 'favorite_node_id': None}),
                CompiledSQL("INSERT INTO nodes (data, favorite_node_id) "
                            "VALUES (:data, :favorite_node_id)", 
                    lambda ctx:{'data': 'n5', 'favorite_node_id': n2.id}),
                CompiledSQL(
                    "INSERT INTO nodes (data, favorite_node_id) "
                    "VALUES (:data, :favorite_node_id)", 
                    lambda ctx:{'data': 'n4', 'favorite_node_id': n3.id}),
                CompiledSQL(
                    "INSERT INTO node_to_nodes (left_node_id, right_node_id) "
                    "VALUES (:left_node_id, :right_node_id)", 
                    lambda ctx:[
                        {'right_node_id': n5.id, 'left_node_id': n3.id}, 
                        {'right_node_id': n4.id, 'left_node_id': n3.id}, 
                        {'right_node_id': n3.id, 'left_node_id': n2.id}, 
                        {'right_node_id': n5.id, 'left_node_id': n2.id}
                    ]
                    ),
                CompiledSQL(
                    "INSERT INTO nodes (data, favorite_node_id) "
                    "VALUES (:data, :favorite_node_id)", 
                    lambda ctx:[{'data': 'n1', 'favorite_node_id': n5.id}]
                ),
                CompiledSQL(
                    "INSERT INTO node_to_nodes (left_node_id, right_node_id) "
                    "VALUES (:left_node_id, :right_node_id)", 
                    lambda ctx:[
                        {'right_node_id': n2.id, 'left_node_id': n1.id}, 
                        {'right_node_id': n3.id, 'left_node_id': n1.id}, 
                        {'right_node_id': n4.id, 'left_node_id': n1.id}
                    ])
            )

        sess.delete(n1)
        
        self.assert_sql_execution(
                testing.db,
                sess.flush,
                CompiledSQL(
                    "DELETE FROM node_to_nodes WHERE "
                    "node_to_nodes.left_node_id = :left_node_id AND "
                    "node_to_nodes.right_node_id = :right_node_id",
                    lambda ctx:[
                        {'right_node_id': n2.id, 'left_node_id': n1.id}, 
                        {'right_node_id': n3.id, 'left_node_id': n1.id}, 
                        {'right_node_id': n4.id, 'left_node_id': n1.id}
                    ]
                ),
                CompiledSQL(
                    "DELETE FROM nodes WHERE nodes.id = :id",
                    lambda ctx:{'id': n1.id}
                ),
        )
        
        for n in [n2, n3, n4, n5]:
            sess.delete(n)
            
        # load these collections
        # outside of the flush() below
        n4.children
        n5.children
        
        self.assert_sql_execution(
            testing.db,
            sess.flush,
            CompiledSQL(
                "DELETE FROM node_to_nodes WHERE node_to_nodes.left_node_id "
                "= :left_node_id AND node_to_nodes.right_node_id = "
                ":right_node_id",
                lambda ctx:[
                    {'right_node_id': n5.id, 'left_node_id': n3.id}, 
                    {'right_node_id': n4.id, 'left_node_id': n3.id}, 
                    {'right_node_id': n3.id, 'left_node_id': n2.id}, 
                    {'right_node_id': n5.id, 'left_node_id': n2.id}
                ]
            ),
            CompiledSQL(
                "DELETE FROM nodes WHERE nodes.id = :id",
                lambda ctx:[{'id': n4.id}, {'id': n5.id}]
            ),
            CompiledSQL(
                "DELETE FROM nodes WHERE nodes.id = :id",
                lambda ctx:[{'id': n2.id}, {'id': n3.id}]
            ),
        )
        
        
        
        
        