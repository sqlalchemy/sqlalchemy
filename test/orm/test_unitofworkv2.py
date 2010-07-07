from sqlalchemy.test.testing import eq_, assert_raises, assert_raises_message
from sqlalchemy.test import testing
from sqlalchemy.test.schema import Table, Column
from sqlalchemy import Integer, String, ForeignKey, func
from test.orm import _fixtures, _base
from sqlalchemy.orm import mapper, relationship, backref, \
                            create_session, unitofwork, attributes
from sqlalchemy.test.assertsql import AllOf, CompiledSQL

from test.orm._fixtures import keywords, addresses, Base, Keyword,  \
           Dingaling, item_keywords, dingalings, User, items,\
           orders, Address, users, nodes, \
            order_items, Item, Order, Node, \
            composite_pk_table, CompositePk

class AssertsUOW(object):
    def _get_test_uow(self, session):
        uow = unitofwork.UOWTransaction(session)
        deleted = set(session._deleted)
        new = set(session._new)
        dirty = set(session._dirty_states).difference(deleted)
        for s in new.union(dirty):
            uow.register_object(s)
        for d in deleted:
            uow.register_object(d, isdelete=True)
        return uow
        
    def _assert_uow_size(self,
        session, 
        expected
    ):
        uow = self._get_test_uow(session)
        postsort_actions = uow._generate_actions()
        print postsort_actions
        eq_(len(postsort_actions), expected, postsort_actions)

class UOWTest(_fixtures.FixtureTest, 
                testing.AssertsExecutionResults, AssertsUOW):
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
                    "UPDATE addresses SET user_id=:user_id WHERE "
                    "addresses.id = :addresses_id",
                    lambda ctx: [{u'addresses_id': a1.id, 'user_id': None}]
                ),
                CompiledSQL(
                    "UPDATE addresses SET user_id=:user_id WHERE "
                    "addresses.id = :addresses_id",
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
                    "UPDATE addresses SET user_id=:user_id WHERE "
                    "addresses.id = :addresses_id",
                    lambda ctx: [{u'addresses_id': a1.id, 'user_id': None}]
                ),
                CompiledSQL(
                    "UPDATE addresses SET user_id=:user_id WHERE "
                    "addresses.id = :addresses_id",
                    lambda ctx: [{u'addresses_id': a2.id, 'user_id': None}]
                ),
                CompiledSQL(
                    "DELETE FROM users WHERE users.id = :id",
                    {'id':u1.id}
                ),
        )
    
    def test_many_to_many(self):
        mapper(Item, items, properties={
            'keywords':relationship(Keyword, secondary=item_keywords)
        })
        mapper(Keyword, keywords)
        
        sess = create_session()
        k1 = Keyword(name='k1')
        i1 = Item(description='i1', keywords=[k1])
        sess.add(i1)
        self.assert_sql_execution(
                testing.db,
                sess.flush,
                AllOf(
                    CompiledSQL(
                    "INSERT INTO keywords (name) VALUES (:name)",
                    {'name':'k1'}
                    ),
                    CompiledSQL(
                    "INSERT INTO items (description) VALUES (:description)",
                    {'description':'i1'}
                    ),
                ),
                CompiledSQL(
                    "INSERT INTO item_keywords (item_id, keyword_id) "
                    "VALUES (:item_id, :keyword_id)",
                    lambda ctx:{'item_id':i1.id, 'keyword_id':k1.id}
                )
        )
        
        # test that keywords collection isn't loaded
        sess.expire(i1, ['keywords'])
        i1.description = 'i2'
        self.assert_sql_execution(
                testing.db,
                sess.flush,
                CompiledSQL("UPDATE items SET description=:description "
                            "WHERE items.id = :items_id", 
                            lambda ctx:{'description':'i2', 'items_id':i1.id})
        )
        
    def test_m2o_flush_size(self):
        mapper(User, users)
        mapper(Address, addresses, properties={
            'user':relationship(User, passive_updates=True)
        })
        sess = create_session()
        u1 = User(name='ed')
        sess.add(u1)
        self._assert_uow_size(sess, 2)

    def test_o2m_flush_size(self):
        mapper(User, users, properties={
            'addresses':relationship(Address),
        })
        mapper(Address, addresses)

        sess = create_session()
        u1 = User(name='ed')
        sess.add(u1)
        self._assert_uow_size(sess, 2)

        sess.flush()

        u1.name='jack'

        self._assert_uow_size(sess, 2)
        sess.flush()

        a1 = Address(email_address='foo')
        sess.add(a1)
        sess.flush()

        u1.addresses.append(a1)

        self._assert_uow_size(sess, 6)

        sess.flush()

        sess = create_session()
        u1 = sess.query(User).first()
        u1.name='ed'
        self._assert_uow_size(sess, 2)

        u1.addresses
        self._assert_uow_size(sess, 6)


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
                    "INSERT INTO nodes (parent_id, data) VALUES "
                    "(:parent_id, :data)",
                    {'parent_id': None, 'data': 'n1'}
                ),
                AllOf(
                CompiledSQL(
                    "INSERT INTO nodes (parent_id, data) VALUES "
                    "(:parent_id, :data)",
                    lambda ctx: {'parent_id': n1.id, 'data': 'n2'}
                ),
                CompiledSQL(
                    "INSERT INTO nodes (parent_id, data) VALUES "
                    "(:parent_id, :data)",
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
                    CompiledSQL("UPDATE nodes SET parent_id=:parent_id "
                        "WHERE nodes.id = :nodes_id", 
                        lambda ctx: {'nodes_id':n3.id, 'parent_id':None}),
                    CompiledSQL("UPDATE nodes SET parent_id=:parent_id "
                        "WHERE nodes.id = :nodes_id", 
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
                    "INSERT INTO nodes (parent_id, data) VALUES "
                    "(:parent_id, :data)",
                    {'parent_id': None, 'data': 'n1'}
                ),
                AllOf(
                CompiledSQL(
                    "INSERT INTO nodes (parent_id, data) VALUES "
                    "(:parent_id, :data)",
                    lambda ctx: {'parent_id': n1.id, 'data': 'n2'}
                ),
                CompiledSQL(
                    "INSERT INTO nodes (parent_id, data) VALUES "
                    "(:parent_id, :data)",
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
            'children':relationship(Node, 
                                    backref=backref('parent',
                                                remote_side=nodes.c.id))
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

    def test_singlecycle_flush_size(self):
        mapper(Node, nodes, properties={
            'children':relationship(Node)
        })
        sess = create_session()
        n1 = Node(data='ed')
        sess.add(n1)
        self._assert_uow_size(sess, 2)

        sess.flush()
    
        n1.data='jack'

        self._assert_uow_size(sess, 2)
        sess.flush()
    
        n2 = Node(data='foo')
        sess.add(n2)
        sess.flush()
    
        n1.children.append(n2)

        self._assert_uow_size(sess, 3)
    
        sess.flush()
    
        sess = create_session()
        n1 = sess.query(Node).first()
        n1.data='ed'
        self._assert_uow_size(sess, 2)
    
        n1.children
        self._assert_uow_size(sess, 2)

class SingleCyclePlusAttributeTest(_base.MappedTest,
                    testing.AssertsExecutionResults, AssertsUOW):
    @classmethod
    def define_tables(cls, metadata):
        Table('nodes', metadata,
            Column('id', Integer, primary_key=True,
                                    test_needs_autoincrement=True),
            Column('parent_id', Integer, ForeignKey('nodes.id')),
            Column('data', String(30))
        )
        
        Table('foobars', metadata,
            Column('id', Integer, primary_key=True,
                                    test_needs_autoincrement=True),
            Column('parent_id', Integer, ForeignKey('nodes.id')),
        )

    @testing.resolve_artifact_names
    def test_flush_size(self):
        class Node(Base):
            pass
        class FooBar(Base):
            pass

        mapper(Node, nodes, properties={
            'children':relationship(Node),
            'foobars':relationship(FooBar)
        })
        mapper(FooBar, foobars)

        sess = create_session()
        n1 = Node(data='n1')
        n2 = Node(data='n2')
        n1.children.append(n2)
        sess.add(n1)
        # ensure "foobars" doesn't get yanked in here
        self._assert_uow_size(sess, 3)
        
        n1.foobars.append(FooBar())
        # saveupdateall/deleteall for FooBar added here,
        # plus processstate node.foobars 
        # currently the "all" procs stay in pairs
        self._assert_uow_size(sess, 6)
        
        sess.flush()

class SingleCycleM2MTest(_base.MappedTest, 
                    testing.AssertsExecutionResults, AssertsUOW):

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
        
        # can't really assert the SQL on this easily
        # since there's too many ways to insert the rows.
        # so check the end result
        sess.flush()
        eq_(
            sess.query(node_to_nodes.c.left_node_id,
                            node_to_nodes.c.right_node_id).\
                    order_by(node_to_nodes.c.left_node_id,
                            node_to_nodes.c.right_node_id).\
                    all(), 
            sorted([
                    (n1.id, n2.id), (n1.id, n3.id), (n1.id, n4.id), 
                    (n2.id, n3.id), (n2.id, n5.id), 
                    (n3.id, n5.id), (n3.id, n4.id)
                ])
        )
        
        sess.delete(n1)
        
        self.assert_sql_execution(
                testing.db,
                sess.flush,
                # this is n1.parents firing off, as it should, since
                # passive_deletes is False for n1.parents
                CompiledSQL(
                    "SELECT nodes.id AS nodes_id, nodes.data AS nodes_data, "
                    "nodes.favorite_node_id AS nodes_favorite_node_id FROM "
                    "nodes, node_to_nodes WHERE :param_1 = "
                    "node_to_nodes.right_node_id AND nodes.id = "
                    "node_to_nodes.left_node_id" ,
                    lambda ctx:{u'param_1': n1.id},
                ),    
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
    
class RowswitchAccountingTest(_base.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table('parent', metadata,
            Column('id', Integer, primary_key=True)
        )
        Table('child', metadata, 
            Column('id', Integer, ForeignKey('parent.id'), primary_key=True)
        )
    
    @testing.resolve_artifact_names
    def test_accounting_for_rowswitch(self):
        class Parent(object):
            def __init__(self, id):
                self.id = id
                self.child = Child()
        class Child(object):
            pass

        mapper(Parent, parent, properties={
            'child':relationship(Child, uselist=False, 
                                    cascade="all, delete-orphan",
                                    backref="parent")
        })
        mapper(Child, child)
        
        sess = create_session(autocommit=False)

        p1 = Parent(1)
        sess.add(p1)
        sess.commit()

        sess.close()
        p2 = Parent(1)
        p3 = sess.merge(p2)

        old = attributes.get_history(p3, 'child')[2][0]
        assert old in sess

        sess.flush()

        assert p3.child._sa_instance_state.session_id == sess.hash_key
        assert p3.child in sess

        p4 = Parent(1)
        p5 = sess.merge(p4)

        old = attributes.get_history(p5, 'child')[2][0]
        assert old in sess

        sess.flush()



