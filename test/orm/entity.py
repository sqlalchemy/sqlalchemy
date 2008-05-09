import testenv; testenv.configure_for_tests()
from sqlalchemy import *
from sqlalchemy.orm import *
from testlib import *
from testlib.tables import *
from testlib import fixtures
from orm import _base


class EntityTest(_base.ORMTest):
    """tests mappers that are constructed based on "entity names", which allows the same class
    to have multiple primary mappers """

    def setUpAll(self):
        global user1, user2, address1, address2, metadata, ctx
        metadata = MetaData(testing.db)
        ctx = scoped_session(create_session)

        user1 = Table('user1', metadata,
            Column('user_id', Integer, Sequence('user1_id_seq', optional=True),
                   primary_key=True),
            Column('name', String(60), nullable=False)
            )
        user2 = Table('user2', metadata,
            Column('user_id', Integer, Sequence('user2_id_seq', optional=True),
                   primary_key=True),
            Column('name', String(60), nullable=False)
            )
        address1 = Table('address1', metadata,
            Column('address_id', Integer,
                   Sequence('address1_id_seq', optional=True),
                   primary_key=True),
            Column('user_id', Integer, ForeignKey(user1.c.user_id),
                   nullable=False),
            Column('email', String(100), nullable=False)
            )
        address2 = Table('address2', metadata,
            Column('address_id', Integer,
                   Sequence('address2_id_seq', optional=True),
                   primary_key=True),
            Column('user_id', Integer, ForeignKey(user2.c.user_id),
                   nullable=False),
            Column('email', String(100), nullable=False)
            )
        metadata.create_all()
    def tearDownAll(self):
        metadata.drop_all()
    def tearDown(self):
        ctx.clear()
        clear_mappers()
        for t in metadata.table_iterator(reverse=True):
            t.delete().execute()

    def testbasic(self):
        """tests a pair of one-to-many mapper structures, establishing that both
        parent and child objects honor the "entity_name" attribute attached to the object
        instances."""
        class User(object):
            def __init__(self, **kw):
                pass
        class Address(object):
            def __init__(self, **kw):
                pass

        a1mapper = mapper(Address, address1, entity_name='address1', extension=ctx.extension)
        a2mapper = mapper(Address, address2, entity_name='address2', extension=ctx.extension)
        u1mapper = mapper(User, user1, entity_name='user1', properties ={
            'addresses':relation(a1mapper)
        }, extension=ctx.extension)
        u2mapper =mapper(User, user2, entity_name='user2', properties={
            'addresses':relation(a2mapper)
        }, extension=ctx.extension)
        
        u1 = User(_sa_entity_name='user1')
        u1.name = 'this is user 1'
        a1 = Address(_sa_entity_name='address1')
        a1.email='a1@foo.com'
        u1.addresses.append(a1)

        u2 = User(_sa_entity_name='user2')
        u2.name='this is user 2'
        a2 = Address(_sa_entity_name='address2')
        a2.email='a2@foo.com'
        u2.addresses.append(a2)

        ctx.flush()
        assert user1.select().execute().fetchall() == [(u1.user_id, u1.name)]
        assert user2.select().execute().fetchall() == [(u2.user_id, u2.name)]
        assert address1.select().execute().fetchall() == [(a1.address_id, u1.user_id, 'a1@foo.com')]
        assert address2.select().execute().fetchall() == [(a1.address_id, u2.user_id, 'a2@foo.com')]

        ctx.clear()
        u1list = ctx.query(User, entity_name='user1').all()
        u2list = ctx.query(User, entity_name='user2').all()
        assert len(u1list) == len(u2list) == 1
        assert u1list[0] is not u2list[0]
        assert len(u1list[0].addresses) == len(u2list[0].addresses) == 1

        u1 = ctx.query(User, entity_name='user1').first()
        ctx.refresh(u1)
        ctx.expire(u1)


    def testcascade(self):
        """same as testbasic but relies on session cascading"""
        class User(object):pass
        class Address(object):pass

        a1mapper = mapper(Address, address1, entity_name='address1')
        a2mapper = mapper(Address, address2, entity_name='address2')
        u1mapper = mapper(User, user1, entity_name='user1', properties ={
            'addresses':relation(a1mapper)
        })
        u2mapper =mapper(User, user2, entity_name='user2', properties={
            'addresses':relation(a2mapper)
        })

        sess = create_session()
        u1 = User()
        u1.name = 'this is user 1'
        sess.save(u1, entity_name='user1')
        a1 = Address()
        a1.email='a1@foo.com'
        u1.addresses.append(a1)

        u2 = User()
        u2.name='this is user 2'
        a2 = Address()
        a2.email='a2@foo.com'
        u2.addresses.append(a2)
        sess.save(u2, entity_name='user2')
        print u2.__dict__

        sess.flush()
        assert user1.select().execute().fetchall() == [(u1.user_id, u1.name)]
        assert user2.select().execute().fetchall() == [(u2.user_id, u2.name)]
        assert address1.select().execute().fetchall() == [(a1.address_id, u1.user_id, 'a1@foo.com')]
        assert address2.select().execute().fetchall() == [(a1.address_id, u2.user_id, 'a2@foo.com')]

        sess.clear()
        u1list = sess.query(User, entity_name='user1').all()
        u2list = sess.query(User, entity_name='user2').all()
        assert len(u1list) == len(u2list) == 1
        assert u1list[0] is not u2list[0]
        assert len(u1list[0].addresses) == len(u2list[0].addresses) == 1

    def testpolymorphic(self):
        """tests that entity_name can be used to have two kinds of relations on the same class."""
        class User(object):
            def __init__(self, **kw):
                pass
        class Address1(object):
            def __init__(self, **kw):
                pass
        class Address2(object):
            def __init__(self, **kw):
                pass

        a1mapper = mapper(Address1, address1, extension=ctx.extension)
        a2mapper = mapper(Address2, address2, extension=ctx.extension)
        u1mapper = mapper(User, user1, entity_name='user1', properties ={
            'addresses':relation(a1mapper)
        }, extension=ctx.extension)
        u2mapper =mapper(User, user2, entity_name='user2', properties={
            'addresses':relation(a2mapper)
        }, extension=ctx.extension)

        u1 = User(_sa_entity_name='user1')
        u1.name = 'this is user 1'
        a1 = Address1()
        a1.email='a1@foo.com'
        u1.addresses.append(a1)

        u2 = User(_sa_entity_name='user2')
        u2.name='this is user 2'
        a2 = Address2()
        a2.email='a2@foo.com'
        u2.addresses.append(a2)

        ctx.flush()
        assert user1.select().execute().fetchall() == [(u1.user_id, u1.name)]
        assert user2.select().execute().fetchall() == [(u2.user_id, u2.name)]
        assert address1.select().execute().fetchall() == [(a1.address_id, u1.user_id, 'a1@foo.com')]
        assert address2.select().execute().fetchall() == [(a1.address_id, u2.user_id, 'a2@foo.com')]

        ctx.clear()
        u1list = ctx.query(User, entity_name='user1').all()
        u2list = ctx.query(User, entity_name='user2').all()
        assert len(u1list) == len(u2list) == 1
        assert u1list[0] is not u2list[0]
        assert len(u1list[0].addresses) == len(u2list[0].addresses) == 1
        # the lazy load requires that setup_loader() check that the correct LazyLoader
        # is setting up for each load
        assert isinstance(u1list[0].addresses[0], Address1)
        assert isinstance(u2list[0].addresses[0], Address2)

    def testpolymorphic_deferred(self):
        """test that deferred columns load properly using entity names"""
        class User(object):
            def __init__(self, **kwargs):
                pass
        u1mapper = mapper(User, user1, entity_name='user1', properties ={
            'name':deferred(user1.c.name)
        }, extension=ctx.extension)
        u2mapper =mapper(User, user2, entity_name='user2', properties={
            'name':deferred(user2.c.name)
        }, extension=ctx.extension)

        u1 = User(_sa_entity_name='user1')
        u1.name = 'this is user 1'

        u2 = User(_sa_entity_name='user2')
        u2.name='this is user 2'

        ctx.flush()
        assert user1.select().execute().fetchall() == [(u1.user_id, u1.name)]
        assert user2.select().execute().fetchall() == [(u2.user_id, u2.name)]

        ctx.clear()
        u1list = ctx.query(User, entity_name='user1').all()
        u2list = ctx.query(User, entity_name='user2').all()
        assert len(u1list) == len(u2list) == 1
        assert u1list[0] is not u2list[0]
        # the deferred column load requires that setup_loader() check that the correct DeferredColumnLoader
        # is setting up for each load
        assert u1list[0].name == 'this is user 1'
        assert u2list[0].name == 'this is user 2'

class SelfReferentialTest(_base.MappedTest):
    def define_tables(self, metadata):
        global nodes
            
        nodes = Table('nodes', metadata,
            Column('id', Integer, primary_key=True),
            Column('parent_id', Integer, ForeignKey('nodes.id')),
            Column('data', String(50)),
            Column('type', String(50)),
            )

    # fails inconsistently.  entity name needs deterministic 
    # instrumentation.
    def dont_test_relation(self):
        class Node(fixtures.Base):
            pass
        
        foonodes = nodes.select().where(nodes.c.type=='foo').alias()
        barnodes = nodes.select().where(nodes.c.type=='bar').alias()
        
        # TODO: the order of instrumentation here is not deterministic;
        # therefore the test fails sporadically since "Node.data" references
        # different mappers at different times
        m1 = mapper(Node, nodes)
        m2 = mapper(Node, foonodes, entity_name='foo')
        m3 = mapper(Node, barnodes, entity_name='bar')
        
        m1.add_property('foonodes', relation(m2, primaryjoin=nodes.c.id==foonodes.c.parent_id, 
            backref=backref('foo_parent', remote_side=nodes.c.id, primaryjoin=nodes.c.id==foonodes.c.parent_id)))
        m1.add_property('barnodes', relation(m3, primaryjoin=nodes.c.id==barnodes.c.parent_id, 
            backref=backref('bar_parent', remote_side=nodes.c.id, primaryjoin=nodes.c.id==barnodes.c.parent_id)))
        
        sess = create_session()
        
        n1 = Node(data='n1', type='bat')
        n1.foonodes.append(Node(data='n2', type='foo'))
        Node(data='n3', type='bar', bar_parent=n1)
        sess.save(n1)
        sess.flush()
        sess.clear()
        
        self.assertEquals(sess.query(Node, entity_name="bar").one(), Node(data='n3'))
        self.assertEquals(sess.query(Node).filter(Node.data=='n1').one(), Node(data='n1', foonodes=[Node(data='n2')], barnodes=[Node(data='n3')]))

if __name__ == "__main__":
    testenv.main()
