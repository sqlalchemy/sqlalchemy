from testbase import PersistTest, AssertMixin
import unittest, sys, os
from sqlalchemy import *
import StringIO
import testbase

from tables import *
import tables

"""test cyclical mapper relationships.  Many of the assertions are provided
via running with postgres, which is strict about foreign keys."""

class Tester(object):
    def __init__(self, data=None):
        self.data = data
        print repr(self) + " (%d)" % (id(self))
    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, repr(self.data))
        
class SelfReferentialTest(AssertMixin):
    """tests a self-referential mapper, with an additional list of child objects."""
    def setUpAll(self):
        global t1, t2, metadata
        metadata = BoundMetaData(testbase.db)
        t1 = Table('t1', metadata, 
            Column('c1', Integer, Sequence('t1c1_id_seq', optional=True), primary_key=True),
            Column('parent_c1', Integer, ForeignKey('t1.c1')),
            Column('data', String(20))
        )
        t2 = Table('t2', metadata,
            Column('c1', Integer, Sequence('t2c1_id_seq', optional=True), primary_key=True),
            Column('c1id', Integer, ForeignKey('t1.c1')),
            Column('data', String(20))
        )
        metadata.create_all()
    def tearDownAll(self):
        metadata.drop_all()
    def setUp(self):
        clear_mappers()
    
    def testsingle(self):
        class C1(Tester):
            pass
        m1 = mapper(C1, t1, properties = {
            'c1s':relation(C1, cascade="all"),
            'parent':relation(C1, primaryjoin=t1.c.parent_c1==t1.c.c1, remote_side=t1.c.c1, lazy=True, uselist=False)
        })
        a = C1('head c1')
        a.c1s.append(C1('another c1'))
        sess = create_session( )
        sess.save(a)
        sess.flush()
        sess.delete(a)
        sess.flush()
    
    def testmanytooneonly(self):
        """test that the circular dependency sort can assemble a many-to-one dependency processor
        when only the object on the "many" side is actually in the list of modified objects.
        this requires that the circular sort add the other side of the relation into the UOWTransaction
        so that the dependency operation can be tacked onto it.
        
        This also affects inheritance relationships since they rely upon circular sort as well.
        """
        class C1(Tester):
            pass
        mapper(C1, t1, properties={
            'parent':relation(C1, primaryjoin=t1.c.parent_c1==t1.c.c1, remote_side=t1.c.c1)
        })
        sess = create_session()
        c1 = C1()
        sess.save(c1)
        sess.flush()
        sess.clear()
        c1 = sess.query(C1).get(c1.c1)
        c2 = C1()
        c2.parent = c1
        sess.save(c2)
        sess.flush()
        assert c2.parent_c1==c1.c1
        
    def testcycle(self):
        class C1(Tester):
            pass
        class C2(Tester):
            pass
        
        m1 = mapper(C1, t1, properties = {
            'c1s' : relation(C1, cascade="all"),
            'c2s' : relation(mapper(C2, t2), private=True)
        })

        a = C1('head c1')
        a.c1s.append(C1('child1'))
        a.c1s.append(C1('child2'))
        a.c1s[0].c1s.append(C1('subchild1'))
        a.c1s[0].c1s.append(C1('subchild2'))
        a.c1s[1].c2s.append(C2('child2 data1'))
        a.c1s[1].c2s.append(C2('child2 data2'))
        sess = create_session( )
        sess.save(a)
        sess.flush()
        
        sess.delete(a)
        sess.flush()
    def testeagerassertion(self):
        """test that an eager self-referential relationship raises an error."""
        class C1(Tester):
            pass
        class C2(Tester):
            pass
        
        m1 = mapper(C1, t1, properties = {
            'c1s' : relation(C1, lazy=False),
        })
        
        try:
            m1.compile()
            assert False
        except exceptions.ArgumentError:
            assert True

class SelfReferentialNoPKTest(AssertMixin):
    def setUpAll(self):
        global table, meta
        meta = BoundMetaData(testbase.db)
        table = Table('item', meta,
           Column('id', Integer, primary_key=True),
           Column('uuid', String(32), unique=True, nullable=False),
           Column('parent_uuid', String(32), ForeignKey('item.uuid'), nullable=True),
        )
        meta.create_all()
    def tearDown(self):
        table.delete().execute()
    def tearDownAll(self):
        meta.drop_all()
    def testbasic(self):
        class TT(object):
            def __init__(self):
                self.uuid = hex(id(self))
        mapper(TT, table, properties={'children':relation(TT, remote_side=[table.c.parent_uuid], backref=backref('parent', remote_side=[table.c.uuid]))})
        s = create_session()
        t1 = TT()
        t1.children.append(TT())
        t1.children.append(TT())
        s.save(t1)
        s.flush()
        s.clear()
        t = s.query(TT).get_by(id=t1.id)
        assert t.children[0].parent_uuid == t1.uuid
    def testlazyclause(self):
        class TT(object):
            def __init__(self):
                self.uuid = hex(id(self))
        mapper(TT, table, properties={'children':relation(TT, remote_side=[table.c.parent_uuid], backref=backref('parent', remote_side=[table.c.uuid]))})
        s = create_session()
        t1 = TT()
        t2 = TT()
        t1.children.append(t2)
        s.save(t1)
        s.flush()
        s.clear()

        t = s.query(TT).get_by(id=t2.id)
        assert t.uuid == t2.uuid
        assert t.parent.uuid == t1.uuid
        
class InheritTestOne(AssertMixin):
    def setUpAll(self):
        global parent, child1, child2, meta
        meta = BoundMetaData(testbase.db)
        parent = Table("parent", meta,
            Column("id", Integer, primary_key=True),
            Column("parent_data", String(50)),
            Column("type", String(10))
            )

        child1 = Table("child1", meta,
            Column("id", Integer, ForeignKey("parent.id"), primary_key=True),
            Column("child1_data", String(50))
            )

        child2 = Table("child2", meta,
            Column("id", Integer, ForeignKey("parent.id"), primary_key=True),
            Column("child1_id", Integer, ForeignKey("child1.id"), nullable=False),
            Column("child2_data", String(50))
            )
        meta.create_all()
    def tearDownAll(self):
        meta.drop_all()
    def testmanytooneonly(self):
        """test similar to SelfReferentialTest.testmanytooneonly"""
        class Parent(object):
                pass

        mapper(Parent, parent)

        class Child1(Parent):
                pass

        mapper(Child1, child1, inherits=Parent)

        class Child2(Parent):
                pass

        mapper(Child2, child2, properties={
                        "child1": relation(Child1,
                                primaryjoin=child2.c.child1_id==child1.c.id,
                        )
                },inherits=Parent)

        session = create_session()

        c1 = Child1()
        c1.child1_data = "qwerty"
        session.save(c1)
        session.flush()
        session.clear()

        c1 = session.query(Child1).get_by(child1_data="qwerty")
        c2 = Child2()
        c2.child1 = c1
        c2.child2_data = "asdfgh"
        session.save(c2)
        # the flush will fail if the UOW does not set up a many-to-one DP 
        # attached to a task corresponding to c1, since "child1_id" is not nullable
        session.flush()
        
class BiDirectionalOneToManyTest(AssertMixin):
    """tests two mappers with a one-to-many relation to each other."""
    def setUpAll(self):
        global t1, t2, metadata
        metadata = BoundMetaData(testbase.db)
        t1 = Table('t1', metadata, 
            Column('c1', Integer, Sequence('t1c1_id_seq', optional=True), primary_key=True),
            Column('c2', Integer, ForeignKey('t2.c1'))
        )
        t2 = Table('t2', metadata,
            Column('c1', Integer, Sequence('t2c1_id_seq', optional=True), primary_key=True),
            Column('c2', Integer, ForeignKey('t1.c1', use_alter=True, name='t1c1_fk'))
        )
        metadata.create_all()
    def tearDownAll(self):
        metadata.drop_all()
    def tearDown(self):
        clear_mappers()
    def testcycle(self):
        class C1(object):pass
        class C2(object):pass
        
        m2 = mapper(C2, t2, properties={
            'c1s': relation(C1, primaryjoin=t2.c.c1==t1.c.c2, uselist=True)
        })
        m1 = mapper(C1, t1, properties = {
            'c2s' : relation(C2, primaryjoin=t1.c.c1==t2.c.c2, uselist=True)
        })
        a = C1()
        b = C2()
        c = C1()
        d = C2()
        e = C2()
        f = C2()
        a.c2s.append(b)
        d.c1s.append(c)
        b.c1s.append(c)
        sess = create_session()
        [sess.save(x) for x in [a,b,c,d,e,f]]
        sess.flush()

class BiDirectionalOneToManyTest2(AssertMixin):
    """tests two mappers with a one-to-many relation to each other, with a second one-to-many on one of the mappers"""
    def setUpAll(self):
        global t1, t2, t3, metadata
        metadata = BoundMetaData(testbase.db)
        t1 = Table('t1', metadata, 
            Column('c1', Integer, Sequence('t1c1_id_seq', optional=True), primary_key=True),
            Column('c2', Integer, ForeignKey('t2.c1')),
        )
        t2 = Table('t2', metadata,
            Column('c1', Integer, Sequence('t2c1_id_seq', optional=True), primary_key=True),
            Column('c2', Integer, ForeignKey('t1.c1', use_alter=True, name='t1c1_fq')),
        )
        t3 = Table('t1_data', metadata, 
            Column('c1', Integer, Sequence('t1dc1_id_seq', optional=True), primary_key=True),
            Column('t1id', Integer, ForeignKey('t1.c1')),
            Column('data', String(20)))
        metadata.create_all()
        
    def tearDown(self):
        clear_mappers()

    def tearDownAll(self):
        metadata.drop_all()
        
    def testcycle(self):
        class C1(object):pass
        class C2(object):pass
        class C1Data(object):
            def __init__(self, data=None):
                self.data = data
                
        m2 = mapper(C2, t2, properties={
            'c1s': relation(C1, primaryjoin=t2.c.c1==t1.c.c2, uselist=True)
        })
        m1 = mapper(C1, t1, properties = {
            'c2s' : relation(C2, primaryjoin=t1.c.c1==t2.c.c2, uselist=True),
            'data' : relation(mapper(C1Data, t3))
        })
        
        a = C1()
        b = C2()
        c = C1()
        d = C2()
        e = C2()
        f = C2()
        a.c2s.append(b)
        d.c1s.append(c)
        b.c1s.append(c)
        a.data.append(C1Data('c1data1'))
        a.data.append(C1Data('c1data2'))
        c.data.append(C1Data('c1data3'))
        sess = create_session()
        [sess.save(x) for x in [a,b,c,d,e,f]]
        sess.flush()

        sess.delete(d)
        sess.delete(c)
        sess.flush()

class OneToManyManyToOneTest(AssertMixin):
    """tests two mappers, one has a one-to-many on the other mapper, the other has a separate many-to-one relationship to the first.
    two tests will have a row for each item that is dependent on the other.  without the "post_update" flag, such relationships
    raise an exception when dependencies are sorted."""
    def setUpAll(self):
        global metadata
        metadata = BoundMetaData(testbase.db)
        global person    
        global ball
        ball = Table('ball', metadata,
         Column('id', Integer, Sequence('ball_id_seq', optional=True), primary_key=True),
         Column('person_id', Integer, ForeignKey('person.id', use_alter=True, name='fk_person_id')),
         Column('data', String(30))
         )
        person = Table('person', metadata,
         Column('id', Integer, Sequence('person_id_seq', optional=True), primary_key=True),
         Column('favorite_ball_id', Integer, ForeignKey('ball.id')),
         Column('data', String(30))
         )

        metadata.create_all()
        
    def tearDownAll(self):
        metadata.drop_all()
        
    def tearDown(self):
        clear_mappers()

    def testcycle(self):
        """this test has a peculiar aspect in that it doesnt create as many dependent 
        relationships as the other tests, and revealed a small glitch in the circular dependency sorting."""
        class Person(object):
         pass

        class Ball(object):
         pass

        Ball.mapper = mapper(Ball, ball)
        Person.mapper = mapper(Person, person, properties= dict(
         balls = relation(Ball.mapper, primaryjoin=ball.c.person_id==person.c.id, remote_side=ball.c.person_id),
         favorateBall = relation(Ball.mapper, primaryjoin=person.c.favorite_ball_id==ball.c.id, remote_side=person.c.favorite_ball_id),
         )
        )

        print str(Person.mapper.props['balls'].primaryjoin)
        
        b = Ball()
        p = Person()
        p.balls.append(b)
        sess = create_session()
        sess.save(b)
        sess.save(b)
        sess.flush()

    def testpostupdate_m2o(self):
        """tests a cycle between two rows, with a post_update on the many-to-one"""
        class Person(object):
            def __init__(self, data):
                self.data = data

        class Ball(object):
            def __init__(self, data):
                self.data = data

        Ball.mapper = mapper(Ball, ball)
        Person.mapper = mapper(Person, person, properties= dict(
         balls = relation(Ball.mapper, primaryjoin=ball.c.person_id==person.c.id, remote_side=ball.c.person_id, post_update=False, private=True),
         favorateBall = relation(Ball.mapper, primaryjoin=person.c.favorite_ball_id==ball.c.id, remote_side=person.c.favorite_ball_id, post_update=True),
         )
        )

        print str(Person.mapper.props['balls'].primaryjoin)

        b = Ball('some data')
        p = Person('some data')
        p.balls.append(b)
        p.balls.append(Ball('some data'))
        p.balls.append(Ball('some data'))
        p.balls.append(Ball('some data'))
        p.favorateBall = b
        sess = create_session()
        sess.save(b)
        sess.save(p)
        
        self.assert_sql(db, lambda: sess.flush(), [
            (
                "INSERT INTO person (favorite_ball_id, data) VALUES (:favorite_ball_id, :data)",
                {'favorite_ball_id': None, 'data':'some data'}
            ),
            (
                "INSERT INTO ball (person_id, data) VALUES (:person_id, :data)",
                lambda ctx:{'person_id':p.id, 'data':'some data'}
            ),
            (
                "INSERT INTO ball (person_id, data) VALUES (:person_id, :data)",
                lambda ctx:{'person_id':p.id, 'data':'some data'}
            ),
            (
                "INSERT INTO ball (person_id, data) VALUES (:person_id, :data)",
                lambda ctx:{'person_id':p.id, 'data':'some data'}
            ),
            (
                "INSERT INTO ball (person_id, data) VALUES (:person_id, :data)",
                lambda ctx:{'person_id':p.id, 'data':'some data'}
            ),
            (
                "UPDATE person SET favorite_ball_id=:favorite_ball_id WHERE person.id = :person_id",
                lambda ctx:{'favorite_ball_id':p.favorateBall.id,'person_id':p.id}
            )
        ], 
        with_sequences= [
                (
                    "INSERT INTO person (id, favorite_ball_id, data) VALUES (:id, :favorite_ball_id, :data)",
                    lambda ctx:{'id':ctx.last_inserted_ids()[0], 'favorite_ball_id': None, 'data':'some data'}
                ),
                (
                    "INSERT INTO ball (id, person_id, data) VALUES (:id, :person_id, :data)",
                    lambda ctx:{'id':ctx.last_inserted_ids()[0],'person_id':p.id, 'data':'some data'}
                ),
                (
                    "INSERT INTO ball (id, person_id, data) VALUES (:id, :person_id, :data)",
                    lambda ctx:{'id':ctx.last_inserted_ids()[0],'person_id':p.id, 'data':'some data'}
                ),
                (
                    "INSERT INTO ball (id, person_id, data) VALUES (:id, :person_id, :data)",
                    lambda ctx:{'id':ctx.last_inserted_ids()[0],'person_id':p.id, 'data':'some data'}
                ),
                (
                    "INSERT INTO ball (id, person_id, data) VALUES (:id, :person_id, :data)",
                    lambda ctx:{'id':ctx.last_inserted_ids()[0],'person_id':p.id, 'data':'some data'}
                ),
                # heres the post update 
                (
                    "UPDATE person SET favorite_ball_id=:favorite_ball_id WHERE person.id = :person_id",
                    lambda ctx:{'favorite_ball_id':p.favorateBall.id,'person_id':p.id}
                )
            ])
        sess.delete(p)
        self.assert_sql(db, lambda: sess.flush(), [
            # heres the post update (which is a pre-update with deletes)
            (
                "UPDATE person SET favorite_ball_id=:favorite_ball_id WHERE person.id = :person_id",
                lambda ctx:{'person_id': p.id, 'favorite_ball_id': None}
            ),
            (
                "DELETE FROM ball WHERE ball.id = :id",
                None
                # order cant be predicted, but something like:
                #lambda ctx:[{'id': 1L}, {'id': 4L}, {'id': 3L}, {'id': 2L}]
            ),
            (
                "DELETE FROM person WHERE person.id = :id",
                lambda ctx:[{'id': p.id}]
            )


        ])
        
    def testpostupdate_o2m(self):
        """tests a cycle between two rows, with a post_update on the one-to-many"""
        class Person(object):
            def __init__(self, data):
                self.data = data

        class Ball(object):
            def __init__(self, data):
                self.data = data

        Ball.mapper = mapper(Ball, ball)
        Person.mapper = mapper(Person, person, properties= dict(
         balls = relation(Ball.mapper, primaryjoin=ball.c.person_id==person.c.id, remote_side=ball.c.person_id, private=True, post_update=True, backref='person'),
         favorateBall = relation(Ball.mapper, primaryjoin=person.c.favorite_ball_id==ball.c.id, remote_side=person.c.favorite_ball_id),
         )
        )

        print str(Person.mapper.props['balls'].primaryjoin)

        b = Ball('some data')
        p = Person('some data')
        p.balls.append(b)
        b2 = Ball('some data')
        p.balls.append(b2)
        b3 = Ball('some data')
        p.balls.append(b3)
        b4 = Ball('some data')
        p.balls.append(b4)
        p.favorateBall = b
        sess = create_session()
        [sess.save(x) for x in [b,p,b2,b3,b4]]

        self.assert_sql(db, lambda: sess.flush(), [
                (
                    "INSERT INTO ball (person_id, data) VALUES (:person_id, :data)",
                    {'person_id':None, 'data':'some data'}
                ),
                (
                    "INSERT INTO ball (person_id, data) VALUES (:person_id, :data)",
                    {'person_id':None, 'data':'some data'}
                ),
                (
                    "INSERT INTO ball (person_id, data) VALUES (:person_id, :data)",
                    {'person_id':None, 'data':'some data'}
                ),
                (
                    "INSERT INTO ball (person_id, data) VALUES (:person_id, :data)",
                    {'person_id':None, 'data':'some data'}
                ),
                (
                    "INSERT INTO person (favorite_ball_id, data) VALUES (:favorite_ball_id, :data)",
                    lambda ctx:{'favorite_ball_id':b.id, 'data':'some data'}
                ),
                # heres the post update on each one-to-many item
                (
                    "UPDATE ball SET person_id=:person_id WHERE ball.id = :ball_id",
                    lambda ctx:{'person_id':p.id,'ball_id':b.id}
                ),
                (
                    "UPDATE ball SET person_id=:person_id WHERE ball.id = :ball_id",
                    lambda ctx:{'person_id':p.id,'ball_id':b2.id}
                ),
                (
                    "UPDATE ball SET person_id=:person_id WHERE ball.id = :ball_id",
                    lambda ctx:{'person_id':p.id,'ball_id':b3.id}
                ),
                (
                    "UPDATE ball SET person_id=:person_id WHERE ball.id = :ball_id",
                    lambda ctx:{'person_id':p.id,'ball_id':b4.id}
                ),
        ],
        with_sequences=[
            (
                "INSERT INTO ball (id, person_id, data) VALUES (:id, :person_id, :data)",
                lambda ctx:{'id':ctx.last_inserted_ids()[0], 'person_id':None, 'data':'some data'}
            ),
            (
                "INSERT INTO ball (id, person_id, data) VALUES (:id, :person_id, :data)",
                lambda ctx:{'id':ctx.last_inserted_ids()[0], 'person_id':None, 'data':'some data'}
            ),
            (
                "INSERT INTO ball (id, person_id, data) VALUES (:id, :person_id, :data)",
                lambda ctx:{'id':ctx.last_inserted_ids()[0], 'person_id':None, 'data':'some data'}
            ),
            (
                "INSERT INTO ball (id, person_id, data) VALUES (:id, :person_id, :data)",
                lambda ctx:{'id':ctx.last_inserted_ids()[0], 'person_id':None, 'data':'some data'}
            ),
            (
                "INSERT INTO person (id, favorite_ball_id, data) VALUES (:id, :favorite_ball_id, :data)",
                lambda ctx:{'id':ctx.last_inserted_ids()[0], 'favorite_ball_id':b.id, 'data':'some data'}
            ),
            (
                "UPDATE ball SET person_id=:person_id WHERE ball.id = :ball_id",
                lambda ctx:{'person_id':p.id,'ball_id':b.id}
            ),
            (
                "UPDATE ball SET person_id=:person_id WHERE ball.id = :ball_id",
                lambda ctx:{'person_id':p.id,'ball_id':b2.id}
            ),
            (
                "UPDATE ball SET person_id=:person_id WHERE ball.id = :ball_id",
                lambda ctx:{'person_id':p.id,'ball_id':b3.id}
            ),
            (
                "UPDATE ball SET person_id=:person_id WHERE ball.id = :ball_id",
                lambda ctx:{'person_id':p.id,'ball_id':b4.id}
            ),
        ])

        sess.delete(p)
        self.assert_sql(db, lambda: sess.flush(), [
            (
                "UPDATE ball SET person_id=:person_id WHERE ball.id = :ball_id",
                lambda ctx:{'person_id': None, 'ball_id': b.id}
            ),
            (
                "UPDATE ball SET person_id=:person_id WHERE ball.id = :ball_id",
                lambda ctx:{'person_id': None, 'ball_id': b2.id}
            ),
            (
                "UPDATE ball SET person_id=:person_id WHERE ball.id = :ball_id",
                lambda ctx:{'person_id': None, 'ball_id': b3.id}
            ),
            (
                "UPDATE ball SET person_id=:person_id WHERE ball.id = :ball_id",
                lambda ctx:{'person_id': None, 'ball_id': b4.id}
            ),
            (
                "DELETE FROM person WHERE person.id = :id",
                lambda ctx:[{'id':p.id}]
            ),
            (
                "DELETE FROM ball WHERE ball.id = :id",
                lambda ctx:[{'id': b.id}, {'id': b2.id}, {'id': b3.id}, {'id': b4.id}]
            )
        ])

class SelfReferentialPostUpdateTest(AssertMixin):
    def setUpAll(self):
        global metadata, node_table
        metadata = BoundMetaData(testbase.db)
        node_table = Table('node', metadata,
            Column('id', Integer, Sequence('nodeid_id_seq', optional=True), primary_key=True),
            Column('path', String(50), nullable=False),
            Column('parent_id', Integer, ForeignKey('node.id'), nullable=True),
            Column('prev_sibling_id', Integer, ForeignKey('node.id'), nullable=True),
            Column('next_sibling_id', Integer, ForeignKey('node.id'), nullable=True)
        )
        node_table.create()
    def tearDownAll(self):
        node_table.drop()
    
    def testbasic(self):
        """test that post_update only fires off when needed.
        
        this test case used to produce many superfluous update statements, particularly upon delete"""
        class Node(object):
            def __init__(self, path=''):
                self.path = path

        n_mapper = mapper(Node, node_table, properties={
            'children': relation(
                Node,
                primaryjoin=node_table.c.id==node_table.c.parent_id,
                lazy=True,
                cascade="all",
                backref=backref("parent", primaryjoin=node_table.c.parent_id==node_table.c.id, remote_side=node_table.c.id)
            ),
            'prev_sibling': relation(
                Node,
                primaryjoin=node_table.c.prev_sibling_id==node_table.c.id,
                remote_side=node_table.c.id,
                lazy=True,
                uselist=False
            ),
            'next_sibling': relation(
                Node,
                primaryjoin=node_table.c.next_sibling_id==node_table.c.id,
                remote_side=node_table.c.id,
                lazy=True,
                uselist=False,
                post_update=True
            )
        })

        session = create_session()

        def append_child(parent, child):
            if len(parent.children):
                parent.children[-1].next_sibling = child
                child.prev_sibling = parent.children[-1]
            parent.children.append(child)
        
        def remove_child(parent, child):
            child.parent = None
            node = child.next_sibling
            node.prev_sibling = child.prev_sibling
            child.prev_sibling.next_sibling = node
            session.delete(child)
        root = Node('root')

        about = Node('about')
        cats = Node('cats')
        stories = Node('stories')
        bruce = Node('bruce')

        append_child(root, about)
        assert(about.prev_sibling is None)
        append_child(root, cats)
        assert(cats.prev_sibling is about)
        assert(cats.next_sibling is None)
        assert(about.next_sibling is cats)
        assert(about.prev_sibling is None)
        append_child(root, stories)
        append_child(root, bruce)
        session.save(root)
        session.flush()

        remove_child(root, cats)
        # pre-trigger lazy loader on 'cats' to make the test easier
        cats.children
        self.assert_sql(db, lambda: session.flush(), [
            (
                "UPDATE node SET prev_sibling_id=:prev_sibling_id WHERE node.id = :node_id",
                lambda ctx:{'prev_sibling_id':about.id, 'node_id':stories.id}
            ),
            (
                "UPDATE node SET next_sibling_id=:next_sibling_id WHERE node.id = :node_id",
                lambda ctx:{'next_sibling_id':stories.id, 'node_id':about.id}
            ),
            (
                "UPDATE node SET next_sibling_id=:next_sibling_id WHERE node.id = :node_id",
                lambda ctx:{'next_sibling_id':None, 'node_id':cats.id}
            ),
            (
                "DELETE FROM node WHERE node.id = :id",
                lambda ctx:[{'id':cats.id}]
            ),
        ])

class SelfReferentialPostUpdateTest2(AssertMixin):
    def setUpAll(self):
        global metadata, a_table
        metadata = BoundMetaData(testbase.db)
        a_table = Table("a", metadata,
                Column("id", Integer(), primary_key=True),
                Column("fui", String()),
                Column("b", Integer(), ForeignKey("a.id")),
            )
        a_table.create()
    def tearDownAll(self):
        a_table.drop()
    def testbasic(self):
        """test that post_update remembers to be involved in update operations as well, 
        since it replaces the normal dependency processing completely [ticket:413]"""
        class a(object): 
            def __init__(self, fui):
                self.fui = fui

        mapper(a, a_table, properties={
            'foo': relation(a, remote_side=[a_table.c.id], post_update=True),
        })

        session = create_session()

        f1 = a("f1")
        session.save(f1)
        session.flush()

        f2 = a("f2")
        f2.foo = f1
        # at this point f1 is already inserted.  but we need post_update
        # to fire off anyway
        session.save(f2)
        session.flush()
        
        session.clear()
        f1 = session.query(a).get(f1.id)
        f2 = session.query(a).get(f2.id)
        assert f2.foo is f1
        
if __name__ == "__main__":
    testbase.main()        

