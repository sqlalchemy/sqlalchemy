from testbase import PersistTest, AssertMixin
import unittest, sys, os
from sqlalchemy import *
import StringIO
import testbase

from tables import *
import tables

# TODO: need assertion conditions in this suite


"""test cyclical mapper relationships.  No assertions yet, but run it with postgres and the 
foreign key checks alone will usually not work if something is wrong"""
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
            Column('c1', Integer, primary_key=True),
            Column('parent_c1', Integer, ForeignKey('t1.c1')),
            Column('data', String(20))
        )
        t2 = Table('t2', metadata,
            Column('c1', Integer, primary_key=True),
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
            'parent':relation(C1, primaryjoin=t1.c.parent_c1==t1.c.c1, foreignkey=t1.c.c1, lazy=True, uselist=False)
        })
        a = C1('head c1')
        a.c1s.append(C1('another c1'))
        sess = create_session(echo_uow=False)
        sess.save(a)
        sess.flush()
        sess.delete(a)
        sess.flush()
        
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
        sess = create_session(echo_uow=False)
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
class BiDirectionalOneToManyTest(AssertMixin):
    """tests two mappers with a one-to-many relation to each other."""
    def setUpAll(self):
        global t1, t2, metadata
        metadata = BoundMetaData(testbase.db)
        t1 = Table('t1', metadata, 
            Column('c1', Integer, primary_key=True),
            Column('c2', Integer, ForeignKey('t2.c1'))
        )
        t2 = Table('t2', metadata,
            Column('c1', Integer, primary_key=True),
            Column('c2', Integer)
        )
        metadata.create_all()
        t2.c.c2.append_item(ForeignKey('t1.c1'))
    def tearDownAll(self):
        t1.drop()
        t2.drop()
        #metadata.drop_all()
    def tearDown(self):
        clear_mappers()
    def testcycle(self):
        class C1(object):pass
        class C2(object):pass
        
        m2 = mapper(C2, t2)
        m1 = mapper(C1, t1, properties = {
            'c2s' : relation(m2, primaryjoin=t1.c.c2==t2.c.c1, uselist=True)
        })
        m2.add_property('c1s', relation(m1, primaryjoin=t2.c.c2==t1.c.c1, uselist=True))
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
            Column('c1', Integer, primary_key=True),
            Column('c2', Integer, ForeignKey('t2.c1')),
        )
        t2 = Table('t2', metadata,
            Column('c1', Integer, primary_key=True),
            Column('c2', Integer),
        )
        t2.create()
        t1.create()
        t2.c.c2.append_item(ForeignKey('t1.c1'))
        t3 = Table('t1_data', metadata, 
            Column('c1', Integer, primary_key=True),
            Column('t1id', Integer, ForeignKey('t1.c1')),
            Column('data', String(20)))
        t3.create()
        
    def tearDown(self):
        clear_mappers()

    def tearDownAll(self):
        t3.drop()
        t1.drop()
        t2.drop()
        
    def testcycle(self):
        class C1(object):pass
        class C2(object):pass
        class C1Data(object):
            def __init__(self, data=None):
                self.data = data
                
        m2 = mapper(C2, t2)
        m1 = mapper(C1, t1, properties = {
            'c2s' : relation(m2, primaryjoin=t1.c.c2==t2.c.c1, uselist=True),
            'data' : relation(mapper(C1Data, t3))
        })
        m2.add_property('c1s', relation(m1, primaryjoin=t2.c.c2==t1.c.c1, uselist=True))
        
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
         Column('person_id', Integer),
         )
        person = Table('person', metadata,
         Column('id', Integer, Sequence('person_id_seq', optional=True), primary_key=True),
         Column('favorite_ball_id', Integer, ForeignKey('ball.id')),
#         Column('favorite_ball_id', Integer),
         )

        ball.create()
        person.create()
#        person.c.favorite_ball_id.append_item(ForeignKey('ball.id'))
        ball.c.person_id.append_item(ForeignKey('person.id'))
        
        # make the test more complete for postgres
        if db.engine.__module__.endswith('postgres'):
            db.execute("alter table ball add constraint fk_ball_person foreign key (person_id) references person(id)", {})
    def tearDownAll(self):
        if db.engine.__module__.endswith('postgres'):
            db.execute("alter table ball drop constraint fk_ball_person", {})
        person.drop()
        ball.drop()
        
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
         balls = relation(Ball.mapper, primaryjoin=ball.c.person_id==person.c.id, foreignkey=ball.c.person_id),
         favorateBall = relation(Ball.mapper, primaryjoin=person.c.favorite_ball_id==ball.c.id, foreignkey=person.c.favorite_ball_id),
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
         pass

        class Ball(object):
         pass

        Ball.mapper = mapper(Ball, ball)
        Person.mapper = mapper(Person, person, properties= dict(
         balls = relation(Ball.mapper, primaryjoin=ball.c.person_id==person.c.id, foreignkey=ball.c.person_id, post_update=False, private=True),
         favorateBall = relation(Ball.mapper, primaryjoin=person.c.favorite_ball_id==ball.c.id, foreignkey=person.c.favorite_ball_id, post_update=True),
         )
        )

        print str(Person.mapper.props['balls'].primaryjoin)

        b = Ball()
        p = Person()
        p.balls.append(b)
        p.balls.append(Ball())
        p.balls.append(Ball())
        p.balls.append(Ball())
        p.favorateBall = b
        sess = create_session()
        sess.save(b)
        sess.save(p)
        
        self.assert_sql(db, lambda: sess.flush(), [
            (
                "INSERT INTO person (favorite_ball_id) VALUES (:favorite_ball_id)",
                {'favorite_ball_id': None}
            ),
            (
                "INSERT INTO ball (person_id) VALUES (:person_id)",
                lambda ctx:{'person_id':p.id}
            ),
            (
                "INSERT INTO ball (person_id) VALUES (:person_id)",
                lambda ctx:{'person_id':p.id}
            ),
            (
                "INSERT INTO ball (person_id) VALUES (:person_id)",
                lambda ctx:{'person_id':p.id}
            ),
            (
                "INSERT INTO ball (person_id) VALUES (:person_id)",
                lambda ctx:{'person_id':p.id}
            ),
            (
                "UPDATE person SET favorite_ball_id=:favorite_ball_id WHERE person.id = :person_id",
                lambda ctx:{'favorite_ball_id':p.favorateBall.id,'person_id':p.id}
            )
        ], 
        with_sequences= [
                (
                    "INSERT INTO person (id, favorite_ball_id) VALUES (:id, :favorite_ball_id)",
                    lambda ctx:{'id':ctx.last_inserted_ids()[0], 'favorite_ball_id': None}
                ),
                (
                    "INSERT INTO ball (id, person_id) VALUES (:id, :person_id)",
                    lambda ctx:{'id':ctx.last_inserted_ids()[0],'person_id':p.id}
                ),
                (
                    "INSERT INTO ball (id, person_id) VALUES (:id, :person_id)",
                    lambda ctx:{'id':ctx.last_inserted_ids()[0],'person_id':p.id}
                ),
                (
                    "INSERT INTO ball (id, person_id) VALUES (:id, :person_id)",
                    lambda ctx:{'id':ctx.last_inserted_ids()[0],'person_id':p.id}
                ),
                (
                    "INSERT INTO ball (id, person_id) VALUES (:id, :person_id)",
                    lambda ctx:{'id':ctx.last_inserted_ids()[0],'person_id':p.id}
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
         pass

        class Ball(object):
         pass

        Ball.mapper = mapper(Ball, ball)
        Person.mapper = mapper(Person, person, properties= dict(
         balls = relation(Ball.mapper, primaryjoin=ball.c.person_id==person.c.id, foreignkey=ball.c.person_id, private=True, post_update=True),
         favorateBall = relation(Ball.mapper, primaryjoin=person.c.favorite_ball_id==ball.c.id, foreignkey=person.c.favorite_ball_id),
         )
        )

        print str(Person.mapper.props['balls'].primaryjoin)

        b = Ball()
        p = Person()
        p.balls.append(b)
        b2 = Ball()
        p.balls.append(b2)
        b3 = Ball()
        p.balls.append(b3)
        b4 = Ball()
        p.balls.append(b4)
        p.favorateBall = b
        sess = create_session()
        [sess.save(x) for x in [b,p,b2,b3,b4]]

        self.assert_sql(db, lambda: sess.flush(), [
                (
                    "INSERT INTO ball (person_id) VALUES (:person_id)",
                    {'person_id':None}
                ),
                (
                    "INSERT INTO ball (person_id) VALUES (:person_id)",
                    {'person_id':None}
                ),
                (
                    "INSERT INTO ball (person_id) VALUES (:person_id)",
                    {'person_id':None}
                ),
                (
                    "INSERT INTO ball (person_id) VALUES (:person_id)",
                    {'person_id':None}
                ),
                (
                    "INSERT INTO person (favorite_ball_id) VALUES (:favorite_ball_id)",
                    lambda ctx:{'favorite_ball_id':b.id}
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
                "INSERT INTO ball (id, person_id) VALUES (:id, :person_id)",
                lambda ctx:{'id':ctx.last_inserted_ids()[0], 'person_id':None}
            ),
            (
                "INSERT INTO ball (id, person_id) VALUES (:id, :person_id)",
                lambda ctx:{'id':ctx.last_inserted_ids()[0], 'person_id':None}
            ),
            (
                "INSERT INTO ball (id, person_id) VALUES (:id, :person_id)",
                lambda ctx:{'id':ctx.last_inserted_ids()[0], 'person_id':None}
            ),
            (
                "INSERT INTO ball (id, person_id) VALUES (:id, :person_id)",
                lambda ctx:{'id':ctx.last_inserted_ids()[0], 'person_id':None}
            ),
            (
                "INSERT INTO person (id, favorite_ball_id) VALUES (:id, :favorite_ball_id)",
                lambda ctx:{'id':ctx.last_inserted_ids()[0], 'favorite_ball_id':b.id}
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
                None
                # the order of deletion is not predictable, but its roughly:
                # lambda ctx:[{'id': b.id}, {'id': b2.id}, {'id': b3.id}, {'id': b4.id}]
            )
        ])
        
if __name__ == "__main__":
    testbase.main()        

