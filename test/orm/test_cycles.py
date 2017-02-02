"""Tests cyclical mapper relationships.

We might want to try an automated generate of much of this, all combos of
T1<->T2, with o2m or m2o between them, and a third T3 with o2m/m2o to one/both
T1/T2.

"""
from sqlalchemy import testing
from sqlalchemy import Integer, String, ForeignKey
from sqlalchemy.testing.schema import Table, Column
from sqlalchemy.orm import mapper, relationship, backref, \
    create_session, sessionmaker
from sqlalchemy.testing import eq_, is_
from sqlalchemy.testing.assertsql import RegexSQL, CompiledSQL, AllOf
from sqlalchemy.testing import fixtures


class SelfReferentialTest(fixtures.MappedTest):
    """A self-referential mapper with an additional list of child objects."""

    @classmethod
    def define_tables(cls, metadata):
        Table('t1', metadata,
              Column('c1', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('parent_c1', Integer, ForeignKey('t1.c1')),
              Column('data', String(20)))
        Table('t2', metadata,
              Column('c1', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('c1id', Integer, ForeignKey('t1.c1')),
              Column('data', String(20)))

    @classmethod
    def setup_classes(cls):
        class C1(cls.Basic):
            def __init__(self, data=None):
                self.data = data

        class C2(cls.Basic):
            def __init__(self, data=None):
                self.data = data

    def test_single(self):
        C1, t1 = self.classes.C1, self.tables.t1

        mapper(C1, t1, properties={
            'c1s': relationship(C1, cascade="all"),
            'parent': relationship(C1,
                                   primaryjoin=t1.c.parent_c1 == t1.c.c1,
                                   remote_side=t1.c.c1,
                                   lazy='select',
                                   uselist=False)})
        a = C1('head c1')
        a.c1s.append(C1('another c1'))

        sess = create_session()
        sess.add(a)
        sess.flush()
        sess.delete(a)
        sess.flush()

    def test_many_to_one_only(self):
        """

        test that the circular dependency sort can assemble a many-to-one
        dependency processor when only the object on the "many" side is
        actually in the list of modified objects.

        """

        C1, t1 = self.classes.C1, self.tables.t1

        mapper(C1, t1, properties={
            'parent': relationship(C1,
                                   primaryjoin=t1.c.parent_c1 == t1.c.c1,
                                   remote_side=t1.c.c1)})

        c1 = C1()

        sess = create_session()
        sess.add(c1)
        sess.flush()
        sess.expunge_all()
        c1 = sess.query(C1).get(c1.c1)
        c2 = C1()
        c2.parent = c1
        sess.add(c2)
        sess.flush()
        assert c2.parent_c1 == c1.c1

    def test_cycle(self):
        C2, C1, t2, t1 = (self.classes.C2,
                          self.classes.C1,
                          self.tables.t2,
                          self.tables.t1)

        mapper(C1, t1, properties={
            'c1s': relationship(C1, cascade="all"),
            'c2s': relationship(mapper(C2, t2), cascade="all, delete-orphan")})

        a = C1('head c1')
        a.c1s.append(C1('child1'))
        a.c1s.append(C1('child2'))
        a.c1s[0].c1s.append(C1('subchild1'))
        a.c1s[0].c1s.append(C1('subchild2'))
        a.c1s[1].c2s.append(C2('child2 data1'))
        a.c1s[1].c2s.append(C2('child2 data2'))
        sess = create_session()
        sess.add(a)
        sess.flush()

        sess.delete(a)
        sess.flush()

    def test_setnull_ondelete(self):
        C1, t1 = self.classes.C1, self.tables.t1

        mapper(C1, t1, properties={
            'children': relationship(C1)
        })

        sess = create_session()
        c1 = C1()
        c2 = C1()
        c1.children.append(c2)
        sess.add(c1)
        sess.flush()
        assert c2.parent_c1 == c1.c1

        sess.delete(c1)
        sess.flush()
        assert c2.parent_c1 is None

        sess.expire_all()
        assert c2.parent_c1 is None


class SelfReferentialNoPKTest(fixtures.MappedTest):
    """A self-referential relationship that joins on a column other than the
    primary key column"""

    @classmethod
    def define_tables(cls, metadata):
        Table('item', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('uuid', String(32), unique=True, nullable=False),
              Column('parent_uuid', String(32), ForeignKey('item.uuid'),
                     nullable=True))

    @classmethod
    def setup_classes(cls):
        class TT(cls.Basic):
            def __init__(self):
                self.uuid = hex(id(self))

    @classmethod
    def setup_mappers(cls):
        item, TT = cls.tables.item, cls.classes.TT

        mapper(TT, item, properties={
            'children': relationship(
                TT,
                remote_side=[item.c.parent_uuid],
                backref=backref('parent', remote_side=[item.c.uuid]))})

    def test_basic(self):
        TT = self.classes.TT

        t1 = TT()
        t1.children.append(TT())
        t1.children.append(TT())

        s = create_session()
        s.add(t1)
        s.flush()
        s.expunge_all()
        t = s.query(TT).filter_by(id=t1.id).one()
        eq_(t.children[0].parent_uuid, t1.uuid)

    def test_lazy_clause(self):
        TT = self.classes.TT

        s = create_session()
        t1 = TT()
        t2 = TT()
        t1.children.append(t2)
        s.add(t1)
        s.flush()
        s.expunge_all()

        t = s.query(TT).filter_by(id=t2.id).one()
        eq_(t.uuid, t2.uuid)
        eq_(t.parent.uuid, t1.uuid)


class InheritTestOne(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table("parent", metadata,
              Column("id", Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column("parent_data", String(50)),
              Column("type", String(10)))

        Table("child1", metadata,
              Column("id", Integer, ForeignKey("parent.id"), primary_key=True),
              Column("child1_data", String(50)))

        Table("child2", metadata,
              Column("id", Integer, ForeignKey("parent.id"), primary_key=True),
              Column("child1_id", Integer, ForeignKey("child1.id"),
                     nullable=False),
              Column("child2_data", String(50)))

    @classmethod
    def setup_classes(cls):
        class Parent(cls.Basic):
            pass

        class Child1(Parent):
            pass

        class Child2(Parent):
            pass

    @classmethod
    def setup_mappers(cls):
        child1, child2, parent, Parent, Child1, Child2 = (cls.tables.child1,
                                                          cls.tables.child2,
                                                          cls.tables.parent,
                                                          cls.classes.Parent,
                                                          cls.classes.Child1,
                                                          cls.classes.Child2)

        mapper(Parent, parent)
        mapper(Child1, child1, inherits=Parent)
        mapper(Child2, child2, inherits=Parent, properties=dict(
            child1=relationship(
                Child1,
                primaryjoin=child2.c.child1_id == child1.c.id)))

    def test_many_to_one_only(self):
        """test similar to SelfReferentialTest.testmanytooneonly"""

        Child1, Child2 = self.classes.Child1, self.classes.Child2

        session = create_session()

        c1 = Child1()
        c1.child1_data = "qwerty"
        session.add(c1)
        session.flush()
        session.expunge_all()

        c1 = session.query(Child1).filter_by(child1_data="qwerty").one()
        c2 = Child2()
        c2.child1 = c1
        c2.child2_data = "asdfgh"
        session.add(c2)

        # the flush will fail if the UOW does not set up a many-to-one DP
        # attached to a task corresponding to c1, since "child1_id" is not
        # nullable
        session.flush()


class InheritTestTwo(fixtures.MappedTest):
    """

    The fix in BiDirectionalManyToOneTest raised this issue, regarding the
    'circular sort' containing UOWTasks that were still polymorphic, which
    could create duplicate entries in the final sort

    """

    @classmethod
    def define_tables(cls, metadata):
        Table('a', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('cid', Integer, ForeignKey('c.id')))

        Table('b', metadata,
              Column('id', Integer, ForeignKey("a.id"), primary_key=True))

        Table('c', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('aid', Integer,
                     ForeignKey('a.id', name="foo")))

    @classmethod
    def setup_classes(cls):
        class A(cls.Basic):
            pass

        class B(A):
            pass

        class C(cls.Basic):
            pass

    def test_flush(self):
        a, A, c, b, C, B = (self.tables.a,
                            self.classes.A,
                            self.tables.c,
                            self.tables.b,
                            self.classes.C,
                            self.classes.B)

        mapper(A, a, properties={
            'cs': relationship(C, primaryjoin=a.c.cid == c.c.id)})

        mapper(B, b, inherits=A, inherit_condition=b.c.id == a.c.id)

        mapper(C, c, properties={
            'arel': relationship(A, primaryjoin=a.c.id == c.c.aid)})

        sess = create_session()
        bobj = B()
        sess.add(bobj)
        cobj = C()
        sess.add(cobj)
        sess.flush()


class BiDirectionalManyToOneTest(fixtures.MappedTest):
    run_define_tables = 'each'

    @classmethod
    def define_tables(cls, metadata):
        Table('t1', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('data', String(30)),
              Column('t2id', Integer, ForeignKey('t2.id')))
        Table('t2', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('data', String(30)),
              Column('t1id', Integer,
                     ForeignKey('t1.id', name="foo_fk")))
        Table('t3', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('data', String(30)),
              Column('t1id', Integer, ForeignKey('t1.id'), nullable=False),
              Column('t2id', Integer, ForeignKey('t2.id'), nullable=False))

    @classmethod
    def setup_classes(cls):
        class T1(cls.Basic):
            pass

        class T2(cls.Basic):
            pass

        class T3(cls.Basic):
            pass

    @classmethod
    def setup_mappers(cls):
        t2, T2, T3, t1, t3, T1 = (cls.tables.t2,
                                  cls.classes.T2,
                                  cls.classes.T3,
                                  cls.tables.t1,
                                  cls.tables.t3,
                                  cls.classes.T1)

        mapper(T1, t1, properties={
            't2': relationship(T2, primaryjoin=t1.c.t2id == t2.c.id)})
        mapper(T2, t2, properties={
            't1': relationship(T1, primaryjoin=t2.c.t1id == t1.c.id)})
        mapper(T3, t3, properties={
            't1': relationship(T1),
            't2': relationship(T2)})

    def test_reflush(self):
        T2, T3, T1 = (self.classes.T2,
                      self.classes.T3,
                      self.classes.T1)

        o1 = T1()
        o1.t2 = T2()
        sess = create_session()
        sess.add(o1)
        sess.flush()

        # the bug here is that the dependency sort comes up with T1/T2 in a
        # cycle, but there are no T1/T2 objects to be saved.  therefore no
        # "cyclical subtree" gets generated, and one or the other of T1/T2
        # gets lost, and processors on T3 don't fire off.  the test will then
        # fail because the FK's on T3 are not nullable.
        o3 = T3()
        o3.t1 = o1
        o3.t2 = o1.t2
        sess.add(o3)
        sess.flush()

    def test_reflush_2(self):
        """A variant on test_reflush()"""

        T2, T3, T1 = (self.classes.T2,
                      self.classes.T3,
                      self.classes.T1)

        o1 = T1()
        o1.t2 = T2()
        sess = create_session()
        sess.add(o1)
        sess.flush()

        # in this case, T1, T2, and T3 tasks will all be in the cyclical
        # tree normally.  the dependency processors for T3 are part of the
        # 'extradeps' collection so they all get assembled into the tree
        # as well.
        o1a = T1()
        o2a = T2()
        sess.add(o1a)
        sess.add(o2a)
        o3b = T3()
        o3b.t1 = o1a
        o3b.t2 = o2a
        sess.add(o3b)

        o3 = T3()
        o3.t1 = o1
        o3.t2 = o1.t2
        sess.add(o3)
        sess.flush()


class BiDirectionalOneToManyTest(fixtures.MappedTest):
    """tests two mappers with a one-to-many relationship to each other."""

    run_define_tables = 'each'

    @classmethod
    def define_tables(cls, metadata):
        Table('t1', metadata,
              Column('c1', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('c2', Integer, ForeignKey('t2.c1')))

        Table('t2', metadata,
              Column('c1', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('c2', Integer,
                     ForeignKey('t1.c1', name='t1c1_fk')))

    @classmethod
    def setup_classes(cls):
        class C1(cls.Basic):
            pass

        class C2(cls.Basic):
            pass

    def test_cycle(self):
        C2, C1, t2, t1 = (self.classes.C2,
                          self.classes.C1,
                          self.tables.t2,
                          self.tables.t1)

        mapper(C2, t2, properties={
            'c1s': relationship(C1,
                                primaryjoin=t2.c.c1 == t1.c.c2,
                                uselist=True)})
        mapper(C1, t1, properties={
            'c2s': relationship(C2,
                                primaryjoin=t1.c.c1 == t2.c.c2,
                                uselist=True)})

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
        sess.add_all((a, b, c, d, e, f))
        sess.flush()


class BiDirectionalOneToManyTest2(fixtures.MappedTest):
    """Two mappers with a one-to-many relationship to each other,
    with a second one-to-many on one of the mappers"""

    run_define_tables = 'each'

    @classmethod
    def define_tables(cls, metadata):
        Table('t1', metadata,
              Column('c1', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('c2', Integer, ForeignKey('t2.c1')),
              test_needs_autoincrement=True)

        Table('t2', metadata,
              Column('c1', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('c2', Integer,
                     ForeignKey('t1.c1', name='t1c1_fq')),
              test_needs_autoincrement=True)

        Table('t1_data', metadata,
              Column('c1', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('t1id', Integer, ForeignKey('t1.c1')),
              Column('data', String(20)),
              test_needs_autoincrement=True)

    @classmethod
    def setup_classes(cls):
        class C1(cls.Basic):
            pass

        class C2(cls.Basic):
            pass

        class C1Data(cls.Basic):
            pass

    @classmethod
    def setup_mappers(cls):
        t2, t1, C1Data, t1_data, C2, C1 = (cls.tables.t2,
                                           cls.tables.t1,
                                           cls.classes.C1Data,
                                           cls.tables.t1_data,
                                           cls.classes.C2,
                                           cls.classes.C1)

        mapper(C2, t2, properties={
            'c1s': relationship(C1,
                                primaryjoin=t2.c.c1 == t1.c.c2,
                                uselist=True)})
        mapper(C1, t1, properties={
            'c2s': relationship(C2,
                                primaryjoin=t1.c.c1 == t2.c.c2,
                                uselist=True),
            'data': relationship(mapper(C1Data, t1_data))})

    def test_cycle(self):
        C2, C1, C1Data = (self.classes.C2,
                          self.classes.C1,
                          self.classes.C1Data)

        a = C1()
        b = C2()
        c = C1()
        d = C2()
        e = C2()
        f = C2()
        a.c2s.append(b)
        d.c1s.append(c)
        b.c1s.append(c)
        a.data.append(C1Data(data='c1data1'))
        a.data.append(C1Data(data='c1data2'))
        c.data.append(C1Data(data='c1data3'))
        sess = create_session()
        sess.add_all((a, b, c, d, e, f))
        sess.flush()

        sess.delete(d)
        sess.delete(c)
        sess.flush()


class OneToManyManyToOneTest(fixtures.MappedTest):
    """

    Tests two mappers, one has a one-to-many on the other mapper, the other
    has a separate many-to-one relationship to the first.  two tests will have
    a row for each item that is dependent on the other.  without the
    "post_update" flag, such relationships raise an exception when
    dependencies are sorted.

    """
    run_define_tables = 'each'

    @classmethod
    def define_tables(cls, metadata):
        Table('ball', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('person_id', Integer,
                     ForeignKey('person.id', name='fk_person_id')),
              Column('data', String(30)))

        Table('person', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('favorite_ball_id', Integer, ForeignKey('ball.id')),
              Column('data', String(30)))

    @classmethod
    def setup_classes(cls):
        class Person(cls.Basic):
            pass

        class Ball(cls.Basic):
            pass

    def test_cycle(self):
        """
        This test has a peculiar aspect in that it doesn't create as many
        dependent relationships as the other tests, and revealed a small
        glitch in the circular dependency sorting.

        """

        person, ball, Ball, Person = (self.tables.person,
                                      self.tables.ball,
                                      self.classes.Ball,
                                      self.classes.Person)

        mapper(Ball, ball)
        mapper(Person, person, properties=dict(
            balls=relationship(Ball,
                               primaryjoin=ball.c.person_id == person.c.id,
                               remote_side=ball.c.person_id),
            favorite=relationship(
                Ball,
                primaryjoin=person.c.favorite_ball_id == ball.c.id,
                remote_side=ball.c.id)))

        b = Ball()
        p = Person()
        p.balls.append(b)
        sess = create_session()
        sess.add(p)
        sess.flush()

    def test_post_update_m2o(self):
        """A cycle between two rows, with a post_update on the many-to-one"""

        person, ball, Ball, Person = (self.tables.person,
                                      self.tables.ball,
                                      self.classes.Ball,
                                      self.classes.Person)

        mapper(Ball, ball)
        mapper(Person, person, properties=dict(
            balls=relationship(Ball,
                               primaryjoin=ball.c.person_id == person.c.id,
                               remote_side=ball.c.person_id,
                               post_update=False,
                               cascade="all, delete-orphan"),
            favorite=relationship(
                Ball,
                primaryjoin=person.c.favorite_ball_id == ball.c.id,
                remote_side=person.c.favorite_ball_id,
                post_update=True)))

        b = Ball(data='some data')
        p = Person(data='some data')
        p.balls.append(b)
        p.balls.append(Ball(data='some data'))
        p.balls.append(Ball(data='some data'))
        p.balls.append(Ball(data='some data'))
        p.favorite = b
        sess = create_session()
        sess.add(b)
        sess.add(p)

        self.assert_sql_execution(
            testing.db,
            sess.flush,
            RegexSQL("^INSERT INTO person", {'data': 'some data'}),
            RegexSQL("^INSERT INTO ball", lambda c: {
                     'person_id': p.id, 'data': 'some data'}),
            RegexSQL("^INSERT INTO ball", lambda c: {
                     'person_id': p.id, 'data': 'some data'}),
            RegexSQL("^INSERT INTO ball", lambda c: {
                     'person_id': p.id, 'data': 'some data'}),
            RegexSQL("^INSERT INTO ball", lambda c: {
                     'person_id': p.id, 'data': 'some data'}),
            CompiledSQL("UPDATE person SET favorite_ball_id=:favorite_ball_id "
                        "WHERE person.id = :person_id",
                        lambda ctx: {
                            'favorite_ball_id': p.favorite.id,
                            'person_id': p.id}
                        ),
        )

        sess.delete(p)

        self.assert_sql_execution(
            testing.db,
            sess.flush,
            CompiledSQL("UPDATE person SET favorite_ball_id=:favorite_ball_id "
                        "WHERE person.id = :person_id",
                        lambda ctx: {'person_id': p.id,
                                     'favorite_ball_id': None}),
            # lambda ctx:[{'id': 1L}, {'id': 4L}, {'id': 3L}, {'id': 2L}])
            CompiledSQL("DELETE FROM ball WHERE ball.id = :id", None),
            CompiledSQL("DELETE FROM person WHERE person.id = :id",
                        lambda ctx: [{'id': p.id}])
        )

    def test_post_update_backref(self):
        """test bidirectional post_update."""

        person, ball, Ball, Person = (self.tables.person,
                                      self.tables.ball,
                                      self.classes.Ball,
                                      self.classes.Person)

        mapper(Ball, ball)
        mapper(Person, person, properties=dict(
            balls=relationship(Ball,
                               primaryjoin=ball.c.person_id == person.c.id,
                               remote_side=ball.c.person_id, post_update=True,
                               backref=backref('person', post_update=True)
                               ),
            favorite=relationship(
                Ball,
                primaryjoin=person.c.favorite_ball_id == ball.c.id,
                remote_side=person.c.favorite_ball_id)
        ))

        sess = sessionmaker()()
        p1 = Person(data='p1')
        p2 = Person(data='p2')
        p3 = Person(data='p3')

        b1 = Ball(data='b1')

        b1.person = p1
        sess.add_all([p1, p2, p3])
        sess.commit()

        # switch here.  the post_update
        # on ball.person can't get tripped up
        # by the fact that there's a "reverse" prop.
        b1.person = p2
        sess.commit()
        eq_(
            p2, b1.person
        )

        # do it the other way
        p3.balls.append(b1)
        sess.commit()
        eq_(
            p3, b1.person
        )

    def test_post_update_o2m(self):
        """A cycle between two rows, with a post_update on the one-to-many"""

        person, ball, Ball, Person = (self.tables.person,
                                      self.tables.ball,
                                      self.classes.Ball,
                                      self.classes.Person)

        mapper(Ball, ball)
        mapper(Person, person, properties=dict(
            balls=relationship(Ball,
                               primaryjoin=ball.c.person_id == person.c.id,
                               remote_side=ball.c.person_id,
                               cascade="all, delete-orphan",
                               post_update=True,
                               backref='person'),
            favorite=relationship(
                Ball,
                primaryjoin=person.c.favorite_ball_id == ball.c.id,
                remote_side=person.c.favorite_ball_id)))

        b = Ball(data='some data')
        p = Person(data='some data')
        p.balls.append(b)
        b2 = Ball(data='some data')
        p.balls.append(b2)
        b3 = Ball(data='some data')
        p.balls.append(b3)
        b4 = Ball(data='some data')
        p.balls.append(b4)
        p.favorite = b
        sess = create_session()
        sess.add_all((b, p, b2, b3, b4))

        self.assert_sql_execution(
            testing.db,
            sess.flush,
            CompiledSQL("INSERT INTO ball (person_id, data) "
                        "VALUES (:person_id, :data)",
                        {'person_id': None, 'data': 'some data'}),

            CompiledSQL("INSERT INTO ball (person_id, data) "
                        "VALUES (:person_id, :data)",
                        {'person_id': None, 'data': 'some data'}),

            CompiledSQL("INSERT INTO ball (person_id, data) "
                        "VALUES (:person_id, :data)",
                        {'person_id': None, 'data': 'some data'}),

            CompiledSQL("INSERT INTO ball (person_id, data) "
                        "VALUES (:person_id, :data)",
                        {'person_id': None, 'data': 'some data'}),

            CompiledSQL("INSERT INTO person (favorite_ball_id, data) "
                        "VALUES (:favorite_ball_id, :data)",
                        lambda ctx: {'favorite_ball_id': b.id,
                                     'data': 'some data'}),

            CompiledSQL("UPDATE ball SET person_id=:person_id "
                        "WHERE ball.id = :ball_id",
                        lambda ctx: [
                            {'person_id': p.id, 'ball_id': b.id},
                            {'person_id': p.id, 'ball_id': b2.id},
                            {'person_id': p.id, 'ball_id': b3.id},
                            {'person_id': p.id, 'ball_id': b4.id}
                        ]),
        )

        sess.delete(p)

        self.assert_sql_execution(testing.db, sess.flush,
                                  CompiledSQL(
                                      "UPDATE ball SET person_id=:person_id "
                                      "WHERE ball.id = :ball_id",
                                      lambda ctx: [
                                          {'person_id': None,
                                           'ball_id': b.id},
                                          {'person_id': None,
                                           'ball_id': b2.id},
                                          {'person_id': None,
                                           'ball_id': b3.id},
                                          {'person_id': None,
                                           'ball_id': b4.id}
                                      ]
                                  ),
                                  CompiledSQL(
                                      "DELETE FROM person "
                                      "WHERE person.id = :id",
                                      lambda ctx: [{'id': p.id}]),

                                  CompiledSQL(
                                      "DELETE FROM ball WHERE ball.id = :id",
                                      lambda ctx: [{'id': b.id},
                                                   {'id': b2.id},
                                                   {'id': b3.id},
                                                   {'id': b4.id}])
                                  )

    def test_post_update_m2o_detect_none(self):
        person, ball, Ball, Person = (
            self.tables.person,
            self.tables.ball,
            self.classes.Ball,
            self.classes.Person)

        mapper(Ball, ball, properties={
            'person': relationship(
                Person, post_update=True,
                primaryjoin=person.c.id == ball.c.person_id)
        })
        mapper(Person, person)

        sess = create_session(autocommit=False, expire_on_commit=True)
        sess.add(Ball(person=Person()))
        sess.commit()
        b1 = sess.query(Ball).first()

        # needs to be unloaded
        assert 'person' not in b1.__dict__
        b1.person = None

        self.assert_sql_execution(
            testing.db,
            sess.flush,
            CompiledSQL(
                "UPDATE ball SET person_id=:person_id "
                "WHERE ball.id = :ball_id",
                lambda ctx: {'person_id': None, 'ball_id': b1.id})
        )

        is_(b1.person, None)


class SelfReferentialPostUpdateTest(fixtures.MappedTest):
    """Post_update on a single self-referential mapper.


    """

    @classmethod
    def define_tables(cls, metadata):
        Table('node', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('path', String(50), nullable=False),
              Column('parent_id', Integer,
                     ForeignKey('node.id'), nullable=True),
              Column('prev_sibling_id', Integer,
                     ForeignKey('node.id'), nullable=True),
              Column('next_sibling_id', Integer,
                     ForeignKey('node.id'), nullable=True))

    @classmethod
    def setup_classes(cls):
        class Node(cls.Basic):
            def __init__(self, path=''):
                self.path = path

    def test_one(self):
        """Post_update only fires off when needed.

        This test case used to produce many superfluous update statements,
        particularly upon delete

        """

        node, Node = self.tables.node, self.classes.Node

        mapper(Node, node, properties={
            'children': relationship(
                Node,
                primaryjoin=node.c.id == node.c.parent_id,
                cascade="all",
                backref=backref("parent", remote_side=node.c.id)
            ),
            'prev_sibling': relationship(
                Node,
                primaryjoin=node.c.prev_sibling_id == node.c.id,
                remote_side=node.c.id,
                uselist=False),
            'next_sibling': relationship(
                Node,
                primaryjoin=node.c.next_sibling_id == node.c.id,
                remote_side=node.c.id,
                uselist=False,
                post_update=True)})

        session = create_session()

        def append_child(parent, child):
            if parent.children:
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
        session.add(root)
        session.flush()

        remove_child(root, cats)

        # pre-trigger lazy loader on 'cats' to make the test easier
        cats.children
        self.assert_sql_execution(
            testing.db,
            session.flush,
            AllOf(
                CompiledSQL("UPDATE node SET prev_sibling_id=:prev_sibling_id "
                            "WHERE node.id = :node_id",
                            lambda ctx: {'prev_sibling_id': about.id,
                                         'node_id': stories.id}),

                CompiledSQL("UPDATE node SET next_sibling_id=:next_sibling_id "
                            "WHERE node.id = :node_id",
                            lambda ctx: {'next_sibling_id': stories.id,
                                         'node_id': about.id}),

                CompiledSQL("UPDATE node SET next_sibling_id=:next_sibling_id "
                            "WHERE node.id = :node_id",
                            lambda ctx: {'next_sibling_id': None,
                                         'node_id': cats.id}),
            ),

            CompiledSQL("DELETE FROM node WHERE node.id = :id",
                        lambda ctx: [{'id': cats.id}])
        )

        session.delete(root)

        self.assert_sql_execution(
            testing.db,
            session.flush,
            CompiledSQL("UPDATE node SET next_sibling_id=:next_sibling_id "
                        "WHERE node.id = :node_id",
                        lambda ctx: [
                            {'node_id': about.id, 'next_sibling_id': None},
                            {'node_id': stories.id, 'next_sibling_id': None}
                        ]
                        ),
            AllOf(
                CompiledSQL("DELETE FROM node WHERE node.id = :id",
                            lambda ctx: {'id': about.id}
                            ),
                CompiledSQL("DELETE FROM node WHERE node.id = :id",
                            lambda ctx: {'id': stories.id}
                            ),
                CompiledSQL("DELETE FROM node WHERE node.id = :id",
                            lambda ctx: {'id': bruce.id}
                            ),
            ),
            CompiledSQL("DELETE FROM node WHERE node.id = :id",
                        lambda ctx: {'id': root.id}
                        ),
        )
        about = Node('about')
        cats = Node('cats')
        about.next_sibling = cats
        cats.prev_sibling = about
        session.add(about)
        session.flush()
        session.delete(about)
        cats.prev_sibling = None
        session.flush()


class SelfReferentialPostUpdateTest2(fixtures.MappedTest):

    @classmethod
    def define_tables(cls, metadata):
        Table("a_table", metadata,
              Column("id", Integer(), primary_key=True,
                     test_needs_autoincrement=True),
              Column("fui", String(128)),
              Column("b", Integer(), ForeignKey("a_table.id")))

    @classmethod
    def setup_classes(cls):
        class A(cls.Basic):
            pass

    def test_one(self):
        """
        Test that post_update remembers to be involved in update operations as
        well, since it replaces the normal dependency processing completely
        [ticket:413]

        """

        A, a_table = self.classes.A, self.tables.a_table

        mapper(A, a_table, properties={
            'foo': relationship(A,
                                remote_side=[a_table.c.id],
                                post_update=True)})

        session = create_session()

        f1 = A(fui="f1")
        session.add(f1)
        session.flush()

        f2 = A(fui="f2", foo=f1)

        # at this point f1 is already inserted.  but we need post_update
        # to fire off anyway
        session.add(f2)
        session.flush()
        session.expunge_all()

        f1 = session.query(A).get(f1.id)
        f2 = session.query(A).get(f2.id)
        assert f2.foo is f1


class SelfReferentialPostUpdateTest3(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table('parent', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('name', String(50), nullable=False),
              Column('child_id', Integer,
                     ForeignKey('child.id', name='c1'), nullable=True))

        Table('child', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('name', String(50), nullable=False),
              Column('child_id', Integer,
                     ForeignKey('child.id')),
              Column('parent_id', Integer,
                     ForeignKey('parent.id'), nullable=True))

    @classmethod
    def setup_classes(cls):
        class Parent(cls.Basic):
            def __init__(self, name=''):
                self.name = name

        class Child(cls.Basic):
            def __init__(self, name=''):
                self.name = name

    def test_one(self):
        Child, Parent, parent, child = (self.classes.Child,
                                        self.classes.Parent,
                                        self.tables.parent,
                                        self.tables.child)

        mapper(Parent, parent, properties={
            'children': relationship(
                Child,
                primaryjoin=parent.c.id == child.c.parent_id),
            'child': relationship(
                Child,
                primaryjoin=parent.c.child_id == child.c.id, post_update=True)
        })
        mapper(Child, child, properties={
            'parent': relationship(Child, remote_side=child.c.id)
        })

        session = create_session()
        p1 = Parent('p1')
        c1 = Child('c1')
        c2 = Child('c2')
        p1.children = [c1, c2]
        c2.parent = c1
        p1.child = c2

        session.add_all([p1, c1, c2])
        session.flush()

        p2 = Parent('p2')
        c3 = Child('c3')
        p2.children = [c3]
        p2.child = c3
        session.add(p2)

        session.delete(c2)
        p1.children.remove(c2)
        p1.child = None
        session.flush()

        p2.child = None
        session.flush()


class PostUpdateBatchingTest(fixtures.MappedTest):
    """test that lots of post update cols batch together into a single UPDATE.
    """

    @classmethod
    def define_tables(cls, metadata):
        Table('parent', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('name', String(50), nullable=False),
              Column('c1_id', Integer,
                     ForeignKey('child1.id', name='c1'), nullable=True),
              Column('c2_id', Integer,
                     ForeignKey('child2.id', name='c2'), nullable=True),
              Column('c3_id', Integer,
                     ForeignKey('child3.id', name='c3'), nullable=True)
              )

        Table('child1', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('name', String(50), nullable=False),
              Column('parent_id', Integer,
                     ForeignKey('parent.id'), nullable=False))

        Table('child2', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('name', String(50), nullable=False),
              Column('parent_id', Integer,
                     ForeignKey('parent.id'), nullable=False))

        Table('child3', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('name', String(50), nullable=False),
              Column('parent_id', Integer,
                     ForeignKey('parent.id'), nullable=False))

    @classmethod
    def setup_classes(cls):
        class Parent(cls.Basic):
            def __init__(self, name=''):
                self.name = name

        class Child1(cls.Basic):
            def __init__(self, name=''):
                self.name = name

        class Child2(cls.Basic):
            def __init__(self, name=''):
                self.name = name

        class Child3(cls.Basic):
            def __init__(self, name=''):
                self.name = name

    def test_one(self):
        child1, child2, child3, Parent, parent, Child1, Child2, Child3 = (
            self.tables.child1,
            self.tables.child2,
            self.tables.child3,
            self.classes.Parent,
            self.tables.parent,
            self.classes.Child1,
            self.classes.Child2,
            self.classes.Child3)

        mapper(Parent, parent, properties={
            'c1s': relationship(
                Child1,
                primaryjoin=child1.c.parent_id == parent.c.id),
            'c2s': relationship(
                Child2,
                primaryjoin=child2.c.parent_id == parent.c.id),
            'c3s': relationship(
                Child3, primaryjoin=child3.c.parent_id == parent.c.id),

            'c1': relationship(
                Child1,
                primaryjoin=child1.c.id == parent.c.c1_id, post_update=True),
            'c2': relationship(
                Child2,
                primaryjoin=child2.c.id == parent.c.c2_id, post_update=True),
            'c3': relationship(
                Child3,
                primaryjoin=child3.c.id == parent.c.c3_id, post_update=True),
        })
        mapper(Child1, child1)
        mapper(Child2, child2)
        mapper(Child3, child3)

        sess = create_session()

        p1 = Parent('p1')
        c11, c12, c13 = Child1('c1'), Child1('c2'), Child1('c3')
        c21, c22, c23 = Child2('c1'), Child2('c2'), Child2('c3')
        c31, c32, c33 = Child3('c1'), Child3('c2'), Child3('c3')

        p1.c1s = [c11, c12, c13]
        p1.c2s = [c21, c22, c23]
        p1.c3s = [c31, c32, c33]
        sess.add(p1)
        sess.flush()

        p1.c1 = c12
        p1.c2 = c23
        p1.c3 = c31

        self.assert_sql_execution(
            testing.db,
            sess.flush,
            CompiledSQL(
                "UPDATE parent SET c1_id=:c1_id, c2_id=:c2_id, c3_id=:c3_id "
                "WHERE parent.id = :parent_id",
                lambda ctx: {'c2_id': c23.id, 'parent_id': p1.id,
                             'c1_id': c12.id, 'c3_id': c31.id}
            )
        )

        p1.c1 = p1.c2 = p1.c3 = None

        self.assert_sql_execution(
            testing.db,
            sess.flush,
            CompiledSQL(
                "UPDATE parent SET c1_id=:c1_id, c2_id=:c2_id, c3_id=:c3_id "
                "WHERE parent.id = :parent_id",
                lambda ctx: {'c2_id': None, 'parent_id': p1.id,
                             'c1_id': None, 'c3_id': None}
            )
        )
