from test.lib.testing import assert_raises, \
    assert_raises_message, eq_
import sqlalchemy as sa
from test.lib import testing
from sqlalchemy import Integer, String, ForeignKey
from test.lib.schema import Table
from test.lib.schema import Column
from sqlalchemy.orm import mapper, relationship, create_session, \
    exc as orm_exc, sessionmaker
from test.lib import fixtures


class M2MTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table('place', metadata,
            Column('place_id', Integer, sa.Sequence('pid_seq', optional=True),
                   primary_key=True),
            Column('name', String(30), nullable=False),
            test_needs_acid=True,
            )

        Table('transition', metadata,
            Column('transition_id', Integer,
                   sa.Sequence('tid_seq', optional=True), primary_key=True),
            Column('name', String(30), nullable=False),
            test_needs_acid=True,
            )

        Table('place_thingy', metadata,
            Column('thingy_id', Integer, sa.Sequence('thid_seq', optional=True),
                   primary_key=True),
            Column('place_id', Integer, ForeignKey('place.place_id'),
                   nullable=False),
            Column('name', String(30), nullable=False),
            test_needs_acid=True,
            )

        # association table #1
        Table('place_input', metadata,
            Column('place_id', Integer, ForeignKey('place.place_id')),
            Column('transition_id', Integer,
                   ForeignKey('transition.transition_id')),
                   test_needs_acid=True,
                   )

        # association table #2
        Table('place_output', metadata,
            Column('place_id', Integer, ForeignKey('place.place_id')),
            Column('transition_id', Integer,
                   ForeignKey('transition.transition_id')),
                   test_needs_acid=True,
                   )

        Table('place_place', metadata,
              Column('pl1_id', Integer, ForeignKey('place.place_id')),
              Column('pl2_id', Integer, ForeignKey('place.place_id')),
              test_needs_acid=True,
              )

    @classmethod
    def setup_classes(cls):
        class Place(cls.Basic):
            def __init__(self, name=None):
                self.name = name
            def __str__(self):
                return "(Place '%s')" % self.name
            __repr__ = __str__

        class PlaceThingy(cls.Basic):
            def __init__(self, name=None):
                self.name = name

        class Transition(cls.Basic):
            def __init__(self, name=None):
                self.name = name
                self.inputs = []
                self.outputs = []
            def __repr__(self):
                return ' '.join((object.__repr__(self),
                                 repr(self.inputs),
                                 repr(self.outputs)))

    def test_error(self):
        place, Transition, place_input, Place, transition = (self.tables.place,
                                self.classes.Transition,
                                self.tables.place_input,
                                self.classes.Place,
                                self.tables.transition)

        mapper(Place, place, properties={
            'transitions':relationship(Transition, secondary=place_input, backref='places')
        })
        mapper(Transition, transition, properties={
            'places':relationship(Place, secondary=place_input, backref='transitions')
        })
        assert_raises_message(sa.exc.ArgumentError, "Error creating backref",
                                 sa.orm.configure_mappers)

    def test_circular(self):
        """test a many-to-many relationship from a table to itself."""

        place, Place, place_place = (self.tables.place,
                                self.classes.Place,
                                self.tables.place_place)

        mapper(Place, place, properties={
            'places': relationship(
                        Place,
                        secondary=place_place, 
                        primaryjoin=place.c.place_id==place_place.c.pl1_id,
                        secondaryjoin=place.c.place_id==place_place.c.pl2_id,
                        order_by=place_place.c.pl2_id
                )
        })

        sess = create_session()
        p1 = Place('place1')
        p2 = Place('place2')
        p3 = Place('place3')
        p4 = Place('place4')
        p5 = Place('place5')
        p6 = Place('place6')
        p7 = Place('place7')
        sess.add_all((p1, p2, p3, p4, p5, p6, p7))
        p1.places.append(p2)
        p1.places.append(p3)
        p5.places.append(p6)
        p6.places.append(p1)
        p7.places.append(p1)
        p1.places.append(p5)
        p4.places.append(p3)
        p3.places.append(p4)
        sess.flush()

        sess.expunge_all()
        l = sess.query(Place).order_by(place.c.place_id).all()
        (p1, p2, p3, p4, p5, p6, p7) = l
        assert p1.places == [p2,p3,p5]
        assert p5.places == [p6]
        assert p7.places == [p1]
        assert p6.places == [p1]
        assert p4.places == [p3]
        assert p3.places == [p4]
        assert p2.places == []

        for p in l:
            pp = p.places
            print "Place " + str(p) +" places " + repr(pp)

        [sess.delete(p) for p in p1,p2,p3,p4,p5,p6,p7]
        sess.flush()

    def test_circular_mutation(self):
        """Test that a mutation in a self-ref m2m of both sides succeeds."""


        place, Place, place_place = (self.tables.place,
                                self.classes.Place,
                                self.tables.place_place)

        mapper(Place, place, properties={
            'child_places': relationship(
                        Place,
                        secondary=place_place, 
                        primaryjoin=place.c.place_id==place_place.c.pl1_id,
                        secondaryjoin=place.c.place_id==place_place.c.pl2_id,
                        order_by=place_place.c.pl2_id,
                        backref='parent_places'
                )
        })

        sess = create_session()
        p1 = Place('place1')
        p2 = Place('place2')
        p2.parent_places = [p1]
        sess.add_all([p1, p2])
        p1.parent_places.append(p2)
        sess.flush()

        sess.expire_all()
        assert p1 in p2.parent_places
        assert p2 in p1.parent_places


    def test_double(self):
        """test that a mapper can have two eager relationships to the same table, via
        two different association tables.  aliases are required."""

        place_input, transition, Transition, PlaceThingy, place, place_thingy, Place, place_output = (self.tables.place_input,
                                self.tables.transition,
                                self.classes.Transition,
                                self.classes.PlaceThingy,
                                self.tables.place,
                                self.tables.place_thingy,
                                self.classes.Place,
                                self.tables.place_output)


        Place.mapper = mapper(Place, place, properties = {
            'thingies':relationship(mapper(PlaceThingy, place_thingy), lazy='joined')
        })

        Transition.mapper = mapper(Transition, transition, properties = dict(
            inputs = relationship(Place.mapper, place_output, lazy='joined'),
            outputs = relationship(Place.mapper, place_input, lazy='joined'),
            )
        )

        tran = Transition('transition1')
        tran.inputs.append(Place('place1'))
        tran.outputs.append(Place('place2'))
        tran.outputs.append(Place('place3'))
        sess = create_session()
        sess.add(tran)
        sess.flush()

        sess.expunge_all()
        r = sess.query(Transition).all()
        self.assert_unordered_result(r, Transition,
            {'name': 'transition1',
            'inputs': (Place, [{'name':'place1'}]),
            'outputs': (Place, [{'name':'place2'}, {'name':'place3'}])
            })

    def test_bidirectional(self):
        place_input, transition, Transition, Place, place, place_output = (self.tables.place_input,
                                self.tables.transition,
                                self.classes.Transition,
                                self.classes.Place,
                                self.tables.place,
                                self.tables.place_output)

        Place.mapper = mapper(Place, place)
        Transition.mapper = mapper(Transition, transition, properties = dict(
            inputs = relationship(Place.mapper, place_output, lazy='select', backref='inputs'),
            outputs = relationship(Place.mapper, place_input, lazy='select', backref='outputs'),
            )
        )

        t1 = Transition('transition1')
        t2 = Transition('transition2')
        t3 = Transition('transition3')
        p1 = Place('place1')
        p2 = Place('place2')
        p3 = Place('place3')

        t1.inputs.append(p1)
        t1.inputs.append(p2)
        t1.outputs.append(p3)
        t2.inputs.append(p1)
        p2.inputs.append(t2)
        p3.inputs.append(t2)
        p1.outputs.append(t1)
        sess = create_session()
        sess.add_all((t1, t2, t3,p1, p2, p3))
        sess.flush()

        self.assert_result([t1], Transition, {'outputs': (Place, [{'name':'place3'}, {'name':'place1'}])})
        self.assert_result([p2], Place, {'inputs': (Transition, [{'name':'transition1'},{'name':'transition2'}])})

    @testing.requires.sane_multi_rowcount
    def test_stale_conditions(self):
        Place, Transition, place_input, place, transition = (self.classes.Place,
                                self.classes.Transition,
                                self.tables.place_input,
                                self.tables.place,
                                self.tables.transition)

        mapper(Place, place, properties={
            'transitions':relationship(Transition, secondary=place_input, 
                                            passive_updates=False)
        })
        mapper(Transition, transition)

        p1 = Place('place1')
        t1 = Transition('t1')
        p1.transitions.append(t1)
        sess = sessionmaker()()
        sess.add_all([p1, t1])
        sess.commit()

        p1.place_id
        p1.transitions

        sess.execute("delete from place_input", mapper=Place)
        p1.place_id = 7

        assert_raises_message(
            orm_exc.StaleDataError,
            r"UPDATE statement on table 'place_input' expected to "
            r"update 1 row\(s\); Only 0 were matched.",
            sess.commit
        )
        sess.rollback()

        p1.place_id
        p1.transitions
        sess.execute("delete from place_input", mapper=Place)
        p1.transitions.remove(t1)
        assert_raises_message(
            orm_exc.StaleDataError,
            r"DELETE statement on table 'place_input' expected to "
            r"delete 1 row\(s\); Only 0 were matched.",
            sess.commit
        )

class M2MTest2(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table('student', metadata,
              Column('name', String(20), primary_key=True))

        Table('course', metadata,
              Column('name', String(20), primary_key=True))

        Table('enroll', metadata,
              Column('student_id', String(20), ForeignKey('student.name'),
                     primary_key=True),
            Column('course_id', String(20), ForeignKey('course.name'),
                   primary_key=True))

    @classmethod
    def setup_classes(cls):
        class Student(cls.Basic):
            def __init__(self, name=''):
                self.name = name
        class Course(cls.Basic):
            def __init__(self, name=''):
                self.name = name

    def test_circular(self):
        course, enroll, Student, student, Course = (self.tables.course,
                                self.tables.enroll,
                                self.classes.Student,
                                self.tables.student,
                                self.classes.Course)


        mapper(Student, student)
        mapper(Course, course, properties={
            'students': relationship(Student, enroll, backref='courses')})

        sess = create_session()
        s1 = Student('Student1')
        c1 = Course('Course1')
        c2 = Course('Course2')
        c3 = Course('Course3')
        s1.courses.append(c1)
        s1.courses.append(c2)
        c3.students.append(s1)
        self.assert_(len(s1.courses) == 3)
        self.assert_(len(c1.students) == 1)
        sess.add(s1)
        sess.flush()
        sess.expunge_all()
        s = sess.query(Student).filter_by(name='Student1').one()
        c = sess.query(Course).filter_by(name='Course3').one()
        self.assert_(len(s.courses) == 3)
        del s.courses[1]
        self.assert_(len(s.courses) == 2)

    def test_dupliates_raise(self):
        """test constraint error is raised for dupe entries in a list"""

        course, enroll, Student, student, Course = (self.tables.course,
                                self.tables.enroll,
                                self.classes.Student,
                                self.tables.student,
                                self.classes.Course)


        mapper(Student, student)
        mapper(Course, course, properties={
            'students': relationship(Student, enroll, backref='courses')})

        sess = create_session()
        s1 = Student("s1")
        c1 = Course('c1')
        s1.courses.append(c1)
        s1.courses.append(c1)
        sess.add(s1)
        assert_raises(sa.exc.DBAPIError, sess.flush)

    def test_delete(self):
        """A many-to-many table gets cleared out with deletion from the backref side"""

        course, enroll, Student, student, Course = (self.tables.course,
                                self.tables.enroll,
                                self.classes.Student,
                                self.tables.student,
                                self.classes.Course)


        mapper(Student, student)
        mapper(Course, course, properties = {
            'students': relationship(Student, enroll, lazy='select',
                                 backref='courses')})

        sess = create_session()
        s1 = Student('Student1')
        c1 = Course('Course1')
        c2 = Course('Course2')
        c3 = Course('Course3')
        s1.courses.append(c1)
        s1.courses.append(c2)
        c3.students.append(s1)
        sess.add(s1)
        sess.flush()
        sess.delete(s1)
        sess.flush()
        assert enroll.count().scalar() == 0

class M2MTest3(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table('c', metadata,
            Column('c1', Integer, primary_key = True),
            Column('c2', String(20)))

        Table('a', metadata,
            Column('a1', Integer, primary_key=True),
            Column('a2', String(20)),
            Column('c1', Integer, ForeignKey('c.c1')))

        Table('c2a1', metadata,
            Column('c1', Integer, ForeignKey('c.c1')),
            Column('a1', Integer, ForeignKey('a.a1')))

        Table('c2a2', metadata,
            Column('c1', Integer, ForeignKey('c.c1')),
            Column('a1', Integer, ForeignKey('a.a1')))

        Table('b', metadata,
            Column('b1', Integer, primary_key=True),
            Column('a1', Integer, ForeignKey('a.a1')),
            Column('b2', sa.Boolean))

    def test_basic(self):
        a, c, b, c2a1, c2a2 = (self.tables.a,
                                self.tables.c,
                                self.tables.b,
                                self.tables.c2a1,
                                self.tables.c2a2)

        class C(object):pass
        class A(object):pass
        class B(object):pass

        mapper(B, b)

        mapper(A, a, properties={
            'tbs': relationship(B, primaryjoin=sa.and_(b.c.a1 == a.c.a1,
                                                   b.c.b2 == True),
                            lazy='joined')})

        mapper(C, c, properties={
            'a1s': relationship(A, secondary=c2a1, lazy='joined'),
            'a2s': relationship(A, secondary=c2a2, lazy='joined')})

        assert create_session().query(C).with_labels().statement is not None

        # TODO: seems like just a test for an ancient exception throw.
        # how about some data/inserts/queries/assertions for this one

class M2MTest4(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        table1 = Table("table1", metadata,
            Column('col1', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('col2', String(30))
            )

        table2 = Table("table2", metadata,
            Column('col1', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('col2', String(30)),
            )

        table3 = Table('table3', metadata,
            Column('t1', Integer, ForeignKey('table1.col1')),
            Column('t2', Integer, ForeignKey('table2.col1')),
            )

    def test_delete_parent(self):
        table2, table3, table1 = (self.tables.table2,
                                self.tables.table3,
                                self.tables.table1)

        class A(fixtures.ComparableEntity):
            pass
        class B(fixtures.ComparableEntity):
            pass

        mapper(A, table1, properties={
            'bs':relationship(B, secondary=table3, backref='as', order_by=table3.c.t1)
        })
        mapper(B, table2)

        sess = create_session()
        a1 = A(col2='a1')
        a2 = A(col2='a2')
        b1 = B(col2='b1')
        b2 = B(col2='b2')
        a1.bs.append(b1)
        a2.bs.append(b2)
        for x in [a1,a2]:
            sess.add(x)
        sess.flush()
        sess.expunge_all()

        alist = sess.query(A).order_by(A.col1).all()
        eq_(
            [
                A(bs=[B(col2='b1')]), A(bs=[B(col2='b2')])
            ],
            alist)

        for a in alist:
            sess.delete(a)
        sess.flush()
        eq_(sess.query(table3).count(), 0)


