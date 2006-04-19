from sqlalchemy import *
import testbase
import string
import sqlalchemy.attributes as attr

class Place(object):
    '''represents a place'''
    def __init__(self, name=None):
        self.name = name
    def __str__(self):
        return "(Place '%s')" % self.name
    def __repr__(self):
        return str(self)

class PlaceThingy(object):
    '''represents a thingy attached to a Place'''
    def __init__(self, name=None):
        self.name = name
    
class Transition(object):
    '''represents a transition'''
    def __init__(self, name=None):
        self.name = name
        self.inputs = []
        self.outputs = []
    def __repr__(self):
        return object.__repr__(self)+ " " + repr(self.inputs) + " " + repr(self.outputs)
        
class M2MTest(testbase.AssertMixin):
    def setUpAll(self):
        db = testbase.db
        global place
        place = Table('place', db,
            Column('place_id', Integer, Sequence('pid_seq', optional=True), primary_key=True),
            Column('name', String(30), nullable=False),
            )

        global transition
        transition = Table('transition', db,
            Column('transition_id', Integer, Sequence('tid_seq', optional=True), primary_key=True),
            Column('name', String(30), nullable=False),
            )

        global place_thingy
        place_thingy = Table('place_thingy', db,
            Column('thingy_id', Integer, Sequence('thid_seq', optional=True), primary_key=True),
            Column('place_id', Integer, ForeignKey('place.place_id'), nullable=False),
            Column('name', String(30), nullable=False)
            )
            
        # association table #1
        global place_input
        place_input = Table('place_input', db,
            Column('place_id', Integer, ForeignKey('place.place_id')),
            Column('transition_id', Integer, ForeignKey('transition.transition_id')),
            )

        # association table #2
        global place_output
        place_output = Table('place_output', db,
            Column('place_id', Integer, ForeignKey('place.place_id')),
            Column('transition_id', Integer, ForeignKey('transition.transition_id')),
            )

        global place_place
        place_place = Table('place_place', db,
            Column('pl1_id', Integer, ForeignKey('place.place_id')),
            Column('pl2_id', Integer, ForeignKey('place.place_id')),
            )

        place.create()
        transition.create()
        place_input.create()
        place_output.create()
        place_thingy.create()
        place_place.create()

    def tearDownAll(self):
        place_place.drop()
        place_input.drop()
        place_output.drop()
        place_thingy.drop()
        place.drop()
        transition.drop()
        #testbase.db.tables.clear()

    def setUp(self):
        objectstore.clear()
        clear_mappers()

    def tearDown(self):
        place_place.delete().execute()
        place_input.delete().execute()
        place_output.delete().execute()
        transition.delete().execute()
        place.delete().execute()

    def testcircular(self):
        """tests a many-to-many relationship from a table to itself."""

        Place.mapper = mapper(Place, place)

        Place.mapper.add_property('places', relation(
            Place.mapper, secondary=place_place, primaryjoin=place.c.place_id==place_place.c.pl1_id,
            secondaryjoin=place.c.place_id==place_place.c.pl2_id,
            order_by=place_place.c.pl2_id,
            lazy=True,
            ))

        p1 = Place('place1')
        p2 = Place('place2')
        p3 = Place('place3')
        p4 = Place('place4')
        p5 = Place('place5')
        p6 = Place('place6')
        p7 = Place('place7')

        p1.places.append(p2)
        p1.places.append(p3)
        p5.places.append(p6)
        p6.places.append(p1)
        p7.places.append(p1)
        p1.places.append(p5)
        p4.places.append(p3)
        p3.places.append(p4)
        objectstore.flush()

        objectstore.clear()
        l = Place.mapper.select(order_by=place.c.place_id)
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
            self.echo("Place " + str(p) +" places " + repr(pp))

        objectstore.delete(p1,p2,p3,p4,p5,p6,p7)
        objectstore.flush()

    def testdouble(self):
        """tests that a mapper can have two eager relations to the same table, via
        two different association tables.  aliases are required."""

        Place.mapper = mapper(Place, place, properties = {
            'thingies':relation(mapper(PlaceThingy, place_thingy), lazy=False)
        })
        
        Transition.mapper = mapper(Transition, transition, properties = dict(
            inputs = relation(Place.mapper, place_output, lazy=False, selectalias='op_alias'),
            outputs = relation(Place.mapper, place_input, lazy=False, selectalias='ip_alias'),
            )
        )

        tran = Transition('transition1')
        tran.inputs.append(Place('place1'))
        tran.outputs.append(Place('place2'))
        tran.outputs.append(Place('place3'))
        objectstore.commit()

        objectstore.clear()
        r = Transition.mapper.select()
        self.assert_result(r, Transition, 
            {'name':'transition1', 
            'inputs' : (Place, [{'name':'place1'}]),
            'outputs' : (Place, [{'name':'place2'}, {'name':'place3'}])
            }
            )    

    def testbidirectional(self):
        """tests a bi-directional many-to-many relationship."""
        Place.mapper = mapper(Place, place)
        Transition.mapper = mapper(Transition, transition, properties = dict(
            inputs = relation(Place.mapper, place_output, lazy=True, backref='inputs'),
            outputs = relation(Place.mapper, place_input, lazy=True, backref='outputs'),
            )
        )

        Place.eagermapper = Place.mapper.options(
            eagerload('inputs', selectalias='ip_alias'), 
            eagerload('outputs', selectalias='op_alias')
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
        
        objectstore.commit()
        
        self.assert_result([t1], Transition, {'outputs': (Place, [{'name':'place3'}, {'name':'place1'}])})
        self.assert_result([p2], Place, {'inputs': (Transition, [{'name':'transition1'},{'name':'transition2'}])})

class M2MTest2(testbase.AssertMixin):        
    def setUpAll(self):
        db = testbase.db
        global studentTbl
        studentTbl = Table('student', db, Column('name', String(20), primary_key=True))
        global courseTbl
        courseTbl = Table('course', db, Column('name', String(20), primary_key=True))
        global enrolTbl
        enrolTbl = Table('enrol', db,
            Column('student_id', String(20), ForeignKey('student.name'),primary_key=True),
            Column('course_id', String(20), ForeignKey('course.name'), primary_key=True))

        studentTbl.create()
        courseTbl.create()
        enrolTbl.create()

    def tearDownAll(self):
        enrolTbl.drop()
        studentTbl.drop()
        courseTbl.drop()
        #testbase.db.tables.clear()

    def setUp(self):
        objectstore.clear()
        clear_mappers()

    def tearDown(self):
        enrolTbl.delete().execute()
        courseTbl.delete().execute()
        studentTbl.delete().execute()

    def testcircular(self): 
        class Student(object):
            def __init__(self, name=''):
                self.name = name
        class Course(object):
            def __init__(self, name=''):
                self.name = name
        Student.mapper = mapper(Student, studentTbl)
        Course.mapper = mapper(Course, courseTbl, properties = {
            'students': relation(Student.mapper, enrolTbl, lazy=True, backref='courses')
        })
        s1 = Student('Student1')
        c1 = Course('Course1')
        c2 = Course('Course2')
        c3 = Course('Course3')
        s1.courses.append(c1)
        s1.courses.append(c2)
        c1.students.append(s1)
        c3.students.append(s1)
        self.assert_(len(s1.courses) == 3)
        self.assert_(len(c1.students) == 1)
        objectstore.commit()
        objectstore.clear()
        s = Student.mapper.get_by(name='Student1')
        c = Course.mapper.get_by(name='Course3')
        self.assert_(len(s.courses) == 3)
        del s.courses[1]
        self.assert_(len(s.courses) == 2)
        
class M2MTest3(testbase.AssertMixin):    
	def setUpAll(self):
		e = testbase.db
		global c, c2a1, c2a2, b, a
		c = Table('c', e, 
			Column('c1', Integer, primary_key = True),
			Column('c2', String(20)),
		).create()

		a = Table('a', e, 
			Column('a1', Integer, primary_key=True),
			Column('a2', String(20)),
			Column('c1', Integer, ForeignKey('c.c1'))
			).create()

		c2a1 = Table('ctoaone', e, 
			Column('c1', Integer, ForeignKey('c.c1')),
			Column('a1', Integer, ForeignKey('a.a1'))
		).create()
		c2a2 = Table('ctoatwo', e, 
			Column('c1', Integer, ForeignKey('c.c1')),
			Column('a1', Integer, ForeignKey('a.a1'))
		).create()

		b = Table('b', e, 
			Column('b1', Integer, primary_key=True),
			Column('a1', Integer, ForeignKey('a.a1')),
			Column('b2', Boolean)
		).create()

	def tearDownAll(self):
		b.drop()
		c2a2.drop()
		c2a1.drop()
		a.drop()
		c.drop()
        #testbase.db.tables.clear()

	def testbasic(self):
		class C(object):pass
		class A(object):pass
		class B(object):pass

		assign_mapper(B, b)

		assign_mapper(A, a, 
			properties = {
				'tbs' : relation(B, primaryjoin=and_(b.c.a1==a.c.a1, b.c.b2 == True), lazy=False),
			}
		)

		assign_mapper(C, c, 
			properties = {
				'a1s' : relation(A, secondary=c2a1, lazy=False),
				'a2s' : relation(A, secondary=c2a2, lazy=False)
			}
		)

		o1 = C.get(1)



if __name__ == "__main__":    
    testbase.main()

