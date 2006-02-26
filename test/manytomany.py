from sqlalchemy import *
import testbase
import string
import sqlalchemy.attributes as attr

class Place(object):
    '''represents a place'''
    def __init__(self, name=None):
        self.name = name

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

        place.create()
        transition.create()
        place_input.create()
        place_output.create()
        place_thingy.create()

    def tearDownAll(self):
        place_input.drop()
        place_output.drop()
        place_thingy.drop()
        place.drop()
        transition.drop()

    def setUp(self):
        objectstore.clear()
        clear_mappers()

    def tearDown(self):
        place_input.delete().execute()
        place_output.delete().execute()
        transition.delete().execute()
        place.delete().execute()

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

    def testcircular(self):
        """tests a circular many-to-many relationship.  this requires that the mapper
        "break off" a new "mapper stub" to indicate a third depedendent processor."""
        Place.mapper = mapper(Place, place)
        Transition.mapper = mapper(Transition, transition, properties = dict(
            inputs = relation(Place.mapper, place_output, lazy=True, backref='inputs'),
            outputs = relation(Place.mapper, place_input, lazy=True, backref='outputs'),
            )
        )
        #Place.mapper.add_property('inputs', relation(Transition.mapper, place_output, lazy=True, attributeext=attr.ListBackrefExtension('inputs')))
        #Place.mapper.add_property('outputs', relation(Transition.mapper, place_input, lazy=True, attributeext=attr.ListBackrefExtension('outputs')))

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

