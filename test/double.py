from sqlalchemy import *
import testbase
import string

class Place(object):
    '''represents a place'''
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
        
class DoubleTest(testbase.AssertMixin):
    def setUpAll(self):
        db = testbase.db
        global place
        place = Table('place', db,
            Column('place_id', Integer, primary_key=True),
            Column('name', String(30), nullable=False),
            )

        global transition
        transition = Table('transition', db,
            Column('transition_id', Integer, primary_key=True),
            Column('name', String(30), nullable=False),
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

    def tearDownAll(self):
        place_input.drop()
        place_output.drop()
        place.drop()
        transition.drop()

    def setUp(self):
        objectstore.clear()
        clear_mappers()

    def testdouble(self):
        """tests that a mapper can have two eager relations to the same table, via
        two different association tables.  aliases are required."""

        Place.mapper = mapper(Place, place)
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

if __name__ == "__main__":    
    testbase.main()

