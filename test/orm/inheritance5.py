from sqlalchemy import *
import testbase

class AttrSettable(object):
    def __init__(self, **kwargs):
        [setattr(self, k, v) for k, v in kwargs.iteritems()]
    def __repr__(self):
        return self.__class__.__name__ + ' ' + ','.join(["%s=%s" % (k,v) for k, v in self.__dict__.iteritems() if k[0] != '_'])


class RelationTest1(testbase.PersistTest):
    """test self-referential relationships on polymorphic mappers"""
    def setUpAll(self):
        global people, managers, metadata
        metadata = BoundMetaData(testbase.db)

        people = Table('people', metadata, 
           Column('person_id', Integer, Sequence('person_id_seq', optional=True), primary_key=True),
           Column('manager_id', Integer, ForeignKey('managers.person_id', use_alter=True, name="mpid_fq")),
           Column('name', String(50)),
           Column('type', String(30)))

        managers = Table('managers', metadata, 
           Column('person_id', Integer, ForeignKey('people.person_id'), primary_key=True),
           Column('status', String(30)),
           Column('manager_name', String(50))
           )

        metadata.create_all()

    def tearDownAll(self):
        metadata.drop_all()

    def tearDown(self):
        clear_mappers()
        people.update().execute(manager_id=None)
        for t in metadata.table_iterator(reverse=True):
            t.delete().execute()

    def testbasic(self):
        class Person(AttrSettable):
            pass
        class Manager(Person):
            pass

        mapper(Person, people, properties={
            'manager':relation(Manager, primaryjoin=people.c.manager_id==managers.c.person_id, uselist=False)
        })
        mapper(Manager, managers, inherits=Person, inherit_condition=people.c.person_id==managers.c.person_id)
        try:
            compile_mappers()
        except exceptions.ArgumentError, ar:
            assert str(ar) == "Cant determine relation direction for 'manager' on mapper 'Mapper|Person|people' with primary join 'people.manager_id = managers.person_id' - foreign key columns are present in both the parent and the child's mapped tables.  Specify 'foreignkey' argument."

        clear_mappers()

        mapper(Person, people, properties={
            'manager':relation(Manager, primaryjoin=people.c.manager_id==managers.c.person_id, foreignkey=people.c.manager_id, uselist=False, post_update=True)
        })
        mapper(Manager, managers, inherits=Person, inherit_condition=people.c.person_id==managers.c.person_id)

        session = create_session()
        p = Person(name='some person')
        m = Manager(name='some manager')
        p.manager = m
        session.save(p)
        session.flush()
        session.clear()

        p = session.query(Person).get(p.person_id)
        m = session.query(Manager).get(m.person_id)
        print p, m, p.manager
        assert p.manager is m
            
class RelationTest2(testbase.AssertMixin):
    """test self-referential relationships on polymorphic mappers"""
    def setUpAll(self):
        global people, managers, metadata
        metadata = BoundMetaData(testbase.db)

        people = Table('people', metadata, 
           Column('person_id', Integer, Sequence('person_id_seq', optional=True), primary_key=True),
           Column('name', String(50)),
           Column('type', String(30)))

        managers = Table('managers', metadata, 
           Column('person_id', Integer, ForeignKey('people.person_id'), primary_key=True),
           Column('manager_id', Integer, ForeignKey('people.person_id')),
           Column('status', String(30)),
           )

        metadata.create_all()

    def tearDownAll(self):
        metadata.drop_all()

    def tearDown(self):
        clear_mappers()
        for t in metadata.table_iterator(reverse=True):
            t.delete().execute()

    def testrelationonsubclass(self):
        class Person(AttrSettable):
            pass
        class Manager(Person):
            pass

        poly_union = polymorphic_union({
            'person':people.select(people.c.type=='person'),
            'manager':managers.join(people, people.c.person_id==managers.c.person_id)
        }, None)

        mapper(Person, people, select_table=poly_union, polymorphic_identity='person', polymorphic_on=people.c.type)
        mapper(Manager, managers, inherits=Person, inherit_condition=people.c.person_id==managers.c.person_id, polymorphic_identity='manager',
              properties={
                'colleague':relation(Person, primaryjoin=managers.c.manager_id==people.c.person_id, uselist=False)
        })
        class_mapper(Person).compile()
        sess = create_session()
        p = Person(name='person1')
        m = Manager(name='manager1')
        m.colleague = p
        sess.save(m)
        sess.flush()
        
        sess.clear()
        p = sess.query(Person).get(p.person_id)
        m = sess.query(Manager).get(m.person_id)
        print p
        print m
        assert m.colleague is p

class RelationTest3(testbase.AssertMixin):
    """test self-referential relationships on polymorphic mappers"""
    def setUpAll(self):
        global people, managers, metadata
        metadata = BoundMetaData(testbase.db)

        people = Table('people', metadata, 
           Column('person_id', Integer, Sequence('person_id_seq', optional=True), primary_key=True),
           Column('colleague_id', Integer, ForeignKey('people.person_id')),
           Column('name', String(50)),
           Column('type', String(30)))

        managers = Table('managers', metadata, 
           Column('person_id', Integer, ForeignKey('people.person_id'), primary_key=True),
           Column('status', String(30)),
           )

        metadata.create_all()

    def tearDownAll(self):
        metadata.drop_all()

    def tearDown(self):
        clear_mappers()
        for t in metadata.table_iterator(reverse=True):
            t.delete().execute()

    def testrelationonbaseclass(self):
        class Person(AttrSettable):
            pass
        class Manager(Person):
            pass

        poly_union = polymorphic_union({
            'manager':managers.join(people, people.c.person_id==managers.c.person_id),
            'person':people.select(people.c.type=='person')
        }, None)

        mapper(Person, people, select_table=poly_union, polymorphic_identity='person', polymorphic_on=people.c.type,
              properties={
                'colleagues':relation(Person, primaryjoin=people.c.colleague_id==people.c.person_id, 
                    remote_side=people.c.person_id, uselist=True)
                }        
        )
        mapper(Manager, managers, inherits=Person, inherit_condition=people.c.person_id==managers.c.person_id, polymorphic_identity='manager')

        sess = create_session()
        p = Person(name='person1')
        p2 = Person(name='person2')
        m = Manager(name='manager1')
        p.colleagues.append(p2)
        m.colleagues.append(p2)
        sess.save(m)
        sess.save(p)
        sess.flush()
        
        sess.clear()
        p = sess.query(Person).get(p.person_id)
        p2 = sess.query(Person).get(p2.person_id)
        print p, p2, p.colleagues
        assert len(p.colleagues) == 1
        assert p.colleagues == [p2]

class RelationTest4(testbase.AssertMixin):
    def setUpAll(self):
        global metadata, people, engineers, managers, cars
        metadata = BoundMetaData(testbase.db)
        people = Table('people', metadata, 
           Column('person_id', Integer, primary_key=True),
           Column('name', String(50)))

        engineers = Table('engineers', metadata, 
           Column('person_id', Integer, ForeignKey('people.person_id'), primary_key=True),
           Column('status', String(30)))

        managers = Table('managers', metadata, 
           Column('person_id', Integer, ForeignKey('people.person_id'), primary_key=True),
           Column('longer_status', String(70)))

        cars = Table('cars', metadata, 
           Column('car_id', Integer, primary_key=True),
           Column('owner', Integer, ForeignKey('people.person_id')))
        metadata.create_all()
    def tearDownAll(self):
        metadata.drop_all()
    def tearDown(self):
        clear_mappers()
        for t in metadata.table_iterator(reverse=True):
            t.delete().execute()
    
    def testmanytoonepolymorphic(self):
        """in this test, the polymorphic union is between two subclasses, but does not include the base table by itself
         in the union.  however, the primaryjoin condition is going to be against the base table, and its a many-to-one
         relationship (unlike the test in polymorph.py) so the column in the base table is explicit.  Can the ClauseAdapter
         figure out how to alias the primaryjoin to the polymorphic union ?"""
        # class definitions
        class Person(object):
            def __init__(self, **kwargs):
                for key, value in kwargs.iteritems():
                    setattr(self, key, value)
            def __repr__(self):
                return "Ordinary person %s" % self.name
        class Engineer(Person):
            def __repr__(self):
                return "Engineer %s, status %s" % (self.name, self.status)
        class Manager(Person):
            def __repr__(self):
                return "Manager %s, status %s" % (self.name, self.longer_status)
        class Car(object):
            def __init__(self, **kwargs):
                for key, value in kwargs.iteritems():
                    setattr(self, key, value)
            def __repr__(self):
                return "Car number %d" % self.car_id

        # create a union that represents both types of joins.  
        employee_join = polymorphic_union(
            {
                'engineer':people.join(engineers),
                'manager':people.join(managers),
            }, "type", 'employee_join')
            
        person_mapper   = mapper(Person, people, select_table=employee_join,polymorphic_on=employee_join.c.type, polymorphic_identity='person')
        engineer_mapper = mapper(Engineer, engineers, inherits=person_mapper, polymorphic_identity='engineer')
        manager_mapper  = mapper(Manager, managers, inherits=person_mapper, polymorphic_identity='manager')
        car_mapper      = mapper(Car, cars, properties= {'employee':relation(person_mapper)})
        
        # so the primaryjoin is "people.c.person_id==cars.c.owner".  the "lazy" clause will be
        # "people.c.person_id=?".  the employee_join is two selects union'ed together, one of which 
        # will contain employee.c.person_id the other contains manager.c.person_id.  people.c.person_id is not explicitly in 
        # either column clause in this case.  we can modify polymorphic_union to always put the "base" column in which would fix this,
        # but im not sure if that really fixes the issue in all cases and its too far from the problem.
        # instead, when the primaryjoin is adapted to point to the polymorphic union and is targeting employee_join.c.person_id, 
        # it has to use not just straight column correspondence but also "keys_ok=True", meaning it will link up to any column 
        # with the name "person_id", as opposed to columns that descend directly from people.c.person_id.  polymorphic unions
        # require the cols all match up on column name which then determine the top selectable names, so matching by name is OK.

        session = create_session()

        # creating 5 managers named from M1 to E5
        for i in range(1,5):
            session.save(Manager(name="M%d" % i,longer_status="YYYYYYYYY"))
        # creating 5 engineers named from E1 to E5
        for i in range(1,5):
            session.save(Engineer(name="E%d" % i,status="X"))

        session.flush()

        engineer4 = session.query(Engineer).selectfirst_by(name="E4")

        car1 = Car(owner=engineer4.person_id)
        session.save(car1)
        session.flush()

        session.clear()
        
        car1 = session.query(Car).get(car1.car_id)
        usingGet = session.query(person_mapper).get(car1.owner)
        usingProperty = car1.employee

        # All print should output the same person (engineer E4)
        assert str(engineer4) == "Engineer E4, status X"
        assert str(usingGet) == "Engineer E4, status X"
        assert str(usingProperty) == "Engineer E4, status X"

        session.clear()
        
        # and now for the lightning round, eager !
        car1 = session.query(Car).options(eagerload('employee')).get(car1.car_id)
        assert str(car1.employee) == "Engineer E4, status X"

class RelationTest5(testbase.AssertMixin):
    def setUpAll(self):
        global metadata, people, engineers, managers, cars
        metadata = BoundMetaData(testbase.db)
        people = Table('people', metadata, 
           Column('person_id', Integer, primary_key=True),
           Column('name', String(50)),
           Column('type', String(50)))

        engineers = Table('engineers', metadata, 
           Column('person_id', Integer, ForeignKey('people.person_id'), primary_key=True),
           Column('status', String(30)))

        managers = Table('managers', metadata, 
           Column('person_id', Integer, ForeignKey('people.person_id'), primary_key=True),
           Column('longer_status', String(70)))

        cars = Table('cars', metadata, 
           Column('car_id', Integer, primary_key=True),
           Column('owner', Integer, ForeignKey('people.person_id')))
        metadata.create_all()
    def tearDownAll(self):
        metadata.drop_all()
    def tearDown(self):
        clear_mappers()
        for t in metadata.table_iterator(reverse=True):
            t.delete().execute()
    
    def testeagerempty(self):
        """an easy one...test parent object with child relation to an inheriting mapper, using eager loads,
        works when there are no child objects present"""
        class Person(object):
            def __init__(self, **kwargs):
                for key, value in kwargs.iteritems():
                    setattr(self, key, value)
            def __repr__(self):
                return "Ordinary person %s" % self.name
        class Engineer(Person):
            def __repr__(self):
                return "Engineer %s, status %s" % (self.name, self.status)
        class Manager(Person):
            def __repr__(self):
                return "Manager %s, status %s" % (self.name, self.longer_status)
        class Car(object):
            def __init__(self, **kwargs):
                for key, value in kwargs.iteritems():
                    setattr(self, key, value)
            def __repr__(self):
                return "Car number %d" % self.car_id

        person_mapper   = mapper(Person, people, polymorphic_on=people.c.type, polymorphic_identity='person')
        engineer_mapper = mapper(Engineer, engineers, inherits=person_mapper, polymorphic_identity='engineer')
        manager_mapper  = mapper(Manager, managers, inherits=person_mapper, polymorphic_identity='manager')
        car_mapper      = mapper(Car, cars, properties= {'manager':relation(manager_mapper, lazy=False)})

        sess = create_session()
        car1 = Car()
        car2 = Car()
        car2.manager = Manager()
        sess.save(car1)
        sess.save(car2)
        sess.flush()
        sess.clear()
        
        carlist = sess.query(Car).select()
        assert carlist[0].manager is None
        assert carlist[1].manager.person_id == car2.manager.person_id
        
if __name__ == "__main__":    
    testbase.main()
        