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

if __name__ == "__main__":    
    testbase.main()
        