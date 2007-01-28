import testbase
from sqlalchemy import *
import sets

# test classes
class Person(object):
    def __init__(self, **kwargs):
        for key, value in kwargs.iteritems():
            setattr(self, key, value)
    def get_name(self):
        try:
            return getattr(self, 'person_name')
        except AttributeError:
            return getattr(self, 'name')
    def __repr__(self):
        return "Ordinary person %s" % self.get_name()
class Engineer(Person):
    def __repr__(self):
        return "Engineer %s, status %s, engineer_name %s, primary_language %s" % (self.get_name(), self.status, self.engineer_name, self.primary_language)
class Manager(Person):
    def __repr__(self):
        return "Manager %s, status %s, manager_name %s" % (self.get_name(), self.status, self.manager_name)
class Company(object):
    def __init__(self, **kwargs):
        for key, value in kwargs.iteritems():
            setattr(self, key, value)
    def __repr__(self):
        return "Company %s" % self.name

class MultipleTableTest(testbase.PersistTest):
    def setUpAll(self, use_person_column=False):
        global companies, people, engineers, managers, metadata
        metadata = BoundMetaData(testbase.db)
        
        # a table to store companies
        companies = Table('companies', metadata, 
           Column('company_id', Integer, Sequence('company_id_seq', optional=True), primary_key=True),
           Column('name', String(50)))

        # we will define an inheritance relationship between the table "people" and "engineers",
        # and a second inheritance relationship between the table "people" and "managers"
        people = Table('people', metadata, 
           Column('person_id', Integer, Sequence('person_id_seq', optional=True), primary_key=True),
           Column('company_id', Integer, ForeignKey('companies.company_id')),
           Column('name', String(50)),
           Column('type', String(30)))

        engineers = Table('engineers', metadata, 
           Column('person_id', Integer, ForeignKey('people.person_id'), primary_key=True),
           Column('status', String(30)),
           Column('engineer_name', String(50)),
           Column('primary_language', String(50)),
          )

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
        for t in metadata.table_iterator(reverse=True):
            t.delete().execute().close()

    def test_f_f_f(self):
        self.do_test(False, False, False)
    def test_f_f_t(self):
        self.do_test(False, False, True)
    def test_f_t_f(self):
        self.do_test(False, True, False)
    def test_f_t_t(self):
        self.do_test(False, True, True)
    def test_t_f_f(self):
        self.do_test(True, False, False)
    def test_t_f_t(self):
        self.do_test(True, False, True)
    def test_t_t_f(self):
        self.do_test(True, True, False)
    def test_t_t_t(self):
        self.do_test(True, True, True)
    def test_f_f_f_t(self):
        self.do_test(False, False, False, True)
    def test_f_f_t_t(self):
        self.do_test(False, False, True, True)
    def test_f_t_f_t(self):
        self.do_test(False, True, False, True)
    def test_f_t_t_t(self):
        self.do_test(False, True, True, True)
    def test_t_f_f_t(self):
        self.do_test(True, False, False, True)
    def test_t_f_t_t(self):
        self.do_test(True, False, True, True)
    def test_t_t_f_t(self):
        self.do_test(True, True, False, True)
    def test_t_t_t_t(self):
        self.do_test(True, True, True, True)
        
    def testcompile(self):
        person_join = polymorphic_union( {
            'engineer':people.join(engineers),
            'manager':people.join(managers),
            'person':people.select(people.c.type=='person'),
            }, None, 'pjoin')

        person_mapper = mapper(Person, people, select_table=person_join, polymorphic_on=person_join.c.type, polymorphic_identity='person')
        mapper(Engineer, engineers, inherits=person_mapper, polymorphic_identity='engineer')
        mapper(Manager, managers, inherits=person_mapper, polymorphic_identity='manager')

        session = create_session()
        session.save(Manager(name='Tom', status='knows how to manage things'))
        session.save(Engineer(name='Kurt', status='knows how to hack'))
        session.flush()
        print session.query(Engineer).select()

        print session.query(Person).select()        
    
    def testcompile2(self):
        """test that a mapper can reference a property whose mapper inherits from this one."""
        person_join = polymorphic_union( {
            'engineer':people.join(engineers),
            'manager':people.join(managers),
            'person':people.select(people.c.type=='person'),
            }, None, 'pjoin')


        person_mapper = mapper(Person, people, select_table=person_join, polymorphic_on=person_join.c.type,
                    polymorphic_identity='person', 
                    properties = dict(managers = relation(Manager, lazy=True))
                )

        mapper(Engineer, engineers, inherits=person_mapper, polymorphic_identity='engineer')
        mapper(Manager, managers, inherits=person_mapper, polymorphic_identity='manager')

        #person_mapper.compile()
        class_mapper(Manager).compile()

    def testcompile3(self):
        """test that a mapper referencing an inheriting mapper in a self-referential relationship does 
        not allow an eager load to be set up."""
        person_join = polymorphic_union( {
            'engineer':people.join(engineers),
            'manager':people.join(managers),
            'person':people.select(people.c.type=='person'),
            }, None, 'pjoin')

        person_mapper = mapper(Person, people, select_table=person_join, polymorphic_on=person_join.c.type,
                    polymorphic_identity='person', 
                    properties = dict(managers = relation(Manager, lazy=False))
                )

        mapper(Engineer, engineers, inherits=person_mapper, polymorphic_identity='engineer')
        mapper(Manager, managers, inherits=person_mapper, polymorphic_identity='manager')

        try:
            class_mapper(Manager).compile()
            assert False
        except exceptions.ArgumentError:
            assert True
        
    def do_test(self, include_base=False, lazy_relation=True, redefine_colprop=False, use_literal_join=False):
        """tests the polymorph.py example, with several options:
        
        include_base - whether or not to include the base 'person' type in the union.
        lazy_relation - whether or not the Company relation to People is lazy or eager.
        redefine_colprop - if we redefine the 'name' column to be 'people_name' on the base Person class
        """
        # create a union that represents both types of joins.  
        if include_base:
            person_join = polymorphic_union(
                {
                    'engineer':people.join(engineers),
                    'manager':people.join(managers),
                    'person':people.select(people.c.type=='person'),
                }, None, 'pjoin')
        else:
            person_join = polymorphic_union(
                {
                    'engineer':people.join(engineers),
                    'manager':people.join(managers),
                }, None, 'pjoin')

        if redefine_colprop:
            person_mapper = mapper(Person, people, select_table=person_join, polymorphic_on=person_join.c.type, polymorphic_identity='person', properties= {'person_name':people.c.name})
        else:
            person_mapper = mapper(Person, people, select_table=person_join, polymorphic_on=person_join.c.type, polymorphic_identity='person')
            
        mapper(Engineer, engineers, inherits=person_mapper, polymorphic_identity='engineer')
        mapper(Manager, managers, inherits=person_mapper, polymorphic_identity='manager')

        if use_literal_join:
            mapper(Company, companies, properties={
                'employees': relation(Person, lazy=lazy_relation, primaryjoin=people.c.company_id==companies.c.company_id, private=True, 
                backref="company"
                )
            })
        else:
            mapper(Company, companies, properties={
                'employees': relation(Person, lazy=lazy_relation, private=True, 
                backref="company"
                )
            })
            
        if redefine_colprop:
            person_attribute_name = 'person_name'
        else:
            person_attribute_name = 'name'
        
        session = create_session()
        c = Company(name='company1')
        c.employees.append(Manager(status='AAB', manager_name='manager1', **{person_attribute_name:'pointy haired boss'}))
        c.employees.append(Engineer(status='BBA', engineer_name='engineer1', primary_language='java', **{person_attribute_name:'dilbert'}))
        if include_base:
            c.employees.append(Person(status='HHH', **{person_attribute_name:'joesmith'}))
        c.employees.append(Engineer(status='CGG', engineer_name='engineer2', primary_language='python', **{person_attribute_name:'wally'}))
        c.employees.append(Manager(status='ABA', manager_name='manager2', **{person_attribute_name:'jsmith'}))
        session.save(c)
        print session.new
        session.flush()
        session.clear()
        id = c.company_id
        c = session.query(Company).get(id)
        for e in c.employees:
            print e, e._instance_key, e.company
        if include_base:
            assert sets.Set([e.get_name() for e in c.employees]) == sets.Set(['pointy haired boss', 'dilbert', 'joesmith', 'wally', 'jsmith'])
        else:
            assert sets.Set([e.get_name() for e in c.employees]) == sets.Set(['pointy haired boss', 'dilbert', 'wally', 'jsmith'])
        print "\n"

        
        # test selecting from the query, using the base mapped table (people) as the selection criterion.
        # in the case of the polymorphic Person query, the "people" selectable should be adapted to be "person_join"
        dilbert = session.query(Person).selectfirst(people.c.name=='dilbert')
        dilbert2 = session.query(Engineer).selectfirst(people.c.name=='dilbert')
        assert dilbert is dilbert2

        # test selecting from the query, joining against an alias of the base "people" table.  test that
        # the "palias" alias does *not* get sucked up into the "person_join" conversion.
        palias = people.alias("palias")
        session.query(Person).selectfirst((palias.c.name=='dilbert') & (palias.c.person_id==people.c.person_id))
        dilbert2 = session.query(Engineer).selectfirst((palias.c.name=='dilbert') & (palias.c.person_id==people.c.person_id))
        assert dilbert is dilbert2

        session.query(Person).selectfirst((engineers.c.engineer_name=="engineer1") & (engineers.c.person_id==people.c.person_id))
        dilbert2 = session.query(Engineer).selectfirst(engineers.c.engineer_name=="engineer1")
        assert dilbert is dilbert2
        
        
        dilbert.engineer_name = 'hes dibert!'

        session.flush()
        session.clear()

        c = session.query(Company).get(id)
        for e in c.employees:
            print e, e._instance_key

        session.delete(c)
        session.flush()

    def test_insert_order(self):
        person_join = polymorphic_union(
            {
                'engineer':people.join(engineers),
                'manager':people.join(managers),
                'person':people.select(people.c.type=='person'),
            }, None, 'pjoin')

        person_mapper = mapper(Person, people, select_table=person_join, polymorphic_on=person_join.c.type, polymorphic_identity='person')

        mapper(Engineer, engineers, inherits=person_mapper, polymorphic_identity='engineer')
        mapper(Manager, managers, inherits=person_mapper, polymorphic_identity='manager')
        mapper(Company, companies, properties={
            'employees': relation(Person, private=True, backref='company', order_by=person_join.c.person_id)
        })

        session = create_session()
        c = Company(name='company1')
        c.employees.append(Manager(status='AAB', manager_name='manager1', name='pointy haired boss'))
        c.employees.append(Engineer(status='BBA', engineer_name='engineer1', primary_language='java', name='dilbert'))
        c.employees.append(Person(status='HHH', name='joesmith'))
        c.employees.append(Engineer(status='CGG', engineer_name='engineer2', primary_language='python', name='wally'))
        c.employees.append(Manager(status='ABA', manager_name='manager2', name='jsmith'))
        session.save(c)
        session.flush()
        session.clear()
        c = session.query(Company).get(c.company_id)
        for e in c.employees:
            print e, e._instance_key, e.company
        
        assert [e.get_name() for e in c.employees] == ['pointy haired boss', 'dilbert', 'joesmith', 'wally', 'jsmith']

if __name__ == "__main__":    
    testbase.main()

