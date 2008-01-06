"""tests the Query object's ability to work with polymorphic selectables
and inheriting mappers."""

# TODO: under construction !

import testbase
import sets
from sqlalchemy import *
from sqlalchemy.orm import *
from testlib import *
from testlib import fixtures

class Company(fixtures.Base):
    pass
    
class Person(fixtures.Base):
    pass
class Engineer(Person):
    pass
class Manager(Person):
    pass
class Boss(Manager):
    pass

class Paperwork(fixtures.Base):
    pass

class PolymorphicQueryTest(ORMTest):
    keep_data = True
    keep_mappers = True
    
    def define_tables(self, metadata):
        global companies, people, engineers, managers, boss, paperwork
        
        companies = Table('companies', metadata, 
           Column('company_id', Integer, Sequence('company_id_seq', optional=True), primary_key=True),
           Column('name', String(50)))

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

        boss = Table('boss', metadata, 
            Column('boss_id', Integer, ForeignKey('managers.person_id'), primary_key=True),
            Column('golf_swing', String(30)),
            )

        paperwork = Table('paperwork', metadata, 
            Column('paperwork_id', Integer, primary_key=True),
            Column('description', String(50)), 
            Column('person_id', Integer, ForeignKey('people.person_id')))
            
        # create the most awkward polymorphic selects possible; 
        # the union does not include the "people" table by itself nor does it have
        # "people.person_id" directly in it, and it also does not include at all
        # the "boss" table
        person_join = polymorphic_union(
            {
                'engineer':people.join(engineers),
                'manager':people.join(managers),
            }, None, 'pjoin')
            
        # separate join for second-level inherit    
        manager_join = people.join(managers).outerjoin(boss)

        mapper(Company, companies, properties={
            'employees':relation(Person)
        })
        mapper(Person, people, select_table=person_join, polymorphic_on=people.c.type, polymorphic_identity='person', order_by=person_join.c.person_id, 
            properties={
                'paperwork':relation(Paperwork)
            })
        mapper(Engineer, engineers, inherits=Person, polymorphic_identity='engineer')
        mapper(Manager, managers, select_table=manager_join, inherits=Person, polymorphic_identity='manager')
        mapper(Boss, boss, inherits=Manager, polymorphic_identity='boss')
        mapper(Paperwork, paperwork)
        
    def insert_data(self):
        c1 = Company(name="MegaCorp, Inc.")
        c2 = Company(name="Elbonia, Inc.")
        e1 = Engineer(name="dilbert", engineer_name="dilbert", primary_language="java", status="regular engineer", paperwork=[
            Paperwork(description="tps report #1"),
            Paperwork(description="tps report #2")
        ])
        e2 = Engineer(name="wally", engineer_name="wally", primary_language="c++", status="regular engineer", paperwork=[
            Paperwork(description="tps report #3"),
            Paperwork(description="tps report #4")
        ])
        b1 = Boss(name="pointy haired boss", golf_swing="fore", manager_name="pointy", status="da boss", paperwork=[
            Paperwork(description="review #1"),
        ])
        m1 = Manager(name="dogbert", manager_name="dogbert", status="regular manager", paperwork=[
            Paperwork(description="review #2"),
            Paperwork(description="review #3")
        ])
        c1.employees = [e1, e2, b1, m1]
        
        e3 = Engineer(name="vlad", engineer_name="vlad", primary_language="cobol", status="elbonian engineer")
        c2.employees = [e3]
        sess = create_session()
        sess.save(c1)
        sess.save(c2)
        sess.flush()
        sess.clear()
        
        global all_employees, c1_employees, c2_employees
        all_employees = [e1, e2, b1, m1, e3]
        c1_employees = [e1, e2, b1, m1]
        c2_employees = [e3]
    
    def test_load_all(self):
        sess = create_session()
        
        self.assertEquals(sess.query(Person).all(), all_employees)

if __name__ == "__main__":    
    testbase.main()
        
        
        