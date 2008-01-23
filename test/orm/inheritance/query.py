"""tests the Query object's ability to work with polymorphic selectables
and inheriting mappers."""

# TODO: under construction !

import testenv; testenv.configure_for_tests()
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

class Machine(fixtures.Base):
    pass
    
class Paperwork(fixtures.Base):
    pass

def make_test(select_type):
    class PolymorphicQueryTest(ORMTest):
        keep_data = True
        keep_mappers = True

        def define_tables(self, metadata):
            global companies, people, engineers, managers, boss, paperwork, machines

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
         
            machines = Table('machines', metadata,
                Column('machine_id', Integer, primary_key=True),
                Column('name', String(50)),
                Column('engineer_id', Integer, ForeignKey('engineers.person_id')))
            
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

            clear_mappers()
            
            mapper(Company, companies, properties={
                'employees':relation(Person)
            })

            mapper(Machine, machines)

            if select_type == '':
                person_join = manager_join = None
            elif select_type == 'Unions':
                person_join = polymorphic_union(
                    {
                        'engineer':people.join(engineers),
                        'manager':people.join(managers),
                    }, None, 'pjoin')

                manager_join = people.join(managers).outerjoin(boss)
            elif select_type == 'AliasedJoins':
                person_join = people.outerjoin(engineers).outerjoin(managers).select(use_labels=True).alias('pjoin')
                manager_join = people.join(managers).outerjoin(boss).select(use_labels=True).alias('mjoin')
            elif select_type == 'Joins':
                person_join = people.outerjoin(engineers).outerjoin(managers)
                manager_join = people.join(managers).outerjoin(boss)


            # testing a order_by here as well; the surrogate mapper has to adapt it
            mapper(Person, people, 
                select_table=person_join, 
                polymorphic_on=people.c.type, polymorphic_identity='person', order_by=people.c.person_id, 
                properties={
                    'paperwork':relation(Paperwork)
                })
            mapper(Engineer, engineers, inherits=Person, polymorphic_identity='engineer', properties={
                    'machines':relation(Machine)
                })
            mapper(Manager, managers, select_table=manager_join, inherits=Person, polymorphic_identity='manager')
            mapper(Boss, boss, inherits=Manager, polymorphic_identity='boss')
            mapper(Paperwork, paperwork)
        

        def insert_data(self):
            global all_employees, c1_employees, c2_employees, e1, e2, b1, m1, e3, c1, c2

            c1 = Company(name="MegaCorp, Inc.")
            c2 = Company(name="Elbonia, Inc.")
            e1 = Engineer(name="dilbert", engineer_name="dilbert", primary_language="java", status="regular engineer", paperwork=[
                Paperwork(description="tps report #1"),
                Paperwork(description="tps report #2")
            ], machines=[
                Machine(name='IBM ThinkPad'),
                Machine(name='IPhone'),
            ])
            e2 = Engineer(name="wally", engineer_name="wally", primary_language="c++", status="regular engineer", paperwork=[
                Paperwork(description="tps report #3"),
                Paperwork(description="tps report #4")
            ], machines=[
                Machine(name="Commodore 64")
            ])
            b1 = Boss(name="pointy haired boss", golf_swing="fore", manager_name="pointy", status="da boss", paperwork=[
                Paperwork(description="review #1"),
            ])
            m1 = Manager(name="dogbert", manager_name="dogbert", status="regular manager", paperwork=[
                Paperwork(description="review #2"),
                Paperwork(description="review #3")
            ])
            c1.employees = [e1, e2, b1, m1]

            e3 = Engineer(name="vlad", engineer_name="vlad", primary_language="cobol", status="elbonian engineer", paperwork=[
                Paperwork(description='elbonian missive #3')
            ], machines=[
                    Machine(name="Commodore 64"),
                    Machine(name="IBM 3270")
            ])
        
            c2.employees = [e3]
            sess = create_session()
            sess.save(c1)
            sess.save(c2)
            sess.flush()
            sess.clear()

            all_employees = [e1, e2, b1, m1, e3]
            c1_employees = [e1, e2, b1, m1]
            c2_employees = [e3]
            
        def test_get(self):
            sess = create_session()
            
            # for all mappers, ensure the primary key has been calculated as just the "person_id"
            # column
            self.assertEquals(sess.query(Person).get(e1.person_id), Engineer(name="dilbert"))
            self.assertEquals(sess.query(Engineer).get(e1.person_id), Engineer(name="dilbert"))
            self.assertEquals(sess.query(Manager).get(b1.person_id), Boss(name="pointy haired boss"))
            
        def test_filter_on_subclass(self):
            sess = create_session()
            self.assertEquals(sess.query(Engineer).all()[0], Engineer(name="dilbert"))

            self.assertEquals(sess.query(Engineer).first(), Engineer(name="dilbert"))

            self.assertEquals(sess.query(Engineer).filter(Engineer.person_id==e1.person_id).first(), Engineer(name="dilbert"))

            self.assertEquals(sess.query(Manager).filter(Manager.person_id==m1.person_id).one(), Manager(name="dogbert"))

            self.assertEquals(sess.query(Manager).filter(Manager.person_id==b1.person_id).one(), Boss(name="pointy haired boss"))
        
            self.assertEquals(sess.query(Boss).filter(Boss.person_id==b1.person_id).one(), Boss(name="pointy haired boss"))

        def test_join_from_polymorphic(self):
            sess = create_session()
        
            for aliased in (True, False):
                self.assertEquals(sess.query(Person).join('paperwork', aliased=aliased).filter(Paperwork.description.like('%review%')).all(), [b1, m1])

                self.assertEquals(sess.query(Person).join('paperwork', aliased=aliased).filter(Paperwork.description.like('%#2%')).all(), [e1, m1])

                self.assertEquals(sess.query(Engineer).join('paperwork', aliased=aliased).filter(Paperwork.description.like('%#2%')).all(), [e1])

                self.assertEquals(sess.query(Person).join('paperwork', aliased=aliased).filter(Person.c.name.like('%dog%')).filter(Paperwork.description.like('%#2%')).all(), [m1])
    
        def test_join_to_polymorphic(self):
            sess = create_session()
            self.assertEquals(sess.query(Company).join('employees').filter(Person.name=='vlad').one(), c2)

            self.assertEquals(sess.query(Company).join('employees', aliased=True).filter(Person.name=='vlad').one(), c2)
    
        def test_join_to_subclass(self):
            sess = create_session()

            if select_type == '':
                self.assertEquals(sess.query(Company).select_from(companies.join(people).join(engineers)).filter(Engineer.primary_language=='java').all(), [c1])
                self.assertEquals(sess.query(Company).join(('employees', people.join(engineers))).filter(Engineer.primary_language=='java').all(), [c1])
                self.assertEquals(sess.query(Person).select_from(people.join(engineers)).join(Engineer.machines).all(), [e1, e2, e3])
                self.assertEquals(sess.query(Person).select_from(people.join(engineers)).join(Engineer.machines).filter(Machine.name.ilike("%ibm%")).all(), [e1, e3])
                self.assertEquals(sess.query(Company).join([('employees', people.join(engineers)), Engineer.machines]).all(), [c1, c2])
                self.assertEquals(sess.query(Company).join([('employees', people.join(engineers)), Engineer.machines]).filter(Machine.name.ilike("%thinkpad%")).all(), [c1])
            else:
                self.assertEquals(sess.query(Company).select_from(companies.join(people).join(engineers)).filter(Engineer.primary_language=='java').all(), [c1])
                self.assertEquals(sess.query(Company).join(['employees']).filter(Engineer.primary_language=='java').all(), [c1])
                self.assertEquals(sess.query(Person).join(Engineer.machines).all(), [e1, e2, e3])
                self.assertEquals(sess.query(Person).join(Engineer.machines).filter(Machine.name.ilike("%ibm%")).all(), [e1, e3])
                self.assertEquals(sess.query(Company).join(['employees', Engineer.machines]).all(), [c1, c2])
                self.assertEquals(sess.query(Company).join(['employees', Engineer.machines]).filter(Machine.name.ilike("%thinkpad%")).all(), [c1])
        
        def test_join_through_polymorphic(self):

            sess = create_session()

            for aliased in (True, False):
                self.assertEquals(
                    sess.query(Company).\
                        join(['employees', 'paperwork'], aliased=aliased).filter(Paperwork.description.like('%#2%')).all(),
                    [c1]
                )

                self.assertEquals(
                    sess.query(Company).\
                        join(['employees', 'paperwork'], aliased=aliased).filter(Paperwork.description.like('%#%')).all(),
                    [c1, c2]
                )

                self.assertEquals(
                    sess.query(Company).\
                        join(['employees', 'paperwork'], aliased=aliased).filter(Person.name.in_(['dilbert', 'vlad'])).filter(Paperwork.description.like('%#2%')).all(),
                    [c1]
                )
        
                self.assertEquals(
                    sess.query(Company).\
                        join(['employees', 'paperwork'], aliased=aliased).filter(Person.name.in_(['dilbert', 'vlad'])).filter(Paperwork.description.like('%#%')).all(),
                    [c1, c2]
                )

                self.assertEquals(
                    sess.query(Company).join('employees', aliased=aliased).filter(Person.name.in_(['dilbert', 'vlad'])).\
                        join('paperwork', from_joinpoint=True, aliased=aliased).filter(Paperwork.description.like('%#2%')).all(),
                    [c1]
                )

                self.assertEquals(
                    sess.query(Company).join('employees', aliased=aliased).filter(Person.name.in_(['dilbert', 'vlad'])).\
                        join('paperwork', from_joinpoint=True, aliased=aliased).filter(Paperwork.description.like('%#%')).all(),
                    [c1, c2]
                )
        
        def test_filter_on_baseclass(self):
            sess = create_session()

            self.assertEquals(sess.query(Person).all(), all_employees)

            self.assertEquals(sess.query(Person).first(), all_employees[0])
        
            self.assertEquals(sess.query(Person).filter(Person.person_id==e2.person_id).one(), e2)
    
    PolymorphicQueryTest.__name__ = "Polymorphic%sTest" % select_type
    return PolymorphicQueryTest

for select_type in ('', 'Unions', 'AliasedJoins', 'Joins'):
    testclass = make_test(select_type)
    exec("%s = testclass" % testclass.__name__)
    
del testclass

if __name__ == "__main__":
    testenv.main()
