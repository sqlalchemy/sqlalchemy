"""tests the Query object's ability to work with polymorphic selectables
and inheriting mappers."""

# TODO: under construction !

import testenv; testenv.configure_for_tests()
import sets
from sqlalchemy import *
from sqlalchemy.orm import *
from sqlalchemy import exceptions
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
                'employees':relation(Person, order_by=people.c.person_id)
            })

            mapper(Machine, machines)

            if select_type == '':
                person_join = manager_join = None
                person_with_polymorphic = None
                manager_with_polymorphic = None
            elif select_type == 'Polymorphic':
                person_join = manager_join = None
                person_with_polymorphic = '*'
                manager_with_polymorphic = '*'
            elif select_type == 'Unions':
                person_join = polymorphic_union(
                    {
                        'engineer':people.join(engineers),
                        'manager':people.join(managers),
                    }, None, 'pjoin')

                manager_join = people.join(managers).outerjoin(boss)
                person_with_polymorphic = ([Person, Manager, Engineer], person_join)
                manager_with_polymorphic = ('*', manager_join)
            elif select_type == 'AliasedJoins':
                person_join = people.outerjoin(engineers).outerjoin(managers).select(use_labels=True).alias('pjoin')
                manager_join = people.join(managers).outerjoin(boss).select(use_labels=True).alias('mjoin')
                person_with_polymorphic = ([Person, Manager, Engineer], person_join)
                manager_with_polymorphic = ('*', manager_join)
            elif select_type == 'Joins':
                person_join = people.outerjoin(engineers).outerjoin(managers)
                manager_join = people.join(managers).outerjoin(boss)
                person_with_polymorphic = ([Person, Manager, Engineer], person_join)
                manager_with_polymorphic = ('*', manager_join)


            # testing a order_by here as well; the surrogate mapper has to adapt it
            mapper(Person, people, 
                with_polymorphic=person_with_polymorphic, 
                polymorphic_on=people.c.type, polymorphic_identity='person', order_by=people.c.person_id, 
                properties={
                    'paperwork':relation(Paperwork)
                })
            mapper(Engineer, engineers, inherits=Person, polymorphic_identity='engineer', properties={
                    'machines':relation(Machine)
                })
            mapper(Manager, managers, with_polymorphic=manager_with_polymorphic, 
                        inherits=Person, polymorphic_identity='manager')
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
        
        def test_loads_at_once(self):
            """test that all objects load from the full query, when with_polymorphic is used"""
            
            sess = create_session()
            def go():
                self.assertEquals(sess.query(Person).all(), all_employees)
            self.assert_sql_count(testing.db, go, {'':14, 'Polymorphic':9}.get(select_type, 10))

        def test_primary_eager_aliasing(self):
            sess = create_session()
            def go():
                self.assertEquals(sess.query(Person).options(eagerload(Engineer.machines))[1:3].all(), all_employees[1:3])
            self.assert_sql_count(testing.db, go, {'':6, 'Polymorphic':3}.get(select_type, 4))

            sess = create_session()
            def go():
                self.assertEquals(sess.query(Person).with_polymorphic('*').options(eagerload(Engineer.machines))[1:3].all(), all_employees[1:3])
            self.assert_sql_count(testing.db, go, 3)
            
            
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

        def test_join_from_with_polymorphic(self):
            sess = create_session()

            for aliased in (True, False):
                sess.clear()
                self.assertEquals(sess.query(Person).with_polymorphic(Manager).join('paperwork', aliased=aliased).filter(Paperwork.description.like('%review%')).all(), [b1, m1])

                sess.clear()
                self.assertEquals(sess.query(Person).with_polymorphic([Manager, Engineer]).join('paperwork', aliased=aliased).filter(Paperwork.description.like('%#2%')).all(), [e1, m1])

                sess.clear()
                self.assertEquals(sess.query(Person).with_polymorphic([Manager, Engineer]).join('paperwork', aliased=aliased).filter(Person.c.name.like('%dog%')).filter(Paperwork.description.like('%#2%')).all(), [m1])
    
        def test_join_to_polymorphic(self):
            sess = create_session()
            self.assertEquals(sess.query(Company).join('employees').filter(Person.name=='vlad').one(), c2)

            self.assertEquals(sess.query(Company).join('employees', aliased=True).filter(Person.name=='vlad').one(), c2)
        
        def test_polymorphic_any(self):
            sess = create_session()

            self.assertEquals(
                sess.query(Company).filter(Company.employees.of_type(Engineer).any(Engineer.primary_language=='cobol')).one(),
                c2
                )

            self.assertEquals(
                sess.query(Company).filter(Company.employees.of_type(Boss).any(Boss.golf_swing=='fore')).one(),
                c1
                )
            self.assertEquals(
                sess.query(Company).filter(Company.employees.of_type(Boss).any(Manager.manager_name=='pointy')).one(),
                c1
                )

            if select_type == '':
                self.assertEquals(
                    sess.query(Company).filter(Company.employees.any(and_(Engineer.primary_language=='cobol', people.c.person_id==engineers.c.person_id))).one(),
                    c2
                    )
        
        def test_expire(self):
            """test that individual column refresh doesn't get tripped up by the select_table mapper"""
            
            sess = create_session()
            m1 = sess.query(Manager).filter(Manager.name=='dogbert').one()
            sess.expire(m1)
            assert m1.status == 'regular manager'

            m2 = sess.query(Manager).filter(Manager.name=='pointy haired boss').one()
            sess.expire(m2, ['manager_name', 'golf_swing'])
            assert m2.golf_swing=='fore'
            
        def test_with_polymorphic(self):
            
            sess = create_session()
            
            # compare to entities without related collections to prevent additional lazy SQL from firing on 
            # loaded entities
            emps_without_relations = [
                Engineer(name="dilbert", engineer_name="dilbert", primary_language="java", status="regular engineer"),
                Engineer(name="wally", engineer_name="wally", primary_language="c++", status="regular engineer"),
                Boss(name="pointy haired boss", golf_swing="fore", manager_name="pointy", status="da boss"),
                Manager(name="dogbert", manager_name="dogbert", status="regular manager"),
                Engineer(name="vlad", engineer_name="vlad", primary_language="cobol", status="elbonian engineer")
            ]
            
            def go():
                self.assertEquals(sess.query(Person).with_polymorphic(Engineer).filter(Engineer.primary_language=='java').all(), emps_without_relations[0:1])
            self.assert_sql_count(testing.db, go, 1)
            
            sess.clear()
            def go():
                self.assertEquals(sess.query(Person).with_polymorphic('*').all(), emps_without_relations)
            self.assert_sql_count(testing.db, go, 1)

            sess.clear()
            def go():
                self.assertEquals(sess.query(Person).with_polymorphic(Engineer).all(), emps_without_relations)
            self.assert_sql_count(testing.db, go, 3)

            sess.clear()
            def go():
                self.assertEquals(sess.query(Person).with_polymorphic(Engineer, people.outerjoin(engineers)).all(), emps_without_relations)
            self.assert_sql_count(testing.db, go, 3)
            
            sess.clear()
            def go():
                # limit the polymorphic join down to just "Person", overriding select_table
                self.assertEquals(sess.query(Person).with_polymorphic(Person).all(), emps_without_relations)
            self.assert_sql_count(testing.db, go, 6)
        
        def test_relation_to_polymorphic(self):
            assert_result = [
                Company(name="MegaCorp, Inc.", employees=[
                    Engineer(name="dilbert", engineer_name="dilbert", primary_language="java", status="regular engineer", machines=[Machine(name="IBM ThinkPad"), Machine(name="IPhone")]),
                    Engineer(name="wally", engineer_name="wally", primary_language="c++", status="regular engineer"),
                    Boss(name="pointy haired boss", golf_swing="fore", manager_name="pointy", status="da boss"),
                    Manager(name="dogbert", manager_name="dogbert", status="regular manager"),
                ]),
                Company(name="Elbonia, Inc.", employees=[
                    Engineer(name="vlad", engineer_name="vlad", primary_language="cobol", status="elbonian engineer")
                ])
            ]
            
            sess = create_session()
            def go():
                # test load Companies with lazy load to 'employees'
                self.assertEquals(sess.query(Company).all(), assert_result)
            self.assert_sql_count(testing.db, go, {'':9, 'Polymorphic':4}.get(select_type, 5))
        
            sess = create_session()
            def go():
                # currently, it doesn't matter if we say Company.employees, or Company.employees.of_type(Engineer).  eagerloader doesn't
                # pick up on the "of_type()" as of yet.
                self.assertEquals(sess.query(Company).options(eagerload_all([Company.employees.of_type(Engineer), Engineer.machines])).all(), assert_result)
            
            # in the case of select_type='', the eagerload doesn't take in this case; 
            # it eagerloads company->people, then a load for each of 5 rows, then lazyload of "machines"            
            self.assert_sql_count(testing.db, go, {'':7, 'Polymorphic':1}.get(select_type, 2))

        def test_eagerload_on_subclass(self):
            sess = create_session()
            def go():
                # test load People with eagerload to engineers + machines
                self.assertEquals(sess.query(Person).with_polymorphic('*').options(eagerload([Engineer.machines])).filter(Person.name=='dilbert').all(), 
                [Engineer(name="dilbert", engineer_name="dilbert", primary_language="java", status="regular engineer", machines=[Machine(name="IBM ThinkPad"), Machine(name="IPhone")])]
                )
            self.assert_sql_count(testing.db, go, 1)
            
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
            
            # non-polymorphic
            self.assertEquals(sess.query(Engineer).join(Engineer.machines).all(), [e1, e2, e3])
            self.assertEquals(sess.query(Engineer).join(Engineer.machines).filter(Machine.name.ilike("%ibm%")).all(), [e1, e3])

            # here's the new way
            self.assertEquals(sess.query(Company).join(Company.employees.of_type(Engineer)).filter(Engineer.primary_language=='java').all(), [c1])
            self.assertEquals(sess.query(Company).join([Company.employees.of_type(Engineer), 'machines']).filter(Machine.name.ilike("%thinkpad%")).all(), [c1])

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

for select_type in ('', 'Polymorphic', 'Unions', 'AliasedJoins', 'Joins'):
    testclass = make_test(select_type)
    exec("%s = testclass" % testclass.__name__)
    
del testclass

class SelfReferentialTest(ORMTest):
    keep_mappers = True
    
    def define_tables(self, metadata):
        global people, engineers
        people = Table('people', metadata,
           Column('person_id', Integer, Sequence('person_id_seq', optional=True), primary_key=True),
           Column('name', String(50)),
           Column('type', String(30)))

        engineers = Table('engineers', metadata,
           Column('person_id', Integer, ForeignKey('people.person_id'), primary_key=True),
           Column('primary_language', String(50)),
           Column('reports_to_id', Integer, ForeignKey('people.person_id'))
          )

        mapper(Person, people, polymorphic_on=people.c.type, polymorphic_identity='person')
        mapper(Engineer, engineers, inherits=Person, 
          inherit_condition=engineers.c.person_id==people.c.person_id,
          polymorphic_identity='engineer', properties={
          'reports_to':relation(Person, primaryjoin=people.c.person_id==engineers.c.reports_to_id)
        })
    
    def test_has(self):
        
        p1 = Person(name='dogbert')
        e1 = Engineer(name='dilbert', primary_language='java', reports_to=p1)
        sess = create_session()
        sess.save(p1)
        sess.save(e1)
        sess.flush()
        sess.clear()
        
        self.assertEquals(sess.query(Engineer).filter(Engineer.reports_to.has(Person.name=='dogbert')).first(), Engineer(name='dilbert'))
        
    def test_join(self):
        p1 = Person(name='dogbert')
        e1 = Engineer(name='dilbert', primary_language='java', reports_to=p1)
        sess = create_session()
        sess.save(p1)
        sess.save(e1)
        sess.flush()
        sess.clear()
        
        self.assertEquals(sess.query(Engineer).join('reports_to', aliased=True).filter(Person.name=='dogbert').first(), Engineer(name='dilbert'))
        
    def test_noalias_raises(self):
        sess = create_session()
        def go():
            sess.query(Engineer).join('reports_to')
        self.assertRaises(exceptions.InvalidRequestError, go)

if __name__ == "__main__":
    testenv.main()
