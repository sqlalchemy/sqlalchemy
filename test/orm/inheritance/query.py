"""tests the Query object's ability to work with polymorphic selectables
and inheriting mappers."""

# TODO: under construction !

import testenv; testenv.configure_for_tests()
import sets
from sqlalchemy import *
from sqlalchemy.orm import *
from sqlalchemy import exc as sa_exc
from testlib import *
from testlib import fixtures
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.engine import default

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
    class PolymorphicQueryTest(ORMTest, AssertsCompiledSQL):
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
                    'paperwork':relation(Paperwork, order_by=paperwork.c.paperwork_id)
                })
            mapper(Engineer, engineers, inherits=Person, polymorphic_identity='engineer', properties={
                    'machines':relation(Machine, order_by=machines.c.machine_id)
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
                self.assertEquals(sess.query(Person).options(eagerload(Engineer.machines))[1:3], all_employees[1:3])
            self.assert_sql_count(testing.db, go, {'':6, 'Polymorphic':3}.get(select_type, 4))

            sess = create_session()

            # assert the JOINs dont over JOIN
            assert sess.query(Person).with_polymorphic('*').options(eagerload(Engineer.machines)).limit(2).offset(1).with_labels().subquery().count().scalar() == 2

            def go():
                self.assertEquals(sess.query(Person).with_polymorphic('*').options(eagerload(Engineer.machines))[1:3], all_employees[1:3])
            self.assert_sql_count(testing.db, go, 3)
            
            
        def test_get(self):
            sess = create_session()
            
            # for all mappers, ensure the primary key has been calculated as just the "person_id"
            # column
            self.assertEquals(sess.query(Person).get(e1.person_id), Engineer(name="dilbert", primary_language="java"))
            self.assertEquals(sess.query(Engineer).get(e1.person_id), Engineer(name="dilbert", primary_language="java"))
            self.assertEquals(sess.query(Manager).get(b1.person_id), Boss(name="pointy haired boss", golf_swing="fore"))
            
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

                self.assertEquals(sess.query(Person).join('paperwork', aliased=aliased).filter(Person.name.like('%dog%')).filter(Paperwork.description.like('%#2%')).all(), [m1])

        def test_join_from_with_polymorphic(self):
            sess = create_session()

            for aliased in (True, False):
                sess.clear()
                self.assertEquals(sess.query(Person).with_polymorphic(Manager).join('paperwork', aliased=aliased).filter(Paperwork.description.like('%review%')).all(), [b1, m1])

                sess.clear()
                self.assertEquals(sess.query(Person).with_polymorphic([Manager, Engineer]).join('paperwork', aliased=aliased).filter(Paperwork.description.like('%#2%')).all(), [e1, m1])

                sess.clear()
                self.assertEquals(sess.query(Person).with_polymorphic([Manager, Engineer]).join('paperwork', aliased=aliased).filter(Person.name.like('%dog%')).filter(Paperwork.description.like('%#2%')).all(), [m1])
    
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

            if select_type != '':
                self.assertEquals(
                    sess.query(Person).filter(Engineer.machines.any(Machine.name=="Commodore 64")).all(), [e2, e3]
                )

            self.assertEquals(
                sess.query(Person).filter(Person.paperwork.any(Paperwork.description=="review #2")).all(), [m1]
            )
            
            self.assertEquals(
                sess.query(Company).filter(Company.employees.of_type(Engineer).any(and_(Engineer.primary_language=='cobol'))).one(),
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
            
            
            self.assertRaises(sa_exc.InvalidRequestError, sess.query(Person).with_polymorphic, Paperwork)
            self.assertRaises(sa_exc.InvalidRequestError, sess.query(Engineer).with_polymorphic, Boss)
            self.assertRaises(sa_exc.InvalidRequestError, sess.query(Engineer).with_polymorphic, Person)
            
            # compare to entities without related collections to prevent additional lazy SQL from firing on 
            # loaded entities
            emps_without_relations = [
                Engineer(name="dilbert", engineer_name="dilbert", primary_language="java", status="regular engineer"),
                Engineer(name="wally", engineer_name="wally", primary_language="c++", status="regular engineer"),
                Boss(name="pointy haired boss", golf_swing="fore", manager_name="pointy", status="da boss"),
                Manager(name="dogbert", manager_name="dogbert", status="regular manager"),
                Engineer(name="vlad", engineer_name="vlad", primary_language="cobol", status="elbonian engineer")
            ]
            self.assertEquals(sess.query(Person).with_polymorphic('*').all(), emps_without_relations)
            
            
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
            self.assertEquals(sess.query(Company).join(('employees', people.join(engineers))).filter(Engineer.primary_language=='java').all(), [c1])

            if select_type == '':
                self.assertEquals(sess.query(Company).select_from(companies.join(people).join(engineers)).filter(Engineer.primary_language=='java').all(), [c1])
                self.assertEquals(sess.query(Company).join(('employees', people.join(engineers))).filter(Engineer.primary_language=='java').all(), [c1])
                
                ealias = aliased(Engineer)
                self.assertEquals(sess.query(Company).join(('employees', ealias)).filter(ealias.primary_language=='java').all(), [c1])

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
        def test_explicit_polymorphic_join(self):
            sess = create_session()

            # join from Company to Engineer; join condition formulated by
            # ORMJoin using regular table foreign key connections.  Engineer
            # is expressed as "(select * people join engineers) as anon_1"
            # so the join is contained.
            self.assertEquals(
                sess.query(Company).join(Engineer).filter(Engineer.engineer_name=='vlad').one(),
                c2
            )

            # same, using explicit join condition.  Query.join() must adapt the on clause
            # here to match the subquery wrapped around "people join engineers".
            self.assertEquals(
                sess.query(Company).join((Engineer, Company.company_id==Engineer.company_id)).filter(Engineer.engineer_name=='vlad').one(),
                c2
            )
                
        
        def test_filter_on_baseclass(self):
            sess = create_session()

            self.assertEquals(sess.query(Person).all(), all_employees)

            self.assertEquals(sess.query(Person).first(), all_employees[0])
        
            self.assertEquals(sess.query(Person).filter(Person.person_id==e2.person_id).one(), e2)
    
        def test_from_alias(self):
            sess = create_session()
            
            palias = aliased(Person)
            self.assertEquals(
                sess.query(palias).filter(palias.name.in_(['dilbert', 'wally'])).all(),
                [e1, e2]
            )
            
        def test_self_referential(self):
            sess = create_session()
            
            c1_employees = [e1, e2, b1, m1]
            
            palias = aliased(Person)
            self.assertEquals(
                sess.query(Person, palias).filter(Person.company_id==palias.company_id).filter(Person.name=='dogbert').\
                    filter(Person.person_id>palias.person_id).order_by(Person.person_id, palias.person_id).all(), 
                [
                    (m1, e1),
                    (m1, e2),
                    (m1, b1),
                ]
            )

            self.assertEquals(
                sess.query(Person, palias).filter(Person.company_id==palias.company_id).filter(Person.name=='dogbert').\
                    filter(Person.person_id>palias.person_id).from_self().order_by(Person.person_id, palias.person_id).all(), 
                [
                    (m1, e1),
                    (m1, e2),
                    (m1, b1),
                ]
            )
        
        def test_nesting_queries(self):
            sess = create_session()
            
            # query.statement places a flag "no_adapt" on the returned statement.  This prevents
            # the polymorphic adaptation in the second "filter" from hitting it, which would pollute 
            # the subquery and usually results in recursion overflow errors within the adaption.
            subq = sess.query(engineers.c.person_id).filter(Engineer.primary_language=='java').statement.as_scalar()
            
            self.assertEquals(sess.query(Person).filter(Person.person_id==subq).one(), e1)
            
        def test_mixed_entities(self):
            sess = create_session()

            self.assertEquals(
                sess.query(Company.name, Person).join(Company.employees).filter(Company.name=='Elbonia, Inc.').all(),
                [(u'Elbonia, Inc.', 
                    Engineer(status=u'elbonian engineer',engineer_name=u'vlad',name=u'vlad',primary_language=u'cobol'))]
            )

            self.assertEquals(
                sess.query(Person, Company.name).join(Company.employees).filter(Company.name=='Elbonia, Inc.').all(),
                [(Engineer(status=u'elbonian engineer',engineer_name=u'vlad',name=u'vlad',primary_language=u'cobol'),
                    u'Elbonia, Inc.')]
            )
            
            
            self.assertEquals(
                sess.query(Manager.name).all(), 
                [('pointy haired boss', ), ('dogbert',)]
            )

            self.assertEquals(
                sess.query(Manager.name + " foo").all(), 
                [('pointy haired boss foo', ), ('dogbert foo',)]
            )

            row = sess.query(Engineer.name, Engineer.primary_language).filter(Engineer.name=='dilbert').first()
            assert row.name == 'dilbert'
            assert row.primary_language == 'java'
            

            self.assertEquals(
                sess.query(Engineer.name, Engineer.primary_language).all(),
                [(u'dilbert', u'java'), (u'wally', u'c++'), (u'vlad', u'cobol')]
            )

            self.assertEquals(
                sess.query(Boss.name, Boss.golf_swing).all(),
                [(u'pointy haired boss', u'fore')]
            )
            
            # TODO: I think raise error on these for now.  different inheritance/loading schemes have different
            # results here, all incorrect
            #
            # self.assertEquals(
            #    sess.query(Person.name, Engineer.primary_language).all(),
            #    []
            # )
            
            # self.assertEquals(
            #    sess.query(Person.name, Engineer.primary_language, Manager.manager_name).all(),
            #    []
            # )

            self.assertEquals(
                sess.query(Person.name, Company.name).join(Company.employees).filter(Company.name=='Elbonia, Inc.').all(),
                [(u'vlad',u'Elbonia, Inc.')]
            )

            self.assertEquals(
                sess.query(Engineer.primary_language).filter(Person.type=='engineer').all(),
                [(u'java',), (u'c++',), (u'cobol',)]
            )

            if select_type != '':
                self.assertEquals(
                    sess.query(Engineer, Company.name).join(Company.employees).filter(Person.type=='engineer').all(),
                    [
                    (Engineer(status=u'regular engineer',engineer_name=u'dilbert',name=u'dilbert',company_id=1,primary_language=u'java',person_id=1,type=u'engineer'), u'MegaCorp, Inc.'), 
                    (Engineer(status=u'regular engineer',engineer_name=u'wally',name=u'wally',company_id=1,primary_language=u'c++',person_id=2,type=u'engineer'), u'MegaCorp, Inc.'), 
                    (Engineer(status=u'elbonian engineer',engineer_name=u'vlad',name=u'vlad',company_id=2,primary_language=u'cobol',person_id=5,type=u'engineer'), u'Elbonia, Inc.')
                    ]
                )
            
                self.assertEquals(
                    sess.query(Engineer.primary_language, Company.name).join(Company.employees).filter(Person.type=='engineer').order_by(desc(Engineer.primary_language)).all(),
                    [(u'java', u'MegaCorp, Inc.'), (u'cobol', u'Elbonia, Inc.'), (u'c++', u'MegaCorp, Inc.')]
                )

            palias = aliased(Person)
            self.assertEquals(
                sess.query(Person, Company.name, palias).join(Company.employees).filter(Company.name=='Elbonia, Inc.').filter(palias.name=='dilbert').all(),
                [(Engineer(status=u'elbonian engineer',engineer_name=u'vlad',name=u'vlad',primary_language=u'cobol'),
                    u'Elbonia, Inc.', 
                    Engineer(status=u'regular engineer',engineer_name=u'dilbert',name=u'dilbert',company_id=1,primary_language=u'java',person_id=1,type=u'engineer'))]
            )

            self.assertEquals(
                sess.query(palias, Company.name, Person).join(Company.employees).filter(Company.name=='Elbonia, Inc.').filter(palias.name=='dilbert').all(),
                [(Engineer(status=u'regular engineer',engineer_name=u'dilbert',name=u'dilbert',company_id=1,primary_language=u'java',person_id=1,type=u'engineer'),
                    u'Elbonia, Inc.', 
                    Engineer(status=u'elbonian engineer',engineer_name=u'vlad',name=u'vlad',primary_language=u'cobol'),)
                ]
            )

            self.assertEquals(
                sess.query(Person.name, Company.name, palias.name).join(Company.employees).filter(Company.name=='Elbonia, Inc.').filter(palias.name=='dilbert').all(),
                [(u'vlad', u'Elbonia, Inc.', u'dilbert')]
            )
            
            palias = aliased(Person)
            self.assertEquals(
                sess.query(Person.type, Person.name, palias.type, palias.name).filter(Person.company_id==palias.company_id).filter(Person.name=='dogbert').\
                    filter(Person.person_id>palias.person_id).order_by(Person.person_id, palias.person_id).all(), 
                [(u'manager', u'dogbert', u'engineer', u'dilbert'), 
                (u'manager', u'dogbert', u'engineer', u'wally'), 
                (u'manager', u'dogbert', u'boss', u'pointy haired boss')]
            )
        
            self.assertEquals(
                sess.query(Person.name, Paperwork.description).filter(Person.person_id==Paperwork.person_id).order_by(Person.name, Paperwork.description).all(), 
                [(u'dilbert', u'tps report #1'), (u'dilbert', u'tps report #2'), (u'dogbert', u'review #2'), 
                (u'dogbert', u'review #3'), 
                (u'pointy haired boss', u'review #1'), 
                (u'vlad', u'elbonian missive #3'),
                (u'wally', u'tps report #3'), 
                (u'wally', u'tps report #4'),
                ]
            )

            if select_type != '':
                self.assertEquals(
                    sess.query(func.count(Person.person_id)).filter(Engineer.primary_language=='java').all(), 
                    [(1, )]
                )
            
            self.assertEquals(
                sess.query(Company.name, func.count(Person.person_id)).filter(Company.company_id==Person.company_id).group_by(Company.name).order_by(Company.name).all(),
                [(u'Elbonia, Inc.', 1), (u'MegaCorp, Inc.', 4)]
            )

            self.assertEquals(
                sess.query(Company.name, func.count(Person.person_id)).join(Company.employees).group_by(Company.name).order_by(Company.name).all(),
                [(u'Elbonia, Inc.', 1), (u'MegaCorp, Inc.', 4)]
            )
    
    
    PolymorphicQueryTest.__name__ = "Polymorphic%sTest" % select_type
    return PolymorphicQueryTest

for select_type in ('', 'Polymorphic', 'Unions', 'AliasedJoins', 'Joins'):
    testclass = make_test(select_type)
    exec("%s = testclass" % testclass.__name__)
    
del testclass

class SelfReferentialTestJoinedToBase(ORMTest):
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

    def test_oftype_aliases_in_exists(self):
        e1 = Engineer(name='dilbert', primary_language='java')
        e2 = Engineer(name='wally', primary_language='c++', reports_to=e1)
        sess = create_session()
        sess.add_all([e1, e2])
        sess.flush()
        
        self.assertEquals(sess.query(Engineer).filter(Engineer.reports_to.of_type(Engineer).has(Engineer.name=='dilbert')).first(), e2)
        
    def test_join(self):
        p1 = Person(name='dogbert')
        e1 = Engineer(name='dilbert', primary_language='java', reports_to=p1)
        sess = create_session()
        sess.save(p1)
        sess.save(e1)
        sess.flush()
        sess.clear()
        
        self.assertEquals(
            sess.query(Engineer).join('reports_to', aliased=True).filter(Person.name=='dogbert').first(), 
            Engineer(name='dilbert'))

class SelfReferentialTestJoinedToJoined(ORMTest):
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
           Column('reports_to_id', Integer, ForeignKey('managers.person_id'))
          )
          
        managers = Table('managers', metadata,
            Column('person_id', Integer, ForeignKey('people.person_id'), primary_key=True),
        )

        mapper(Person, people, polymorphic_on=people.c.type, polymorphic_identity='person')
        mapper(Manager, managers, inherits=Person, polymorphic_identity='manager')
        
        mapper(Engineer, engineers, inherits=Person, 
          polymorphic_identity='engineer', properties={
          'reports_to':relation(Manager, primaryjoin=managers.c.person_id==engineers.c.reports_to_id)
        })

    def test_has(self):

        m1 = Manager(name='dogbert')
        e1 = Engineer(name='dilbert', primary_language='java', reports_to=m1)
        sess = create_session()
        sess.save(m1)
        sess.save(e1)
        sess.flush()
        sess.clear()

        self.assertEquals(sess.query(Engineer).filter(Engineer.reports_to.has(Manager.name=='dogbert')).first(), Engineer(name='dilbert'))

    def test_join(self):
        m1 = Manager(name='dogbert')
        e1 = Engineer(name='dilbert', primary_language='java', reports_to=m1)
        sess = create_session()
        sess.save(m1)
        sess.save(e1)
        sess.flush()
        sess.clear()

        self.assertEquals(
            sess.query(Engineer).join('reports_to', aliased=True).filter(Manager.name=='dogbert').first(), 
            Engineer(name='dilbert'))
        

class M2MFilterTest(ORMTest):
    keep_mappers = True
    keep_data = True
    
    def define_tables(self, metadata):
        global people, engineers, Organization
        
        organizations = Table('organizations', metadata,
            Column('id', Integer, Sequence('org_id_seq', optional=True), primary_key=True),
            Column('name', String(50)),
            )
        engineers_to_org = Table('engineers_org', metadata,
            Column('org_id', Integer, ForeignKey('organizations.id')),
            Column('engineer_id', Integer, ForeignKey('engineers.person_id')),
        )
        
        people = Table('people', metadata,
           Column('person_id', Integer, Sequence('person_id_seq', optional=True), primary_key=True),
           Column('name', String(50)),
           Column('type', String(30)))

        engineers = Table('engineers', metadata,
           Column('person_id', Integer, ForeignKey('people.person_id'), primary_key=True),
           Column('primary_language', String(50)),
          )
        
        class Organization(fixtures.Base):
            pass
            
        mapper(Organization, organizations, properties={
            'engineers':relation(Engineer, secondary=engineers_to_org, backref='organizations')
        })
        
        mapper(Person, people, polymorphic_on=people.c.type, polymorphic_identity='person')
        mapper(Engineer, engineers, inherits=Person, polymorphic_identity='engineer')
    
    def insert_data(self):
        e1 = Engineer(name='e1')
        e2 = Engineer(name='e2')
        e3 = Engineer(name='e3')
        e4 = Engineer(name='e4')
        org1 = Organization(name='org1', engineers=[e1, e2])
        org2 = Organization(name='org2', engineers=[e3, e4])
        
        sess = create_session()
        sess.save(org1)
        sess.save(org2)
        sess.flush()
        
    def test_not_contains(self):
        sess = create_session()
        
        e1 = sess.query(Person).filter(Engineer.name=='e1').one()
        
        # this works
        self.assertEquals(sess.query(Organization).filter(~Organization.engineers.of_type(Engineer).contains(e1)).all(), [Organization(name='org2')])

        # this had a bug
        self.assertEquals(sess.query(Organization).filter(~Organization.engineers.contains(e1)).all(), [Organization(name='org2')])
    
    def test_any(self):
        sess = create_session()
        self.assertEquals(sess.query(Organization).filter(Organization.engineers.of_type(Engineer).any(Engineer.name=='e1')).all(), [Organization(name='org1')])
        self.assertEquals(sess.query(Organization).filter(Organization.engineers.any(Engineer.name=='e1')).all(), [Organization(name='org1')])

class SelfReferentialM2MTest(ORMTest, AssertsCompiledSQL):
    def define_tables(self, metadata):
        Base = declarative_base(metadata=metadata)

        secondary_table = Table('secondary', Base.metadata,
           Column('left_id', Integer, ForeignKey('parent.id'), nullable=False),
           Column('right_id', Integer, ForeignKey('parent.id'), nullable=False))
          
        global Parent, Child1, Child2
        class Parent(Base):
           __tablename__ = 'parent'
           id = Column(Integer, primary_key=True)
           cls = Column(String(50))
           __mapper_args__ = dict(polymorphic_on = cls )

        class Child1(Parent):
           __tablename__ = 'child1'
           id = Column(Integer, ForeignKey('parent.id'), primary_key=True)
           __mapper_args__ = dict(polymorphic_identity = 'child1')

        class Child2(Parent):
           __tablename__ = 'child2'
           id = Column(Integer, ForeignKey('parent.id'), primary_key=True)
           __mapper_args__ = dict(polymorphic_identity = 'child2')

        Child1.left_child2 = relation(Child2, secondary = secondary_table,
               primaryjoin = Parent.id == secondary_table.c.right_id,
               secondaryjoin = Parent.id == secondary_table.c.left_id,
               uselist = False,
                               )

    def test_eager_join(self):
        session = create_session()
        
        c1 = Child1()
        c1.left_child2 = Child2()
        session.add(c1)
        session.flush()
        
        q = session.query(Child1).options(eagerload('left_child2'))

        # test that the splicing of the join works here, doesnt break in the middle of "parent join child1"
        self.assert_compile(q.limit(1).with_labels().statement, 
        "SELECT anon_1.parent_id AS anon_1_parent_id, anon_1.child1_id AS anon_1_child1_id, "\
        "anon_1.parent_cls AS anon_1_parent_cls, anon_2.parent_id AS anon_2_parent_id, "\
        "anon_2.child2_id AS anon_2_child2_id, anon_2.parent_cls AS anon_2_parent_cls FROM "\
        "(SELECT parent.id AS parent_id, child1.id AS child1_id, parent.cls AS parent_cls FROM parent "\
        "JOIN child1 ON parent.id = child1.id  LIMIT 1) AS anon_1 LEFT OUTER JOIN secondary AS secondary_1 "\
        "ON anon_1.parent_id = secondary_1.right_id LEFT OUTER JOIN (SELECT parent.id AS parent_id, "\
        "parent.cls AS parent_cls, child2.id AS child2_id FROM parent JOIN child2 ON parent.id = child2.id) "\
        "AS anon_2 ON anon_2.parent_id = secondary_1.left_id"
        , dialect=default.DefaultDialect())

        # another way to check
        assert q.limit(1).with_labels().subquery().count().scalar() == 1
        
        assert q.first() is c1

if __name__ == "__main__":
    testenv.main()
