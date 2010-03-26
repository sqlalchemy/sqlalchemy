from sqlalchemy.test.testing import eq_
from sqlalchemy import *
from sqlalchemy.orm import *

from sqlalchemy.test import testing
from test.orm import _fixtures
from test.orm._base import MappedTest, ComparableEntity
from sqlalchemy.test.schema import Table, Column


class SingleInheritanceTest(MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table('employees', metadata,
            Column('employee_id', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('name', String(50)),
            Column('manager_data', String(50)),
            Column('engineer_info', String(50)),
            Column('type', String(20)))

        Table('reports', metadata,
              Column('report_id', Integer, primary_key=True, test_needs_autoincrement=True),
              Column('employee_id', ForeignKey('employees.employee_id')),
              Column('name', String(50)),
        )

    @classmethod
    def setup_classes(cls):
        class Employee(ComparableEntity):
            pass
        class Manager(Employee):
            pass
        class Engineer(Employee):
            pass
        class JuniorEngineer(Engineer):
            pass

    @classmethod
    @testing.resolve_artifact_names
    def setup_mappers(cls):
        mapper(Employee, employees, polymorphic_on=employees.c.type)
        mapper(Manager, inherits=Employee, polymorphic_identity='manager')
        mapper(Engineer, inherits=Employee, polymorphic_identity='engineer')
        mapper(JuniorEngineer, inherits=Engineer, polymorphic_identity='juniorengineer')
        
    @testing.resolve_artifact_names
    def test_single_inheritance(self):

        session = create_session()

        m1 = Manager(name='Tom', manager_data='knows how to manage things')
        e1 = Engineer(name='Kurt', engineer_info='knows how to hack')
        e2 = JuniorEngineer(name='Ed', engineer_info='oh that ed')
        session.add_all([m1, e1, e2])
        session.flush()

        assert session.query(Employee).all() == [m1, e1, e2]
        assert session.query(Engineer).all() == [e1, e2]
        assert session.query(Manager).all() == [m1]
        assert session.query(JuniorEngineer).all() == [e2]
        
        m1 = session.query(Manager).one()
        session.expire(m1, ['manager_data'])
        eq_(m1.manager_data, "knows how to manage things")

        row = session.query(Engineer.name, Engineer.employee_id).filter(Engineer.name=='Kurt').first()
        assert row.name == 'Kurt'
        assert row.employee_id == e1.employee_id

    @testing.resolve_artifact_names
    def test_multi_qualification(self):
        session = create_session()
        
        m1 = Manager(name='Tom', manager_data='knows how to manage things')
        e1 = Engineer(name='Kurt', engineer_info='knows how to hack')
        e2 = JuniorEngineer(name='Ed', engineer_info='oh that ed')
        
        session.add_all([m1, e1, e2])
        session.flush()

        ealias = aliased(Engineer)
        eq_(
            session.query(Manager, ealias).all(), 
            [(m1, e1), (m1, e2)]
        )
    
        eq_(
            session.query(Manager.name).all(),
            [("Tom",)]
        )

        eq_(
            session.query(Manager.name, ealias.name).all(),
            [("Tom", "Kurt"), ("Tom", "Ed")]
        )

        eq_(
            session.query(func.upper(Manager.name), func.upper(ealias.name)).all(),
            [("TOM", "KURT"), ("TOM", "ED")]
        )

        eq_(
            session.query(Manager).add_entity(ealias).all(),
            [(m1, e1), (m1, e2)]
        )
        
        eq_(
            session.query(Manager.name).add_column(ealias.name).all(),
            [("Tom", "Kurt"), ("Tom", "Ed")]
        )
        
        # TODO: I think raise error on this for now
        # self.assertEquals(
        #    session.query(Employee.name, Manager.manager_data, Engineer.engineer_info).all(), 
        #    []
        # )
        
    @testing.resolve_artifact_names
    def test_select_from(self):
        sess = create_session()
        m1 = Manager(name='Tom', manager_data='data1')
        m2 = Manager(name='Tom2', manager_data='data2')
        e1 = Engineer(name='Kurt', engineer_info='knows how to hack')
        e2 = JuniorEngineer(name='Ed', engineer_info='oh that ed')
        sess.add_all([m1, m2, e1, e2])
        sess.flush()
        
        eq_(
            sess.query(Manager).select_from(employees.select().limit(10)).all(), 
            [m1, m2]
        )
        
    @testing.resolve_artifact_names
    def test_count(self):
        sess = create_session()
        m1 = Manager(name='Tom', manager_data='data1')
        m2 = Manager(name='Tom2', manager_data='data2')
        e1 = Engineer(name='Kurt', engineer_info='data3')
        e2 = JuniorEngineer(name='marvin', engineer_info='data4')
        sess.add_all([m1, m2, e1, e2])
        sess.flush()

        eq_(sess.query(Manager).count(), 2)
        eq_(sess.query(Engineer).count(), 2)
        eq_(sess.query(Employee).count(), 4)
        
        eq_(sess.query(Manager).filter(Manager.name.like('%m%')).count(), 2)
        eq_(sess.query(Employee).filter(Employee.name.like('%m%')).count(), 3)

    @testing.resolve_artifact_names
    def test_type_filtering(self):
        class Report(ComparableEntity): pass

        mapper(Report, reports, properties={
            'employee': relationship(Employee, backref='reports')})
        sess = create_session()

        m1 = Manager(name='Tom', manager_data='data1')
        r1 = Report(employee=m1)
        sess.add_all([m1, r1])
        sess.flush()
        rq = sess.query(Report)

        assert len(rq.filter(Report.employee.of_type(Manager).has()).all()) == 1
        assert len(rq.filter(Report.employee.of_type(Engineer).has()).all()) == 0

    @testing.resolve_artifact_names
    def test_type_joins(self):
        class Report(ComparableEntity): pass

        mapper(Report, reports, properties={
            'employee': relationship(Employee, backref='reports')})
        sess = create_session()

        m1 = Manager(name='Tom', manager_data='data1')
        r1 = Report(employee=m1)
        sess.add_all([m1, r1])
        sess.flush()

        rq = sess.query(Report)

        assert len(rq.join(Report.employee.of_type(Manager)).all()) == 1
        assert len(rq.join(Report.employee.of_type(Engineer)).all()) == 0


class RelationshipToSingleTest(MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table('employees', metadata,
            Column('employee_id', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('name', String(50)),
            Column('manager_data', String(50)),
            Column('engineer_info', String(50)),
            Column('type', String(20)),
            Column('company_id', Integer, ForeignKey('companies.company_id'))
        )
        
        Table('companies', metadata,
            Column('company_id', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('name', String(50)),
        )
    
    @classmethod
    def setup_classes(cls):
        class Company(ComparableEntity):
            pass
            
        class Employee(ComparableEntity):
            pass
        class Manager(Employee):
            pass
        class Engineer(Employee):
            pass
        class JuniorEngineer(Engineer):
            pass

    @testing.resolve_artifact_names
    def test_of_type(self):
        mapper(Company, companies, properties={
            'employees':relationship(Employee, backref='company')
        })
        mapper(Employee, employees, polymorphic_on=employees.c.type)
        mapper(Manager, inherits=Employee, polymorphic_identity='manager')
        mapper(Engineer, inherits=Employee, polymorphic_identity='engineer')
        mapper(JuniorEngineer, inherits=Engineer, polymorphic_identity='juniorengineer')
        sess = sessionmaker()()
        
        c1 = Company(name='c1')
        c2 = Company(name='c2')
        
        m1 = Manager(name='Tom', manager_data='data1', company=c1)
        m2 = Manager(name='Tom2', manager_data='data2', company=c2)
        e1 = Engineer(name='Kurt', engineer_info='knows how to hack', company=c2)
        e2 = JuniorEngineer(name='Ed', engineer_info='oh that ed', company=c1)
        sess.add_all([c1, c2, m1, m2, e1, e2])
        sess.commit()
        sess.expunge_all()
        eq_(
            sess.query(Company).filter(Company.employees.of_type(JuniorEngineer).any()).all(),
            [
                Company(name='c1'),
            ]
        )

        eq_(
            sess.query(Company).join(Company.employees.of_type(JuniorEngineer)).all(),
            [
                Company(name='c1'),
            ]
        )


    @testing.resolve_artifact_names
    def test_relationship_to_subclass(self):
        mapper(Company, companies, properties={
            'engineers':relationship(Engineer)
        })
        mapper(Employee, employees, polymorphic_on=employees.c.type, properties={
            'company':relationship(Company)
        })
        mapper(Manager, inherits=Employee, polymorphic_identity='manager')
        mapper(Engineer, inherits=Employee, polymorphic_identity='engineer')
        mapper(JuniorEngineer, inherits=Engineer, polymorphic_identity='juniorengineer')
        sess = sessionmaker()()
        
        c1 = Company(name='c1')
        c2 = Company(name='c2')
        
        m1 = Manager(name='Tom', manager_data='data1', company=c1)
        m2 = Manager(name='Tom2', manager_data='data2', company=c2)
        e1 = Engineer(name='Kurt', engineer_info='knows how to hack', company=c2)
        e2 = JuniorEngineer(name='Ed', engineer_info='oh that ed', company=c1)
        sess.add_all([c1, c2, m1, m2, e1, e2])
        sess.commit()

        eq_(c1.engineers, [e2])
        eq_(c2.engineers, [e1])
        
        sess.expunge_all()
        eq_(sess.query(Company).order_by(Company.name).all(), 
            [
                Company(name='c1', engineers=[JuniorEngineer(name='Ed')]),
                Company(name='c2', engineers=[Engineer(name='Kurt')])
            ]
        )

        # eager load join should limit to only "Engineer"
        sess.expunge_all()
        eq_(sess.query(Company).options(joinedload('engineers')).order_by(Company.name).all(), 
            [
                Company(name='c1', engineers=[JuniorEngineer(name='Ed')]),
                Company(name='c2', engineers=[Engineer(name='Kurt')])
            ]
       )

        # join() to Company.engineers, Employee as the requested entity
        sess.expunge_all()
        eq_(sess.query(Company, Employee).join(Company.engineers).order_by(Company.name).all(),
            [
                (Company(name='c1'), JuniorEngineer(name='Ed')),
                (Company(name='c2'), Engineer(name='Kurt'))
            ]
        )
        
        # join() to Company.engineers, Engineer as the requested entity.
        # this actually applies the IN criterion twice which is less than ideal.
        sess.expunge_all()
        eq_(sess.query(Company, Engineer).join(Company.engineers).order_by(Company.name).all(),
            [
                (Company(name='c1'), JuniorEngineer(name='Ed')),
                (Company(name='c2'), Engineer(name='Kurt'))
            ]
        )

        # join() to Company.engineers without any Employee/Engineer entity
        sess.expunge_all()
        eq_(sess.query(Company).join(Company.engineers).filter(Engineer.name.in_(['Tom', 'Kurt'])).all(),
            [
                Company(name='c2')
            ]
        )

        # this however fails as it does not limit the subtypes to just "Engineer".  
        # with joins constructed by filter(), we seem to be following a policy where
        # we don't try to make decisions on how to join to the target class, whereas when using join() we
        # seem to have a lot more capabilities.
        # we might want to document "advantages of join() vs. straight filtering", or add a large
        # section to "inheritance" laying out all the various behaviors Query has.
        @testing.fails_on_everything_except()
        def go():
            sess.expunge_all()
            eq_(sess.query(Company).\
                filter(Company.company_id==Engineer.company_id).filter(Engineer.name.in_(['Tom', 'Kurt'])).all(),
                [
                    Company(name='c2')
                ]
            )
        go()
        
class SingleOnJoinedTest(MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        global persons_table, employees_table
        
        persons_table = Table('persons', metadata,
           Column('person_id', Integer, primary_key=True, test_needs_autoincrement=True),
           Column('name', String(50)),
           Column('type', String(20), nullable=False)
        )

        employees_table = Table('employees', metadata,
           Column('person_id', Integer, ForeignKey('persons.person_id'),primary_key=True),
           Column('employee_data', String(50)),
           Column('manager_data', String(50)),
        )
    
    def test_single_on_joined(self):
        class Person(_fixtures.Base):
            pass
        class Employee(Person):
            pass
        class Manager(Employee):
            pass
        
        mapper(Person, persons_table, polymorphic_on=persons_table.c.type, polymorphic_identity='person')
        mapper(Employee, employees_table, inherits=Person,polymorphic_identity='engineer')
        mapper(Manager, inherits=Employee,polymorphic_identity='manager')
        
        sess = create_session()
        sess.add(Person(name='p1'))
        sess.add(Employee(name='e1', employee_data='ed1'))
        sess.add(Manager(name='m1', employee_data='ed2', manager_data='md1'))
        sess.flush()
        sess.expunge_all()
        
        eq_(sess.query(Person).order_by(Person.person_id).all(), [
            Person(name='p1'),
            Employee(name='e1', employee_data='ed1'),
            Manager(name='m1', employee_data='ed2', manager_data='md1')
        ])
        sess.expunge_all()

        eq_(sess.query(Employee).order_by(Person.person_id).all(), [
            Employee(name='e1', employee_data='ed1'),
            Manager(name='m1', employee_data='ed2', manager_data='md1')
        ])
        sess.expunge_all()

        eq_(sess.query(Manager).order_by(Person.person_id).all(), [
            Manager(name='m1', employee_data='ed2', manager_data='md1')
        ])
        sess.expunge_all()
        
        def go():
            eq_(sess.query(Person).with_polymorphic('*').order_by(Person.person_id).all(), [
                Person(name='p1'),
                Employee(name='e1', employee_data='ed1'),
                Manager(name='m1', employee_data='ed2', manager_data='md1')
            ])
        self.assert_sql_count(testing.db, go, 1)
    
