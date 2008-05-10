import testenv; testenv.configure_for_tests()
from sqlalchemy import *
from sqlalchemy.orm import *
from testlib import *
from testlib.fixtures import Base

class SingleInheritanceTest(ORMTest):
    def define_tables(self, metadata):
        global employees_table
        employees_table = Table('employees', metadata,
            Column('employee_id', Integer, primary_key=True),
            Column('name', String(50)),
            Column('manager_data', String(50)),
            Column('engineer_info', String(50)),
            Column('type', String(20))
        )

    def test_single_inheritance(self):
        class Employee(Base):
            pass
        class Manager(Employee):
            pass
        class Engineer(Employee):
            pass
        class JuniorEngineer(Engineer):
            pass

        mapper(Employee, employees_table, polymorphic_on=employees_table.c.type)
        mapper(Manager, inherits=Employee, polymorphic_identity='manager')
        mapper(Engineer, inherits=Employee, polymorphic_identity='engineer')
        mapper(JuniorEngineer, inherits=Engineer, polymorphic_identity='juniorengineer')

        session = create_session()

        m1 = Manager(name='Tom', manager_data='knows how to manage things')
        e1 = Engineer(name='Kurt', engineer_info='knows how to hack')
        e2 = JuniorEngineer(name='Ed', engineer_info='oh that ed')
        session.save(m1)
        session.save(e1)
        session.save(e2)
        session.flush()

        assert session.query(Employee).all() == [m1, e1, e2]
        assert session.query(Engineer).all() == [e1, e2]
        assert session.query(Manager).all() == [m1]
        assert session.query(JuniorEngineer).all() == [e2]

class SingleOnJoinedTest(ORMTest):
    def define_tables(self, metadata):
        global persons_table, employees_table
        
        persons_table = Table('persons', metadata,
           Column('person_id', Integer, primary_key=True),
           Column('name', String(50)),
           Column('type', String(20), nullable=False)
        )

        employees_table = Table('employees', metadata,
           Column('person_id', Integer, ForeignKey('persons.person_id'),primary_key=True),
           Column('employee_data', String(50)),
           Column('manager_data', String(50)),
        )
    
    def test_single_on_joined(self):
        class Person(Base):
            pass
        class Employee(Person):
            pass
        class Manager(Employee):
            pass
        
        mapper(Person, persons_table, polymorphic_on=persons_table.c.type, polymorphic_identity='person')
        mapper(Employee, employees_table, inherits=Person,polymorphic_identity='engineer')
        mapper(Manager, inherits=Employee,polymorphic_identity='manager')
        
        sess = create_session()
        sess.save(Person(name='p1'))
        sess.save(Employee(name='e1', employee_data='ed1'))
        sess.save(Manager(name='m1', employee_data='ed2', manager_data='md1'))
        sess.flush()
        sess.clear()
        
        self.assertEquals(sess.query(Person).order_by(Person.person_id).all(), [
            Person(name='p1'),
            Employee(name='e1', employee_data='ed1'),
            Manager(name='m1', employee_data='ed2', manager_data='md1')
        ])
        sess.clear()

        self.assertEquals(sess.query(Employee).order_by(Person.person_id).all(), [
            Employee(name='e1', employee_data='ed1'),
            Manager(name='m1', employee_data='ed2', manager_data='md1')
        ])
        sess.clear()

        self.assertEquals(sess.query(Manager).order_by(Person.person_id).all(), [
            Manager(name='m1', employee_data='ed2', manager_data='md1')
        ])
        sess.clear()
        
        def go():
            self.assertEquals(sess.query(Person).with_polymorphic('*').order_by(Person.person_id).all(), [
                Person(name='p1'),
                Employee(name='e1', employee_data='ed1'),
                Manager(name='m1', employee_data='ed2', manager_data='md1')
            ])
        self.assert_sql_count(testing.db, go, 1)
    
if __name__ == '__main__':
    testenv.main()
