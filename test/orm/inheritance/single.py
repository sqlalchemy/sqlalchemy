import testbase
from sqlalchemy import *
from sqlalchemy.orm import *
from testlib import *


class SingleInheritanceTest(AssertMixin):
    def setUpAll(self):
        metadata = MetaData(testbase.db)
        global employees_table
        employees_table = Table('employees', metadata, 
            Column('employee_id', Integer, primary_key=True),
            Column('name', String(50)),
            Column('manager_data', String(50)),
            Column('engineer_info', String(50)),
            Column('type', String(20))
        )
        employees_table.create()
    def tearDownAll(self):
        employees_table.drop()
    def testbasic(self):
        class Employee(object):
            def __init__(self, name):
                self.name = name
            def __repr__(self):
                return self.__class__.__name__ + " " + self.name

        class Manager(Employee):
            def __init__(self, name, manager_data):
                self.name = name
                self.manager_data = manager_data
            def __repr__(self):
                return self.__class__.__name__ + " " + self.name + " " +  self.manager_data

        class Engineer(Employee):
            def __init__(self, name, engineer_info):
                self.name = name
                self.engineer_info = engineer_info
            def __repr__(self):
                return self.__class__.__name__ + " " + self.name + " " +  self.engineer_info

        class JuniorEngineer(Engineer):
            pass

        employee_mapper = mapper(Employee, employees_table, polymorphic_on=employees_table.c.type)
        manager_mapper = mapper(Manager, inherits=employee_mapper, polymorphic_identity='manager')
        engineer_mapper = mapper(Engineer, inherits=employee_mapper, polymorphic_identity='engineer')
        junior_engineer = mapper(JuniorEngineer, inherits=engineer_mapper, polymorphic_identity='juniorengineer')

        session = create_session()

        m1 = Manager('Tom', 'knows how to manage things')
        e1 = Engineer('Kurt', 'knows how to hack')
        e2 = JuniorEngineer('Ed', 'oh that ed')
        session.save(m1)
        session.save(e1)
        session.save(e2)
        session.flush()

        assert session.query(Employee).select() == [m1, e1, e2]
        assert session.query(Engineer).select() == [e1, e2]
        assert session.query(Manager).select() == [m1]
        assert session.query(JuniorEngineer).select() == [e2]
        
if __name__ == '__main__':
    testbase.main()
