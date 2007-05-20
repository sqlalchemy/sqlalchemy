from sqlalchemy import *
import testbase

class ConcreteTest1(testbase.ORMTest):
    def define_tables(self, metadata):
        global managers_table, engineers_table
        managers_table = Table('managers', metadata, 
            Column('employee_id', Integer, primary_key=True),
            Column('name', String(50)),
            Column('manager_data', String(50)),
        )

        engineers_table = Table('engineers', metadata, 
            Column('employee_id', Integer, primary_key=True),
            Column('name', String(50)),
            Column('engineer_info', String(50)),
        )

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

        pjoin = polymorphic_union({
            'manager':managers_table,
            'engineer':engineers_table
        }, 'type', 'pjoin')

        employee_mapper = mapper(Employee, pjoin, polymorphic_on=pjoin.c.type)
        manager_mapper = mapper(Manager, managers_table, inherits=employee_mapper, concrete=True, polymorphic_identity='manager')
        engineer_mapper = mapper(Engineer, engineers_table, inherits=employee_mapper, concrete=True, polymorphic_identity='engineer')

        session = create_session()
        session.save(Manager('Tom', 'knows how to manage things'))
        session.save(Engineer('Kurt', 'knows how to hack'))
        session.flush()
        session.clear()

        assert set([repr(x) for x in session.query(Employee).select()]) == set(["Engineer Kurt knows how to hack", "Manager Tom knows how to manage things"])
        assert set([repr(x) for x in session.query(Manager).select()]) == set(["Manager Tom knows how to manage things"])
        assert set([repr(x) for x in session.query(Engineer).select()]) == set(["Engineer Kurt knows how to hack"])

    def testwithrelation(self):
        pass
        
        # TODO: test a self-referential relationship on a concrete polymorphic mapping


if __name__ == '__main__':
    testbase.main()