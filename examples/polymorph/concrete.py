from sqlalchemy import *

metadata = MetaData()

managers_table = Table('managers', metadata, 
    Column('employee_id', Integer, primary_key=True),
    Column('name', String(50)),
    Column('manager_data', String(40))
)

engineers_table = Table('engineers', metadata, 
    Column('employee_id', Integer, primary_key=True),
    Column('name', String(50)),
    Column('engineer_info', String(40))
)

engine = create_engine('sqlite:///', echo=True)
metadata.create_all(engine)


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


session = create_session(bind_to=engine)

m1 = Manager("pointy haired boss", "manager1")
e1 = Engineer("wally", "engineer1")
e2 = Engineer("dilbert", "engineer2")

session.save(m1)
session.save(e1)
session.save(e2)
session.flush()

employees = session.query(Employee).select()
print [e for e in employees]

