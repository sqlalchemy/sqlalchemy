from sqlalchemy import create_engine, MetaData, Table, Column, Integer, \
    String
from sqlalchemy.orm import mapper, sessionmaker, polymorphic_union

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
        return self.__class__.__name__ + " " + \
                    self.name + " " +  self.manager_data

class Engineer(Employee):
    def __init__(self, name, engineer_info):
        self.name = name
        self.engineer_info = engineer_info
    def __repr__(self):
        return self.__class__.__name__ + " " + \
                    self.name + " " +  self.engineer_info


pjoin = polymorphic_union({
    'manager':managers_table,
    'engineer':engineers_table
}, 'type', 'pjoin')

employee_mapper = mapper(Employee, pjoin, polymorphic_on=pjoin.c.type)
manager_mapper = mapper(Manager, managers_table,
                        inherits=employee_mapper, concrete=True,
                        polymorphic_identity='manager')
engineer_mapper = mapper(Engineer, engineers_table,
                         inherits=employee_mapper, concrete=True,
                         polymorphic_identity='engineer')


session = sessionmaker(engine)()

m1 = Manager("pointy haired boss", "manager1")
e1 = Engineer("wally", "engineer1")
e2 = Engineer("dilbert", "engineer2")

session.add(m1)
session.add(e1)
session.add(e2)
session.commit()

print session.query(Employee).all()

