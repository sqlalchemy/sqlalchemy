# TODO: make unit tests out of all this

####### multiple table

from sqlalchemy import *

#db = create_engine('sqlite:///', echo=True)
db = create_engine('postgres://scott:tiger@127.0.0.1/test', echo=True)
metadata = BoundMetaData(db)

session = create_session()

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


people = Table('people', metadata, 
    Column('person_id', Integer, primary_key=True),
    Column('name', String(50)),
    Column('type', String(30)))

engineers = Table('engineers', metadata, 
    Column('person_id', Integer, ForeignKey('people.person_id'), primary_key=True),
    Column('engineer_info', String(50)),
    )

managers = Table('managers', metadata, 
    Column('person_id', Integer, ForeignKey('people.person_id'), primary_key=True),
    Column('manager_data', String(50)),
    )

people_managers = Table('people_managers', metadata,
    Column('person_id', Integer, ForeignKey("people.person_id")),
    Column('manager_id', Integer, ForeignKey("managers.person_id"))
)

person_join = polymorphic_union( {
    'engineer':people.join(engineers),
    'manager':people.join(managers),
    'person':people.select(people.c.type=='person'),
    }, None, 'pjoin')




person_mapper = mapper(Employee, people, select_table=person_join, polymorphic_on=person_join.c.type, polymorphic_identity='person',
        properties = dict(managers = relation(Manager, secondary=people_managers, lazy=False))
        )



mapper(Engineer, engineers, inherits=person_mapper, polymorphic_identity='engineer')
mapper(Manager, managers, inherits=person_mapper, polymorphic_identity='manager')



def create_some_employees():
    people.create()
    engineers.create()
    managers.create()
    people_managers.create()
    session.save(Manager('Tom', 'knows how to manage things'))
    session.save(Engineer('Kurt', 'knows how to hack'))
    session.flush()
    session.query(Manager).select()
try:
    create_some_employees()
finally:
    metadata.drop_all()


####### concrete table

from sqlalchemy import *

db = create_engine("sqlite:///:memory:")

metadata = BoundMetaData(db)
session = create_session()

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


managers_table = Table('managers', metadata, 
    Column('employee_id', Integer, primary_key=True),
    Column('name', String(50)),
    Column('manager_data', String(50)),
).create()

engineers_table = Table('engineers', metadata, 
    Column('employee_id', Integer, primary_key=True),
    Column('name', String(50)),
    Column('engineer_info', String(50)),
).create()



pjoin = polymorphic_union({
    'manager':managers_table,
    'engineer':engineers_table
}, 'type', 'pjoin')

employee_mapper = mapper(Employee, pjoin, polymorphic_on=pjoin.c.type)
manager_mapper = mapper(Manager, managers_table, inherits=employee_mapper, concrete=True, polymorphic_identity='manager')
engineer_mapper = mapper(Engineer, engineers_table, inherits=employee_mapper, concrete=True, polymorphic_identity='engineer')



session.save(Manager('Tom', 'knows how to manage things'))
session.save(Engineer('Kurt', 'knows how to hack'))
session.flush()


# this gives you [Engineer Kurt knows how to hack, Manager Tom knows how to manage things]
# as it should be
session.query(Employee).select()

# this fails
session.query(Engineer).select()

# this fails
session.query(Manager).select()


############ single table
from sqlalchemy import *

db = create_engine("sqlite:///:memory:")

metadata = BoundMetaData(db)
session = create_session()

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


employees_table = Table('employees', metadata, 
    Column('employee_id', Integer, primary_key=True),
    Column('name', String(50)),
    Column('manager_data', String(50)),
    Column('engineer_info', String(50)),
    Column('type', String(20))
)

employee_mapper = mapper(Employee, employees_table, polymorphic_on=employees_table.c.type)
manager_mapper = mapper(Manager, inherits=employee_mapper, polymorphic_identity='manager')
engineer_mapper = mapper(Engineer, inherits=employee_mapper, polymorphic_identity='engineer')


employees_table.create()

session.save(Manager('Tom', 'knows how to manage things'))
session.save(Engineer('Kurt', 'knows how to hack'))
session.flush()


# this gives you [Engineer Kurt knows how to hack, Manager Tom knows how to manage things]
# as it should be
session.query(Employee).select()

# this gives you [Engineer Kurt knows how to hack, Manager Tom knows how to manage things]
# instead of [Engineer Kurt knows how to hack]
session.query(Engineer).select()


# this gives you [Engineer Kurt knows how to hack, Manager Tom knows how to manage things]
# instead of [Manager Tom knows how to manage things]
session.query(Manager).select()
