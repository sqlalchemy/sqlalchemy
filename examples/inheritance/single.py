from sqlalchemy import MetaData, Table, Column, Integer, String, \
    ForeignKey, create_engine
from sqlalchemy.orm import mapper, relationship, sessionmaker

metadata = MetaData()

# a table to store companies
companies = Table('companies', metadata, 
   Column('company_id', Integer, primary_key=True),
   Column('name', String(50)))

employees_table = Table('employees', metadata, 
    Column('employee_id', Integer, primary_key=True),
    Column('company_id', Integer, ForeignKey('companies.company_id')),
    Column('name', String(50)),
    Column('type', String(20)),
    Column('status', String(20)),
    Column('engineer_name', String(50)),
    Column('primary_language', String(50)),
    Column('manager_name', String(50))
)


class Person(object):
    def __init__(self, **kwargs):
        for key, value in kwargs.iteritems():
            setattr(self, key, value)
    def __repr__(self):
        return "Ordinary person %s" % self.name
class Engineer(Person):
    def __repr__(self):
        return "Engineer %s, status %s, engineer_name %s, "\
                    "primary_language %s" % \
                        (self.name, self.status, 
                        self.engineer_name, self.primary_language)
class Manager(Person):
    def __repr__(self):
        return "Manager %s, status %s, manager_name %s" % \
                    (self.name, self.status, self.manager_name)
class Company(object):
    def __init__(self, **kwargs):
        for key, value in kwargs.iteritems():
            setattr(self, key, value)
    def __repr__(self):
        return "Company %s" % self.name

person_mapper = mapper(Person, employees_table,
                       polymorphic_on=employees_table.c.type,
                       polymorphic_identity='person')
manager_mapper = mapper(Manager, inherits=person_mapper,
                        polymorphic_identity='manager')
engineer_mapper = mapper(Engineer, inherits=person_mapper,
                         polymorphic_identity='engineer')

mapper(Company, companies, properties={
    'employees': relationship(Person, lazy=True, backref='company')
})


engine = create_engine('sqlite:///', echo=True)

metadata.create_all(engine)

session = sessionmaker(engine)()

c = Company(name='company1')
c.employees.append(Manager(name='pointy haired boss', status='AAB',
                   manager_name='manager1'))
c.employees.append(Engineer(name='dilbert', status='BBA',
                   engineer_name='engineer1', primary_language='java'))
c.employees.append(Person(name='joesmith', status='HHH'))
c.employees.append(Engineer(name='wally', status='CGG',
                   engineer_name='engineer2', primary_language='python'
                   ))
c.employees.append(Manager(name='jsmith', status='ABA',
                   manager_name='manager2'))
session.add(c)
session.commit()

c = session.query(Company).get(1)
for e in c.employees:
    print e, e.company

print "\n"

dilbert = session.query(Person).filter_by(name='dilbert').one()
dilbert2 = session.query(Engineer).filter_by(name='dilbert').one()
assert dilbert is dilbert2

dilbert.engineer_name = 'hes dibert!'

session.flush()
session.expunge_all()

c = session.query(Company).get(1)
for e in c.employees:
    print e

session.delete(c)
session.commit()
