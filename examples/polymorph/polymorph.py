from sqlalchemy import *
import sys

# this example illustrates how to create a relationship to a list of objects,
# where each object in the list has a different type.  The typed objects will
# extend from a common base class, although this same approach can be used
# with 

db = create_engine('sqlite://', echo=True, echo_uow=False)
#db = create_engine('postgres://user=scott&password=tiger&host=127.0.0.1&database=test', echo=True, echo_uow=False)

# a table to store companies
companies = Table('companies', db, 
   Column('company_id', Integer, primary_key=True),
   Column('name', String(50))).create()

# we will define an inheritance relationship between the table "people" and "engineers",
# and a second inheritance relationship between the table "people" and "managers"
people = Table('people', db, 
   Column('person_id', Integer, primary_key=True),
   Column('company_id', Integer, ForeignKey('companies.company_id')),
   Column('name', String(50))).create()
   
engineers = Table('engineers', db, 
   Column('person_id', Integer, ForeignKey('people.person_id'), primary_key=True),
   Column('description', String(50))).create()
   
managers = Table('managers', db, 
   Column('person_id', Integer, ForeignKey('people.person_id'), primary_key=True),
   Column('description', String(50))).create()

  
# create our classes.  The Engineer and Manager classes extend from Person.
class Person(object):
    def __repr__(self):
        return "Ordinary person %s" % self.name
class Engineer(Person):
    def __repr__(self):
        return "Engineer %s, description %s" % (self.name, self.description)
class Manager(Person):
    def __repr__(self):
        return "Manager %s, description %s" % (self.name, self.description)
class Company(object):
    def __repr__(self):
        return "Company %s" % self.name

# next we assign Person mappers.  Since these are the first mappers we are
# creating for these classes, they automatically become the "primary mappers", which
# define the dependency relationships between the classes, so we do a straight
# inheritance setup, i.e. no modifications to how objects are loaded or anything like that.
assign_mapper(Person, people)
assign_mapper(Engineer, engineers, inherits=Person.mapper)
assign_mapper(Manager, managers, inherits=Person.mapper)

# next, we define a query that is going to load Managers and Engineers in one shot.
# we will use a UNION ALL with an extra hardcoded column to indicate the type of object.
# this can also be done via several LEFT OUTER JOINS but a UNION is more appropriate
# since they are distinct result sets.
# The select() statement is also given an alias 'pjoin', since the mapper requires
# that all Selectables have a name.  
#
# TECHNIQUE - when you want to load a certain set of objects from a in one query, all the
# columns in the Selectable must have unique names.  Dont worry about mappers at this point,
# just worry about making a query where if you were to view the results, you could tell everything
# you need to know from each row how to construct an object instance from it.  this is the
# essence of "resultset-based-mapping", which is the core ideology of SQLAlchemy.
#
person_join = select(
                [people, managers.c.description,column("'manager'").label('type')], 
                people.c.person_id==managers.c.person_id).union_all(
            select(
            [people, engineers.c.description, column("'engineer'").label('type')],
            people.c.person_id==engineers.c.person_id)).alias('pjoin')


# lets print out what this Selectable looks like.  The mapper is going to take the selectable and
# Select off of it, with the flag "use_labels" which indicates to prefix column names with the table
# name.  So here is what our mapper will see:
print "Person selectable:", str(person_join.select(use_labels=True)), "\n"


# MapperExtension object.
class PersonLoader(MapperExtension):
    def create_instance(self, mapper, row, imap, class_):
        if row['pjoin_type'] =='engineer':
            return Engineer()
        elif row['pjoin_type'] =='manager':
            return Manager()
        else:
            return Person()
ext = PersonLoader()

# set up the polymorphic mapper, which maps the person_join we set up to
# the Person class, using an instance of PersonLoader.  
people_mapper = mapper(Person, person_join, extension=ext)

# create a mapper for Company.  the 'employees' relationship points to 
# our new people_mapper. 
#
# the dependency relationships which take effect on commit (i.e. the order of 
# inserts/deletes) will be established against the Person class's primary 
# mapper, and when the Engineer and 
# Manager objects are found in the 'employees' list, the primary mappers
# for those subclasses will register 
# themselves as dependent on the Person mapper's save operations.
# (translation: it'll work)
# TODO: get the eager loading to work (the compound select alias doesnt like being aliased itself)
assign_mapper(Company, companies, properties={
    'employees': relation(people_mapper, private=True)
})

c = Company(name='company1')
c.employees.append(Manager(name='pointy haired boss', description='manager1'))
c.employees.append(Engineer(name='dilbert', description='engineer1'))
c.employees.append(Engineer(name='wally', description='engineer2'))
c.employees.append(Manager(name='jsmith', description='manager2'))
objectstore.commit()

objectstore.clear()

c = Company.get(1)
for e in c.employees:
    print e, e._instance_key

print "\n"

dilbert = c.employees[1]
dilbert.description = 'hes dibert!'
objectstore.commit()

objectstore.clear()
c = Company.get(1)
for e in c.employees:
    print e, e._instance_key

objectstore.delete(c)
objectstore.commit()


managers.drop()
engineers.drop()
people.drop()
companies.drop()
