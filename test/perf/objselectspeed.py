import testenv; testenv.simple_setup()
import time, gc, resource
from sqlalchemy import *
from sqlalchemy.orm import *


db = create_engine('sqlite://')
metadata = MetaData(db)
Person_table = Table('Person', metadata,
                     Column('id', Integer, primary_key=True),
                     Column('name', String(40)),
                     Column('sex', Integer),
                     Column('age', Integer))

class RawPerson(object): pass
class Person(object): pass
mapper(Person, Person_table)
compile_mappers()

def setup():
    metadata.create_all()
    i = Person_table.insert()
    data = [{'name':'John Doe','sex':1,'age':35}] * 100
    for j in xrange(500):
        i.execute(data)
    print "Inserted 50,000 rows"

def sqlite_select(entity_cls):
    conn = db.connect().connection
    cr = conn.cursor()
    cr.execute("SELECT id, name, sex, age FROM Person")
    people = []
    for row in cr.fetchall():
        person = entity_cls()
        person.id = row[0]
        person.name = row[1]
        person.sex = row[2]
        person.age = row[3]
        people.append(person)
    cr.close()
    conn.close()

def sql_select(entity_cls):
    people = []
    for row in Person_table.select().execute().fetchall():
        person = entity_cls()
        person.id = row.id
        person.name = row.name
        person.sex = row.sex
        person.age = row.age
        people.append(person)

def orm_select():
    session = create_session()
    people = session.query(Person).all()

def all():
    setup()
    try:
        t, t2 = 0, 0
        def usage(label):
            now = resource.getrusage(resource.RUSAGE_SELF)
            print "%s: %0.3fs real, %0.3fs user, %0.3fs sys" % (
                label, t2 - t,
                now.ru_utime - usage.last.ru_utime,
                now.ru_stime - usage.last.ru_stime)
            usage.snap(now)
        usage.snap = lambda stats=None: setattr(
            usage, 'last', stats or resource.getrusage(resource.RUSAGE_SELF))

        gc.collect()
        usage.snap()
        t = time.clock()
        sqlite_select(RawPerson)
        t2 = time.clock()
        usage('sqlite select/native')

        gc.collect()
        usage.snap()
        t = time.clock()
        sqlite_select(Person)
        t2 = time.clock()
        usage('sqlite select/instrumented')

        gc.collect()
        usage.snap()
        t = time.clock()
        sql_select(RawPerson)
        t2 = time.clock()
        usage('sqlalchemy.sql select/native')

        gc.collect()
        usage.snap()
        t = time.clock()
        sql_select(Person)
        t2 = time.clock()
        usage('sqlalchemy.sql select/instrumented')

        gc.collect()
        usage.snap()
        t = time.clock()
        orm_select()
        t2 = time.clock()
        usage('sqlalchemy.orm fetch')
    finally:
        metadata.drop_all()


if __name__ == '__main__':
    all()
