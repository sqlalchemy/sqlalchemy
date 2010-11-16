import time, resource
from sqlalchemy import *
from sqlalchemy.orm import *
from test.lib import *
from test.lib.util import gc_collect


NUM = 100

metadata = MetaData(testing.db)
Person_table = Table('Person', metadata,
                     Column('id', Integer, primary_key=True),
                     Column('name', String(40)),
                     Column('sex', Integer),
                     Column('age', Integer))

Email_table = Table('Email', metadata,
                    Column('id', Integer, primary_key=True),
                    Column('person_id', Integer, ForeignKey('Person.id')),
                    Column('address', String(300)))

class Person(object):
    pass
class Email(object):
    def __repr__(self):
        return '<email %s %s>' % (getattr(self, 'id', None),
                                  getattr(self, 'address', None))

mapper(Person, Person_table, properties={
    'emails': relationship(Email, backref='owner', lazy='joined')
    })
mapper(Email, Email_table)
compile_mappers()

def setup():
    metadata.create_all()
    i = Person_table.insert()
    data = [{'name':'John Doe','sex':1,'age':35}] * NUM
    i.execute(data)

    i = Email_table.insert()
    for j in xrange(1, NUM + 1):
        i.execute(address='foo@bar', person_id=j)
        if j % 2:
            i.execute(address='baz@quux', person_id=j)

    print "Inserted %d rows." % (NUM + NUM + (NUM // 2))

def orm_select(session):
    return session.query(Person).all()

@profiling.profiled('update_and_flush')
def update_and_flush(session, people):
    for p in people:
        p.name = 'Exene Cervenka'
        p.sex = 2
        p.emails[0].address = 'hoho@lala'
    session.flush()

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

        session = create_session()

        gc_collect()
        usage.snap()
        t = time.clock()
        people = orm_select(session)
        t2 = time.clock()
        usage('load objects')

        gc_collect()
        usage.snap()
        t = time.clock()
        update_and_flush(session, people)
        t2 = time.clock()
        usage('update and flush')
    finally:
        metadata.drop_all()


if __name__ == '__main__':
    all()
