import testbase
from sqlalchemy import *
from sqlalchemy.orm import *
from testlib import *


db = create_engine('sqlite://')
metadata = MetaData(db)
Person_table = Table('Person', metadata,
    Column('name', String(40)),
    Column('sex', Integer),
    Column('age', Integer))

@profiling.profiled('test_many_inserts', always=True)
def test_many_inserts(n):
    print "Inserting %s records into SQLite(memory) with SQLAlchemy"%n
    i = Person_table.insert()
    i.execute([{'name':'John Doe','sex':1,'age':35} for j in xrange(n)])
    s = Person_table.select()
    r = s.execute()
    print "Number of records selected: %s\n"%(len(r.fetchall()))

def all():
    metadata.create_all()
    try:
        test_many_inserts(100000)
    finally:
        metadata.drop_all()

if __name__ == '__main__':
    all()
