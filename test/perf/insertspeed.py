import testbase
from sqlalchemy import *
import time
import hotshot
import hotshot.stats

db = create_engine('sqlite://')
metadata = MetaData(db)
Person_table = Table('Person', metadata,
    Column('name', String(40)),
    Column('sex', Integer),
    Column('age', Integer))


def sa_unprofiled_inserts(n):
    print "Inserting %s records into SQLite(memory) with SQLAlchemy"%n
    i = Person_table.insert()
    i.execute([{'name':'John Doe','sex':1,'age':35} for j in xrange(n)])
    s = Person_table.select()

def sqlite_unprofiled_inserts(n):
    conn = db.connect().connection
    c = conn.cursor()
    persons = [('john doe', 1, 35) for i in xrange(n)]
    c.executemany("insert into Person(name, sex, age) values (?,?,?)", persons)
    
def test_many_inserts(n):
    prof = hotshot.Profile("test_many_inserts.prof")
    prof.start()
    
    print "Inserting %s records into SQLite(memory) with SQLAlchemy"%n
    i = Person_table.insert()
    i.execute([{'name':'John Doe','sex':1,'age':35} for j in xrange(n)])
    s = Person_table.select()
    r = s.execute()
    print "Fetching all rows and columns"
    res = [[value for value in row] for row in r.fetchall()]
    print "Number of records selected: %s\n"%(len(res))

    prof.stop()
    prof.close()
    stats = hotshot.stats.load("test_many_inserts.prof")
    stats.sort_stats('time', 'calls')
    stats.print_stats()

def all():
    metadata.create_all()
    try:
        t = time.clock()
        sqlite_unprofiled_inserts(100000)
        t2 = time.clock()
        print "sqlite unprofiled inserts took %d seconds" % (t2 - t)
        
        Person_table.delete().execute()

        t = time.clock()
        sa_unprofiled_inserts(100000)
        t2 = time.clock()
        print "sqlalchemy unprofiled inserts took %d seconds" % (t2 - t)

        Person_table.delete().execute()

        test_many_inserts(50000)
    finally:
        metadata.drop_all()

if __name__ == '__main__':
    all()
