import sys, time
from sqlalchemy import *
from sqlalchemy.orm import *
from test.lib import profiling

db = create_engine('sqlite://')
metadata = MetaData(db)
Person_table = Table('Person', metadata,
    Column('name', String(40)),
    Column('sex', Integer),
    Column('age', Integer))


def sa_unprofiled_insertmany(n):
    i = Person_table.insert()
    i.execute([{'name':'John Doe','sex':1,'age':35} for j in xrange(n)])

def sqlite_unprofiled_insertmany(n):
    conn = db.connect().connection
    c = conn.cursor()
    persons = [('john doe', 1, 35) for i in xrange(n)]
    c.executemany("insert into Person(name, sex, age) values (?,?,?)", persons)

@profiling.profiled('sa_profiled_insert_many', always=True)
def sa_profiled_insert_many(n):
    i = Person_table.insert()
    i.execute([{'name':'John Doe','sex':1,'age':35} for j in xrange(n)])
    s = Person_table.select()
    r = s.execute()
    res = [[value for value in row] for row in r.fetchall()]

def sqlite_unprofiled_insert(n):
    conn = db.connect().connection
    c = conn.cursor()
    for j in xrange(n):
        c.execute("insert into Person(name, sex, age) values (?,?,?)",
                  ('john doe', 1, 35))

def sa_unprofiled_insert(n):
    # Another option is to build Person_table.insert() outside of the
    # loop. But it doesn't make much of a difference, so might as well
    # use the worst-case/naive version here.
    for j in xrange(n):
        Person_table.insert().execute({'name':'John Doe','sex':1,'age':35})

@profiling.profiled('sa_profiled_insert', always=True)
def sa_profiled_insert(n):
    i = Person_table.insert()
    for j in xrange(n):
        i.execute({'name':'John Doe','sex':1,'age':35})
    s = Person_table.select()
    r = s.execute()
    res = [[value for value in row] for row in r.fetchall()]

def run_timed(fn, label, *args, **kw):
    metadata.drop_all()
    metadata.create_all()

    sys.stdout.write("%s (%s): " % (label, ', '.join([str(a) for a in args])))
    sys.stdout.flush()

    t = time.clock()
    fn(*args, **kw)
    t2 = time.clock()

    sys.stdout.write("%0.2f seconds\n" % (t2 - t))

def run_profiled(fn, label, *args, **kw):
    metadata.drop_all()
    metadata.create_all()

    print "%s (%s)" % (label, ', '.join([str(a) for a in args]))
    fn(*args, **kw)

def all():
    try:
        print "Bulk INSERTS via executemany():\n"

        run_timed(sqlite_unprofiled_insertmany,
                  'pysqlite bulk insert',
                  50000)

        run_timed(sa_unprofiled_insertmany,
                  'SQLAlchemy bulk insert',
                  50000)

        run_profiled(sa_profiled_insert_many,
                     'SQLAlchemy bulk insert/select, profiled',
                     50000)

        print "\nIndividual INSERTS via execute():\n"

        run_timed(sqlite_unprofiled_insert,
                  "pysqlite individual insert",
                  50000)

        run_timed(sa_unprofiled_insert,
                  "SQLAlchemy individual insert",
                  50000)

        run_profiled(sa_profiled_insert,
                     'SQLAlchemy individual insert/select, profiled',
                     50000)

    finally:
        metadata.drop_all()

if __name__ == '__main__':
    all()
