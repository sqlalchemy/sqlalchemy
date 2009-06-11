# load test of connection pool
import thread, time
from sqlalchemy import *
import sqlalchemy.pool as pool
from sqlalchemy.test import testing

db = create_engine(testing.db.url, pool_timeout=30, echo_pool=True)
metadata = MetaData(db)

users_table = Table('users', metadata,
  Column('user_id', Integer, primary_key=True),
  Column('user_name', String(40)),
  Column('password', String(10)))
metadata.drop_all()
metadata.create_all()

users_table.insert().execute([{'user_name':'user#%d' % i, 'password':'pw#%d' % i} for i in range(1000)])

def runfast():
    while True:
        c = db.pool.connect()
        time.sleep(.5)
        c.close()
#        result = users_table.select(limit=100).execute()
#        d = {}
#        for row in result:
#            for col in row.keys():
#                d[col] = row[col]
#        time.sleep(.005)
#        result.close()
        print "runfast cycle complete"

#thread.start_new_thread(runslow, ())
for x in xrange(0,50):
    thread.start_new_thread(runfast, ())

time.sleep(100)
