# this program should open three connections.  then after five seconds, the remaining
# 45 threads should receive a timeout error.  then the program will just stop until
# ctrl-C is pressed.  it should *NOT* open a bunch of new connections.

from sqlalchemy import *
import sqlalchemy.pool as pool
import psycopg2 as psycopg
import thread,time
psycopg = pool.manage(psycopg,pool_size=2,max_overflow=1, timeout=5, echo=True)
print psycopg
db = create_engine('postgres://scott:tiger@127.0.0.1/test',pool=psycopg,strategy='threadlocal')
print db.connection_provider._pool
metadata = MetaData(db)

users_table = Table('users', metadata,
  Column('user_id', Integer, primary_key=True),
  Column('user_name', String(40)),
  Column('password', String(10)))
metadata.create_all()

class User(object):
    pass
usermapper = mapper(User, users_table)

#Then i create loads of threads and in run() of each thread:
def run():
    session = create_session()
    transaction = session.create_transaction()
    query = session.query(User)
    u1=query.select(User.c.user_id==3)
    
for x in range(0,50):
    thread.start_new_thread(run, ())

while True:
    time.sleep(5)
