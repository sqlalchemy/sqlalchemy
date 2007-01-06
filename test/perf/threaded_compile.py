# tests the COMPILE_MUTEX in mapper compilation

from sqlalchemy import *
import thread, time, random
from sqlalchemy.orm import mapperlib

meta = BoundMetaData('sqlite:///foo.db')

t1 = Table('t1', meta, 
    Column('c1', Integer, primary_key=True),
    Column('c2', String(30))
    )
    
t2 = Table('t2', meta,
    Column('c1', Integer, primary_key=True),
    Column('c2', String(30)),
    Column('t1c1', None, ForeignKey('t1.c1'))
)
meta.create_all()

class T1(object):
    pass

class T2(object):
    pass
    
class FakeLock(object):
    def acquire(self):pass
    def release(self):pass

# uncomment this to disable the mutex in mapper compilation;
# should produce thread collisions    
#mapperlib._COMPILE_MUTEX = FakeLock()

existing_compile_all = mapperlib.Mapper._compile_all
state = [False]
# decorate mapper's _compile_all() method; the mutex in mapper.compile()
# should insure that this method is only called once by a single thread only
def monkeypatch_compile_all(self):
    if state[0]:
        raise "thread collision"
    state[0] = True
    try:
        print "compile", thread.get_ident()
        time.sleep(1 + random.random())
        existing_compile_all(self)
    finally:
        state[0] = False
mapperlib.Mapper._compile_all = monkeypatch_compile_all

def run1():
    print "T1", thread.get_ident()
    class_mapper(T1)

def run2():
    print "T2", thread.get_ident()
    class_mapper(T2)

for i in range(0,1):
    clear_mappers()
    mapper(T1, t1, properties={'t2':relation(T2, backref="t1")})
    mapper(T2, t2)
    #compile_mappers()
    print "START"
    for j in range(0, 5):
        thread.start_new_thread(run1, ())
        thread.start_new_thread(run2, ())
    print "WAIT"
    time.sleep(5)
    
