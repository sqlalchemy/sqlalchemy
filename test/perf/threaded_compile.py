"""test that mapper compilation is threadsafe, including
when additional mappers are created while the existing
collection is being compiled."""

from sqlalchemy import *
from sqlalchemy.orm import *
import thread, time
from sqlalchemy.orm import mapperlib


meta = MetaData('sqlite:///foo.db')

t1 = Table('t1', meta,
    Column('c1', Integer, primary_key=True),
    Column('c2', String(30))
    )

t2 = Table('t2', meta,
    Column('c1', Integer, primary_key=True),
    Column('c2', String(30)),
    Column('t1c1', None, ForeignKey('t1.c1'))
)
t3 = Table('t3', meta,
    Column('c1', Integer, primary_key=True),
    Column('c2', String(30)),
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

def run1():
    for i in range(50):
        print "T1", thread.get_ident()
        class_mapper(T1)
        time.sleep(.05)

def run2():
    for i in range(50):
        print "T2", thread.get_ident()
        class_mapper(T2)
        time.sleep(.057)

def run3():
    for i in range(50):
        def foo():
            print "FOO", thread.get_ident()
            class Foo(object):pass
            mapper(Foo, t3)
            class_mapper(Foo).compile()
        foo()
        time.sleep(.05)

mapper(T1, t1, properties={'t2':relationship(T2, backref="t1")})
mapper(T2, t2)
print "START"
for j in range(0, 5):
    thread.start_new_thread(run1, ())
    thread.start_new_thread(run2, ())
    thread.start_new_thread(run3, ())
    thread.start_new_thread(run3, ())
    thread.start_new_thread(run3, ())
print "WAIT"
time.sleep(5)
