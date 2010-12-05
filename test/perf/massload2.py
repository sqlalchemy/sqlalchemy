import testenv; testenv.simple_setup()
import time
from sqlalchemy import *
from sqlalchemy.orm import *

metadata = MetaData(create_engine('sqlite://', echo=True))

t1s = Table( 't1s', metadata,
    Column( 'id', Integer, primary_key=True),
    Column('data', String(100))
    )

t2s = Table( 't2s', metadata,
    Column( 'id', Integer, primary_key=True),
    Column( 't1id', Integer, ForeignKey("t1s.id"), nullable=True ))

t3s = Table( 't3s', metadata,
    Column( 'id', Integer, primary_key=True),
    Column( 't2id', Integer, ForeignKey("t2s.id"), nullable=True ))

t4s = Table( 't4s', metadata,
    Column( 'id', Integer, primary_key=True),
    Column( 't3id', Integer, ForeignKey("t3s.id"), nullable=True ))

[t.create() for t in [t1s,t2s,t3s,t4s]]

class T1( object ): pass
class T2( object ): pass
class T3( object ): pass
class T4( object ): pass

mapper( T1, t1s )
mapper( T2, t2s )
mapper( T3, t3s )
mapper( T4, t4s )

cascade = "all, delete-orphan"
use_backref = True

if use_backref:
    class_mapper(T1).add_property( 't2s', relationship(T2, backref=backref("t1", cascade=cascade), cascade=cascade))
    class_mapper(T2).add_property ( 't3s', relationship(T3, backref=backref("t2",cascade=cascade), cascade=cascade) )
    class_mapper(T3).add_property( 't4s', relationship(T4, backref=backref("t3", cascade=cascade), cascade=cascade) )
else:
    T1.mapper.add_property( 't2s', relationship(T2, cascade=cascade))
    T2.mapper.add_property ( 't3s', relationship(T3, cascade=cascade) )
    T3.mapper.add_property( 't4s', relationship(T4, cascade=cascade) )

now = time.time()
print "start"
sess = create_session()
o1 = T1()
sess.save(o1)
for i2 in range(10):
    o2 = T2()
    o1.t2s.append( o2 )

    for i3 in range( 10 ):
        o3 = T3()
        o2.t3s.append( o3 )

        for i4 in range( 10 ):
            o3.t4s.append ( T4() )
            print i2, i3, i4

print len([s for s in sess])
print "flushing"
sess.flush()
total = time.time() - now
print "done,total time", total
