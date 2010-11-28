import sqlalchemy as sa
from sqlalchemy import create_engine, MetaData, orm
from sqlalchemy import Column, ForeignKey
from sqlalchemy import Integer, String
from sqlalchemy.orm import mapper
from test.lib import profiling

class Object(object):
    pass

class Q(Object):
    pass

class A(Object):
    pass

class C(Object):
    pass

class WC(C):
    pass

engine = create_engine('sqlite:///:memory:', echo=True)

sm = orm.sessionmaker(bind=engine)

SA_Session = orm.scoped_session(sm)

SA_Metadata = MetaData()

object_table =  sa.Table('Object',
                          SA_Metadata,
                          Column('ObjectID', Integer,primary_key=True),
                          Column('Type', String(1), nullable=False))

q_table = sa.Table('Q',
                   SA_Metadata,
                   Column('QID', Integer, ForeignKey('Object.ObjectID'),primary_key=True))

c_table = sa.Table('C',
                   SA_Metadata,
                   Column('CID', Integer, ForeignKey('Object.ObjectID'),primary_key=True))

wc_table = sa.Table('WC',
                    SA_Metadata,
                    Column('WCID', Integer, ForeignKey('C.CID'), primary_key=True))

a_table = sa.Table('A',
                   SA_Metadata,
                   Column('AID', Integer, ForeignKey('Object.ObjectID'),primary_key=True),
                   Column('QID', Integer, ForeignKey('Q.QID')),
                   Column('CID', Integer, ForeignKey('C.CID')))

mapper(Object, object_table, polymorphic_on=object_table.c.Type, polymorphic_identity='O')

mapper(Q, q_table, inherits=Object, polymorphic_identity='Q')
mapper(C, c_table, inherits=Object, polymorphic_identity='C')
mapper(WC, wc_table, inherits=C, polymorphic_identity='W')

mapper(A, a_table, inherits=Object, polymorphic_identity='A',
       properties = {
                     'Q' : orm.relation(Q,primaryjoin=a_table.c.QID==q_table.c.QID,
                                        backref='As'
                                        ),
                     'C' : orm.relation(C,primaryjoin=a_table.c.CID==c_table.c.CID,
                                        backref='A',
                                        uselist=False)
                     }
       )

SA_Metadata.create_all(engine)

@profiling.profiled('large_flush', always=True, sort=['file'])
def generate_error():
    q = Q()
    for j in range(100): #at 306 the error does not pop out (depending on recursion depth)
        a = A()
        a.Q = q
        a.C = WC()

    SA_Session.add(q)
    SA_Session.commit() #here the error pops out

generate_error()