"""Illustrates customized class instrumentation, using
the :mod:`sqlalchemy.ext.instrumentation` extension package.

In this example, mapped classes are modified to
store their state in a dictionary attached to an attribute
named "_goofy_dict", instead of using __dict__.
this example illustrates how to replace SQLAlchemy's class
descriptors with a user-defined system.


"""
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, Text,\
    ForeignKey
from sqlalchemy.orm import mapper, relationship, Session

from sqlalchemy.orm.attributes import set_attribute, get_attribute, \
    del_attribute
from sqlalchemy.orm.instrumentation import is_instrumented

from sqlalchemy.ext.instrumentation import InstrumentationManager

class MyClassState(InstrumentationManager):
    def get_instance_dict(self, class_, instance):
        return instance._goofy_dict

    def initialize_instance_dict(self, class_, instance):
        instance.__dict__['_goofy_dict'] = {}

    def install_state(self, class_, instance, state):
        instance.__dict__['_goofy_dict']['state'] = state

    def state_getter(self, class_):
        def find(instance):
            return instance.__dict__['_goofy_dict']['state']
        return find

class MyClass(object):
    __sa_instrumentation_manager__ = MyClassState

    def __init__(self, **kwargs):
        for k in kwargs:
            setattr(self, k, kwargs[k])

    def __getattr__(self, key):
        if is_instrumented(self, key):
            return get_attribute(self, key)
        else:
            try:
                return self._goofy_dict[key]
            except KeyError:
                raise AttributeError(key)

    def __setattr__(self, key, value):
        if is_instrumented(self, key):
            set_attribute(self, key, value)
        else:
            self._goofy_dict[key] = value

    def __delattr__(self, key):
        if is_instrumented(self, key):
            del_attribute(self, key)
        else:
            del self._goofy_dict[key]


if __name__ == '__main__':
    engine = create_engine('sqlite://')
    meta = MetaData()

    table1 = Table('table1', meta,
                    Column('id', Integer, primary_key=True),
                    Column('name', Text))
    table2 = Table('table2', meta,
                    Column('id', Integer, primary_key=True),
                    Column('name', Text),
                    Column('t1id', Integer, ForeignKey('table1.id')))
    meta.create_all(engine)

    class A(MyClass):
        pass

    class B(MyClass):
        pass

    mapper(A, table1, properties={
        'bs': relationship(B)
    })

    mapper(B, table2)

    a1 = A(name='a1', bs=[B(name='b1'), B(name='b2')])

    assert a1.name == 'a1'
    assert a1.bs[0].name == 'b1'

    sess = Session(engine)
    sess.add(a1)

    sess.commit()

    a1 = sess.query(A).get(a1.id)

    assert a1.name == 'a1'
    assert a1.bs[0].name == 'b1'

    a1.bs.remove(a1.bs[0])

    sess.commit()

    a1 = sess.query(A).get(a1.id)
    assert len(a1.bs) == 1
