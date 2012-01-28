"""this example illustrates how to replace SQLAlchemy's class descriptors with
a user-defined system.

This sort of thing is appropriate for integration with frameworks that
redefine class behaviors in their own way, such that SQLA's default
instrumentation is not compatible.

The example illustrates redefinition of instrumentation at the class level as
well as the collection level, and redefines the storage of the class to store
state within "instance._goofy_dict" instead of "instance.__dict__". Note that
the default collection implementations can be used with a custom attribute
system as well.

"""
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, Text,\
    ForeignKey
from sqlalchemy.orm import mapper, relationship, Session,\
    InstrumentationManager

from sqlalchemy.orm.attributes import set_attribute, get_attribute, \
    del_attribute
from sqlalchemy.orm.instrumentation import is_instrumented
from sqlalchemy.orm.collections import collection_adapter


class MyClassState(InstrumentationManager):
    def __init__(self, cls):
        self.states = {}

    def instrument_attribute(self, class_, key, attr):
        pass

    def install_descriptor(self, class_, key, attr):
        pass

    def uninstall_descriptor(self, class_, key, attr):
        pass

    def instrument_collection_class(self, class_, key, collection_class):
        return MyCollection

    def get_instance_dict(self, class_, instance):
        return instance._goofy_dict

    def initialize_instance_dict(self, class_, instance):
        instance.__dict__['_goofy_dict'] = {}

    def initialize_collection(self, key, state, factory):
        data = factory()
        return MyCollectionAdapter(key, state, data), data

    def install_state(self, class_, instance, state):
        self.states[id(instance)] = state

    def state_getter(self, class_):
        def find(instance):
            return self.states[id(instance)]
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

class MyCollectionAdapter(object):
    """An wholly alternative instrumentation implementation."""

    def __init__(self, key, state, collection):
        self.key = key
        self.state = state
        self.collection = collection
        setattr(collection, '_sa_adapter', self)

    def unlink(self, data):
        setattr(data, '_sa_adapter', None)

    def adapt_like_to_iterable(self, obj):
        return iter(obj)

    def append_with_event(self, item, initiator=None):
        self.collection.add(item, emit=initiator)

    def append_multiple_without_event(self, items):
        self.collection.members.extend(items)

    def append_without_event(self, item):
        self.collection.add(item, emit=False)

    def remove_with_event(self, item, initiator=None):
        self.collection.remove(item, emit=initiator)

    def remove_without_event(self, item):
        self.collection.remove(item, emit=False)

    def clear_with_event(self, initiator=None):
        for item in list(self):
            self.remove_with_event(item, initiator)
    def clear_without_event(self):
        for item in list(self):
            self.remove_without_event(item)
    def __iter__(self):
        return iter(self.collection)

    def fire_append_event(self, item, initiator=None):
        if initiator is not False and item is not None:
            self.state.get_impl(self.key).\
                        fire_append_event(self.state, self.state.dict, item,
                                                        initiator)

    def fire_remove_event(self, item, initiator=None):
        if initiator is not False and item is not None:
            self.state.get_impl(self.key).\
                        fire_remove_event(self.state, self.state.dict, item,
                                                        initiator)

    def fire_pre_remove_event(self, initiator=None):
        self.state.get_impl(self.key).\
                        fire_pre_remove_event(self.state, self.state.dict, 
                                                        initiator)

class MyCollection(object):
    def __init__(self):
        self.members = list()
    def add(self, object, emit=None):
        self.members.append(object)
        collection_adapter(self).fire_append_event(object, emit)
    def remove(self, object, emit=None):
        collection_adapter(self).fire_pre_remove_event(object)
        self.members.remove(object)
        collection_adapter(self).fire_remove_event(object, emit)
    def __getitem__(self, index):
        return self.members[index]
    def __iter__(self):
        return iter(self.members)
    def __len__(self):
        return len(self.members)

if __name__ == '__main__':
    meta = MetaData(create_engine('sqlite://'))

    table1 = Table('table1', meta, 
                    Column('id', Integer, primary_key=True), 
                    Column('name', Text))
    table2 = Table('table2', meta, 
                    Column('id', Integer, primary_key=True), 
                    Column('name', Text), 
                    Column('t1id', Integer, ForeignKey('table1.id')))
    meta.create_all()

    class A(MyClass):
        pass

    class B(MyClass):
        pass

    mapper(A, table1, properties={
        'bs':relationship(B)
    })

    mapper(B, table2)

    a1 = A(name='a1', bs=[B(name='b1'), B(name='b2')])

    assert a1.name == 'a1'
    assert a1.bs[0].name == 'b1'
    assert isinstance(a1.bs, MyCollection)

    sess = Session()
    sess.add(a1)

    sess.commit()

    a1 = sess.query(A).get(a1.id)

    assert a1.name == 'a1'
    assert a1.bs[0].name == 'b1'
    assert isinstance(a1.bs, MyCollection)

    a1.bs.remove(a1.bs[0])

    sess.commit()

    a1 = sess.query(A).get(a1.id)
    assert len(a1.bs) == 1
