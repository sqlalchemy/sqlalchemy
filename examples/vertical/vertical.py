"""this example illustrates a "vertical table".  an object is stored with individual attributes 
represented in distinct database rows.  This allows objects to be created with dynamically changing
fields that are all persisted in a normalized fashion."""

from sqlalchemy import (create_engine, MetaData, Table, Column, Integer, String,
    ForeignKey, PickleType, DateTime, and_)
from sqlalchemy.orm import mapper, relation, sessionmaker, scoped_session
from sqlalchemy.orm.collections import mapped_collection
import datetime

engine = create_engine('sqlite://', echo=False)
meta = MetaData(engine)

Session = scoped_session(sessionmaker())

# represent Entity objects
entities = Table('entities', meta, 
    Column('entity_id', Integer, primary_key=True),
    Column('title', String(100), nullable=False),
    )

# represent named, typed fields
entity_fields = Table('entity_fields', meta,
    Column('field_id', Integer, primary_key=True),
    Column('name', String(40), nullable=False),
    Column('datatype', String(30), nullable=False))
    
# associate a field row with an entity row, including a typed value
entity_values = Table('entity_values', meta, 
    Column('value_id', Integer, primary_key=True),
    Column('field_id', Integer, ForeignKey('entity_fields.field_id'), nullable=False),
    Column('entity_id', Integer, ForeignKey('entities.entity_id'), nullable=False),
    Column('int_value', Integer), 
    Column('string_value', String(500)),
    Column('binary_value', PickleType),
    Column('datetime_value', DateTime))

meta.create_all()

class Entity(object):
    """a persistable dynamic object.  
    
    Marshalls attributes into a dictionary which is 
    mapped to the database.
    
    """
    def __init__(self, **kwargs):
        for k in kwargs:
            setattr(self, k, kwargs[k])
            
    def __getattr__(self, key):
        """Proxy requests for attributes to the underlying _entities dictionary."""

        if key[0] == '_':
            return super(Entity, self).__getattr__(key)
        try:
            return self._entities[key].value
        except KeyError:
            raise AttributeError(key)

    def __setattr__(self, key, value):
        """Proxy requests for attribute set operations to the underlying _entities dictionary."""

        if key[0] == "_" or hasattr(Entity, key):
            object.__setattr__(self, key, value)
            return
            
        try:
            ev = self._entities[key]
            ev.value = value
        except KeyError:
            ev = _EntityValue(key, value)
            self._entities[key] = ev
        
class _EntityField(object):
    """Represents a field of a particular name and datatype."""

    def __init__(self, name, datatype):
        self.name = name
        self.datatype = datatype

class _EntityValue(object):
    """Represents an individual value."""

    def __init__(self, key, value):
        datatype = self._figure_datatype(value)
        field = \
            Session.query(_EntityField).filter(
                and_(_EntityField.name==key, _EntityField.datatype==datatype)
            ).first()

        if not field:
            field = _EntityField(key, datatype)
            Session.add(field)
        
        self.field = field
        setattr(self, self.field.datatype + "_value", value)
    
    def _figure_datatype(self, value):
        typemap = {
            int:'int',
            str:'string',
            datetime.datetime:'datetime',
        }
        for k in typemap:
            if isinstance(value, k):
                return typemap[k]
        else:
            return 'binary'

    def _get_value(self):
        return getattr(self, self.field.datatype + "_value")

    def _set_value(self, value):
        setattr(self, self.field.datatype + "_value", value)
    value = property(_get_value, _set_value)
    
    def name(self):
        return self.field.name
    name = property(name)


# the mappers are a straightforward eager chain of 
# Entity--(1->many)->EntityValue-(many->1)->EntityField
# notice that we are identifying each mapper to its connecting
# relation by just the class itself.
mapper(_EntityField, entity_fields)
mapper(
    _EntityValue, entity_values,
    properties = {
        'field' : relation(_EntityField, lazy=False, cascade='all')
    }
)

mapper(Entity, entities, properties = {
    '_entities' : relation(
                        _EntityValue, 
                        lazy=False, 
                        cascade='all', 
                        collection_class=mapped_collection(lambda entityvalue: entityvalue.field.name)
                    )
})

session = Session()
entity1 = Entity(
    title = 'this is the first entity',
    name = 'this is the name',
    price = 43,
    data = ('hello', 'there')
)

entity2 = Entity(
    title = 'this is the second entity',
    name = 'this is another name',
    price = 50,
    data = ('hoo', 'ha')
)

session.add_all([entity1, entity2])
session.commit()

for entity in session.query(Entity):
    print "Entity id %d:" % entity.entity_id, entity.title, entity.name, entity.price, entity.data

# perform some changes, add a new Entity

entity1.price = 90
entity1.title = 'another new title'
entity2.data = {'oof':5,'lala':8}

entity3 = Entity(
    title = 'third entity',
    name = 'new name',
    price = '$1.95',    # note we change 'price' to be a string.
                        # this creates a new _EntityField separate from the
                        # one used by integer 'price'.
    data = 'some data'
)
session.add(entity3)

session.commit()

print "----------------"
for entity in session.query(Entity):
    print "Entity id %d:" % entity.entity_id, entity.title, entity.name, entity.price, entity.data

print "----------------"
# illustrate each _EntityField that's been created and list each Entity which uses it
for ent_id, name, datatype in session.query(_EntityField.field_id, _EntityField.name, _EntityField.datatype):
    print name, datatype, "(Enitites:",  ",".join([
        str(entid) for entid in session.query(Entity.entity_id).\
            join(
                (_EntityValue, _EntityValue.entity_id==Entity.entity_id), 
                (_EntityField, _EntityField.field_id==_EntityValue.field_id)
            ).filter(_EntityField.field_id==ent_id)
    ]), ")"

# delete all the Entity objects
for entity in session.query(Entity):
    session.delete(entity)
session.commit()
