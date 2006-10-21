from sqlalchemy import *
import datetime
import sys

"""this example illustrates a "vertical table".  an object is stored with individual attributes 
represented in distinct database rows.  This allows objects to be created with dynamically changing
fields that are all persisted in a normalized fashion."""
            
e = BoundMetaData('sqlite://', echo=True)

# this table represents Entity objects.  each Entity gets a row in this table,
# with a primary key and a title.
entities = Table('entities', e, 
    Column('entity_id', Integer, primary_key=True),
    Column('title', String(100), nullable=False),
    )

# this table represents dynamic fields that can be associated
# with values attached to an Entity.
# a field has an ID, a name, and a datatype.  
entity_fields = Table('entity_fields', e,
    Column('field_id', Integer, primary_key=True),
    Column('name', String(40), nullable=False),
    Column('datatype', String(30), nullable=False))
    
# this table represents attributes that are attached to an 
# Entity object.  It combines a row from entity_fields with an actual value.
# the value is stored in one of four columns, corresponding to the datatype
# of the field value.
entity_values = Table('entity_values', e, 
    Column('value_id', Integer, primary_key=True),
    Column('field_id', Integer, ForeignKey('entity_fields.field_id'), nullable=False),
    Column('entity_id', Integer, ForeignKey('entities.entity_id'), nullable=False),
    Column('int_value', Integer), 
    Column('string_value', String(500)),
    Column('binary_value', PickleType),
    Column('datetime_value', DateTime))

e.create_all()

class EntityDict(dict):
    """this is a dictionary that implements an append() and an __iter__ method.
    such a dictionary can be used with SQLAlchemy list-based attributes."""
    def append(self, entityvalue):
        self[entityvalue.field.name] = entityvalue
    def __iter__(self):
        return iter(self.values())
    
class Entity(object):
    """represents an Entity.  The __getattr__ method is overridden to search the
    object's _entities dictionary for the appropriate value, and the __setattribute__
    method is overridden to set all non "_" attributes as EntityValues within the 
    _entities dictionary. """

    def __getattr__(self, key):
        """getattr proxies requests for attributes which dont 'exist' on the object
        to the underying _entities dictionary."""
        if key[0] == '_':
            return super(Entity, self).__getattr__(key)
        try:
            return self._entities[key].value
        except KeyError:
            raise AttributeError(key)
    def __setattr__(self, key, value):
        """setattr proxies certain requests to set attributes as EntityValues within
        the _entities dictionary.  This one is tricky as it has to allow underscore attributes,
        as well as attributes managed by the Mapper, to be set by default mechanisms.  Since the 
        mapper uses property-like objects on the Entity class to manage attributes, we check
        for the key as an attribute of the class and if present, default to normal __setattr__
        mechanisms, else we use the special logic which creates EntityValue objects in the
        _entities dictionary."""
        if key[0] == "_" or hasattr(Entity, key):
            object.__setattr__(self, key, value)
            return
        try:
            ev = self._entities[key]
            ev.value = value
        except KeyError:
            ev = EntityValue(key, value)
            self._entities[key] = ev
        
class EntityField(object):
    """this class represents a row in the entity_fields table."""
    def __init__(self, name=None):
        self.name = name
        self.datatype = None

class EntityValue(object):
    """the main job of EntityValue is to hold onto a value, corresponding the type of 
    the value to the underlying datatype of its EntityField."""
    def __init__(self, key=None, value=None):
        if key is not None:
            sess = create_session()
            self.field = sess.query(EntityField).get_by(name=key) or EntityField(key)
            # close the session, which will make a loaded EntityField a detached instance
            sess.close()
            if self.field.datatype is None:
                if isinstance(value, int):
                    self.field.datatype = 'int'
                elif isinstance(value, str):
                    self.field.datatype = 'string'
                elif isinstance(value, datetime.datetime):
                    self.field.datatype = 'datetime'
                else:
                    self.field.datatype = 'binary'
            setattr(self, self.field.datatype + "_value", value)
    def _get_value(self):
        return getattr(self, self.field.datatype + "_value")
    def _set_value(self, value):
        setattr(self, self.field.datatype + "_value", value)
    name = property(lambda s: s.field.name)
    value = property(_get_value, _set_value)

# the mappers are a straightforward eager chain of 
# Entity--(1->many)->EntityValue-(many->1)->EntityField
# notice that we are identifying each mapper to its connecting
# relation by just the class itself.
mapper(EntityField, entity_fields)
mapper(
    EntityValue, entity_values,
    properties = {
        'field' : relation(EntityField, lazy=False, cascade='all')
    }
)

mapper(Entity, entities, properties = {
    '_entities' : relation(EntityValue, lazy=False, cascade='save-update', collection_class=EntityDict)
})

# create two entities.  the objects can be used about as regularly as
# any object can.
session = create_session()
entity = Entity()
entity.title = 'this is the first entity'
entity.name =  'this is the name'
entity.price = 43
entity.data = ('hello', 'there')

entity2 = Entity()
entity2.title = 'this is the second entity'
entity2.name = 'this is another name'
entity2.price = 50
entity2.data = ('hoo', 'ha')

# commit
[session.save(x) for x in (entity, entity2)]
session.flush()

# we would like to illustate loading everything totally clean from 
# the database, so we clear out the session
session.clear()

# select both objects and print
entities = session.query(Entity).select()
for entity in entities:
    print entity.title, entity.name, entity.price, entity.data

# now change some data
entities[0].price=90
entities[0].title = 'another new title'
entities[1].data = {'oof':5,'lala':8}
entity3 = Entity()
entity3.title = 'third entity'
entity3.name = 'new name'
entity3.price = '$1.95'
entity3.data = 'some data'
session.save(entity3)

# commit changes.  the correct rows are updated, nothing else.
session.flush()

# lets see if that one came through.  clear the session, re-select
# and print
session.clear()
entities = session.query(Entity).select()
for entity in entities:
    print entity.title, entity.name, entity.price, entity.data
