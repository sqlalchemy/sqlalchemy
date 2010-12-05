"""Mapping a vertical table as a dictionary.

This example illustrates accessing and modifying a "vertical" (or
"properties", or pivoted) table via a dict-like interface.  These are tables
that store free-form object properties as rows instead of columns.  For
example, instead of::

  # A regular ("horizontal") table has columns for 'species' and 'size'
  Table('animal', metadata,
        Column('id', Integer, primary_key=True),
        Column('species', Unicode),
        Column('size', Unicode))

A vertical table models this as two tables: one table for the base or parent
entity, and another related table holding key/value pairs::

  Table('animal', metadata,
        Column('id', Integer, primary_key=True))

  # The properties table will have one row for a 'species' value, and
  # another row for the 'size' value.
  Table('properties', metadata
        Column('animal_id', Integer, ForeignKey('animal.id'),
               primary_key=True),
        Column('key', UnicodeText),
        Column('value', UnicodeText))

Because the key/value pairs in a vertical scheme are not fixed in advance,
accessing them like a Python dict can be very convenient.  The example below
can be used with many common vertical schemas as-is or with minor adaptations.

"""

class VerticalProperty(object):
    """A key/value pair.

    This class models rows in the vertical table.
    """

    def __init__(self, key, value):
        self.key = key
        self.value = value

    def __repr__(self):
        return '<%s %r=%r>' % (self.__class__.__name__, self.key, self.value)


class VerticalPropertyDictMixin(object):
    """Adds obj[key] access to a mapped class.

    This is a mixin class.  It can be inherited from directly, or included
    with multiple inheritence.

    Classes using this mixin must define two class properties::

    _property_type:
      The mapped type of the vertical key/value pair instances.  Will be
      invoked with two positional arugments: key, value

    _property_mapping:
      A string, the name of the Python attribute holding a dict-based
      relationship of _property_type instances.

    Using the VerticalProperty class above as an example,::

      class MyObj(VerticalPropertyDictMixin):
          _property_type = VerticalProperty
          _property_mapping = 'props'

      mapper(MyObj, sometable, properties={
        'props': relationship(VerticalProperty,
                          collection_class=attribute_mapped_collection('key'))})

    Dict-like access to MyObj is proxied through to the 'props' relationship::

      myobj['key'] = 'value'
      # ...is shorthand for:
      myobj.props['key'] = VerticalProperty('key', 'value')

      myobj['key'] = 'updated value']
      # ...is shorthand for:
      myobj.props['key'].value = 'updated value'

      print myobj['key']
      # ...is shorthand for:
      print myobj.props['key'].value

    """

    _property_type = VerticalProperty
    _property_mapping = None

    __map = property(lambda self: getattr(self, self._property_mapping))

    def __getitem__(self, key):
        return self.__map[key].value

    def __setitem__(self, key, value):
        property = self.__map.get(key, None)
        if property is None:
            self.__map[key] = self._property_type(key, value)
        else:
            property.value = value

    def __delitem__(self, key):
        del self.__map[key]

    def __contains__(self, key):
        return key in self.__map

    # Implement other dict methods to taste.  Here are some examples:
    def keys(self):
        return self.__map.keys()

    def values(self):
        return [prop.value for prop in self.__map.values()]

    def items(self):
        return [(key, prop.value) for key, prop in self.__map.items()]

    def __iter__(self):
        return iter(self.keys())


if __name__ == '__main__':
    from sqlalchemy import (MetaData, Table, Column, Integer, Unicode,
        ForeignKey, UnicodeText, and_, not_)
    from sqlalchemy.orm import mapper, relationship, Session
    from sqlalchemy.orm.collections import attribute_mapped_collection

    metadata = MetaData()

    # Here we have named animals, and a collection of facts about them.
    animals = Table('animal', metadata,
                    Column('id', Integer, primary_key=True),
                    Column('name', Unicode(100)))

    facts = Table('facts', metadata,
                  Column('animal_id', Integer, ForeignKey('animal.id'),
                         primary_key=True),
                  Column('key', Unicode(64), primary_key=True),
                  Column('value', UnicodeText, default=None),)

    class AnimalFact(VerticalProperty):
        """A fact about an animal."""

    class Animal(VerticalPropertyDictMixin):
        """An animal.

        Animal facts are available via the 'facts' property or by using
        dict-like accessors on an Animal instance::

          cat['color'] = 'calico'
          # or, equivalently:
          cat.facts['color'] = AnimalFact('color', 'calico')
        """

        _property_type = AnimalFact
        _property_mapping = 'facts'

        def __init__(self, name):
            self.name = name

        def __repr__(self):
            return '<%s %r>' % (self.__class__.__name__, self.name)


    mapper(Animal, animals, properties={
        'facts': relationship(
            AnimalFact, backref='animal',
            collection_class=attribute_mapped_collection('key')),
        })
    mapper(AnimalFact, facts)


    metadata.bind = 'sqlite:///'
    metadata.create_all()
    session = Session()

    stoat = Animal(u'stoat')
    stoat[u'color'] = u'reddish'
    stoat[u'cuteness'] = u'somewhat'

    # dict-like assignment transparently creates entries in the
    # stoat.facts collection:
    print stoat.facts[u'color']

    session.add(stoat)
    session.commit()

    critter = session.query(Animal).filter(Animal.name == u'stoat').one()
    print critter[u'color']
    print critter[u'cuteness']

    critter[u'cuteness'] = u'very'

    print 'changing cuteness:'
    metadata.bind.echo = True
    session.commit()
    metadata.bind.echo = False

    marten = Animal(u'marten')
    marten[u'color'] = u'brown'
    marten[u'cuteness'] = u'somewhat'
    session.add(marten)

    shrew = Animal(u'shrew')
    shrew[u'cuteness'] = u'somewhat'
    shrew[u'poisonous-part'] = u'saliva'
    session.add(shrew)

    loris = Animal(u'slow loris')
    loris[u'cuteness'] = u'fairly'
    loris[u'poisonous-part'] = u'elbows'
    session.add(loris)
    session.commit()

    q = (session.query(Animal).
         filter(Animal.facts.any(
           and_(AnimalFact.key == u'color',
                AnimalFact.value == u'reddish'))))
    print 'reddish animals', q.all()

    # Save some typing by wrapping that up in a function:
    with_characteristic = lambda key, value: and_(AnimalFact.key == key,
                                                  AnimalFact.value == value)

    q = (session.query(Animal).
         filter(Animal.facts.any(
           with_characteristic(u'color', u'brown'))))
    print 'brown animals', q.all()

    q = (session.query(Animal).
           filter(not_(Animal.facts.any(
                         with_characteristic(u'poisonous-part', u'elbows')))))
    print 'animals without poisonous-part == elbows', q.all()

    q = (session.query(Animal).
         filter(Animal.facts.any(AnimalFact.value == u'somewhat')))
    print 'any animal with any .value of "somewhat"', q.all()

    # Facts can be queried as well.
    q = (session.query(AnimalFact).
         filter(with_characteristic(u'cuteness', u'very')))
    print 'just the facts', q.all()


    metadata.drop_all()
