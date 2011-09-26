"""Mapping a polymorphic-valued vertical table as a dictionary.

This example illustrates accessing and modifying a "vertical" (or
"properties", or pivoted) table via a dict-like interface.  The 'dictlike.py'
example explains the basics of vertical tables and the general approach.  This
example adds a twist- the vertical table holds several "value" columns, one
for each type of data that can be stored.  For example::

  Table('properties', metadata
        Column('owner_id', Integer, ForeignKey('owner.id'),
               primary_key=True),
        Column('key', UnicodeText),
        Column('type', Unicode(16)),
        Column('int_value', Integer),
        Column('char_value', UnicodeText),
        Column('bool_value', Boolean),
        Column('decimal_value', Numeric(10,2)))

For any given properties row, the value of the 'type' column will point to the
'_value' column active for that row.

This example approach uses exactly the same dict mapping approach as the
'dictlike' example.  It only differs in the mapping for vertical rows.  Here,
we'll use a @hybrid_property to build a smart '.value' attribute that wraps up
reading and writing those various '_value' columns and keeps the '.type' up to
date.

Class decorators are used, so Python 2.6 or greater is required.
"""

from sqlalchemy.orm.interfaces import PropComparator
from sqlalchemy.orm import comparable_property
from sqlalchemy.ext.hybrid import hybrid_property

# Using the VerticalPropertyDictMixin from the base example
from dictlike import VerticalPropertyDictMixin

class PolymorphicVerticalProperty(object):
    """A key/value pair with polymorphic value storage.

    Supplies a smart 'value' attribute that provides convenient read/write
    access to the row's current value without the caller needing to worry
    about the 'type' attribute or multiple columns.

    The 'value' attribute can also be used for basic comparisons in queries,
    allowing the row's logical value to be compared without foreknowledge of
    which column it might be in.  This is not going to be a very efficient
    operation on the database side, but it is possible.  If you're mapping to
    an existing database and you have some rows with a value of str('1') and
    others of int(1), then this could be useful.

    Subclasses must provide a 'type_map' class attribute with the following
    form::

      type_map = {
         <python type> : ('type column value', 'column name'),
         # ...
      }

    For example,::

      type_map = {
        int: ('integer', 'integer_value'),
        str: ('varchar', 'varchar_value'),
      }

    Would indicate that a Python int value should be stored in the
    'integer_value' column and the .type set to 'integer'.  Conversely, if the
    value of '.type' is 'integer, then the 'integer_value' column is consulted
    for the current value.
    """

    type_map = {
        type(None): (None, None),
        }

    def __init__(self, key, value=None):
        self.key = key
        self.value = value

    @hybrid_property
    def value(self):
        for discriminator, field in self.type_map.values():
            if self.type == discriminator:
                return getattr(self, field)
        return None

    @value.setter
    def value(self, value):
        py_type = type(value)
        if py_type not in self.type_map:
            raise TypeError(py_type)

        for field_type in self.type_map:
            discriminator, field = self.type_map[field_type]
            field_value = None
            if py_type == field_type:
                self.type = discriminator
                field_value = value
            if field is not None:
                setattr(self, field, field_value)

    @value.deleter
    def value(self):
        self._set_value(None)

    @value.comparator
    class value(PropComparator):
        """A comparator for .value, builds a polymorphic comparison via CASE.

        """
        def __init__(self, cls):
            self.cls = cls

        def _case(self):
            whens = [(text("'%s'" % p[0]), cast(getattr(self.cls, p[1]), String))
                     for p in self.cls.type_map.values()
                     if p[1] is not None]
            return case(whens, self.cls.type, null())
        def __eq__(self, other):
            return self._case() == cast(other, String)
        def __ne__(self, other):
            return self._case() != cast(other, String)

    def __repr__(self):
        return '<%s %r=%r>' % (self.__class__.__name__, self.key, self.value)


if __name__ == '__main__':
    from sqlalchemy import (MetaData, Table, Column, Integer, Unicode,
        ForeignKey, UnicodeText, and_, not_, or_, String, Boolean, cast, text,
        null, case, create_engine)
    from sqlalchemy.orm import mapper, relationship, Session
    from sqlalchemy.orm.collections import attribute_mapped_collection

    metadata = MetaData()

    animals = Table('animal', metadata,
                    Column('id', Integer, primary_key=True),
                    Column('name', Unicode(100)))

    chars = Table('facts', metadata,
                  Column('animal_id', Integer, ForeignKey('animal.id'),
                         primary_key=True),
                  Column('key', Unicode(64), primary_key=True),
                  Column('type', Unicode(16), default=None),
                  Column('int_value', Integer, default=None),
                  Column('char_value', UnicodeText, default=None),
                  Column('boolean_value', Boolean, default=None))

    class AnimalFact(PolymorphicVerticalProperty):
        type_map = {
            int: (u'integer', 'int_value'),
            unicode: (u'char', 'char_value'),
            bool: (u'boolean', 'boolean_value'),
            type(None): (None, None),
            }

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

    mapper(AnimalFact, chars)

    engine = create_engine('sqlite://', echo=True)

    metadata.create_all(engine)
    session = Session(engine)

    stoat = Animal(u'stoat')
    stoat[u'color'] = u'red'
    stoat[u'cuteness'] = 7
    stoat[u'weasel-like'] = True

    session.add(stoat)
    session.commit()

    critter = session.query(Animal).filter(Animal.name == u'stoat').one()
    print critter[u'color']
    print critter[u'cuteness']

    print "changing cuteness value and type:"
    critter[u'cuteness'] = u'very cute'

    session.commit()

    marten = Animal(u'marten')
    marten[u'cuteness'] = 5
    marten[u'weasel-like'] = True
    marten[u'poisonous'] = False
    session.add(marten)

    shrew = Animal(u'shrew')
    shrew[u'cuteness'] = 5
    shrew[u'weasel-like'] = False
    shrew[u'poisonous'] = True

    session.add(shrew)
    session.commit()

    q = (session.query(Animal).
         filter(Animal.facts.any(
           and_(AnimalFact.key == u'weasel-like',
                AnimalFact.value == True))))
    print 'weasel-like animals', q.all()

    # Save some typing by wrapping that up in a function:
    with_characteristic = lambda key, value: and_(AnimalFact.key == key,
                                                  AnimalFact.value == value)

    q = (session.query(Animal).
         filter(Animal.facts.any(
           with_characteristic(u'weasel-like', True))))
    print 'weasel-like animals again', q.all()

    q = (session.query(Animal).
           filter(Animal.facts.any(with_characteristic(u'poisonous', False))))
    print 'animals with poisonous=False', q.all()

    q = (session.query(Animal).
         filter(or_(Animal.facts.any(
                      with_characteristic(u'poisonous', False)),
                    not_(Animal.facts.any(AnimalFact.key == u'poisonous')))))
    print 'non-poisonous animals', q.all()

    q = (session.query(Animal).
         filter(Animal.facts.any(AnimalFact.value == 5)))
    print 'any animal with a .value of 5', q.all()

    # Facts can be queried as well.
    q = (session.query(AnimalFact).
         filter(with_characteristic(u'cuteness', u'very cute')))
    print q.all()

    session.close()
    metadata.drop_all(engine)
