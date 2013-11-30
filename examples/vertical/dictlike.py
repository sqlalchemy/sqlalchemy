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
from __future__ import unicode_literals

class ProxiedDictMixin(object):
    """Adds obj[key] access to a mapped class.

    This class basically proxies dictionary access to an attribute
    called ``_proxied``.  The class which inherits this class
    should have an attribute called ``_proxied`` which points to a dictionary.

    """

    def __len__(self):
        return len(self._proxied)

    def __iter__(self):
        return iter(self._proxied)

    def __getitem__(self, key):
        return self._proxied[key]

    def __contains__(self, key):
        return key in self._proxied

    def __setitem__(self, key, value):
        self._proxied[key] = value

    def __delitem__(self, key):
        del self._proxied[key]


if __name__ == '__main__':
    from sqlalchemy import (Column, Integer, Unicode,
        ForeignKey, UnicodeText, and_, create_engine)
    from sqlalchemy.orm import relationship, Session
    from sqlalchemy.orm.collections import attribute_mapped_collection
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.ext.associationproxy import association_proxy

    Base = declarative_base()

    class AnimalFact(Base):
        """A fact about an animal."""

        __tablename__ = 'animal_fact'

        animal_id = Column(ForeignKey('animal.id'), primary_key=True)
        key = Column(Unicode(64), primary_key=True)
        value = Column(UnicodeText)

    class Animal(ProxiedDictMixin, Base):
        """an Animal"""

        __tablename__ = 'animal'

        id = Column(Integer, primary_key=True)
        name = Column(Unicode(100))

        facts = relationship("AnimalFact",
                    collection_class=attribute_mapped_collection('key'))

        _proxied = association_proxy("facts", "value",
                            creator=
                            lambda key, value: AnimalFact(key=key, value=value))

        def __init__(self, name):
            self.name = name

        def __repr__(self):
            return "Animal(%r)" % self.name

        @classmethod
        def with_characteristic(self, key, value):
            return self.facts.any(key=key, value=value)

    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)

    session = Session(bind=engine)

    stoat = Animal('stoat')
    stoat['color'] = 'reddish'
    stoat['cuteness'] = 'somewhat'

    # dict-like assignment transparently creates entries in the
    # stoat.facts collection:
    print(stoat.facts['color'])

    session.add(stoat)
    session.commit()

    critter = session.query(Animal).filter(Animal.name == 'stoat').one()
    print(critter['color'])
    print(critter['cuteness'])

    critter['cuteness'] = 'very'

    print('changing cuteness:')

    marten = Animal('marten')
    marten['color'] = 'brown'
    marten['cuteness'] = 'somewhat'
    session.add(marten)

    shrew = Animal('shrew')
    shrew['cuteness'] = 'somewhat'
    shrew['poisonous-part'] = 'saliva'
    session.add(shrew)

    loris = Animal('slow loris')
    loris['cuteness'] = 'fairly'
    loris['poisonous-part'] = 'elbows'
    session.add(loris)

    q = (session.query(Animal).
         filter(Animal.facts.any(
           and_(AnimalFact.key == 'color',
                AnimalFact.value == 'reddish'))))
    print('reddish animals', q.all())

    q = session.query(Animal).\
            filter(Animal.with_characteristic("color", 'brown'))
    print('brown animals', q.all())

    q = session.query(Animal).\
           filter(~Animal.with_characteristic("poisonous-part", 'elbows'))
    print('animals without poisonous-part == elbows', q.all())

    q = (session.query(Animal).
         filter(Animal.facts.any(value='somewhat')))
    print('any animal with any .value of "somewhat"', q.all())



