.. _associationproxy:

Association Proxy
=================

.. module:: sqlalchemy.ext.associationproxy

``associationproxy`` is used to create a read/write view of a
target attribute across a relationship.  It essentially conceals
the usage of a "middle" attribute between two endpoints, and 
can be used to cherry-pick fields from a collection of
related objects or to reduce the verbosity of using the association
object pattern.   Applied creatively, the association proxy allows
the construction of sophisticated collections and dictionary 
views of virtually any geometry, persisted to the database using
standard, transparently configured relational patterns.

Simplifying Scalar Collections
------------------------------

Consider a many-to-many mapping between two classes, ``User`` and ``Keyword``.
Each ``User`` can have any number of ``Keyword`` objects, and vice-versa
(the many-to-many pattern is described at :ref:`relationships_many_to_many`)::

    from sqlalchemy import Column, Integer, String, ForeignKey, Table
    from sqlalchemy.orm import relationship
    from sqlalchemy.ext.declarative import declarative_base

    Base = declarative_base()

    class User(Base):
        __tablename__ = 'user'
        id = Column(Integer, primary_key=True)
        name = Column(String(64))
        kw = relationship("Keyword", secondary=lambda: userkeywords_table)

        def __init__(self, name):
            self.name = name

    class Keyword(Base):
        __tablename__ = 'keyword'
        id = Column(Integer, primary_key=True)
        keyword = Column('keyword', String(64))

        def __init__(self, keyword):
            self.keyword = keyword

    userkeywords_table = Table('userkeywords', Base.metadata,
        Column('user_id', Integer, ForeignKey("user.id"),
               primary_key=True),
        Column('keyword_id', Integer, ForeignKey("keyword.id"),
               primary_key=True)
    )

We can append ``Keyword`` objects to a target ``User``, and access the
``.keyword`` attribute of each ``Keyword`` in order to see the value, but
the extra hop introduced by ``.kw`` can be awkward::

    >>> user = User('jek')
    >>> user.kw.append(Keyword('cheese inspector'))
    >>> print user.kw
    [<__main__.Keyword object at 0x12bf830>]
    >>> print user.kw[0].keyword
    cheese inspector
    >>> print [keyword.keyword for keyword in user.kw]
    ['cheese inspector']

With ``association_proxy`` you have a "view" of the relationship that contains
just the ``.keyword`` of the related objects.  Below we illustrate
how to bridge the gap between the ``kw`` collection and the ``keyword``
attribute of each ``Keyword``::

    from sqlalchemy.ext.associationproxy import association_proxy

    class User(Base):
        __tablename__ = 'user'
        id = Column(Integer, primary_key=True)
        name = Column(String(64))
        kw = relationship("Keyword", secondary=lambda: userkeywords_table)

        def __init__(self, name):
            self.name = name

        # proxy the 'keyword' attribute from the 'kw' relationship
        keywords = association_proxy('kw', 'keyword')

We can now reference the ``.keywords`` collection as a listing of strings,
which is both readable and writeable::

    >>> user = User('jek')
    >>> user.keywords.append('cheese inspector')
    >>> user.keywords
    ['cheese inspector']
    >>> user.keywords.append('snack ninja')
    >>> user.kw
    [<__main__.Keyword object at 0x12cdd30>, <__main__.Keyword object at 0x12cde30>]

The association proxy is nothing more than a Python `descriptor <http://docs.python.org/howto/descriptor.html>`_, 
as opposed to a :class:`.MapperProperty`-based construct such as :func:`.relationship` or :func:`.column_property`.  
It is always defined in a declarative fashion along with its parent class, regardless of 
whether or not Declarative or classical mappings are used.

The proxy is read/write.  New associated objects are created on demand when
values are added to the proxy, and modifying or removing an entry through
the proxy also affects the underlying collection.

 - The association proxy property is backed by a mapper-defined relationship,
   either a collection or scalar.

 - You can access and modify both the proxy and the backing
   relationship. Changes in one are immediate in the other.

 - The proxy acts like the type of the underlying collection.  A list gets a
   list-like proxy, a dict a dict-like proxy, and so on.

 - Multiple proxies for the same relationship are fine.

 - Proxies are lazy, and won't trigger a load of the backing relationship until
   they are accessed.

 - The relationship is inspected to determine the type of the related objects.

 - To construct new instances, the type is called with the value being
   assigned, or key and value for dicts.

 - A "creator" function can be used to create instances instead.

Above, the ``Keyword.__init__`` takes a single argument ``keyword``, which
maps conveniently to the value being set through the proxy.  A ``creator``
function can be used if more flexibility is required.

Because the proxies are backed by a regular relationship collection, all of the
usual hooks and patterns for using collections are still in effect.  The
most convenient behavior is the automatic setting of "parent"-type
relationships on assignment.  In the example above, nothing special had to
be done to associate the ``Keyword`` to the ``User``.  Simply adding it to the
collection is sufficient.

Simplifying Association Proxies
-------------------------------

Association proxies are useful for keeping "association objects" out
the way during regular use.  The "association object" pattern is an
extended form of a many-to-many relationship, and is described at
:ref:`association_pattern`.

Suppose our ``userkeywords`` table above had additional columns
which we'd like to map explicitly, but in most cases we don't 
require direct access to these attributes.  Below, we illustrate
a new mapping which introduces the ``UserKeyword`` class, which 
is mapped to the ``userkeywords`` table illustrated earlier.
This class adds an additional column ``special_key``.   We
create an association proxy on the ``User`` class called
``keywords``, which will bridge the gap from the ``user_keywords``
collection to the ``Keyword`` object referenced by each 
``UserKeyword``::

    from sqlalchemy import Column, Integer, String, ForeignKey
    from sqlalchemy.orm import relationship, backref

    from sqlalchemy.ext.associationproxy import association_proxy
    from sqlalchemy.ext.declarative import declarative_base

    Base = declarative_base()

    class User(Base):
        __tablename__ = 'user'
        id = Column(Integer, primary_key=True)
        name = Column(String(64))
        keywords = association_proxy('user_keywords', 'keyword')

        def __init__(self, name):
            self.name = name

    class Keyword(Base):
        __tablename__ = 'keyword'
        id = Column(Integer, primary_key=True)
        keyword = Column('keyword', String(64))

        def __init__(self, keyword):
            self.keyword = keyword

        def __repr__(self):
            return 'Keyword(%s)' % repr(self.keyword)

    class UserKeyword(Base):
        __tablename__ = 'user_keyword'
        user_id = Column(Integer, ForeignKey('user.id'), primary_key=True)
        keyword_id = Column(Integer, ForeignKey('keyword.id'), primary_key=True)
        special_key = Column(String(50))
        user = relationship(User, backref=backref("user_keywords", cascade="all, delete-orphan"))
        keyword = relationship(Keyword)

        def __init__(self, keyword=None, user=None, special_key=None):
            self.user = user
            self.keyword = keyword
            self.special_key = special_key

With the above mapping, we first illustrate usage of the ``UserKeyword`` class
explicitly, creating a ``User``, ``Keyword``, and the association::

    >>> user = User('log')
    >>> kw1  = Keyword('new_from_blammo')

    >>> # Creating a UserKeyword association object will add a Keyword.
    ... # the "user" reference assignment in the UserKeyword() constructor
    ... # populates "user_keywords" via backref.
    ... UserKeyword(kw1, user)
    <__main__.UserKeyword object at 0x12d9a50>

    >>> # Accessing Keywords requires traversing UserKeywords
    ... print user.user_keywords[0]
    <__main__.UserKeyword object at 0x12d9a50>

    >>> print user.user_keywords[0].keyword
    Keyword('new_from_blammo')

Next we illustrate using the association proxy, which is accessible via
the ``keywords`` attribute on each ``User`` object.  Using the proxy,
the ``UserKeyword`` object is created for us automatically, passing in
the given ``Keyword`` object as the first positional argument by default::

    >>> for kw in (Keyword('its_big'), Keyword('its_heavy'), Keyword('its_wood')):
    ...     user.keywords.append(kw)
    ... 
    >>> print user.keywords
    [Keyword('new_from_blammo'), Keyword('its_big'), Keyword('its_heavy'), Keyword('its_wood')]

In each call to ``keywords.append()``, the association proxy performs the
operation as::

    user.user_keywords.append(UserKeyword(kw))

As each ``UserKeyword`` is added to the ``.user_keywords`` collection, the bidirectional
relationship established between ``User.user_keywords`` and ``UserKeyword.user`` establishes
the parent ``User`` as the current value of ``UserKeyword.user``, and the new ``UserKeyword``
object is fully populated.

Using the creator argument
^^^^^^^^^^^^^^^^^^^^^^^^^^

The above example featured usage of the default "creation" function for the association proxy,
which is to call the constructor of the class mapped by the first attribute, in this case
that of ``UserKeyword``.  It is often necessary to supply a custom construction function
specific to the context in which the association proxy is used.   For example, if
we wanted the ``special_key`` argument to be populated specifically when the 
association proxy collection were used.    The ``creator`` argument is given a single-argument
function to achieve this, often using a lambda for succinctness::

    class User(Base):
        # ...

        keywords = association_proxy('user_keywords', 'keyword', 
                        creator=lambda k:UserKeyword(keyword=k, special_key='special'))

When the proxied collection is based on a Python mapping (e.g. a ``dict``-like object),
the ``creator`` argument accepts a **two** argument callable, passing in the key and value
used in the collection population.  Below we map our ``UserKeyword`` association object
to the ``User`` object using a dictionary interface, where the ``special_key`` attribute
of ``UserKeyword`` is used as the key in this dictionary, and the ``UserKeyword`` as 
the value.  The association proxy with ``creator`` can give us a dictionary of ``special_key``
linked to ``Keyword`` objects::

    from sqlalchemy import Column, Integer, String, ForeignKey
    from sqlalchemy.orm import relationship, backref
    from sqlalchemy.ext.associationproxy import association_proxy
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.orm.collections import attribute_mapped_collection

    Base = declarative_base()

    class User(Base):
        __tablename__ = 'user'
        id = Column(Integer, primary_key=True)
        name = Column(String(64))
        keywords = association_proxy('user_keywords', 'keyword', 
                        creator=lambda k, v:UserKeyword(special_key=k, keyword=v)
                    )

        def __init__(self, name):
            self.name = name

    class Keyword(Base):
        __tablename__ = 'keyword'
        id = Column(Integer, primary_key=True)
        keyword = Column('keyword', String(64))

        def __init__(self, keyword):
            self.keyword = keyword

        def __repr__(self):
            return 'Keyword(%s)' % repr(self.keyword)

    class UserKeyword(Base):
        __tablename__ = 'user_keyword'
        user_id = Column(Integer, ForeignKey('user.id'), primary_key=True)
        keyword_id = Column(Integer, ForeignKey('keyword.id'), primary_key=True)
        special_key = Column(String)
        user = relationship(User, backref=backref(
                                "user_keywords", 
                                collection_class=attribute_mapped_collection("special_key"),
                                cascade="all, delete-orphan"
                                )
                            )
        keyword = relationship(Keyword)

        def __init__(self, keyword=None, user=None, special_key=None):
            self.user = user
            self.keyword = keyword
            self.special_key = special_key

The ``.keywords`` collection is now a dictionary of string keys to ``Keyword`` 
objects::

    >>> user = User('log')
    >>> kw1  = Keyword('new_from_blammo')

    >>> user.keywords['sk1'] = Keyword('kw1')
    >>> user.keywords['sk2'] = Keyword('kw2')

    >>> print user.keywords
    {'sk1': Keyword('kw1'), 'sk2': Keyword('kw2')}

Building Complex Views
----------------------

Given our previous examples of proxying from relationship to scalar
attribute, proxying across an association object, and proxying dictionaries,
we can combine all three techniques together to give ``User`` 
a ``keywords`` dictionary that deals strictly with the string value 
of ``special_key`` mapped to the string ``keyword``.  Both the ``UserKeyword``
and ``Keyword`` classes are entirely concealed.  This is achieved by building
an association proxy on ``User`` that refers to an association proxy
present on ``UserKeyword``::

    from sqlalchemy import Column, Integer, String, ForeignKey
    from sqlalchemy.orm import relationship, backref

    from sqlalchemy.ext.associationproxy import association_proxy
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.orm.collections import attribute_mapped_collection

    Base = declarative_base()

    class User(Base):
        __tablename__ = 'user'
        id = Column(Integer, primary_key=True)
        name = Column(String(64))
        keywords = association_proxy(
                    'user_keywords', 
                    'keyword', 
                    creator=lambda k, v:UserKeyword(special_key=k, keyword=v)
                    )

        def __init__(self, name):
            self.name = name

    class Keyword(Base):
        __tablename__ = 'keyword'
        id = Column(Integer, primary_key=True)
        keyword = Column('keyword', String(64))

        def __init__(self, keyword):
            self.keyword = keyword

    class UserKeyword(Base):
        __tablename__ = 'user_keyword'
        user_id = Column(Integer, ForeignKey('user.id'), primary_key=True)
        keyword_id = Column(Integer, ForeignKey('keyword.id'), primary_key=True)
        special_key = Column(String)
        user = relationship(User, backref=backref(
                    "user_keywords", 
                    collection_class=attribute_mapped_collection("special_key"),
                    cascade="all, delete-orphan"
                    )
                )
        kw = relationship(Keyword)
        keyword = association_proxy('kw', 'keyword')

``.keywords`` is now a dictionary of string to string, where ``UserKeyword`` and ``Keyword`` objects are created and removed
for us transparently using the association proxy, persisted and loaded using the ORM::

    >>> user = User('log')
    >>> user.keywords = {
    ...     'sk1':'kw1',
    ...     'sk2':'kw2'
    ... }
    >>> print user.keywords
    {'sk1': 'kw1', 'sk2': 'kw2'}

    >>> user.keywords['sk3'] = 'kw3'
    >>> del user.keywords['sk2']
    >>> print user.keywords
    {'sk1': 'kw1', 'sk3': 'kw3'}

    >>> print user.user_keywords['sk3'].kw
    <__main__.Keyword object at 0x12ceb90>

Querying with Association Proxies
---------------------------------

The :class:`.AssociationProxy` features simple SQL construction capabilities
which relate down to the underlying :func:`.relationship` in use as well
as the target attribute.  For example, the :meth:`.PropComparator.any`
and :meth:`.PropComparator.has` operations are available for an association
proxy that is specifically proxying two relationships, and will produce
a "nested" EXISTS clause, such as in our basic association object example::

    >>> print session.query(User).filter(User.keywords.any(keyword='jek'))
    SELECT user.id AS user_id, user.name AS user_name 
    FROM user 
    WHERE EXISTS (SELECT 1 
    FROM user_keyword 
    WHERE user.id = user_keyword.user_id AND (EXISTS (SELECT 1 
    FROM keyword 
    WHERE keyword.id = user_keyword.keyword_id AND keyword.keyword = :keyword_1)))

For a proxy to a scalar attribute, ``__eq__()`` is supported::

    >>> print session.query(UserKeyword).filter(UserKeyword.keyword == 'jek')
    SELECT user_keyword.*
    FROM user_keyword 
    WHERE EXISTS (SELECT 1 
        FROM keyword 
        WHERE keyword.id = user_keyword.keyword_id AND keyword.keyword = :keyword_1)

and :meth:`.PropComparator.contains` for proxy to scalar collection::

    >>> print session.query(User).filter(User.keywords.contains('jek'))
    SELECT user.*
    FROM user 
    WHERE EXISTS (SELECT 1 
    FROM userkeywords, keyword 
    WHERE user.id = userkeywords.user_id 
        AND keyword.id = userkeywords.keyword_id 
        AND keyword.keyword = :keyword_1)

:class:`.AssociationProxy` can be used with :meth:`.Query.join` somewhat manually
using the :attr:`~.AssociationProxy.attr` attribute in a star-args context (new in 0.7.3)::

    q = session.query(User).join(*User.keywords)

:attr:`~.AssociationProxy.attr` is composed of :attr:`.AssociationProxy.local_attr` and :attr:`.AssociationProxy.remote_attr`,
which are just synonyms for the actual proxied attributes, and can also
be used for querying (also new in 0.7.3)::

    uka = aliased(UserKeyword)
    ka = aliased(Keyword)
    q = session.query(User).\
            join(uka, User.keywords.local_attr).\
            join(ka, User.keywords.remote_attr)

API Documentation
-----------------

.. autofunction:: association_proxy

.. autoclass:: AssociationProxy
   :members:
   :undoc-members: