.. _associationproxy:

associationproxy
================

.. module:: sqlalchemy.ext.associationproxy

``associationproxy`` is used to create a simplified, read/write view of a
relationship.  It can be used to cherry-pick fields from a collection of
related objects or to greatly simplify access to associated objects in an
association relationship.

Simplifying Relationships
-------------------------

Consider this "association object" mapping::

    users_table = Table('users', metadata,
        Column('id', Integer, primary_key=True),
        Column('name', String(64)),
    )

    keywords_table = Table('keywords', metadata,
        Column('id', Integer, primary_key=True),
        Column('keyword', String(64))
    )

    userkeywords_table = Table('userkeywords', metadata,
        Column('user_id', Integer, ForeignKey("users.id"),
               primary_key=True),
        Column('keyword_id', Integer, ForeignKey("keywords.id"),
               primary_key=True)
    )

    class User(object):
        def __init__(self, name):
            self.name = name

    class Keyword(object):
        def __init__(self, keyword):
            self.keyword = keyword

    mapper(User, users_table, properties={
        'kw': relationship(Keyword, secondary=userkeywords_table)
        })
    mapper(Keyword, keywords_table)

Above are three simple tables, modeling users, keywords and a many-to-many
relationship between the two.  These ``Keyword`` objects are little more
than a container for a name, and accessing them via the relationship is
awkward::

    user = User('jek')
    user.kw.append(Keyword('cheese inspector'))
    print user.kw
    # [<__main__.Keyword object at 0xb791ea0c>]
    print user.kw[0].keyword
    # 'cheese inspector'
    print [keyword.keyword for keyword in user.kw]
    # ['cheese inspector']

With ``association_proxy`` you have a "view" of the relationship that contains
just the ``.keyword`` of the related objects.  The proxy is a Python
property, and unlike the mapper relationship, is defined in your class::

    from sqlalchemy.ext.associationproxy import association_proxy

    class User(object):
        def __init__(self, name):
            self.name = name

        # proxy the 'keyword' attribute from the 'kw' relationship
        keywords = association_proxy('kw', 'keyword')

    # ...
    >>> user.kw
    [<__main__.Keyword object at 0xb791ea0c>]
    >>> user.keywords
    ['cheese inspector']
    >>> user.keywords.append('snack ninja')
    >>> user.keywords
    ['cheese inspector', 'snack ninja']
    >>> user.kw
    [<__main__.Keyword object at 0x9272a4c>, <__main__.Keyword object at 0xb7b396ec>]

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

 - A ````creator```` function can be used to create instances instead.

Above, the ``Keyword.__init__`` takes a single argument ``keyword``, which
maps conveniently to the value being set through the proxy.  A ``creator``
function could have been used instead if more flexibility was required.

Because the proxies are backed by a regular relationship collection, all of the
usual hooks and patterns for using collections are still in effect.  The
most convenient behavior is the automatic setting of "parent"-type
relationships on assignment.  In the example above, nothing special had to
be done to associate the Keyword to the User.  Simply adding it to the
collection is sufficient.

Simplifying Association Object Relationships
--------------------------------------------

Association proxies are also useful for keeping ``association objects`` out
the way during regular use.  For example, the ``userkeywords`` table
might have a bunch of auditing columns that need to get updated when changes
are made- columns that are updated but seldom, if ever, accessed in your
application.  A proxy can provide a very natural access pattern for the
relationship.

.. sourcecode:: python

    from sqlalchemy.ext.associationproxy import association_proxy

    # users_table and keywords_table tables as above, then:

    def get_current_uid():
        """Return the uid of the current user."""
        return 1  # hardcoded for this example

    userkeywords_table = Table('userkeywords', metadata,
        Column('user_id', Integer, ForeignKey("users.id"), primary_key=True),
        Column('keyword_id', Integer, ForeignKey("keywords.id"), primary_key=True),
        # add some auditing columns
        Column('updated_at', DateTime, default=datetime.now),
        Column('updated_by', Integer, default=get_current_uid, onupdate=get_current_uid),
    )

    def _create_uk_by_keyword(keyword):
        """A creator function."""
        return UserKeyword(keyword=keyword)

    class User(object):
        def __init__(self, name):
            self.name = name
        keywords = association_proxy('user_keywords', 'keyword', creator=_create_uk_by_keyword)

    class Keyword(object):
        def __init__(self, keyword):
            self.keyword = keyword
        def __repr__(self):
            return 'Keyword(%s)' % repr(self.keyword)

    class UserKeyword(object):
        def __init__(self, user=None, keyword=None):
            self.user = user
            self.keyword = keyword

    mapper(User, users_table)
    mapper(Keyword, keywords_table)
    mapper(UserKeyword, userkeywords_table, properties={
        'user': relationship(User, backref='user_keywords'),
        'keyword': relationship(Keyword),
    })

    user = User('log')
    kw1  = Keyword('new_from_blammo')

    # Creating a UserKeyword association object will add a Keyword.
    # the "user" reference assignment in the UserKeyword() constructor
    # populates "user_keywords" via backref.
    UserKeyword(user, kw1)

    # Accessing Keywords requires traversing UserKeywords
    print user.user_keywords[0]
    # <__main__.UserKeyword object at 0xb79bbbec>

    print user.user_keywords[0].keyword
    # Keyword('new_from_blammo')

    # Lots of work.

    # It's much easier to go through the association proxy!
    for kw in (Keyword('its_big'), Keyword('its_heavy'), Keyword('its_wood')):
        user.keywords.append(kw)

    print user.keywords
    # [Keyword('new_from_blammo'), Keyword('its_big'), Keyword('its_heavy'), Keyword('its_wood')]


Building Complex Views
----------------------

.. sourcecode:: python

    stocks_table = Table("stocks", meta,
       Column('symbol', String(10), primary_key=True),
       Column('last_price', Numeric)
    )

    brokers_table = Table("brokers", meta,
       Column('id', Integer,primary_key=True),
       Column('name', String(100), nullable=False)
    )

    holdings_table = Table("holdings", meta,
      Column('broker_id', Integer, ForeignKey('brokers.id'), primary_key=True),
      Column('symbol', String(10), ForeignKey('stocks.symbol'), primary_key=True),
      Column('shares', Integer)
    )

Above are three tables, modeling stocks, their brokers and the number of
shares of a stock held by each broker.  This situation is quite different
from the association example above.  ``shares`` is a *property of the
relationship*, an important one that we need to use all the time.

For this example, it would be very convenient if ``Broker`` objects had a
dictionary collection that mapped ``Stock`` instances to the shares held for
each.  That's easy::

    from sqlalchemy.ext.associationproxy import association_proxy
    from sqlalchemy.orm.collections import attribute_mapped_collection

    def _create_holding(stock, shares):
        """A creator function, constructs Holdings from Stock and share quantity."""
        return Holding(stock=stock, shares=shares)

    class Broker(object):
        def __init__(self, name):
            self.name = name

        holdings = association_proxy('by_stock', 'shares', creator=_create_holding)

    class Stock(object):
        def __init__(self, symbol):
            self.symbol = symbol
            self.last_price = 0

    class Holding(object):
        def __init__(self, broker=None, stock=None, shares=0):
            self.broker = broker
            self.stock = stock
            self.shares = shares

    mapper(Stock, stocks_table)
    mapper(Broker, brokers_table, properties={
        'by_stock': relationship(Holding,
            collection_class=attribute_mapped_collection('stock'))
    })
    mapper(Holding, holdings_table, properties={
        'stock': relationship(Stock),
        'broker': relationship(Broker)
    })

Above, we've set up the ``by_stock`` relationship collection to act as a
dictionary, using the ``.stock`` property of each Holding as a key.

Populating and accessing that dictionary manually is slightly inconvenient
because of the complexity of the Holdings association object::

    stock = Stock('ZZK')
    broker = Broker('paj')

    broker.by_stock[stock] = Holding(broker, stock, 10)
    print broker.by_stock[stock].shares
    # 10

The ``holdings`` proxy we've added to the ``Broker`` class hides the details
of the ``Holding`` while also giving access to ``.shares``::

    for stock in (Stock('JEK'), Stock('STPZ')):
        broker.holdings[stock] = 123

    for stock, shares in broker.holdings.items():
        print stock, shares

    session.add(broker)
    session.commit()
    
    # lets take a peek at that holdings_table after committing changes to the db
    print list(holdings_table.select().execute())
    # [(1, 'ZZK', 10), (1, 'JEK', 123), (1, 'STEPZ', 123)]

Further examples can be found in the ``examples/`` directory in the
SQLAlchemy distribution.

API
---

.. autofunction:: association_proxy

.. autoclass:: AssociationProxy
   :members:
   :undoc-members: