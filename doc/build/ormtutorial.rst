.. _ormtutorial_toplevel:

==========================
Object Relational Tutorial
==========================
In this tutorial we will cover a basic SQLAlchemy object-relational mapping scenario, where we store and retrieve Python objects from a database representation.  The tutorial is in doctest format, meaning each ``>>>`` line represents something you can type at a Python command prompt, and the following text represents the expected return value.

Version Check
=============

A quick check to verify that we are on at least **version 0.6** of SQLAlchemy::

    >>> import sqlalchemy
    >>> sqlalchemy.__version__ # doctest:+SKIP
    0.6.0

Connecting
==========

For this tutorial we will use an in-memory-only SQLite database.  To connect we use :func:`~sqlalchemy.create_engine`::

    >>> from sqlalchemy import create_engine
    >>> engine = create_engine('sqlite:///:memory:', echo=True)

The ``echo`` flag is a shortcut to setting up SQLAlchemy logging, which is accomplished via Python's standard ``logging`` module.  With it enabled, we'll see all the generated SQL produced.  If you are working through this tutorial and want less output generated, set it to ``False``.   This tutorial will format the SQL behind a popup window so it doesn't get in our way; just click the "SQL" links to see what's being generated.

Define and Create a Table
==========================
Next we want to tell SQLAlchemy about our tables.  We will start with just a single table called ``users``, which will store records for the end-users using our application (lets assume it's a website).  We define our tables within a catalog called :class:`~sqlalchemy.schema.MetaData`, using the :class:`~sqlalchemy.schema.Table` construct, which is used in a manner similar to SQL's CREATE TABLE syntax::

    >>> from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey
    >>> metadata = MetaData()
    >>> users_table = Table('users', metadata,
    ...     Column('id', Integer, primary_key=True),
    ...     Column('name', String),
    ...     Column('fullname', String),
    ...     Column('password', String)
    ... )

:ref:`metadata_toplevel` covers all about how to define :class:`~sqlalchemy.schema.Table` objects, as well as how to load their definition from an existing database (known as **reflection**).

Next, we can issue CREATE TABLE statements derived from our table metadata, by calling :func:`~sqlalchemy.schema.MetaData.create_all` and passing it the ``engine`` instance which points to our database.  This will check for the presence of a table first before creating, so it's safe to call multiple times:

.. sourcecode:: python+sql

    {sql}>>> metadata.create_all(engine) # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
    PRAGMA table_info("users")
    ()
    CREATE TABLE users (
        id INTEGER NOT NULL,
        name VARCHAR,
        fullname VARCHAR,
        password VARCHAR,
        PRIMARY KEY (id)
    )
    ()
    COMMIT

.. note:: Users familiar with the syntax of CREATE TABLE may notice that the
    VARCHAR columns were generated without a length; on SQLite and Postgresql,
    this is a valid datatype, but on others, it's not allowed. So if running
    this tutorial on one of those databases, and you wish to use SQLAlchemy to
    issue CREATE TABLE, a "length" may be provided to the :class:`~sqlalchemy.types.String` type as
    below::

        Column('name', String(50))

    The length field on :class:`~sqlalchemy.types.String`, as well as similar precision/scale fields
    available on :class:`~sqlalchemy.types.Integer`, :class:`~sqlalchemy.types.Numeric`, etc. are not referenced by
    SQLAlchemy other than when creating tables.

    Additionally, Firebird and Oracle require sequences to generate new
    primary key identifiers, and SQLAlchemy doesn't generate or assume these
    without being instructed. For that, you use the :class:`~sqlalchemy.schema.Sequence` construct::

        from sqlalchemy import Sequence
        Column('id', Integer, Sequence('user_id_seq'), primary_key=True)

    A full, foolproof :class:`~sqlalchemy.schema.Table` is therefore::

        users_table = Table('users', metadata,
           Column('id', Integer, Sequence('user_id_seq'), primary_key=True),
           Column('name', String(50)),
           Column('fullname', String(50)),
           Column('password', String(12))
        )

Define a Python Class to be Mapped
===================================
While the :class:`~sqlalchemy.schema.Table` object defines information about our database, it does not say anything about the definition or behavior of the business objects used by our application;  SQLAlchemy views this as a separate concern.  To correspond to our ``users`` table, let's create a rudimentary ``User`` class.  It only need subclass Python's built-in ``object`` class (i.e. it's a new style class)::

    >>> class User(object):
    ...     def __init__(self, name, fullname, password):
    ...         self.name = name
    ...         self.fullname = fullname
    ...         self.password = password
    ...
    ...     def __repr__(self):
    ...        return "<User('%s','%s', '%s')>" % (self.name, self.fullname, self.password)

The class has an ``__init__()`` and a ``__repr__()`` method for convenience.  These methods are both entirely optional, and can be of any form.  SQLAlchemy never calls ``__init__()`` directly.

Setting up the Mapping
======================
With our ``users_table`` and ``User`` class, we now want to map the two together.  That's where the SQLAlchemy ORM package comes in.  We'll use the ``mapper`` function to create a **mapping** between ``users_table`` and ``User``::

    >>> from sqlalchemy.orm import mapper
    >>> mapper(User, users_table) # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
    <Mapper at 0x...; User>

The ``mapper()`` function creates a new :class:`~sqlalchemy.orm.mapper.Mapper` object and stores it away for future reference, associated with our class.  Let's now create and inspect a ``User`` object::

    >>> ed_user = User('ed', 'Ed Jones', 'edspassword')
    >>> ed_user.name
    'ed'
    >>> ed_user.password
    'edspassword'
    >>> str(ed_user.id)
    'None'

The ``id`` attribute, which while not defined by our ``__init__()`` method, exists due to the ``id`` column present within the ``users_table`` object.  By default, the ``mapper`` creates class attributes for all columns present within the :class:`~sqlalchemy.schema.Table`.  These class attributes exist as Python descriptors, and define **instrumentation** for the mapped class.  The functionality of this instrumentation is very rich and includes the ability to track modifications and automatically load new data from the database when needed.

Since we have not yet told SQLAlchemy to persist ``Ed Jones`` within the database, its id is ``None``.  When we persist the object later, this attribute will be populated with a newly generated value.

Creating Table, Class and Mapper All at Once Declaratively
===========================================================
The preceding approach to configuration involved a
:class:`~sqlalchemy.schema.Table`, a user-defined class, and
a call to``mapper()``.  This illustrates classical SQLAlchemy usage, which values
the highest separation of concerns possible.  
A large number of applications don't require this degree of
separation, and for those SQLAlchemy offers an alternate "shorthand"
configurational style called :mod:`~sqlalchemy.ext.declarative`.  
For many applications, this is the only style of configuration needed.
Our above example using this style is as follows:: 

    >>> from sqlalchemy.ext.declarative import declarative_base

    >>> Base = declarative_base()
    >>> class User(Base):
    ...     __tablename__ = 'users'
    ...
    ...     id = Column(Integer, primary_key=True)
    ...     name = Column(String)
    ...     fullname = Column(String)
    ...     password = Column(String)
    ...
    ...     def __init__(self, name, fullname, password):
    ...         self.name = name
    ...         self.fullname = fullname
    ...         self.password = password
    ...
    ...     def __repr__(self):
    ...        return "<User('%s','%s', '%s')>" % (self.name, self.fullname, self.password)

Above, the :func:`~sqlalchemy.ext.declarative.declarative_base` function defines a new class which
we name ``Base``, from which all of our ORM-enabled classes will
derive.  Note that we define :class:`~sqlalchemy.schema.Column`
objects with no "name" field, since it's inferred from the given
attribute name. 

The underlying :class:`~sqlalchemy.schema.Table` object created by our
:func:`~sqlalchemy.ext.declarative.declarative_base` version of ``User`` is accessible via the
``__table__`` attribute:: 

    >>> users_table = User.__table__

The owning :class:`~sqlalchemy.schema.MetaData` object is available as well::

    >>> metadata = Base.metadata

Full documentation for :mod:`~sqlalchemy.ext.declarative` can be found
in the :doc:`reference/index` section for :doc:`reference/ext/declarative`.

Yet another "declarative" method is available for SQLAlchemy as a third party library called `Elixir <http://elixir.ematia.de/>`_.  This is a full-featured configurational product which also includes many higher level mapping configurations built in.  Like declarative, once classes and mappings are defined, ORM usage is the same as with a classical SQLAlchemy configuration.

Creating a Session
==================

We're now ready to start talking to the database.  The ORM's "handle" to the database is the :class:`~sqlalchemy.orm.session.Session`.  When we first set up the application, at the same level as our :func:`~sqlalchemy.create_engine` statement, we define a :class:`~sqlalchemy.orm.session.Session` class which will serve as a factory for new :class:`~sqlalchemy.orm.session.Session` objects:

.. sourcecode:: python+sql

    >>> from sqlalchemy.orm import sessionmaker
    >>> Session = sessionmaker(bind=engine)

In the case where your application does not yet have an :class:`~sqlalchemy.engine.base.Engine` when you define your module-level objects, just set it up like this:

.. sourcecode:: python+sql

    >>> Session = sessionmaker()

Later, when you create your engine with :func:`~sqlalchemy.create_engine`, connect it to the :class:`~sqlalchemy.orm.session.Session` using ``configure()``:

.. sourcecode:: python+sql

    >>> Session.configure(bind=engine)  # once engine is available

This custom-made :class:`~sqlalchemy.orm.session.Session` class will create new :class:`~sqlalchemy.orm.session.Session` objects which are bound to our database.  Other transactional characteristics may be defined when calling :func:`~sqlalchemy.orm.sessionmaker` as well; these are described in a later chapter.  Then, whenever you need to have a conversation with the database, you instantiate a :class:`~sqlalchemy.orm.session.Session`::

    >>> session = Session()

The above :class:`~sqlalchemy.orm.session.Session` is associated with our SQLite ``engine``, but it hasn't opened any connections yet.  When it's first used, it retrieves a connection from a pool of connections maintained by the ``engine``, and holds onto it until we commit all changes and/or close the session object.

Adding new Objects
==================

To persist our ``User`` object, we ``add()`` it to our :class:`~sqlalchemy.orm.session.Session`::

    >>> ed_user = User('ed', 'Ed Jones', 'edspassword')
    >>> session.add(ed_user)

At this point, the instance is **pending**; no SQL has yet been issued.  The :class:`~sqlalchemy.orm.session.Session` will issue the SQL to persist ``Ed Jones`` as soon as is needed, using a process known as a **flush**.  If we query the database for ``Ed Jones``, all pending information will first be flushed, and the query is issued afterwards.

For example, below we create a new :class:`~sqlalchemy.orm.query.Query` object which loads instances of ``User``.  We "filter by" the ``name`` attribute of ``ed``, and indicate that we'd like only the first result in the full list of rows.  A ``User`` instance is returned which is equivalent to that which we've added:

.. sourcecode:: python+sql

    {sql}>>> our_user = session.query(User).filter_by(name='ed').first() # doctest:+ELLIPSIS,+NORMALIZE_WHITESPACE
    BEGIN
    INSERT INTO users (name, fullname, password) VALUES (?, ?, ?)
    ('ed', 'Ed Jones', 'edspassword')
    SELECT users.id AS users_id, users.name AS users_name, users.fullname AS users_fullname, users.password AS users_password
    FROM users
    WHERE users.name = ?
     LIMIT 1 OFFSET 0
    ('ed',)
    {stop}>>> our_user
    <User('ed','Ed Jones', 'edspassword')>

In fact, the :class:`~sqlalchemy.orm.session.Session` has identified that the row returned is the **same** row as one already represented within its internal map of objects, so we actually got back the identical instance as that which we just added::

    >>> ed_user is our_user
    True

The ORM concept at work here is known as an **identity map** and ensures that all operations upon a particular row within a :class:`~sqlalchemy.orm.session.Session` operate upon the same set of data.  Once an object with a particular primary key is present in the :class:`~sqlalchemy.orm.session.Session`, all SQL queries on that :class:`~sqlalchemy.orm.session.Session` will always return the same Python object for that particular primary key; it also will raise an error if an attempt is made to place a second, already-persisted object with the same primary key within the session.

We can add more ``User`` objects at once using :func:`~sqlalchemy.orm.session.Session.add_all`:

.. sourcecode:: python+sql

    >>> session.add_all([
    ...     User('wendy', 'Wendy Williams', 'foobar'),
    ...     User('mary', 'Mary Contrary', 'xxg527'),
    ...     User('fred', 'Fred Flinstone', 'blah')])

Also, Ed has already decided his password isn't too secure, so lets change it:

.. sourcecode:: python+sql

    >>> ed_user.password = 'f8s7ccs'

The :class:`~sqlalchemy.orm.session.Session` is paying attention.  It knows, for example, that ``Ed Jones`` has been modified:

.. sourcecode:: python+sql

    >>> session.dirty
    IdentitySet([<User('ed','Ed Jones', 'f8s7ccs')>])

and that three new ``User`` objects are pending:

.. sourcecode:: python+sql

    >>> session.new  # doctest: +NORMALIZE_WHITESPACE
    IdentitySet([<User('wendy','Wendy Williams', 'foobar')>,
    <User('mary','Mary Contrary', 'xxg527')>,
    <User('fred','Fred Flinstone', 'blah')>])

We tell the :class:`~sqlalchemy.orm.session.Session` that we'd like to issue all remaining changes to the database and commit the transaction, which has been in progress throughout.  We do this via ``commit()``:

.. sourcecode:: python+sql

    {sql}>>> session.commit()
    UPDATE users SET password=? WHERE users.id = ?
    ('f8s7ccs', 1)
    INSERT INTO users (name, fullname, password) VALUES (?, ?, ?)
    ('wendy', 'Wendy Williams', 'foobar')
    INSERT INTO users (name, fullname, password) VALUES (?, ?, ?)
    ('mary', 'Mary Contrary', 'xxg527')
    INSERT INTO users (name, fullname, password) VALUES (?, ?, ?)
    ('fred', 'Fred Flinstone', 'blah')
    COMMIT

``commit()`` flushes whatever remaining changes remain to the database, and commits the transaction.  The connection resources referenced by the session are now returned to the connection pool.  Subsequent operations with this session will occur in a **new** transaction, which will again re-acquire connection resources when first needed.

If we look at Ed's ``id`` attribute, which earlier was ``None``, it now has a value:

.. sourcecode:: python+sql

    {sql}>>> ed_user.id # doctest: +NORMALIZE_WHITESPACE
    BEGIN
    SELECT users.id AS users_id, users.name AS users_name, users.fullname AS users_fullname, users.password AS users_password
    FROM users
    WHERE users.id = ?
    (1,)
    {stop}1

After the :class:`~sqlalchemy.orm.session.Session` inserts new rows in the database, all newly generated identifiers and database-generated defaults become available on the instance, either immediately or via load-on-first-access.  In this case, the entire row was re-loaded on access because a new transaction was begun after we issued ``commit()``.  SQLAlchemy by default refreshes data from a previous transaction the first time it's accessed within a new transaction, so that the most recent state is available.  The level of reloading is configurable as is described in the chapter on Sessions.

Rolling Back
============
Since the :class:`~sqlalchemy.orm.session.Session` works within a transaction, we can roll back changes made too.   Let's make two changes that we'll revert; ``ed_user``'s user name gets set to ``Edwardo``:

.. sourcecode:: python+sql

    >>> ed_user.name = 'Edwardo'

and we'll add another erroneous user, ``fake_user``:

.. sourcecode:: python+sql

    >>> fake_user = User('fakeuser', 'Invalid', '12345')
    >>> session.add(fake_user)

Querying the session, we can see that they're flushed into the current transaction:

.. sourcecode:: python+sql

    {sql}>>> session.query(User).filter(User.name.in_(['Edwardo', 'fakeuser'])).all() #doctest: +NORMALIZE_WHITESPACE
    UPDATE users SET name=? WHERE users.id = ?
    ('Edwardo', 1)
    INSERT INTO users (name, fullname, password) VALUES (?, ?, ?)
    ('fakeuser', 'Invalid', '12345')
    SELECT users.id AS users_id, users.name AS users_name, users.fullname AS users_fullname, users.password AS users_password
    FROM users
    WHERE users.name IN (?, ?)
    ('Edwardo', 'fakeuser')
    {stop}[<User('Edwardo','Ed Jones', 'f8s7ccs')>, <User('fakeuser','Invalid', '12345')>]

Rolling back, we can see that ``ed_user``'s name is back to ``ed``, and ``fake_user`` has been kicked out of the session:

.. sourcecode:: python+sql

    {sql}>>> session.rollback()
    ROLLBACK
    {stop}

    {sql}>>> ed_user.name #doctest: +NORMALIZE_WHITESPACE
    BEGIN
    SELECT users.id AS users_id, users.name AS users_name, users.fullname AS users_fullname, users.password AS users_password
    FROM users
    WHERE users.id = ?
    (1,)
    {stop}u'ed'
    >>> fake_user in session
    False

issuing a SELECT illustrates the changes made to the database:

.. sourcecode:: python+sql

    {sql}>>> session.query(User).filter(User.name.in_(['ed', 'fakeuser'])).all() #doctest: +NORMALIZE_WHITESPACE
    SELECT users.id AS users_id, users.name AS users_name, users.fullname AS users_fullname, users.password AS users_password
    FROM users
    WHERE users.name IN (?, ?)
    ('ed', 'fakeuser')
    {stop}[<User('ed','Ed Jones', 'f8s7ccs')>]

.. _ormtutorial_querying:

Querying
========

A :class:`~sqlalchemy.orm.query.Query` is created using the :class:`~sqlalchemy.orm.session.Session.query()` function on :class:`~sqlalchemy.orm.session.Session`.  This function takes a variable number of arguments, which can be any combination of classes and class-instrumented descriptors.  Below, we indicate a :class:`~sqlalchemy.orm.query.Query` which loads ``User`` instances.  When evaluated in an iterative context, the list of ``User`` objects present is returned:

.. sourcecode:: python+sql

    {sql}>>> for instance in session.query(User).order_by(User.id): # doctest: +NORMALIZE_WHITESPACE
    ...     print instance.name, instance.fullname
    SELECT users.id AS users_id, users.name AS users_name,
    users.fullname AS users_fullname, users.password AS users_password
    FROM users ORDER BY users.id
    ()
    {stop}ed Ed Jones
    wendy Wendy Williams
    mary Mary Contrary
    fred Fred Flinstone

The :class:`~sqlalchemy.orm.query.Query` also accepts ORM-instrumented descriptors as arguments.  Any time multiple class entities or column-based entities are expressed as arguments to the :class:`~sqlalchemy.orm.session.Session.query()` function, the return result is expressed as tuples:

.. sourcecode:: python+sql

    {sql}>>> for name, fullname in session.query(User.name, User.fullname): # doctest: +NORMALIZE_WHITESPACE
    ...     print name, fullname
    SELECT users.name AS users_name, users.fullname AS users_fullname
    FROM users
    ()
    {stop}ed Ed Jones
    wendy Wendy Williams
    mary Mary Contrary
    fred Fred Flinstone

The tuples returned by :class:`~sqlalchemy.orm.query.Query` are *named* tuples, and can be treated much like an ordinary Python object.  The names are the same as the attribute's name for an attribute, and the class name for a class:

.. sourcecode:: python+sql

    {sql}>>> for row in session.query(User, User.name).all(): #doctest: +NORMALIZE_WHITESPACE
    ...    print row.User, row.name
    SELECT users.id AS users_id, users.name AS users_name, users.fullname AS users_fullname, users.password AS users_password
    FROM users
    ()
    {stop}<User('ed','Ed Jones', 'f8s7ccs')> ed
    <User('wendy','Wendy Williams', 'foobar')> wendy
    <User('mary','Mary Contrary', 'xxg527')> mary
    <User('fred','Fred Flinstone', 'blah')> fred

You can control the names using the ``label()`` construct for scalar attributes and ``aliased()`` for class constructs:

.. sourcecode:: python+sql

    >>> from sqlalchemy.orm import aliased
    >>> user_alias = aliased(User, name='user_alias')
    {sql}>>> for row in session.query(user_alias, user_alias.name.label('name_label')).all(): #doctest: +NORMALIZE_WHITESPACE
    ...    print row.user_alias, row.name_label
    SELECT users_1.id AS users_1_id, users_1.name AS users_1_name, users_1.fullname AS users_1_fullname, users_1.password AS users_1_password, users_1.name AS name_label
    FROM users AS users_1
    (){stop}
    <User('ed','Ed Jones', 'f8s7ccs')> ed
    <User('wendy','Wendy Williams', 'foobar')> wendy
    <User('mary','Mary Contrary', 'xxg527')> mary
    <User('fred','Fred Flinstone', 'blah')> fred

Basic operations with :class:`~sqlalchemy.orm.query.Query` include issuing LIMIT and OFFSET, most conveniently using Python array slices and typically in conjunction with ORDER BY:

.. sourcecode:: python+sql

    {sql}>>> for u in session.query(User).order_by(User.id)[1:3]: #doctest: +NORMALIZE_WHITESPACE
    ...    print u
    SELECT users.id AS users_id, users.name AS users_name, users.fullname AS users_fullname, users.password AS users_password
    FROM users ORDER BY users.id
    LIMIT 2 OFFSET 1
    (){stop}
    <User('wendy','Wendy Williams', 'foobar')>
    <User('mary','Mary Contrary', 'xxg527')>

and filtering results, which is accomplished either with :func:`~sqlalchemy.orm.query.Query.filter_by`, which uses keyword arguments:

.. sourcecode:: python+sql

    {sql}>>> for name, in session.query(User.name).filter_by(fullname='Ed Jones'): # doctest: +NORMALIZE_WHITESPACE
    ...    print name
    SELECT users.name AS users_name FROM users
    WHERE users.fullname = ?
    ('Ed Jones',)
    {stop}ed

...or :func:`~sqlalchemy.orm.query.Query.filter`, which uses more flexible SQL expression language constructs.  These allow you to use regular Python operators with the class-level attributes on your mapped class:

.. sourcecode:: python+sql

    {sql}>>> for name, in session.query(User.name).filter(User.fullname=='Ed Jones'): # doctest: +NORMALIZE_WHITESPACE
    ...    print name
    SELECT users.name AS users_name FROM users
    WHERE users.fullname = ?
    ('Ed Jones',)
    {stop}ed

The :class:`~sqlalchemy.orm.query.Query` object is fully *generative*, meaning that most method calls return a new :class:`~sqlalchemy.orm.query.Query` object upon which further criteria may be added.  For example, to query for users named "ed" with a full name of "Ed Jones", you can call :func:`~sqlalchemy.orm.query.Query.filter` twice, which joins criteria using ``AND``:

.. sourcecode:: python+sql

    {sql}>>> for user in session.query(User).filter(User.name=='ed').filter(User.fullname=='Ed Jones'): # doctest: +NORMALIZE_WHITESPACE
    ...    print user
    SELECT users.id AS users_id, users.name AS users_name, users.fullname AS users_fullname, users.password AS users_password
    FROM users
    WHERE users.name = ? AND users.fullname = ?
    ('ed', 'Ed Jones')
    {stop}<User('ed','Ed Jones', 'f8s7ccs')>


Common Filter Operators
-----------------------

Here's a rundown of some of the most common operators used in :func:`~sqlalchemy.orm.query.Query.filter`:

* equals::

    query.filter(User.name == 'ed')

* not equals::

    query.filter(User.name != 'ed')

* LIKE::

    query.filter(User.name.like('%ed%'))

* IN::

    query.filter(User.name.in_(['ed', 'wendy', 'jack']))

    # works with query objects too:

    query.filter(User.name.in_(session.query(User.name).filter(User.name.like('%ed%'))))

* NOT IN::

    query.filter(~User.name.in_(['ed', 'wendy', 'jack']))

* IS NULL::

    filter(User.name == None)

* IS NOT NULL::

    filter(User.name != None)

* AND::

    from sqlalchemy import and_
    filter(and_(User.name == 'ed', User.fullname == 'Ed Jones'))

    # or call filter()/filter_by() multiple times
    filter(User.name == 'ed').filter(User.fullname == 'Ed Jones')

* OR::

    from sqlalchemy import or_
    filter(or_(User.name == 'ed', User.name == 'wendy'))

* match::

    query.filter(User.name.match('wendy'))

 The contents of the match parameter are database backend specific.

Returning Lists and Scalars
---------------------------

The :meth:`~sqlalchemy.orm.query.Query.all()`, :meth:`~sqlalchemy.orm.query.Query.one()`, and :meth:`~sqlalchemy.orm.query.Query.first()` methods of :class:`~sqlalchemy.orm.query.Query` immediately issue SQL and return a non-iterator value.  :meth:`~sqlalchemy.orm.query.Query.all()` returns a list:

.. sourcecode:: python+sql

    >>> query = session.query(User).filter(User.name.like('%ed')).order_by(User.id)
    {sql}>>> query.all() #doctest: +NORMALIZE_WHITESPACE
    SELECT users.id AS users_id, users.name AS users_name, users.fullname AS users_fullname, users.password AS users_password
    FROM users
    WHERE users.name LIKE ? ORDER BY users.id
    ('%ed',)
    {stop}[<User('ed','Ed Jones', 'f8s7ccs')>, <User('fred','Fred Flinstone', 'blah')>]

:meth:`~sqlalchemy.orm.query.Query.first()` applies a limit of one and returns the first result as a scalar:

.. sourcecode:: python+sql

    {sql}>>> query.first() #doctest: +NORMALIZE_WHITESPACE
    SELECT users.id AS users_id, users.name AS users_name, users.fullname AS users_fullname, users.password AS users_password
    FROM users
    WHERE users.name LIKE ? ORDER BY users.id
     LIMIT 1 OFFSET 0
    ('%ed',)
    {stop}<User('ed','Ed Jones', 'f8s7ccs')>

:meth:`~sqlalchemy.orm.query.Query.one()`, fully fetches all rows, and if not exactly one object identity or composite row is present in the result, raises an error:

.. sourcecode:: python+sql

    {sql}>>> from sqlalchemy.orm.exc import MultipleResultsFound
    >>> try: #doctest: +NORMALIZE_WHITESPACE
    ...     user = query.one()
    ... except MultipleResultsFound, e:
    ...     print e
    SELECT users.id AS users_id, users.name AS users_name, users.fullname AS users_fullname, users.password AS users_password
    FROM users
    WHERE users.name LIKE ? ORDER BY users.id
    ('%ed',)
    {stop}Multiple rows were found for one()

.. sourcecode:: python+sql

    {sql}>>> from sqlalchemy.orm.exc import NoResultFound
    >>> try: #doctest: +NORMALIZE_WHITESPACE
    ...     user = query.filter(User.id == 99).one()
    ... except NoResultFound, e:
    ...     print e
    SELECT users.id AS users_id, users.name AS users_name, users.fullname AS users_fullname, users.password AS users_password
    FROM users
    WHERE users.name LIKE ? AND users.id = ? ORDER BY users.id
    ('%ed', 99)
    {stop}No row was found for one()

Using Literal SQL
-----------------

Literal strings can be used flexibly with :class:`~sqlalchemy.orm.query.Query`.  Most methods accept strings in addition to SQLAlchemy clause constructs.  For example, :meth:`~sqlalchemy.orm.query.Query.filter()` and :meth:`~sqlalchemy.orm.query.Query.order_by()`:

.. sourcecode:: python+sql

    {sql}>>> for user in session.query(User).filter("id<224").order_by("id").all(): #doctest: +NORMALIZE_WHITESPACE
    ...     print user.name
    SELECT users.id AS users_id, users.name AS users_name, users.fullname AS users_fullname, users.password AS users_password
    FROM users
    WHERE id<224 ORDER BY id
    ()
    {stop}ed
    wendy
    mary
    fred

Bind parameters can be specified with string-based SQL, using a colon.  To specify the values, use the :meth:`~sqlalchemy.orm.query.Query.params()` method:

.. sourcecode:: python+sql

    {sql}>>> session.query(User).filter("id<:value and name=:name").\
    ...     params(value=224, name='fred').order_by(User.id).one() # doctest: +NORMALIZE_WHITESPACE
    SELECT users.id AS users_id, users.name AS users_name, users.fullname AS users_fullname, users.password AS users_password
    FROM users
    WHERE id<? and name=? ORDER BY users.id
    (224, 'fred')
    {stop}<User('fred','Fred Flinstone', 'blah')>

To use an entirely string-based statement, using :meth:`~sqlalchemy.orm.query.Query.from_statement()`; just ensure that the columns clause of the statement contains the column names normally used by the mapper (below illustrated using an asterisk):

.. sourcecode:: python+sql

    {sql}>>> session.query(User).from_statement("SELECT * FROM users where name=:name").params(name='ed').all()
    SELECT * FROM users where name=?
    ('ed',)
    {stop}[<User('ed','Ed Jones', 'f8s7ccs')>]

You can use :meth:`~sqlalchemy.orm.query.Query.from_statement()` to go completely "raw", using string names to identify desired columns:

.. sourcecode:: python+sql

    {sql}>>> session.query("id", "name", "thenumber12").from_statement("SELECT id, name, 12 as thenumber12 FROM users where name=:name").params(name='ed').all()
    SELECT id, name, 12 as thenumber12 FROM users where name=?
    ('ed',)
    {stop}[(1, u'ed', 12)]

Counting
--------

:class:`~sqlalchemy.orm.query.Query` includes a convenience method for counting called :meth:`~sqlalchemy.orm.query.Query.count()`:

.. sourcecode:: python+sql

    {sql}>>> session.query(User).filter(User.name.like('%ed')).count() #doctest: +NORMALIZE_WHITESPACE
    SELECT count(1) AS count_1
    FROM users
    WHERE users.name LIKE ?
    ('%ed',)
    {stop}2

The :meth:`~sqlalchemy.orm.query.Query.count()` method is used to determine how many rows the SQL statement would return, and is mainly intended to return a simple count of a single type of entity, in this case ``User``.   For more complicated sets of columns or entities where the "thing to be counted" needs to be indicated more specifically, :meth:`~sqlalchemy.orm.query.Query.count()` is probably not what you want.  Below, a query for individual columns does return the expected result:

.. sourcecode:: python+sql

    {sql}>>> session.query(User.id, User.name).filter(User.name.like('%ed')).count() #doctest: +NORMALIZE_WHITESPACE
    SELECT count(1) AS count_1
    FROM (SELECT users.id AS users_id, users.name AS users_name
    FROM users
    WHERE users.name LIKE ?) AS anon_1
    ('%ed',)
    {stop}2

...but if you look at the generated SQL, SQLAlchemy saw that we were placing individual column expressions and decided to wrap whatever it was we were doing in a subquery, so as to be assured that it returns the "number of rows".   This defensive behavior is not really needed here and in other cases is not what we want at all, such as if we wanted a grouping of counts per name:

.. sourcecode:: python+sql

    {sql}>>> session.query(User.name).group_by(User.name).count()  #doctest: +NORMALIZE_WHITESPACE
    SELECT count(1) AS count_1
    FROM (SELECT users.name AS users_name
    FROM users GROUP BY users.name) AS anon_1
    ()
    {stop}4

We don't want the number ``4``, we wanted some rows back.   So for detailed queries where you need to count something specific, use the ``func.count()`` function as a column expression:

.. sourcecode:: python+sql

    >>> from sqlalchemy import func
    {sql}>>> session.query(func.count(User.name), User.name).group_by(User.name).all()  #doctest: +NORMALIZE_WHITESPACE
    SELECT count(users.name) AS count_1, users.name AS users_name
    FROM users GROUP BY users.name
    {stop}()
    [(1, u'ed'), (1, u'fred'), (1, u'mary'), (1, u'wendy')]

Building a Relationship
=======================

Now let's consider a second table to be dealt with.  Users in our system also can store any number of email addresses associated with their username.  This implies a basic one to many association from the ``users_table`` to a new table which stores email addresses, which we will call ``addresses``.  Using declarative, we define this table along with its mapped class, ``Address``:

.. sourcecode:: python+sql

    >>> from sqlalchemy import ForeignKey
    >>> from sqlalchemy.orm import relationship, backref
    >>> class Address(Base):
    ...     __tablename__ = 'addresses'
    ...     id = Column(Integer, primary_key=True)
    ...     email_address = Column(String, nullable=False)
    ...     user_id = Column(Integer, ForeignKey('users.id'))
    ...
    ...     user = relationship(User, backref=backref('addresses', order_by=id))
    ...
    ...     def __init__(self, email_address):
    ...         self.email_address = email_address
    ...
    ...     def __repr__(self):
    ...         return "<Address('%s')>" % self.email_address

The above class introduces a **foreign key** constraint which references the ``users`` table.  This defines for SQLAlchemy the relationship between the two tables at the database level.  The relationship between the ``User`` and ``Address`` classes is defined separately using the :func:`~sqlalchemy.orm.relationship()` function, which defines an attribute ``user`` to be placed on the ``Address`` class, as well as an ``addresses`` collection to be placed on the ``User`` class.  Such a relationship is known as a **bidirectional** relationship.   Because of the placement of the foreign key, from ``Address`` to ``User`` it is **many to one**, and from ``User`` to ``Address`` it is **one to many**.  SQLAlchemy is automatically aware of many-to-one/one-to-many based on foreign keys.

.. note:: The :func:`~sqlalchemy.orm.relationship()` function has historically been known as :func:`~sqlalchemy.orm.relation()`, which is the name that's available in all versions of SQLAlchemy prior to 0.6beta2, including the 0.5 and 0.4 series. :func:`~sqlalchemy.orm.relationship()` is only available starting with SQLAlchemy 0.6beta2.  :func:`~sqlalchemy.orm.relation()` will remain available in SQLAlchemy for the foreseeable future to enable cross-compatibility.

The :func:`~sqlalchemy.orm.relationship()` function is extremely flexible, and could just have easily been defined on the ``User`` class:

.. sourcecode:: python+sql

    class User(Base):
        # ....
        addresses = relationship(Address, order_by=Address.id, backref="user")

We are also free to not define a backref, and to define the :func:`~sqlalchemy.orm.relationship()` only on one class and not the other.   It is also possible to define two separate :func:`~sqlalchemy.orm.relationship()` constructs for either direction, which is generally safe for many-to-one and one-to-many relationships, but not for many-to-many relationships.

When using the ``declarative`` extension, :func:`~sqlalchemy.orm.relationship()` gives us the option to use strings for most arguments that concern the target class, in the case that the target class has not yet been defined.  This **only** works in conjunction with ``declarative``:

.. sourcecode:: python+sql

    class User(Base):
        ....
        addresses = relationship("Address", order_by="Address.id", backref="user")

When ``declarative`` is not in use, you typically define your :func:`~sqlalchemy.orm.mapper()` well after the target classes and :class:`~sqlalchemy.schema.Table` objects have been defined, so string expressions are not needed.

We'll need to create the ``addresses`` table in the database, so we will issue another CREATE from our metadata, which will skip over tables which have already been created:

.. sourcecode:: python+sql

    {sql}>>> metadata.create_all(engine) # doctest: +NORMALIZE_WHITESPACE
    PRAGMA table_info("users")
    ()
    PRAGMA table_info("addresses")
    ()
    CREATE TABLE addresses (
        id INTEGER NOT NULL,
        email_address VARCHAR NOT NULL,
        user_id INTEGER,
        PRIMARY KEY (id),
         FOREIGN KEY(user_id) REFERENCES users (id)
    )
    ()
    COMMIT

Working with Related Objects
=============================

Now when we create a ``User``, a blank ``addresses`` collection will be present.  Various collection types, such as sets and dictionaries, are possible here (see :ref:`advdatamapping_entitycollections` for details), but by default, the collection is a Python list.

.. sourcecode:: python+sql

    >>> jack = User('jack', 'Jack Bean', 'gjffdd')
    >>> jack.addresses
    []

We are free to add ``Address`` objects on our ``User`` object.  In this case we just assign a full list directly:

.. sourcecode:: python+sql

    >>> jack.addresses = [Address(email_address='jack@google.com'), Address(email_address='j25@yahoo.com')]

When using a bidirectional relationship, elements added in one direction automatically become visible in the other direction.  This is the basic behavior of the **backref** keyword, which maintains the relationship purely in memory, without using any SQL:

.. sourcecode:: python+sql

    >>> jack.addresses[1]
    <Address('j25@yahoo.com')>

    >>> jack.addresses[1].user
    <User('jack','Jack Bean', 'gjffdd')>

Let's add and commit ``Jack Bean`` to the database.  ``jack`` as well as the two ``Address`` members in his ``addresses`` collection are both added to the session at once, using a process known as **cascading**:

.. sourcecode:: python+sql

    >>> session.add(jack)
    {sql}>>> session.commit()
    INSERT INTO users (name, fullname, password) VALUES (?, ?, ?)
    ('jack', 'Jack Bean', 'gjffdd')
    INSERT INTO addresses (email_address, user_id) VALUES (?, ?)
    ('jack@google.com', 5)
    INSERT INTO addresses (email_address, user_id) VALUES (?, ?)
    ('j25@yahoo.com', 5)
    COMMIT

Querying for Jack, we get just Jack back.  No SQL is yet issued for Jack's addresses:

.. sourcecode:: python+sql

    {sql}>>> jack = session.query(User).filter_by(name='jack').one() #doctest: +NORMALIZE_WHITESPACE
    BEGIN
    SELECT users.id AS users_id, users.name AS users_name, users.fullname AS users_fullname, users.password AS users_password
    FROM users
    WHERE users.name = ?
    ('jack',)

    {stop}>>> jack
    <User('jack','Jack Bean', 'gjffdd')>

Let's look at the ``addresses`` collection.  Watch the SQL:

.. sourcecode:: python+sql

    {sql}>>> jack.addresses #doctest: +NORMALIZE_WHITESPACE
    SELECT addresses.id AS addresses_id, addresses.email_address AS addresses_email_address, addresses.user_id AS addresses_user_id
    FROM addresses
    WHERE ? = addresses.user_id ORDER BY addresses.id
    (5,)
    {stop}[<Address('jack@google.com')>, <Address('j25@yahoo.com')>]

When we accessed the ``addresses`` collection, SQL was suddenly issued.  This is an example of a **lazy loading relationship**.  The ``addresses`` collection is now loaded and behaves just like an ordinary list.

If you want to reduce the number of queries (dramatically, in many cases), we can apply an **eager load** to the query operation, using the :func:`~sqlalchemy.orm.joinedload` function.  This function is a **query option** that gives additional instructions to the query on how we would like it to load, in this case we'd like to indicate that we'd like ``addresses`` to load "eagerly".  SQLAlchemy then constructs an outer join between the ``users`` and ``addresses`` tables, and loads them at once, populating the ``addresses`` collection on each ``User`` object if it's not already populated:

.. sourcecode:: python+sql

    >>> from sqlalchemy.orm import joinedload

    {sql}>>> jack = session.query(User).\
    ...                        options(joinedload('addresses')).\
    ...                        filter_by(name='jack').one() #doctest: +NORMALIZE_WHITESPACE
    SELECT users.id AS users_id, users.name AS users_name, users.fullname AS users_fullname,
    users.password AS users_password, addresses_1.id AS addresses_1_id, addresses_1.email_address
    AS addresses_1_email_address, addresses_1.user_id AS addresses_1_user_id
    FROM users LEFT OUTER JOIN addresses AS addresses_1 ON users.id = addresses_1.user_id
    WHERE users.name = ? ORDER BY addresses_1.id
    ('jack',)

    {stop}>>> jack
    <User('jack','Jack Bean', 'gjffdd')>

    >>> jack.addresses
    [<Address('jack@google.com')>, <Address('j25@yahoo.com')>]

See :ref:`mapper_loader_strategies` for information on :func:`~sqlalchemy.orm.joinedload` and its new brother, :func:`~sqlalchemy.orm.subqueryload`.   We'll also see another way to "eagerly" load in the next section.

Querying with Joins
====================

While :func:`~sqlalchemy.orm.joinedload` created a JOIN specifically to populate a collection, we can also work explicitly with joins in many ways.  For example, to construct a simple inner join between ``User`` and ``Address``, we can just :meth:`~sqlalchemy.orm.query.Query.filter()` their related columns together.  Below we load the ``User`` and ``Address`` entities at once using this method:

.. sourcecode:: python+sql

    {sql}>>> for u, a in session.query(User, Address).filter(User.id==Address.user_id).\
    ...         filter(Address.email_address=='jack@google.com').all():   # doctest: +NORMALIZE_WHITESPACE
    ...     print u, a
    SELECT users.id AS users_id, users.name AS users_name, users.fullname AS users_fullname,
    users.password AS users_password, addresses.id AS addresses_id,
    addresses.email_address AS addresses_email_address, addresses.user_id AS addresses_user_id
    FROM users, addresses
    WHERE users.id = addresses.user_id AND addresses.email_address = ?
    ('jack@google.com',)
    {stop}<User('jack','Jack Bean', 'gjffdd')> <Address('jack@google.com')>

Or we can make a real JOIN construct; the most common way is to use :meth:`~sqlalchemy.orm.query.Query.join`:

.. sourcecode:: python+sql

    {sql}>>> session.query(User).join(Address).\
    ...         filter(Address.email_address=='jack@google.com').all() #doctest: +NORMALIZE_WHITESPACE
    SELECT users.id AS users_id, users.name AS users_name, users.fullname AS users_fullname, users.password AS users_password
    FROM users JOIN addresses ON users.id = addresses.user_id
    WHERE addresses.email_address = ?
    ('jack@google.com',)
    {stop}[<User('jack','Jack Bean', 'gjffdd')>]

:meth:`~sqlalchemy.orm.query.Query.join` knows how to join between ``User`` and ``Address`` because there's only one foreign key between them.  If there were no foreign keys, or several, :meth:`~sqlalchemy.orm.query.Query.join` works better when one of the following forms are used::

    query.join((Address, User.id==Address.user_id))  # explicit condition (note the tuple)
    query.join(User.addresses)                       # specify relationship from left to right
    query.join((Address, User.addresses))            # same, with explicit target
    query.join('addresses')                          # same, using a string

Note that when :meth:`~sqlalchemy.orm.query.Query.join` is called with an explicit target as well as an ON clause, we use a tuple as the argument.  This is so that multiple joins can be chained together, as in::

    session.query(Foo).join(
                            Foo.bars, 
                            (Bat, bar.bats),
                            (Widget, Bat.widget_id==Widget.id)
                            )

The above would produce SQL something like ``foo JOIN bars ON <onclause> JOIN bats ON <onclause> JOIN widgets ON <onclause>``.

The general functionality of :meth:`~sqlalchemy.orm.query.Query.join()` is also available as a standalone function :func:`~sqlalchemy.orm.join`, which is an ORM-enabled version of the same function present in the SQL expression language.  This function accepts two or three arguments (left side, right side, optional ON clause) and can be used in conjunction with 
the :meth:`~sqlalchemy.orm.query.Query.select_from` method to set an explicit FROM clause:

.. sourcecode:: python+sql

    >>> from sqlalchemy.orm import join
    {sql}>>> session.query(User).\
    ...                select_from(join(User, Address, User.addresses)).\
    ...                filter(Address.email_address=='jack@google.com').all() #doctest: +NORMALIZE_WHITESPACE
    SELECT users.id AS users_id, users.name AS users_name, users.fullname AS users_fullname, users.password AS users_password
    FROM users JOIN addresses ON users.id = addresses.user_id
    WHERE addresses.email_address = ?
    ('jack@google.com',)
    {stop}[<User('jack','Jack Bean', 'gjffdd')>]

Using join() to Eagerly Load Collections/Attributes
-------------------------------------------------------

The "eager loading" capabilities of the :func:`~sqlalchemy.orm.joinedload` function and the join-construction capabilities of :meth:`~sqlalchemy.orm.query.Query.join()` or an equivalent can be combined together using the :func:`~sqlalchemy.orm.contains_eager` option.   This is typically used 
for a query that is already joining to some related entity (more often than not via many-to-one), and you'd like the related entity to also be loaded onto the resulting objects
in one step without the need for additional queries and without the "automatic" join embedded
by the :func:`~sqlalchemy.orm.joinedload` function:

.. sourcecode:: python+sql

    >>> from sqlalchemy.orm import contains_eager
    {sql}>>> for address in session.query(Address).\
    ...                join(Address.user).\
    ...                filter(User.name=='jack').\
    ...                options(contains_eager(Address.user)): #doctest: +NORMALIZE_WHITESPACE
    ...         print address, address.user
    SELECT users.id AS users_id, users.name AS users_name, users.fullname AS users_fullname,
     users.password AS users_password, addresses.id AS addresses_id, 
     addresses.email_address AS addresses_email_address, addresses.user_id AS addresses_user_id 
    FROM addresses JOIN users ON users.id = addresses.user_id 
    WHERE users.name = ?
    ('jack',)
    {stop}<Address('jack@google.com')> <User('jack','Jack Bean', 'gjffdd')>
    <Address('j25@yahoo.com')> <User('jack','Jack Bean', 'gjffdd')>

Note that above the join was used both to limit the rows to just those ``Address`` objects which
had a related ``User`` object with the name "jack".   It's safe to have the ``Address.user`` attribute populated with this user using an inner join.  However, when filtering on a join that 
is filtering on a particular member of a collection, using :func:`~sqlalchemy.orm.contains_eager` to populate a related collection may populate the collection with only part of what it actually references, since the collection itself is filtered.


Using Aliases
-------------

When querying across multiple tables, if the same table needs to be referenced more than once, SQL typically requires that the table be *aliased* with another name, so that it can be distinguished against other occurrences of that table.  The :class:`~sqlalchemy.orm.query.Query` supports this most explicitly using the ``aliased`` construct.  Below we join to the ``Address`` entity twice, to locate a user who has two distinct email addresses at the same time:

.. sourcecode:: python+sql

    >>> from sqlalchemy.orm import aliased
    >>> adalias1 = aliased(Address)
    >>> adalias2 = aliased(Address)
    {sql}>>> for username, email1, email2 in \
    ...     session.query(User.name, adalias1.email_address, adalias2.email_address).\
    ...     join((adalias1, User.addresses), (adalias2, User.addresses)).\
    ...     filter(adalias1.email_address=='jack@google.com').\
    ...     filter(adalias2.email_address=='j25@yahoo.com'):
    ...     print username, email1, email2      # doctest: +NORMALIZE_WHITESPACE
    SELECT users.name AS users_name, addresses_1.email_address AS addresses_1_email_address,
    addresses_2.email_address AS addresses_2_email_address
    FROM users JOIN addresses AS addresses_1 ON users.id = addresses_1.user_id
    JOIN addresses AS addresses_2 ON users.id = addresses_2.user_id
    WHERE addresses_1.email_address = ? AND addresses_2.email_address = ?
    ('jack@google.com', 'j25@yahoo.com')
    {stop}jack jack@google.com j25@yahoo.com

Using Subqueries
----------------

The :class:`~sqlalchemy.orm.query.Query` is suitable for generating statements which can be used as subqueries.  Suppose we wanted to load ``User`` objects along with a count of how many ``Address`` records each user has.  The best way to generate SQL like this is to get the count of addresses grouped by user ids, and JOIN to the parent.  In this case we use a LEFT OUTER JOIN so that we get rows back for those users who don't have any addresses, e.g.::

    SELECT users.*, adr_count.address_count FROM users LEFT OUTER JOIN
        (SELECT user_id, count(*) AS address_count FROM addresses GROUP BY user_id) AS adr_count
        ON users.id=adr_count.user_id

Using the :class:`~sqlalchemy.orm.query.Query`, we build a statement like this from the inside out.  The ``statement`` accessor returns a SQL expression representing the statement generated by a particular :class:`~sqlalchemy.orm.query.Query` - this is an instance of a ``select()`` construct, which are described in :ref:`sqlexpression_toplevel`::

    >>> from sqlalchemy.sql import func
    >>> stmt = session.query(Address.user_id, func.count('*').label('address_count')).group_by(Address.user_id).subquery()

The ``func`` keyword generates SQL functions, and the ``subquery()`` method on :class:`~sqlalchemy.orm.query.Query` produces a SQL expression construct representing a SELECT statement embedded within an alias (it's actually shorthand for ``query.statement.alias()``).

Once we have our statement, it behaves like a :class:`~sqlalchemy.schema.Table` construct, such as the one we created for ``users`` at the start of this tutorial.  The columns on the statement are accessible through an attribute called ``c``:

.. sourcecode:: python+sql

    {sql}>>> for u, count in session.query(User, stmt.c.address_count).\
    ...     outerjoin((stmt, User.id==stmt.c.user_id)).order_by(User.id): # doctest: +NORMALIZE_WHITESPACE
    ...     print u, count
    SELECT users.id AS users_id, users.name AS users_name,
    users.fullname AS users_fullname, users.password AS users_password,
    anon_1.address_count AS anon_1_address_count
    FROM users LEFT OUTER JOIN (SELECT addresses.user_id AS user_id, count(?) AS address_count
    FROM addresses GROUP BY addresses.user_id) AS anon_1 ON users.id = anon_1.user_id
    ORDER BY users.id
    ('*',)
    {stop}<User('ed','Ed Jones', 'f8s7ccs')> None
    <User('wendy','Wendy Williams', 'foobar')> None
    <User('mary','Mary Contrary', 'xxg527')> None
    <User('fred','Fred Flinstone', 'blah')> None
    <User('jack','Jack Bean', 'gjffdd')> 2

Selecting Entities from Subqueries
----------------------------------

Above, we just selected a result that included a column from a subquery.  What if we wanted our subquery to map to an entity ?   For this we use ``aliased()`` to associate an "alias" of a mapped class to a subquery:

.. sourcecode:: python+sql

    {sql}>>> stmt = session.query(Address).filter(Address.email_address != 'j25@yahoo.com').subquery()
    >>> adalias = aliased(Address, stmt)
    >>> for user, address in session.query(User, adalias).join((adalias, User.addresses)): # doctest: +NORMALIZE_WHITESPACE
    ...     print user, address
    SELECT users.id AS users_id, users.name AS users_name, users.fullname AS users_fullname,
    users.password AS users_password, anon_1.id AS anon_1_id,
    anon_1.email_address AS anon_1_email_address, anon_1.user_id AS anon_1_user_id
    FROM users JOIN (SELECT addresses.id AS id, addresses.email_address AS email_address, addresses.user_id AS user_id
    FROM addresses
    WHERE addresses.email_address != ?) AS anon_1 ON users.id = anon_1.user_id
    ('j25@yahoo.com',)
    {stop}<User('jack','Jack Bean', 'gjffdd')> <Address('jack@google.com')>

Using EXISTS
------------

The EXISTS keyword in SQL is a boolean operator which returns True if the given expression contains any rows.  It may be used in many scenarios in place of joins, and is also useful for locating rows which do not have a corresponding row in a related table.

There is an explicit EXISTS construct, which looks like this:

.. sourcecode:: python+sql

    >>> from sqlalchemy.sql import exists
    >>> stmt = exists().where(Address.user_id==User.id)
    {sql}>>> for name, in session.query(User.name).filter(stmt):   # doctest: +NORMALIZE_WHITESPACE
    ...     print name
    SELECT users.name AS users_name
    FROM users
    WHERE EXISTS (SELECT *
    FROM addresses
    WHERE addresses.user_id = users.id)
    ()
    {stop}jack

The :class:`~sqlalchemy.orm.query.Query` features several operators which make usage of EXISTS automatically.  Above, the statement can be expressed along the ``User.addresses`` relationship using ``any()``:

.. sourcecode:: python+sql

    {sql}>>> for name, in session.query(User.name).filter(User.addresses.any()):   # doctest: +NORMALIZE_WHITESPACE
    ...     print name
    SELECT users.name AS users_name
    FROM users
    WHERE EXISTS (SELECT 1
    FROM addresses
    WHERE users.id = addresses.user_id)
    ()
    {stop}jack

``any()`` takes criterion as well, to limit the rows matched:

.. sourcecode:: python+sql

    {sql}>>> for name, in session.query(User.name).\
    ...     filter(User.addresses.any(Address.email_address.like('%google%'))):   # doctest: +NORMALIZE_WHITESPACE
    ...     print name
    SELECT users.name AS users_name
    FROM users
    WHERE EXISTS (SELECT 1
    FROM addresses
    WHERE users.id = addresses.user_id AND addresses.email_address LIKE ?)
    ('%google%',)
    {stop}jack

``has()`` is the same operator as ``any()`` for many-to-one relationships (note the ``~`` operator here too, which means "NOT"):

.. sourcecode:: python+sql

    {sql}>>> session.query(Address).filter(~Address.user.has(User.name=='jack')).all() # doctest: +NORMALIZE_WHITESPACE
    SELECT addresses.id AS addresses_id, addresses.email_address AS addresses_email_address,
    addresses.user_id AS addresses_user_id
    FROM addresses
    WHERE NOT (EXISTS (SELECT 1
    FROM users
    WHERE users.id = addresses.user_id AND users.name = ?))
    ('jack',)
    {stop}[]

Common Relationship Operators
-----------------------------

Here's all the operators which build on relationships:

* equals (used for many-to-one)::

    query.filter(Address.user == someuser)

* not equals (used for many-to-one)::

    query.filter(Address.user != someuser)

* IS NULL (used for many-to-one)::

    query.filter(Address.user == None)

* contains (used for one-to-many and many-to-many collections)::

    query.filter(User.addresses.contains(someaddress))

* any (used for one-to-many and many-to-many collections)::

    query.filter(User.addresses.any(Address.email_address == 'bar'))

    # also takes keyword arguments:
    query.filter(User.addresses.any(email_address='bar'))

* has (used for many-to-one)::

    query.filter(Address.user.has(name='ed'))

* with_parent (used for any relationship)::

    session.query(Address).with_parent(someuser, 'addresses')

Deleting
========

Let's try to delete ``jack`` and see how that goes.  We'll mark as deleted in the session, then we'll issue a ``count`` query to see that no rows remain:

.. sourcecode:: python+sql

    >>> session.delete(jack)
    {sql}>>> session.query(User).filter_by(name='jack').count() # doctest: +NORMALIZE_WHITESPACE
    UPDATE addresses SET user_id=? WHERE addresses.id = ?
    (None, 1)
    UPDATE addresses SET user_id=? WHERE addresses.id = ?
    (None, 2)
    DELETE FROM users WHERE users.id = ?
    (5,)
    SELECT count(1) AS count_1
    FROM users
    WHERE users.name = ?
    ('jack',)
    {stop}0

So far, so good.  How about Jack's ``Address`` objects ?

.. sourcecode:: python+sql

    {sql}>>> session.query(Address).filter(
    ...     Address.email_address.in_(['jack@google.com', 'j25@yahoo.com'])
    ...  ).count() # doctest: +NORMALIZE_WHITESPACE
    SELECT count(1) AS count_1
    FROM addresses
    WHERE addresses.email_address IN (?, ?)
    ('jack@google.com', 'j25@yahoo.com')
    {stop}2

Uh oh, they're still there !  Analyzing the flush SQL, we can see that the ``user_id`` column of each address was set to NULL, but the rows weren't deleted.  SQLAlchemy doesn't assume that deletes cascade, you have to tell it to do so.

Configuring delete/delete-orphan Cascade
----------------------------------------

We will configure **cascade** options on the ``User.addresses`` relationship to change the behavior.  While SQLAlchemy allows you to add new attributes and relationships to mappings at any point in time, in this case the existing relationship needs to be removed, so we need to tear down the mappings completely and start again.  This is not a typical operation and is here just for illustrative purposes.

Removing all ORM state is as follows:

.. sourcecode:: python+sql

    >>> session.close()  # roll back and close the transaction
    >>> from sqlalchemy.orm import clear_mappers
    >>> clear_mappers() # clear mappers

Below, we use ``mapper()`` to reconfigure an ORM mapping for ``User`` and ``Address``, on our existing but currently un-mapped classes.  The ``User.addresses`` relationship now has ``delete, delete-orphan`` cascade on it, which indicates that DELETE operations will cascade to attached ``Address`` objects as well as ``Address`` objects which are removed from their parent:

.. sourcecode:: python+sql

    >>> mapper(User, users_table, properties={    # doctest: +ELLIPSIS
    ...     'addresses':relationship(Address, backref='user', cascade="all, delete, delete-orphan")
    ... })
    <Mapper at 0x...; User>

    >>> addresses_table = Address.__table__
    >>> mapper(Address, addresses_table) # doctest: +ELLIPSIS
    <Mapper at 0x...; Address>

Now when we load Jack (below using ``get()``, which loads by primary key), removing an address from his ``addresses`` collection will result in that ``Address`` being deleted:

.. sourcecode:: python+sql

    # load Jack by primary key
    {sql}>>> jack = session.query(User).get(5)    #doctest: +NORMALIZE_WHITESPACE
    BEGIN
    SELECT users.id AS users_id, users.name AS users_name, users.fullname AS users_fullname, users.password AS users_password
    FROM users
    WHERE users.id = ?
    (5,)
    {stop}

    # remove one Address (lazy load fires off)
    {sql}>>> del jack.addresses[1] #doctest: +NORMALIZE_WHITESPACE
    SELECT addresses.id AS addresses_id, addresses.email_address AS addresses_email_address, addresses.user_id AS addresses_user_id
    FROM addresses
    WHERE ? = addresses.user_id
    (5,)
    {stop}

    # only one address remains
    {sql}>>> session.query(Address).filter(
    ...     Address.email_address.in_(['jack@google.com', 'j25@yahoo.com'])
    ... ).count() # doctest: +NORMALIZE_WHITESPACE
    DELETE FROM addresses WHERE addresses.id = ?
    (2,)
    SELECT count(1) AS count_1
    FROM addresses
    WHERE addresses.email_address IN (?, ?)
    ('jack@google.com', 'j25@yahoo.com')
    {stop}1

Deleting Jack will delete both Jack and his remaining ``Address``:

.. sourcecode:: python+sql

    >>> session.delete(jack)

    {sql}>>> session.query(User).filter_by(name='jack').count() # doctest: +NORMALIZE_WHITESPACE
    DELETE FROM addresses WHERE addresses.id = ?
    (1,)
    DELETE FROM users WHERE users.id = ?
    (5,)
    SELECT count(1) AS count_1
    FROM users
    WHERE users.name = ?
    ('jack',)
    {stop}0

    {sql}>>> session.query(Address).filter(
    ...    Address.email_address.in_(['jack@google.com', 'j25@yahoo.com'])
    ... ).count() # doctest: +NORMALIZE_WHITESPACE
    SELECT count(1) AS count_1
    FROM addresses
    WHERE addresses.email_address IN (?, ?)
    ('jack@google.com', 'j25@yahoo.com')
    {stop}0

Building a Many To Many Relationship
====================================

We're moving into the bonus round here, but lets show off a many-to-many relationship.  We'll sneak in some other features too, just to take a tour.  We'll make our application a blog application, where users can write ``BlogPost`` items, which have ``Keyword`` items associated with them.

The declarative setup is as follows:

.. sourcecode:: python+sql

    >>> from sqlalchemy import Text

    >>> # association table
    >>> post_keywords = Table('post_keywords', metadata,
    ...     Column('post_id', Integer, ForeignKey('posts.id')),
    ...     Column('keyword_id', Integer, ForeignKey('keywords.id'))
    ... )

    >>> class BlogPost(Base):
    ...     __tablename__ = 'posts'
    ...
    ...     id = Column(Integer, primary_key=True)
    ...     user_id = Column(Integer, ForeignKey('users.id'))
    ...     headline = Column(String(255), nullable=False)
    ...     body = Column(Text)
    ...
    ...     # many to many BlogPost<->Keyword
    ...     keywords = relationship('Keyword', secondary=post_keywords, backref='posts')
    ...
    ...     def __init__(self, headline, body, author):
    ...         self.author = author
    ...         self.headline = headline
    ...         self.body = body
    ...
    ...     def __repr__(self):
    ...         return "BlogPost(%r, %r, %r)" % (self.headline, self.body, self.author)

    >>> class Keyword(Base):
    ...     __tablename__ = 'keywords'
    ...
    ...     id = Column(Integer, primary_key=True)
    ...     keyword = Column(String(50), nullable=False, unique=True)
    ...
    ...     def __init__(self, keyword):
    ...         self.keyword = keyword

Above, the many-to-many relationship is ``BlogPost.keywords``.  The defining feature of a many-to-many relationship is the ``secondary`` keyword argument which references a :class:`~sqlalchemy.schema.Table` object representing the association table.  This table only contains columns which reference the two sides of the relationship; if it has *any* other columns, such as its own primary key, or foreign keys to other tables, SQLAlchemy requires a different usage pattern called the "association object", described at :ref:`association_pattern`.

The many-to-many relationship is also bi-directional using the ``backref`` keyword.  This is the one case where usage of ``backref`` is generally required, since if a separate ``posts`` relationship were added to the ``Keyword`` entity, both relationships would independently add and remove rows from the ``post_keywords`` table and produce conflicts.

We would also like our ``BlogPost`` class to have an ``author`` field.  We will add this as another bidirectional relationship, except one issue we'll have is that a single user might have lots of blog posts.  When we access ``User.posts``, we'd like to be able to filter results further so as not to load the entire collection.  For this we use a setting accepted by :func:`~sqlalchemy.orm.relationship` called ``lazy='dynamic'``, which configures an alternate **loader strategy** on the attribute.  To use it on the "reverse" side of a :func:`~sqlalchemy.orm.relationship`, we use the :func:`~sqlalchemy.orm.backref` function:

.. sourcecode:: python+sql

    >>> from sqlalchemy.orm import backref
    >>> # "dynamic" loading relationship to User
    >>> BlogPost.author = relationship(User, backref=backref('posts', lazy='dynamic'))

Create new tables:

.. sourcecode:: python+sql

    {sql}>>> metadata.create_all(engine) # doctest: +NORMALIZE_WHITESPACE
    PRAGMA table_info("users")
    ()
    PRAGMA table_info("addresses")
    ()
    PRAGMA table_info("posts")
    ()
    PRAGMA table_info("keywords")
    ()
    PRAGMA table_info("post_keywords")
    ()
    CREATE TABLE posts (
        id INTEGER NOT NULL,
        user_id INTEGER,
        headline VARCHAR(255) NOT NULL,
        body TEXT,
        PRIMARY KEY (id),
         FOREIGN KEY(user_id) REFERENCES users (id)
    )
    ()
    COMMIT
    CREATE TABLE keywords (
        id INTEGER NOT NULL,
        keyword VARCHAR(50) NOT NULL,
        PRIMARY KEY (id),
         UNIQUE (keyword)
    )
    ()
    COMMIT
    CREATE TABLE post_keywords (
        post_id INTEGER,
        keyword_id INTEGER,
         FOREIGN KEY(post_id) REFERENCES posts (id),
         FOREIGN KEY(keyword_id) REFERENCES keywords (id)
    )
    ()
    COMMIT

Usage is not too different from what we've been doing.  Let's give Wendy some blog posts:

.. sourcecode:: python+sql

    {sql}>>> wendy = session.query(User).filter_by(name='wendy').one() #doctest: +NORMALIZE_WHITESPACE
    SELECT users.id AS users_id, users.name AS users_name, users.fullname AS users_fullname, users.password AS users_password
    FROM users
    WHERE users.name = ?
    ('wendy',)
    {stop}
    >>> post = BlogPost("Wendy's Blog Post", "This is a test", wendy)
    >>> session.add(post)

We're storing keywords uniquely in the database, but we know that we don't have any yet, so we can just create them:

.. sourcecode:: python+sql

    >>> post.keywords.append(Keyword('wendy'))
    >>> post.keywords.append(Keyword('firstpost'))

We can now look up all blog posts with the keyword 'firstpost'.   We'll use the ``any`` operator to locate "blog posts where any of its keywords has the keyword string 'firstpost'":

.. sourcecode:: python+sql

    {sql}>>> session.query(BlogPost).filter(BlogPost.keywords.any(keyword='firstpost')).all() #doctest: +NORMALIZE_WHITESPACE
    INSERT INTO keywords (keyword) VALUES (?)
    ('wendy',)
    INSERT INTO keywords (keyword) VALUES (?)
    ('firstpost',)
    INSERT INTO posts (user_id, headline, body) VALUES (?, ?, ?)
    (2, "Wendy's Blog Post", 'This is a test')
    INSERT INTO post_keywords (post_id, keyword_id) VALUES (?, ?)
    ((1, 1), (1, 2))
    SELECT posts.id AS posts_id, posts.user_id AS posts_user_id, posts.headline AS posts_headline, posts.body AS posts_body
    FROM posts
    WHERE EXISTS (SELECT 1
    FROM post_keywords, keywords
    WHERE posts.id = post_keywords.post_id AND keywords.id = post_keywords.keyword_id AND keywords.keyword = ?)
    ('firstpost',)
    {stop}[BlogPost("Wendy's Blog Post", 'This is a test', <User('wendy','Wendy Williams', 'foobar')>)]

If we want to look up just Wendy's posts, we can tell the query to narrow down to her as a parent:

.. sourcecode:: python+sql

    {sql}>>> session.query(BlogPost).filter(BlogPost.author==wendy).\
    ... filter(BlogPost.keywords.any(keyword='firstpost')).all() #doctest: +NORMALIZE_WHITESPACE
    SELECT posts.id AS posts_id, posts.user_id AS posts_user_id, posts.headline AS posts_headline, posts.body AS posts_body
    FROM posts
    WHERE ? = posts.user_id AND (EXISTS (SELECT 1
    FROM post_keywords, keywords
    WHERE posts.id = post_keywords.post_id AND keywords.id = post_keywords.keyword_id AND keywords.keyword = ?))
    (2, 'firstpost')
    {stop}[BlogPost("Wendy's Blog Post", 'This is a test', <User('wendy','Wendy Williams', 'foobar')>)]

Or we can use Wendy's own ``posts`` relationship, which is a "dynamic" relationship, to query straight from there:

.. sourcecode:: python+sql

    {sql}>>> wendy.posts.filter(BlogPost.keywords.any(keyword='firstpost')).all() #doctest: +NORMALIZE_WHITESPACE
    SELECT posts.id AS posts_id, posts.user_id AS posts_user_id, posts.headline AS posts_headline, posts.body AS posts_body
    FROM posts
    WHERE ? = posts.user_id AND (EXISTS (SELECT 1
    FROM post_keywords, keywords
    WHERE posts.id = post_keywords.post_id AND keywords.id = post_keywords.keyword_id AND keywords.keyword = ?))
    (2, 'firstpost')
    {stop}[BlogPost("Wendy's Blog Post", 'This is a test', <User('wendy','Wendy Williams', 'foobar')>)]

Further Reference
==================

Query Reference: :ref:`query_api_toplevel`

Further information on mapping setups are in :ref:`datamapping_toplevel`.

Further information on working with Sessions: :ref:`session_toplevel`.
