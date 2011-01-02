# ext/sqlsoup.py
# Copyright (C) 2005-2011 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""
Introduction
============

SqlSoup provides a convenient way to access existing database
tables without having to declare table or mapper classes ahead
of time. It is built on top of the SQLAlchemy ORM and provides a
super-minimalistic interface to an existing database.

SqlSoup effectively provides a coarse grained, alternative
interface to working with the SQLAlchemy ORM, providing a "self
configuring" interface for extremely rudimental operations. It's
somewhat akin to a "super novice mode" version of the ORM. While
SqlSoup can be very handy, users are strongly encouraged to use
the full ORM for non-trivial applications.

Suppose we have a database with users, books, and loans tables
(corresponding to the PyWebOff dataset, if you're curious).

Creating a SqlSoup gateway is just like creating an SQLAlchemy
engine::

    >>> from sqlalchemy.ext.sqlsoup import SqlSoup
    >>> db = SqlSoup('sqlite:///:memory:')

or, you can re-use an existing engine::

    >>> db = SqlSoup(engine)

You can optionally specify a schema within the database for your
SqlSoup::

    >>> db.schema = myschemaname

Loading objects
===============

Loading objects is as easy as this::

    >>> users = db.users.all()
    >>> users.sort()
    >>> users
    [
        MappedUsers(name=u'Joe Student',email=u'student@example.edu',
                password=u'student',classname=None,admin=0), 
        MappedUsers(name=u'Bhargan Basepair',email=u'basepair@example.edu',
                password=u'basepair',classname=None,admin=1)
    ]

Of course, letting the database do the sort is better::

    >>> db.users.order_by(db.users.name).all()
    [
        MappedUsers(name=u'Bhargan Basepair',email=u'basepair@example.edu',
            password=u'basepair',classname=None,admin=1), 
        MappedUsers(name=u'Joe Student',email=u'student@example.edu',
            password=u'student',classname=None,admin=0)
    ]

Field access is intuitive::

    >>> users[0].email
    u'student@example.edu'

Of course, you don't want to load all users very often. Let's
add a WHERE clause. Let's also switch the order_by to DESC while
we're at it::

    >>> from sqlalchemy import or_, and_, desc
    >>> where = or_(db.users.name=='Bhargan Basepair', db.users.email=='student@example.edu')
    >>> db.users.filter(where).order_by(desc(db.users.name)).all()
    [
        MappedUsers(name=u'Joe Student',email=u'student@example.edu',
            password=u'student',classname=None,admin=0), 
        MappedUsers(name=u'Bhargan Basepair',email=u'basepair@example.edu',
            password=u'basepair',classname=None,admin=1)
    ]

You can also use .first() (to retrieve only the first object
from a query) or .one() (like .first when you expect exactly one
user -- it will raise an exception if more were returned)::

    >>> db.users.filter(db.users.name=='Bhargan Basepair').one()
    MappedUsers(name=u'Bhargan Basepair',email=u'basepair@example.edu',
            password=u'basepair',classname=None,admin=1)

Since name is the primary key, this is equivalent to

    >>> db.users.get('Bhargan Basepair')
    MappedUsers(name=u'Bhargan Basepair',email=u'basepair@example.edu',
        password=u'basepair',classname=None,admin=1)

This is also equivalent to

    >>> db.users.filter_by(name='Bhargan Basepair').one()
    MappedUsers(name=u'Bhargan Basepair',email=u'basepair@example.edu',
        password=u'basepair',classname=None,admin=1)

filter_by is like filter, but takes kwargs instead of full
clause expressions. This makes it more concise for simple
queries like this, but you can't do complex queries like the
or\_ above or non-equality based comparisons this way.

Full query documentation
------------------------

Get, filter, filter_by, order_by, limit, and the rest of the
query methods are explained in detail in
:ref:`ormtutorial_querying`.

Modifying objects
=================

Modifying objects is intuitive::

    >>> user = _
    >>> user.email = 'basepair+nospam@example.edu'
    >>> db.commit()

(SqlSoup leverages the sophisticated SQLAlchemy unit-of-work
code, so multiple updates to a single object will be turned into
a single ``UPDATE`` statement when you commit.)

To finish covering the basics, let's insert a new loan, then
delete it::

    >>> book_id = db.books.filter_by(title='Regional Variation in Moss').first().id
    >>> db.loans.insert(book_id=book_id, user_name=user.name)
    MappedLoans(book_id=2,user_name=u'Bhargan Basepair',loan_date=None)

    >>> loan = db.loans.filter_by(book_id=2, user_name='Bhargan Basepair').one()
    >>> db.delete(loan)
    >>> db.commit()

You can also delete rows that have not been loaded as objects.
Let's do our insert/delete cycle once more, this time using the
loans table's delete method. (For SQLAlchemy experts: note that
no flush() call is required since this delete acts at the SQL
level, not at the Mapper level.) The same where-clause
construction rules apply here as to the select methods::

    >>> db.loans.insert(book_id=book_id, user_name=user.name)
    MappedLoans(book_id=2,user_name=u'Bhargan Basepair',loan_date=None)
    >>> db.loans.delete(db.loans.book_id==2)

You can similarly update multiple rows at once. This will change the
book_id to 1 in all loans whose book_id is 2::

    >>> db.loans.update(db.loans.book_id==2, book_id=1)
    >>> db.loans.filter_by(book_id=1).all()
    [MappedLoans(book_id=1,user_name=u'Joe Student',
        loan_date=datetime.datetime(2006, 7, 12, 0, 0))]


Joins
=====

Occasionally, you will want to pull out a lot of data from related
tables all at once.  In this situation, it is far more efficient to
have the database perform the necessary join.  (Here we do not have *a
lot of data* but hopefully the concept is still clear.)  SQLAlchemy is
smart enough to recognize that loans has a foreign key to users, and
uses that as the join condition automatically::

    >>> join1 = db.join(db.users, db.loans, isouter=True)
    >>> join1.filter_by(name='Joe Student').all()
    [
        MappedJoin(name=u'Joe Student',email=u'student@example.edu',
            password=u'student',classname=None,admin=0,book_id=1,
            user_name=u'Joe Student',loan_date=datetime.datetime(2006, 7, 12, 0, 0))
    ]

If you're unfortunate enough to be using MySQL with the default MyISAM
storage engine, you'll have to specify the join condition manually,
since MyISAM does not store foreign keys.  Here's the same join again,
with the join condition explicitly specified::

    >>> db.join(db.users, db.loans, db.users.name==db.loans.user_name, isouter=True)
    <class 'sqlalchemy.ext.sqlsoup.MappedJoin'>

You can compose arbitrarily complex joins by combining Join objects
with tables or other joins.  Here we combine our first join with the
books table::

    >>> join2 = db.join(join1, db.books)
    >>> join2.all()
    [
        MappedJoin(name=u'Joe Student',email=u'student@example.edu',
            password=u'student',classname=None,admin=0,book_id=1,
            user_name=u'Joe Student',loan_date=datetime.datetime(2006, 7, 12, 0, 0),
            id=1,title=u'Mustards I Have Known',published_year=u'1989',
            authors=u'Jones')
    ]

If you join tables that have an identical column name, wrap your join
with `with_labels`, to disambiguate columns with their table name
(.c is short for .columns)::

    >>> db.with_labels(join1).c.keys()
    [u'users_name', u'users_email', u'users_password', 
        u'users_classname', u'users_admin', u'loans_book_id', 
        u'loans_user_name', u'loans_loan_date']

You can also join directly to a labeled object::

    >>> labeled_loans = db.with_labels(db.loans)
    >>> db.join(db.users, labeled_loans, isouter=True).c.keys()
    [u'name', u'email', u'password', u'classname', 
        u'admin', u'loans_book_id', u'loans_user_name', u'loans_loan_date']


Relationships
=============

You can define relationships on SqlSoup classes:

    >>> db.users.relate('loans', db.loans)

These can then be used like a normal SA property:

    >>> db.users.get('Joe Student').loans
    [MappedLoans(book_id=1,user_name=u'Joe Student',
                    loan_date=datetime.datetime(2006, 7, 12, 0, 0))]

    >>> db.users.filter(~db.users.loans.any()).all()
    [MappedUsers(name=u'Bhargan Basepair',
            email='basepair+nospam@example.edu',
            password=u'basepair',classname=None,admin=1)]

relate can take any options that the relationship function
accepts in normal mapper definition:

    >>> del db._cache['users']
    >>> db.users.relate('loans', db.loans, order_by=db.loans.loan_date, cascade='all, delete-orphan')

Advanced Use
============

Sessions, Transations and Application Integration
-------------------------------------------------

**Note:** please read and understand this section thoroughly
before using SqlSoup in any web application.

SqlSoup uses a ScopedSession to provide thread-local sessions.
You can get a reference to the current one like this::

    >>> session = db.session

The default session is available at the module level in SQLSoup,
via::

    >>> from sqlalchemy.ext.sqlsoup import Session

The configuration of this session is ``autoflush=True``,
``autocommit=False``. This means when you work with the SqlSoup
object, you need to call ``db.commit()`` in order to have
changes persisted. You may also call ``db.rollback()`` to roll
things back.

Since the SqlSoup object's Session automatically enters into a
transaction as soon as it's used, it is *essential* that you
call ``commit()`` or ``rollback()`` on it when the work within a
thread completes. This means all the guidelines for web
application integration at :ref:`session_lifespan` must be
followed.

The SqlSoup object can have any session or scoped session
configured onto it. This is of key importance when integrating
with existing code or frameworks such as Pylons. If your
application already has a ``Session`` configured, pass it to
your SqlSoup object::

    >>> from myapplication import Session
    >>> db = SqlSoup(session=Session)

If the ``Session`` is configured with ``autocommit=True``, use
``flush()`` instead of ``commit()`` to persist changes - in this
case, the ``Session`` closes out its transaction immediately and
no external management is needed. ``rollback()`` is also not
available. Configuring a new SQLSoup object in "autocommit" mode
looks like::

    >>> from sqlalchemy.orm import scoped_session, sessionmaker
    >>> db = SqlSoup('sqlite://', session=scoped_session(sessionmaker(autoflush=False, expire_on_commit=False, autocommit=True)))


Mapping arbitrary Selectables
-----------------------------

SqlSoup can map any SQLAlchemy :class:`.Selectable` with the map
method. Let's map an :func:`.expression.select` object that uses an aggregate
function; we'll use the SQLAlchemy :class:`.Table` that SqlSoup
introspected as the basis. (Since we're not mapping to a simple
table or join, we need to tell SQLAlchemy how to find the
*primary key* which just needs to be unique within the select,
and not necessarily correspond to a *real* PK in the database.)::

    >>> from sqlalchemy import select, func
    >>> b = db.books._table
    >>> s = select([b.c.published_year, func.count('*').label('n')], from_obj=[b], group_by=[b.c.published_year])
    >>> s = s.alias('years_with_count')
    >>> years_with_count = db.map(s, primary_key=[s.c.published_year])
    >>> years_with_count.filter_by(published_year='1989').all()
    [MappedBooks(published_year=u'1989',n=1)]

Obviously if we just wanted to get a list of counts associated with
book years once, raw SQL is going to be less work. The advantage of
mapping a Select is reusability, both standalone and in Joins. (And if
you go to full SQLAlchemy, you can perform mappings like this directly
to your object models.)

An easy way to save mapped selectables like this is to just hang them on
your db object::

    >>> db.years_with_count = years_with_count

Python is flexible like that!

Raw SQL
-------

SqlSoup works fine with SQLAlchemy's text construct, described
in :ref:`sqlexpression_text`. You can also execute textual SQL
directly using the `execute()` method, which corresponds to the
`execute()` method on the underlying `Session`. Expressions here
are expressed like ``text()`` constructs, using named parameters
with colons::

    >>> rp = db.execute('select name, email from users where name like :name order by name', name='%Bhargan%')
    >>> for name, email in rp.fetchall(): print name, email
    Bhargan Basepair basepair+nospam@example.edu

Or you can get at the current transaction's connection using
`connection()`. This is the raw connection object which can
accept any sort of SQL expression or raw SQL string passed to
the database::

    >>> conn = db.connection()
    >>> conn.execute("'select name, email from users where name like ? order by name'", '%Bhargan%')

Dynamic table names
-------------------

You can load a table whose name is specified at runtime with the
entity() method:

    >>> tablename = 'loans'
    >>> db.entity(tablename) == db.loans
    True

entity() also takes an optional schema argument. If none is
specified, the default schema is used.

"""

from sqlalchemy import Table, MetaData, join
from sqlalchemy import schema, sql, util
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm import scoped_session, sessionmaker, mapper, \
                            class_mapper, relationship, session,\
                            object_session
from sqlalchemy.orm.interfaces import MapperExtension, EXT_CONTINUE
from sqlalchemy.exceptions import SQLAlchemyError, InvalidRequestError, ArgumentError
from sqlalchemy.sql import expression


__all__ = ['PKNotFoundError', 'SqlSoup']

Session = scoped_session(sessionmaker(autoflush=True, autocommit=False))

class AutoAdd(MapperExtension):
    def __init__(self, scoped_session):
        self.scoped_session = scoped_session

    def instrument_class(self, mapper, class_):
        class_.__init__ = self._default__init__(mapper)

    def _default__init__(ext, mapper):
        def __init__(self, **kwargs):
            for key, value in kwargs.iteritems():
                setattr(self, key, value)
        return __init__

    def init_instance(self, mapper, class_, oldinit, instance, args, kwargs):
        session = self.scoped_session()
        session._save_without_cascade(instance)
        return EXT_CONTINUE

    def init_failed(self, mapper, class_, oldinit, instance, args, kwargs):
        sess = object_session(instance)
        if sess:
            sess.expunge(instance)
        return EXT_CONTINUE

class PKNotFoundError(SQLAlchemyError):
    pass

def _ddl_error(cls):
    msg = 'SQLSoup can only modify mapped Tables (found: %s)' \
          % cls._table.__class__.__name__
    raise InvalidRequestError(msg)

# metaclass is necessary to expose class methods with getattr, e.g.
# we want to pass db.users.select through to users._mapper.select
class SelectableClassType(type):
    def insert(cls, **kwargs):
        _ddl_error(cls)

    def __clause_element__(cls):
        return cls._table

    def __getattr__(cls, attr):
        if attr == '_query':
            # called during mapper init
            raise AttributeError()
        return getattr(cls._query, attr)

class TableClassType(SelectableClassType):
    def insert(cls, **kwargs):
        o = cls()
        o.__dict__.update(kwargs)
        return o

    def relate(cls, propname, *args, **kwargs):
        class_mapper(cls)._configure_property(propname, relationship(*args, **kwargs))

def _is_outer_join(selectable):
    if not isinstance(selectable, sql.Join):
        return False
    if selectable.isouter:
        return True
    return _is_outer_join(selectable.left) or _is_outer_join(selectable.right)

def _selectable_name(selectable):
    if isinstance(selectable, sql.Alias):
        return _selectable_name(selectable.element)
    elif isinstance(selectable, sql.Select):
        return ''.join(_selectable_name(s) for s in selectable.froms)
    elif isinstance(selectable, schema.Table):
        return selectable.name.capitalize()
    else:
        x = selectable.__class__.__name__
        if x[0] == '_':
            x = x[1:]
        return x

def _class_for_table(session, engine, selectable, base_cls, mapper_kwargs):
    selectable = expression._clause_element_as_expr(selectable)
    mapname = 'Mapped' + _selectable_name(selectable)
    # Py2K
    if isinstance(mapname, unicode): 
        engine_encoding = engine.dialect.encoding 
        mapname = mapname.encode(engine_encoding)
    # end Py2K

    if isinstance(selectable, Table):
        klass = TableClassType(mapname, (base_cls,), {})
    else:
        klass = SelectableClassType(mapname, (base_cls,), {})

    def _compare(self, o):
        L = list(self.__class__.c.keys())
        L.sort()
        t1 = [getattr(self, k) for k in L]
        try:
            t2 = [getattr(o, k) for k in L]
        except AttributeError:
            raise TypeError('unable to compare with %s' % o.__class__)
        return t1, t2

    # python2/python3 compatible system of 
    # __cmp__ - __lt__ + __eq__

    def __lt__(self, o):
        t1, t2 = _compare(self, o)
        return t1 < t2

    def __eq__(self, o):
        t1, t2 = _compare(self, o)
        return t1 == t2

    def __repr__(self):
        L = ["%s=%r" % (key, getattr(self, key, ''))
             for key in self.__class__.c.keys()]
        return '%s(%s)' % (self.__class__.__name__, ','.join(L))

    for m in ['__eq__', '__repr__', '__lt__']:
        setattr(klass, m, eval(m))
    klass._table = selectable
    klass.c = expression.ColumnCollection()
    mappr = mapper(klass,
                   selectable,
                   extension=AutoAdd(session),
                   **mapper_kwargs)

    for k in mappr.iterate_properties:
        klass.c[k.key] = k.columns[0]

    klass._query = session.query_property()
    return klass

class SqlSoup(object):
    """Represent an ORM-wrapped database resource."""

    def __init__(self, engine_or_metadata, base=object, session=None):
        """Initialize a new :class:`.SqlSoup`.

        :param engine_or_metadata: a string database URL, :class:`.Engine` 
          or :class:`.MetaData` object to associate with. If the
          argument is a :class:`.MetaData`, it should be *bound*
          to an :class:`.Engine`.
        :param base: a class which will serve as the default class for 
          returned mapped classes.  Defaults to ``object``.
        :param session: a :class:`.ScopedSession` or :class:`.Session` with
          which to associate ORM operations for this :class:`.SqlSoup` instance.
          If ``None``, a :class:`.ScopedSession` that's local to this 
          module is used.

        """

        self.session = session or Session
        self.base=base

        if isinstance(engine_or_metadata, MetaData):
            self._metadata = engine_or_metadata
        elif isinstance(engine_or_metadata, (basestring, Engine)):
            self._metadata = MetaData(engine_or_metadata)
        else:
            raise ArgumentError("invalid engine or metadata argument %r" % 
                                engine_or_metadata)

        self._cache = {}
        self.schema = None

    @property
    def bind(self):
        """The :class:`.Engine` associated with this :class:`.SqlSoup`."""
        return self._metadata.bind

    engine = bind

    def delete(self, instance):
        """Mark an instance as deleted."""

        self.session.delete(instance)

    def execute(self, stmt, **params):
        """Execute a SQL statement.

        The statement may be a string SQL string,
        an :func:`.expression.select` construct, or an :func:`.expression.text` 
        construct.

        """
        return self.session.execute(sql.text(stmt, bind=self.bind), **params)

    @property
    def _underlying_session(self):
        if isinstance(self.session, session.Session):
            return self.session
        else:
            return self.session()

    def connection(self):
        """Return the current :class:`.Connection` in use by the current transaction."""

        return self._underlying_session._connection_for_bind(self.bind)

    def flush(self):
        """Flush pending changes to the database.

        See :meth:`.Session.flush`.

        """
        self.session.flush()

    def rollback(self):
        """Rollback the current transction.

        See :meth:`.Session.rollback`.

        """
        self.session.rollback()

    def commit(self):
        """Commit the current transaction.

        See :meth:`.Session.commit`.

        """
        self.session.commit()

    def clear(self):
        """Synonym for :meth:`.SqlSoup.expunge_all`."""

        self.session.expunge_all()

    def expunge(self, instance):
        """Remove an instance from the :class:`.Session`.

        See :meth:`.Session.expunge`.

        """
        self.session.expunge(instance)

    def expunge_all(self):
        """Clear all objects from the current :class:`.Session`.

        See :meth:`.Session.expunge_all`.

        """
        self.session.expunge_all()

    def map_to(self, attrname, tablename=None, selectable=None, 
                    schema=None, base=None, mapper_args=util.frozendict()):
        """Configure a mapping to the given attrname.

        This is the "master" method that can be used to create any 
        configuration.

        (new in 0.6.6)

        :param attrname: String attribute name which will be
          established as an attribute on this :class:.`.SqlSoup`
          instance.
        :param base: a Python class which will be used as the
          base for the mapped class. If ``None``, the "base"
          argument specified by this :class:`.SqlSoup`
          instance's constructor will be used, which defaults to
          ``object``.
        :param mapper_args: Dictionary of arguments which will
          be passed directly to :func:`.orm.mapper`.
        :param tablename: String name of a :class:`.Table` to be
          reflected. If a :class:`.Table` is already available,
          use the ``selectable`` argument. This argument is
          mutually exclusive versus the ``selectable`` argument.
        :param selectable: a :class:`.Table`, :class:`.Join`, or
          :class:`.Select` object which will be mapped. This
          argument is mutually exclusive versus the ``tablename``
          argument.
        :param schema: String schema name to use if the
          ``tablename`` argument is present.


        """
        if attrname in self._cache:
            raise InvalidRequestError(
                "Attribute '%s' is already mapped to '%s'" % (
                attrname,
                class_mapper(self._cache[attrname]).mapped_table
            ))

        if tablename is not None:
            if not isinstance(tablename, basestring):
                raise ArgumentError("'tablename' argument must be a string."
                                    )
            if selectable is not None:
                raise ArgumentError("'tablename' and 'selectable' "
                                    "arguments are mutually exclusive")

            selectable = Table(tablename, 
                                        self._metadata, 
                                        autoload=True, 
                                        autoload_with=self.bind, 
                                        schema=schema or self.schema)
        elif schema:
            raise ArgumentError("'tablename' argument is required when "
                                "using 'schema'.")
        elif selectable is not None:
            if not isinstance(selectable, expression.FromClause):
                raise ArgumentError("'selectable' argument must be a "
                                    "table, select, join, or other "
                                    "selectable construct.")
        else:
            raise ArgumentError("'tablename' or 'selectable' argument is "
                                    "required.")

        if not selectable.primary_key.columns:
            if tablename:
                raise PKNotFoundError(
                            "table '%s' does not have a primary "
                            "key defined" % tablename)
            else:
                raise PKNotFoundError(
                            "selectable '%s' does not have a primary "
                            "key defined" % selectable)

        mapped_cls = _class_for_table(
            self.session,
            self.engine,
            selectable,
            base or self.base,
            mapper_args
        )
        self._cache[attrname] = mapped_cls
        return mapped_cls


    def map(self, selectable, base=None, **mapper_args):
        """Map a selectable directly.

        The class and its mapping are not cached and will
        be discarded once dereferenced (as of 0.6.6).

        :param selectable: an :func:`.expression.select` construct.
        :param base: a Python class which will be used as the
          base for the mapped class. If ``None``, the "base"
          argument specified by this :class:`.SqlSoup`
          instance's constructor will be used, which defaults to
          ``object``.
        :param mapper_args: Dictionary of arguments which will
          be passed directly to :func:`.orm.mapper`.

        """

        return _class_for_table(
            self.session,
            self.engine,
            selectable,
            base or self.base,
            mapper_args
        )

    def with_labels(self, selectable, base=None, **mapper_args):
        """Map a selectable directly, wrapping the 
        selectable in a subquery with labels.

        The class and its mapping are not cached and will
        be discarded once dereferenced (as of 0.6.6).

        :param selectable: an :func:`.expression.select` construct.
        :param base: a Python class which will be used as the
          base for the mapped class. If ``None``, the "base"
          argument specified by this :class:`.SqlSoup`
          instance's constructor will be used, which defaults to
          ``object``.
        :param mapper_args: Dictionary of arguments which will
          be passed directly to :func:`.orm.mapper`.

        """

        # TODO give meaningful aliases
        return self.map(
                    expression._clause_element_as_expr(selectable).
                            select(use_labels=True).
                            alias('foo'), base=base, **mapper_args)

    def join(self, left, right, onclause=None, isouter=False, 
                base=None, **mapper_args):
        """Create an :func:`.expression.join` and map to it.

        The class and its mapping are not cached and will
        be discarded once dereferenced (as of 0.6.6).

        :param left: a mapped class or table object.
        :param right: a mapped class or table object.
        :param onclause: optional "ON" clause construct..
        :param isouter: if True, the join will be an OUTER join.
        :param base: a Python class which will be used as the
          base for the mapped class. If ``None``, the "base"
          argument specified by this :class:`.SqlSoup`
          instance's constructor will be used, which defaults to
          ``object``.
        :param mapper_args: Dictionary of arguments which will
          be passed directly to :func:`.orm.mapper`.

        """

        j = join(left, right, onclause=onclause, isouter=isouter)
        return self.map(j, base=base, **mapper_args)

    def entity(self, attr, schema=None):
        """Return the named entity from this :class:`.SqlSoup`, or 
        create if not present.

        For more generalized mapping, see :meth:`.map_to`.

        """
        try:
            return self._cache[attr]
        except KeyError, ke:
            return self.map_to(attr, tablename=attr, schema=schema)

    def __getattr__(self, attr):
        return self.entity(attr)

    def __repr__(self):
        return 'SqlSoup(%r)' % self._metadata

