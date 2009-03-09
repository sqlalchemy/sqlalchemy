"""
Introduction
============

SqlSoup provides a convenient way to access database tables without
having to declare table or mapper classes ahead of time.

Suppose we have a database with users, books, and loans tables
(corresponding to the PyWebOff dataset, if you're curious).  For
testing purposes, we'll create this db as follows::

    >>> from sqlalchemy import create_engine
    >>> e = create_engine('sqlite:///:memory:')
    >>> for sql in _testsql: e.execute(sql) #doctest: +ELLIPSIS
    <...

Creating a SqlSoup gateway is just like creating an SQLAlchemy
engine::

    >>> from sqlalchemy.ext.sqlsoup import SqlSoup
    >>> db = SqlSoup('sqlite:///:memory:')

or, you can re-use an existing metadata or engine::

    >>> db = SqlSoup(MetaData(e))

You can optionally specify a schema within the database for your
SqlSoup::

    # >>> db.schema = myschemaname


Loading objects
===============

Loading objects is as easy as this::

    >>> users = db.users.all()
    >>> users.sort()
    >>> users
    [MappedUsers(name=u'Joe Student',email=u'student@example.edu',password=u'student',classname=None,admin=0), MappedUsers(name=u'Bhargan Basepair',email=u'basepair@example.edu',password=u'basepair',classname=None,admin=1)]

Of course, letting the database do the sort is better::

    >>> db.users.order_by(db.users.name).all()
    [MappedUsers(name=u'Bhargan Basepair',email=u'basepair@example.edu',password=u'basepair',classname=None,admin=1), MappedUsers(name=u'Joe Student',email=u'student@example.edu',password=u'student',classname=None,admin=0)]

Field access is intuitive::

    >>> users[0].email
    u'student@example.edu'

Of course, you don't want to load all users very often.  Let's add a
WHERE clause.  Let's also switch the order_by to DESC while we're at
it::

    >>> from sqlalchemy import or_, and_, desc
    >>> where = or_(db.users.name=='Bhargan Basepair', db.users.email=='student@example.edu')
    >>> db.users.filter(where).order_by(desc(db.users.name)).all()
    [MappedUsers(name=u'Joe Student',email=u'student@example.edu',password=u'student',classname=None,admin=0), MappedUsers(name=u'Bhargan Basepair',email=u'basepair@example.edu',password=u'basepair',classname=None,admin=1)]

You can also use .first() (to retrieve only the first object from a query) or
.one() (like .first when you expect exactly one user -- it will raise an
exception if more were returned)::

    >>> db.users.filter(db.users.name=='Bhargan Basepair').one()
    MappedUsers(name=u'Bhargan Basepair',email=u'basepair@example.edu',password=u'basepair',classname=None,admin=1)

Since name is the primary key, this is equivalent to

    >>> db.users.get('Bhargan Basepair')
    MappedUsers(name=u'Bhargan Basepair',email=u'basepair@example.edu',password=u'basepair',classname=None,admin=1)

This is also equivalent to

    >>> db.users.filter_by(name='Bhargan Basepair').one()
    MappedUsers(name=u'Bhargan Basepair',email=u'basepair@example.edu',password=u'basepair',classname=None,admin=1)

filter_by is like filter, but takes kwargs instead of full clause expressions.
This makes it more concise for simple queries like this, but you can't do
complex queries like the or\_ above or non-equality based comparisons this way.

Full query documentation
------------------------

Get, filter, filter_by, order_by, limit, and the rest of the
query methods are explained in detail in the `SQLAlchemy documentation`__.

__ http://www.sqlalchemy.org/docs/04/ormtutorial.html#datamapping_querying


Modifying objects
=================

Modifying objects is intuitive::

    >>> user = _
    >>> user.email = 'basepair+nospam@example.edu'
    >>> db.flush()

(SqlSoup leverages the sophisticated SQLAlchemy unit-of-work code, so
multiple updates to a single object will be turned into a single
``UPDATE`` statement when you flush.)

To finish covering the basics, let's insert a new loan, then delete
it::

    >>> book_id = db.books.filter_by(title='Regional Variation in Moss').first().id
    >>> db.loans.insert(book_id=book_id, user_name=user.name)
    MappedLoans(book_id=2,user_name=u'Bhargan Basepair',loan_date=None)
    >>> db.flush()

    >>> loan = db.loans.filter_by(book_id=2, user_name='Bhargan Basepair').one()
    >>> db.delete(loan)
    >>> db.flush()

You can also delete rows that have not been loaded as objects. Let's
do our insert/delete cycle once more, this time using the loans
table's delete method. (For SQLAlchemy experts: note that no flush()
call is required since this delete acts at the SQL level, not at the
Mapper level.) The same where-clause construction rules apply here as
to the select methods.

::

    >>> db.loans.insert(book_id=book_id, user_name=user.name)
    MappedLoans(book_id=2,user_name=u'Bhargan Basepair',loan_date=None)
    >>> db.flush()
    >>> db.loans.delete(db.loans.book_id==2)

You can similarly update multiple rows at once. This will change the
book_id to 1 in all loans whose book_id is 2::

    >>> db.loans.update(db.loans.book_id==2, book_id=1)
    >>> db.loans.filter_by(book_id=1).all()
    [MappedLoans(book_id=1,user_name=u'Joe Student',loan_date=datetime.datetime(2006, 7, 12, 0, 0))]


Joins
=====

Occasionally, you will want to pull out a lot of data from related
tables all at once.  In this situation, it is far more efficient to
have the database perform the necessary join.  (Here we do not have *a
lot of data* but hopefully the concept is still clear.)  SQLAlchemy is
smart enough to recognize that loans has a foreign key to users, and
uses that as the join condition automatically.

::

    >>> join1 = db.join(db.users, db.loans, isouter=True)
    >>> join1.filter_by(name='Joe Student').all()
    [MappedJoin(name=u'Joe Student',email=u'student@example.edu',password=u'student',classname=None,admin=0,book_id=1,user_name=u'Joe Student',loan_date=datetime.datetime(2006, 7, 12, 0, 0))]

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
    [MappedJoin(name=u'Joe Student',email=u'student@example.edu',password=u'student',classname=None,admin=0,book_id=1,user_name=u'Joe Student',loan_date=datetime.datetime(2006, 7, 12, 0, 0),id=1,title=u'Mustards I Have Known',published_year=u'1989',authors=u'Jones')]

If you join tables that have an identical column name, wrap your join
with `with_labels`, to disambiguate columns with their table name
(.c is short for .columns)::

    >>> db.with_labels(join1).c.keys()
    [u'users_name', u'users_email', u'users_password', u'users_classname', u'users_admin', u'loans_book_id', u'loans_user_name', u'loans_loan_date']

You can also join directly to a labeled object::

    >>> labeled_loans = db.with_labels(db.loans)
    >>> db.join(db.users, labeled_loans, isouter=True).c.keys()
    [u'name', u'email', u'password', u'classname', u'admin', u'loans_book_id', u'loans_user_name', u'loans_loan_date']


Relations
=========

You can define relations on SqlSoup classes:

    >>> db.users.relate('loans', db.loans)

These can then be used like a normal SA property:

    >>> db.users.get('Joe Student').loans
    [MappedLoans(book_id=1,user_name=u'Joe Student',loan_date=datetime.datetime(2006, 7, 12, 0, 0))]

    >>> db.users.filter(~db.users.loans.any()).all()
    [MappedUsers(name=u'Bhargan Basepair',email='basepair+nospam@example.edu',password=u'basepair',classname=None,admin=1)]


relate can take any options that the relation function accepts in normal mapper definition:

    >>> del db._cache['users']
    >>> db.users.relate('loans', db.loans, order_by=db.loans.loan_date, cascade='all, delete-orphan')


Advanced Use
============

Accessing the Session
---------------------

SqlSoup uses a ScopedSession to provide thread-local sessions.  You
can get a reference to the current one like this::

    >>> from sqlalchemy.ext.sqlsoup import Session
    >>> session = Session()

Now you have access to all the standard session-based SA features,
such as transactions.  (SqlSoup's ``flush()`` is normally
transactionalized, but you can perform manual transaction management
if you need a transaction to span multiple flushes.)


Mapping arbitrary Selectables
-----------------------------

SqlSoup can map any SQLAlchemy ``Selectable`` with the map
method. Let's map a ``Select`` object that uses an aggregate function;
we'll use the SQLAlchemy ``Table`` that SqlSoup introspected as the
basis. (Since we're not mapping to a simple table or join, we need to
tell SQLAlchemy how to find the *primary key* which just needs to be
unique within the select, and not necessarily correspond to a *real*
PK in the database.)

::

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

SqlSoup works fine with SQLAlchemy's `text block support`__.

__ http://www.sqlalchemy.org/docs/04/sqlexpression.html#sql_text

You can also access the SqlSoup's `engine` attribute to compose SQL
directly.  The engine's ``execute`` method corresponds to the one of a
DBAPI cursor, and returns a ``ResultProxy`` that has ``fetch`` methods
you would also see on a cursor::

    >>> rp = db.bind.execute('select name, email from users order by name')
    >>> for name, email in rp.fetchall(): print name, email
    Bhargan Basepair basepair+nospam@example.edu
    Joe Student student@example.edu

You can also pass this engine object to other SQLAlchemy constructs.


Dynamic table names
-------------------

You can load a table whose name is specified at runtime with the entity() method:

    >>> tablename = 'loans'
    >>> db.entity(tablename) == db.loans
    True

entity() also takes an optional schema argument.  If none is specified, the
default schema is used.


Extra tests
===========

Boring tests here.  Nothing of real expository value.

::

    >>> db.users.filter_by(classname=None).order_by(db.users.name).all()
    [MappedUsers(name=u'Bhargan Basepair',email=u'basepair+nospam@example.edu',password=u'basepair',classname=None,admin=1), MappedUsers(name=u'Joe Student',email=u'student@example.edu',password=u'student',classname=None,admin=0)]

    >>> db.nopk
    Traceback (most recent call last):
    ...
    PKNotFoundError: table 'nopk' does not have a primary key defined [columns: i]

    >>> db.nosuchtable
    Traceback (most recent call last):
    ...
    NoSuchTableError: nosuchtable

    >>> years_with_count.insert(published_year='2007', n=1)
    Traceback (most recent call last):
    ...
    InvalidRequestError: SQLSoup can only modify mapped Tables (found: Alias)

    [tests clear()]
    >>> db.loans.count()
    1
    >>> _ = db.loans.insert(book_id=1, user_name='Bhargan Basepair')
    >>> db.expunge_all()
    >>> db.flush()
    >>> db.loans.count()
    1
"""

from sqlalchemy import Table, MetaData, join
from sqlalchemy import schema, sql
from sqlalchemy.orm import scoped_session, sessionmaker, mapper, class_mapper, relation
from sqlalchemy.exceptions import SQLAlchemyError, InvalidRequestError
from sqlalchemy.sql import expression

_testsql = """
CREATE TABLE books (
    id                   integer PRIMARY KEY, -- auto-increments in sqlite
    title                text NOT NULL,
    published_year       char(4) NOT NULL,
    authors              text NOT NULL
);

CREATE TABLE users (
    name                 varchar(32) PRIMARY KEY,
    email                varchar(128) NOT NULL,
    password             varchar(128) NOT NULL,
    classname            text,
    admin                int NOT NULL -- 0 = false
);

CREATE TABLE loans (
    book_id              int PRIMARY KEY REFERENCES books(id),
    user_name            varchar(32) references users(name)
        ON DELETE SET NULL ON UPDATE CASCADE,
    loan_date            datetime DEFAULT current_timestamp
);

insert into users(name, email, password, admin)
values('Bhargan Basepair', 'basepair@example.edu', 'basepair', 1);
insert into users(name, email, password, admin)
values('Joe Student', 'student@example.edu', 'student', 0);

insert into books(title, published_year, authors)
values('Mustards I Have Known', '1989', 'Jones');
insert into books(title, published_year, authors)
values('Regional Variation in Moss', '1971', 'Flim and Flam');

insert into loans(book_id, user_name, loan_date)
values (
    (select min(id) from books),
    (select name from users where name like 'Joe%'),
    '2006-07-12 0:0:0')
;

CREATE TABLE nopk (
    i                    int
);
""".split(';')

__all__ = ['PKNotFoundError', 'SqlSoup']

Session = scoped_session(sessionmaker(autoflush=True))

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

    def delete(cls, *args, **kwargs):
        _ddl_error(cls)

    def update(cls, whereclause=None, values=None, **kwargs):
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

    def delete(cls, *args, **kwargs):
        cls._table.delete(*args, **kwargs).execute()

    def update(cls, whereclause=None, values=None, **kwargs):
        cls._table.update(whereclause, values).execute(**kwargs)

    def relate(cls, propname, *args, **kwargs):
        class_mapper(cls)._configure_property(propname, relation(*args, **kwargs))

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

def class_for_table(selectable, **mapper_kwargs):
    selectable = expression._clause_element_as_expr(selectable)
    mapname = 'Mapped' + _selectable_name(selectable)
    if isinstance(mapname, unicode): 
        engine_encoding = selectable.metadata.bind.dialect.encoding 
        mapname = mapname.encode(engine_encoding)
    if isinstance(selectable, Table):
        klass = TableClassType(mapname, (object,), {})
    else:
        klass = SelectableClassType(mapname, (object,), {})
    
    def __cmp__(self, o):
        L = self.__class__.c.keys()
        L.sort()
        t1 = [getattr(self, k) for k in L]
        try:
            t2 = [getattr(o, k) for k in L]
        except AttributeError:
            raise TypeError('unable to compare with %s' % o.__class__)
        return cmp(t1, t2)

    def __repr__(self):
        L = ["%s=%r" % (key, getattr(self, key, ''))
             for key in self.__class__.c.keys()]
        return '%s(%s)' % (self.__class__.__name__, ','.join(L))

    for m in ['__cmp__', '__repr__']:
        setattr(klass, m, eval(m))
    klass._table = selectable
    klass.c = expression.ColumnCollection()
    mappr = mapper(klass,
                   selectable,
                   extension=Session.extension,
                   allow_null_pks=_is_outer_join(selectable),
                   **mapper_kwargs)
                   
    for k in mappr.iterate_properties:
        klass.c[k.key] = k.columns[0]

    klass._query = Session.query_property()
    return klass

class SqlSoup:
    def __init__(self, *args, **kwargs):
        """Initialize a new ``SqlSoup``.

        `args` may either be an ``SQLEngine`` or a set of arguments
        suitable for passing to ``create_engine``.
        """

        # meh, sometimes having method overloading instead of kwargs would be easier
        if isinstance(args[0], MetaData):
            args = list(args)
            metadata = args.pop(0)
            if args or kwargs:
                raise ArgumentError('Extra arguments not allowed when metadata is given')
        else:
            metadata = MetaData(*args, **kwargs)
        self._metadata = metadata
        self._cache = {}
        self.schema = None

    def engine(self):
        return self._metadata.bind

    engine = property(engine)
    bind = engine

    def delete(self, *args, **kwargs):
        Session.delete(*args, **kwargs)

    def flush(self):
        Session.flush()

    def clear(self):
        Session.expunge_all()

    def expunge_all(self):
        Session.expunge_all()

    def map(self, selectable, **kwargs):
        try:
            t = self._cache[selectable]
        except KeyError:
            t = class_for_table(selectable, **kwargs)
            self._cache[selectable] = t
        return t

    def with_labels(self, item):
        # TODO give meaningful aliases
        return self.map(expression._clause_element_as_expr(item).select(use_labels=True).alias('foo'))

    def join(self, *args, **kwargs):
        j = join(*args, **kwargs)
        return self.map(j)

    def entity(self, attr, schema=None):
        try:
            t = self._cache[attr]
        except KeyError:
            table = Table(attr, self._metadata, autoload=True, schema=schema or self.schema)
            if not table.primary_key.columns:
                raise PKNotFoundError('table %r does not have a primary key defined [columns: %s]' % (attr, ','.join(table.c.keys())))
            if table.columns:
                t = class_for_table(table)
            else:
                t = None
            self._cache[attr] = t
        return t
    
    def __getattr__(self, attr):
        return self.entity(attr)

    def __repr__(self):
        return 'SqlSoup(%r)' % self._metadata

if __name__ == '__main__':
    import logging
    logging.basicConfig()
    import doctest
    doctest.testmod()
