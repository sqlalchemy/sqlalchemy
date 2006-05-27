"""
SqlSoup provides a convenient way to access database tables without having
to declare table or mapper classes ahead of time.

Suppose we have a database with users, books, and loans tables
(corresponding to the PyWebOff dataset, if you're curious).
For testing purposes, we can create this db as follows:

>>> from sqlalchemy import create_engine
>>> e = create_engine('sqlite:///:memory:')
>>> for sql in _testsql: e.execute(sql) #doctest: +ELLIPSIS
<...

Creating a SqlSoup gateway is just like creating an SqlAlchemy engine:
>>> from sqlalchemy.ext.sqlsoup import SqlSoup
>>> soup = SqlSoup('sqlite:///:memory:')

or, you can re-use an existing metadata:
>>> soup = SqlSoup(BoundMetaData(e))

Loading objects is as easy as this:
>>> users = soup.users.select()
>>> users.sort()
>>> users
[Class_Users(name='Bhargan Basepair',email='basepair@example.edu',password='basepair',classname=None,admin=1), Class_Users(name='Joe Student',email='student@example.edu',password='student',classname=None,admin=0)]

Of course, letting the database do the sort is better (".c" is short for ".columns"):
>>> soup.users.select(order_by=[soup.users.c.name])
[Class_Users(name='Bhargan Basepair',email='basepair@example.edu',password='basepair',classname=None,admin=1), Class_Users(name='Joe Student',email='student@example.edu',password='student',classname=None,admin=0)]

Field access is intuitive:
>>> users[0].email
u'basepair@example.edu'

Of course, you don't want to load all users very often.  The common case is to
select by a key or other field:
>>> soup.users.selectone_by(name='Bhargan Basepair')
Class_Users(name='Bhargan Basepair',email='basepair@example.edu',password='basepair',classname=None,admin=1)

All the SqlAlchemy mapper select variants (select, select_by, selectone, selectone_by, selectfirst, selectfirst_by)
are available.  See the SqlAlchemy documentation for details:
http://www.sqlalchemy.org/docs/sqlconstruction.myt

Modifying objects is intuitive:
>>> user = _
>>> user.email = 'basepair+nospam@example.edu'
>>> soup.flush()

(SqlSoup leverages the sophisticated SqlAlchemy unit-of-work code, so
multiple updates to a single object will be turned into a single UPDATE
statement when you flush.)

Finally, insert and delete.  Let's insert a new loan, then delete it:
>>> soup.loans.insert(book_id=soup.books.selectfirst(soup.books.c.title=='Regional Variation in Moss').id, user_name=user.name)
Class_Loans(book_id=2,user_name='Bhargan Basepair',loan_date=None)
>>> soup.flush()

>>> loan = soup.loans.selectone_by(book_id=2, user_name='Bhargan Basepair')
>>> soup.delete(loan)
>>> soup.flush()
"""

from sqlalchemy import *
from sqlalchemy.ext.sessioncontext import SessionContext
from sqlalchemy.ext.assignmapper import assign_mapper
from sqlalchemy.exceptions import *


_testsql = """
CREATE TABLE books (
    id                   integer PRIMARY KEY, -- auto-SERIAL in sqlite
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
    loan_date            date DEFAULT current_timestamp
);

insert into users(name, email, password, admin)
values('Bhargan Basepair', 'basepair@example.edu', 'basepair', 1);
insert into users(name, email, password, admin)
values('Joe Student', 'student@example.edu', 'student', 0);

insert into books(title, published_year, authors)
values('Mustards I Have Known', '1989', 'Jones');
insert into books(title, published_year, authors)
values('Regional Variation in Moss', '1971', 'Flim and Flam');

insert into loans(book_id, user_name)
values (
    (select min(id) from books), 
    (select name from users where name like 'Joe%'))
;
""".split(';')

__all__ = ['NoSuchTableError', 'SqlSoup']

#
# thread local SessionContext
#
class Objectstore(SessionContext):
    def __getattr__(self, key):
        return getattr(self.current, key)
    def get_session(self):
        return self.current

objectstore = Objectstore(create_session)

class NoSuchTableError(SQLAlchemyError): pass

# metaclass is necessary to expose class methods with getattr, e.g.
# we want to pass db.users.select through to users._mapper.select
class TableClassType(type):
    def insert(cls, **kwargs):
        o = cls()
        o.__dict__.update(kwargs)
        return o
    def __getattr__(cls, attr):
        if attr == '_mapper':
            # called during mapper init
            raise AttributeError()
        return getattr(cls._mapper, attr)

def class_for_table(table):
    klass = TableClassType('Class_' + table.name.capitalize(), (object,), {})
    def __repr__(self):
        import locale
        encoding = locale.getdefaultlocale()[1]
        L = []
        for k in self.__class__.c.keys():
            value = getattr(self, k, '')
            if isinstance(value, unicode):
                value = value.encode(encoding)
            L.append("%s=%r" % (k, value))
        return '%s(%s)' % (self.__class__.__name__, ','.join(L))
    klass.__repr__ = __repr__
    klass._mapper = mapper(klass, table, extension=objectstore.mapper_extension)
    return klass

class SqlSoup:
    def __init__(self, *args, **kwargs):
        """
        args may either be an SQLEngine or a set of arguments suitable
        for passing to create_engine
        """
        from sqlalchemy import MetaData
        # meh, sometimes having method overloading instead of kwargs would be easier
        if isinstance(args[0], MetaData):
            args = list(args)
            metadata = args.pop(0)
            if args or kwargs:
                raise ArgumentError('Extra arguments not allowed when metadata is given')
        else:
            metadata = BoundMetaData(*args, **kwargs)
        self._metadata = metadata
        self._cache = {}
    def delete(self, *args, **kwargs):
        objectstore.delete(*args, **kwargs)
    def flush(self):
        objectstore.get_session().flush()
    def rollback(self):
        objectstore.clear()
    def _reset(self):
        # for debugging
        self._cache = {}
        self.rollback()
    def __getattr__(self, attr):
        try:
            t = self._cache[attr]
        except KeyError:
            table = Table(attr, self._metadata, autoload=True)
            if table.columns:
                t = class_for_table(table)
            else:
                t = None
            self._cache[attr] = t
        if not t:
            raise NoSuchTableError('%s does not exist' % attr)
        return t

if __name__ == '__main__':
    import doctest
    doctest.testmod()
