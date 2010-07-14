.. _engines_toplevel:

================
Database Engines
================
The **Engine** is the starting point for any SQLAlchemy application.  It's "home base" for the actual database and its DBAPI, delivered to the SQLAlchemy application through a connection pool and a **Dialect**, which describes how to talk to a specific kind of database/DBAPI combination.   

The general structure is this::

                                         +-----------+                        __________
                                     /---|   Pool    |---\                   (__________)
                 +-------------+    /    +-----------+    \     +--------+   |          |
    connect() <--|   Engine    |---x                       x----| DBAPI  |---| database |
                 +-------------+    \    +-----------+    /     +--------+   |          |
                                     \---|  Dialect  |---/                   |__________|
                                         +-----------+                       (__________)

Where above, a :class:`~sqlalchemy.engine.base.Engine` references both a  :class:`~sqlalchemy.engine.base.Dialect` and :class:`~sqlalchemy.pool.Pool`, which together interpret the DBAPI's module functions as well as the behavior of the database.

Creating an engine is just a matter of issuing a single call, :func:`create_engine()`::

    engine = create_engine('postgresql://scott:tiger@localhost:5432/mydatabase')

The above engine invokes the ``postgresql`` dialect and a connection pool which references ``localhost:5432``.

Note that the appropriate usage of :func:`create_engine()` is once per particular configuration, held globally for the lifetime of a single application process (not including child processes via ``fork()`` - these would require a new engine).   A single :class:`~sqlalchemy.engine.base.Engine` manages connections on behalf of the process and is intended to be called upon in a concurrent fashion.   Creating engines for each particular operation is not the intended usage.

The engine can be used directly to issue SQL to the database.  The most generic way is to use connections, which you get via the ``connect()`` method::

    connection = engine.connect()
    result = connection.execute("select username from users")
    for row in result:
        print "username:", row['username']
    connection.close()

The connection is an instance of :class:`~sqlalchemy.engine.base.Connection`, which is a **proxy** object for an actual DBAPI connection.  The returned result is an instance of :class:`~sqlalchemy.engine.ResultProxy`, which acts very much like a DBAPI cursor.

When you say ``engine.connect()``, a new :class:`~sqlalchemy.engine.base.Connection` object is created, and a DBAPI connection is retrieved from the connection pool.  Later, when you call ``connection.close()``, the DBAPI connection is returned to the pool; nothing is actually "closed" from the perspective of the database.

To execute some SQL more quickly, you can skip the :class:`~sqlalchemy.engine.base.Connection` part and just say::

    result = engine.execute("select username from users")
    for row in result:
        print "username:", row['username']
    result.close()

Where above, the ``execute()`` method on the :class:`~sqlalchemy.engine.base.Engine` does the ``connect()`` part for you, and returns the :class:`~sqlalchemy.engine.base.ResultProxy` directly.  The actual :class:`~sqlalchemy.engine.base.Connection` is *inside* the :class:`~sqlalchemy.engine.base.ResultProxy`, waiting for you to finish reading the result.  In this case, when you ``close()`` the :class:`~sqlalchemy.engine.base.ResultProxy`, the underlying :class:`~sqlalchemy.engine.base.Connection` is closed, which returns the DBAPI connection to the pool.

To summarize the above two examples, when you use a :class:`~sqlalchemy.engine.base.Connection` object, it's known as **explicit execution**.  When you don't see the :class:`~sqlalchemy.engine.base.Connection` object, but you still use the ``execute()`` method on the :class:`~sqlalchemy.engine.base.Engine`, it's called **explicit, connectionless execution**.   A third variant of execution also exists called **implicit execution**; this will be described later.

The :class:`~sqlalchemy.engine.base.Engine` and :class:`~sqlalchemy.engine.base.Connection` can do a lot more than what we illustrated above; SQL strings are only its most rudimentary function.  Later chapters will describe how "constructed SQL" expressions can be used with engines; in many cases, you don't have to deal with the :class:`~sqlalchemy.engine.base.Engine` at all after it's created.  The Object Relational Mapper (ORM), an optional feature of SQLAlchemy, also uses the :class:`~sqlalchemy.engine.base.Engine` in order to get at connections; that's also a case where you can often create the engine once, and then forget about it.

.. _supported_dbapis:

Supported Databases
====================

SQLAlchemy includes many :class:`~sqlalchemy.engine.base.Dialect` implementations for various 
backends; each is described as its own package in the :ref:`sqlalchemy.dialects_toplevel` package.  A 
SQLAlchemy dialect always requires that an appropriate DBAPI driver is installed.

The table below summarizes the state of DBAPI support in SQLAlchemy 0.6.  The values 
translate as:

* yes / Python platform - The SQLAlchemy dialect is mostly or fully operational on the target platform.   
* yes / OS platform - The DBAPI supports that platform.
* no / Python platform - The DBAPI does not support that platform, or there is no SQLAlchemy dialect support.  
* no / OS platform - The DBAPI does not support that platform.
* partial - the DBAPI is partially usable on the target platform but has major unresolved issues.
* development - a development version of the dialect exists, but is not yet usable.
* thirdparty - the dialect itself is maintained by a third party, who should be consulted for
  information on current support.
* \* - indicates the given DBAPI is the "default" for SQLAlchemy, i.e. when just the database name is specified

=========================  ===========================  ===========  ===========   ===========  =================  ============
Driver                     Connect string               Py2K         Py3K          Jython       Unix               Windows
=========================  ===========================  ===========  ===========   ===========  =================  ============
**DB2/Informix IDS**
ibm-db_                    thirdparty                   thirdparty   thirdparty    thirdparty   thirdparty         thirdparty
**Firebird**
kinterbasdb_               ``firebird+kinterbasdb``\*   yes          development   no           yes                yes
**Informix**
informixdb_                ``informix+informixdb``\*    development  development   no           unknown            unknown
**MaxDB**
sapdb_                     ``maxdb+sapdb``\*            development  development   no           yes                unknown
**Microsoft Access**
pyodbc_                    ``access+pyodbc``\*          development  development   no           unknown            yes
**Microsoft SQL Server**
adodbapi_                  ``mssql+adodbapi``           development  development   no           no                 yes
`jTDS JDBC Driver`_        ``mssql+zxjdbc``             no           no            development  yes                yes
mxodbc_                    ``mssql+mxodbc``             yes          development   no           yes with FreeTDS_  yes
pyodbc_                    ``mssql+pyodbc``\*           yes          development   no           yes with FreeTDS_  yes
pymssql_                   ``mssql+pymssql``            yes          development   no           yes                yes
**MySQL**
`MySQL Connector/J`_       ``mysql+zxjdbc``             no           no            yes          yes                yes
`MySQL Connector/Python`_  ``mysql+mysqlconnector``     yes          partial       no           yes                yes
mysql-python_              ``mysql+mysqldb``\*          yes          development   no           yes                yes
OurSQL_                    ``mysql+oursql``             yes          partial       no           yes                yes
**Oracle**
cx_oracle_                 ``oracle+cx_oracle``\*       yes          development   no           yes                yes
`Oracle JDBC Driver`_      ``oracle+zxjdbc``            no           no            yes          yes                yes
**Postgresql**
pg8000_                    ``postgresql+pg8000``        yes          yes           no           yes                yes
`PostgreSQL JDBC Driver`_  ``postgresql+zxjdbc``        no           no            yes          yes                yes
psycopg2_                  ``postgresql+psycopg2``\*    yes          development   no           yes                yes
pypostgresql_              ``postgresql+pypostgresql``  no           yes           no           yes                yes
**SQLite**
pysqlite_                  ``sqlite+pysqlite``\*        yes          yes           no           yes                yes
sqlite3_                   ``sqlite+pysqlite``\*        yes          yes           no           yes                yes
**Sybase ASE**
mxodbc_                    ``sybase+mxodbc``            development  development   no           yes                yes
pyodbc_                    ``sybase+pyodbc``\*          partial      development   no           unknown            unknown
python-sybase_             ``sybase+pysybase``          partial      development   no           yes                yes
=========================  ===========================  ===========  ===========   ===========  =================  ============

.. _psycopg2: http://www.initd.org/
.. _pg8000: http://pybrary.net/pg8000/
.. _pypostgresql: http://python.projects.postgresql.org/
.. _mysql-python: http://sourceforge.net/projects/mysql-python
.. _MySQL Connector/Python: https://launchpad.net/myconnpy
.. _OurSQL: http://packages.python.org/oursql/
.. _PostgreSQL JDBC Driver: http://jdbc.postgresql.org/
.. _sqlite3: http://docs.python.org/library/sqlite3.html
.. _pysqlite: http://pypi.python.org/pypi/pysqlite/
.. _MySQL Connector/J: http://dev.mysql.com/downloads/connector/j/
.. _cx_Oracle: http://cx-oracle.sourceforge.net/
.. _Oracle JDBC Driver: http://www.oracle.com/technology/software/tech/java/sqlj_jdbc/index.html
.. _kinterbasdb:  http://firebirdsql.org/index.php?op=devel&sub=python
.. _pyodbc: http://code.google.com/p/pyodbc/
.. _mxodbc: http://www.egenix.com/products/python/mxODBC/
.. _FreeTDS: http://www.freetds.org/
.. _adodbapi: http://adodbapi.sourceforge.net/
.. _pymssql: http://code.google.com/p/pymssql/
.. _jTDS JDBC Driver: http://jtds.sourceforge.net/
.. _ibm-db: http://code.google.com/p/ibm-db/
.. _informixdb: http://informixdb.sourceforge.net/
.. _sapdb: http://www.sapdb.org/sapdbapi.html
.. _python-sybase: http://python-sybase.sourceforge.net/

Further detail on dialects is available at :ref:`sqlalchemy.dialects_toplevel` as well as additional notes on the wiki at `Database Notes <http://www.sqlalchemy.org/trac/wiki/DatabaseNotes>`_

create_engine() URL Arguments
==============================

SQLAlchemy indicates the source of an Engine strictly via `RFC-1738 <http://rfc.net/rfc1738.html>`_ style URLs, combined with optional keyword arguments to specify options for the Engine.  The form of the URL is:

    dialect+driver://username:password@host:port/database

Dialect names include the identifying name of the SQLAlchemy dialect which include ``sqlite``, ``mysql``, ``postgresql``, ``oracle``, ``mssql``, and ``firebird``.  The drivername is the name of the DBAPI to be used to connect to the database using all lowercase letters.   If not specified, a "default" DBAPI will be imported if available - this default is typically the most widely known driver available for that backend (i.e. cx_oracle, pysqlite/sqlite3, psycopg2, mysqldb).   For Jython connections, specify the `zxjdbc` driver, which is the JDBC-DBAPI bridge included with Jython.

.. sourcecode:: python+sql

    # postgresql - psycopg2 is the default driver.
    pg_db = create_engine('postgresql://scott:tiger@localhost/mydatabase')
    pg_db = create_engine('postgresql+psycopg2://scott:tiger@localhost/mydatabase')
    pg_db = create_engine('postgresql+pg8000://scott:tiger@localhost/mydatabase')
    pg_db = create_engine('postgresql+pypostgresql://scott:tiger@localhost/mydatabase')

    # postgresql on Jython
    pg_db = create_engine('postgresql+zxjdbc://scott:tiger@localhost/mydatabase')

    # mysql - MySQLdb (mysql-python) is the default driver
    mysql_db = create_engine('mysql://scott:tiger@localhost/foo')
    mysql_db = create_engine('mysql+mysqldb://scott:tiger@localhost/foo')

    # mysql on Jython
    mysql_db = create_engine('mysql+zxjdbc://localhost/foo')

    # mysql with pyodbc (buggy)
    mysql_db = create_engine('mysql+pyodbc://scott:tiger@some_dsn')

    # oracle - cx_oracle is the default driver
    oracle_db = create_engine('oracle://scott:tiger@127.0.0.1:1521/sidname')

    # oracle via TNS name
    oracle_db = create_engine('oracle+cx_oracle://scott:tiger@tnsname')

    # mssql using ODBC datasource names.  PyODBC is the default driver.
    mssql_db = create_engine('mssql://mydsn')
    mssql_db = create_engine('mssql+pyodbc://mydsn')
    mssql_db = create_engine('mssql+adodbapi://mydsn')
    mssql_db = create_engine('mssql+pyodbc://username:password@mydsn')

SQLite connects to file based databases.   The same URL format is used, omitting the hostname, and using the "file" portion as the filename of the database.   This has the effect of four slashes being present for an absolute file path::

    # sqlite://<nohostname>/<path>
    # where <path> is relative:
    sqlite_db = create_engine('sqlite:///foo.db')

    # or absolute, starting with a slash:
    sqlite_db = create_engine('sqlite:////absolute/path/to/foo.db')

To use a SQLite ``:memory:`` database, specify an empty URL::

    sqlite_memory_db = create_engine('sqlite://')

The :class:`~sqlalchemy.engine.base.Engine` will ask the connection pool for a connection when the ``connect()`` or ``execute()`` methods are called.  The default connection pool, :class:`~sqlalchemy.pool.QueuePool`, as well as the default connection pool used with SQLite, :class:`~sqlalchemy.pool.SingletonThreadPool`, will open connections to the database on an as-needed basis.  As concurrent statements are executed, :class:`~sqlalchemy.pool.QueuePool` will grow its pool of connections to a default size of five, and will allow a default "overflow" of ten.   Since the :class:`~sqlalchemy.engine.base.Engine` is essentially "home base" for the connection pool, it follows that you should keep a single :class:`~sqlalchemy.engine.base.Engine` per database established within an application, rather than creating a new one for each connection.

Custom DBAPI connect() arguments
--------------------------------

Custom arguments used when issuing the ``connect()`` call to the underlying DBAPI may be issued in three distinct ways.  String-based arguments can be passed directly from the URL string as query arguments:

.. sourcecode:: python+sql

    db = create_engine('postgresql://scott:tiger@localhost/test?argument1=foo&argument2=bar')

If SQLAlchemy's database connector is aware of a particular query argument, it may convert its type from string to its proper type.

:func:`~sqlalchemy.create_engine` also takes an argument ``connect_args`` which is an additional dictionary that will be passed to ``connect()``.  This can be used when arguments of a type other than string are required, and SQLAlchemy's database connector has no type conversion logic present for that parameter:

.. sourcecode:: python+sql

    db = create_engine('postgresql://scott:tiger@localhost/test', connect_args = {'argument1':17, 'argument2':'bar'})

The most customizable connection method of all is to pass a ``creator`` argument, which specifies a callable that returns a DBAPI connection:

.. sourcecode:: python+sql

    def connect():
        return psycopg.connect(user='scott', host='localhost')

    db = create_engine('postgresql://', creator=connect)

.. _create_engine_args:

Database Engine Options
========================

Keyword options can also be specified to :func:`~sqlalchemy.create_engine`, following the string URL as follows:

.. sourcecode:: python+sql

    db = create_engine('postgresql://...', encoding='latin1', echo=True)

Options common to all database dialects are described at :func:`~sqlalchemy.create_engine`.

More On Connections
====================

Recall from the beginning of this section that the Engine provides a ``connect()`` method which returns a :class:`~sqlalchemy.engine.base.Connection` object.  :class:`~sqlalchemy.engine.base.Connection` is a *proxy* object which maintains a reference to a DBAPI connection instance.  The ``close()`` method on :class:`~sqlalchemy.engine.base.Connection` does not actually close the DBAPI connection, but instead returns it to the connection pool referenced by the :class:`~sqlalchemy.engine.base.Engine`.  :class:`~sqlalchemy.engine.base.Connection` will also automatically return its resources to the connection pool when the object is garbage collected, i.e. its ``__del__()`` method is called.  When using the standard C implementation of Python, this method is usually called immediately as soon as the object is dereferenced.  With other Python implementations such as Jython, this is not so guaranteed.

The ``execute()`` methods on both :class:`~sqlalchemy.engine.base.Engine` and :class:`~sqlalchemy.engine.base.Connection` can also receive SQL clause constructs as well::

    connection = engine.connect()
    result = connection.execute(select([table1], table1.c.col1==5))
    for row in result:
        print row['col1'], row['col2']
    connection.close()

The above SQL construct is known as a ``select()``.  The full range of SQL constructs available are described in :ref:`sqlexpression_toplevel`.

Both :class:`~sqlalchemy.engine.base.Connection` and :class:`~sqlalchemy.engine.base.Engine` fulfill an interface known as :class:`~sqlalchemy.engine.base.Connectable` which specifies common functionality between the two objects, namely being able to call ``connect()`` to return a :class:`~sqlalchemy.engine.base.Connection` object (:class:`~sqlalchemy.engine.base.Connection` just returns itself), and being able to call ``execute()`` to get a result set.   Following this, most SQLAlchemy functions and objects which accept an :class:`~sqlalchemy.engine.base.Engine` as a parameter or attribute with which to execute SQL will also accept a :class:`~sqlalchemy.engine.base.Connection`.  This argument is named ``bind``::

    engine = create_engine('sqlite:///:memory:')

    # specify some Table metadata
    metadata = MetaData()
    table = Table('sometable', metadata, Column('col1', Integer))

    # create the table with the Engine
    table.create(bind=engine)

    # drop the table with a Connection off the Engine
    connection = engine.connect()
    table.drop(bind=connection)

.. index::
   single: thread safety; connections

Connection facts:

* the Connection object is **not thread-safe**.  While a Connection can be shared among threads using properly synchronized access, this is also not recommended as many DBAPIs have issues with, if not outright disallow, sharing of connection state between threads.
* The Connection object represents a single dbapi connection checked out from the connection pool.  In this state, the connection pool has no affect upon the connection, including its expiration or timeout state.  For the connection pool to properly manage connections, **connections should be returned to the connection pool (i.e. ``connection.close()``) whenever the connection is not in use**.  If your application has a need for management of multiple connections or is otherwise long running (this includes all web applications, threaded or not), don't hold a single connection open at the module level.

Using Transactions with Connection
===================================

The :class:`~sqlalchemy.engine.base.Connection` object provides a ``begin()`` method which returns a :class:`~sqlalchemy.engine.base.Transaction` object.  This object is usually used within a try/except clause so that it is guaranteed to ``rollback()`` or ``commit()``::

    trans = connection.begin()
    try:
        r1 = connection.execute(table1.select())
        connection.execute(table1.insert(), col1=7, col2='this is some data')
        trans.commit()
    except:
        trans.rollback()
        raise

The :class:`~sqlalchemy.engine.base.Transaction` object also handles "nested" behavior by keeping track of the outermost begin/commit pair.  In this example, two functions both issue a transaction on a Connection, but only the outermost Transaction object actually takes effect when it is committed.

.. sourcecode:: python+sql

    # method_a starts a transaction and calls method_b
    def method_a(connection):
        trans = connection.begin() # open a transaction
        try:
            method_b(connection)
            trans.commit()  # transaction is committed here
        except:
            trans.rollback() # this rolls back the transaction unconditionally
            raise

    # method_b also starts a transaction
    def method_b(connection):
        trans = connection.begin() # open a transaction - this runs in the context of method_a's transaction
        try:
            connection.execute("insert into mytable values ('bat', 'lala')")
            connection.execute(mytable.insert(), col1='bat', col2='lala')
            trans.commit()  # transaction is not committed yet
        except:
            trans.rollback() # this rolls back the transaction unconditionally
            raise

    # open a Connection and call method_a
    conn = engine.connect()
    method_a(conn)
    conn.close()

Above, ``method_a`` is called first, which calls ``connection.begin()``.  Then it calls ``method_b``. When ``method_b`` calls ``connection.begin()``, it just increments a counter that is decremented when it calls ``commit()``.  If either ``method_a`` or ``method_b`` calls ``rollback()``, the whole transaction is rolled back.  The transaction is not committed until ``method_a`` calls the ``commit()`` method.  This "nesting" behavior allows the creation of functions which "guarantee" that a transaction will be used if one was not already available, but will automatically participate in an enclosing transaction if one exists.

Note that SQLAlchemy's Object Relational Mapper also provides a way to control transaction scope at a higher level; this is described in :ref:`unitofwork_transaction`.

.. index::
   single: thread safety; transactions

Transaction Facts:

* the Transaction object, just like its parent Connection, is **not thread-safe**.

Understanding Autocommit
------------------------


The above transaction example illustrates how to use :class:`~sqlalchemy.engine.base.Transaction` so that several executions can take part in the same transaction.  What happens when we issue an INSERT, UPDATE or DELETE call without using :class:`~sqlalchemy.engine.base.Transaction`?  The answer is **autocommit**.  While many DBAPIs  implement a flag called ``autocommit``, the current SQLAlchemy behavior is such that it implements its own autocommit.  This is achieved by detecting statements which represent data-changing operations, i.e. INSERT, UPDATE, DELETE, etc., and then issuing a COMMIT automatically if no transaction is in progress.  The detection is based on compiled statement attributes, or in the case of a text-only statement via regular expressions.

.. sourcecode:: python+sql

    conn = engine.connect()
    conn.execute("INSERT INTO users VALUES (1, 'john')")  # autocommits

.. _dbengine_implicit:

Connectionless Execution, Implicit Execution
=============================================

Recall from the first section we mentioned executing with and without a :class:`~sqlalchemy.engine.base.Connection`.  ``Connectionless`` execution refers to calling the ``execute()`` method on an object which is not a :class:`~sqlalchemy.engine.base.Connection`, which could be on the :class:`~sqlalchemy.engine.base.Engine` itself, or could be a constructed SQL object.  When we say "implicit", we mean that we are calling the ``execute()`` method on an object which is neither a :class:`~sqlalchemy.engine.base.Connection` nor an :class:`~sqlalchemy.engine.base.Engine` object; this can only be used with constructed SQL objects which have their own ``execute()`` method, and can be "bound" to an :class:`~sqlalchemy.engine.base.Engine`.  A description of "constructed SQL objects" may be found in :ref:`sqlexpression_toplevel`.

A summary of all three methods follows below.  First, assume the usage of the following :class:`~sqlalchemy.schema.MetaData` and :class:`~sqlalchemy.schema.Table` objects; while we haven't yet introduced these concepts, for now you only need to know that we are representing a database table, and are creating an "executable" SQL construct which issues a statement to the database.  These objects are described in :ref:`metadata_toplevel`.

.. sourcecode:: python+sql

    meta = MetaData()
    users_table = Table('users', meta,
        Column('id', Integer, primary_key=True),
        Column('name', String(50))
    )

Explicit execution delivers the SQL text or constructed SQL expression to the ``execute()`` method of :class:`~sqlalchemy.engine.base.Connection`:

.. sourcecode:: python+sql

    engine = create_engine('sqlite:///file.db')
    connection = engine.connect()
    result = connection.execute(users_table.select())
    for row in result:
        # ....
    connection.close()

Explicit, connectionless execution delivers the expression to the ``execute()`` method of :class:`~sqlalchemy.engine.base.Engine`:

.. sourcecode:: python+sql

    engine = create_engine('sqlite:///file.db')
    result = engine.execute(users_table.select())
    for row in result:
        # ....
    result.close()

Implicit execution is also connectionless, and calls the ``execute()`` method on the expression itself, utilizing the fact that either an :class:`~sqlalchemy.engine.base.Engine` or :class:`~sqlalchemy.engine.base.Connection` has been *bound* to the expression object (binding is discussed further in the next section, :ref:`metadata_toplevel`):

.. sourcecode:: python+sql

    engine = create_engine('sqlite:///file.db')
    meta.bind = engine
    result = users_table.select().execute()
    for row in result:
        # ....
    result.close()

In both "connectionless" examples, the :class:`~sqlalchemy.engine.base.Connection` is created behind the scenes; the :class:`~sqlalchemy.engine.base.ResultProxy` returned by the ``execute()`` call references the :class:`~sqlalchemy.engine.base.Connection` used to issue the SQL statement.   When we issue ``close()`` on the :class:`~sqlalchemy.engine.base.ResultProxy`, or if the result set object falls out of scope and is garbage collected, the underlying :class:`~sqlalchemy.engine.base.Connection` is closed for us, resulting in the DBAPI connection being returned to the pool.

.. _threadlocal_strategy:

Using the Threadlocal Execution Strategy
-----------------------------------------

The "threadlocal" engine strategy is used by non-ORM applications which wish to bind a transaction to the current thread, such that all parts of the application can participate in that transaction implicitly without the need to explicitly reference a :class:`~sqlalchemy.engine.base.Connection`.   "threadlocal" is designed for a very specific pattern of use, and is not appropriate unless this very specfic pattern, described below, is what's desired.  It has **no impact** on the "thread safety" of SQLAlchemy components or one's application.  It also should not be used when using an ORM :class:`~sqlalchemy.orm.session.Session` object, as the :class:`~sqlalchemy.orm.session.Session` itself represents an ongoing transaction and itself handles the job of maintaining connection and transactional resources.

Enabling ``threadlocal`` is achieved as follows:

.. sourcecode:: python+sql

    db = create_engine('mysql://localhost/test', strategy='threadlocal')

When the engine above is used in a "connectionless" style, meaning ``engine.execute()`` is called, a DBAPI connection is retrieved from the connection pool and then associated with the current thread.   Subsequent operations on the :class:`~sqlalchemy.engine.base.Engine` while the DBAPI connection remains checked out will make use of the *same* DBAPI connection object.  The connection stays allocated until all returned :class:`~sqlalchemy.engine.base.ResultProxy` objects are closed, which occurs for a particular :class:`~sqlalchemy.engine.base.ResultProxy` after all pending results are fetched, or immediately for an operation which returns no rows (such as an INSERT).

.. sourcecode:: python+sql

    # execute one statement and receive results.  r1 now references a DBAPI connection resource.
    r1 = db.execute("select * from table1")

    # execute a second statement and receive results.  r2 now references the *same* resource as r1
    r2 = db.execute("select * from table2")

    # fetch a row on r1 (assume more results are pending)
    row1 = r1.fetchone()

    # fetch a row on r2 (same)
    row2 = r2.fetchone()

    # close r1.  the connection is still held by r2.
    r1.close()

    # close r2.  with no more references to the underlying connection resources, they
    # are returned to the pool.
    r2.close()

The above example does not illustrate any pattern that is particularly useful, as it is not a frequent occurence that two execute/result fetching operations "leapfrog" one another.  There is a slight savings of connection pool checkout overhead between the two operations, and an implicit sharing of the same transactional context, but since there is no explicitly declared transaction, this association is short lived.

The real usage of "threadlocal" comes when we want several operations to occur within the scope of a shared transaction.  The :class:`~sqlalchemy.engine.base.Engine` now has ``begin()``, ``commit()`` and ``rollback()`` methods which will retrieve a connection resource from the pool and establish a new transaction, maintaining the connection against the current thread until the transaction is committed or rolled back:

.. sourcecode:: python+sql

    db.begin()
    try:
        call_operation1()
        call_operation2()
        db.commit()
    except:
        db.rollback()

``call_operation1()`` and ``call_operation2()`` can make use of the :class:`~sqlalchemy.engine.base.Engine` as a global variable, using the "connectionless" execution style, and their operations will participate in the same transaction:

.. sourcecode:: python+sql

    def call_operation1():
        engine.execute("insert into users values (?, ?)", 1, "john")

    def call_operation2():
        users.update(users.c.user_id==5).execute(name='ed')

When using threadlocal, operations that do call upon the ``engine.connect()`` method will receive a :class:`~sqlalchemy.engine.base.Connection` that is **outside** the scope of the transaction.  This can be used for operations such as logging the status of an operation regardless of transaction success:

.. sourcecode:: python+sql

    db.begin()
    conn = db.connect()
    try:
        conn.execute(log_table.insert(), message="Operation started")
        call_operation1()
        call_operation2()
        db.commit()
        conn.execute(log_table.insert(), message="Operation succeeded")
    except:
        db.rollback()
        conn.execute(log_table.insert(), message="Operation failed")
    finally:
        conn.close()

Functions which are written to use an explicit :class:`~sqlalchemy.engine.base.Connection` object, but wish to participate in the threadlocal transaction, can receive their :class:`~sqlalchemy.engine.base.Connection` object from the ``contextual_connect()`` method, which returns a :class:`~sqlalchemy.engine.base.Connection` that is **inside** the scope of the transaction:

.. sourcecode:: python+sql

    conn = db.contextual_connect()
    call_operation3(conn)
    conn.close()

Calling ``close()`` on the "contextual" connection does not release the connection resources to the pool if other resources are making use of it.  A resource-counting mechanism is employed so that the connection is released back to the pool only when all users of that connection, including the transaction established by ``engine.begin()``, have been completed.

So remember - if you're not sure if you need to use ``strategy="threadlocal"`` or not, the answer is **no** !  It's driven by a specific programming pattern that is generally not the norm.

.. _dbengine_logging:

Configuring Logging
====================

Python's standard `logging <http://www.python.org/doc/lib/module-logging.html>`_ module is used to implement informational and debug log output with SQLAlchemy.  This allows SQLAlchemy's logging to integrate in a standard way with other applications and libraries.  The ``echo`` and ``echo_pool`` flags that are present on :func:`~sqlalchemy.create_engine`, as well as the ``echo_uow`` flag used on :class:`~sqlalchemy.orm.session.Session`, all interact with regular loggers.

This section assumes familiarity with the above linked logging module.  All logging performed by SQLAlchemy exists underneath the ``sqlalchemy`` namespace, as used by ``logging.getLogger('sqlalchemy')``.  When logging has been configured (i.e. such as via ``logging.basicConfig()``), the general namespace of SA loggers that can be turned on is as follows:

* ``sqlalchemy.engine`` - controls SQL echoing.  set to ``logging.INFO`` for SQL query output, ``logging.DEBUG`` for query + result set output.
* ``sqlalchemy.dialects`` - controls custom logging for SQL dialects.  See the documentation of individual dialects for details. 
* ``sqlalchemy.pool`` - controls connection pool logging.  set to ``logging.INFO`` or lower to log connection pool checkouts/checkins.
* ``sqlalchemy.orm`` - controls logging of various ORM functions.  set to ``logging.INFO`` for configurational logging as well as unit of work dumps, ``logging.DEBUG`` for extensive logging during query and flush() operations.  Subcategories of ``sqlalchemy.orm`` include:
    * ``sqlalchemy.orm.attributes`` - logs certain instrumented attribute operations, such as triggered callables
    * ``sqlalchemy.orm.mapper`` - logs Mapper configuration and operations
    * ``sqlalchemy.orm.unitofwork`` - logs flush() operations, including dependency sort graphs and other operations
    * ``sqlalchemy.orm.strategies`` - logs relationship loader operations (i.e. lazy and eager loads)
    * ``sqlalchemy.orm.sync`` - logs synchronization of attributes from parent to child instances during a flush()

For example, to log SQL queries as well as unit of work debugging:

.. sourcecode:: python+sql

    import logging

    logging.basicConfig()
    logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
    logging.getLogger('sqlalchemy.orm.unitofwork').setLevel(logging.DEBUG)

By default, the log level is set to ``logging.ERROR`` within the entire ``sqlalchemy`` namespace so that no log operations occur, even within an application that has logging enabled otherwise.

The ``echo`` flags present as keyword arguments to :func:`~sqlalchemy.create_engine` and others as well as the ``echo`` property on :class:`~sqlalchemy.engine.base.Engine`, when set to ``True``, will first attempt to ensure that logging is enabled.  Unfortunately, the ``logging`` module provides no way of determining if output has already been configured (note we are referring to if a logging configuration has been set up, not just that the logging level is set).  For this reason, any ``echo=True`` flags will result in a call to ``logging.basicConfig()`` using sys.stdout as the destination.  It also sets up a default format using the level name, timestamp, and logger name.  Note that this configuration has the affect of being configured **in addition** to any existing logger configurations.  Therefore, **when using Python logging, ensure all echo flags are set to False at all times**, to avoid getting duplicate log lines.

The logger name of instance such as an :class:`~sqlalchemy.engine.base.Engine` or :class:`~sqlalchemy.pool.Pool` defaults to using a truncated hex identifier string.  To set this to a specific name, use the "logging_name" and "pool_logging_name" keyword arguments with :func:`sqlalchemy.create_engine`.
