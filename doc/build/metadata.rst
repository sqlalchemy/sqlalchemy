.. _metadata_toplevel:

==================
Database Meta Data
==================

Describing Databases with MetaData
==================================

The core of SQLAlchemy's query and object mapping operations are supported by **database metadata**, which is comprised of Python objects that describe tables and other schema-level objects.  These objects can be created by explicitly naming the various components and their properties, using the Table, Column, ForeignKey, Index, and Sequence objects imported from ``sqlalchemy.schema``.  There is also support for **reflection** of some entities, which means you only specify the *name* of the entities and they are recreated from the database automatically.

A collection of metadata entities is stored in an object aptly named ``MetaData``::

    from sqlalchemy import *
    
    metadata = MetaData()

To represent a Table, use the ``Table`` class::

    users = Table('users', metadata, 
        Column('user_id', Integer, primary_key = True),
        Column('user_name', String(16), nullable = False),
        Column('email_address', String(60), key='email'),
        Column('password', String(20), nullable = False)
    )
    
    user_prefs = Table('user_prefs', metadata, 
        Column('pref_id', Integer, primary_key=True),
        Column('user_id', Integer, ForeignKey("users.user_id"), nullable=False),
        Column('pref_name', String(40), nullable=False),
        Column('pref_value', String(100))
    )

The specific datatypes for each Column, such as Integer, String, etc. are described in :mod:`sqlalchemy.types`, and exist within the module ``sqlalchemy.types`` as well as the global ``sqlalchemy`` namespace.

.. _metadata_foreignkeys:

Defining Foreign Keys
---------------------

Foreign keys are most easily specified by the ``ForeignKey`` object within a ``Column`` object.  For a composite foreign key, i.e. a foreign key that contains multiple columns referencing multiple columns to a composite primary key, an explicit syntax is provided which allows the correct table CREATE statements to be generated::

    # a table with a composite primary key
    invoices = Table('invoices', metadata, 
        Column('invoice_id', Integer, primary_key=True),
        Column('ref_num', Integer, primary_key=True),
        Column('description', String(60), nullable=False)
    )
    
    # a table with a composite foreign key referencing the parent table
    invoice_items = Table('invoice_items', metadata, 
        Column('item_id', Integer, primary_key=True),
        Column('item_name', String(60), nullable=False),
        Column('invoice_id', Integer, nullable=False),
        Column('ref_num', Integer, nullable=False),
        ForeignKeyConstraint(['invoice_id', 'ref_num'], ['invoices.invoice_id', 'invoices.ref_num'])
    )
    
Above, the ``invoice_items`` table will have ``ForeignKey`` objects automatically added to the ``invoice_id`` and ``ref_num`` ``Column`` objects as a result of the additional ``ForeignKeyConstraint`` object.

Accessing Tables and Columns
----------------------------

The ``MetaData`` object supports some handy methods, such as getting a list of Tables in the order (or reverse) of their dependency::

    >>> for t in metadata.table_iterator(reverse=False):
    ...    print t.name
    users
    user_prefs
        
And ``Table`` provides an interface to the table's properties as well as that of its columns::

    employees = Table('employees', metadata, 
        Column('employee_id', Integer, primary_key=True),
        Column('employee_name', String(60), nullable=False, key='name'),
        Column('employee_dept', Integer, ForeignKey("departments.department_id"))
    )
    
    # access the column "EMPLOYEE_ID":
    employees.columns.employee_id
    
    # or just
    employees.c.employee_id
    
    # via string
    employees.c['employee_id']
    
    # iterate through all columns
    for c in employees.c:
        print c
        
    # get the table's primary key columns
    for primary_key in employees.primary_key:
        print primary_key
    
    # get the table's foreign key objects:
    for fkey in employees.foreign_keys:
        print fkey
        
    # access the table's MetaData:
    employees.metadata
    
    # access the table's bound Engine or Connection, if its MetaData is bound:
    employees.bind
    
    # access a column's name, type, nullable, primary key, foreign key
    employees.c.employee_id.name
    employees.c.employee_id.type
    employees.c.employee_id.nullable
    employees.c.employee_id.primary_key
    employees.c.employee_dept.foreign_key
    
    # get the "key" of a column, which defaults to its name, but can 
    # be any user-defined string:
    employees.c.name.key
    
    # access a column's table:
    employees.c.employee_id.table is employees
    
    # get the table related by a foreign key
    fcolumn = employees.c.employee_dept.foreign_key.column.table

.. _metadata_binding:

Binding MetaData to an Engine or Connection 
--------------------------------------------

A ``MetaData`` object can be associated with an ``Engine`` or an individual ``Connection``; this process is called **binding**.  The term used to describe "an engine or a connection" is often referred to as a **connectable**.  Binding allows the ``MetaData`` and the elements which it contains to perform operations against the database directly, using the connection resources to which it's bound.   Common operations which are made more convenient through binding include being able to generate SQL constructs which know how to execute themselves, creating ``Table`` objects which query the database for their column and constraint information, and issuing CREATE or DROP statements.

To bind ``MetaData`` to an ``Engine``, use the ``bind`` attribute::

    engine = create_engine('sqlite://', **kwargs)
    
    # create MetaData 
    meta = MetaData()

    # bind to an engine
    meta.bind = engine

Once this is done, the ``MetaData`` and its contained ``Table`` objects can access the database directly::

    meta.create_all()  # issue CREATE statements for all tables
    
    # describe a table called 'users', query the database for its columns
    users_table = Table('users', meta, autoload=True)
    
    # generate a SELECT statement and execute
    result = users_table.select().execute()

Note that the feature of binding engines is **completely optional**.  All of the operations which take advantage of "bound" ``MetaData`` also can be given an ``Engine`` or ``Connection`` explicitly with which to perform the operation.   The equivalent "non-bound" of the above would be::

    meta.create_all(engine)  # issue CREATE statements for all tables
    
    # describe a table called 'users',  query the database for its columns
    users_table = Table('users', meta, autoload=True, autoload_with=engine)
    
    # generate a SELECT statement and execute
    result = engine.execute(users_table.select())

Reflecting Tables
-----------------


A ``Table`` object can be created without specifying any of its contained attributes, using the argument ``autoload=True`` in conjunction with the table's name and possibly its schema (if not the databases "default" schema).  (You can also specify a list or set of column names to autoload as the kwarg include_columns, if you only want to load a subset of the columns in the actual database.)  This will issue the appropriate queries to the database in order to locate all properties of the table required for SQLAlchemy to use it effectively, including its column names and datatypes, foreign and primary key constraints, and in some cases its default-value generating attributes.   To use ``autoload=True``, the table's ``MetaData`` object need be bound to an ``Engine`` or ``Connection``, or alternatively the ``autoload_with=<some connectable>`` argument can be passed.  Below we illustrate autoloading a table and then iterating through the names of its columns::

    >>> messages = Table('messages', meta, autoload=True)
    >>> [c.name for c in messages.columns]
    ['message_id', 'message_name', 'date']

Note that if a reflected table has a foreign key referencing another table, the related ``Table`` object  will be automatically created within the ``MetaData`` object if it does not exist already.  Below, suppose table ``shopping_cart_items`` references a table ``shopping_carts``.  After reflecting, the ``shopping carts`` table is present:

.. sourcecode:: pycon+sql

    >>> shopping_cart_items = Table('shopping_cart_items', meta, autoload=True)
    >>> 'shopping_carts' in meta.tables:
    True
        
To get direct access to 'shopping_carts', simply instantiate it via the ``Table`` constructor.  ``Table`` uses a special constructor that will return the already created ``Table`` instance if it's already present:

.. sourcecode:: python+sql

    shopping_carts = Table('shopping_carts', meta)

Of course, it's a good idea to use ``autoload=True`` with the above table regardless.  This is so that if it hadn't been loaded already, the operation will load the table.  The autoload operation only occurs for the table if it hasn't already been loaded; once loaded, new calls to ``Table`` will not re-issue any reflection queries.

Overriding Reflected Columns 
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


Individual columns can be overridden with explicit values when reflecting tables; this is handy for specifying custom datatypes, constraints such as primary keys that may not be configured within the database, etc.::

    >>> mytable = Table('mytable', meta,
    ... Column('id', Integer, primary_key=True),   # override reflected 'id' to have primary key
    ... Column('mydata', Unicode(50)),    # override reflected 'mydata' to be Unicode
    ... autoload=True)

Reflecting All Tables at Once 
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


The ``MetaData`` object can also get a listing of tables and reflect the full set.  This is achieved by using the ``reflect()`` method.  After calling it, all located tables are present within the ``MetaData`` object's dictionary of tables::

    meta = MetaData()
    meta.reflect(bind=someengine)
    users_table = meta.tables['users']
    addresses_table = meta.tables['addresses']
    
``metadata.reflect()`` is also a handy way to clear or drop all tables in a database::

    meta = MetaData()
    meta.reflect(bind=someengine)
    for table in reversed(meta.sorted_tables):
        someengine.execute(table.delete())

Specifying the Schema Name 
---------------------------


Some databases support the concept of multiple schemas.  A ``Table`` can reference this by specifying the ``schema`` keyword argument::

    financial_info = Table('financial_info', meta,
        Column('id', Integer, primary_key=True),
        Column('value', String(100), nullable=False),
        schema='remote_banks'
    )

Within the ``MetaData`` collection, this table will be identified by the combination of ``financial_info`` and ``remote_banks``.  If another table called ``financial_info`` is referenced without the ``remote_banks`` schema, it will refer to a different ``Table``.  ``ForeignKey`` objects can reference columns in this table using the form ``remote_banks.financial_info.id``.

ON UPDATE and ON DELETE 
------------------------


``ON UPDATE`` and ``ON DELETE`` clauses to a table create are specified within the ``ForeignKeyConstraint`` object, using the ``onupdate`` and ``ondelete`` keyword arguments::

    foobar = Table('foobar', meta,
        Column('id', Integer, primary_key=True),
        Column('lala', String(40)),
        ForeignKeyConstraint(['lala'],['hoho.lala'], onupdate="CASCADE", ondelete="CASCADE"))

Note that these clauses are not supported on SQLite, and require ``InnoDB`` tables when used with MySQL.  They may also not be supported on other databases.

Other Options 
--------------

``Tables`` may support database-specific options, such as MySQL's ``engine`` option that can specify "MyISAM", "InnoDB", and other backends for the table::

    addresses = Table('engine_email_addresses', meta,
        Column('address_id', Integer, primary_key = True),
        Column('remote_user_id', Integer, ForeignKey(users.c.user_id)),
        Column('email_address', String(20)),
        mysql_engine='InnoDB'
    )
    
Creating and Dropping Database Tables 
======================================

Creating and dropping individual tables can be done via the ``create()`` and ``drop()`` methods of ``Table``; these methods take an optional ``bind`` parameter which references an ``Engine`` or a ``Connection``.  If not supplied, the ``Engine`` bound to the ``MetaData`` will be used, else an error is raised:

.. sourcecode:: python+sql

    meta = MetaData()
    meta.bind = 'sqlite:///:memory:'

    employees = Table('employees', meta, 
        Column('employee_id', Integer, primary_key=True),
        Column('employee_name', String(60), nullable=False, key='name'),
        Column('employee_dept', Integer, ForeignKey("departments.department_id"))
    )
    {sql}employees.create()
    CREATE TABLE employees(
    employee_id SERIAL NOT NULL PRIMARY KEY,
    employee_name VARCHAR(60) NOT NULL,
    employee_dept INTEGER REFERENCES departments(department_id)
    )
    {}            

``drop()`` method:

.. sourcecode:: python+sql

    {sql}employees.drop(bind=e)
    DROP TABLE employees
    {}            

The ``create()`` and ``drop()`` methods also support an optional keyword argument ``checkfirst`` which will issue the database's appropriate pragma statements to check if the table exists before creating or dropping::

    employees.create(bind=e, checkfirst=True)
    employees.drop(checkfirst=False)
    
Entire groups of Tables can be created and dropped directly from the ``MetaData`` object with ``create_all()`` and ``drop_all()``.  These methods always check for the existence of each table before creating or dropping.  Each method takes an optional ``bind`` keyword argument which can reference an ``Engine`` or a ``Connection``.  If no engine is specified, the underlying bound ``Engine``,  if any, is used:

.. sourcecode:: python+sql

    engine = create_engine('sqlite:///:memory:')
    
    metadata = MetaData()
    
    users = Table('users', metadata, 
        Column('user_id', Integer, primary_key = True),
        Column('user_name', String(16), nullable = False),
        Column('email_address', String(60), key='email'),
        Column('password', String(20), nullable = False)
    )
    
    user_prefs = Table('user_prefs', metadata, 
        Column('pref_id', Integer, primary_key=True),
        Column('user_id', Integer, ForeignKey("users.user_id"), nullable=False),
        Column('pref_name', String(40), nullable=False),
        Column('pref_value', String(100))
    )
    
    {sql}metadata.create_all(bind=engine)
    PRAGMA table_info(users){}
    CREATE TABLE users(
            user_id INTEGER NOT NULL PRIMARY KEY, 
            user_name VARCHAR(16) NOT NULL, 
            email_address VARCHAR(60), 
            password VARCHAR(20) NOT NULL
    )
    PRAGMA table_info(user_prefs){}
    CREATE TABLE user_prefs(
            pref_id INTEGER NOT NULL PRIMARY KEY, 
            user_id INTEGER NOT NULL REFERENCES users(user_id), 
            pref_name VARCHAR(40) NOT NULL, 
            pref_value VARCHAR(100)
    )

Column Insert/Update Defaults 
==============================
    

SQLAlchemy includes several constructs which provide default values provided during INSERT and UPDATE statements.  The defaults may be provided as Python constants, Python functions, or SQL expressions, and the SQL expressions themselves may be "pre-executed", executed inline within the insert/update statement itself, or can be created as a SQL level "default" placed on the table definition itself.  A "default" value by definition is only invoked if no explicit value is passed into the INSERT or UPDATE statement.

Pre-Executed Python Functions 
------------------------------


The "default" keyword argument on Column can reference a Python value or callable which is invoked at the time of an insert::

    # a function which counts upwards
    i = 0
    def mydefault():
        global i
        i += 1
        return i

    t = Table("mytable", meta, 
        # function-based default
        Column('id', Integer, primary_key=True, default=mydefault),
    
        # a scalar default
        Column('key', String(10), default="default")
    )

Similarly, the "onupdate" keyword does the same thing for update statements:

.. sourcecode:: python+sql

    import datetime
    
    t = Table("mytable", meta, 
        Column('id', Integer, primary_key=True),
    
        # define 'last_updated' to be populated with datetime.now()
        Column('last_updated', DateTime, onupdate=datetime.datetime.now),
    )

Pre-executed and Inline SQL Expressions 
----------------------------------------


The "default" and "onupdate" keywords may also be passed SQL expressions, including select statements or direct function calls:

.. sourcecode:: python+sql

    t = Table("mytable", meta, 
        Column('id', Integer, primary_key=True),
    
        # define 'create_date' to default to now()
        Column('create_date', DateTime, default=func.now()),
    
        # define 'key' to pull its default from the 'keyvalues' table
        Column('key', String(20), default=keyvalues.select(keyvalues.c.type='type1', limit=1))

        # define 'last_modified' to use the current_timestamp SQL function on update
        Column('last_modified', DateTime, onupdate=func.current_timestamp())
        )

The above SQL functions are usually executed "inline" with the INSERT or UPDATE statement being executed.  In some cases, the function is "pre-executed" and its result pre-fetched explicitly.  This happens under the following circumstances:

* the column is a primary key column

* the database dialect does not support a usable ``cursor.lastrowid`` accessor (or equivalent); this currently includes PostgreSQL, Oracle, and Firebird.

* the statement is a single execution, i.e. only supplies one set of parameters and doesn't use "executemany" behavior

* the ``inline=True`` flag is not set on the ``Insert()`` or ``Update()`` construct.

For a statement execution which is not an executemany, the returned ``ResultProxy`` will contain a collection accessible via ``result.postfetch_cols()`` which contains a list of all ``Column`` objects which had an inline-executed default.  Similarly, all parameters which were bound to the statement, including all Python and SQL expressions which were pre-executed, are present in the ``last_inserted_params()`` or ``last_updated_params()`` collections on ``ResultProxy``.  The ``last_inserted_ids()`` collection contains a list of primary key values for the row inserted.  

DDL-Level Defaults 
-------------------


A variant on a SQL expression default is the ``server_default``, which gets placed in the CREATE TABLE statement during a ``create()`` operation:

.. sourcecode:: python+sql

    t = Table('test', meta,
        Column('abc', String(20), server_default='abc'),
        Column('created_at', DateTime, server_default=text("sysdate"))
    )

A create call for the above table will produce::

    CREATE TABLE test (
        abc varchar(20) default 'abc',
        created_at datetime default sysdate
    )

The behavior of ``server_default`` is similar to that of a regular SQL default; if it's placed on a primary key column for a database which doesn't have a way to "postfetch" the ID, and the statement is not "inlined", the SQL expression is pre-executed; otherwise, SQLAlchemy lets the default fire off on the database side normally.

Triggered Columns 
------------------

Columns with values set by a database trigger or other external process may be called out with a marker::

    t = Table('test', meta,
        Column('abc', String(20), server_default=FetchedValue())
        Column('def', String(20), server_onupdate=FetchedValue())
    )

These markers do not emit a ````default```` clause when the table is created, however they do set the same internal flags as a static ``server_default`` clause, providing hints to higher-level tools that a "post-fetch" of these rows should be performed after an insert or update.

Defining Sequences 
-------------------


A table with a sequence looks like:

.. sourcecode:: python+sql

    table = Table("cartitems", meta, 
        Column("cart_id", Integer, Sequence('cart_id_seq'), primary_key=True),
        Column("description", String(40)),
        Column("createdate", DateTime())
    )

The ``Sequence`` object works a lot like the ``default`` keyword on ``Column``, except that it only takes effect on a database which supports sequences.  When used with a database that does not support sequences, the ``Sequence`` object has no effect; therefore it's safe to place on a table which is used against multiple database backends.  The same rules for pre- and inline execution apply.

When the ``Sequence`` is associated with a table, CREATE and DROP statements issued for that table will also issue CREATE/DROP for the sequence object as well, thus "bundling" the sequence object with its parent table.

The flag ``optional=True`` on ``Sequence`` will produce a sequence that is only used on databases which have no "autoincrementing" capability.  For example, PostgreSQL supports primary key generation using the SERIAL keyword, whereas Oracle has no such capability.  Therefore, a ``Sequence`` placed on a primary key column with ``optional=True`` will only be used with an Oracle backend but not PostgreSQL.

A sequence can also be executed standalone, using an ``Engine`` or ``Connection``, returning its next value in a database-independent fashion:

.. sourcecode:: python+sql

    seq = Sequence('some_sequence')
    nextid = connection.execute(seq)

Defining Constraints and Indexes 
=================================


UNIQUE Constraint
-----------------


Unique constraints can be created anonymously on a single column using the ``unique`` keyword on ``Column``.  Explicitly named unique constraints and/or those with multiple columns are created via the ``UniqueConstraint`` table-level construct.

.. sourcecode:: python+sql

    meta = MetaData()
    mytable = Table('mytable', meta,
    
        # per-column anonymous unique constraint
        Column('col1', Integer, unique=True),
        
        Column('col2', Integer),
        Column('col3', Integer),
        
        # explicit/composite unique constraint.  'name' is optional.
        UniqueConstraint('col2', 'col3', name='uix_1')
        )

CHECK Constraint
----------------


Check constraints can be named or unnamed and can be created at the Column or Table level, using the ``CheckConstraint`` construct.  The text of the check constraint is passed directly through to the database, so there is limited "database independent" behavior.  Column level check constraints generally should only refer to the column to which they are placed, while table level constraints can refer to any columns in the table.

Note that some databases do not actively support check constraints such as MySQL and SQLite.

.. sourcecode:: python+sql

    meta = MetaData()
    mytable = Table('mytable', meta,
    
        # per-column CHECK constraint
        Column('col1', Integer, CheckConstraint('col1>5')),
        
        Column('col2', Integer),
        Column('col3', Integer),
        
        # table level CHECK constraint.  'name' is optional.
        CheckConstraint('col2 > col3 + 5', name='check1')
        )
    
Indexes
-------


Indexes can be created anonymously (using an auto-generated name "ix_&lt;column label&gt;") for a single column using the inline ``index`` keyword on ``Column``, which also modifies the usage of ``unique`` to apply the uniqueness to the index itself, instead of adding a separate UNIQUE constraint.  For indexes with specific names or which encompass more than one column, use the ``Index`` construct, which requires a name.  

Note that the ``Index`` construct is created **externally** to the table which it corresponds, using ``Column`` objects and not strings.

.. sourcecode:: python+sql

    meta = MetaData()
    mytable = Table('mytable', meta,
        # an indexed column, with index "ix_mytable_col1"
        Column('col1', Integer, index=True),

        # a uniquely indexed column with index "ix_mytable_col2"
        Column('col2', Integer, index=True, unique=True),

        Column('col3', Integer),
        Column('col4', Integer),

        Column('col5', Integer),
        Column('col6', Integer),
        )

    # place an index on col3, col4
    Index('idx_col34', mytable.c.col3, mytable.c.col4)

    # place a unique index on col5, col6
    Index('myindex', mytable.c.col5, mytable.c.col6, unique=True)

The ``Index`` objects will be created along with the CREATE statements for the table itself.  An index can also be created on its own independently of the table:

.. sourcecode:: python+sql

    # create a table
    sometable.create()

    # define an index
    i = Index('someindex', sometable.c.col5)

    # create the index, will use the table's bound connectable if the ``bind`` keyword argument not specified
    i.create()

Adapting Tables to Alternate Metadata 
======================================


A ``Table`` object created against a specific ``MetaData`` object can be re-created against a new MetaData using the ``tometadata`` method:

.. sourcecode:: python+sql

    # create two metadata
    meta1 = MetaData('sqlite:///querytest.db')
    meta2 = MetaData()
                        
    # load 'users' from the sqlite engine
    users_table = Table('users', meta1, autoload=True)
    
    # create the same Table object for the plain metadata
    users_table_2 = users_table.tometadata(meta2)
    
    
