from ez_setup import use_setuptools
use_setuptools()
from setuptools import setup, find_packages

setup(name = "SQLAlchemy",
    version = "0.91alpha",
    description = "Database Abstraction Library",
    author = "Mike Bayer",
    author_email = "mike_mp@zzzcomputing.com",
    url = "http://www.sqlalchemy.org",
    packages = find_packages('lib'),
    package_dir = {'':'lib'},
    license = "MIT License",
    long_description = """\
SQLAlchemy is:

- the Python SQL toolkit and Object Relational Mapper for application developers and programmers who want the full power and flexibility of SQL.

- a library that provides enterprise-level persistence patterns: eager loading of multiple types of objects using outer joins, Data Mapper, Unit of Work, all-or-nothing commits, bind parameters used for all literal values, batched updates and deletes.

- a set of distinct tools that build upon each other. The lower level tools, such as the connection pool, can be used completely independently of the higher levels, such as the data mapper. Higher levels always provide ways to affect and expose the lower levels, when customization is required.

- extremely easy to use for basic tasks, such as: get a thread-safe and pooled connection to a database; execute SQL queries constructed from Python expressions, strings, or combinations of both; load a bunch of objects from the database, modify their data, and commit only everything that changed in one thread-safe operation.

- powerful enough to use for complicated tasks, such as: load objects and their child objects all in one query via eager loading; map objects to arbitrary tables, joins and select statements; combine multiple tables together to load whole sets of related or unrelated objects from a single result set.

- high performing, allowing pre-compilation of SQL queries, heavy usage of bind parameters which allow a database to cache its query plans more effectively.

- extensible. Query compilation, data mapping, the typing system, interaction with DBAPIs can be extended and augmented in many ways.

SQLAlchemy's Philosophy:

- SQL databases behave less and less like object collections the more size and performance start to matter; object collections behave less and less like tables and rows the more abstraction starts to matter. SQLAlchemy aims to accomodate both of these principles.

SQLAlchemy includes:

- a connection pool, with the ability to transparently "wrap" any DBAPI module's connect() method into a thread-local and pooled resource.

- Python function-based query construction. Allows boolean expressions, operators, functions, table aliases, selectable subqueries, create/update/insert/delete queries, correlated updates, correlated EXISTS clauses, UNION clauses, inner and outer joins, bind parameters, free mixing of literal text within expressions, as little or as much as desired. Query-compilation is vendor-specific; the same query object can be compiled into any number of resulting SQL strings depending on its compilation algorithm.

- a table metadata description system, which can automatically load table data, or allow it to be described. Tables, foreign key constraints, and sequences can be created or dropped.

- support for Postgres (psycopg1/2), Oracle (cx_Oracle), SQLite (pysqlite), and MySQL (MySQLdb).

- an Object Relational Mapper that supports the Data Mapper algorithm, objects created across multiple tables, lazy or eager loading of related objects.

- a Unit Of Work system which organizes pending CRUD operations into queues and commits them all in one batch. Performs a topological "dependency sort" of all items to be committed and deleted and groups redundant statements together. This produces the maxiumum efficiency and transaction safety, and minimizes chances of deadlocks. Modeled after Fowler's "Unit of Work" pattern as well as Java Hibernate.

- automatic thread-local operation for: pooled connections, object identity maps, transactional contexts, units of work

SQLAlchemy has the advantages:

- database mapping and class design are totally separate. Persisted objects have no subclassing requirement (other than 'object') and are POPO's : plain old Python objects. They retain serializability (pickling) for usage in various caching systems and session objects. SQLAlchemy "decorates" classes with non-intrusive property accessors to automatically log object creates and modifications with the UnitOfWork engine, to lazyload related data, as well as to track attribute change histories.

- Custom list classes can be used with eagerly or lazily loaded child object lists, allowing rich relationships to be created on the fly as SQLAlchemy appends child objects to an object attribute.

- support for multiple (composite) primary keys, as well as support for "association" objects that represent the middle of a "many-to-many" relationship.

- support for self-referential mappers. Adjacency list structures can be created, saved, and deleted with proper cascading, with no extra programming.

- support for mapping objects from multiple tables, joins, and arbitrary select statements.

- any number of mappers can be created for a particular class, for classes that are persisted in more than one way. Mappers can create copies of themselves with modified behavior.

- an extension interface allows mapping behavior to be augmented or replaced within all mapping functions.

- data mapping can be used in a row-based manner. any bizarre hyper-optimized query that you or your DBA can cook up, you can run in SQLAlchemy, and as long as it returns the expected columns within a rowset, you can get your objects from it. For a rowset that contains more than one kind of object per row, multiple mappers can be chained together to return multiple object instance lists from a single database round trip.

- all generated queries are compiled to use bind parameters for all literals. This way databases can maximally optimize the caching of compiled query plans. All DBAPI2 bind parameter schemes, named and positional, are supported. SQLAlchemy always works with named parameters on the interface side for consistency and convenience; named parameters are then converted to positional upon execution for those DBAPI implementations that require it.

- a type system that allows pre- and post- processing of data, both at the bind parameter and the result set level. User-defined types can be freely mixed with built-in types. Generic types as well as SQL-specific types are available.

`Development SVN <http://svn.sqlalchemy.org/sqlalchemy/trunk#egg=sqlalchemy-dev>`_\
""",
    classifiers = [
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python",
        "Topic :: Database :: Front-Ends",
    ]
    )




