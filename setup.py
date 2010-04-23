"""setup.py

Please see README for basic installation instructions.

"""

# If using distutils (not distribute/setuptools),
# set this flag to True to compile C extensions.
# Otherwise use --with-cextensions
BUILD_CEXTENSIONS = False

import os
import sys
import re

extra = {}
if sys.version_info >= (3, 0):
    # monkeypatch our preprocessor
    # onto the 2to3 tool.  
    from sa2to3 import refactor_string
    from lib2to3.refactor import RefactoringTool
    RefactoringTool.refactor_string = refactor_string

    extra.update(
        use_2to3=True,
    )

try:
    from setuptools import setup, Extension, Feature
except ImportError:
    from distutils.core import setup, Extension
    Feature = None

if Feature:
    extra.update(
        features = {'cextensions' : Feature(
            "optional C speed-enhancements",
            standard = False,
            ext_modules = [
                Extension('sqlalchemy.cprocessors',
                       sources=['lib/sqlalchemy/cextension/processors.c']),
                Extension('sqlalchemy.cresultproxy',
                       sources=['lib/sqlalchemy/cextension/resultproxy.c'])
            ],
        )}
    )
elif BUILD_CEXTENSIONS:
    extra.update(
        ext_modules = [
                Extension('sqlalchemy.cprocessors',
                      sources=['lib/sqlalchemy/cextension/processors.c']),
                Extension('sqlalchemy.cresultproxy',
                      sources=['lib/sqlalchemy/cextension/resultproxy.c'])
            ]
    )

def find_packages(dir_):
    packages = []
    for _dir, subdirectories, files in os.walk(os.path.join(dir_, 'sqlalchemy')):
        if '__init__.py' in files:
            lib, fragment = _dir.split(os.sep, 1)
            packages.append(fragment.replace(os.sep, '.'))
    return packages

if sys.version_info < (2, 4):
    raise Exception("SQLAlchemy requires Python 2.4 or higher.")

v = open(os.path.join(os.path.dirname(__file__), 'lib', 'sqlalchemy', '__init__.py'))
VERSION = re.compile(r".*__version__ = '(.*?)'", re.S).match(v.read()).group(1)
v.close()

setup(name = "SQLAlchemy",
      version = VERSION,
      description = "Database Abstraction Library",
      author = "Mike Bayer",
      author_email = "mike_mp@zzzcomputing.com",
      url = "http://www.sqlalchemy.org",
      packages = find_packages('lib'),
      package_dir = {'':'lib'},
      license = "MIT License",

      # TODO: this is nice, but Python 3 support ?
      # any way to make it not install for build/install ?
      #setup_requires=["setuptools_hg"],

      tests_require = ['nose >= 0.11'],
      test_suite = "nose.collector",
      entry_points = {
          'nose.plugins.0.10': [
              'sqlalchemy = sqlalchemy.test.noseplugin:NoseSQLAlchemy',
              ]
          },
      
      long_description = """\
SQLAlchemy is:

    * The Python SQL toolkit and Object Relational Mapper that gives application developers the full power and flexibility of SQL. SQLAlchemy provides a full suite of well known enterprise-level persistence patterns, designed for efficient and high-performing database access, adapted into a simple and Pythonic domain language.
    * extremely easy to use for all the basic tasks, such as: accessing pooled connections, constructing SQL from Python expressions, finding object instances, and commiting object modifications back to the database.
    * powerful enough for complicated tasks, such as: eager load a graph of objects and their dependencies via joins; map recursive adjacency structures automatically; map objects to not just tables but to any arbitrary join or select statement; combine multiple tables together to load whole sets of otherwise unrelated objects from a single result set; commit entire graphs of object changes in one step.
    * built to conform to what DBAs demand, including the ability to swap out generated SQL with hand-optimized statements, full usage of bind parameters for all literal values, fully transactionalized and consistent updates using Unit of Work.
    * modular. Different parts of SQLAlchemy can be used independently of the rest, including the connection pool, SQL construction, and ORM. SQLAlchemy is constructed in an open style that allows plenty of customization, with an architecture that supports custom datatypes, custom SQL extensions, and ORM plugins which can augment or extend mapping functionality.

SQLAlchemy's Philosophy:

    * SQL databases behave less and less like object collections the more size and performance start to matter; object collections behave less and less like tables and rows the more abstraction starts to matter. SQLAlchemy aims to accomodate both of these principles.
    * Your classes aren't tables, and your objects aren't rows. Databases aren't just collections of tables; they're relational algebra engines. You don't have to select from just tables, you can select from joins, subqueries, and unions. Database and domain concepts should be visibly decoupled from the beginning, allowing both sides to develop to their full potential.
    * For example, table metadata (objects that describe tables) are declared distinctly from the classes theyre designed to store. That way database relationship concepts don't interfere with your object design concepts, and vice-versa; the transition from table-mapping to selectable-mapping is seamless; a class can be mapped against the database in more than one way. SQLAlchemy provides a powerful mapping layer that can work as automatically or as manually as you choose, determining relationships based on foreign keys or letting you define the join conditions explicitly, to bridge the gap between database and domain.

SQLAlchemy's Advantages:

    * The Unit Of Work system organizes pending CRUD operations into queues and commits them all in one batch. It then performs a topological "dependency sort" of all items to be committed and deleted and groups redundant statements together. This produces the maxiumum efficiency and transaction safety, and minimizes chances of deadlocks. Modeled after Fowler's "Unit of Work" pattern as well as Java Hibernate.
    * Function-based query construction allows boolean expressions, operators, functions, table aliases, selectable subqueries, create/update/insert/delete queries, correlated updates, correlated EXISTS clauses, UNION clauses, inner and outer joins, bind parameters, free mixing of literal text within expressions, as little or as much as desired. Query-compilation is vendor-specific; the same query object can be compiled into any number of resulting SQL strings depending on its compilation algorithm.
    * Database mapping and class design are totally separate. Persisted objects have no subclassing requirement (other than 'object') and are POPO's : plain old Python objects. They retain serializability (pickling) for usage in various caching systems and session objects. SQLAlchemy "decorates" classes with non-intrusive property accessors to automatically log object creates and modifications with the UnitOfWork engine, to lazyload related data, as well as to track attribute change histories.
    * Custom list classes can be used with eagerly or lazily loaded child object lists, allowing rich relationships to be created on the fly as SQLAlchemy appends child objects to an object attribute.
    * Composite (multiple-column) primary keys are supported, as are "association" objects that represent the middle of a "many-to-many" relationship.
    * Self-referential tables and mappers are supported. Adjacency list structures can be created, saved, and deleted with proper cascading, with no extra programming.
    * Data mapping can be used in a row-based manner. Any bizarre hyper-optimized query that you or your DBA can cook up, you can run in SQLAlchemy, and as long as it returns the expected columns within a rowset, you can get your objects from it. For a rowset that contains more than one kind of object per row, multiple mappers can be chained together to return multiple object instance lists from a single database round trip.
    * The type system allows pre- and post- processing of data, both at the bind parameter and the result set level. User-defined types can be freely mixed with built-in types. Generic types as well as SQL-specific types are available.

""",
      classifiers = [
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Topic :: Database :: Front-Ends",
        "Operating System :: OS Independent",
        ],
        **extra
      )
