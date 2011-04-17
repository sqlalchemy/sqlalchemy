"""setup.py

Please see README for basic installation instructions.

"""

import os
import re
import sys
from distutils.command.build_ext import build_ext
from distutils.errors import (CCompilerError, DistutilsExecError,
                              DistutilsPlatformError)
try:
    from setuptools import setup, Extension, Feature
    has_setuptools = True
except ImportError:
    has_setuptools = False
    from distutils.core import setup, Extension
    Feature = None
    try:  # Python 3
        from distutils.command.build_py import build_py_2to3 as build_py
    except ImportError:  # Python 2
        from distutils.command.build_py import build_py

cmdclass = {}
pypy = hasattr(sys, 'pypy_version_info')
py3k = False
extra = {}
if sys.version_info < (2, 4):
    raise Exception("SQLAlchemy requires Python 2.4 or higher.")
elif sys.version_info >= (3, 0):
    py3k = True
    # monkeypatch our preprocessor
    # onto the 2to3 tool.
    from sa2to3 import refactor_string
    from lib2to3.refactor import RefactoringTool
    RefactoringTool.refactor_string = refactor_string

    if has_setuptools:
        extra.update(
            use_2to3=True,
        )
    else:
        cmdclass['build_py'] = build_py

ext_modules = [
    Extension('sqlalchemy.cprocessors',
           sources=['lib/sqlalchemy/cextension/processors.c']),
    Extension('sqlalchemy.cresultproxy',
           sources=['lib/sqlalchemy/cextension/resultproxy.c'])
    ]

if sys.platform == 'win32' and sys.version_info > (2, 6):
   # 2.6's distutils.msvc9compiler can raise an IOError when failing to
   # find the compiler
   ext_errors = (
                    CCompilerError, DistutilsExecError, 
                    DistutilsPlatformError, IOError)
else:
   ext_errors = (CCompilerError, DistutilsExecError, DistutilsPlatformError)

class BuildFailed(Exception):

    def __init__(self):
        self.cause = sys.exc_info()[1] # work around py 2/3 different syntax

class ve_build_ext(build_ext):
    # This class allows C extension building to fail.

    def run(self):
        try:
            build_ext.run(self)
        except DistutilsPlatformError:
            raise BuildFailed()

    def build_extension(self, ext):
        try:
            build_ext.build_extension(self, ext)
        except ext_errors:
            raise BuildFailed()

cmdclass['build_ext'] = ve_build_ext

def status_msgs(*msgs):
    print('*' * 75)
    for msg in msgs:
        print(msg)
    print('*' * 75)

def find_packages(dir_):
    packages = []
    for pkg in ['sqlalchemy']:
        for _dir, subdirectories, files in (
                os.walk(os.path.join(dir_, pkg))
            ):
            if '__init__.py' in files:
                lib, fragment = _dir.split(os.sep, 1)
                packages.append(fragment.replace(os.sep, '.'))
    return packages

v = open(os.path.join(os.path.dirname(__file__), 
                        'lib', 'sqlalchemy', '__init__.py'))
VERSION = re.compile(r".*__version__ = '(.*?)'",
                     re.S).match(v.read()).group(1)
v.close()

def run_setup(with_cext):
    kwargs = extra.copy()
    if with_cext:
        if Feature:
            kwargs['features'] = {'cextensions': Feature(
                    "optional C speed-enhancements",
                    standard=True,
                    ext_modules=ext_modules
                    )}
        else:
            kwargs['ext_modules'] = ext_modules

    setup(name="SQLAlchemy",
          version=VERSION,
          description="Database Abstraction Library",
          author="Mike Bayer",
          author_email="mike_mp@zzzcomputing.com",
          url="http://www.sqlalchemy.org",
          packages=find_packages('lib'),
          package_dir={'': 'lib'},
          license="MIT License",
          cmdclass=cmdclass,

          tests_require=['nose >= 0.11'],
          test_suite="sqla_nose",

          long_description="""\
    SQLAlchemy is:

        * The Python SQL toolkit and Object Relational Mapper
          that gives application developers the full power and
          flexibility of SQL. SQLAlchemy provides a full suite
          of well known enterprise-level persistence patterns,
          designed for efficient and high-performing database
          access, adapted into a simple and Pythonic domain
          language.
        * extremely easy to use for all the basic tasks, such
          as: accessing pooled connections, constructing SQL
          from Python expressions, finding object instances, and
          commiting object modifications back to the database.
        * powerful enough for complicated tasks, such as: eager
          load a graph of objects and their dependencies via
          joins; map recursive adjacency structures
          automatically; map objects to not just tables but to
          any arbitrary join or select statement; combine
          multiple tables together to load whole sets of
          otherwise unrelated objects from a single result set;
          commit entire graphs of object changes in one step.
        * built to conform to what DBAs demand, including the
          ability to swap out generated SQL with hand-optimized
          statements, full usage of bind parameters for all
          literal values, fully transactionalized and consistent
          updates using Unit of Work.
        * modular. Different parts of SQLAlchemy can be used
          independently of the rest, including the connection
          pool, SQL construction, and ORM. SQLAlchemy is
          constructed in an open style that allows plenty of
          customization, with an architecture that supports
          custom datatypes, custom SQL extensions, and ORM
          plugins which can augment or extend mapping
          functionality.

    SQLAlchemy's Philosophy:

        * SQL databases behave less and less like object
          collections the more size and performance start to
          matter; object collections behave less and less like
          tables and rows the more abstraction starts to matter.
          SQLAlchemy aims to accomodate both of these
          principles.
        * Your classes aren't tables, and your objects aren't
          rows. Databases aren't just collections of tables;
          they're relational algebra engines. You don't have to
          select from just tables, you can select from joins,
          subqueries, and unions. Database and domain concepts
          should be visibly decoupled from the beginning,
          allowing both sides to develop to their full
          potential.
        * For example, table metadata (objects that describe
          tables) are declared distinctly from the classes
          theyre designed to store. That way database
          relationship concepts don't interfere with your object
          design concepts, and vice-versa; the transition from
          table-mapping to selectable-mapping is seamless; a
          class can be mapped against the database in more than
          one way. SQLAlchemy provides a powerful mapping layer
          that can work as automatically or as manually as you
          choose, determining relationships based on foreign
          keys or letting you define the join conditions
          explicitly, to bridge the gap between database and
          domain.

    SQLAlchemy's Advantages:

        * The Unit Of Work system organizes pending CRUD
          operations into queues and commits them all in one
          batch. It then performs a topological "dependency
          sort" of all items to be committed and deleted and
          groups redundant statements together. This produces
          the maxiumum efficiency and transaction safety, and
          minimizes chances of deadlocks. Modeled after Fowler's
          "Unit of Work" pattern as well as Java Hibernate.
        * Function-based query construction allows boolean
          expressions, operators, functions, table aliases,
          selectable subqueries, create/update/insert/delete
          queries, correlated updates, correlated EXISTS
          clauses, UNION clauses, inner and outer joins, bind
          parameters, free mixing of literal text within
          expressions, as little or as much as desired.
          Query-compilation is vendor-specific; the same query
          object can be compiled into any number of resulting
          SQL strings depending on its compilation algorithm.
        * Database mapping and class design are totally
          separate. Persisted objects have no subclassing
          requirement (other than 'object') and are POPO's :
          plain old Python objects. They retain serializability
          (pickling) for usage in various caching systems and
          session objects. SQLAlchemy "decorates" classes with
          non-intrusive property accessors to automatically log
          object creates and modifications with the UnitOfWork
          engine, to lazyload related data, as well as to track
          attribute change histories.
        * Custom list classes can be used with eagerly or lazily
          loaded child object lists, allowing rich relationships
          to be created on the fly as SQLAlchemy appends child
          objects to an object attribute.
        * Composite (multiple-column) primary keys are
          supported, as are "association" objects that represent
          the middle of a "many-to-many" relationship.
        * Self-referential tables and mappers are supported.
          Adjacency list structures can be created, saved, and
          deleted with proper cascading, with no extra
          programming.
        * Data mapping can be used in a row-based manner. Any
          bizarre hyper-optimized query that you or your DBA can
          cook up, you can run in SQLAlchemy, and as long as it
          returns the expected columns within a rowset, you can
          get your objects from it. For a rowset that contains
          more than one kind of object per row, multiple mappers
          can be chained together to return multiple object
          instance lists from a single database round trip.
        * The type system allows pre- and post- processing of
          data, both at the bind parameter and the result set
          level. User-defined types can be freely mixed with
          built-in types. Generic types as well as SQL-specific
          types are available.

    """,
          classifiers=[
            "Development Status :: 4 - Beta",
            "Intended Audience :: Developers",
            "License :: OSI Approved :: MIT License",
            "Programming Language :: Python",
            "Programming Language :: Python :: 3",
            "Topic :: Database :: Front-Ends",
            "Operating System :: OS Independent",
            ],
            **kwargs
          )

if pypy or py3k:
    run_setup(False)
    status_msgs(
        "WARNING: C extensions are not supported on " +
            "this Python platform, speedups are not enabled.",
        "Plain-Python build succeeded."
    )
else:
    try:
        run_setup(True)
    except BuildFailed:
        exc = sys.exc_info()[1] # work around py 2/3 different syntax
        status_msgs(
            exc.cause,
            "WARNING: The C extension could not be compiled, " +
                "speedups are not enabled.",
            "Failure information, if any, is above.",
            "Retrying the build without the C extension now."
        )

        run_setup(False)

        status_msgs(
            "WARNING: The C extension could not be compiled, " +
                "speedups are not enabled.",
            "Plain-Python build succeeded."
        )
