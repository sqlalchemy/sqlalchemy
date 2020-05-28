"""A performance profiling suite for a variety of SQLAlchemy use cases.

Each suite focuses on a specific use case with a particular performance
profile and associated implications:

* bulk inserts
* individual inserts, with or without transactions
* fetching large numbers of rows
* running lots of short queries

All suites include a variety of use patterns illustrating both Core
and ORM use, and are generally sorted in order of performance from worst
to greatest, inversely based on amount of functionality provided by SQLAlchemy,
greatest to least (these two things generally correspond perfectly).

A command line tool is presented at the package level which allows
individual suites to be run::

    $ python -m examples.performance --help
    usage: python -m examples.performance [-h] [--test TEST] [--dburl DBURL]
                                          [--num NUM] [--profile] [--dump]
                                          [--echo]

                                          {bulk_inserts,large_resultsets,single_inserts}

    positional arguments:
      {bulk_inserts,large_resultsets,single_inserts}
                            suite to run

    optional arguments:
      -h, --help            show this help message and exit
      --test TEST           run specific test name
      --dburl DBURL         database URL, default sqlite:///profile.db
      --num NUM             Number of iterations/items/etc for tests;
                            default is module-specific
      --profile             run profiling and dump call counts
      --dump                dump full call profile (implies --profile)
      --echo                Echo SQL output

An example run looks like::

    $ python -m examples.performance bulk_inserts

Or with options::

    $ python -m examples.performance bulk_inserts \\
        --dburl mysql+mysqldb://scott:tiger@localhost/test \\
        --profile --num 1000

.. seealso::

    :ref:`faq_how_to_profile`

File Listing
-------------

.. autosource::


Running all tests with time
---------------------------

This is the default form of run::

    $ python -m examples.performance single_inserts
    Tests to run: test_orm_commit, test_bulk_save,
                  test_bulk_insert_dictionaries, test_core,
                  test_core_query_caching, test_dbapi_raw_w_connect,
                  test_dbapi_raw_w_pool

    test_orm_commit : Individual INSERT/COMMIT pairs via the
        ORM (10000 iterations); total time 13.690218 sec
    test_bulk_save : Individual INSERT/COMMIT pairs using
        the "bulk" API  (10000 iterations); total time 11.290371 sec
    test_bulk_insert_dictionaries : Individual INSERT/COMMIT pairs using
        the "bulk" API with dictionaries (10000 iterations);
        total time 10.814626 sec
    test_core : Individual INSERT/COMMIT pairs using Core.
        (10000 iterations); total time 9.665620 sec
    test_core_query_caching : Individual INSERT/COMMIT pairs using Core
        with query caching (10000 iterations); total time 9.209010 sec
    test_dbapi_raw_w_connect : Individual INSERT/COMMIT pairs w/ DBAPI +
        connection each time (10000 iterations); total time 9.551103 sec
    test_dbapi_raw_w_pool : Individual INSERT/COMMIT pairs w/ DBAPI +
        connection pool (10000 iterations); total time 8.001813 sec

Dumping Profiles for Individual Tests
--------------------------------------

A Python profile output can be dumped for all tests, or more commonly
individual tests::

    $ python -m examples.performance single_inserts --test test_core --num 1000 --dump
    Tests to run: test_core
    test_core : Individual INSERT/COMMIT pairs using Core. (1000 iterations); total fn calls 186109
             186109 function calls (186102 primitive calls) in 1.089 seconds

       Ordered by: internal time, call count

       ncalls  tottime  percall  cumtime  percall filename:lineno(function)
         1000    0.634    0.001    0.634    0.001 {method 'commit' of 'sqlite3.Connection' objects}
         1000    0.154    0.000    0.154    0.000 {method 'execute' of 'sqlite3.Cursor' objects}
         1000    0.021    0.000    0.074    0.000 /Users/classic/dev/sqlalchemy/lib/sqlalchemy/sql/compiler.py:1950(_get_colparams)
         1000    0.015    0.000    0.034    0.000 /Users/classic/dev/sqlalchemy/lib/sqlalchemy/engine/default.py:503(_init_compiled)
            1    0.012    0.012    1.091    1.091 examples/performance/single_inserts.py:79(test_core)

        ...


.. _examples_profiling_writeyourown:

Writing your Own Suites
-----------------------

The profiler suite system is extensible, and can be applied to your own set
of tests.  This is a valuable technique to use in deciding upon the proper
approach for some performance-critical set of routines.  For example,
if we wanted to profile the difference between several kinds of loading,
we can create a file ``test_loads.py``, with the following content::

    from examples.performance import Profiler
    from sqlalchemy import Integer, Column, create_engine, ForeignKey
    from sqlalchemy.orm import relationship, joinedload, subqueryload, Session
    from sqlalchemy.ext.declarative import declarative_base

    Base = declarative_base()
    engine = None
    session = None


    class Parent(Base):
        __tablename__ = 'parent'
        id = Column(Integer, primary_key=True)
        children = relationship("Child")


    class Child(Base):
        __tablename__ = 'child'
        id = Column(Integer, primary_key=True)
        parent_id = Column(Integer, ForeignKey('parent.id'))


    # Init with name of file, default number of items
    Profiler.init("test_loads", 1000)


    @Profiler.setup_once
    def setup_once(dburl, echo, num):
        "setup once.  create an engine, insert fixture data"
        global engine
        engine = create_engine(dburl, echo=echo)
        Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)
        sess = Session(engine)
        sess.add_all([
            Parent(children=[Child() for j in range(100)])
            for i in range(num)
        ])
        sess.commit()


    @Profiler.setup
    def setup(dburl, echo, num):
        "setup per test.  create a new Session."
        global session
        session = Session(engine)
        # pre-connect so this part isn't profiled (if we choose)
        session.connection()


    @Profiler.profile
    def test_lazyload(n):
        "load everything, no eager loading."

        for parent in session.query(Parent):
            parent.children


    @Profiler.profile
    def test_joinedload(n):
        "load everything, joined eager loading."

        for parent in session.query(Parent).options(joinedload("children")):
            parent.children


    @Profiler.profile
    def test_subqueryload(n):
        "load everything, subquery eager loading."

        for parent in session.query(Parent).options(subqueryload("children")):
            parent.children

    if __name__ == '__main__':
        Profiler.main()

We can run our new script directly::

    $ python test_loads.py  --dburl postgresql+psycopg2://scott:tiger@localhost/test
    Running setup once...
    Tests to run: test_lazyload, test_joinedload, test_subqueryload
    test_lazyload : load everything, no eager loading. (1000 iterations); total time 11.971159 sec
    test_joinedload : load everything, joined eager loading. (1000 iterations); total time 2.754592 sec
    test_subqueryload : load everything, subquery eager loading. (1000 iterations); total time 2.977696 sec


"""  # noqa
import argparse
import cProfile
import gc
import os
import pstats
import re
import sys
import time


class Profiler(object):
    tests = []

    _setup = None
    _setup_once = None
    name = None
    num = 0

    def __init__(self, options):
        self.test = options.test
        self.dburl = options.dburl
        self.profile = options.profile
        self.dump = options.dump
        self.raw = options.raw
        self.callers = options.callers
        self.num = options.num
        self.echo = options.echo
        self.sort = options.sort
        self.gc = options.gc
        self.stats = []

    @classmethod
    def init(cls, name, num):
        cls.name = name
        cls.num = num

    @classmethod
    def profile(cls, fn):
        if cls.name is None:
            raise ValueError(
                "Need to call Profile.init(<suitename>, <default_num>) first."
            )
        cls.tests.append(fn)
        return fn

    @classmethod
    def setup(cls, fn):
        if cls._setup is not None:
            raise ValueError("setup function already set to %s" % cls._setup)
        cls._setup = staticmethod(fn)
        return fn

    @classmethod
    def setup_once(cls, fn):
        if cls._setup_once is not None:
            raise ValueError(
                "setup_once function already set to %s" % cls._setup_once
            )
        cls._setup_once = staticmethod(fn)
        return fn

    def run(self):
        if self.test:
            tests = [fn for fn in self.tests if fn.__name__ == self.test]
            if not tests:
                raise ValueError("No such test: %s" % self.test)
        else:
            tests = self.tests

        if self._setup_once:
            print("Running setup once...")
            self._setup_once(self.dburl, self.echo, self.num)
        print("Tests to run: %s" % ", ".join([t.__name__ for t in tests]))
        for test in tests:
            self._run_test(test)
            self.stats[-1].report()

    def _run_with_profile(self, fn, sort):
        pr = cProfile.Profile()
        pr.enable()
        try:
            result = fn(self.num)
        finally:
            pr.disable()

        stats = pstats.Stats(pr)

        self.stats.append(TestResult(self, fn, stats=stats, sort=sort))
        return result

    def _run_with_time(self, fn):
        now = time.time()
        try:
            return fn(self.num)
        finally:
            total = time.time() - now
            self.stats.append(TestResult(self, fn, total_time=total))

    def _run_test(self, fn):
        if self._setup:
            self._setup(self.dburl, self.echo, self.num)
        if self.gc:
            # gc.set_debug(gc.DEBUG_COLLECTABLE)
            gc.set_debug(gc.DEBUG_STATS)
        if self.profile or self.dump:
            self._run_with_profile(fn, self.sort)
        else:
            self._run_with_time(fn)
        if self.gc:
            gc.set_debug(0)

    @classmethod
    def main(cls):

        parser = argparse.ArgumentParser("python -m examples.performance")

        if cls.name is None:
            parser.add_argument(
                "name", choices=cls._suite_names(), help="suite to run"
            )

            if len(sys.argv) > 1:
                potential_name = sys.argv[1]
                try:
                    __import__(__name__ + "." + potential_name)
                except ImportError:
                    pass

        parser.add_argument("--test", type=str, help="run specific test name")

        parser.add_argument(
            "--dburl",
            type=str,
            default="sqlite:///profile.db",
            help="database URL, default sqlite:///profile.db",
        )
        parser.add_argument(
            "--num",
            type=int,
            default=cls.num,
            help="Number of iterations/items/etc for tests; "
            "default is %d module-specific" % cls.num,
        )
        parser.add_argument(
            "--profile",
            action="store_true",
            help="run profiling and dump call counts",
        )
        parser.add_argument(
            "--sort",
            type=str,
            default="cumulative",
            help="profiling sort, defaults to cumulative",
        )
        parser.add_argument(
            "--dump",
            action="store_true",
            help="dump full call profile (implies --profile)",
        )
        parser.add_argument(
            "--raw",
            type=str,
            help="dump raw profile data to file (implies --profile)",
        )
        parser.add_argument(
            "--callers",
            action="store_true",
            help="print callers as well (implies --dump)",
        )
        parser.add_argument(
            "--gc", action="store_true", help="turn on GC debug stats"
        )
        parser.add_argument(
            "--echo", action="store_true", help="Echo SQL output"
        )
        args = parser.parse_args()

        args.dump = args.dump or args.callers
        args.profile = args.profile or args.dump or args.raw

        if cls.name is None:
            __import__(__name__ + "." + args.name)

        Profiler(args).run()

    @classmethod
    def _suite_names(cls):
        suites = []
        for file_ in os.listdir(os.path.dirname(__file__)):
            match = re.match(r"^([a-z].*).py$", file_)
            if match:
                suites.append(match.group(1))
        return suites


class TestResult(object):
    def __init__(
        self, profile, test, stats=None, total_time=None, sort="cumulative"
    ):
        self.profile = profile
        self.test = test
        self.stats = stats
        self.total_time = total_time
        self.sort = sort

    def report(self):
        print(self._summary())
        if self.profile.profile:
            self.report_stats()

    def _summary(self):
        summary = "%s : %s (%d iterations)" % (
            self.test.__name__,
            self.test.__doc__,
            self.profile.num,
        )
        if self.total_time:
            summary += "; total time %f sec" % self.total_time
        if self.stats:
            summary += "; total fn calls %d" % self.stats.total_calls
        return summary

    def report_stats(self):
        if self.profile.dump:
            self._dump(self.sort)
        elif self.profile.raw:
            self._dump_raw()

    def _dump(self, sort):
        self.stats.sort_stats(*re.split(r"[ ,]", self.sort))
        self.stats.print_stats()
        if self.profile.callers:
            self.stats.print_callers()

    def _dump_raw(self):
        self.stats.dump_stats(self.profile.raw)
