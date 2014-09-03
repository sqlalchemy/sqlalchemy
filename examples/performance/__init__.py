"""A performance profiling suite for a variety of SQLAlchemy use cases.

The suites here each focus on some specific type of use case, one which
has a particular performance profile:

* bulk inserts
* individual inserts, with or without transactions
* fetching large numbers of rows
* running lots of small queries

All suites include a variety of use patterns with both the Core and
ORM, and are sorted in order of performance from worst to greatest,
inversely based on amount of functionality provided by SQLAlchemy,
greatest to least (these two things generally correspond perfectly).

Each suite is run as a module, and provides a consistent command line
interface::

    $ python -m examples.performance.bulk_inserts --profile --num 1000

Using ``--help`` will allow all options::

    $ python -m examples.performance.bulk_inserts --help
usage: bulk_inserts.py [-h] [--test TEST] [--dburl DBURL] [--num NUM]
                       [--profile] [--dump] [--runsnake] [--echo]

optional arguments:
  -h, --help     show this help message and exit
  --test TEST    run specific test name
  --dburl DBURL  database URL, default sqlite:///profile.db
  --num NUM      Number of iterations/items/etc for tests, default 100000
  --profile      run profiling and dump call counts
  --dump         dump full call profile (implies --profile)
  --runsnake     invoke runsnakerun (implies --profile)
  --echo         Echo SQL output


"""
import argparse
import cProfile
import StringIO
import pstats
import os
import time



class Profiler(object):
    tests = []

    def __init__(self, setup, options):
        self.setup = setup
        self.test = options.test
        self.dburl = options.dburl
        self.runsnake = options.runsnake
        self.profile = options.profile
        self.dump = options.dump
        self.num = options.num
        self.echo = options.echo
        self.stats = []

    @classmethod
    def profile(cls, fn):
        cls.tests.append(fn)
        return fn

    def run(self):
        if self.test:
            tests = [fn for fn in self.tests if fn.__name__ == self.test]
            if not tests:
                raise ValueError("No such test: %s" % self.test)
        else:
            tests = self.tests

        print("Tests to run: %s" % ", ".join([t.__name__ for t in tests]))
        for test in tests:
            self._run_test(test)
            self.stats[-1].report()

    def _run_with_profile(self, fn):
        pr = cProfile.Profile()
        pr.enable()
        try:
            result = fn(self.num)
        finally:
            pr.disable()

        output = StringIO.StringIO()
        stats = pstats.Stats(pr, stream=output).sort_stats('cumulative')

        self.stats.append(TestResult(self, fn, stats=stats))
        return result

    def _run_with_time(self, fn):
        now = time.time()
        try:
            return fn(self.num)
        finally:
            total = time.time() - now
            self.stats.append(TestResult(self, fn, total_time=total))

    def _run_test(self, fn):
        self.setup(self.dburl, self.echo)
        if self.profile or self.runsnake or self.dump:
            self._run_with_profile(fn)
        else:
            self._run_with_time(fn)

    @classmethod
    def main(cls, setup):
        parser = argparse.ArgumentParser()

        parser.add_argument(
            "--test", type=str,
            help="run specific test name"
        )
        parser.add_argument(
            '--dburl', type=str, default="sqlite:///profile.db",
            help="database URL, default sqlite:///profile.db"
        )
        parser.add_argument(
            '--num', type=int, default=100000,
            help="Number of iterations/items/etc for tests, default 100000"
        )
        parser.add_argument(
            '--profile', action='store_true',
            help='run profiling and dump call counts')
        parser.add_argument(
            '--dump', action='store_true',
            help='dump full call profile (implies --profile)')
        parser.add_argument(
            '--runsnake', action='store_true',
            help='invoke runsnakerun (implies --profile)')
        parser.add_argument(
            '--echo', action='store_true',
            help="Echo SQL output"
            )
        args = parser.parse_args()

        args.profile = args.profile or args.dump or args.runsnake

        Profiler(setup, args).run()


class TestResult(object):
    def __init__(self, profile, test, stats=None, total_time=None):
        self.profile = profile
        self.test = test
        self.stats = stats
        self.total_time = total_time

    def report(self):
        print(self._summary())
        if self.profile.profile:
            self.report_stats()

    def _summary(self):
        summary = "%s : %s (%d iterations)" % (
            self.test.__name__, self.test.__doc__, self.profile.num)
        if self.total_time:
            summary += "; total time %f sec" % self.total_time
        if self.stats:
            summary += "; total fn calls %d" % self.stats.total_calls
        return summary

    def report_stats(self):
        if self.profile.runsnake:
            self._runsnake()
        elif self.profile.dump:
            self._dump()

    def _dump(self):
        self.stats.sort_stats('time', 'calls')
        self.stats.print_stats()

    def _runsnake(self):
        filename = "%s.profile" % self.test.__name__
        try:
            self.stats.dump_stats(filename)
            os.system("runsnake %s" % filename)
        finally:
            os.remove(filename)

