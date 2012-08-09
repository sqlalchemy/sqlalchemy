"""Profiling support for unit and performance tests.

These are special purpose profiling methods which operate
in a more fine-grained way than nose's profiling plugin.

"""

import os
import sys
from test.lib.util import gc_collect, decorator
from nose import SkipTest
import pstats
import time
import collections
try:
    import cProfile
except ImportError:
    cProfile = None

def profiled(target=None, **target_opts):
    """Function profiling.

    @profiled('label')
    or
    @profiled('label', report=True, sort=('calls',), limit=20)

    Enables profiling for a function when 'label' is targetted for
    profiling.  Report options can be supplied, and override the global
    configuration and command-line options.
    """

    profile_config = {'targets': set(),
                       'report': True,
                       'print_callers': False,
                       'print_callees': False,
                       'graphic': False,
                       'sort': ('time', 'calls'),
                       'limit': None}
    if target is None:
        target = 'anonymous_target'

    filename = "%s.prof" % target

    @decorator
    def decorate(fn, *args, **kw):
        elapsed, load_stats, result = _profile(
            filename, fn, *args, **kw)

        graphic = target_opts.get('graphic', profile_config['graphic'])
        if graphic:
            os.system("runsnake %s" % filename)
        else:
            report = target_opts.get('report', profile_config['report'])
            if report:
                sort_ = target_opts.get('sort', profile_config['sort'])
                limit = target_opts.get('limit', profile_config['limit'])
                print ("Profile report for target '%s' (%s)" % (
                    target, filename)
                    )

                stats = load_stats()
                stats.sort_stats(*sort_)
                if limit:
                    stats.print_stats(limit)
                else:
                    stats.print_stats()

                print_callers = target_opts.get('print_callers',
                                                profile_config['print_callers'])
                if print_callers:
                    stats.print_callers()

                print_callees = target_opts.get('print_callees',
                                                profile_config['print_callees'])
                if print_callees:
                    stats.print_callees()

        os.unlink(filename)
        return result
    return decorate

def function_call_count(count=None, versions={}, variance=0.05):
    """Assert a target for a test case's function call count."""

    py_version = '.'.join([str(v) for v in sys.version_info])

    @decorator
    def decorate(fn, *args, **kw):
        if cProfile is None:
            raise SkipTest("cProfile is not installed")
        if count is None:
            raise SkipTest("no count installed")
        gc_collect()

        stats_total_calls, callcount, fn_result = _count_calls(
            {"exclude": _exclude},
            fn, *args, **kw
        )
        print("Pstats calls: %d Adjusted callcount %d  Expected %d" % (
                stats_total_calls,
                callcount,
                count
            ))
        deviance = int(callcount * variance)
        if abs(callcount - count) > deviance:
            raise AssertionError(
                "Adjusted function call count %s not within %s%% "
                "of expected %s. (cProfile reported %s "
                    "calls, Python version %s)" % (
                callcount, (variance * 100),
                count, stats_total_calls, py_version))
        return fn_result

    return decorate

py3k = sys.version_info >= (3,)

def _paths(key, stats, cache, seen=None):
    if seen is None:
        seen = collections.defaultdict(int)
    fname, lineno, fn_name = key
    if seen.get(key):
        return

    if key in cache:
        for item in cache[key]:
            yield item
    else:
        seen[key] += 1
        try:
            path_element = (fname, lineno, fn_name)
            paths_to_yield = []
            (cc, nc, tt, ct, callers) = stats[key]
            if not callers:
                paths_to_yield.append((path_element,))
                yield (path_element,)

            for subkey in callers:
                sub_cc, sub_nc, sub_tt, sub_ct = callers[subkey]
                parent_iterator = list(_paths(subkey, stats, cache, seen))
                path_element = (fname, lineno, fn_name)
                for parent in parent_iterator:
                    paths_to_yield.append(parent + (path_element,))
                    yield parent + (path_element,)
            cache[key] = paths_to_yield
        finally:
            seen[key] -= 1

def _exclude(path):

    for pfname, plineno, pfuncname in path:
        if "compat" in pfname or \
            "processors" in pfname or \
            "cutils" in pfname:
            return True
        if "threading.py" in pfname:
            return True

    if (
            "result.py" in pfname or
            "engine/base.py" in pfname
        ) and pfuncname in ("__iter__", "__getitem__"):
        return True

    if "utf_8.py" in pfname and pfuncname == "decode":
        return True

    if path[-1][2] in (
            "<built-in method exec>",
            "<listcomp>"
            ):
        return True

    if '_thread.RLock' in path[-1][2]:
        return True

    return False

def _count_calls(options, fn, *args, **kw):
    total_time, stats_loader, fn_result = _profile(fn, *args, **kw)
    exclude_fn = options.get("exclude", None)

    stats = stats_loader()

    callcount = 0
    report = []
    path_cache = {}
    for (fname, lineno, fn_name), (cc, nc, tt, ct, callers) \
                in stats.stats.items():
        exclude = 0
        paths = list(_paths((fname, lineno, fn_name), stats.stats, path_cache))
        for path in paths:
            if not path:
                continue
            if exclude_fn and exclude_fn(path):
                exclude += 1

        adjusted_cc = cc
        if exclude:
            adjust = (float(exclude) / len(paths)) * cc
            adjusted_cc -= adjust
        dirname, fname = os.path.split(fname)
        #print(" ".join([str(x) for x in [fname, lineno, fn_name, cc, adjusted_cc]]))
        report.append(" ".join([str(x) for x in [fname, lineno, fn_name, cc, adjusted_cc]]))
        callcount += adjusted_cc

    #stats.sort_stats('calls', 'cumulative')
    #stats.print_stats()
    report.sort()
    print "\n".join(report)
    return stats.total_calls, callcount, fn_result

def _profile(fn, *args, **kw):
    filename = "%s.prof" % fn.__name__

    def load_stats():
        st = pstats.Stats(filename)
        os.unlink(filename)
        return st

    began = time.time()
    cProfile.runctx('result = fn(*args, **kw)', globals(), locals(),
                    filename=filename)
    ended = time.time()

    return ended - began, load_stats, locals()['result']
