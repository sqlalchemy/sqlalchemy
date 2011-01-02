# test/profiling.py
# Copyright (C) 2005-2011 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Profiling support for unit and performance tests.

These are special purpose profiling methods which operate
in a more fine-grained way than nose's profiling plugin.

"""

import os, sys
from sqlalchemy_nose import config
from sqlalchemy.test.util import function_named, gc_collect
from nose import SkipTest

__all__ = 'profiled', 'function_call_count', 'conditional_call_count'

all_targets = set()
profile_config = { 'targets': set(),
                   'report': True,
                   'sort': ('time', 'calls'),
                   'limit': None }
profiler = None

def profiled(target=None, **target_opts):
    """Optional function profiling.

    @profiled('label')
    or
    @profiled('label', report=True, sort=('calls',), limit=20)

    Enables profiling for a function when 'label' is targetted for
    profiling.  Report options can be supplied, and override the global
    configuration and command-line options.
    """

    # manual or automatic namespacing by module would remove conflict issues
    if target is None:
        target = 'anonymous_target'
    elif target in all_targets:
        print "Warning: redefining profile target '%s'" % target
    all_targets.add(target)

    filename = "%s.prof" % target

    def decorator(fn):
        def profiled(*args, **kw):
            if (target not in profile_config['targets'] and
                not target_opts.get('always', None)):
                return fn(*args, **kw)

            elapsed, load_stats, result = _profile(
                filename, fn, *args, **kw)

            report = target_opts.get('report', profile_config['report'])
            if report:
                sort_ = target_opts.get('sort', profile_config['sort'])
                limit = target_opts.get('limit', profile_config['limit'])
                print "Profile report for target '%s' (%s)" % (
                    target, filename)

                stats = load_stats()
                stats.sort_stats(*sort_)
                if limit:
                    stats.print_stats(limit)
                else:
                    stats.print_stats()
                #stats.print_callers()
            os.unlink(filename)
            return result
        return function_named(profiled, fn.__name__)
    return decorator

def function_call_count(count=None, versions={}, variance=0.05):
    """Assert a target for a test case's function call count.

    count
      Optional, general target function call count.

    versions
      Optional, a dictionary of Python version strings to counts,
      for example::

        { '2.5.1': 110,
          '2.5': 100,
          '2.4': 150 }

      The best match for the current running python will be used.
      If none match, 'count' will be used as the fallback.

    variance
      An +/- deviation percentage, defaults to 5%.
    """

    # this could easily dump the profile report if --verbose is in effect

    version_info = list(sys.version_info)
    py_version = '.'.join([str(v) for v in sys.version_info])
    try:
        from sqlalchemy.cprocessors import to_float
        cextension = True
    except ImportError:
        cextension = False

    while version_info:
        version = '.'.join([str(v) for v in version_info])
        if cextension:
            version += "+cextension"
        if version in versions:
            count = versions[version]
            break
        version_info.pop()

    if count is None:
        return lambda fn: fn

    def decorator(fn):
        def counted(*args, **kw):
            try:
                filename = "%s.prof" % fn.__name__

                elapsed, stat_loader, result = _profile(
                    filename, fn, *args, **kw)

                stats = stat_loader()
                calls = stats.total_calls

                stats.sort_stats('calls', 'cumulative')
                stats.print_stats()
                #stats.print_callers()
                deviance = int(count * variance)
                if (calls < (count - deviance) or
                    calls > (count + deviance)):
                    raise AssertionError(
                        "Function call count %s not within %s%% "
                        "of expected %s. (Python version %s)" % (
                        calls, (variance * 100), count, py_version))

                return result
            finally:
                if os.path.exists(filename):
                    os.unlink(filename)
        return function_named(counted, fn.__name__)
    return decorator

def conditional_call_count(discriminator, categories):
    """Apply a function call count conditionally at runtime.

    Takes two arguments, a callable that returns a key value, and a dict
    mapping key values to a tuple of arguments to function_call_count.

    The callable is not evaluated until the decorated function is actually
    invoked.  If the `discriminator` returns a key not present in the
    `categories` dictionary, no call count assertion is applied.

    Useful for integration tests, where running a named test in isolation may
    have a function count penalty not seen in the full suite, due to lazy
    initialization in the DB-API, SA, etc.
    """

    def decorator(fn):
        def at_runtime(*args, **kw):
            criteria = categories.get(discriminator(), None)
            if criteria is None:
                return fn(*args, **kw)

            rewrapped = function_call_count(*criteria)(fn)
            return rewrapped(*args, **kw)
        return function_named(at_runtime, fn.__name__)
    return decorator


def _profile(filename, fn, *args, **kw):
    global profiler
    if not profiler:
        if sys.version_info > (2, 5):
            try:
                import cProfile
                profiler = 'cProfile'
            except ImportError:
                pass
        if not profiler:
            try:
                import hotshot
                profiler = 'hotshot'
            except ImportError:
                profiler = 'skip'

    if profiler == 'skip':
        raise SkipTest('Profiling not supported on this platform')
    elif profiler == 'cProfile':
        return _profile_cProfile(filename, fn, *args, **kw)
    else:
        return _profile_hotshot(filename, fn, *args, **kw)

def _profile_cProfile(filename, fn, *args, **kw):
    import cProfile, gc, pstats, time

    load_stats = lambda: pstats.Stats(filename)
    gc_collect()

    began = time.time()
    cProfile.runctx('result = fn(*args, **kw)', globals(), locals(),
                    filename=filename)
    ended = time.time()

    return ended - began, load_stats, locals()['result']

def _profile_hotshot(filename, fn, *args, **kw):
    import gc, hotshot, hotshot.stats, time
    load_stats = lambda: hotshot.stats.load(filename)

    gc_collect()
    prof = hotshot.Profile(filename)
    began = time.time()
    prof.start()
    try:
        result = fn(*args, **kw)
    finally:
        prof.stop()
        ended = time.time()
        prof.close()

    return ended - began, load_stats, result

