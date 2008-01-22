"""Profiling support for unit and performance tests."""

import os, sys
from testlib.config import parser, post_configure
from testlib.compat import *
import testlib.config

__all__ = 'profiled', 'function_call_count', 'conditional_call_count'

all_targets = set()
profile_config = { 'targets': set(),
                   'report': True,
                   'sort': ('time', 'calls'),
                   'limit': None }

def profiled(target=None, **target_opts):
    """Optional function profiling.

    @profiled('label')
    or
    @profiled('label', report=True, sort=('calls',), limit=20)

    Enables profiling for a function when 'label' is targetted for
    profiling.  Report options can be supplied, and override the global
    configuration and command-line options.
    """

    import time, hotshot, hotshot.stats

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

            prof = hotshot.Profile(filename)
            began = time.time()
            prof.start()
            try:
                result = fn(*args, **kw)
            finally:
                prof.stop()
                ended = time.time()
                prof.close()

            if not testlib.config.options.quiet:
                print "Profiled target '%s', wall time: %.2f seconds" % (
                    target, ended - began)

            report = target_opts.get('report', profile_config['report'])
            if report and testlib.config.options.verbose:
                sort_ = target_opts.get('sort', profile_config['sort'])
                limit = target_opts.get('limit', profile_config['limit'])
                print "Profile report for target '%s' (%s)" % (
                    target, filename)

                stats = hotshot.stats.load(filename)
                stats.sort_stats(*sort_)
                if limit:
                    stats.print_stats(limit)
                else:
                    stats.print_stats()

            assert_range = target_opts.get('call_range')
            if assert_range:
                if isinstance(assert_range, dict):
                    assert_range = assert_range.get(testlib.config.db, 'default')
                stats = hotshot.stats.load(filename)
                assert stats.total_calls >= assert_range[0] and stats.total_calls <= assert_range[1], stats.total_calls

            os.unlink(filename)
            return result
        return _function_named(profiled, fn.__name__)
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

    while version_info:
        version = '.'.join([str(v) for v in version_info])
        if version in versions:
            count = versions[version]
            break
        version_info.pop()

    if count is None:
        return lambda fn: fn

    import hotshot, hotshot.stats

    def decorator(fn):
        def counted(*args, **kw):
            try:
                filename = "%s.prof" % fn.__name__

                prof = hotshot.Profile(filename)
                prof.start()
                try:
                    result = fn(*args, **kw)
                finally:
                    prof.stop()
                    prof.close()

                stats = hotshot.stats.load(filename)
                calls = stats.total_calls
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
        return _function_named(counted, fn.__name__)
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
        return _function_named(at_runtime, fn.__name__)
    return decorator
