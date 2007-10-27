"""Profiling support for unit and performance tests."""

import testbase
from testlib.config import parser, post_configure
import testlib.config
import os

__all__ = 'profiled',

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
        try:
            profiled.__name__ = fn.__name__
        except:
            pass
        return profiled
    return decorator
