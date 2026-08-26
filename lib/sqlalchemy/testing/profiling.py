# testing/profiling.py
# Copyright (C) 2005-2026 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php
# mypy: allow-untyped-calls
#
# allow-untyped-calls is for sqlalchemy.testing.config, which is itself
# not typed.


"""Profiling support for unit and performance tests.

These are special purpose profiling methods which operate
in a more fine-grained way than nose's profiling plugin.

"""

from __future__ import annotations

import collections
import contextlib
import os
import platform
import pstats
import re
import sys
from typing import Any
from typing import Callable
from typing import DefaultDict
from typing import Generator
from typing import Optional
from typing import Tuple

from . import config
from .profiles_file import PlatformKey
from .profiles_file import ProfilesFile
from .profiles_file import TestKey
from .util import gc_collect
from ..util import freethreading
from ..util import has_compiled_ext

try:
    import cProfile
except ImportError:
    cProfile = None  # type: ignore[assignment]

_profile_stats: ProfileStatsFile = None  # type: ignore[assignment]
"""global ProfileStatsFileInstance.

plugin_base assigns this at the start of all tests.

"""


_current_test: TestKey = None  # type: ignore[assignment]
"""String id of current test.

plugin_base assigns this at the start of each test using
_start_current_test.

"""


def _start_current_test(id_: TestKey) -> None:
    global _current_test
    _current_test = id_

    if _profile_stats.force_write:
        _profile_stats.reset_count()


class ProfileStatsFile:
    """Store per-platform/fn profiling results in a file.

    There was no json module available when this was written, but now
    the file format which is very deterministically line oriented is kind of
    handy in any case for diffs and merges.

    The parsing and rendering of the file itself, as well as of the platform
    keys used within it, is in :mod:`.testing.profiles_file`, which is shared
    with ``tools/profiles.py``.  This class adds the bookkeeping needed while
    the test suite runs.

    """

    def __init__(
        self,
        filename: str,
        sort: str = "cumulative",
        dump: Optional[str] = None,
    ) -> None:
        self.force_write = (
            config.options is not None and config.options.force_write_profiles
        )
        self.write = self.force_write or (
            config.options is not None and config.options.write_profiles
        )
        self.profiles = ProfilesFile(filename)
        self.fname = self.profiles.path
        self.short_fname = os.path.split(self.fname)[-1]
        self.dump = dump
        self.sort = sort

        # (test key, platform key) -> number of counts consumed so far
        # within the current test
        self._current_counts: DefaultDict[Tuple[TestKey, PlatformKey], int] = (
            collections.defaultdict(int)
        )

        if self.write:
            # rewrite for the case where features changed,
            # etc.
            self._write()

    @property
    def platform_key(self) -> PlatformKey:
        db = config.db
        assert db is not None, "no database configured"

        dbapi_flags = []
        if db.dialect.is_async:
            dbapi_flags.append("async")
        if db.name == "sqlite" and db.dialect._is_url_file_db(db.url):
            dbapi_flags.append("file")

        # major.minor only; the micro version isn't part of the key
        py_version = f"{sys.version_info.major}.{sys.version_info.minor}"
        if freethreading:
            py_version += "t"

        return PlatformKey(
            machine=platform.machine(),
            system=platform.system().lower(),
            implementation=platform.python_implementation().lower(),
            python=py_version,
            dialect=db.name,
            driver=db.driver,
            dbapi_flags=tuple(dbapi_flags),
            cextensions=has_compiled_ext(),
        )

    def has_stats(self) -> bool:
        return (
            self.profiles.counts_for(_current_test, self.platform_key)
            is not None
        )

    def result(self, callcount: int) -> Optional[Tuple[Optional[int], int]]:
        test_key = _current_test
        platform_key = self.platform_key

        counts = self.profiles.setdefault_counts(test_key, platform_key)
        current_count = self._current_counts[test_key, platform_key]

        result: Optional[Tuple[Optional[int], int]]
        if len(counts) <= current_count:
            counts.append(callcount)
            if self.write:
                self._write()
            result = None
        else:
            result = (
                self.profiles.lineno_for(test_key, platform_key),
                counts[current_count],
            )
        self._current_counts[test_key, platform_key] = current_count + 1
        return result

    def reset_count(self) -> None:
        counts = self.profiles.counts_for(_current_test, self.platform_key)
        if counts is not None:
            counts[:] = []

    def replace(self, callcount: int) -> None:
        test_key = _current_test
        platform_key = self.platform_key

        counts = self.profiles.counts_for(test_key, platform_key)
        assert counts, "replace() called before result() recorded a count"

        current_count = self._current_counts[test_key, platform_key]
        if current_count < len(counts):
            counts[current_count - 1] = callcount
        else:
            counts[-1] = callcount
        if self.write:
            self._write()

    def _write(self) -> None:
        print("Writing profile file %s" % self.fname)
        self.profiles.write()


def function_call_count(
    variance: float = 0.05, times: int = 1, warmup: int = 0
) -> Callable[..., Any]:
    """Assert a target for a test case's function call count.

    The main purpose of this assertion is to detect changes in
    callcounts for various functions - the actual number is not as important.
    Callcounts are stored in a file keyed to Python version and OS platform
    information.  This file is generated automatically for new tests,
    and versioned so that unexpected changes in callcounts will be detected.

    """

    from sqlalchemy.util import decorator

    @decorator
    def wrap(fn: Callable[..., Any], *args: Any, **kw: Any) -> Any:
        for warm in range(warmup):
            fn(*args, **kw)

        timerange = range(times)
        with count_functions(variance=variance):
            for time in timerange:
                rv = fn(*args, **kw)
            return rv

    return wrap


@contextlib.contextmanager
def count_functions(variance: float = 0.05) -> Generator[None, None, None]:
    if cProfile is None:
        raise config._skip_test_exception("cProfile is not installed")

    if not _profile_stats.has_stats() and not _profile_stats.write:
        config.skip_test(
            "No profiling stats available on this "
            "platform for this function.  Run tests with "
            "--write-profiles to add statistics to %s for "
            "this platform." % _profile_stats.short_fname
        )

    gc_collect()

    pr = cProfile.Profile()
    pr.enable()
    # began = time.time()
    yield
    # ended = time.time()
    pr.disable()

    # s = StringIO()
    stats = pstats.Stats(pr, stream=sys.stdout)

    # timespent = ended - began
    callcount = stats.total_calls  # type: ignore[attr-defined]

    expected = _profile_stats.result(callcount)

    if expected is None:
        expected_count = None
    else:
        line_no, expected_count = expected

    print("Pstats calls: %d Expected %s" % (callcount, expected_count))
    stats.sort_stats(*re.split(r"[, ]", _profile_stats.sort))
    stats.print_stats()
    if _profile_stats.dump:
        base, ext = os.path.splitext(_profile_stats.dump)
        test_name = _current_test.split(".")[-1]
        dumpfile = "%s_%s%s" % (base, test_name, ext or ".profile")
        stats.dump_stats(dumpfile)
        print("Dumped stats to file %s" % dumpfile)
    # stats.print_callers()
    if _profile_stats.force_write:
        _profile_stats.replace(callcount)
    elif expected_count:
        deviance = int(callcount * variance)
        failed = abs(callcount - expected_count) > deviance

        if failed:
            if _profile_stats.write:
                _profile_stats.replace(callcount)
            else:
                raise AssertionError(
                    "Adjusted function call count %s not within %s%% "
                    "of expected %s, platform %s. Rerun with "
                    "--write-profiles to "
                    "regenerate this callcount."
                    % (
                        callcount,
                        (variance * 100),
                        expected_count,
                        _profile_stats.platform_key,
                    )
                )
