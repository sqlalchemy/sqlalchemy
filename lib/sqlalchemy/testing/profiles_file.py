# testing/profiles_file.py
# Copyright (C) 2005-2026 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php

"""Format of the call count file used by ``test/aaa_profiling/``.

``test/profiles.txt`` stores the expected function call counts asserted by
the profiling suite.  Each line is keyed to a "platform key" that encodes
the machine architecture, operating system, Python implementation and
version, the database / DBAPI in use, and whether or not the C extensions
were built:

.. sourcecode:: txt

    test.aaa_profiling.test_orm.MergeTest.test_merge_load \
x86_64_linux_cpython_3.14_postgresql_psycopg_dbapiunicode_cextensions 1234

A call count is only asserted when the running environment matches a key
that's present in the file; otherwise the test is skipped.

Two different things read and write this file: :mod:`.testing.profiling`,
which records counts as the suite runs, and ``tools/profiles.py``, which
maintains the file from the outside.  Both use this module so that the
format is spelled out in exactly one place.

.. note::

    ``tools/profiles.py`` loads this module directly from the source tree,
    without importing the ``sqlalchemy`` package, so that the maintenance
    tooling and the noxfile that drives it work in an environment where
    SQLAlchemy itself isn't installed.  Keep the imports here to the
    standard library only.

"""

from __future__ import annotations

import os
import re
from typing import Dict
from typing import KeysView
from typing import List
from typing import NamedTuple
from typing import Optional
from typing import Set
from typing import Tuple
from typing import TypeAlias
from typing import Union

HEADER = """\
# SQLAlchemy call count profiles.
# This file is written out on a per-environment basis.
# For each test in aaa_profiling, the corresponding function and
# environment is located within this file.  If it doesn't exist,
# the test is skipped.
# If a callcount does exist, it is compared to what we received.
# assertions are raised if the counts do not match.
#
# To add a new callcount test, apply the function_call_count
# decorator and re-run the tests using the --write-profiles
# option - this file will be rewritten including the new count.
#
"""

DB_TOKEN_TO_DATABASE = {
    "sqlite": "sqlite",
    "postgresql": "postgresql",
    "mysql": "mysql",
    "mariadb": "mysql",
    "oracle": "oracle",
    "mssql": "mssql",
}
"""Map the dialect name found in a platform key to a ``--db`` name.

The dialect name is what the *server* turned out to be, so e.g. running
``--db mysql`` against a MariaDB server records ``mariadb`` in the key.

"""

TestKey: TypeAlias = str
"""A test's id as reported by the plugin, e.g.
``test.aaa_profiling.test_orm.MergeTest.test_merge_load``.

An opaque string; the profiling suite never takes one apart.

"""

_EMPTY_COUNTS: Dict[PlatformKey, List[int]] = {}

_PY_VERSION = re.compile(r"^\d+\.\d+t?$")

_UNICODE_TOKEN = "dbapiunicode"


class PlatformKey(NamedTuple):
    """The environment half of a line in ``test/profiles.txt``."""

    machine: str
    """``platform.machine()``, e.g. ``x86_64``; may contain underscores."""

    system: str
    """lower case ``platform.system()``, e.g. ``linux``."""

    implementation: str
    """lower case ``platform.python_implementation()``, e.g. ``cpython``."""

    python: str
    """major.minor Python version, with a ``t`` suffix for freethreading."""

    dialect: str
    """the dialect name reported by the backend, e.g. ``mariadb``."""

    driver: str
    """the DBAPI name, e.g. ``psycopg``."""

    dbapi_flags: Tuple[str, ...]
    """extra DBAPI tokens; ``async`` and / or ``file``, in that order."""

    cextensions: bool
    """whether the C / Cython extensions were built."""

    def __str__(self) -> str:
        return "_".join(
            [
                self.machine,
                self.system,
                self.implementation,
                self.python,
                self.dialect,
                self.driver,
                *self.dbapi_flags,
                _UNICODE_TOKEN,
                self.cext_token,
            ]
        )

    @classmethod
    def parse(cls, key: str) -> PlatformKey:
        """Parse a platform key string.

        Raises ``ValueError`` if the key isn't in the format written by
        :meth:`.PlatformKey.__str__`.

        """

        tokens = key.split("_")

        if tokens[-1] not in ("cextensions", "nocextensions"):
            raise ValueError(f"unrecognized cextensions token in {key!r}")
        cextensions = tokens.pop() == "cextensions"

        if tokens[-1] != _UNICODE_TOKEN:
            raise ValueError(f"unrecognized {_UNICODE_TOKEN} token in {key!r}")
        tokens.pop()

        for idx, token in enumerate(tokens):
            if _PY_VERSION.match(token):
                break
        else:
            raise ValueError(f"no python version found in {key!r}")

        if idx < 3 or len(tokens) < idx + 3:
            raise ValueError(f"can't parse platform key {key!r}")

        return cls(
            machine="_".join(tokens[: idx - 2]),
            system=tokens[idx - 2],
            implementation=tokens[idx - 1],
            python=tokens[idx],
            dialect=tokens[idx + 1],
            driver=tokens[idx + 2],
            dbapi_flags=tuple(tokens[idx + 3 :]),
            cextensions=cextensions,
        )

    @property
    def database(self) -> str:
        """the ``--db`` name that would regenerate this key."""

        return DB_TOKEN_TO_DATABASE.get(self.dialect, self.dialect)

    @property
    def cext_token(self) -> str:
        return "cextensions" if self.cextensions else "nocextensions"


class ProfilesFile:
    """The parsed contents of ``test/profiles.txt``.

    The entries are reached through the accessors below rather than through
    the underlying dictionary; :meth:`.ProfilesFile.setdefault_counts` and
    :meth:`.ProfilesFile.counts_for` hand back the live list of call counts
    for a test on a platform, which the caller is free to mutate in place,
    and :meth:`.ProfilesFile.write` then serializes whatever is present.

    """

    path: str

    _data: Dict[TestKey, Dict[PlatformKey, List[int]]]
    """test key -> platform key -> call counts."""

    linenos: Dict[Tuple[TestKey, PlatformKey], int]
    """(test key, platform key) -> one based line number as read."""

    def __init__(self, path: Union[str, os.PathLike[str]]):
        self.path = os.path.abspath(path)
        self._data = {}
        self.linenos = {}
        self.read()

    def read(self) -> None:
        """Populate from the file, if it exists.

        Raises ``ValueError`` for a line that isn't in the format written
        by :meth:`.ProfilesFile.write`, including a platform key that
        :meth:`.PlatformKey.parse` doesn't recognize.

        """

        self._data.clear()
        self.linenos.clear()

        try:
            profile_f = open(self.path)
        except OSError:
            return

        with profile_f:
            for lineno, line in enumerate(profile_f, 1):
                line = line.strip()
                if not line or line.startswith("#"):
                    continue

                try:
                    test_key, raw_key, counts = line.split()
                    platform_key = PlatformKey.parse(raw_key)
                    call_counts = [int(count) for count in counts.split(",")]
                except ValueError as ve:
                    raise ValueError(
                        f"{self.path}:{lineno}: can't parse {line!r}: {ve}"
                    ) from ve

                self._data.setdefault(test_key, {})[platform_key] = call_counts
                self.linenos[test_key, platform_key] = lineno

    def write(self) -> None:
        """Rewrite the file from what's currently recorded."""

        with open(self.path, "w") as profile_f:
            profile_f.write(HEADER)
            for test_key in sorted(self._data):
                per_fn = self._data[test_key]
                profile_f.write("\n# TEST: %s\n\n" % test_key)
                for platform_key in sorted(per_fn, key=str):
                    counts = ",".join(
                        str(count) for count in per_fn[platform_key]
                    )
                    profile_f.write(
                        "%s %s %s\n" % (test_key, platform_key, counts)
                    )

    def test_keys(self) -> KeysView[TestKey]:
        """All test keys present in the file."""

        return self._data.keys()

    def platforms_for(self, test_key: TestKey) -> KeysView[PlatformKey]:
        """The platform keys recorded for one test."""

        return self._data.get(test_key, _EMPTY_COUNTS).keys()

    def counts_for(
        self, test_key: TestKey, platform_key: PlatformKey
    ) -> Optional[List[int]]:
        """The call counts recorded for a test on a platform, or ``None``.

        The list returned is the one stored here; mutating it in place
        updates what :meth:`.ProfilesFile.write` will emit.

        """

        return self._data.get(test_key, _EMPTY_COUNTS).get(platform_key)

    def setdefault_counts(
        self, test_key: TestKey, platform_key: PlatformKey
    ) -> List[int]:
        """Like :meth:`.ProfilesFile.counts_for`, adding an empty entry for
        the test and platform if there isn't one yet.

        """

        return self._data.setdefault(test_key, {}).setdefault(platform_key, [])

    def remove_entry(
        self, test_key: TestKey, platform_key: PlatformKey
    ) -> None:
        """Drop one test / platform entry.

        The test itself is dropped once its last platform is gone.

        """

        per_fn = self._data.get(test_key)
        if per_fn is None:
            return
        per_fn.pop(platform_key, None)
        if not per_fn:
            del self._data[test_key]

    def remove_test(self, test_key: TestKey) -> int:
        """Drop every entry for one test, returning how many there were."""

        return len(self._data.pop(test_key, {}))

    def platform_keys(self) -> Set[PlatformKey]:
        """All platform keys present anywhere in the file."""

        return {
            platform_key
            for per_fn in self._data.values()
            for platform_key in per_fn
        }

    def entry_count(self) -> int:
        """Total number of recorded lines."""

        return sum(len(per_fn) for per_fn in self._data.values())

    def lineno_for(
        self, test_key: TestKey, platform_key: PlatformKey
    ) -> Optional[int]:
        return self.linenos.get((test_key, platform_key))
