"""Nox configuration for SQLAlchemy."""

from __future__ import annotations

import multiprocessing
import os
from pathlib import Path
import sys
from typing import Dict
from typing import List
from typing import Set
from typing import Tuple

import nox
from nox.command import CommandFailed

if sys.version_info > (3, 12):
    nox.needs_version = ">=2025.10.16"

nox.options.default_venv_backend = "venv"

if True:
    sys.path.insert(0, ".")
    from tools.profiles import PROFILE_DATABASES
    from tools.profiles import PROFILE_PYTHONS
    from tools.profiles import recorded_databases
    from tools.toxnox import apply_pytest_opts
    from tools.toxnox import extract_opts
    from tools.toxnox import tox_parameters


PYTHON_VERSIONS = ["3.11", "3.12", "3.13", "3.14", "3.14t", "3.15"]
DATABASES = ["sqlite", "sqlite_file", "postgresql", "mysql", "oracle", "mssql"]
CEXT = ["_auto", "cext", "nocext"]
GREENLET = ["_greenlet", "nogreenlet"]
BACKENDONLY = ["_all", "backendonly", "memusage"]

PROFILE_DBAPI_BUILDS: Dict[str, List[Tuple[List[str], List[str]]]] = {
    "postgresql": [
        # psycopg 3's C speedups ship as a separate distribution which is
        # picked up at import time; they're worth tens of thousands of
        # calls in the resultset suite, and the platform key records which
        # build was loaded, so both get their own entries.  Uninstalling
        # is what selects the pure Python build - the venv is reused
        # between runs, so its absence can't be assumed.
        (["psycopg-binary", "psycopg-c"], ["sqlalchemy[postgresql-psycopg]"]),
    ],
}
"""Extra DBAPI builds the ``profiles`` session regenerates a backend for.

Keyed on backend name, each entry is one additional pass over the suite,
as ``(packages to uninstall first, packages to install)``.  These run
before the pass made with the backend's ``tests-<db>`` dependency group,
which is the only pass for a backend that isn't listed here.

"""

WHEEL_TEST_SCOPES: Dict[str, List[str]] = {
    "full": ["test"],
    "reduced": [
        "test/base",
        "test/engine/test_processors.py",
        "test/sql/test_resultset.py",
    ],
}
"""Test paths run by the ``test-wheel`` session, per scope.

"reduced" covers the compiled extensions only, for emulated
architectures where the full suite is too slow to run per wheel.

"""

# table of ``--dbdriver`` names to use on the pytest command line, which
# match to dialect names
DB_CLI_NAMES = {
    "sqlite": {
        "nogreenlet": {"sqlite", "pysqlite_numeric"},
        "greenlet": {"aiosqlite"},
    },
    "sqlite_file": {
        "nogreenlet": {"sqlite"},
        "greenlet": {"aiosqlite"},
    },
    "postgresql": {
        "nogreenlet": {"psycopg2", "pg8000", "psycopg"},
        "greenlet": {"asyncpg", "psycopg_async"},
    },
    "mysql": {
        "nogreenlet": {"mysqldb", "pymysql", "mariadbconnector"},
        "greenlet": {"asyncmy", "aiomysql"},
    },
    "oracle": {
        "nogreenlet": {"oracledb"},
        "greenlet": {"oracledb_async"},
    },
    "mssql": {
        "nogreenlet": {"pyodbc", "pymssql", "mssqlpython"},
        "greenlet": {"aioodbc"},
    },
}


def _setup_for_driver(
    session: nox.Session,
    cmd: List[str],
    basename: str,
    greenlet: bool = False,
) -> None:

    # install driver deps listed out in pyproject.toml
    nogreenlet_deps = f"tests-{basename.replace('_', '-')}"
    greenlet_deps = f"tests-{basename.replace('_', '-')}-asyncio"

    deps = nox.project.dependency_groups(
        pyproject,
        (greenlet_deps if greenlet else nogreenlet_deps),
    )
    if deps:
        session.install(*deps)

    # set up top level ``--db`` sent to pytest command line, which looks
    # up a base URL in the [db] section of setup.cfg.   Environment variable
    # substitution used by CI is also available.

    # e.g. TOX_POSTGRESQL, TOX_MYSQL, etc.
    dburl_env = f"TOX_{basename.upper()}"
    # e.g. --db=postgresql, --db=mysql, etc.
    default_dburl = f"--db={basename}"
    cmd.extend(os.environ.get(dburl_env, default_dburl).split())

    # set up extra drivers using --dbdriver.   this first looks in
    # an environment variable before making use of the DB_CLI_NAMES
    # lookup table

    # e.g. EXTRA_PG_DRIVERS, EXTRA_MYSQL_DRIVERS, etc.
    if basename == "postgresql":
        extra_driver_env = "EXTRA_PG_DRIVERS"
    else:
        extra_driver_env = f"EXTRA_{basename.upper()}_DRIVERS"
    env_dbdrivers = os.environ.get(extra_driver_env, None)
    if env_dbdrivers:
        cmd.extend(env_dbdrivers.split())
        return

    # use fixed names in DB_CLI_NAMES
    extra_drivers: Dict[str, Set[str]] = DB_CLI_NAMES[basename]
    dbdrivers = extra_drivers["nogreenlet"]
    if greenlet:
        dbdrivers.update(extra_drivers["greenlet"])

    # use equals sign so that we avoid
    # https://github.com/pytest-dev/pytest/issues/13913
    cmd.extend([f"--dbdriver={dbdriver}" for dbdriver in dbdrivers])


pyproject = nox.project.load_toml("pyproject.toml")

nox.options.sessions = ["tests"]
nox.options.tags = ["py"]


@nox.session()
@tox_parameters(
    ["python", "database", "cext", "greenlet", "backendonly"],
    [
        PYTHON_VERSIONS,
        DATABASES,
        CEXT,
        GREENLET,
        BACKENDONLY,
    ],
)
def tests(
    session: nox.Session,
    database: str,
    greenlet: str,
    backendonly: str,
    cext: str,
) -> None:
    """run the main test suite"""

    _tests(
        session,
        database,
        greenlet=greenlet == "_greenlet",
        backendonly=backendonly == "backendonly",
        platform_intensive=backendonly == "memusage",
        cext=cext,
    )


@nox.session(name="coverage")
@tox_parameters(
    ["database", "cext", "backendonly"],
    [DATABASES, CEXT, ["_all", "backendonly"]],
    base_tag="coverage",
)
def coverage(
    session: nox.Session, database: str, cext: str, backendonly: str
) -> None:
    """Run tests with coverage."""

    _tests(
        session,
        database,
        cext,
        timing_intensive=False,
        backendonly=backendonly == "backendonly",
        coverage=True,
    )


@nox.session(name="github-cext-greenlet")
def github_cext_greenlet(session: nox.Session) -> None:
    """run tests for github actions"""

    _tests(session, "sqlite", "cext", greenlet=True, timing_intensive=False)


@nox.session(name="github-cext")
def github_cext(session: nox.Session) -> None:
    """run tests for github actions"""

    _tests(session, "sqlite", "cext", greenlet=False, timing_intensive=False)


@nox.session(name="github-nocext")
def github_nocext(session: nox.Session) -> None:
    """run tests for github actions"""

    _tests(session, "sqlite", "nocext", greenlet=False)


@nox.session(name="test-wheel")
@nox.parametrize(
    "scope", [nox.param(scope, id=scope) for scope in WHEEL_TEST_SCOPES]
)
def test_wheel(session: nox.Session, scope: str) -> None:
    """test a wheel that is already installed in the current environment.

    Run by cibuildwheel from inside the environment it created for the
    wheel it just built, so nothing is installed here and no venv is
    created::

        nox -f {project}/noxfile.py --no-venv -s "test-wheel(reduced)"

    The "reduced" scope is for architectures that have no runner and are
    built under QEMU emulation, where the full suite takes about an hour
    per wheel.  Only the compiled extensions vary by architecture, so it
    runs just the suites that exercise them, which includes the paired
    Py*/Cy* tests comparing each compiled module against the pure Python
    module it was generated from.

    """

    # nothing is installed by this session; the wheel under test is
    # already present, and nothing may shadow it
    session.env["PYTHONPATH"] = ""

    # PYTHONNOUSERSITE disables the ./lib/ path insertion in
    # test/conftest.py, so that the installed wheel is imported rather
    # than the local checkout.  equivalent to passing -s to python
    session.env["PYTHONNOUSERSITE"] = "1"

    # a wheel with no compiled extensions must fail here rather than
    # silently skipping the suites that require them
    session.env["REQUIRE_SQLALCHEMY_CEXT"] = "1"

    cmd = ["python", "-m", "pytest"]
    cmd.extend(os.environ.get("TOX_WORKERS", "-n4").split())
    cmd.extend(["-q", "--nomemory", "--notimingintensive", "--nomypy"])
    cmd.extend(WHEEL_TEST_SCOPES[scope])

    session.run(*cmd)


def _tests(
    session: nox.Session,
    database: str,
    cext: str = "_auto",
    greenlet: bool = True,
    backendonly: bool = False,
    platform_intensive: bool = False,
    timing_intensive: bool = True,
    coverage: bool = False,
) -> None:

    # ensure external PYTHONPATH not interfering
    session.env["PYTHONPATH"] = ""

    # PYTHONNOUSERSITE - this *MUST* be set so that the ./lib/ import
    # set up explicitly in test/conftest.py is *disabled*, so that
    # when SQLAlchemy is built into the .nox area, we use that and not the
    # local checkout, at least when usedevelop=False
    session.env["PYTHONNOUSERSITE"] = "1"

    freethreaded = isinstance(session.python, str) and session.python.endswith(
        "t"
    )

    if freethreaded:
        session.env["PYTHON_GIL"] = "0"

        # greenlet frequently crashes with freethreading, so omit
        # for the near future
        greenlet = False

    session.env["SQLALCHEMY_WARN_20"] = "1"

    if cext == "cext":
        session.env["REQUIRE_SQLALCHEMY_CEXT"] = "1"
    elif cext == "nocext":
        session.env["DISABLE_SQLALCHEMY_CEXT"] = "1"

    includes_excludes: dict[str, list[str]] = {"k": [], "m": []}

    if coverage:
        timing_intensive = False

    if platform_intensive:
        # platform_intensive refers to test/aaa_profiling/test_memusage.py.
        # it's only run exclusively of all other tests.   does not include
        # greenlet related tests
        greenlet = False
        # with "-m memory_intensive", only that suite will run, all
        # other tests will be deselected by pytest
        includes_excludes["m"].append("memory_intensive")
    elif backendonly:
        # with "-m backendonly", only tests with the backend pytest mark
        # (or pytestplugin equivalent, like __backend__) will be selected
        # by pytest.
        # memory intensive is deselected to prevent these from running
        includes_excludes["m"].extend(["backend", "not memory_intensive"])
    else:
        includes_excludes["m"].append("not memory_intensive")

        # the mypy suite is also run exclusively from the test_mypy
        # session
        includes_excludes["m"].append("not mypy")

        if not timing_intensive:
            includes_excludes["m"].append("not timing_intensive")

    cmd = ["python", "-m", "pytest"]

    default_workers = f"-n{int(multiprocessing.cpu_count() * 0.8)}"
    cmd.extend(os.environ.get("TOX_WORKERS", default_workers).split())

    if coverage:
        assert not platform_intensive
        includes_excludes["k"].append("not aaa_profiling")
        session.install("-e", ".")
        session.install(*nox.project.dependency_groups(pyproject, "coverage"))
    else:
        session.install(".")

    session.install(*nox.project.dependency_groups(pyproject, "tests"))

    if greenlet:
        session.install(
            *nox.project.dependency_groups(pyproject, "tests_greenlet")
        )
    else:
        # note: if on SQLAlchemy 2.0, for "nogreenlet" need to do an explicit
        # uninstall of greenlet since it's included in sqlalchemy dependencies
        # in 2.1 it's an optional dependency
        session.run("pip", "uninstall", "-y", "greenlet")

    _setup_for_driver(session, cmd, database, greenlet=greenlet)

    for letter, collection in includes_excludes.items():
        if collection:
            cmd.extend([f"-{letter}", " and ".join(collection)])

    posargs = apply_pytest_opts(
        session,
        "sqlalchemy",
        [
            database,
            cext,
            "_greenlet" if greenlet else "nogreenlet",
            "memusage" if platform_intensive else "_nomemusage",
            "backendonly" if backendonly else "_notbackendonly",
        ],
        coverage=coverage,
    )

    if database in ["oracle", "mssql"]:
        cmd.extend(["--low-connections"])

    if database in ["oracle", "mssql", "sqlite_file"]:
        # use equals sign so that we avoid
        # https://github.com/pytest-dev/pytest/issues/13913
        cmd.extend(["--write-idents=db_idents.txt"])

    cmd.extend(posargs)

    try:
        session.run(*cmd)
    finally:
        # Run cleanup for oracle/mssql
        if database in ["oracle", "mssql", "sqlite_file"] and os.path.exists(
            "db_idents.txt"
        ):
            session.run("python", "reap_dbs.py", "db_idents.txt")
            os.unlink("db_idents.txt")


@nox.session(name="profiles")
@tox_parameters(
    ["python", "cext", "database"],
    [PROFILE_PYTHONS, ["cext", "nocext"], PROFILE_DATABASES],
    base_tag="profiles",
)
def profiles(session: nox.Session, cext: str, database: str) -> None:
    """regenerate the call counts in test/profiles.txt.

    This replaces the former ``regen_callcounts.tox.ini`` runner.  With no
    arguments the full matrix is regenerated, which is every interpreter in
    ``tools/profiles.py -> PROFILE_PYTHONS``, with and without the C
    extensions, against every backend in ``PROFILE_DATABASES``::

        nox -s profiles

    Individual cells of that matrix are addressable in the usual ways::

        nox -s "profiles(py314-nocext-postgresql)"
        nox -t py314-profiles

    The backend is a session parameter rather than a pytest argument, so
    that each cell installs its own drivers; a URL other than the built in
    one for a backend comes from ``TOX_<BACKEND>`` in the environment, the
    same as for the ``tests`` session.

    A subset of the suite may be passed through to pytest, in which case
    the cells for backends that have no counts recorded for those tests
    skip themselves before installing anything.   Most of
    ``test/aaa_profiling/`` requires an in-memory SQLite database, so
    regenerating e.g. the ORM suite runs one backend rather than five::

        nox -s profiles -- test/aaa_profiling/test_orm.py

    That decision is made by looking at what's already in
    ``test/profiles.txt``, so a brand new test file, having nothing recorded
    yet, runs on every backend.  ``--all-dbs`` says the same thing
    explicitly, for a selection that is partly recorded already::

        nox -s profiles -- --all-dbs test/aaa_profiling/test_new_thing.py

    A backend whose DBAPI ships in more than one build - psycopg 3, with
    and without its C speedups - is run once per build, since the platform
    key records which one the counts came from; see
    ``PROFILE_DBAPI_BUILDS``.

    Entries for interpreters and backends that are no longer regenerated
    stay in the file until they're removed; see
    ``python tools/profiles.py --help``.

    """

    posargs, opts = extract_opts(session.posargs, "all-dbs")

    if any(
        arg.startswith(("-n", "--numprocesses", "--dist")) for arg in posargs
    ):
        session.error(
            "profiles cannot be regenerated under pytest-xdist; "
            "test/profiles.txt is rewritten in place by the test run"
        )

    if any(arg.split("=")[0] in ("--db", "--dburi") for arg in posargs):
        session.error(
            "the backend is a session parameter here, e.g. "
            "nox -s 'profiles(py314-nocext-postgresql)'; set "
            f"TOX_{database.upper()} to run it against a different URL"
        )

    # bail before installing anything; a given selection usually only has
    # counts recorded for one backend, leaving most cells with nothing to do
    if not opts.all_dbs and database not in recorded_databases(posargs):
        session.skip(
            f"no call counts recorded for {database} for this selection; "
            "pass --all-dbs to run it anyway"
        )

    # ensure external PYTHONPATH not interfering; see comments in _tests()
    session.env["PYTHONPATH"] = ""
    session.env["PYTHONNOUSERSITE"] = "1"

    if cext == "cext":
        session.env["REQUIRE_SQLALCHEMY_CEXT"] = "1"
    else:
        session.env["DISABLE_SQLALCHEMY_CEXT"] = "1"

    session.install(".")
    session.install(*nox.project.dependency_groups(pyproject, "tests"))

    cmd = ["python", "-m", "pytest"]

    # no -n; the profile file is rewritten in place as tests run.
    # memory / timing intensive suites don't record call counts at all
    cmd.extend(
        [
            "-x",
            "-m",
            "not memory_intensive and not timing_intensive",
            "--force-write-profiles",
        ]
    )

    if not any(arg.startswith(("test/", f"test{os.sep}")) for arg in posargs):
        cmd.append("test/aaa_profiling")

    # e.g. TOX_POSTGRESQL, TOX_MYSQL, etc.
    dburl_env = f"TOX_{database.upper()}"
    cmd.extend(os.environ.get(dburl_env, f"--db={database}").split())

    if database in ("oracle", "mssql"):
        cmd.append("--low-connections")

    cmd.extend(posargs)

    # a DBAPI that ships in more than one build gets a pass for each, since
    # the platform key tells them apart; the dependency group install is
    # the last of them so that the venv is left in its normal state
    deps = nox.project.dependency_groups(
        pyproject, f"tests-{database.replace('_', '-')}"
    )
    for uninstall, install in PROFILE_DBAPI_BUILDS.get(database, []) + [
        ([], deps)
    ]:
        if uninstall:
            session.run("python", "-m", "pip", "uninstall", "-y", *uninstall)
        if install:
            session.install(*install)

        session.run(*cmd)

    session.log(
        "call counts written; 'python tools/profiles.py check' will report "
        "entries in test/profiles.txt that are no longer regenerated"
    )


@nox.session(name="pep484")
def test_pep484(session: nox.Session) -> None:
    """Run mypy type checking."""

    session.install(*nox.project.dependency_groups(pyproject, "mypy"))

    session.install("-e", ".")

    session.run(
        "mypy",
        "noxfile.py",
        "./lib/sqlalchemy",
    )


@nox.session(name="mypy", python="3.14")
def test_mypy(session: nox.Session) -> None:
    """run the typing integration test suite"""

    session.install(*nox.project.dependency_groups(pyproject, "mypy"))

    session.install("-e", ".")

    posargs = apply_pytest_opts(
        session,
        "sqlalchemy",
        ["mypy"],
    )

    cmd = ["pytest", "-m", "mypy"]

    session.run(*cmd, *posargs)


@nox.session(name="pep8", python="3.14")
def test_pep8(session: nox.Session) -> None:
    """Run linting and formatting checks."""

    for pattern in ["*.so", "*.pyd", "*.dylib"]:
        for filepath in Path("lib/sqlalchemy").rglob(pattern):
            filepath.unlink()

    session.install("-e", ".")

    session.install(*nox.project.dependency_groups(pyproject, "lint"))

    failed = []

    for cmd in [
        "flake8p ./lib/ ./test/ ./examples/ noxfile.py "
        "setup.py doc/build/conf.py",
        # run "unused argument" lints on asyncio, as we have a lot of
        # proxy methods here
        "flake8p  --ignore='' --select='U100,U101' "
        "./lib/sqlalchemy/ext/asyncio "
        "./lib/sqlalchemy/orm/scoping.py",
        "black --check ./lib/ ./test/ ./examples/ setup.py doc/build/conf.py",
        "slotscheck -m sqlalchemy",
        "python ./tools/format_docs_code.py --check",
        "python ./tools/generate_tuple_map_overloads.py --check",
        "python ./tools/generate_proxy_methods.py --check",
        "python ./tools/sync_test_files.py --check",
        "python ./tools/generate_sql_functions.py --check",
        "python ./tools/normalize_file_headers.py --check",
        "python ./tools/cython_imports.py --check",
        "python ./tools/walk_packages.py",
    ]:
        try:
            session.run(*cmd.split())
        except CommandFailed as err:
            failed.append((cmd, err))

    if failed:
        session.error(f"failed with {len(failed)} errors")
