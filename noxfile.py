"""Nox configuration for SQLAlchemy."""

from __future__ import annotations

import os
import sys
from typing import Dict
from typing import List
from typing import Set

import nox

if True:
    sys.path.insert(0, ".")
    from tools.toxnox import extract_opts
    from tools.toxnox import tox_parameters


PYTHON_VERSIONS = ["3.10", "3.11", "3.12", "3.13", "3.13t", "3.14", "3.14t"]
DATABASES = ["sqlite", "sqlite_file", "postgresql", "mysql", "oracle", "mssql"]
CEXT = ["_auto", "cext", "nocext"]
GREENLET = ["_greenlet", "nogreenlet"]
BACKENDONLY = ["_all", "backendonly", "memusage"]

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
        "nogreenlet": {"cx_oracle", "oracledb"},
        "greenlet": {"oracledb_async"},
    },
    "mssql": {"nogreenlet": {"pyodbc", "pymssql"}, "greenlet": {"aioodbc"}},
}


def _setup_for_driver(
    session: nox.Session,
    cmd: List[str],
    basename: str,
    greenlet: bool = False,
) -> None:

    # install driver deps listed out in pyproject.toml
    nogreenlet_deps = f"tests-{basename.replace("_", "-")}"
    greenlet_deps = f"tests-{basename.replace("_", "-")}-asyncio"

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
    # e.g. --db postgresql, --db mysql, etc.
    default_dburl = f"--db {basename}"
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

    for dbdriver in dbdrivers:
        cmd.extend(["--dbdriver", dbdriver])


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

    _tests(session, "sqlite", "cext", greenlet=False)


def _tests(
    session: nox.Session,
    database: str,
    cext: str = "_auto",
    greenlet: bool = True,
    backendonly: bool = False,
    platform_intensive: bool = False,
    timing_intensive: bool = True,
    coverage: bool = False,
    mypy: bool = False,
) -> None:
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
        # by pytest
        includes_excludes["m"].append("backend")
    else:
        includes_excludes["m"].append("not memory_intensive")

        # the mypy suite is also run exclusively from the test_mypy
        # session
        includes_excludes["m"].append("not mypy")

        if not timing_intensive:
            includes_excludes["m"].append("not timing_intensive")

    cmd = ["python", "-m", "pytest"]

    if coverage:
        assert not platform_intensive
        cmd.extend(
            [
                "--cov=sqlalchemy",
                "--cov-append",
                "--cov-report",
                "term",
                "--cov-report",
                "xml",
            ],
        )
        includes_excludes["k"].append("not aaa_profiling")

    cmd.extend(os.environ.get("TOX_WORKERS", "-n4").split())

    if coverage:
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

    posargs, opts = extract_opts(session.posargs, "generate-junit", "dry-run")

    if opts.generate_junit:
        # produce individual junit files that are per-database
        junitfile = f"junit-{database}.xml"
        cmd.extend(["--junitxml", junitfile])

    cmd.extend(posargs)

    if opts.dry_run:
        print(f"DRY RUN: command is: \n{' '.join(cmd)}")
        return

    try:
        session.run(*cmd)
    finally:
        # Run cleanup for oracle/mssql
        if database in ["oracle", "mssql"]:
            session.run("python", "reap_dbs.py", "db_idents.txt")


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


@nox.session(name="mypy")
def test_mypy(session: nox.Session) -> None:
    """run the typing integration test suite"""

    session.install(*nox.project.dependency_groups(pyproject, "mypy"))

    session.install("-e", ".")

    posargs, opts = extract_opts(session.posargs, "generate-junit")

    cmd = ["pytest", "-m", "mypy"]
    if opts.generate_junit:
        # produce individual junit files that are per-database
        junitfile = "junit-mypy.xml"
        cmd.extend(["--junitxml", junitfile])

    session.run(*cmd, *posargs)


@nox.session(name="pep8")
def test_pep8(session: nox.Session) -> None:
    """Run linting and formatting checks."""

    session.install("-e", ".")

    session.install(*nox.project.dependency_groups(pyproject, "lint"))

    for cmd in [
        "flake8 ./lib/ ./test/ ./examples/ noxfile.py "
        "setup.py doc/build/conf.py",
        "flake8  --extend-ignore='' ./lib/sqlalchemy/ext/asyncio "
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

        session.run(*cmd.split())
