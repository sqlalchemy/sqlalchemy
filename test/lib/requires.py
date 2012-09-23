"""Global database feature support policy.

Provides decorators to mark tests requiring specific feature support from the
target database.

"""

from exclusions import \
     skip, \
     skip_if,\
     only_if,\
     only_on,\
     fails_on,\
     fails_on_everything_except,\
     fails_if,\
     SpecPredicate

def no_support(db, reason):
    return SpecPredicate(db, description=reason)

def exclude(db, op, spec, description=None):
    return SpecPredicate(db, op, spec, description=description)

from sqlalchemy import util
from test.lib import config
import testing
import sys

crashes = skip


def _chain_decorators_on(fn, *decorators):
    for decorator in reversed(decorators):
        fn = decorator(fn)
    return fn

def deferrable_or_no_constraints(fn):
    """Target database must support derferable constraints."""

    return skip_if([
        no_support('firebird', 'not supported by database'),
        no_support('mysql', 'not supported by database'),
        no_support('mssql', 'not supported by database'),
        ])(fn)

def foreign_keys(fn):
    """Target database must support foreign keys."""

    return skip_if(
            no_support('sqlite', 'not supported by database')
        )(fn)


def unbounded_varchar(fn):
    """Target database must support VARCHAR with no length"""

    return skip_if([
            "firebird", "oracle", "mysql"
        ], "not supported by database"
        )(fn)

def boolean_col_expressions(fn):
    """Target database must support boolean expressions as columns"""
    return skip_if([
        no_support('firebird', 'not supported by database'),
        no_support('oracle', 'not supported by database'),
        no_support('mssql', 'not supported by database'),
        no_support('sybase', 'not supported by database'),
        no_support('maxdb', 'FIXME: verify not supported by database'),
        no_support('informix', 'not supported by database'),
    ])(fn)

def standalone_binds(fn):
    """target database/driver supports bound parameters as column expressions
    without being in the context of a typed column.

    """
    return skip_if(["firebird", "mssql+mxodbc"],
            "not supported by driver")(fn)

def identity(fn):
    """Target database must support GENERATED AS IDENTITY or a facsimile.

    Includes GENERATED AS IDENTITY, AUTOINCREMENT, AUTO_INCREMENT, or other
    column DDL feature that fills in a DB-generated identifier at INSERT-time
    without requiring pre-execution of a SEQUENCE or other artifact.

    """
    return skip_if(["firebird", "oracle", "postgresql", "sybase"],
            "not supported by database"
        )(fn)

def reflectable_autoincrement(fn):
    """Target database must support tables that can automatically generate
    PKs assuming they were reflected.

    this is essentially all the DBs in "identity" plus Postgresql, which
    has SERIAL support.  FB and Oracle (and sybase?) require the Sequence to
    be explicitly added, including if the table was reflected.
    """
    return skip_if(["firebird", "oracle", "sybase"],
            "not supported by database"
        )(fn)

def binary_comparisons(fn):
    """target database/driver can allow BLOB/BINARY fields to be compared
    against a bound parameter value.
    """
    return skip_if(["oracle", "mssql"],
            "not supported by database/driver"
        )(fn)

def independent_cursors(fn):
    """Target must support simultaneous, independent database cursors
    on a single connection."""

    return skip_if(["mssql+pyodbc", "mssql+mxodbc"], "no driver support")

def independent_connections(fn):
    """Target must support simultaneous, independent database connections."""

    # This is also true of some configurations of UnixODBC and probably win32
    # ODBC as well.
    return skip_if([
                no_support("sqlite",
                        "independent connections disabled "
                            "when :memory: connections are used"),
                exclude("mssql", "<", (9, 0, 0),
                        "SQL Server 2005+ is required for "
                            "independent connections"
                    )
                ]
            )(fn)

def updateable_autoincrement_pks(fn):
    """Target must support UPDATE on autoincrement/integer primary key."""

    return skip_if(["mssql", "sybase"],
            "IDENTITY columns can't be updated")(fn)

def isolation_level(fn):
    return _chain_decorators_on(
        fn,
        only_on(('postgresql', 'sqlite', 'mysql'),
                    "DBAPI has no isolation level support"),
        fails_on('postgresql+pypostgresql',
                      'pypostgresql bombs on multiple isolation level calls')
    )

def row_triggers(fn):
    """Target must support standard statement-running EACH ROW triggers."""

    return skip_if([
        # no access to same table
        no_support('mysql', 'requires SUPER priv'),
        exclude('mysql', '<', (5, 0, 10), 'not supported by database'),

        # huh?  TODO: implement triggers for PG tests, remove this
        no_support('postgresql',
                'PG triggers need to be implemented for tests'),
    ])(fn)

def correlated_outer_joins(fn):
    """Target must support an outer join to a subquery which
    correlates to the parent."""

    return skip_if("oracle", 'Raises "ORA-01799: a column may not be '
                'outer-joined to a subquery"')(fn)

def update_from(fn):
    """Target must support UPDATE..FROM syntax"""

    return only_on(['postgresql', 'mssql', 'mysql'],
            "Backend does not support UPDATE..FROM")(fn)


def savepoints(fn):
    """Target database must support savepoints."""

    return skip_if([
                "access",
                "sqlite",
                "sybase",
                ("mysql", "<", (5, 0, 3)),
                ("informix", "<", (11, 55, "xC3"))
                ], "savepoints not supported")(fn)

def denormalized_names(fn):
    """Target database must have 'denormalized', i.e.
    UPPERCASE as case insensitive names."""

    return skip_if(
                lambda: not testing.db.dialect.requires_name_normalize,
                "Backend does not require denormalized names."
            )(fn)

def schemas(fn):
    """Target database must support external schemas, and have one
    named 'test_schema'."""

    return skip_if([
                "sqlte",
                "firebird"
            ], "no schema support")

def sequences(fn):
    """Target database must support SEQUENCEs."""

    return only_if([
            "postgresql", "firebird", "oracle"
        ], "no SEQUENCE support")(fn)

def update_nowait(fn):
    """Target database must support SELECT...FOR UPDATE NOWAIT"""
    return skip_if(["access", "firebird", "mssql", "mysql", "sqlite", "sybase"],
            "no FOR UPDATE NOWAIT support"
        )(fn)

def subqueries(fn):
    """Target database must support subqueries."""

    return skip_if(exclude('mysql', '<', (4, 1, 1)), 'no subquery support')(fn)

def intersect(fn):
    """Target database must support INTERSECT or equivalent."""

    return fails_if([
            "firebird", "mysql", "sybase", "informix"
        ], 'no support for INTERSECT')(fn)

def except_(fn):
    """Target database must support EXCEPT or equivalent (i.e. MINUS)."""
    return fails_if([
            "firebird", "mysql", "sybase", "informix"
        ], 'no support for EXCEPT')(fn)

def offset(fn):
    """Target database must support some method of adding OFFSET or
    equivalent to a result set."""
    return fails_if([
            "sybase"
        ], 'no support for OFFSET or equivalent')(fn)

def window_functions(fn):
    return only_if([
                "postgresql", "mssql", "oracle"
            ], "Backend does not support window functions")(fn)

def returning(fn):
    return only_if(["postgresql", "mssql", "oracle", "firebird"],
            "'returning' not supported by database"
        )(fn)

def two_phase_transactions(fn):
    """Target database must support two-phase transactions."""

    return skip_if([
        no_support('access', 'two-phase xact not supported by database'),
        no_support('firebird', 'no SA implementation'),
        no_support('maxdb', 'two-phase xact not supported by database'),
        no_support('mssql', 'two-phase xact not supported by drivers'),
        no_support('oracle', 'two-phase xact not implemented in SQLA/oracle'),
        no_support('drizzle', 'two-phase xact not supported by database'),
        no_support('sqlite', 'two-phase xact not supported by database'),
        no_support('sybase', 'two-phase xact not supported by drivers/SQLA'),
        no_support('postgresql+zxjdbc',
                'FIXME: JDBC driver confuses the transaction state, may '
                   'need separate XA implementation'),
        exclude('mysql', '<', (5, 0, 3),
                    'two-phase xact not supported by database'),
        ])(fn)

def views(fn):
    """Target database must support VIEWs."""

    return skip_if("drizzle", "no VIEW support")(fn)

def unicode_connections(fn):
    """Target driver must support some encoding of Unicode across the wire."""
    # TODO: expand to exclude MySQLdb versions w/ broken unicode
    return skip_if([
        exclude('mysql', '<', (4, 1, 1), 'no unicode connection support'),
        ])(fn)

def unicode_ddl(fn):
    """Target driver must support some encoding of Unicode across the wire."""
    # TODO: expand to exclude MySQLdb versions w/ broken unicode
    return skip_if([
        no_support('maxdb', 'database support flakey'),
        no_support('oracle', 'FIXME: no support in database?'),
        no_support('sybase', 'FIXME: guessing, needs confirmation'),
        no_support('mssql+pymssql', 'no FreeTDS support'),
        exclude('mysql', '<', (4, 1, 1), 'no unicode connection support'),
        ])(fn)

def sane_rowcount(fn):
    return skip_if(
        lambda: not testing.db.dialect.supports_sane_rowcount,
        "driver doesn't support 'sane' rowcount"
    )(fn)

def cextensions(fn):
    return skip_if(
            lambda: not _has_cextensions(), "C extensions not installed"
            )(fn)


def emulated_lastrowid(fn):
    """"target dialect retrieves cursor.lastrowid or an equivalent
    after an insert() construct executes.
    """
    return fails_on_everything_except('mysql+mysqldb', 'mysql+oursql',
                                   'sqlite+pysqlite', 'mysql+pymysql',
                                   'mssql+pyodbc', 'mssql+mxodbc')(fn)

def dbapi_lastrowid(fn):
    """"target backend includes a 'lastrowid' accessor on the DBAPI
    cursor object.

    """
    return fails_on_everything_except('mysql+mysqldb', 'mysql+oursql',
                                   'sqlite+pysqlite', 'mysql+pymysql')(fn)

def sane_multi_rowcount(fn):
    return skip_if(
                lambda: not testing.db.dialect.supports_sane_multi_rowcount,
                "driver doesn't support 'sane' multi row count"
            )

def nullsordering(fn):
    """Target backends that support nulls ordering."""
    return _chain_decorators_on(
        fn,
        fails_on_everything_except('postgresql', 'oracle', 'firebird')
    )

def reflects_pk_names(fn):
    """Target driver reflects the name of primary key constraints."""
    return _chain_decorators_on(
        fn,
        fails_on_everything_except('postgresql', 'oracle')
    )

def python2(fn):
    return _chain_decorators_on(
        fn,
        skip_if(
            lambda: sys.version_info >= (3,),
            "Python version 2.xx is required."
            )
    )

def python3(fn):
    return _chain_decorators_on(
        fn,
        skip_if(
            lambda: sys.version_info < (3,),
            "Python version 3.xx is required."
            )
    )

def python26(fn):
    return _chain_decorators_on(
        fn,
        skip_if(
            lambda: sys.version_info < (2, 6),
            "Python version 2.6 or greater is required"
        )
    )

def python25(fn):
    return _chain_decorators_on(
        fn,
        skip_if(
            lambda: sys.version_info < (2, 5),
            "Python version 2.5 or greater is required"
        )
    )

def cpython(fn):
    return _chain_decorators_on(
         fn,
         only_if(lambda: util.cpython,
           "cPython interpreter needed"
         )
    )

def predictable_gc(fn):
    """target platform must remove all cycles unconditionally when
    gc.collect() is called, as well as clean out unreferenced subclasses.

    """
    return cpython(fn)

def sqlite(fn):
    return _chain_decorators_on(
        fn,
        skip_if(lambda: not _has_sqlite())
    )

def ad_hoc_engines(fn):
    """Test environment must allow ad-hoc engine/connection creation.

    DBs that scale poorly for many connections, even when closed, i.e.
    Oracle, may use the "--low-connections" option which flags this requirement
    as not present.

    """
    return _chain_decorators_on(
        fn,
        skip_if(lambda: config.options.low_connections)
    )

def skip_mysql_on_windows(fn):
    """Catchall for a large variety of MySQL on Windows failures"""

    return _chain_decorators_on(
        fn,
        skip_if(_has_mysql_on_windows,
            "Not supported on MySQL + Windows"
        )
    )

def english_locale_on_postgresql(fn):
    return _chain_decorators_on(
        fn,
        skip_if(lambda: testing.against('postgresql') \
                and not testing.db.scalar('SHOW LC_COLLATE').startswith('en'))
    )

def selectone(fn):
    """target driver must support the literal statement 'select 1'"""
    return _chain_decorators_on(
        fn,
        skip_if(lambda: testing.against('oracle'),
            "non-standard SELECT scalar syntax")
    )

def _has_cextensions():
    try:
        from sqlalchemy import cresultproxy, cprocessors
        return True
    except ImportError:
        return False

def _has_sqlite():
    from sqlalchemy import create_engine
    try:
        e = create_engine('sqlite://')
        return True
    except ImportError:
        return False

def _has_mysql_on_windows():
    return testing.against('mysql') and \
            testing.db.dialect._detect_casing(testing.db) == 1

def _has_mysql_fully_case_sensitive():
    return testing.against('mysql') and \
            testing.db.dialect._detect_casing(testing.db) == 0

