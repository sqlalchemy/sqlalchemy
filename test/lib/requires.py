"""Global database feature support policy.

Provides decorators to mark tests requiring specific feature support from the
target database.

"""

from testing import \
     _block_unconditionally as no_support, \
     _chain_decorators_on, \
     exclude, \
     emits_warning_on,\
     skip_if,\
     only_on,\
     fails_on,\
     fails_on_everything_except,\
     fails_if
from sqlalchemy import util
from test.lib import config
import testing
import sys

def deferrable_or_no_constraints(fn):
    """Target database must support derferable constraints."""
    return _chain_decorators_on(
        fn,
        no_support('firebird', 'not supported by database'),
        no_support('mysql', 'not supported by database'),
        no_support('mssql', 'not supported by database'),
        )

def foreign_keys(fn):
    """Target database must support foreign keys."""
    return _chain_decorators_on(
        fn,
        no_support('sqlite', 'not supported by database'),
        )


def unbounded_varchar(fn):
    """Target database must support VARCHAR with no length"""
    return _chain_decorators_on(
        fn,
        no_support('firebird', 'not supported by database'),
        no_support('oracle', 'not supported by database'),
        no_support('mysql', 'not supported by database'),
    )

def boolean_col_expressions(fn):
    """Target database must support boolean expressions as columns"""
    return _chain_decorators_on(
        fn,
        no_support('firebird', 'not supported by database'),
        no_support('oracle', 'not supported by database'),
        no_support('mssql', 'not supported by database'),
        no_support('sybase', 'not supported by database'),
        no_support('maxdb', 'FIXME: verify not supported by database'),
        no_support('informix', 'not supported by database'),
    )

def identity(fn):
    """Target database must support GENERATED AS IDENTITY or a facsimile.

    Includes GENERATED AS IDENTITY, AUTOINCREMENT, AUTO_INCREMENT, or other
    column DDL feature that fills in a DB-generated identifier at INSERT-time
    without requiring pre-execution of a SEQUENCE or other artifact.

    """
    return _chain_decorators_on(
        fn,
        no_support('firebird', 'not supported by database'),
        no_support('oracle', 'not supported by database'),
        no_support('postgresql', 'not supported by database'),
        no_support('sybase', 'not supported by database'),
        )

def independent_cursors(fn):
    """Target must support simultaneous, independent database cursors on a single connection."""

    return _chain_decorators_on(
        fn,
        no_support('mssql+pyodbc', 'no driver support'),
        no_support('mssql+mxodbc', 'no driver support'),
        )

def independent_connections(fn):
    """Target must support simultaneous, independent database connections."""

    # This is also true of some configurations of UnixODBC and probably win32
    # ODBC as well.
    return _chain_decorators_on(
        fn,
        no_support('sqlite', 'Independent connections disabled when '
                            ':memory: connections are used'),
        exclude('mssql', '<', (9, 0, 0),
                'SQL Server 2005+ is required for independent connections'),
        )

def updateable_autoincrement_pks(fn):
    """Target must support UPDATE on autoincrement/integer primary key."""
    return _chain_decorators_on(
        fn,
        no_support('mssql', "IDENTITY cols can't be updated"),
        no_support('sybase', "IDENTITY cols can't be updated"),
    )

def isolation_level(fn):
    return _chain_decorators_on(
        fn,
        only_on(('postgresql', 'sqlite'), "DBAPI has no isolation level support"),
        fails_on('postgresql+pypostgresql',
                      'pypostgresql bombs on multiple isolation level calls')
    )

def row_triggers(fn):
    """Target must support standard statement-running EACH ROW triggers."""
    return _chain_decorators_on(
        fn,
        # no access to same table
        no_support('mysql', 'requires SUPER priv'),
        exclude('mysql', '<', (5, 0, 10), 'not supported by database'),

        # huh?  TODO: implement triggers for PG tests, remove this
        no_support('postgresql', 'PG triggers need to be implemented for tests'),
        )

def correlated_outer_joins(fn):
    """Target must support an outer join to a subquery which correlates to the parent."""

    return _chain_decorators_on(
        fn,
        no_support('oracle', 'Raises "ORA-01799: a column may not be outer-joined to a subquery"')
    )

def update_from(fn):
    """Target must support UPDATE..FROM syntax"""
    return _chain_decorators_on(
        fn,
        only_on(('postgresql', 'mssql', 'mysql'), 
            "Backend does not support UPDATE..FROM")
    )

def savepoints(fn):
    """Target database must support savepoints."""
    return _chain_decorators_on(
        fn,
        no_support('access', 'savepoints not supported'),
        no_support('sqlite', 'savepoints not supported'),
        no_support('sybase', 'savepoints not supported'),
        exclude('mysql', '<', (5, 0, 3), 'savepoints not supported'),
        exclude('informix', '<', (11, 55, 'xC3'), 'savepoints not supported'),
        )

def denormalized_names(fn):
    """Target database must have 'denormalized', i.e. UPPERCASE as case insensitive names."""

    return skip_if(
                lambda: not testing.db.dialect.requires_name_normalize,
                "Backend does not require denomralized names."
            )(fn)

def schemas(fn):
    """Target database must support external schemas, and have one named 'test_schema'."""

    return _chain_decorators_on(
        fn,
        no_support('sqlite', 'no schema support'),
        no_support('firebird', 'no schema support')
    )

def sequences(fn):
    """Target database must support SEQUENCEs."""
    return _chain_decorators_on(
        fn,
        no_support('access', 'no SEQUENCE support'),
        no_support('drizzle', 'no SEQUENCE support'),
        no_support('mssql', 'no SEQUENCE support'),
        no_support('mysql', 'no SEQUENCE support'),
        no_support('sqlite', 'no SEQUENCE support'),
        no_support('sybase', 'no SEQUENCE support'),
        no_support('informix', 'no SEQUENCE support'),
        )

def update_nowait(fn):
    """Target database must support SELECT...FOR UPDATE NOWAIT"""
    return _chain_decorators_on(
        fn,
        no_support('access', 'no FOR UPDATE NOWAIT support'),
        no_support('firebird', 'no FOR UPDATE NOWAIT support'),
        no_support('mssql', 'no FOR UPDATE NOWAIT support'),
        no_support('mysql', 'no FOR UPDATE NOWAIT support'),
        no_support('sqlite', 'no FOR UPDATE NOWAIT support'),
        no_support('sybase', 'no FOR UPDATE NOWAIT support'),
    )

def subqueries(fn):
    """Target database must support subqueries."""
    return _chain_decorators_on(
        fn,
        exclude('mysql', '<', (4, 1, 1), 'no subquery support'),
        )

def intersect(fn):
    """Target database must support INTERSECT or equivlaent."""
    return _chain_decorators_on(
        fn,
        fails_on('firebird', 'no support for INTERSECT'),
        fails_on('mysql', 'no support for INTERSECT'),
        fails_on('sybase', 'no support for INTERSECT'),
        fails_on('informix', 'no support for INTERSECT'),
    )

def except_(fn):
    """Target database must support EXCEPT or equivlaent (i.e. MINUS)."""
    return _chain_decorators_on(
        fn,
        fails_on('firebird', 'no support for EXCEPT'),
        fails_on('mysql', 'no support for EXCEPT'),
        fails_on('sybase', 'no support for EXCEPT'),
        fails_on('informix', 'no support for EXCEPT'),
    )

def offset(fn):
    """Target database must support some method of adding OFFSET or equivalent to a result set."""
    return _chain_decorators_on(
        fn,
        fails_on('sybase', 'no support for OFFSET or equivalent'),
    )

def window_functions(fn):
    return _chain_decorators_on(
        fn,
        only_on(('postgresql', 'mssql', 'oracle'),
                "Backend does not support window functions"),
    )

def returning(fn):
    return _chain_decorators_on(
        fn,
        no_support('access', "'returning' not supported by database"),
        no_support('sqlite', "'returning' not supported by database"),
        no_support('mysql', "'returning' not supported by database"),
        no_support('maxdb', "'returning' not supported by database"),
        no_support('sybase', "'returning' not supported by database"),
        no_support('informix', "'returning' not supported by database"),
    )

def two_phase_transactions(fn):
    """Target database must support two-phase transactions."""
    return _chain_decorators_on(
        fn,
        no_support('access', 'not supported by database'),
        no_support('firebird', 'no SA implementation'),
        no_support('maxdb', 'not supported by database'),
        no_support('mssql', 'FIXME: guessing, needs confirmation'),
        no_support('oracle', 'no SA implementation'),
        no_support('drizzle', 'not supported by database'),
        no_support('sqlite', 'not supported by database'),
        no_support('sybase', 'FIXME: guessing, needs confirmation'),
        no_support('postgresql+zxjdbc', 'FIXME: JDBC driver confuses the transaction state, may '
                   'need separate XA implementation'),
        exclude('mysql', '<', (5, 0, 3), 'not supported by database'),
        )

def views(fn):
    """Target database must support VIEWs."""
    return _chain_decorators_on(
        fn,
        no_support('drizzle', 'no VIEW support'),
        )

def unicode_connections(fn):
    """Target driver must support some encoding of Unicode across the wire."""
    # TODO: expand to exclude MySQLdb versions w/ broken unicode
    return _chain_decorators_on(
        fn,
        exclude('mysql', '<', (4, 1, 1), 'no unicode connection support'),
        )

def unicode_ddl(fn):
    """Target driver must support some encoding of Unicode across the wire."""
    # TODO: expand to exclude MySQLdb versions w/ broken unicode
    return _chain_decorators_on(
        fn,
        no_support('maxdb', 'database support flakey'),
        no_support('oracle', 'FIXME: no support in database?'),
        no_support('sybase', 'FIXME: guessing, needs confirmation'),
        no_support('mssql+pymssql', 'no FreeTDS support'),
        exclude('mysql', '<', (4, 1, 1), 'no unicode connection support'),
        )

def sane_rowcount(fn):
    return _chain_decorators_on(
        fn,
        skip_if(lambda: not testing.db.dialect.supports_sane_rowcount)
    )

def cextensions(fn):
    return _chain_decorators_on(
        fn,
        skip_if(lambda: not _has_cextensions(), "C extensions not installed")
    )

def dbapi_lastrowid(fn):
    if util.pypy:
        return _chain_decorators_on(
            fn,
            fails_if(lambda:True)
        )
    else:
        return _chain_decorators_on(
            fn,
            fails_on_everything_except('mysql+mysqldb', 'mysql+oursql',
                                       'sqlite+pysqlite', 'mysql+pymysql'),
        )

def sane_multi_rowcount(fn):
    return _chain_decorators_on(
        fn,
        skip_if(lambda: not testing.db.dialect.supports_sane_multi_rowcount)
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
         skip_if(lambda: util.jython or util.pypy, 
           "cPython interpreter needed"
         )
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
            testing.db.dialect._server_casing == 1

def _has_mysql_fully_case_sensitive():
    return testing.against('mysql') and \
            testing.db.dialect._server_casing == 0

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
