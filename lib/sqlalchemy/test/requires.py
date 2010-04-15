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
     fails_on,\
     fails_on_everything_except

import testing
import sys

def deferrable_constraints(fn):
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
        no_support('sqlite', 'no driver support'),
        exclude('mssql', '<', (9, 0, 0),
                'SQL Server 2005+ is required for independent connections'),
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
    
def savepoints(fn):
    """Target database must support savepoints."""
    return _chain_decorators_on(
        fn,
        emits_warning_on('mssql', 'Savepoint support in mssql is experimental and may lead to data loss.'),
        no_support('access', 'not supported by database'),
        no_support('sqlite', 'not supported by database'),
        no_support('sybase', 'FIXME: guessing, needs confirmation'),
        exclude('mysql', '<', (5, 0, 3), 'not supported by database'),
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
        no_support('mssql', 'no SEQUENCE support'),
        no_support('mysql', 'no SEQUENCE support'),
        no_support('sqlite', 'no SEQUENCE support'),
        no_support('sybase', 'no SEQUENCE support'),
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
    )

def except_(fn):
    """Target database must support EXCEPT or equivlaent (i.e. MINUS)."""
    return _chain_decorators_on(
        fn,
        fails_on('firebird', 'no support for EXCEPT'),
        fails_on('mysql', 'no support for EXCEPT'),
        fails_on('sybase', 'no support for EXCEPT'),
    )

def offset(fn):
    """Target database must support some method of adding OFFSET or equivalent to a result set."""
    return _chain_decorators_on(
        fn,
        fails_on('sybase', 'no support for OFFSET or equivalent'),
    )
    
def returning(fn):
    return _chain_decorators_on(
        fn,
        no_support('access', 'not supported by database'),
        no_support('sqlite', 'not supported by database'),
        no_support('mysql', 'not supported by database'),
        no_support('maxdb', 'not supported by database'),
        no_support('sybase', 'not supported by database'),
        no_support('informix', 'not supported by database'),
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
        no_support('sqlite', 'not supported by database'),
        no_support('sybase', 'FIXME: guessing, needs confirmation'),
        no_support('postgresql+zxjdbc', 'FIXME: JDBC driver confuses the transaction state, may '
                   'need separate XA implementation'),
        exclude('mysql', '<', (5, 0, 3), 'not supported by database'),
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

def reflects_pk_names(fn):
    """Target driver reflects the name of primary key constraints."""
    return _chain_decorators_on(
        fn,
        fails_on_everything_except('postgresql')
    )
    
def python2(fn):
    return _chain_decorators_on(
        fn,
        skip_if(
            lambda: sys.version_info >= (3,),
            "Python version 2.xx is required."
            )
    )
    
def _has_sqlite():
    from sqlalchemy import create_engine
    try:
        e = create_engine('sqlite://')
        return True
    except ImportError:
        return False

def sqlite(fn):
    return _chain_decorators_on(
        fn,
        skip_if(lambda: not _has_sqlite())
    )

