"""Global database feature support policy.

Provides decorators to mark tests requiring specific feature support from the
target database.

"""

from testing import \
     _block_unconditionally as no_support, \
     _chain_decorators_on, \
     exclude, \
     emits_warning_on


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
        no_support('postgres', 'not supported by database'),
        no_support('sybase', 'not supported by database'),
        )

def independent_connections(fn):
    """Target must support simultaneous, independent database connections."""

    # This is also true of some configurations of UnixODBC and probably win32
    # ODBC as well.
    return _chain_decorators_on(
        fn,
        no_support('sqlite', 'no driver support')
        )

def row_triggers(fn):
    """Target must support standard statement-running EACH ROW triggers."""
    return _chain_decorators_on(
        fn,
        # no access to same table
        no_support('mysql', 'requires SUPER priv'),
        exclude('mysql', '<', (5, 0, 10), 'not supported by database'),
        no_support('postgres', 'not supported by database: no statements'),
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

def subqueries(fn):
    """Target database must support subqueries."""
    return _chain_decorators_on(
        fn,
        exclude('mysql', '<', (4, 1, 1), 'no subquery support'),
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
        exclude('mysql', '<', (4, 1, 1), 'no unicode connection support'),
        )
