"""Global database feature support policy.

Provides decorators to mark tests requiring specific feature support from the
target database.

"""

from testlib.testing import \
     _chain_decorators_on, \
     exclude, \
     unsupported


def sequences(fn):
    """Target database must support SEQUENCEs."""
    return _chain_decorators_on(
        fn,
        unsupported('access', 'no SEQUENCE support'),
        unsupported('mssql', 'no SEQUENCE support'),
        unsupported('mysql', 'no SEQUENCE support'),
        unsupported('sqlite', 'no SEQUENCE support'),
        unsupported('sybase', 'no SEQUENCE support'),
        )

def savepoints(fn):
    """Target database must support savepoints."""
    return _chain_decorators_on(
        fn,
        unsupported('access', 'FIXME: guessing, needs confirmation'),
        unsupported('mssql', 'FIXME: guessing, needs confirmation'),
        unsupported('sqlite', 'not supported by database'),
        unsupported('sybase', 'FIXME: guessing, needs confirmation'),
        exclude('mysql', '<', (5, 0, 3), 'not supported by database'),
        )

def two_phase_transactions(fn):
    """Target database must support two-phase transactions."""
    return _chain_decorators_on(
        fn,
        unsupported('access', 'FIXME: guessing, needs confirmation'),
        unsupported('firebird', 'no SA implementation'),
        unsupported('maxdb', 'not supported by database'),
        unsupported('mssql', 'FIXME: guessing, needs confirmation'),
        unsupported('oracle', 'no SA implementation'),
        unsupported('sqlite', 'not supported by database'),
        unsupported('sybase', 'FIXME: guessing, needs confirmation'),
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
        unsupported('maxdb', 'database support flakey'),
        unsupported('oracle', 'FIXME: no support in database?'),
        unsupported('sybase', 'FIXME: guessing, needs confirmation'),
        exclude('mysql', '<', (4, 1, 1), 'no unicode connection support'),
        )

def subqueries(fn):
    """Target database must support subqueries."""
    return _chain_decorators_on(
        fn,
        exclude('mysql', '<', (4, 1, 1), 'no subquery support'),
        )

def foreign_keys(fn):
    """Target database must support foreign keys."""
    return _chain_decorators_on(
        fn,
        unsupported('sqlite', 'not supported by database'),
        )

def deferrable_constraints(fn):
    """Target database must support derferable constraints."""
    return _chain_decorators_on(
        fn,
        unsupported('mysql', 'not supported by database'),
        unsupported('mssql', 'not supported by database'),
        )
