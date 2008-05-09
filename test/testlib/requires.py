"""Global database feature support policy.

Provides decorators to mark tests requiring specific feature support from the
target database.

"""
from testlib import testing

def savepoints(fn):
    """Target database must support savepoints."""
    return (testing.unsupported(
            'access',
            'mssql',
            'sqlite',
            'sybase',
            )
            (testing.exclude('mysql', '<', (5, 0, 3))
             (fn)))

def two_phase_transactions(fn):
    """Target database must support two-phase transactions."""
    return (testing.unsupported(
            'access',
            'firebird',
            'maxdb',
            'mssql',
            'oracle',
            'sqlite',
            'sybase',
            )
            (testing.exclude('mysql', '<', (5, 0, 3))
             (fn)))
