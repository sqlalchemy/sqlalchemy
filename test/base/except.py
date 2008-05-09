"""Tests exceptions and DB-API exception wrapping."""
import testenv; testenv.configure_for_tests()
import sys, unittest
import exceptions as stdlib_exceptions
from sqlalchemy import exceptions as sa_exceptions
from testlib import *


class Error(stdlib_exceptions.StandardError):
    """This class will be old-style on <= 2.4 and new-style on >= 2.5."""
class DatabaseError(Error):
    pass
class OperationalError(DatabaseError):
    pass
class ProgrammingError(DatabaseError):
    def __str__(self):
        return "<%s>" % self.bogus
class OutOfSpec(DatabaseError):
    pass


class WrapTest(unittest.TestCase):
    def test_db_error_normal(self):
        try:
            raise sa_exceptions.DBAPIError.instance(
                '', [], OperationalError())
        except sa_exceptions.DBAPIError:
            self.assert_(True)

    def test_db_error_busted_dbapi(self):
        try:
            raise sa_exceptions.DBAPIError.instance(
                '', [], ProgrammingError())
        except sa_exceptions.DBAPIError, e:
            self.assert_(True)
            self.assert_('Error in str() of DB-API' in e.args[0])

    def test_db_error_noncompliant_dbapi(self):
        try:
            raise sa_exceptions.DBAPIError.instance(
                '', [], OutOfSpec())
        except sa_exceptions.DBAPIError, e:
            self.assert_(e.__class__ is sa_exceptions.DBAPIError)
        except OutOfSpec:
            self.assert_(False)

        # Make sure the DatabaseError recognition logic is limited to
        # subclasses of sqlalchemy.exceptions.DBAPIError
        try:
            raise sa_exceptions.DBAPIError.instance(
                '', [], sa_exceptions.AssertionError())
        except sa_exceptions.DBAPIError, e:
            self.assert_(e.__class__ is sa_exceptions.DBAPIError)
        except sa_exceptions.AssertionError:
            self.assert_(False)

    def test_db_error_keyboard_interrupt(self):
        try:
            raise sa_exceptions.DBAPIError.instance(
                '', [], stdlib_exceptions.KeyboardInterrupt())
        except sa_exceptions.DBAPIError:
            self.assert_(False)
        except stdlib_exceptions.KeyboardInterrupt:
            self.assert_(True)

    def test_db_error_system_exit(self):
        try:
            raise sa_exceptions.DBAPIError.instance(
                '', [], stdlib_exceptions.SystemExit())
        except sa_exceptions.DBAPIError:
            self.assert_(False)
        except stdlib_exceptions.SystemExit:
            self.assert_(True)


if __name__ == "__main__":
    testenv.main()
