#! coding:utf-8

"""Tests exceptions and DB-API exception wrapping."""


from sqlalchemy import exc as sa_exceptions
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import eq_
from sqlalchemy.engine import default
from sqlalchemy.util import u, compat


class Error(Exception):
    pass


class DatabaseError(Error):
    pass


class OperationalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):

    def __str__(self):
        return '<%s>' % self.bogus


class OutOfSpec(DatabaseError):
    pass


# exception with a totally different name...
class WrongNameError(DatabaseError):
    pass


# but they're going to call it their "IntegrityError"
IntegrityError = WrongNameError


# and they're going to subclass it!
class SpecificIntegrityError(WrongNameError):
    pass


class WrapTest(fixtures.TestBase):

    def _translating_dialect_fixture(self):
        d = default.DefaultDialect()
        d.dbapi_exception_translation_map = {
            "WrongNameError": "IntegrityError"
        }
        return d

    def test_db_error_normal(self):
        try:
            raise sa_exceptions.DBAPIError.instance(
                '', [],
                OperationalError(), DatabaseError)
        except sa_exceptions.DBAPIError:
            self.assert_(True)

    def test_tostring(self):
        try:
            raise sa_exceptions.DBAPIError.instance(
                'this is a message',
                None, OperationalError(), DatabaseError)
        except sa_exceptions.DBAPIError as exc:
            eq_(
                str(exc),
                "(test.base.test_except.OperationalError)  "
                "[SQL: 'this is a message'] (Background on this error at: "
                "http://sqlalche.me/e/e3q8)")

    def test_statement_error_no_code(self):
        try:
            raise sa_exceptions.DBAPIError.instance(
                'select * from table', [{"x": 1}],
                sa_exceptions.InvalidRequestError("hello"), DatabaseError)
        except sa_exceptions.StatementError as err:
            eq_(
                str(err),
                "(sqlalchemy.exc.InvalidRequestError) hello "
                "[SQL: 'select * from table'] [parameters: [{'x': 1}]]"
            )
            eq_(err.args, ("(sqlalchemy.exc.InvalidRequestError) hello", ))

    def test_statement_error_w_code(self):
        try:
            raise sa_exceptions.DBAPIError.instance(
                'select * from table', [{"x": 1}],
                sa_exceptions.InvalidRequestError("hello", code="abcd"),
                DatabaseError)
        except sa_exceptions.StatementError as err:
            eq_(
                str(err),
                "(sqlalchemy.exc.InvalidRequestError) hello "
                "[SQL: 'select * from table'] [parameters: [{'x': 1}]] "
                "(Background on this error at: http://sqlalche.me/e/abcd)"
            )
            eq_(err.args, ("(sqlalchemy.exc.InvalidRequestError) hello", ))

    def test_wrap_multi_arg(self):
        # this is not supported by the API but oslo_db is doing it
        orig = sa_exceptions.DBAPIError(False, False, False)
        orig.args = [2006, 'Test raise operational error']
        eq_(
            str(orig),
            "(2006, 'Test raise operational error') "
            "(Background on this error at: http://sqlalche.me/e/dbapi)"
        )

    def test_wrap_unicode_arg(self):
        # this is not supported by the API but oslo_db is doing it
        orig = sa_exceptions.DBAPIError(False, False, False)
        orig.args = [u('méil')]
        eq_(
            compat.text_type(orig),
            compat.u(
                "méil (Background on this error at: "
                "http://sqlalche.me/e/dbapi)")
        )
        eq_(orig.args, (u('méil'),))

    def test_tostring_large_dict(self):
        try:
            raise sa_exceptions.DBAPIError.instance(
                'this is a message',
                {
                    'a': 1, 'b': 2, 'c': 3, 'd': 4, 'e': 5, 'f': 6, 'g': 7,
                    'h': 8, 'i': 9, 'j': 10, 'k': 11
                },
                OperationalError(), DatabaseError)
        except sa_exceptions.DBAPIError as exc:
            assert str(exc).startswith(
                "(test.base.test_except.OperationalError)  "
                "[SQL: 'this is a message'] [parameters: {")

    def test_tostring_large_list(self):
        try:
            raise sa_exceptions.DBAPIError.instance(
                'this is a message',
                [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],
                OperationalError(), DatabaseError)
        except sa_exceptions.DBAPIError as exc:
            assert str(exc).startswith(
                "(test.base.test_except.OperationalError)  "
                "[SQL: 'this is a message'] [parameters: "
                "[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]]")

    def test_tostring_large_executemany(self):
        try:
            raise sa_exceptions.DBAPIError.instance(
                'this is a message',
                [{1: 1}, {1: 1}, {1: 1}, {1: 1}, {1: 1}, {1: 1},
                 {1: 1}, {1: 1}, {1: 1}, {1: 1}, ],
                OperationalError("sql error"), DatabaseError)
        except sa_exceptions.DBAPIError as exc:
            eq_(
                str(exc),
                "(test.base.test_except.OperationalError) sql error "
                "[SQL: 'this is a message'] [parameters: [{1: 1}, "
                "{1: 1}, {1: 1}, {1: 1}, {1: 1}, {1: 1}, {1: 1}, {1: "
                "1}, {1: 1}, {1: 1}]] (Background on this error at: "
                "http://sqlalche.me/e/e3q8)"
            )
            eq_(
                exc.args,
                ("(test.base.test_except.OperationalError) sql error", )
            )
        try:
            raise sa_exceptions.DBAPIError.instance('this is a message', [
                {1: 1}, {1: 1}, {1: 1}, {1: 1}, {1: 1}, {1: 1}, {1: 1},
                {1: 1}, {1: 1}, {1: 1}, {1: 1},
            ], OperationalError(), DatabaseError)
        except sa_exceptions.DBAPIError as exc:
            eq_(str(exc),
                "(test.base.test_except.OperationalError)  "
                "[SQL: 'this is a message'] [parameters: [{1: 1}, "
                "{1: 1}, {1: 1}, {1: 1}, {1: 1}, {1: 1}, "
                "{1: 1}, {1: 1}  ... displaying 10 of 11 total "
                "bound parameter sets ...  {1: 1}, {1: 1}]] "
                "(Background on this error at: http://sqlalche.me/e/e3q8)"
                )
        try:
            raise sa_exceptions.DBAPIError.instance(
                'this is a message',
                [
                    (1, ), (1, ), (1, ), (1, ), (1, ), (1, ),
                    (1, ), (1, ), (1, ), (1, ),
                ], OperationalError(), DatabaseError)

        except sa_exceptions.DBAPIError as exc:
            eq_(
                str(exc),
                "(test.base.test_except.OperationalError)  "
                "[SQL: 'this is a message'] [parameters: [(1,), "
                "(1,), (1,), (1,), (1,), (1,), (1,), (1,), (1,), (1,)]] "
                "(Background on this error at: http://sqlalche.me/e/e3q8)")
        try:
            raise sa_exceptions.DBAPIError.instance('this is a message', [
                (1, ), (1, ), (1, ), (1, ), (1, ), (1, ), (1, ), (1, ), (1, ),
                (1, ), (1, ),
            ], OperationalError(), DatabaseError)
        except sa_exceptions.DBAPIError as exc:
            eq_(str(exc),
                "(test.base.test_except.OperationalError)  "
                "[SQL: 'this is a message'] [parameters: [(1,), "
                "(1,), (1,), (1,), (1,), (1,), (1,), (1,)  "
                "... displaying 10 of 11 total bound "
                "parameter sets ...  (1,), (1,)]] "
                "(Background on this error at: http://sqlalche.me/e/e3q8)"
                )

    def test_db_error_busted_dbapi(self):
        try:
            raise sa_exceptions.DBAPIError.instance(
                '', [],
                ProgrammingError(), DatabaseError)
        except sa_exceptions.DBAPIError as e:
            self.assert_(True)
            self.assert_('Error in str() of DB-API' in e.args[0])

    def test_db_error_noncompliant_dbapi(self):
        try:
            raise sa_exceptions.DBAPIError.instance(
                '', [], OutOfSpec(),
                DatabaseError)
        except sa_exceptions.DBAPIError as e:
            # OutOfSpec subclasses DatabaseError
            self.assert_(e.__class__ is sa_exceptions.DatabaseError)
        except OutOfSpec:
            self.assert_(False)

        try:
            raise sa_exceptions.DBAPIError.instance(
                '', [],
                sa_exceptions.ArgumentError(), DatabaseError)
        except sa_exceptions.DBAPIError as e:
            self.assert_(e.__class__ is sa_exceptions.DBAPIError)
        except sa_exceptions.ArgumentError:
            self.assert_(False)

        dialect = self._translating_dialect_fixture()
        try:
            raise sa_exceptions.DBAPIError.instance(
                '', [],
                sa_exceptions.ArgumentError(), DatabaseError,
                dialect=dialect)
        except sa_exceptions.DBAPIError as e:
            self.assert_(e.__class__ is sa_exceptions.DBAPIError)
        except sa_exceptions.ArgumentError:
            self.assert_(False)

    def test_db_error_dbapi_uses_wrong_names(self):
        dialect = self._translating_dialect_fixture()

        try:
            raise sa_exceptions.DBAPIError.instance(
                '', [], IntegrityError(),
                DatabaseError, dialect=dialect)
        except sa_exceptions.DBAPIError as e:
            self.assert_(e.__class__ is sa_exceptions.IntegrityError)

        try:
            raise sa_exceptions.DBAPIError.instance(
                '', [], SpecificIntegrityError(),
                DatabaseError, dialect=dialect)
        except sa_exceptions.DBAPIError as e:
            self.assert_(e.__class__ is sa_exceptions.IntegrityError)

        try:
            raise sa_exceptions.DBAPIError.instance(
                '', [], SpecificIntegrityError(),
                DatabaseError)
        except sa_exceptions.DBAPIError as e:
            # doesn't work without a dialect
            self.assert_(e.__class__ is not sa_exceptions.IntegrityError)

    def test_db_error_keyboard_interrupt(self):
        try:
            raise sa_exceptions.DBAPIError.instance(
                '', [],
                KeyboardInterrupt(), DatabaseError)
        except sa_exceptions.DBAPIError:
            self.assert_(False)
        except KeyboardInterrupt:
            self.assert_(True)

    def test_db_error_system_exit(self):
        try:
            raise sa_exceptions.DBAPIError.instance(
                '', [],
                SystemExit(), DatabaseError)
        except sa_exceptions.DBAPIError:
            self.assert_(False)
        except SystemExit:
            self.assert_(True)
