"""Tests exceptions and DB-API exception wrapping."""

from itertools import product
import pickle

from sqlalchemy import exc as sa_exceptions
from sqlalchemy.engine import default
from sqlalchemy.testing import combinations_list
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures


class Error(Exception):
    pass


class DatabaseError(Error):
    pass


class OperationalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    def __str__(self):
        return "<%s>" % self.bogus


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
    def test_version_token(self):
        assert sa_exceptions._version_token in (
            "13",
            "14",
            "15",
            "16",
            "20",
            "21",
            "22",
        )

    def _translating_dialect_fixture(self):
        d = default.DefaultDialect()
        d.dbapi_exception_translation_map = {
            "WrongNameError": "IntegrityError"
        }
        return d

    def test_db_error_normal(self):
        try:
            raise sa_exceptions.DBAPIError.instance(
                "", [], OperationalError(), DatabaseError
            )
        except sa_exceptions.DBAPIError:
            self.assert_(True)

    def test_tostring(self):
        try:
            raise sa_exceptions.DBAPIError.instance(
                "this is a message", None, OperationalError(), DatabaseError
            )
        except sa_exceptions.DBAPIError as exc:
            eq_(
                str(exc),
                "(test.base.test_except.OperationalError) \n"
                "[SQL: this is a message]\n"
                "(Background on this error at: https://sqlalche.me/e/%s/e3q8)"
                % sa_exceptions._version_token,
            )

    def test_tostring_with_newlines(self):
        try:
            raise sa_exceptions.DBAPIError.instance(
                "this is a message\nthis is the next line\nthe last line",
                None,
                OperationalError(),
                DatabaseError,
            )
        except sa_exceptions.DBAPIError as exc:
            eq_(
                str(exc),
                "(test.base.test_except.OperationalError) \n"
                "[SQL: this is a message\nthis is the next line\n"
                "the last line]\n"
                "(Background on this error at: https://sqlalche.me/e/%s/e3q8)"
                % sa_exceptions._version_token,
            )

    def test_statement_error_no_code(self):
        try:
            raise sa_exceptions.DBAPIError.instance(
                "select * from table",
                [{"x": 1}],
                sa_exceptions.InvalidRequestError("hello"),
                DatabaseError,
            )
        except sa_exceptions.StatementError as err:
            eq_(
                str(err),
                "(sqlalchemy.exc.InvalidRequestError) hello\n"
                "[SQL: select * from table]\n[parameters: [{'x': 1}]]",
            )
            eq_(err.args, ("(sqlalchemy.exc.InvalidRequestError) hello",))

    def test_statement_error_w_code(self):
        try:
            raise sa_exceptions.DBAPIError.instance(
                "select * from table",
                [{"x": 1}],
                sa_exceptions.InvalidRequestError("hello", code="abcd"),
                DatabaseError,
            )
        except sa_exceptions.StatementError as err:
            eq_(
                str(err),
                "(sqlalchemy.exc.InvalidRequestError) hello\n"
                "[SQL: select * from table]\n"
                "[parameters: [{'x': 1}]]\n"
                "(Background on this error at: https://sqlalche.me/e/%s/abcd)"
                % sa_exceptions._version_token,
            )
            eq_(err.args, ("(sqlalchemy.exc.InvalidRequestError) hello",))

    def test_wrap_multi_arg(self):
        # this is not supported by the API but oslo_db is doing it
        orig = sa_exceptions.DBAPIError(False, False, False)
        orig.args = [2006, "Test raise operational error"]
        eq_(
            str(orig),
            "(2006, 'Test raise operational error')\n"
            "(Background on this error at: https://sqlalche.me/e/%s/dbapi)"
            % sa_exceptions._version_token,
        )

    def test_wrap_unicode_arg(self):
        # this is not supported by the API but oslo_db is doing it
        orig = sa_exceptions.DBAPIError(False, False, False)
        orig.args = ["méil"]
        eq_(
            str(orig),
            "méil\n(Background on this error at: "
            "https://sqlalche.me/e/%s/dbapi)" % sa_exceptions._version_token,
        )
        eq_(orig.args, ("méil",))

    def test_tostring_large_dict(self):
        try:
            raise sa_exceptions.DBAPIError.instance(
                "this is a message",
                {
                    "a": 1,
                    "b": 2,
                    "c": 3,
                    "d": 4,
                    "e": 5,
                    "f": 6,
                    "g": 7,
                    "h": 8,
                    "i": 9,
                    "j": 10,
                    "k": 11,
                },
                OperationalError(),
                DatabaseError,
            )
        except sa_exceptions.DBAPIError as exc:
            assert str(exc).startswith(
                "(test.base.test_except.OperationalError) \n"
                "[SQL: this is a message]\n"
                "[parameters: {"
            )

    def test_tostring_large_list(self):
        try:
            raise sa_exceptions.DBAPIError.instance(
                "this is a message",
                [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],
                OperationalError(),
                DatabaseError,
            )
        except sa_exceptions.DBAPIError as ex:
            assert str(ex).startswith(
                "(test.base.test_except.OperationalError) \n"
                "[SQL: this is a message]\n[parameters: "
                "[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]]"
            )

    def test_tostring_large_executemany(self):
        try:
            raise sa_exceptions.DBAPIError.instance(
                "this is a message",
                [
                    {1: 1},
                    {1: 1},
                    {1: 1},
                    {1: 1},
                    {1: 1},
                    {1: 1},
                    {1: 1},
                    {1: 1},
                    {1: 1},
                    {1: 1},
                ],
                OperationalError("sql error"),
                DatabaseError,
            )
        except sa_exceptions.DBAPIError as exc:
            eq_(
                str(exc),
                "(test.base.test_except.OperationalError) sql error\n"
                "[SQL: this is a message]\n"
                "[parameters: [{1: 1}, {1: 1}, {1: 1}, {1: 1}, {1: 1},"
                " {1: 1}, {1: 1}, {1: 1}, {1: 1}, {1: 1}]]\n"
                "(Background on this error at: https://sqlalche.me/e/%s/e3q8)"
                % sa_exceptions._version_token,
            )
            eq_(
                exc.args,
                ("(test.base.test_except.OperationalError) sql error",),
            )
        try:
            raise sa_exceptions.DBAPIError.instance(
                "this is a message",
                [
                    {1: 1},
                    {1: 1},
                    {1: 1},
                    {1: 1},
                    {1: 1},
                    {1: 1},
                    {1: 1},
                    {1: 1},
                    {1: 1},
                    {1: 1},
                    {1: 1},
                ],
                OperationalError(),
                DatabaseError,
                ismulti=True,
            )
        except sa_exceptions.DBAPIError as exc:
            eq_(
                str(exc),
                "(test.base.test_except.OperationalError) \n"
                "[SQL: this is a message]\n"
                "[parameters: [{1: 1}, "
                "{1: 1}, {1: 1}, {1: 1}, {1: 1}, {1: 1}, "
                "{1: 1}, {1: 1}  ... displaying 10 of 11 total "
                "bound parameter sets ...  {1: 1}, {1: 1}]]\n"
                "(Background on this error at: https://sqlalche.me/e/%s/e3q8)"
                % sa_exceptions._version_token,
            )
        try:
            raise sa_exceptions.DBAPIError.instance(
                "this is a message",
                [(1,), (1,), (1,), (1,), (1,), (1,), (1,), (1,), (1,), (1,)],
                OperationalError(),
                DatabaseError,
            )

        except sa_exceptions.DBAPIError as exc:
            eq_(
                str(exc),
                "(test.base.test_except.OperationalError) \n"
                "[SQL: this is a message]\n"
                "[parameters: [(1,), "
                "(1,), (1,), (1,), (1,), (1,), (1,), (1,), (1,), (1,)]]\n"
                "(Background on this error at: https://sqlalche.me/e/%s/e3q8)"
                % sa_exceptions._version_token,
            )
        try:
            raise sa_exceptions.DBAPIError.instance(
                "this is a message",
                [
                    (1,),
                    (1,),
                    (1,),
                    (1,),
                    (1,),
                    (1,),
                    (1,),
                    (1,),
                    (1,),
                    (1,),
                    (1,),
                ],
                OperationalError(),
                DatabaseError,
                ismulti=True,
            )
        except sa_exceptions.DBAPIError as exc:
            eq_(
                str(exc),
                "(test.base.test_except.OperationalError) \n"
                "[SQL: this is a message]\n"
                "[parameters: [(1,), "
                "(1,), (1,), (1,), (1,), (1,), (1,), (1,)  "
                "... displaying 10 of 11 total bound "
                "parameter sets ...  (1,), (1,)]]\n"
                "(Background on this error at: https://sqlalche.me/e/%s/e3q8)"
                % sa_exceptions._version_token,
            )

    def test_db_error_busted_dbapi(self):
        try:
            raise sa_exceptions.DBAPIError.instance(
                "", [], ProgrammingError(), DatabaseError
            )
        except sa_exceptions.DBAPIError as e:
            self.assert_(True)
            self.assert_("Error in str() of DB-API" in e.args[0])

    def test_db_error_noncompliant_dbapi(self):
        try:
            raise sa_exceptions.DBAPIError.instance(
                "", [], OutOfSpec(), DatabaseError
            )
        except sa_exceptions.DBAPIError as e:
            # OutOfSpec subclasses DatabaseError
            self.assert_(e.__class__ is sa_exceptions.DatabaseError)
        except OutOfSpec:
            self.assert_(False)

        try:
            raise sa_exceptions.DBAPIError.instance(
                "", [], sa_exceptions.ArgumentError(), DatabaseError
            )
        except sa_exceptions.DBAPIError as e:
            self.assert_(e.__class__ is sa_exceptions.DBAPIError)
        except sa_exceptions.ArgumentError:
            self.assert_(False)

        dialect = self._translating_dialect_fixture()
        try:
            raise sa_exceptions.DBAPIError.instance(
                "",
                [],
                sa_exceptions.ArgumentError(),
                DatabaseError,
                dialect=dialect,
            )
        except sa_exceptions.DBAPIError as e:
            self.assert_(e.__class__ is sa_exceptions.DBAPIError)
        except sa_exceptions.ArgumentError:
            self.assert_(False)

    def test_db_error_dbapi_uses_wrong_names(self):
        dialect = self._translating_dialect_fixture()

        try:
            raise sa_exceptions.DBAPIError.instance(
                "", [], IntegrityError(), DatabaseError, dialect=dialect
            )
        except sa_exceptions.DBAPIError as e:
            self.assert_(e.__class__ is sa_exceptions.IntegrityError)

        try:
            raise sa_exceptions.DBAPIError.instance(
                "",
                [],
                SpecificIntegrityError(),
                DatabaseError,
                dialect=dialect,
            )
        except sa_exceptions.DBAPIError as e:
            self.assert_(e.__class__ is sa_exceptions.IntegrityError)

        try:
            raise sa_exceptions.DBAPIError.instance(
                "", [], SpecificIntegrityError(), DatabaseError
            )
        except sa_exceptions.DBAPIError as e:
            # doesn't work without a dialect
            self.assert_(e.__class__ is not sa_exceptions.IntegrityError)

    def test_db_error_keyboard_interrupt(self):
        try:
            raise sa_exceptions.DBAPIError.instance(
                "", [], KeyboardInterrupt(), DatabaseError
            )
        except sa_exceptions.DBAPIError:
            self.assert_(False)
        except KeyboardInterrupt:
            self.assert_(True)

    def test_db_error_system_exit(self):
        try:
            raise sa_exceptions.DBAPIError.instance(
                "", [], SystemExit(), DatabaseError
            )
        except sa_exceptions.DBAPIError:
            self.assert_(False)
        except SystemExit:
            self.assert_(True)


def details(cls):
    inst = cls("msg", "stmt", (), "orig")
    inst.add_detail("d1")
    inst.add_detail("d2")
    return inst


class EqException(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __eq__(self, other):
        return isinstance(other, EqException) and other.msg == self.msg


ALL_EXC = [
    (
        [sa_exceptions.SQLAlchemyError],
        [lambda cls: cls(1, 2, code="42")],
    ),
    ([sa_exceptions.ObjectNotExecutableError], [lambda cls: cls("xx")]),
    (
        [sa_exceptions.EmulatedDBAPIException],
        [lambda cls: cls("xx", EqException("original"))],
    ),
    (
        [
            sa_exceptions.ArgumentError,
            sa_exceptions.DuplicateColumnError,
            sa_exceptions.ConstraintColumnNotFoundError,
            sa_exceptions.NoSuchModuleError,
            sa_exceptions.NoForeignKeysError,
            sa_exceptions.AmbiguousForeignKeysError,
            sa_exceptions.CompileError,
            sa_exceptions.IdentifierError,
            sa_exceptions.DisconnectionError,
            sa_exceptions.InvalidatePoolError,
            sa_exceptions.TimeoutError,
            sa_exceptions.InvalidRequestError,
            sa_exceptions.IllegalStateChangeError,
            sa_exceptions.NoInspectionAvailable,
            sa_exceptions.PendingRollbackError,
            sa_exceptions.ResourceClosedError,
            sa_exceptions.NoSuchColumnError,
            sa_exceptions.NoResultFound,
            sa_exceptions.MultipleResultsFound,
            sa_exceptions.NoReferenceError,
            sa_exceptions.AwaitRequired,
            sa_exceptions.MissingGreenlet,
            sa_exceptions.NoSuchTableError,
            sa_exceptions.UnreflectableTableError,
            sa_exceptions.UnboundExecutionError,
        ],
        [lambda cls: cls("foo", code="42")],
    ),
    (
        [sa_exceptions.CircularDependencyError],
        [
            lambda cls: cls("msg", ["cycles"], "edges"),
            lambda cls: cls("msg", ["cycles"], "edges", "xx", "zz"),
        ],
    ),
    (
        [sa_exceptions.UnsupportedCompilationError],
        [lambda cls: cls("cmp", "el"), lambda cls: cls("cmp", "el", "msg")],
    ),
    (
        [sa_exceptions.NoReferencedTableError],
        [lambda cls: cls("msg", "tbl")],
    ),
    (
        [sa_exceptions.NoReferencedColumnError],
        [lambda cls: cls("msg", "tbl", "col")],
    ),
    (
        [sa_exceptions.StatementError],
        [
            lambda cls: cls("msg", "stmt", (), "orig"),
            lambda cls: cls("msg", "stmt", (), "orig", True, "99", True),
            details,
        ],
    ),
    (
        [
            sa_exceptions.DBAPIError,
            sa_exceptions.InterfaceError,
            sa_exceptions.DatabaseError,
            sa_exceptions.DataError,
            sa_exceptions.OperationalError,
            sa_exceptions.IntegrityError,
            sa_exceptions.InternalError,
            sa_exceptions.ProgrammingError,
            sa_exceptions.NotSupportedError,
        ],
        [
            lambda cls: cls("stmt", (), "orig"),
            lambda cls: cls("stmt", (), "orig", True, True, "99", True),
            details,
        ],
    ),
    (
        [
            sa_exceptions.SADeprecationWarning,
            sa_exceptions.Base20DeprecationWarning,
            sa_exceptions.LegacyAPIWarning,
            sa_exceptions.MovedIn20Warning,
            sa_exceptions.SAWarning,
        ],
        [lambda cls: cls("foo", code="42")],
    ),
    ([sa_exceptions.SAPendingDeprecationWarning], [lambda cls: cls(1, 2, 3)]),
    ([sa_exceptions.SATestSuiteWarning], [lambda cls: cls()]),
]


class PickleException(fixtures.TestBase):
    def test_all_exc(self):
        found = {
            e
            for e in vars(sa_exceptions).values()
            if isinstance(e, type) and issubclass(e, Exception)
        }

        listed = set()
        for cls_list, _ in ALL_EXC:
            listed.update(cls_list)

        eq_(found, listed)

    def make_combinations():
        unroll = []
        for cls_list, callable_list in ALL_EXC:
            unroll.extend(product(cls_list, callable_list))

        return combinations_list(unroll)

    @make_combinations()
    def test_exc(self, cls, ctor):
        inst = ctor(cls)
        re_created = pickle.loads(pickle.dumps(inst))

        eq_(re_created.__class__, cls)
        eq_(re_created.args, inst.args)
        eq_(re_created.__dict__, inst.__dict__)
