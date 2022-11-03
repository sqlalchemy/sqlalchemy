from sqlalchemy import testing
from sqlalchemy.exc import SADeprecationWarning
from sqlalchemy.testing import eq_
from sqlalchemy.testing import expect_deprecated
from sqlalchemy.testing import fixtures
from sqlalchemy.util.deprecations import _decorate_cls_with_warning
from sqlalchemy.util.deprecations import warn_deprecated_limited
from sqlalchemy.util.langhelpers import _hash_limit_string


class WarnDeprecatedLimitedTest(fixtures.TestBase):
    __backend__ = False

    def test_warn_deprecated_limited_text(self):
        with expect_deprecated("foo has been deprecated"):
            warn_deprecated_limited(
                "%s has been deprecated [%d]", ("foo", 1), "1.3"
            )

    def test_warn_deprecated_limited_cap(self):
        """warn_deprecated_limited() and warn_limited() use
        _hash_limit_string

        actually just verifying that _hash_limit_string works as expected
        """
        occurrences = 500
        cap = 10

        printouts = set()
        messages = set()
        for i in range(occurrences):
            message = _hash_limit_string(
                "this is a unique message: %d", cap, (i,)
            )
            printouts.add(str(message))
            messages.add(message)

        eq_(len(printouts), occurrences)
        eq_(len(messages), cap)


class ClsWarningTest(fixtures.TestBase):
    @testing.fixture
    def dep_cls_fixture(self):
        class Connectable:
            """a docstring"""

            some_member = "foo"

        Connectable = _decorate_cls_with_warning(
            Connectable,
            None,
            SADeprecationWarning,
            "a message",
            "2.0",
            "another message",
        )

        return Connectable

    def test_dep_inspectable(self, dep_cls_fixture):
        """test #8115"""

        import inspect

        class PlainClass:
            some_member = "bar"

        pc_keys = dict(inspect.getmembers(PlainClass()))
        insp_keys = dict(inspect.getmembers(dep_cls_fixture()))

        assert set(insp_keys).intersection(
            (
                "__class__",
                "__doc__",
                "__eq__",
                "__dict__",
                "__weakref__",
                "some_member",
            )
        )
        eq_(set(pc_keys), set(insp_keys))
