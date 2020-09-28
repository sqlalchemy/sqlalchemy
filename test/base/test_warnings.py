from sqlalchemy.testing import eq_
from sqlalchemy.testing import expect_deprecated
from sqlalchemy.testing import fixtures
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
        occurrences = 100
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
