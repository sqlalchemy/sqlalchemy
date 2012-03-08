from test.lib import fixtures
from test.lib.testing import assert_raises_message

from sqlalchemy import processors
try:
    from sqlalchemy import cprocessors
except ImportError:
    cprocessors = None

class _DateProcessorTest(fixtures.TestBase):
    def test_date_no_string(self):
        assert_raises_message(
            ValueError,
            "Couldn't parse date string '2012' - value is not a string",
            self.module.str_to_date, 2012
        )

    def test_datetime_no_string(self):
        assert_raises_message(
            ValueError,
            "Couldn't parse datetime string '2012' - value is not a string",
            self.module.str_to_datetime, 2012
        )

    def test_time_no_string(self):
        assert_raises_message(
            ValueError,
            "Couldn't parse time string '2012' - value is not a string",
            self.module.str_to_time, 2012
        )

    def test_date_invalid_string(self):
        assert_raises_message(
            ValueError,
            "Couldn't parse date string: '5:a'",
            self.module.str_to_date, "5:a"
        )

    def test_datetime_invalid_string(self):
        assert_raises_message(
            ValueError,
            "Couldn't parse datetime string: '5:a'",
            self.module.str_to_datetime, "5:a"
        )

    def test_time_invalid_string(self):
        assert_raises_message(
            ValueError,
            "Couldn't parse time string: '5:a'",
            self.module.str_to_time, "5:a"
        )


class PyDateProcessorTest(_DateProcessorTest):
    module = processors


class CDateProcessorTest(_DateProcessorTest):
    __requires__ = ('cextensions',)
    module = cprocessors
