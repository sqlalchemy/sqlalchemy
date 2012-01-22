from test.lib.testing import eq_, assert_raises, assert_raises_message
from test.lib import testing, fixtures

try:
    from sqlalchemy.cprocessors import str_to_datetime as c_str_to_datetime, \
                                    str_to_date as c_str_to_date, \
                                    str_to_time as c_str_to_time
    from sqlalchemy.processors import py_fallback
    for key, value in py_fallback().items():
        globals()["py_%s" % key] = value
except:
    from sqlalchemy.processors import str_to_datetime as py_str_to_datetime, \
                                        str_to_date as py_str_to_date, \
                                        str_to_time as py_str_to_time

class DateProcessorTest(fixtures.TestBase):
    @testing.requires.cextensions
    def test_c_date_no_string(self):
        assert_raises_message(
            ValueError,
            "Couldn't parse date string '2012' - value is not a string",
            c_str_to_date, 2012
        )

    def test_py_date_no_string(self):
        assert_raises_message(
            ValueError,
            "Couldn't parse date string '2012' - value is not a string",
            py_str_to_date, 2012
        )

    @testing.requires.cextensions
    def test_c_datetime_no_string(self):
        assert_raises_message(
            ValueError,
            "Couldn't parse datetime string '2012' - value is not a string",
            c_str_to_datetime, 2012
        )

    def test_py_datetime_no_string(self):
        assert_raises_message(
            ValueError,
            "Couldn't parse datetime string '2012' - value is not a string",
            py_str_to_datetime, 2012
        )

    @testing.requires.cextensions
    def test_c_time_no_string(self):
        assert_raises_message(
            ValueError,
            "Couldn't parse time string '2012' - value is not a string",
            c_str_to_time, 2012
        )

    def test_py_time_no_string(self):
        assert_raises_message(
            ValueError,
            "Couldn't parse time string '2012' - value is not a string",
            py_str_to_time, 2012
        )

    @testing.requires.cextensions
    def test_c_date_invalid_string(self):
        assert_raises_message(
            ValueError,
            "Couldn't parse date string: '5:a'",
            c_str_to_date, "5:a"
        )

    def test_py_date_invalid_string(self):
        assert_raises_message(
            ValueError,
            "Couldn't parse date string: '5:a'",
            py_str_to_date, "5:a"
        )

    @testing.requires.cextensions
    def test_c_datetime_invalid_string(self):
        assert_raises_message(
            ValueError,
            "Couldn't parse datetime string: '5:a'",
            c_str_to_datetime, "5:a"
        )

    def test_py_datetime_invalid_string(self):
        assert_raises_message(
            ValueError,
            "Couldn't parse datetime string: '5:a'",
            py_str_to_datetime, "5:a"
        )

    @testing.requires.cextensions
    def test_c_time_invalid_string(self):
        assert_raises_message(
            ValueError,
            "Couldn't parse time string: '5:a'",
            c_str_to_time, "5:a"
        )

    def test_py_time_invalid_string(self):
        assert_raises_message(
            ValueError,
            "Couldn't parse time string: '5:a'",
            py_str_to_time, "5:a"
        )


