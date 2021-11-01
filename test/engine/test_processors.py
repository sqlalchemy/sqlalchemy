from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures


class _BooleanProcessorTest(fixtures.TestBase):
    def test_int_to_bool_none(self):
        eq_(self.module.int_to_boolean(None), None)

    def test_int_to_bool_zero(self):
        eq_(self.module.int_to_boolean(0), False)

    def test_int_to_bool_one(self):
        eq_(self.module.int_to_boolean(1), True)

    def test_int_to_bool_positive_int(self):
        eq_(self.module.int_to_boolean(12), True)

    def test_int_to_bool_negative_int(self):
        eq_(self.module.int_to_boolean(-4), True)


class CBooleanProcessorTest(_BooleanProcessorTest):
    __requires__ = ("cextensions",)

    @classmethod
    def setup_test_class(cls):
        from sqlalchemy import cprocessors

        cls.module = cprocessors


class _DateProcessorTest(fixtures.TestBase):
    def test_date_no_string(self):
        assert_raises_message(
            ValueError,
            "Couldn't parse date string '2012' - value is not a string",
            self.module.str_to_date,
            2012,
        )

    def test_datetime_no_string(self):
        assert_raises_message(
            ValueError,
            "Couldn't parse datetime string '2012' - value is not a string",
            self.module.str_to_datetime,
            2012,
        )

    def test_time_no_string(self):
        assert_raises_message(
            ValueError,
            "Couldn't parse time string '2012' - value is not a string",
            self.module.str_to_time,
            2012,
        )

    def test_date_invalid_string(self):
        assert_raises_message(
            ValueError,
            "Couldn't parse date string: '5:a'",
            self.module.str_to_date,
            "5:a",
        )

    def test_datetime_invalid_string(self):
        assert_raises_message(
            ValueError,
            "Couldn't parse datetime string: '5:a'",
            self.module.str_to_datetime,
            "5:a",
        )

    def test_time_invalid_string(self):
        assert_raises_message(
            ValueError,
            "Couldn't parse time string: '5:a'",
            self.module.str_to_time,
            "5:a",
        )


class PyDateProcessorTest(_DateProcessorTest):
    @classmethod
    def setup_test_class(cls):
        from sqlalchemy import processors

        cls.module = type(
            "util",
            (object,),
            dict(
                (k, staticmethod(v))
                for k, v in list(processors.py_fallback().items())
            ),
        )


class CDateProcessorTest(_DateProcessorTest):
    __requires__ = ("cextensions",)

    @classmethod
    def setup_test_class(cls):
        from sqlalchemy import cprocessors

        cls.module = cprocessors
