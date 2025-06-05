import datetime
import re
from types import MappingProxyType

from sqlalchemy import exc
from sqlalchemy.engine import processors
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import combinations
from sqlalchemy.testing import eq_
from sqlalchemy.testing import expect_raises_message
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_none
from sqlalchemy.testing.assertions import expect_deprecated
from sqlalchemy.util import immutabledict


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


class CyBooleanProcessorTest(_BooleanProcessorTest):
    __requires__ = ("cextensions",)

    @classmethod
    def setup_test_class(cls):
        from sqlalchemy.engine import _processors_cy

        cls.module = _processors_cy


class _DateProcessorTest(fixtures.TestBase):
    def test_iso_datetime(self):
        eq_(
            self.module.str_to_datetime("2022-04-03 17:12:34.353"),
            datetime.datetime(2022, 4, 3, 17, 12, 34, 353000),
        )

        eq_(
            self.module.str_to_datetime("2022-04-03 17:12:34.353123"),
            datetime.datetime(2022, 4, 3, 17, 12, 34, 353123),
        )

        eq_(
            self.module.str_to_datetime("2022-04-03 17:12:34"),
            datetime.datetime(2022, 4, 3, 17, 12, 34),
        )

        eq_(
            self.module.str_to_time("17:12:34.353123"),
            datetime.time(17, 12, 34, 353123),
        )

        eq_(
            self.module.str_to_time("17:12:34.353"),
            datetime.time(17, 12, 34, 353000),
        )

        eq_(
            self.module.str_to_time("17:12:34"),
            datetime.time(17, 12, 34),
        )

        eq_(self.module.str_to_date("2022-04-03"), datetime.date(2022, 4, 3))

    @combinations("str_to_datetime", "str_to_time", "str_to_date")
    def test_no_string(self, meth):
        with expect_raises_message(
            TypeError, "fromisoformat: argument must be str"
        ):
            fn = getattr(self.module, meth)
            fn(2012)

    def test_datetime_no_string_custom_reg(self):
        assert_raises_message(
            ValueError,
            "Couldn't parse datetime string '2012' - value is not a string",
            processors.str_to_datetime_processor_factory(
                re.compile(r"(\d+)-(\d+)-(\d+) (\d+):(\d+):(\d+)(?:\.(\d+))?"),
                datetime.datetime,
            ),
            2012,
        )

    def test_time_no_string_custom_reg(self):
        assert_raises_message(
            ValueError,
            "Couldn't parse time string '2012' - value is not a string",
            processors.str_to_datetime_processor_factory(
                re.compile(r"^(\d+):(\d+):(\d+)(?:\.(\d{6}))?$"), datetime.time
            ),
            2012,
        )

    @combinations("str_to_datetime", "str_to_time", "str_to_date")
    def test_invalid_string(self, meth):
        with expect_raises_message(
            ValueError, "Invalid isoformat string: '5:a'"
        ):
            fn = getattr(self.module, meth)
            fn("5:a")

    @combinations("str_to_datetime", "str_to_time", "str_to_date")
    def test_none(self, meth):
        fn = getattr(self.module, meth)
        is_none(fn(None))


class PyDateProcessorTest(_DateProcessorTest):
    @classmethod
    def setup_test_class(cls):
        from sqlalchemy.engine import _processors_cy
        from sqlalchemy.util.langhelpers import load_uncompiled_module

        py_mod = load_uncompiled_module(_processors_cy)

        cls.module = py_mod


class CyDateProcessorTest(_DateProcessorTest):
    __requires__ = ("cextensions",)

    @classmethod
    def setup_test_class(cls):
        from sqlalchemy.engine import _processors_cy

        assert _processors_cy._is_compiled()
        cls.module = _processors_cy


class _DistillArgsTest(fixtures.TestBase):
    def test_distill_20_none(self):
        eq_(self.module._distill_params_20(None), ())

    def test_distill_20_empty_sequence(self):
        with expect_deprecated(
            r"Empty parameter sequence passed to execute\(\). "
            "This use is deprecated and will raise an exception in a "
            "future SQLAlchemy release"
        ):
            eq_(self.module._distill_params_20(()), ())
            eq_(self.module._distill_params_20([]), [])

    def test_distill_20_sequence_dict(self):
        eq_(self.module._distill_params_20(({"a": 1},)), ({"a": 1},))
        eq_(
            self.module._distill_params_20([{"a": 1}, {"a": 2}]),
            [{"a": 1}, {"a": 2}],
        )
        eq_(
            self.module._distill_params_20((MappingProxyType({"a": 1}),)),
            (MappingProxyType({"a": 1}),),
        )

    @combinations(
        [(1, 2, 3)],
        [([1, 2, 3],)],
        [[1, 2, 3]],
        [["a", "b"]],
        [((1, 2, 3),)],
        [[(1, 2, 3)]],
        [((1, 2), (2, 3))],
        [[(1, 2), (2, 3)]],
        argnames="arg",
    )
    def test_distill_20_sequence_error(self, arg):
        with expect_raises_message(
            exc.ArgumentError,
            "List argument must consist only of dictionaries",
        ):
            self.module._distill_params_20(arg)

    def test_distill_20_dict(self):
        eq_(self.module._distill_params_20({"foo": "bar"}), [{"foo": "bar"}])
        eq_(
            self.module._distill_params_20(immutabledict({"foo": "bar"})),
            [immutabledict({"foo": "bar"})],
        )
        eq_(
            self.module._distill_params_20(MappingProxyType({"foo": "bar"})),
            [MappingProxyType({"foo": "bar"})],
        )

    def test_distill_20_error(self):
        with expect_raises_message(
            exc.ArgumentError, "mapping or list expected for parameters"
        ):
            self.module._distill_params_20("foo")
        with expect_raises_message(
            exc.ArgumentError, "mapping or list expected for parameters"
        ):
            self.module._distill_params_20(1)

    def test_distill_raw_none(self):
        eq_(self.module._distill_raw_params(None), ())

    def test_distill_raw_empty_list(self):
        eq_(self.module._distill_raw_params([]), [])

    def test_distill_raw_list_sequence(self):
        eq_(self.module._distill_raw_params([(1, 2, 3)]), [(1, 2, 3)])
        eq_(
            self.module._distill_raw_params([(1, 2), (2, 3)]), [(1, 2), (2, 3)]
        )

    def test_distill_raw_list_dict(self):
        eq_(
            self.module._distill_raw_params([{"a": 1}, {"a": 2}]),
            [{"a": 1}, {"a": 2}],
        )
        eq_(
            self.module._distill_raw_params([MappingProxyType({"a": 1})]),
            [MappingProxyType({"a": 1})],
        )

    def test_distill_raw_sequence_error(self):
        with expect_raises_message(
            exc.ArgumentError,
            "List argument must consist only of tuples or dictionaries",
        ):
            self.module._distill_raw_params([1, 2, 3])
        with expect_raises_message(
            exc.ArgumentError,
            "List argument must consist only of tuples or dictionaries",
        ):
            self.module._distill_raw_params([[1, 2, 3]])
        with expect_raises_message(
            exc.ArgumentError,
            "List argument must consist only of tuples or dictionaries",
        ):
            self.module._distill_raw_params(["a", "b"])

    def test_distill_raw_tuple(self):
        eq_(self.module._distill_raw_params(()), [()])
        eq_(self.module._distill_raw_params((1, 2, 3)), [(1, 2, 3)])

    def test_distill_raw_dict(self):
        eq_(self.module._distill_raw_params({"foo": "bar"}), [{"foo": "bar"}])
        eq_(
            self.module._distill_raw_params(immutabledict({"foo": "bar"})),
            [immutabledict({"foo": "bar"})],
        )
        eq_(
            self.module._distill_raw_params(MappingProxyType({"foo": "bar"})),
            [MappingProxyType({"foo": "bar"})],
        )

    def test_distill_raw_error(self):
        with expect_raises_message(
            exc.ArgumentError, "mapping or sequence expected for parameters"
        ):
            self.module._distill_raw_params("foo")
        with expect_raises_message(
            exc.ArgumentError, "mapping or sequence expected for parameters"
        ):
            self.module._distill_raw_params(1)


class PyDistillArgsTest(_DistillArgsTest):
    @classmethod
    def setup_test_class(cls):
        from sqlalchemy.engine import _util_cy
        from sqlalchemy.util.langhelpers import load_uncompiled_module

        _py_util = load_uncompiled_module(_util_cy)
        cls.module = _py_util


class CyDistillArgsTest(_DistillArgsTest):
    __requires__ = ("cextensions",)

    @classmethod
    def setup_test_class(cls):
        from sqlalchemy.engine import _util_cy

        assert _util_cy._is_compiled()
        cls.module = _util_cy
