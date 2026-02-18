from decimal import Decimal
from typing import assert_type

from sqlalchemy import Column
from sqlalchemy import Float
from sqlalchemy import JSON
from sqlalchemy import Numeric
from sqlalchemy import Select
from sqlalchemy import select
from sqlalchemy.sql.sqltypes import _JSON_VALUE


assert_type(Float(), Float[float])
assert_type(Float(asdecimal=True), Float[Decimal])

assert_type(Numeric(), Numeric[Decimal])
assert_type(Numeric(asdecimal=False), Numeric[float])


def test_json_value_type() -> None:

    j1: _JSON_VALUE = {
        "foo": "bar",
        "bat": {"value1": True},
        "hoho": [1, 2, 3],
    }
    j2: _JSON_VALUE = "foo"
    j3: _JSON_VALUE = 5
    j4: _JSON_VALUE = False
    j5: _JSON_VALUE = None
    j6: _JSON_VALUE = [None, 5, "foo", False]
    j7: _JSON_VALUE = {  # noqa: F841
        "j1": j1,
        "j2": j2,
        "j3": j3,
        "j4": j4,
        "j5": j5,
        "j6": j6,
    }


def test_json_parameterization() -> None:

    # test default type
    x: JSON = JSON()

    assert_type(x, JSON[_JSON_VALUE])

    # test column values

    s1 = select(Column(JSON()))

    assert_type(s1, Select[_JSON_VALUE])

    c1: Column[list[int]] = Column(JSON())
    s2 = select(c1)

    assert_type(s2, Select[list[int]])

    c2 = Column(JSON[list[int]]())
    s3 = select(c2)

    assert_type(s3, Select[list[int]])
