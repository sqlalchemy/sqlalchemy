from decimal import Decimal
from typing import assert_type

from sqlalchemy import Float
from sqlalchemy import Numeric

assert_type(Float(), Float[float])
assert_type(Float(asdecimal=True), Float[Decimal])

assert_type(Numeric(), Numeric[Decimal])
assert_type(Numeric(asdecimal=False), Numeric[float])
