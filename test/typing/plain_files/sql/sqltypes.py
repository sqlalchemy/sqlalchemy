from sqlalchemy import Float
from sqlalchemy import Numeric

# EXPECTED_TYPE: Float[float]
reveal_type(Float())
# EXPECTED_TYPE: Float[Decimal]
reveal_type(Float(asdecimal=True))

# EXPECTED_TYPE: Numeric[Decimal]
reveal_type(Numeric())
# EXPECTED_TYPE: Numeric[float]
reveal_type(Numeric(asdecimal=False))
