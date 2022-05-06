from sqlalchemy import cast
from sqlalchemy import Column
from sqlalchemy import func
from sqlalchemy import Integer
from sqlalchemy import or_
from sqlalchemy import select
from sqlalchemy import Text
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.dialects.postgresql import array
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


class Test(Base):
    __tablename__ = "test_table_json"

    id = Column(Integer, primary_key=True)
    data = Column(JSONB)


elem = func.jsonb_array_elements(Test.data, type_=JSONB).column_valued("elem")

stmt = select(Test).where(
    or_(
        cast("example code", ARRAY(Text)).contained_by(
            array([select(elem["code"].astext).scalar_subquery()])
        ),
        cast("stefan", ARRAY(Text)).contained_by(
            array([select(elem["code"]["new_value"].astext).scalar_subquery()])
        ),
    )
)
print(stmt)
