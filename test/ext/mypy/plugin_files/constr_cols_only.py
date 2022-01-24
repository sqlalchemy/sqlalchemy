from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class A(Base):
    __tablename__ = "a"

    id = Column(Integer, primary_key=True)
    data = Column(String)
    x = Column(Integer)
    y = Column(Integer)


a1 = A(data="d", x=5, y=4)


# EXPECTED_MYPY: Argument "data" to "A" has incompatible type "int"; expected "Optional[str]" # noqa
a2 = A(data=5)

# EXPECTED_MYPY: Unexpected keyword argument "nonexistent" for "A"
a3 = A(nonexistent="hi")

print(a1)
print(a2)
print(a3)
