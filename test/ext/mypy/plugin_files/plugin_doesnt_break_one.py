from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy.orm import registry

reg: registry = registry()


@reg.mapped
class Foo:
    pass
    id: int = Column(Integer())
    name: str = Column(String)


f1 = Foo()


# EXPECTED_MYPY: Name 'u1' is not defined
p: str = u1.name  # noqa
