from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy.orm import registry


reg: registry = registry()


@reg.mapped
class Foo:
    id: int = Column(Integer())
    name: str = Column(String)
    other_name: str = Column(String(50))

    # has a string key in it
    third_name = Column("foo", String(50))

    some_name = "fourth_name"

    fourth_name = Column(some_name, String(50))


f1 = Foo()

# This needs to work, e.g., value is "int" at the instance level
val: int = f1.id  # noqa

# also, the type are not optional, since we used an explicit
# type without Optional
p: str = f1.name

Foo.id.property


Foo(name="n", other_name="on", third_name="tn", fourth_name="fn")
