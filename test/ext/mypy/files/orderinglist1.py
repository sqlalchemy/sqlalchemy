from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy.ext.orderinglist import ordering_list
from sqlalchemy.orm import registry
from sqlalchemy.orm import relationship

mapper_registry: registry = registry()


@mapper_registry.mapped
class A:
    __tablename__ = "a"
    id = Column(Integer, primary_key=True)

    # EXPECTED: Can't infer type from ORM mapped expression assigned to attribute 'parents'; please specify a Python type or Mapped[<python type>] on the left hand side.  # noqa
    parents = relationship("A", collection_class=ordering_list("ordering"))
    parent_id = Column(Integer, ForeignKey("a.id"))
    ordering = Column(Integer)


a1 = A(id=5, ordering=10)

# EXPECTED_MYPY: Argument "parents" to "A" has incompatible type "List[A]"; expected "Mapped[Any]"  # noqa
a2 = A(parents=[a1])
