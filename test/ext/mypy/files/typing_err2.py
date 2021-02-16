from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy.orm import declared_attr
from sqlalchemy.orm import registry
from sqlalchemy.orm import relationship

reg: registry = registry()


@reg.mapped
class Foo:
    id: int = Column(Integer())

    # EXPECTED: Can't infer type from @declared_attr on function 'name'; # noqa
    @declared_attr
    # EXPECTED: Column type should be a TypeEngine subclass not 'builtins.str'
    def name(cls) -> Column[str]:
        return Column(String)

    # EXPECTED: Left hand assignment 'other_name: "Column[String]"' not compatible with ORM mapped expression of type "Mapped[str]" # noqa
    other_name: Column[String] = Column(String)

    # EXPECTED: Can't infer type from @declared_attr on function 'third_name';
    @declared_attr
    # EXPECTED_MYPY: Missing type parameters for generic type "Column"
    def third_name(cls) -> Column:
        return Column(String)

    # EXPECTED: Can't infer type from @declared_attr on function 'some_relationship' # noqa
    @declared_attr
    # EXPECTED_MYPY: Missing type parameters for generic type "relationship"
    def some_relationship(cls) -> relationship:
        return relationship("Bar")


Foo(name="x")
