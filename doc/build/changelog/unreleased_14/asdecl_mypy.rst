.. change::
    :tags: bug, mypy

    Added support for the Mypy extension to correctly interpret a declarative
    base class that's generated using the :func:`_orm.as_declarative` function
    as well as the :meth:`_orm.registry.as_declarative_base` method.
