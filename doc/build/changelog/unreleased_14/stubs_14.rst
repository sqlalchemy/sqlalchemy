.. change::
    :tags: bug, mypy
    :tickets: sqlalchemy/sqlalchemy2-stubs/#14

    Fixed issue in mypy plugin where newly added support for
    :func:`_orm.as_declarative` needed to more fully add the
    ``DeclarativeMeta`` class to the mypy interpreter's state so that it does
    not result in a name not found error; additionally improves how global
    names are setup for the plugin including the ``Mapped`` name.

