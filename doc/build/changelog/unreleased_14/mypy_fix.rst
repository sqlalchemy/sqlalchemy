.. change::
    :tags: bug, mypy
    :versions: 2.0.0rc3

    Adjustments made to the mypy plugin to accommodate for some potential
    changes being made for issue #236 sqlalchemy2-stubs when using SQLAlchemy
    1.4. These changes are being kept in sync within SQLAlchemy 2.0.
    The changes are also backwards compatible with older versions of
    sqlalchemy2-stubs.


.. change::
    :tags: bug, mypy
    :tickets: 9102
    :versions: 2.0.0rc3

    Fixed crash in mypy plugin which could occur on both 1.4 and 2.0 versions
    if a decorator for the :func:`_orm.registry.mapped` decorator were used
    that was referenced in an expression with more than two components (e.g.
    ``@Backend.mapper_registry.mapped``). This scenario is now ignored; when
    using the plugin, the decorator expression needs to be two components (i.e.
    ``@reg.mapped``).
