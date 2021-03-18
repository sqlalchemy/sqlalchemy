.. change::
    :tags: bug, mypy
    :tickets: sqlalchemy/sqlalchemy2-stubs/2

    Fixed issue in MyPy extension which crashed on detecting the type of a
    :class:`.Column` if the type were given with a module prefix like
    ``sa.Integer()``.

