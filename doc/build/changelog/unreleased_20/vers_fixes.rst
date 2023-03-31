.. change::
    :tags: bug, examples

    Fixed issue in "versioned history" example where using a declarative base
    that is derived from :class:`_orm.DeclarativeBase` would fail to be mapped.
    Additionally, repaired the given test suite so that the documented
    instructions for running the example using Python unittest now work again.
