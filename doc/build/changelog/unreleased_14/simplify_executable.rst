.. change::
    :tags: bug, orm

    Fixed an issue where the API to create a custom executable SQL construct
    using the ``sqlalchemy.ext.compiles`` extension according to the
    documentation that's been up for many years would no longer function if
    only ``Executable, ClauseElement`` were used as the base classes,
    additional classes were needed if wanting to use
    :meth:`_orm.Session.execute`. This has been resolved so that those extra
    classes aren't needed.
