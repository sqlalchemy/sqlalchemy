.. change::
    :tags: usecase, orm

    Added :attr:`_orm.ORMExecuteState.bind_mapper` and
    :attr:`_orm.ORMExecuteState.all_mappers` accessors to
    :class:`_orm.ORMExecuteState` event object, so that handlers can respond to
    the target mapper and/or mapped class or classes involved in an ORM
    statement execution.
