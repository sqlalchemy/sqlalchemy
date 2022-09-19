.. change::
    :tags: usecase, orm

    Added :paramref:`_orm.load_only.raiseload` parameter to the
    :func:`_orm.load_only` loader option, so that the unloaded attributes may
    have "raise" behavior rather than lazy loading. Previously there wasn't
    really a way to do this with the :func:`_orm.load_only` option directly.
