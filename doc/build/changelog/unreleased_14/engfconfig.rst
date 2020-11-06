.. change::
    :tags: bug, engine

    Added the "future" keyword to the list of words that are known by the
    :func:`_sa.engine_from_config` function, so that the values "true" and
    "false" may be configured as "boolean" values when using a key such
    as ``sqlalchemy.future = true`` or ``sqlalchemy.future = false``.

