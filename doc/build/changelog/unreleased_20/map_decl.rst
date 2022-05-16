.. change::
    :tags: bug, orm

    Fixed issue where the :meth:`_orm.registry.map_declaratively` method
    would return an internal "mapper config" object and not the
    :class:`.Mapper` object as stated in the API documentation.
