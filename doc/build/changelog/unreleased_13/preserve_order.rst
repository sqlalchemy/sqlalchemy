.. change::
    :tags: feature, orm

    Added new flag :paramref:`.Session.bulk_save_objects.preserve_order` to the
    :meth:`.Session.bulk_save_objects` method, which defaults to True. When set
    to False, the given mappings will be grouped into inserts and updates per
    each object type, to allow for greater opportunities to batch common
    operations together.  Pull request courtesy Alessandro Cucci.
