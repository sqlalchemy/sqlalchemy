.. change::
    :tags: engine, feature

    The :meth:`.ConnectionEvents.set_connection_execution_options`
    and :meth:`.ConnectionEvents.set_engine_execution_options`
    event hooks now allow the given options dictionary to be modified
    in-place, where the new contents will be received as the ultimate
    execution options to be acted upon. Previously, in-place modifications to
    the dictionary were not supported.
