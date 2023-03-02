.. change::
    :tags: bug, typing

    Fixed bug where the :meth:`_engine.Connection.scalars` method was not typed
    as allowing a multiple-parameters list, which is now supported using
    insertmanyvalues operations.
