.. change::
    :tags: engine, change

    The private method ``Connection._execute_compiled`` is removed.  This method may
    have been used for some special purposes however the :class:`.SQLCompiler`
    object has lots of special state that should be set up for an execute call,
    which we don't support.
