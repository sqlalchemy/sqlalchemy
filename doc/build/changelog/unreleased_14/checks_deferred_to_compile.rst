.. change::
    :tags: change, orm, sql

    A selection of Core and ORM query objects now perform much more of their
    Python computational tasks within the compile step, rather than at
    construction time.  This is to support an upcoming caching model that will
    provide for caching of the compiled statement structure based on a cache
    key that is derived from the statement construct, which itself is expected
    to be newly constructed in Python code each time it is used.    This means
    that the internal state of these objects may not be the same as it used to
    be, as well as that some but not all error raise scenarios for various
    kinds of argument validation will occur within the compilation / execution
    phase, rather than at statement construction time.   See the migration
    notes linked below for complete details.

    .. seealso::

        :ref:`change_deferred_construction`

