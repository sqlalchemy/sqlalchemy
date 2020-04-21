.. change::
    :tags: feature, core
    :tickets: 5087, 4395, 4959

    Implemented an all-new :class:`.Result` object that replaces the previous
    ``ResultProxy`` object.   As implemented in Core, the subclass
    :class:`.CursorResult` features a compatible calling interface with the
    previous ``ResultProxy``, and additionally adds a great amount of new
    functionality that can be applied to Core result sets as well as ORM result
    sets, which are now integrated into the same model.   :class:`.Result`
    includes features such as column selection and rearrangement, improved
    fetchmany patterns, uniquing, as well as a variety of implementations that
    can be used to create database results from in-memory structures as well.


    .. seealso::

        :ref:`change_result_14_core`

