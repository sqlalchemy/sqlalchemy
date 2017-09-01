.. change::
    :tags: bug, oracle
    :tickets: 4064

    Partial support for persisting and retrieving the Oracle value
    "infinity" is implemented with cx_Oracle, using Python float values
    only, e.g. ``float("inf")``.  Decimal support is not yet fulfilled by
    the cx_Oracle DBAPI driver.

.. change::
    :tags: bug, oracle

    The cx_Oracle dialect has been reworked and modernized to take advantage of
    new patterns that weren't present in the old 4.x series of cx_Oracle. This
    includes that the minimum cx_Oracle version is the 5.x series and that
    cx_Oracle 6.x is now fully tested. The most significant change involves
    type conversions, primarily regarding the numeric / floating point and LOB
    datatypes, making more effective use of cx_Oracle type handling hooks to
    simplify how bind parameter and result data is processed.

    .. seealso::

        :ref:`change_cxoracle_12`

.. change::
    :tags: bug, oracle
    :tickets: 3997

    two phase support for cx_Oracle has been completely removed for all
    versions of cx_Oracle, whereas in 1.2.0b1 this change only took effect for
    the 6.x series of cx_Oracle.  This feature never worked correctly
    in any version of cx_Oracle and in cx_Oracle 6.x, the API which SQLAlchemy
    relied upon was removed.

    .. seealso::

        :ref:`change_cxoracle_12`

.. change::
    :tags: bug, oracle

    The column keys present in a result set when using :meth:`.Insert.returning`
    with the cx_Oracle backend now use the correct column / label names
    like that of all other dialects.  Previously, these came out as
    ``ret_nnn``.

    .. seealso::

        :ref:`change_cxoracle_12`

.. change::
    :tags: bug, oracle

    Several parameters to the cx_Oracle dialect are now deprecated and will
    have no effect: ``auto_setinputsizes``, ``exclude_setinputsizes``,
    ``allow_twophase``.

    .. seealso::

        :ref:`change_cxoracle_12`

