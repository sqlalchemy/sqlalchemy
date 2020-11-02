.. change::
    :tags: orm, performance

    The bulk update and delete methods :meth:`.Query.update` and
    :meth:`.Query.delete`, as well as their 2.0-style counterparts, now make
    use of RETURNING when the "fetch" strategy is used in order to fetch the
    list of affected primary key identites, rather than emitting a separate
    SELECT, when the backend in use supports RETURNING.  Additionally, the
    "fetch" strategy will in ordinary cases not expire the attributes that have
    been updated, and will instead apply the updated values directly in the
    same way that the "evaluate" strategy does, to avoid having to refresh the
    object.   The "evaluate" strategy will also fall back to expiring
    attributes that were updated to a SQL expression that was unevaluable in
    Python.

    .. seealso::

        :ref:`change_orm_update_returning_14`