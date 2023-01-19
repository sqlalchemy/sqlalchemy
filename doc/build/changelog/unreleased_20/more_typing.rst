.. change::
    :tags: typing, bug
    :tickets: 9122

    The :meth:`_sql.ColumnOperators.in_` and
    :meth:`_sql.ColumnOperators.not_in_` are typed to include
    ``Iterable[Any]`` rather than ``Sequence[Any]`` for more flexibility in
    argument type.


.. change::
    :tags: typing, bug
    :tickets: 9123

    The :func:`_sql.or_` and :func:`_sql.and_` from a typing perspective
    require the first argument to be present, however these functions still
    accept zero arguments which will emit a deprecation warning at runtime.
    Typing is also added to support sending the fixed literal ``False`` for
    :func:`_sql.or_` and ``True`` for :func:`_sql.and_` as the first argument
    only, however the documentation now indicates sending the
    :func:`_sql.false` and :func:`_sql.true` constructs in these cases as a
    more explicit approach.


.. change::
    :tags: typing, bug
    :tickets: 9125

    Fixed typing issue where iterating over a :class:`_orm.Query` object
    was not correctly typed. 
