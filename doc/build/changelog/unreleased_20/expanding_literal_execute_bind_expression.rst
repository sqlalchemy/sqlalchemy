.. change::
    :tags: bug, sql
    :versions: 2.1.7

    Fixed issue where the values of an "expanding" parameter that also uses
    :paramref:`.bindparam.literal_execute`, such as that of an IN expression,
    were split on the ``", "`` separator after having been rendered as
    literals, when the datatype in use includes a
    :meth:`.TypeEngine.bind_expression`.  A value containing a comma followed
    by a space would be rendered as two fragments, each wrapped by the bind
    expression, producing an incorrect comparison.  The bind expression is
    now applied to each value as it is rendered rather than by splitting the
    rendered text.
