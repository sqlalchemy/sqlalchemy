 .. change::
    :tags: bug, typing
    :tickets: 6810

    Completed pep-484 typing for the ``sqlalchemy.sql.functions`` module.
    :func:`_sql.select` constructs made against ``func`` elements should now
    have filled-in return types.
