.. change::
    :tags: change, sql

    Removed the ``sqlalchemy.sql.visitors.iterate_depthfirst`` and
    ``sqlalchemy.sql.visitors.traverse_depthfirst`` functions.  These functions
    were unused by any part of SQLAlchemy.  The
    :func:`_sa.sql.visitors.iterate` and :func:`_sa.sql.visitors.traverse`
    functions are commonly used for these functions.  Also removed unused
    options from the remaining functions including "column_collections",
    "schema_visitor".

