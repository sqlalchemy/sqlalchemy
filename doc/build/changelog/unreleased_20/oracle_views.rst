.. change::
    :tags: change, oracle
    :tickets:`4379`

    Materialized views on oracle are now reflected as views.
    On previous versions of SQLAlchemy the views were returned among
    the table names, not among the view names. As a side effect of
    this change they are not reflected by default by
    :meth:`_sql.MetaData.reflect`, unless ``views=True`` is set.
    To get a list of materialized views, use the new
    inspection method :meth:`.Inspector.get_materialized_view_names`.
