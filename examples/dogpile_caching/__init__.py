"""
Illustrates how to embed
`dogpile.cache <https://dogpilecache.sqlalchemy.org/>`_
functionality with ORM queries, allowing full cache control
as well as the ability to pull "lazy loaded" attributes from long term cache.

In this demo, the following techniques are illustrated:

* Using the :meth:`_orm.SessionEvents.do_orm_execute` event hook
* Basic technique of circumventing :meth:`_orm.Session.execute` to pull from a
  custom cache source instead of the database.
* Rudimental caching with dogpile.cache, using "regions" which allow
  global control over a fixed set of configurations.
* Using custom :class:`.UserDefinedOption` objects to configure options in
  a statement object.

.. seealso::

    :ref:`do_orm_execute_re_executing` - includes a general example of the
    technique presented here.

E.g.::

    # query for Person objects, specifying cache
    stmt = select(Person).options(FromCache("default"))

    # specify that each Person's "addresses" collection comes from
    # cache too
    stmt = stmt.options(RelationshipCache(Person.addresses, "default"))

    # execute and results
    result = session.execute(stmt)

    print(result.scalars.all())

To run, both SQLAlchemy and dogpile.cache must be
installed or on the current PYTHONPATH. The demo will create a local
directory for datafiles, insert initial data, and run. Running the
demo a second time will utilize the cache files already present, and
exactly one SQL statement against two tables will be emitted - the
displayed result however will utilize dozens of lazyloads that all
pull from cache.

The demo scripts themselves, in order of complexity, are run as Python
modules so that relative imports work::

   python -m examples.dogpile_caching.helloworld

   python -m examples.dogpile_caching.relationship_caching

   python -m examples.dogpile_caching.advanced

   python -m examples.dogpile_caching.local_session_caching

.. autosource::
    :files: environment.py, caching_query.py, model.py, fixture_data.py, \
          helloworld.py, relationship_caching.py, advanced.py, \
          local_session_caching.py

"""
