"""
Illustrates how to embed `dogpile.cache <https://dogpilecache.readthedocs.io/>`_
functionality within
the :class:`.Query` object, allowing full cache control as well as the
ability to pull "lazy loaded" attributes from long term cache
as well.

.. versionchanged:: 0.8 The example was modernized to use
   dogpile.cache, replacing Beaker as the caching library in
   use.

In this demo, the following techniques are illustrated:

* Using custom subclasses of :class:`.Query`
* Basic technique of circumventing Query to pull from a
  custom cache source instead of the database.
* Rudimental caching with dogpile.cache, using "regions" which allow
  global control over a fixed set of configurations.
* Using custom :class:`.MapperOption` objects to configure options on
  a Query, including the ability to invoke the options
  deep within an object graph when lazy loads occur.

E.g.::

    # query for Person objects, specifying cache
    q = Session.query(Person).options(FromCache("default"))

    # specify that each Person's "addresses" collection comes from
    # cache too
    q = q.options(RelationshipCache(Person.addresses, "default"))

    # query
    print q.all()

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
