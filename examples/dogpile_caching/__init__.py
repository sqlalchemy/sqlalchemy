"""
Illustrates how to embed `dogpile.cache <http://dogpilecache.readthedocs.org/>`_
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

The demo scripts themselves, in order of complexity, are run as follows::

   python examples/dogpile_caching/helloworld.py

   python examples/dogpile_caching/relationship_caching.py

   python examples/dogpile_caching/advanced.py

   python examples/dogpile_caching/local_session_caching.py


Listing of files:

    environment.py - Establish the Session, a dictionary
    of "regions", a sample cache region against a .dbm
    file, data / cache file paths, and configurations,
    bootstrap fixture data if necessary.

    caching_query.py - Represent functions and classes
    which allow the usage of Dogpile caching with SQLAlchemy.
    Introduces a query option called FromCache.

    model.py - The datamodel, which represents Person that has multiple
    Address objects, each with PostalCode, City, Country

    fixture_data.py - creates demo PostalCode, Address, Person objects
    in the database.

    helloworld.py - the basic idea.

    relationship_caching.py - Illustrates how to add cache options on
    relationship endpoints, so that lazyloads load from cache.

    advanced.py - Further examples of how to use FromCache.  Combines
    techniques from the first two scripts.

    local_session_caching.py - Grok everything so far ?   This example
    creates a new dogpile.cache backend that will persist data in a dictionary
    which is local to the current session.   remove() the session
    and the cache is gone.

"""
