"""
Illustrates how to embed Beaker cache functionality within
the Query object, allowing full cache control as well as the
ability to pull "lazy loaded" attributes from long term cache 
as well.

In this demo, the following techniques are illustrated:

* Using custom subclasses of Query
* Basic technique of circumventing Query to pull from a 
  custom cache source instead of the database.
* Rudimental caching with Beaker, using "regions" which allow
  global control over a fixed set of configurations.
* Using custom MapperOption objects to configure options on 
  a Query, including the ability to invoke the options 
  deep within an object graph when lazy loads occur.

E.g.::

    # query for Person objects, specifying cache
    q = Session.query(Person).options(FromCache("default", "all_people"))

    # specify that each Person's "addresses" collection comes from
    # cache too
    q = q.options(RelationshipCache("default", "by_person", Person.addresses))

    # query
    print q.all()

To run, both SQLAlchemy and Beaker (1.4 or greater) must be
installed or on the current PYTHONPATH. The demo will create a local
directory for datafiles, insert initial data, and run. Running the
demo a second time will utilize the cache files already present, and
exactly one SQL statement against two tables will be emitted - the
displayed result however will utilize dozens of lazyloads that all
pull from cache.

The demo scripts themselves, in order of complexity, are run as follows::

   python examples/beaker_caching/helloworld.py

   python examples/beaker_caching/relationship_caching.py

   python examples/beaker_caching/advanced.py

   python examples/beaker_caching/local_session_caching.py


Listing of files:

    environment.py - Establish the Session, the Beaker cache
    manager, data / cache file paths, and configurations, 
    bootstrap fixture data if necessary.

    caching_query.py - Represent functions and classes 
    which allow the usage of Beaker caching with SQLAlchemy.
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
    creates a new Beaker container that will persist data in a dictionary
    which is local to the current session.   remove() the session
    and the cache is gone.

"""
