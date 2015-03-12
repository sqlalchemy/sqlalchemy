.. _baked_toplevel:

Baked Queries
=============

.. module:: sqlalchemy.ext.baked

``baked`` provides an alternative creational pattern for
:class:`~.query.Query` objects, which allows for caching of the object's
construction and string-compilation steps.  This means that for a
particular :class:`~.query.Query` building scenario that is used more than
once, all of the Python function invocation involved in building the query
from its initial construction up through generating a SQL string will only
occur **once**, rather than for each time that query is built up and executed.

The rationale for this system is to greatly reduce Python interpreter
overhead for everything that occurs **before the SQL is emitted**.
The caching of the "baked" system does **not** in any way reduce SQL calls or
cache the **return results** from the database.  A technique that demonstates
the caching of the SQL calls and result sets themselves is available in
:ref:`examples_caching`.


.. versionadded:: 1.0.0

.. note::

    The :mod:`sqlalchemy.ext.baked` extension should be considered
    **experimental** as of 1.0.0.  It provides a dramatically different system
    of producing queries which has yet to be proven at scale.

Synopsis
--------

Usage of the baked system starts by producing a so-called "bakery", which
represents storage for a particular series of query objects::

    from sqlalchemy.ext import baked

    bakery = baked.bakery()

The above "bakery" will store cached data in an LRU cache that defaults
to 200 elements, noting that an ORM query will typically contain one entry
for the ORM query as invoked, as well as one entry per database dialect for
the SQL string.

The bakery allows us to build up a :class:`~.query.Query` object by specifying
its construction as a series of Python callables, which are typically lambdas.
For succinct usage, it overrides the ``+=`` operator so that a typical
query build-up looks like the following::

    from sqlalchemy import bindparam

    def search_for_user(session, username, email=None):

        baked_query = bakery(lambda session: session.query(User))
        baked_query += lambda q: q.filter(User.name == bindparam('username'))

        baked_query += lambda q: q.order_by(User.id)

        if email:
            baked_query += lambda q: q.filter(User.email == bindparam('email'))

        result = baked_query(session).params(username=username, email=email).all()

        return result

Following are some observations about the above code:

1. The ``baked_query`` object is an instance of :class:`.BakedQuery`.  This
   object is essentially the "builder" for a real orm :class:`~.query.Query`
   object, but it is not itself the *actual* :class:`~.query.Query`
   object.

2. The actual :class:`~.query.Query` object is not built at all, until the
   very end of the function when :meth:`.Result.all` is called.

3. The steps that are added to the ``baked_query`` object are all expressed
   as Python functions,  typically lambdas.  The first lambda given
   to the :func:`.bakery` function receives a :class:`.Session` as its
   argument.  The remaining lambdas each receive a :class:`~.query.Query`
   as their argument.

4. In the above code, even though our application may call upon
   ``search_for_user()`` many times, and even though within each invocation
   we build up an entirely new :class:`.BakedQuery` object,
   *all of the lambdas are only called once*.   Each lambda is **never** called
   a second time for as long as this query is cached in the bakery.

5. The caching is achieved by storing references to the **lambda objects
   themselves** in order to formulate a cache key; that is, the fact that the
   Python interpreter assigns an in-Python identity to these functions is
   what determines how to identify the query on successive runs. For
   those invocations of ``search_for_user()`` where the ``email`` parameter
   is specified, the callable ``lambda q: q.filter(User.email == bindparam('email'))``
   will be part of the cache key that's retrieved; when ``email`` is
   ``None``, this callable is not part of the cache key.

6. Because the lambdas are all called only once, it is essential that no
   variables which may change across calls are referenced **within** the
   lambdas; instead, assuming these are values to be bound into the
   SQL string, we use :func:`.bindparam` to construct named parameters,
   where we apply their actual values later using :meth:`.Result.params`.

Performance
-----------

The baked query probably looks a little odd, a little bit awkward and
a little bit verbose.   However, the savings in
Python performance for a query which is invoked lots of times in an
application are very dramatic.   The example suite ``short_selects``
demonstrated in :ref:`examples_performance` illustrates a comparison
of queries which each return only one row, such as the following regular
query::

    session = Session(bind=engine)
    for id_ in random.sample(ids, n):
        session.query(Customer).filter(Customer.id == id_).one()

compared to the equivalent "baked" query::

    bakery = baked.bakery()
    s = Session(bind=engine)
    for id_ in random.sample(ids, n):
        q = bakery(lambda s: s.query(Customer))
        q += lambda q: q.filter(Customer.id == bindparam('id'))
        q(s).params(id=id_).one()

The difference in Python function call count for an iteration of 10000
calls to each block are::

    test_baked_query : test a baked query of the full entity.
                       (10000 iterations); total fn calls 1951294

    test_orm_query :   test a straight ORM query of the full entity.
                       (10000 iterations); total fn calls 7900535

In terms of number of seconds on a powerful laptop, this comes out as::

    test_baked_query : test a baked query of the full entity.
                       (10000 iterations); total time 2.174126 sec

    test_orm_query :   test a straight ORM query of the full entity.
                       (10000 iterations); total time 7.958516 sec

Note that this test very intentionally features queries that only return one row.
For queries that return many rows, the performance advantage of the baked query will have
less and less of an impact, proportional to the time spent fetching rows.
It is critical to keep in mind that the **baked query feature only applies to
building the query itself, not the fetching of results**.  Using the
baked feature is by no means a guarantee to a much faster application; it is
only a potentially useful feature for those applications that have been measured
as being impacted by this particular form of overhead.

.. topic:: Measure twice, cut once

    For background on how to profile a SQLAlchemy application, please see
    the section :ref:`faq_performance`.  It is essential that performance
    measurement techniques are used when attempting to improve the performance
    of an application.


Lazy Loading Integration
------------------------

The baked query can be integrated with SQLAlchemy's lazy loader feature
transparently.   A future release of SQLAlchemy may enable this by default,
as its use within lazy loading is completely transparent.    For now,
to enable baked lazyloading for all lazyloaders systemwide, call upon
the :func:`.bake_lazy_loaders` function.   This will impact all relationships
that use the ``lazy='select'`` strategy as well as all use of the :func:`.lazyload`
per-query strategy.

"Baked" lazy loading may be enabled on a per-:func:`.relationship` basis
using the ``baked_select`` loader strategy::

    class MyClass(Base):
        # ...

        widgets = relationship("Widget", lazy="baked_select")

The ``baked_select`` strategy is available once any part of the application
has imported the ``sqlalchemy.ext.baked`` module.   The "bakery" used by
this feature is local to the mapper for ``MyClass``.

For per-query use, the :func:`.baked_lazyload` strategy may be used,
which works like any other loader option.


API Documentation
-----------------

.. autofunction:: bakery

.. autoclass:: BakedQuery
    :members:

.. autoclass:: Result
    :members:

.. autofunction:: bake_lazy_loaders

.. autofunction:: unbake_lazy_loaders

.. autofunction:: baked_lazyload

.. autofunction:: baked_lazyload_all
