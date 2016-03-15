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

Rationale
---------

The "lambda" approach above is a superset of what would be a more
traditional "parameterized" approach.   Suppose we wished to build
a simple system where we build a :class:`~.query.Query` just once, then
store it in a dictionary for re-use.   This is possible right now by
just building up the query, and removing its :class:`.Session` by calling
``my_cached_query = query.with_session(None)``::

    my_simple_cache = {}

    def lookup(session, id_argument):
        if "my_key" not in my_simple_cache:
            query = session.query(Model).filter(Model.id == bindparam('id'))
            my_simple_cache["my_key"] = query.with_session(None)
        else:
            query = my_simple_cache["my_key"].with_session(session)

        return query.params(id=id_argument).all()

The above approach gets us a very minimal performance benefit.
By re-using a :class:`~.query.Query`, we save on the Python work within
the ``session.query(Model)`` constructor as well as calling upon
``filter(Model.id == bindparam('id'))``, which will skip for us the building
up of the Core expression as well as sending it to :meth:`.Query.filter`.
However, the approach still regenerates the full :class:`.Select`
object every time when :meth:`.Query.all` is called and additionally this
brand new :class:`.Select` is sent off to the string compilation step every
time, which for a simple case like the above is probably about 70% of the
overhead.

To reduce the additional overhead, we need some more specialized logic,
some way to memoize the construction of the select object and the
construction of the SQL.  There is an example of this on the wiki
in the section `BakedQuery <https://bitbucket.org/zzzeek/sqlalchemy/wiki/UsageRecipes/BakedQuery>`_,
a precursor to this feature, however in that system, we aren't caching
the *construction* of the query.  In order to remove all the overhead,
we need to cache both the construction of the query as well as the SQL
compilation.  Let's assume we adapted the recipe in this way
and made ourselves a method ``.bake()`` that pre-compiles the SQL for the
query, producing a new object that can be invoked with minimal overhead.
Our example becomes::

    my_simple_cache = {}

    def lookup(session, id_argument):

        if "my_key" not in my_simple_cache:
            query = session.query(Model).filter(Model.id == bindparam('id'))
            my_simple_cache["my_key"] = query.with_session(None).bake()
        else:
            query = my_simple_cache["my_key"].with_session(session)

        return query.params(id=id_argument).all()

Above, we've fixed the performance situation, but we still have this
string cache key to deal with.

We can use the "bakery" approach to re-frame the above in a way that
looks less unusual than the "building up lambdas" approach, and more like
a simple improvement upon the simple "reuse a query" approach::

    bakery = baked.bakery()

    def lookup(session, id_argument):
        def create_model_query(session):
            return session.query(Model).filter(Model.id == bindparam('id'))

        parameterized_query = bakery.bake(create_model_query)
        return parameterized_query(session).params(id=id_argument).all()

Above, we use the "baked" system in a manner that is
very similar to the simplistic "cache a query" system.  However, it
uses two fewer lines of code, does not need to manufacture a cache key of
"my_key", and also includes the same feature as our custom "bake" function
that caches 100% of the Python invocation work from the
constructor of the query, to the filter call, to the production
of the :class:`.Select` object, to the string compilation step.

From the above, if we ask ourselves, "what if lookup needs to make conditional decisions
as to the structure of the query?", this is where hopefully it becomes apparent
why "baked" is the way it is.   Instead of a parameterized query building
off from exactly one function (which is how we thought baked might work
originally), we can build it from *any number* of functions.  Consider
our naive example, if we needed to have an additional clause in our
query on a conditional basis::

    my_simple_cache = {}

    def lookup(session, id_argument, include_frobnizzle=False):
        if include_frobnizzle:
            cache_key = "my_key_with_frobnizzle"
        else:
            cache_key = "my_key_without_frobnizzle"

        if cache_key not in my_simple_cache:
            query = session.query(Model).filter(Model.id == bindparam('id'))
            if include_frobnizzle:
                query = query.filter(Model.frobnizzle == True)

            my_simple_cache[cache_key] = query.with_session(None).bake()
        else:
            query = my_simple_cache[cache_key].with_session(session)

        return query.params(id=id_argument).all()

Our "simple" parameterized system must now be tasked with generating
cache keys which take into account whether or not the "include_frobnizzle"
flag was passed, as the presence of this flag means that the generated
SQL would be entirely different.   It should be apparent that as the
complexity of query building goes up, the task of caching these queries
becomes burdensome very quickly.   We can convert the above example
into a direct use of "bakery" as follows::


    bakery = baked.bakery()

    def lookup(session, id_argument, include_frobnizzle=False):
        def create_model_query(session):
            return session.query(Model).filter(Model.id == bindparam('id'))

        parameterized_query = bakery.bake(create_model_query)

        if include_frobnizzle:
            def include_frobnizzle_in_query(query):
                return query.filter(Model.frobnizzle == True)

            parameterized_query = parameterized_query.with_criteria(
                include_frobnizzle_in_query)

        return parameterized_query(session).params(id=id_argument).all()

Above, we again cache not just the query object but all the work it needs
to do in order to generate SQL.  We also no longer need to deal with
making sure we generate a cache key that accurately takes into account
all of the structural modifications we've made; this is now handled
automatically and without the chance of mistakes.

This code sample is a few lines shorter than the naive example, removes
the need to deal with cache keys, and has the vast performance benefits
of the full so-called "baked" feature.  But
still a little verbose!  Hence we take methods like :meth:`.BakedQuery.add_criteria`
and :meth:`.BakedQuery.with_criteria` and shorten them into operators, and
encourage (though certainly not require!) using simple lambdas, only as a
means to reduce verbosity::

    bakery = baked.bakery()

    def lookup(session, id_argument, include_frobnizzle=False):
        parameterized_query = bakery.bake(
            lambda s: s.query(Model).filter(Model.id == bindparam('id'))
          )

        if include_frobnizzle:
            parameterized_query += lambda q: q.filter(Model.frobnizzle == True)

        return parameterized_query(session).params(id=id_argument).all()

Where above, the approach is simpler to implement and much more similar
in code flow to what a non-cached querying function would look like,
hence making code easier to port.

The above description is essentially a summary of the design process used
to arrive at the current "baked" approach.   Starting from the
"normal" approaches, the additional issues of cache key construction and
management,  removal of all redundant Python execution, and queries built up
with conditionals needed to be addressed, leading to the final approach.

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

Opting out with the bake_queries flag
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The :func:`.relationship` construct includes a flag
:paramref:`.relationship.bake_queries` which when set to False will cause
that relationship to opt out of the baked query system, when the
application-wide :func:`.bake_lazy_loaders` function has been called to enable
baked query loaders by default.

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
