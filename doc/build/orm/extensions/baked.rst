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
cache the **return results** from the database.  A technique that demonstrates
the caching of the SQL calls and result sets themselves is available in
:ref:`examples_caching`.

.. deprecated:: 1.4  SQLAlchemy 1.4 and 2.0 feature an all-new direct query
   caching system that removes the need for the :class:`.BakedQuery` system.
   Caching is now transparently active for all Core and ORM queries with no
   action taken by the user, using the system described at :ref:`sql_caching`.


.. deepalchemy::

    The :mod:`sqlalchemy.ext.baked` extension is **not for beginners**.  Using
    it correctly requires a good high level understanding of how SQLAlchemy, the
    database driver, and the backend database interact with each other.  This
    extension presents a very specific kind of optimization that is not ordinarily
    needed.  As noted above, it **does not cache queries**, only the string
    formulation of the SQL itself.

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
   very end of the function when :meth:`_baked.Result.all` is called.

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
   where we apply their actual values later using :meth:`_baked.Result.params`.


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
up of the Core expression as well as sending it to :meth:`_query.Query.filter`.
However, the approach still regenerates the full :class:`_expression.Select`
object every time when :meth:`_query.Query.all` is called and additionally this
brand new :class:`_expression.Select` is sent off to the string compilation step every
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
of the :class:`_expression.Select` object, to the string compilation step.

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

Special Query Techniques
------------------------

This section will describe some techniques for specific query situations.

.. _baked_in:

Using IN expressions
^^^^^^^^^^^^^^^^^^^^

The :meth:`.ColumnOperators.in_` method in SQLAlchemy historically renders
a variable set of bound parameters based on the list of items that's passed
to the method.   This doesn't work for baked queries as the length of that
list can change on different calls.  To solve this problem, the
:paramref:`.bindparam.expanding` parameter supports a late-rendered IN
expression that is safe to be cached inside of baked query.  The actual list
of elements is rendered at statement execution time, rather than at
statement compilation time::

    bakery = baked.bakery()

    baked_query = bakery(lambda session: session.query(User))
    baked_query += lambda q: q.filter(
      User.name.in_(bindparam('username', expanding=True)))

    result = baked_query.with_session(session).params(
      username=['ed', 'fred']).all()

.. seealso::

  :paramref:`.bindparam.expanding`

  :meth:`.ColumnOperators.in_`

Using Subqueries
^^^^^^^^^^^^^^^^

When using :class:`_query.Query` objects, it is often needed that one :class:`_query.Query`
object is used to generate a subquery within another.   In the case where the
:class:`_query.Query` is currently in baked form, an interim method may be used to
retrieve the :class:`_query.Query` object, using the :meth:`.BakedQuery.to_query`
method.  This method is passed the :class:`.Session` or :class:`_query.Query` that is
the argument to the lambda callable used to generate a particular step
of the baked query::

    bakery = baked.bakery()

    # a baked query that will end up being used as a subquery
    my_subq = bakery(lambda s: s.query(User.id))
    my_subq += lambda q: q.filter(User.id == Address.user_id)

    # select a correlated subquery in the top columns list,
    # we have the "session" argument, pass that
    my_q = bakery(
      lambda s: s.query(Address.id, my_subq.to_query(s).as_scalar()))

    # use a correlated subquery in some of the criteria, we have
    # the "query" argument, pass that.
    my_q += lambda q: q.filter(my_subq.to_query(q).exists())

.. versionadded:: 1.3

.. _baked_with_before_compile:

Using the before_compile event
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

As of SQLAlchemy 1.3.11, the use of the :meth:`.QueryEvents.before_compile`
event against a particular :class:`_query.Query` will disallow the baked query
system from caching the query, if the event hook returns a new :class:`_query.Query`
object that is different from the one passed in.  This is so that the
:meth:`.QueryEvents.before_compile` hook may be invoked against a particular
:class:`_query.Query` every time it is used, to accommodate for hooks that
alter the query differently each time.    To allow a
:meth:`.QueryEvents.before_compile` to alter a :meth:`_query.Query` object, but
still to allow the result to be cached, the event can be registered
passing the ``bake_ok=True`` flag::

    @event.listens_for(
        Query, "before_compile", retval=True, bake_ok=True)
    def my_event(query):
        for desc in query.column_descriptions:
            if desc['type'] is User:
                entity = desc['entity']
                query = query.filter(entity.deleted == False)
        return query

The above strategy is appropriate for an event that will modify a
given :class:`_query.Query` in exactly the same way every time, not dependent
on specific parameters or external state that changes.

.. versionadded:: 1.3.11  - added the "bake_ok" flag to the
   :meth:`.QueryEvents.before_compile` event and disallowed caching via
   the "baked" extension from occurring for event handlers that
   return  a new :class:`_query.Query` object if this flag is not set.


Disabling Baked Queries Session-wide
------------------------------------

The flag :paramref:`.Session.enable_baked_queries` may be set to False,
causing all baked queries to not use the cache when used against that
:class:`.Session`::

    session = Session(engine, enable_baked_queries=False)

Like all session flags, it is also accepted by factory objects like
:class:`.sessionmaker` and methods like :meth:`.sessionmaker.configure`.

The immediate rationale for this flag is so that an application
which is seeing issues potentially due to cache key conflicts from user-defined
baked queries or other baked query issues can turn the behavior off, in
order to identify or eliminate baked queries as the cause of an issue.

.. versionadded:: 1.2

Lazy Loading Integration
------------------------

.. versionchanged:: 1.4 As of SQLAlchemy 1.4, the "baked query" system is no
   longer part of the relationship loading system.
   The :ref:`native caching <sql_caching>` system is used instead.


API Documentation
-----------------

.. autofunction:: bakery

.. autoclass:: BakedQuery
    :members:

.. autoclass:: Bakery
    :members:

.. autoclass:: Result
    :members:

