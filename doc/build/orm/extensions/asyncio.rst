.. _asyncio_toplevel:

Asynchronous I/O (asyncio)
==========================

Support for Python asyncio.    Support for Core and ORM usage is
included, using asyncio-compatible dialects.

.. versionadded:: 1.4

The asyncio extension requires at least Python version 3.6.

.. note:: The asyncio extension as of SQLAlchemy 1.4.3 can now be considered to
   be **beta level** software. API details are subject to change however at this
   point it is unlikely for there to be significant backwards-incompatible
   changes.


.. seealso::

    :ref:`change_3414` - initial feature announcement

    :ref:`examples_asyncio` - example scripts illustrating working examples
    of Core and ORM use within the asyncio extension.

Synopsis - Core
---------------

For Core use, the :func:`_asyncio.create_async_engine` function creates an
instance of :class:`_asyncio.AsyncEngine` which then offers an async version of
the traditional :class:`_engine.Engine` API.   The
:class:`_asyncio.AsyncEngine` delivers an :class:`_asyncio.AsyncConnection` via
its :meth:`_asyncio.AsyncEngine.connect` and :meth:`_asyncio.AsyncEngine.begin`
methods which both deliver asynchronous context managers.   The
:class:`_asyncio.AsyncConnection` can then invoke statements using either the
:meth:`_asyncio.AsyncConnection.execute` method to deliver a buffered
:class:`_engine.Result`, or the :meth:`_asyncio.AsyncConnection.stream` method
to deliver a streaming server-side :class:`_asyncio.AsyncResult`::

    import asyncio

    from sqlalchemy.ext.asyncio import create_async_engine

    async def async_main():
        engine = create_async_engine(
            "postgresql+asyncpg://scott:tiger@localhost/test", echo=True,
        )

        async with engine.begin() as conn:
            await conn.run_sync(meta.drop_all)
            await conn.run_sync(meta.create_all)

            await conn.execute(
                t1.insert(), [{"name": "some name 1"}, {"name": "some name 2"}]
            )

        async with engine.connect() as conn:

            # select a Result, which will be delivered with buffered
            # results
            result = await conn.execute(select(t1).where(t1.c.name == "some name 1"))

            print(result.fetchall())


    asyncio.run(async_main())

Above, the :meth:`_asyncio.AsyncConnection.run_sync` method may be used to
invoke special DDL functions such as :meth:`_schema.MetaData.create_all` that
don't include an awaitable hook.

The :class:`_asyncio.AsyncConnection` also features a "streaming" API via
the :meth:`_asyncio.AsyncConnection.stream` method that returns an
:class:`_asyncio.AsyncResult` object.  This result object uses a server-side
cursor and provides an async/await API, such as an async iterator::

    async with engine.connect() as conn:
        async_result = await conn.stream(select(t1))

        async for row in async_result:
            print("row: %s" % (row, ))


Synopsis - ORM
---------------

Using :term:`2.0 style` querying, the :class:`_asyncio.AsyncSession` class
provides full ORM functionality. Within the default mode of use, special care
must be taken to avoid :term:`lazy loading` or other expired-attribute access
involving ORM relationships and column attributes; the next
section :ref:`asyncio_orm_avoid_lazyloads` details this.   The example below
illustrates a complete example including mapper and session configuration::

    import asyncio

    from sqlalchemy import Column
    from sqlalchemy import DateTime
    from sqlalchemy import ForeignKey
    from sqlalchemy import func
    from sqlalchemy import Integer
    from sqlalchemy import String
    from sqlalchemy.ext.asyncio import AsyncSession
    from sqlalchemy.ext.asyncio import create_async_engine
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.future import select
    from sqlalchemy.orm import relationship
    from sqlalchemy.orm import selectinload
    from sqlalchemy.orm import sessionmaker

    Base = declarative_base()


    class A(Base):
        __tablename__ = "a"

        id = Column(Integer, primary_key=True)
        data = Column(String)
        create_date = Column(DateTime, server_default=func.now())
        bs = relationship("B")

        # required in order to access columns with server defaults
        # or SQL expression defaults, subsequent to a flush, without
        # triggering an expired load
        __mapper_args__ = {"eager_defaults": True}


    class B(Base):
        __tablename__ = "b"
        id = Column(Integer, primary_key=True)
        a_id = Column(ForeignKey("a.id"))
        data = Column(String)


    async def async_main():
        engine = create_async_engine(
            "postgresql+asyncpg://scott:tiger@localhost/test",
            echo=True,
        )

        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)
            await conn.run_sync(Base.metadata.create_all)

        # expire_on_commit=False will prevent attributes from being expired
        # after commit.
        async_session = sessionmaker(
            engine, expire_on_commit=False, class_=AsyncSession
        )

        async with async_session() as session:
            async with session.begin():
                session.add_all(
                    [
                        A(bs=[B(), B()], data="a1"),
                        A(bs=[B()], data="a2"),
                        A(bs=[B(), B()], data="a3"),
                    ]
                )

            stmt = select(A).options(selectinload(A.bs))

            result = await session.execute(stmt)

            for a1 in result.scalars():
                print(a1)
                print(f"created at: {a1.create_date}")
                for b1 in a1.bs:
                    print(b1)

            result = await session.execute(select(A).order_by(A.id))

            a1 = result.scalars().first()

            a1.data = "new data"

            await session.commit()

            # access attribute subsequent to commit; this is what
            # expire_on_commit=False allows
            print(a1.data)


    asyncio.run(async_main())

In the example above, the :class:`_asyncio.AsyncSession` is instantiated using
the optional :class:`_orm.sessionmaker` helper, and associated with an
:class:`_asyncio.AsyncEngine` against particular database URL. It is
then used in a Python asynchronous context manager (i.e. ``async with:``
statement) so that it is automatically closed at the end of the block; this is
equivalent to calling the :meth:`_asyncio.AsyncSession.close` method.

.. _asyncio_orm_avoid_lazyloads:

Preventing Implicit IO when Using AsyncSession
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Using traditional asyncio, the application needs to avoid any points at which
IO-on-attribute access may occur. Above, the following measures are taken to
prevent this:

* The :func:`_orm.selectinload` eager loader is employed in order to eagerly
  load the ``A.bs`` collection within the scope of the
  ``await session.execute()`` call::

      stmt = select(A).options(selectinload(A.bs))

  ..

  If the default loader strategy of "lazyload" were left in place, the access
  of the ``A.bs`` attribute would raise an asyncio exception.
  There are a variety of ORM loader options available, which may be configured
  at the default mapping level or used on a per-query basis, documented at
  :ref:`loading_toplevel`.


* The :class:`_asyncio.AsyncSession` is configured using
  :paramref:`_orm.Session.expire_on_commit` set to False, so that we may access
  attributes on an object subsequent to a call to
  :meth:`_asyncio.AsyncSession.commit`, as in the line at the end where we
  access an attribute::

      # create AsyncSession with expire_on_commit=False
      async_session = AsyncSession(engine, expire_on_commit=False)

      # sessionmaker version
      async_session = sessionmaker(
          engine, expire_on_commit=False, class_=AsyncSession
      )

      async with async_session() as session:

          result = await session.execute(select(A).order_by(A.id))

          a1 = result.scalars().first()

          # commit would normally expire all attributes
          await session.commit()

          # access attribute subsequent to commit; this is what
          # expire_on_commit=False allows
          print(a1.data)

* The :paramref:`_schema.Column.server_default` value on the ``created_at``
  column will not be refreshed by default after an INSERT; instead, it is
  normally
  :ref:`expired so that it can be loaded when needed <orm_server_defaults>`.
  Similar behavior applies to a column where the
  :paramref:`_schema.Column.default` parameter is assigned to a SQL expression
  object. To access this value with asyncio, it has to be refreshed within the
  flush process, which is achieved by setting the
  :paramref:`_orm.mapper.eager_defaults` parameter on the mapping::


    class A(Base):
        # ...

        # column with a server_default, or SQL expression default
        create_date = Column(DateTime, server_default=func.now())

        # add this so that it can be accessed
        __mapper_args__ = {"eager_defaults": True}

Other guidelines include:

* Methods like :meth:`_asyncio.AsyncSession.expire` should be avoided in favor of
  :meth:`_asyncio.AsyncSession.refresh`

* Avoid using the ``all`` cascade option documented at :ref:`unitofwork_cascades`
  in favor of listing out the desired cascade features explicitly.   The
  ``all`` cascade option implies among others the :ref:`cascade_refresh_expire`
  setting, which means that the :meth:`.AsyncSession.refresh` method will
  expire the attributes on related objects, but not necessarily refresh those
  related objects assuming eager loading is not configured within the
  :func:`_orm.relationship`, leaving them in an expired state.   A future
  release may introduce the ability to indicate eager loader options when
  invoking :meth:`.Session.refresh` and/or :meth:`.AsyncSession.refresh`.

* Appropriate loader options should be employed for :func:`_orm.deferred`
  columns, if used at all, in addition to that of :func:`_orm.relationship`
  constructs as noted above.  See :ref:`deferred` for background on
  deferred column loading.

* The "dynamic" relationship loader strategy described at
  :ref:`dynamic_relationship` is not compatible with the asyncio approach and
  cannot be used, unless invoked within the
  :meth:`_asyncio.AsyncSession.run_sync` method described at
  :ref:`session_run_sync`.

.. _session_run_sync:

Running Synchronous Methods and Functions under asyncio
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. deepalchemy::  This approach is essentially exposing publicly the
   mechanism by which SQLAlchemy is able to provide the asyncio interface
   in the first place.   While there is no technical issue with doing so, overall
   the approach can probably be considered "controversial" as it works against
   some of the central philosophies of the asyncio programming model, which
   is essentially that any programming statement that can potentially result
   in IO being invoked **must** have an ``await`` call, lest the program
   does not make it explicitly clear every line at which IO may occur.
   This approach does not change that general idea, except that it allows
   a series of synchronous IO instructions to be exempted from this rule
   within the scope of a function call, essentially bundled up into a single
   awaitable.

As an alternative means of integrating traditional SQLAlchemy "lazy loading"
within an asyncio event loop, an **optional** method known as
:meth:`_asyncio.AsyncSession.run_sync` is provided which will run any
Python function inside of a greenlet, where traditional synchronous
programming concepts will be translated to use ``await`` when they reach the
database driver.   A hypothetical approach here is an asyncio-oriented
application can package up database-related methods into functions that are
invoked using :meth:`_asyncio.AsyncSession.run_sync`.

Altering the above example, if we didn't use :func:`_orm.selectinload`
for the ``A.bs`` collection, we could accomplish our treatment of these
attribute accesses within a separate function::

    import asyncio

    from sqlalchemy.ext.asyncio import create_async_engine
    from sqlalchemy.ext.asyncio import AsyncSession

    def fetch_and_update_objects(session):
        """run traditional sync-style ORM code in a function that will be
        invoked within an awaitable.

        """

        # the session object here is a traditional ORM Session.
        # all features are available here including legacy Query use.

        stmt = select(A)

        result = session.execute(stmt)
        for a1 in result.scalars():
            print(a1)

            # lazy loads
            for b1 in a1.bs:
                print(b1)

        # legacy Query use
        a1 = session.query(A).order_by(A.id).first()

        a1.data = "new data"


    async def async_main():
        engine = create_async_engine(
            "postgresql+asyncpg://scott:tiger@localhost/test", echo=True,
        )
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)
            await conn.run_sync(Base.metadata.create_all)

        async with AsyncSession(engine) as session:
            async with session.begin():
                session.add_all(
                    [
                        A(bs=[B(), B()], data="a1"),
                        A(bs=[B()], data="a2"),
                        A(bs=[B(), B()], data="a3"),
                    ]
                )

            await session.run_sync(fetch_and_update_objects)

            await session.commit()

    asyncio.run(async_main())

The above approach of running certain functions within a "sync" runner
has some parallels to an application that runs a SQLAlchemy application
on top of an event-based programming library such as ``gevent``.  The
differences are as follows:

1. unlike when using ``gevent``, we can continue to use the standard Python
   asyncio event loop, or any custom event loop, without the need to integrate
   into the ``gevent`` event loop.

2. There is no "monkeypatching" whatsoever.   The above example makes use of
   a real asyncio driver and the underlying SQLAlchemy connection pool is also
   using the Python built-in ``asyncio.Queue`` for pooling connections.

3. The program can freely switch between async/await code and contained
   functions that use sync code with virtually no performance penalty.  There
   is no "thread executor" or any additional waiters or synchronization in use.

4. The underlying network drivers are also using pure Python asyncio
   concepts, no third party networking libraries as ``gevent`` and ``eventlet``
   provides are in use.

Using multiple asyncio event loops
----------------------------------

An application that makes use of multiple event loops, for example by combining asyncio
with multithreading, should not share the same :class:`_asyncio.AsyncEngine`
with different event loops when using the default pool implementation.

If an :class:`_asyncio.AsyncEngine` is be passed from one event loop to another,
the method :meth:`_asyncio.AsyncEngine.dispose()` should be called before it's
re-used on a new event loop. Failing to do so may lead to a ``RuntimeError``
along the lines of
``Task <Task pending ...> got Future attached to a different loop``

If the same engine must be shared between different loop, it should be configured
to disable pooling using :class:`~sqlalchemy.pool.NullPool`, preventing the Engine
from using any connection more than once::

    from sqlalchemy.pool import NullPool
    engine = create_async_engine(
        "postgresql+asyncpg://user:pass@host/dbname", poolclass=NullPool
    )


.. currentmodule:: sqlalchemy.ext.asyncio

Engine API Documentation
-------------------------

.. autofunction:: create_async_engine

.. autoclass:: AsyncEngine
   :members:

.. autoclass:: AsyncConnection
   :members:

.. autoclass:: AsyncTransaction
   :members:

Result Set API Documentation
----------------------------------

The :class:`_asyncio.AsyncResult` object is an async-adapted version of the
:class:`_result.Result` object.  It is only returned when using the
:meth:`_asyncio.AsyncConnection.stream` or :meth:`_asyncio.AsyncSession.stream`
methods, which return a result object that is on top of an active database
cursor.

.. autoclass:: AsyncResult
   :members:

.. autoclass:: AsyncScalarResult
   :members:

.. autoclass:: AsyncMappingResult
   :members:

ORM Session API Documentation
-----------------------------

.. autoclass:: AsyncSession
   :members:

.. autoclass:: AsyncSessionTransaction
   :members:



