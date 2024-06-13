.. change::
    :tags: usecase, engine

    Modified the internal representation used for adapting asyncio calls to
    greenlets to allow for duck-typed compatibility with third party libraries
    that implement SQLAlchemy's "greenlet-to-asyncio" pattern directly.
    Running code within a greenlet that features the attribute
    ``__sqlalchemy_greenlet_provider__ = True`` will allow calls to
    :func:`sqlalchemy.util.await_only` directly.

