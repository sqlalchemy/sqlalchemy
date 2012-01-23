"""
Illustrates an extension which creates version tables for entities and stores
records for each change. The same idea as Elixir's versioned extension, but
more efficient (uses attribute API to get history) and handles class
inheritance. The given extensions generate an anonymous "history" class which
represents historical versions of the target object.

Usage is illustrated via a unit test module ``test_versioning.py``, which can
be run via nose::

    cd examples/versioning
    nosetests -v 

A fragment of example usage, using declarative::

    from history_meta import Versioned, versioned_session

    Base = declarative_base()

    class SomeClass(Versioned, Base):
        __tablename__ = 'sometable'

        id = Column(Integer, primary_key=True)
        name = Column(String(50))

        def __eq__(self, other):
            assert type(other) is SomeClass and other.id == self.id

    Session = sessionmaker(bind=engine)
    versioned_session(Session)

    sess = Session()
    sc = SomeClass(name='sc1')
    sess.add(sc)
    sess.commit()

    sc.name = 'sc1modified'
    sess.commit()

    assert sc.version == 2

    SomeClassHistory = SomeClass.__history_mapper__.class_

    assert sess.query(SomeClassHistory).\\
                filter(SomeClassHistory.version == 1).\\
                all() \\
                == [SomeClassHistory(version=1, name='sc1')]

The ``Versioned`` mixin is designed to work with declarative.  To use the extension with
classical mappers, the ``_history_mapper`` function can be applied::

    from history_meta import _history_mapper

    m = mapper(SomeClass, sometable)
    _history_mapper(m)

    SomeHistoryClass = SomeClass.__history_mapper__.class_

"""