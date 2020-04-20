"""
Illustrates an extension which creates version tables for entities and stores
records for each change. The given extensions generate an anonymous "history"
class which represents historical versions of the target object.

Compare to the :ref:`examples_versioned_rows` examples which write updates
as new rows in the same table, without using a separate history table.

Usage is illustrated via a unit test module ``test_versioning.py``, which can
be run via ``pytest``::

    # assume SQLAlchemy is installed where pytest is

    cd examples/versioned_history
    pytest test_versioning.py


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

The ``Versioned`` mixin is designed to work with declarative.  To use
the extension with classical mappers, the ``_history_mapper`` function
can be applied::

    from history_meta import _history_mapper

    m = mapper(SomeClass, sometable)
    _history_mapper(m)

    SomeHistoryClass = SomeClass.__history_mapper__.class_

The versioning example also integrates with the ORM optimistic concurrency
feature documented at :ref:`mapper_version_counter`.   To enable this feature,
set the flag ``Versioned.use_mapper_versioning`` to True::

    class SomeClass(Versioned, Base):
        __tablename__ = 'sometable'

        use_mapper_versioning = True

        id = Column(Integer, primary_key=True)
        name = Column(String(50))

        def __eq__(self, other):
            assert type(other) is SomeClass and other.id == self.id

Above, if two instance of ``SomeClass`` with the same version identifier
are updated and sent to the database for UPDATE concurrently, if the database
isolation level allows the two UPDATE statements to proceed, one will fail
because it no longer is against the last known version identifier.

.. autosource::

"""
