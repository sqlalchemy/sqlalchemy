"""
Illustrates an extension which creates version tables for entities and stores records for each change.  The same idea as Elixir's versioned extension, but more efficient (uses attribute API to get history) and handles class inheritance.  The given extensions generate an anonymous "history" class which represents historical versions of the target object.   

Usage is illustrated via a unit test module ``test_versioning.py``, which can be run via nose::

    nosetests -w examples/versioning/

A fragment of example usage, using declarative::

    from history_meta import VersionedMeta, VersionedListener

    Base = declarative_base(metaclass=VersionedMeta, bind=engine)
    Session = sessionmaker(extension=VersionedListener())

    class SomeClass(Base):
        __tablename__ = 'sometable'
    
        id = Column(Integer, primary_key=True)
        name = Column(String(50))
    
        def __eq__(self, other):
            assert type(other) is SomeClass and other.id == self.id
        
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

To apply ``VersionedMeta`` to a subset of classes (probably more typical), the metaclass can be applied on a per-class basis::

    from history_meta import VersionedMeta, VersionedListener

    Base = declarative_base(bind=engine)

    class SomeClass(Base):
        __tablename__ = 'sometable'

        # ...

    class SomeVersionedClass(Base):
        __metaclass__ = VersionedMeta
        __tablename__ = 'someothertable'

        # ...

The ``VersionedMeta`` is a declarative metaclass - to use the extension with plain mappers, the ``_history_mapper`` function can be applied::

    from history_meta import _history_mapper

    m = mapper(SomeClass, sometable)
    _history_mapper(m)

    SomeHistoryClass = SomeClass.__history_mapper__.class_

"""