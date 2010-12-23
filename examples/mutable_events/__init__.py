"""
Illustrates how to build and use "mutable" types, such as dictionaries and
user-defined classes, as scalar attributes which detect in-place changes.

The example is based around the usage of the event model introduced in
:ref:`event_toplevel`, along with the :func:`attributes.flag_modified` function
which establishes the "dirty" flag on a particular mapped attribute.  These
functions are encapsulated in a mixin called ``TrackMutationsMixin``. 
Subclassing ``dict`` to provide "mutation tracking" looks like::

    class MutationDict(TrackMutationsMixin, dict):
        def __init__(self, other):
            self.update(other)
        
        def __setitem__(self, key, value):
            dict.__setitem__(self, key, value)
            self.on_change()
    
        def __delitem__(self, key):
            dict.__delitem__(self, key)
            self.on_change()

    Base = declarative_base()
    class Foo(Base):
        __tablename__ = 'foo'
        id = Column(Integer, primary_key=True)
        data = Column(JSONEncodedDict)

    MutationDict.listen(Foo.data)

"""