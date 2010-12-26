"""
Illustrates how to build and use "mutable" types, such as dictionaries and
user-defined classes, as scalar attributes which detect in-place changes.
These types don't make use of the "mutable=True" flag, which
performs poorly within the ORM and is being phased out, instead allowing
changes on data to associate change events with the parent object 
as they happen in the same way as any other mapped data member.

The example is based around the usage of the event model introduced in
:ref:`event_toplevel`, along with the :func:`~.attributes.flag_modified` function
which establishes the "dirty" flag on a particular mapped attribute.  These
functions are encapsulated in a mixin called ``TrackMutationsMixin``. 
Subclassing ``dict`` to provide "mutation tracking", then 
applying it to a custom dictionary type, looks like::

    class JSONEncodedDict(TypeDecorator):
        "JSON dictionary type from the types documentation"
        
        impl = VARCHAR

        def process_bind_param(self, value, dialect):
            if value is not None:
                value = simplejson.dumps(value, use_decimal=True)
            return value

        def process_result_value(self, value, dialect):
            if value is not None:
                value = simplejson.loads(value, use_decimal=True)
            return value

    class MutationDict(TrackMutationsMixin, dict):
        "Subclass dict to send mutation events to the owning object."
        
        def __init__(self, other):
            self.update(other)
        
        def __setitem__(self, key, value):
            dict.__setitem__(self, key, value)
            self.on_change()
    
        def __delitem__(self, key):
            dict.__delitem__(self, key)
            self.on_change()

    # hypothetical mapping
    Base = declarative_base()
    class Foo(Base):
        __tablename__ = 'foo'
        id = Column(Integer, primary_key=True)
        data = Column(JSONEncodedDict)

    # add mutation tracking to `Foo.data` as a one off
    MutationDict.associate_with_attribute(Foo.data)

The explicit step of associating ``MutationDict`` with ``Foo.data`` can be 
automated across a class of columns using ``associate_with_type()``::

    # add mutation tracking to all mapped attributes
    # that use JSONEncodedDict
    MutationDict.associate_with_type(JSONEncodedDict)
    
All subsequent mappings will have the ``MutationDict`` wrapper applied to
all attributes with ``JSONEncodedDict`` as their type.

The example illustrates the usage of several events, including
:meth:`.on_load`, :meth:`.on_refresh`, :meth:`.on_set`, and 
:meth:`.on_mapper_configured`.

"""