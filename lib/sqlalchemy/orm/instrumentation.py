# orm/instrumentation.py
# Copyright (C) 2005-2012 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Defines SQLAlchemy's system of class instrumentation.

This module is usually not directly visible to user applications, but
defines a large part of the ORM's interactivity.

instrumentation.py deals with registration of end-user classes
for state tracking.   It interacts closely with state.py
and attributes.py which establish per-instance and per-class-attribute
instrumentation, respectively.

SQLA's instrumentation system is completely customizable, in which
case an understanding of the general mechanics of this module is helpful.
An example of full customization is in /examples/custom_attributes.

"""


from sqlalchemy.orm import exc, collections, events
from operator import attrgetter, itemgetter
from sqlalchemy import event, util
import weakref
from sqlalchemy.orm import state, attributes


INSTRUMENTATION_MANAGER = '__sa_instrumentation_manager__'
"""Attribute, elects custom instrumentation when present on a mapped class.

Allows a class to specify a slightly or wildly different technique for
tracking changes made to mapped attributes and collections.

Only one instrumentation implementation is allowed in a given object
inheritance hierarchy.

The value of this attribute must be a callable and will be passed a class
object.  The callable must return one of:

  - An instance of an interfaces.InstrumentationManager or subclass
  - An object implementing all or some of InstrumentationManager (TODO)
  - A dictionary of callables, implementing all or some of the above (TODO)
  - An instance of a ClassManager or subclass

interfaces.InstrumentationManager is public API and will remain stable
between releases.  ClassManager is not public and no guarantees are made
about stability.  Caveat emptor.

This attribute is consulted by the default SQLAlchemy instrumentation
resolution code.  If custom finders are installed in the global
instrumentation_finders list, they may or may not choose to honor this
attribute.

"""

instrumentation_finders = []
"""An extensible sequence of instrumentation implementation finding callables.

Finders callables will be passed a class object.  If None is returned, the
next finder in the sequence is consulted.  Otherwise the return must be an
instrumentation factory that follows the same guidelines as
INSTRUMENTATION_MANAGER.

By default, the only finder is find_native_user_instrumentation_hook, which
searches for INSTRUMENTATION_MANAGER.  If all finders return None, standard
ClassManager instrumentation is used.

"""


class ClassManager(dict):
    """tracks state information at the class level."""

    MANAGER_ATTR = '_sa_class_manager'
    STATE_ATTR = '_sa_instance_state'

    deferred_scalar_loader = None

    original_init = object.__init__

    def __init__(self, class_):
        self.class_ = class_
        self.factory = None  # where we came from, for inheritance bookkeeping
        self.info = {}
        self.new_init = None
        self.mutable_attributes = set()
        self.local_attrs = {}
        self.originals = {}

        self._bases = [mgr for mgr in [
                        manager_of_class(base)
                        for base in self.class_.__bases__
                        if isinstance(base, type)
                 ] if mgr is not None]

        for base in self._bases:
            self.update(base)

        self.manage()
        self._instrument_init()

    dispatch = event.dispatcher(events.InstanceEvents)

    @property
    def is_mapped(self):
        return 'mapper' in self.__dict__

    @util.memoized_property
    def mapper(self):
        # raises unless self.mapper has been assigned
        raise exc.UnmappedClassError(self.class_)

    def _attr_has_impl(self, key):
        """Return True if the given attribute is fully initialized.

        i.e. has an impl.
        """

        return key in self and self[key].impl is not None

    def _subclass_manager(self, cls):
        """Create a new ClassManager for a subclass of this ClassManager's
        class.

        This is called automatically when attributes are instrumented so that
        the attributes can be propagated to subclasses against their own
        class-local manager, without the need for mappers etc. to have already
        pre-configured managers for the full class hierarchy.   Mappers
        can post-configure the auto-generated ClassManager when needed.

        """
        manager = manager_of_class(cls)
        if manager is None:
            manager = _create_manager_for_cls(cls, _source=self)
        return manager

    def _instrument_init(self):
        # TODO: self.class_.__init__ is often the already-instrumented
        # __init__ from an instrumented superclass.  We still need to make
        # our own wrapper, but it would
        # be nice to wrap the original __init__ and not our existing wrapper
        # of such, since this adds method overhead.
        self.original_init = self.class_.__init__
        self.new_init = _generate_init(self.class_, self)
        self.install_member('__init__', self.new_init)

    def _uninstrument_init(self):
        if self.new_init:
            self.uninstall_member('__init__')
            self.new_init = None

    @util.memoized_property
    def _state_constructor(self):
        self.dispatch.first_init(self, self.class_)
        if self.mutable_attributes:
            return state.MutableAttrInstanceState
        else:
            return state.InstanceState

    def manage(self):
        """Mark this instance as the manager for its class."""

        setattr(self.class_, self.MANAGER_ATTR, self)

    def dispose(self):
        """Dissasociate this manager from its class."""

        delattr(self.class_, self.MANAGER_ATTR)

    def manager_getter(self):
        return attrgetter(self.MANAGER_ATTR)

    def instrument_attribute(self, key, inst, propagated=False):
        if propagated:
            if key in self.local_attrs:
                return  # don't override local attr with inherited attr
        else:
            self.local_attrs[key] = inst
            self.install_descriptor(key, inst)
        self[key] = inst

        for cls in self.class_.__subclasses__():
            manager = self._subclass_manager(cls)
            manager.instrument_attribute(key, inst, True)

    def subclass_managers(self, recursive):
        for cls in self.class_.__subclasses__():
            mgr = manager_of_class(cls)
            if mgr is not None and mgr is not self:
                yield mgr
                if recursive:
                    for m in mgr.subclass_managers(True):
                        yield m

    def post_configure_attribute(self, key):
        instrumentation_registry.dispatch.\
                attribute_instrument(self.class_, key, self[key])

    def uninstrument_attribute(self, key, propagated=False):
        if key not in self:
            return
        if propagated:
            if key in self.local_attrs:
                return  # don't get rid of local attr
        else:
            del self.local_attrs[key]
            self.uninstall_descriptor(key)
        del self[key]
        if key in self.mutable_attributes:
            self.mutable_attributes.remove(key)
        for cls in self.class_.__subclasses__():
            manager = manager_of_class(cls)
            if manager:
                manager.uninstrument_attribute(key, True)

    def unregister(self):
        """remove all instrumentation established by this ClassManager."""

        self._uninstrument_init()

        self.mapper = self.dispatch = None
        self.info.clear()

        for key in list(self):
            if key in self.local_attrs:
                self.uninstrument_attribute(key)

    def install_descriptor(self, key, inst):
        if key in (self.STATE_ATTR, self.MANAGER_ATTR):
            raise KeyError("%r: requested attribute name conflicts with "
                           "instrumentation attribute of the same name." %
                           key)
        setattr(self.class_, key, inst)

    def uninstall_descriptor(self, key):
        delattr(self.class_, key)

    def install_member(self, key, implementation):
        if key in (self.STATE_ATTR, self.MANAGER_ATTR):
            raise KeyError("%r: requested attribute name conflicts with "
                           "instrumentation attribute of the same name." %
                           key)
        self.originals.setdefault(key, getattr(self.class_, key, None))
        setattr(self.class_, key, implementation)

    def uninstall_member(self, key):
        original = self.originals.pop(key, None)
        if original is not None:
            setattr(self.class_, key, original)

    def instrument_collection_class(self, key, collection_class):
        return collections.prepare_instrumentation(collection_class)

    def initialize_collection(self, key, state, factory):
        user_data = factory()
        adapter = collections.CollectionAdapter(
            self.get_impl(key), state, user_data)
        return adapter, user_data

    def is_instrumented(self, key, search=False):
        if search:
            return key in self
        else:
            return key in self.local_attrs

    def get_impl(self, key):
        return self[key].impl

    @property
    def attributes(self):
        return self.itervalues()

    ## InstanceState management

    def new_instance(self, state=None):
        instance = self.class_.__new__(self.class_)
        setattr(instance, self.STATE_ATTR,
                    state or self._state_constructor(instance, self))
        return instance

    def setup_instance(self, instance, state=None):
        setattr(instance, self.STATE_ATTR,
                    state or self._state_constructor(instance, self))

    def teardown_instance(self, instance):
        delattr(instance, self.STATE_ATTR)

    def _new_state_if_none(self, instance):
        """Install a default InstanceState if none is present.

        A private convenience method used by the __init__ decorator.

        """
        if hasattr(instance, self.STATE_ATTR):
            return False
        elif self.class_ is not instance.__class__ and \
                self.is_mapped:
            # this will create a new ClassManager for the
            # subclass, without a mapper.  This is likely a
            # user error situation but allow the object
            # to be constructed, so that it is usable
            # in a non-ORM context at least.
            return self._subclass_manager(instance.__class__).\
                        _new_state_if_none(instance)
        else:
            state = self._state_constructor(instance, self)
            setattr(instance, self.STATE_ATTR, state)
            return state

    def state_getter(self):
        """Return a (instance) -> InstanceState callable.

        "state getter" callables should raise either KeyError or
        AttributeError if no InstanceState could be found for the
        instance.
        """

        return attrgetter(self.STATE_ATTR)

    def dict_getter(self):
        return attrgetter('__dict__')

    def has_state(self, instance):
        return hasattr(instance, self.STATE_ATTR)

    def has_parent(self, state, key, optimistic=False):
        """TODO"""
        return self.get_impl(key).hasparent(state, optimistic=optimistic)

    def __nonzero__(self):
        """All ClassManagers are non-zero regardless of attribute state."""
        return True

    def __repr__(self):
        return '<%s of %r at %x>' % (
            self.__class__.__name__, self.class_, id(self))

class _ClassInstrumentationAdapter(ClassManager):
    """Adapts a user-defined InstrumentationManager to a ClassManager."""

    def __init__(self, class_, override, **kw):
        self._adapted = override
        self._get_state = self._adapted.state_getter(class_)
        self._get_dict = self._adapted.dict_getter(class_)

        ClassManager.__init__(self, class_, **kw)

    def manage(self):
        self._adapted.manage(self.class_, self)

    def dispose(self):
        self._adapted.dispose(self.class_)

    def manager_getter(self):
        return self._adapted.manager_getter(self.class_)

    def instrument_attribute(self, key, inst, propagated=False):
        ClassManager.instrument_attribute(self, key, inst, propagated)
        if not propagated:
            self._adapted.instrument_attribute(self.class_, key, inst)

    def post_configure_attribute(self, key):
        super(_ClassInstrumentationAdapter, self).post_configure_attribute(key)
        self._adapted.post_configure_attribute(self.class_, key, self[key])

    def install_descriptor(self, key, inst):
        self._adapted.install_descriptor(self.class_, key, inst)

    def uninstall_descriptor(self, key):
        self._adapted.uninstall_descriptor(self.class_, key)

    def install_member(self, key, implementation):
        self._adapted.install_member(self.class_, key, implementation)

    def uninstall_member(self, key):
        self._adapted.uninstall_member(self.class_, key)

    def instrument_collection_class(self, key, collection_class):
        return self._adapted.instrument_collection_class(
            self.class_, key, collection_class)

    def initialize_collection(self, key, state, factory):
        delegate = getattr(self._adapted, 'initialize_collection', None)
        if delegate:
            return delegate(key, state, factory)
        else:
            return ClassManager.initialize_collection(self, key,
                                                        state, factory)

    def new_instance(self, state=None):
        instance = self.class_.__new__(self.class_)
        self.setup_instance(instance, state)
        return instance

    def _new_state_if_none(self, instance):
        """Install a default InstanceState if none is present.

        A private convenience method used by the __init__ decorator.
        """
        if self.has_state(instance):
            return False
        else:
            return self.setup_instance(instance)

    def setup_instance(self, instance, state=None):
        self._adapted.initialize_instance_dict(self.class_, instance)

        if state is None:
            state = self._state_constructor(instance, self)

        # the given instance is assumed to have no state
        self._adapted.install_state(self.class_, instance, state)
        return state

    def teardown_instance(self, instance):
        self._adapted.remove_state(self.class_, instance)

    def has_state(self, instance):
        try:
            state = self._get_state(instance)
        except exc.NO_STATE:
            return False
        else:
            return True

    def state_getter(self):
        return self._get_state

    def dict_getter(self):
        return self._get_dict

def register_class(class_, **kw):
    """Register class instrumentation.

    Returns the existing or newly created class manager.
    """

    manager = manager_of_class(class_)
    if manager is None:
        manager = _create_manager_for_cls(class_, **kw)
    return manager

def unregister_class(class_):
    """Unregister class instrumentation."""

    instrumentation_registry.unregister(class_)


def is_instrumented(instance, key):
    """Return True if the given attribute on the given instance is
    instrumented by the attributes package.

    This function may be used regardless of instrumentation
    applied directly to the class, i.e. no descriptors are required.

    """
    return manager_of_class(instance.__class__).\
                        is_instrumented(key, search=True)

class InstrumentationRegistry(object):
    """Private instrumentation registration singleton.

    All classes are routed through this registry
    when first instrumented, however the InstrumentationRegistry
    is not actually needed unless custom ClassManagers are in use.

    """

    _manager_finders = weakref.WeakKeyDictionary()
    _state_finders = util.WeakIdentityMapping()
    _dict_finders = util.WeakIdentityMapping()
    _extended = False

    dispatch = event.dispatcher(events.InstrumentationEvents)

    def create_manager_for_cls(self, class_, **kw):
        assert class_ is not None
        assert manager_of_class(class_) is None

        for finder in instrumentation_finders:
            factory = finder(class_)
            if factory is not None:
                break
        else:
            factory = ClassManager

        existing_factories = self._collect_management_factories_for(class_).\
                                difference([factory])
        if existing_factories:
            raise TypeError(
                "multiple instrumentation implementations specified "
                "in %s inheritance hierarchy: %r" % (
                    class_.__name__, list(existing_factories)))

        manager = factory(class_)
        if not isinstance(manager, ClassManager):
            manager = _ClassInstrumentationAdapter(class_, manager)

        if factory != ClassManager and not self._extended:
            # somebody invoked a custom ClassManager.
            # reinstall global "getter" functions with the more
            # expensive ones.
            self._extended = True
            _install_lookup_strategy(self)

        manager.factory = factory
        self._manager_finders[class_] = manager.manager_getter()
        self._state_finders[class_] = manager.state_getter()
        self._dict_finders[class_] = manager.dict_getter()

        self.dispatch.class_instrument(class_)

        return manager

    def _collect_management_factories_for(self, cls):
        """Return a collection of factories in play or specified for a
        hierarchy.

        Traverses the entire inheritance graph of a cls and returns a
        collection of instrumentation factories for those classes. Factories
        are extracted from active ClassManagers, if available, otherwise
        instrumentation_finders is consulted.

        """
        hierarchy = util.class_hierarchy(cls)
        factories = set()
        for member in hierarchy:
            manager = manager_of_class(member)
            if manager is not None:
                factories.add(manager.factory)
            else:
                for finder in instrumentation_finders:
                    factory = finder(member)
                    if factory is not None:
                        break
                else:
                    factory = None
                factories.add(factory)
        factories.discard(None)
        return factories

    def manager_of_class(self, cls):
        # this is only called when alternate instrumentation
        # has been established
        if cls is None:
            return None
        try:
            finder = self._manager_finders[cls]
        except KeyError:
            return None
        else:
            return finder(cls)

    def state_of(self, instance):
        # this is only called when alternate instrumentation
        # has been established
        if instance is None:
            raise AttributeError("None has no persistent state.")
        try:
            return self._state_finders[instance.__class__](instance)
        except KeyError:
            raise AttributeError("%r is not instrumented" %
                                    instance.__class__)

    def dict_of(self, instance):
        # this is only called when alternate instrumentation
        # has been established
        if instance is None:
            raise AttributeError("None has no persistent state.")
        try:
            return self._dict_finders[instance.__class__](instance)
        except KeyError:
            raise AttributeError("%r is not instrumented" %
                                    instance.__class__)

    def unregister(self, class_):
        if class_ in self._manager_finders:
            manager = self.manager_of_class(class_)
            self.dispatch.class_uninstrument(class_)
            manager.unregister()
            manager.dispose()
            del self._manager_finders[class_]
            del self._state_finders[class_]
            del self._dict_finders[class_]
        if ClassManager.MANAGER_ATTR in class_.__dict__:
            delattr(class_, ClassManager.MANAGER_ATTR)

instrumentation_registry = InstrumentationRegistry()


def _install_lookup_strategy(implementation):
    """Replace global class/object management functions
    with either faster or more comprehensive implementations,
    based on whether or not extended class instrumentation
    has been detected.

    This function is called only by InstrumentationRegistry()
    and unit tests specific to this behavior.

    """
    global instance_state, instance_dict, manager_of_class
    if implementation is util.symbol('native'):
        instance_state = attrgetter(ClassManager.STATE_ATTR)
        instance_dict = attrgetter("__dict__")
        def manager_of_class(cls):
            return cls.__dict__.get(ClassManager.MANAGER_ATTR, None)
    else:
        instance_state = instrumentation_registry.state_of
        instance_dict = instrumentation_registry.dict_of
        manager_of_class = instrumentation_registry.manager_of_class
    attributes.instance_state = instance_state
    attributes.instance_dict = instance_dict
    attributes.manager_of_class = manager_of_class

_create_manager_for_cls = instrumentation_registry.create_manager_for_cls

# Install default "lookup" strategies.  These are basically
# very fast attrgetters for key attributes.
# When a custom ClassManager is installed, more expensive per-class
# strategies are copied over these.
_install_lookup_strategy(util.symbol('native'))


def find_native_user_instrumentation_hook(cls):
    """Find user-specified instrumentation management for a class."""
    return getattr(cls, INSTRUMENTATION_MANAGER, None)
instrumentation_finders.append(find_native_user_instrumentation_hook)

def _generate_init(class_, class_manager):
    """Build an __init__ decorator that triggers ClassManager events."""

    # TODO: we should use the ClassManager's notion of the
    # original '__init__' method, once ClassManager is fixed
    # to always reference that.
    original__init__ = class_.__init__
    assert original__init__

    # Go through some effort here and don't change the user's __init__
    # calling signature, including the unlikely case that it has
    # a return value.
    # FIXME: need to juggle local names to avoid constructor argument
    # clashes.
    func_body = """\
def __init__(%(apply_pos)s):
    new_state = class_manager._new_state_if_none(%(self_arg)s)
    if new_state:
        return new_state.initialize_instance(%(apply_kw)s)
    else:
        return original__init__(%(apply_kw)s)
"""
    func_vars = util.format_argspec_init(original__init__, grouped=False)
    func_text = func_body % func_vars

    # Py3K
    #func_defaults = getattr(original__init__, '__defaults__', None)
    #func_kw_defaults = getattr(original__init__, '__kwdefaults__', None)
    # Py2K
    func = getattr(original__init__, 'im_func', original__init__)
    func_defaults = getattr(func, 'func_defaults', None)
    # end Py2K

    env = locals().copy()
    exec func_text in env
    __init__ = env['__init__']
    __init__.__doc__ = original__init__.__doc__
    if func_defaults:
        __init__.func_defaults = func_defaults
    # Py3K
    #if func_kw_defaults:
    #    __init__.__kwdefaults__ = func_kw_defaults
    return __init__
