# descriptor_props.py
# Copyright (C) 2005, 2006, 2007, 2008, 2009, 2010 Michael Bayer
# mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Descriptor proprerties are more "auxilliary" properties
that exist as configurational elements, but don't participate
as actively in the load/persist ORM loop.   They all
build on the "hybrid" extension to produce class descriptors.

"""

from sqlalchemy.orm.interfaces import \
    MapperProperty, PropComparator, StrategizedProperty
from sqlalchemy.orm import attributes
from sqlalchemy import util, sql, exc as sa_exc, event
from sqlalchemy.sql import expression
properties = util.importlater('sqlalchemy.orm', 'properties')

class DescriptorProperty(MapperProperty):
    """:class:`MapperProperty` which proxies access to a 
        user-defined descriptor."""

    def instrument_class(self, mapper):
        from sqlalchemy.ext import hybrid
        
        prop = self
        
        # hackety hack hack
        class _ProxyImpl(object):
            accepts_scalar_loader = False
            expire_missing = True

            def __init__(self, key):
                self.key = key
            
            if hasattr(prop, 'get_history'):
                def get_history(self, state, dict_, **kw):
                    return prop.get_history(state, dict_, **kw)
                
        if self.descriptor is None:
            desc = getattr(mapper.class_, self.key, None)
            if mapper._is_userland_descriptor(desc):
                self.descriptor = desc

        if self.descriptor is None:
            def fset(obj, value):
                setattr(obj, self.name, value)
            def fdel(obj):
                delattr(obj, self.name)
            def fget(obj):
                return getattr(obj, self.name)
            fget.__doc__ = self.doc

            descriptor = hybrid.property_(
                fget=fget,
                fset=fset,
                fdel=fdel,
            )
        elif isinstance(self.descriptor, property):
            descriptor = hybrid.property_(
                fget=self.descriptor.fget,
                fset=self.descriptor.fset,
                fdel=self.descriptor.fdel,
            )
        else:
            descriptor = hybrid.property_(
                fget=self.descriptor.__get__,
                fset=self.descriptor.__set__,
                fdel=self.descriptor.__delete__,
            )
        
        proxy_attr = attributes.\
                    create_proxied_attribute(self.descriptor or descriptor)\
                    (
                        self.parent.class_,
                        self.key, 
                        self.descriptor or descriptor,
                        lambda: self._comparator_factory(mapper)
                    )
        def get_comparator(owner):
            return util.update_wrapper(proxy_attr, descriptor)
        descriptor.expr = get_comparator
        descriptor.impl = _ProxyImpl(self.key)
        mapper.class_manager.instrument_attribute(self.key, descriptor)
    

class CompositeProperty(DescriptorProperty):
    
    def __init__(self, class_, *columns, **kwargs):
        self.columns = columns
        self.composite_class = class_
        self.active_history = kwargs.get('active_history', False)
        self.deferred = kwargs.get('deferred', False)
        self.group = kwargs.get('group', None)
        util.set_creation_order(self)
        self._create_descriptor()
        
    def do_init(self):
        """Initialization which occurs after the :class:`.CompositeProperty` 
        has been associated with its parent mapper.
        
        """
        self._setup_arguments_on_columns()
        self._setup_event_handlers()
    
    def _create_descriptor(self):
        """Create the actual Python descriptor that will serve as 
        the access point on the mapped class.
        
        """

        def fget(instance):
            dict_ = attributes.instance_dict(instance)
            if self.key in dict_:
                return dict_[self.key]
            else:
                dict_[self.key] = composite = self.composite_class(
                    *[getattr(instance, key) for key in self._attribute_keys]
            )
                return composite
                
        def fset(instance, value):
            if value is None:
                fdel(instance)
            else:
                dict_ = attributes.instance_dict(instance)
                dict_[self.key] = value
                for key, value in zip(
                        self._attribute_keys, 
                        value.__composite_values__()):
                    setattr(instance, key, value)
        
        def fdel(instance):
            for key in self._attribute_keys:
                setattr(instance, key, None)
        
        self.descriptor = property(fget, fset, fdel)
        
    def _setup_arguments_on_columns(self):
        """Propigate configuration arguments made on this composite
        to the target columns, for those that apply.
        
        """
        for col in self.columns:
            prop = self.parent._columntoproperty[col]
            prop.active_history = self.active_history
            if self.deferred:
                prop.deferred = self.deferred
                prop.strategy_class = strategies.DeferredColumnLoader
            prop.group = self.group

    def _setup_event_handlers(self):
        """Establish events that will clear out the composite value
        whenever changes in state occur on the target columns.
        
        """
        def load_handler(state):
            state.dict.pop(self.key, None)
            
        def expire_handler(state, keys):
            if keys is None or set(self._attribute_keys).intersection(keys):
                state.dict.pop(self.key, None)
        
        def insert_update_handler(mapper, connection, state):
            state.dict.pop(self.key, None)
            
        event.listen(self.parent, 'on_after_insert', 
                                    insert_update_handler, raw=True)
        event.listen(self.parent, 'on_after_update', 
                                    insert_update_handler, raw=True)
        event.listen(self.parent, 'on_load', load_handler, raw=True)
        event.listen(self.parent, 'on_refresh', load_handler, raw=True)
        event.listen(self.parent, "on_expire", expire_handler, raw=True)
        
        # TODO:  add listeners to the column attributes, which 
        # refresh the composite based on userland settings.
        
        # TODO: add a callable to the composite of the form
        # _on_change(self, attrname) which will send up a corresponding
        # refresh to the column attribute on all parents.  Basically
        # a specialization of the scalars.py example.
        
        
    @util.memoized_property
    def _attribute_keys(self):
        return [
            self.parent._columntoproperty[col].key
            for col in self.columns
        ]
        
    def get_history(self, state, dict_, **kw):
        """Provided for userland code that uses attributes.get_history()."""
        
        added = []
        deleted = []
        
        has_history = False
        for col in self.columns:
            key = self.parent._columntoproperty[col].key
            hist = state.manager[key].impl.get_history(state, dict_)
            if hist.has_changes():
                has_history = True
            
            added.extend(hist.non_deleted())
            if hist.deleted:
                deleted.extend(hist.deleted)
            else:
                deleted.append(None)
        
        if has_history:
            return attributes.History(
                [self.composite_class(*added)],
                (),
                [self.composite_class(*deleted)]
            )
        else:
            return attributes.History(
                (),[self.composite_class(*added)], ()
            )

    def _comparator_factory(self, mapper):
        return CompositeProperty.Comparator(self)

    class Comparator(PropComparator):
        def __init__(self, prop, adapter=None):
            self.prop = prop
            self.adapter = adapter
            
        def __clause_element__(self):
            if self.adapter:
                # TODO: test coverage for adapted composite comparison
                return expression.ClauseList(
                            *[self.adapter(x) for x in self.prop.columns])
            else:
                return expression.ClauseList(*self.prop.columns)
        
        __hash__ = None
        
        def __eq__(self, other):
            if other is None:
                values = [None] * len(self.prop.columns)
            else:
                values = other.__composite_values__()
            return sql.and_(
                    *[a==b for a, b in zip(self.prop.columns, values)])
            
        def __ne__(self, other):
            return sql.not_(self.__eq__(other))

    def __str__(self):
        return str(self.parent.class_.__name__) + "." + self.key

class ConcreteInheritedProperty(DescriptorProperty):
    """A 'do nothing' :class:`MapperProperty` that disables 
    an attribute on a concrete subclass that is only present
    on the inherited mapper, not the concrete classes' mapper.

    Cases where this occurs include:

    * When the superclass mapper is mapped against a 
      "polymorphic union", which includes all attributes from 
      all subclasses.
    * When a relationship() is configured on an inherited mapper,
      but not on the subclass mapper.  Concrete mappers require
      that relationship() is configured explicitly on each 
      subclass. 

    """

    def _comparator_factory(self, mapper):
        comparator_callable = None
        
        for m in self.parent.iterate_to_root():
            p = m._props[self.key]
            if not isinstance(p, ConcreteInheritedProperty):
                comparator_callable = p.comparator_factory
                break
        return comparator_callable
    
    def __init__(self):
        def warn():
            raise AttributeError("Concrete %s does not implement "
                "attribute %r at the instance level.  Add this "
                "property explicitly to %s." % 
                (self.parent, self.key, self.parent))

        class NoninheritedConcreteProp(object):
            def __set__(s, obj, value):
                warn()
            def __delete__(s, obj):
                warn()
            def __get__(s, obj, owner):
                if obj is None:
                    return self.descriptor
                warn()
        self.descriptor = NoninheritedConcreteProp()
        
        
class SynonymProperty(DescriptorProperty):

    def __init__(self, name, map_column=None, 
                            descriptor=None, comparator_factory=None,
                            doc=None):
        self.name = name
        self.map_column = map_column
        self.descriptor = descriptor
        self.comparator_factory = comparator_factory
        self.doc = doc or (descriptor and descriptor.__doc__) or None
        util.set_creation_order(self)

    def _comparator_factory(self, mapper):
        prop = getattr(mapper.class_, self.name).property

        if self.comparator_factory:
            comp = self.comparator_factory(prop, mapper)
        else:
            comp = prop.comparator_factory(prop, mapper)
        return comp

    def set_parent(self, parent, init):
        if self.map_column:
            # implement the 'map_column' option.
            if self.key not in parent.mapped_table.c:
                raise sa_exc.ArgumentError(
                    "Can't compile synonym '%s': no column on table "
                    "'%s' named '%s'" 
                     % (self.name, parent.mapped_table.description, self.key))
            elif parent.mapped_table.c[self.key] in \
                    parent._columntoproperty and \
                    parent._columntoproperty[
                                            parent.mapped_table.c[self.key]
                                        ].key == self.name:
                raise sa_exc.ArgumentError(
                    "Can't call map_column=True for synonym %r=%r, "
                    "a ColumnProperty already exists keyed to the name "
                    "%r for column %r" % 
                    (self.key, self.name, self.name, self.key)
                )
            p = properties.ColumnProperty(parent.mapped_table.c[self.key])
            parent._configure_property(
                                    self.name, p, 
                                    init=init, 
                                    setparent=True)
            p._mapped_by_synonym = self.key
    
        self.parent = parent
        
class ComparableProperty(DescriptorProperty):
    """Instruments a Python property for use in query expressions."""

    def __init__(self, comparator_factory, descriptor=None, doc=None):
        self.descriptor = descriptor
        self.comparator_factory = comparator_factory
        self.doc = doc or (descriptor and descriptor.__doc__) or None
        util.set_creation_order(self)

    def _comparator_factory(self, mapper):
        return self.comparator_factory(self, mapper)
