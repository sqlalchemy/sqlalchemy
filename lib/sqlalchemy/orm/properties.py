# properties.py
# Copyright (C) 2005,2006 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""defines a set of mapper.MapperProperty objects, including basic column properties as 
well as relationships.  the objects rely upon the LoaderStrategy objects in the strategies.py
module to handle load operations.  PropertyLoader also relies upon the dependency.py module
to handle flush-time dependency sorting and processing."""

from sqlalchemy import sql, schema, util, exceptions, sql_util, logging
from sqlalchemy.orm import mapper, sync, strategies, attributes, dependency
from sqlalchemy.orm import session as sessionlib
from sqlalchemy.orm import util as mapperutil
import sets, random
from sqlalchemy.orm.interfaces import *


class SynonymProperty(MapperProperty):
    def __init__(self, name, proxy=False):
        self.name = name
        self.proxy = proxy
    def setup(self, querycontext, **kwargs):
        pass
    def execute(self, selectcontext, instance, row, identitykey, isnew):
        pass
    def do_init(self):
        if not self.proxy:
            return
        class SynonymProp(object):
            def __set__(s, obj, value):
                setattr(obj, self.name, value)
                self.set(None, obj, value)
            def __delete__(s, obj):
                delattr(obj, self.name)
            def __get__(s, obj, owner):
                if obj is None:
                    return s
                return getattr(obj, self.name)
        setattr(self.parent.class_, self.key, SynonymProp())
        
class ColumnProperty(StrategizedProperty):
    """describes an object attribute that corresponds to a table column."""
    def __init__(self, *columns, **kwargs):
        """the list of columns describes a single object property. if there
        are multiple tables joined together for the mapper, this list represents
        the equivalent column as it appears across each table."""
        self.columns = list(columns)
        self.group = kwargs.pop('group', None)
        self.deferred = kwargs.pop('deferred', False)
    def create_strategy(self):
        if self.deferred:
            return strategies.DeferredColumnLoader(self)
        else:
            return strategies.ColumnLoader(self)
    def getattr(self, object):
        return getattr(object, self.key, None)
    def setattr(self, object, value):
        setattr(object, self.key, value)
    def get_history(self, obj, passive=False):
        return sessionlib.attribute_manager.get_history(obj, self.key, passive=passive)
        
ColumnProperty.logger = logging.class_logger(ColumnProperty)
        
mapper.ColumnProperty = ColumnProperty

class PropertyLoader(StrategizedProperty):
    """describes an object property that holds a single item or list of items that correspond
    to a related database table."""
    def __init__(self, argument, secondary, primaryjoin, secondaryjoin, foreignkey=None, uselist=None, private=False, association=None, order_by=False, attributeext=None, backref=None, is_backref=False, post_update=False, cascade=None, viewonly=False, lazy=True, collection_class=None, passive_deletes=False):
        self.uselist = uselist
        self.argument = argument
        self.secondary = secondary
        self.primaryjoin = primaryjoin
        self.secondaryjoin = secondaryjoin
        self.post_update = post_update
        self.direction = None
        self.viewonly = viewonly
        self.lazy = lazy
        self.foreignkey = util.to_set(foreignkey)
        self.collection_class = collection_class
        self.passive_deletes = passive_deletes
        
        if cascade is not None:
            self.cascade = mapperutil.CascadeOptions(cascade)
        else:
            if private:
                self.cascade = mapperutil.CascadeOptions("all, delete-orphan")
            else:
                self.cascade = mapperutil.CascadeOptions("save-update")

        self.association = association
        self.order_by = order_by
        self.attributeext=attributeext
        if isinstance(backref, str):
            # propigate explicitly sent primary/secondary join conditions to the BackRef object if
            # just a string was sent
            if secondary is not None:
                # reverse primary/secondary in case of a many-to-many
                self.backref = BackRef(backref, primaryjoin=secondaryjoin, secondaryjoin=primaryjoin)
            else:
                self.backref = BackRef(backref, primaryjoin=primaryjoin, secondaryjoin=secondaryjoin)
        else:
            self.backref = backref
        self.is_backref = is_backref
        
    private = property(lambda s:s.cascade.delete_orphan)

    def create_strategy(self):
        if self.lazy:
            return strategies.LazyLoader(self)
        elif self.lazy is False:
            return strategies.EagerLoader(self)
        elif self.lazy is None:
            return strategies.NoLoader(self)
            
    def __str__(self):
        return self.__class__.__name__ + " " + str(self.parent) + "->" + self.key + "->" + str(self.mapper)
        
    def cascade_iterator(self, type, object, recursive):
        if not type in self.cascade:
            return
        passive = type != 'delete' or self.passive_deletes
        childlist = sessionlib.attribute_manager.get_history(object, self.key, passive=passive)
        if childlist is None:
            return
        mapper = self.mapper.primary_mapper()
        for c in childlist.added_items() + childlist.deleted_items() + childlist.unchanged_items():
            if c is not None and c not in recursive:
                if not isinstance(c, self.mapper.class_):
                    raise exceptions.AssertionError("Attribute '%s' on class '%s' doesn't handle objects of type '%s'" % (self.key, str(self.parent.class_), str(c.__class__)))
                recursive.add(c)
                yield c
                for c2 in mapper.cascade_iterator(type, c, recursive):
                    yield c2

    def cascade_callable(self, type, object, callable_, recursive):
        if not type in self.cascade:
            return
        
        mapper = self.mapper.primary_mapper()
        passive = type != 'delete' or self.passive_deletes
        for c in sessionlib.attribute_manager.get_as_list(object, self.key, passive=passive):
            if c is not None and c not in recursive:
                if not isinstance(c, self.mapper.class_):
                    raise exceptions.AssertionError("Attribute '%s' on class '%s' doesn't handle objects of type '%s'" % (self.key, str(self.parent.class_), str(c.__class__)))
                recursive.add(c)
                callable_(c, mapper.entity_name)
                mapper.cascade_callable(type, c, callable_, recursive)

    def _get_target_class(self):
        """return the target class of the relation, even if the property has not been initialized yet."""
        if isinstance(self.argument, type):
            return self.argument
        else:
            return self.argument.class_
        
    def do_init(self):
        if isinstance(self.argument, type):
            self.mapper = mapper.class_mapper(self.argument, compile=False)._check_compile()
        elif isinstance(self.argument, mapper.Mapper):
            self.mapper = self.argument._check_compile()
        else:
            raise exceptions.ArgumentError("relation '%s' expects a class or a mapper argument (received: %s)" % (self.key, type(self.argument)))
            
        self.mapper = self.mapper.get_select_mapper()._check_compile()
            
        if self.association is not None:
            if isinstance(self.association, type):
                self.association = mapper.class_mapper(self.association, compile=False)._check_compile()
        
        self.target = self.mapper.mapped_table

        if self.cascade.delete_orphan:
            if self.parent.class_ is self.mapper.class_:
                raise exceptions.ArgumentError("Cant establish 'delete-orphan' cascade rule on a self-referential relationship (attribute '%s' on class '%s').  You probably want cascade='all', which includes delete cascading but not orphan detection." %(self.key, self.parent.class_.__name__))
            self.mapper.primary_mapper().delete_orphans.append((self.key, self.parent.class_))
            
        if self.secondaryjoin is not None and self.secondary is None:
            raise exceptions.ArgumentError("Property '" + self.key + "' specified with secondary join condition but no secondary argument")
        # if join conditions were not specified, figure them out based on foreign keys
        try:
            if self.secondary is not None:
                if self.secondaryjoin is None:
                    self.secondaryjoin = sql.join(self.mapper.unjoined_table, self.secondary).onclause
                if self.primaryjoin is None:
                    self.primaryjoin = sql.join(self.parent.unjoined_table, self.secondary).onclause
            else:
                if self.primaryjoin is None:
                    self.primaryjoin = sql.join(self.parent.unjoined_table, self.target).onclause
        except exceptions.ArgumentError, e:
            raise exceptions.ArgumentError("Error determining primary and/or secondary join for relationship '%s' between mappers '%s' and '%s'.  If the underlying error cannot be corrected, you should specify the 'primaryjoin' (and 'secondaryjoin', if there is an association table present) keyword arguments to the relation() function (or for backrefs, by specifying the backref using the backref() function with keyword arguments) to explicitly specify the join conditions.  Nested error is \"%s\"" % (self.key, self.parent, self.mapper, str(e)))
        # if the foreign key wasnt specified and theres no assocaition table, try to figure
        # out who is dependent on who. we dont need all the foreign keys represented in the join,
        # just one of them.
        if not len(self.foreignkey) and self.secondaryjoin is None:
            # else we usually will have a one-to-many where the secondary depends on the primary
            # but its possible that its reversed
            self._find_dependent()

        # if we are re-initializing, as in a copy made for an inheriting 
        # mapper, dont re-evaluate the direction.
        if self.direction is None:
            self.direction = self._get_direction()
        
        if self.uselist is None and self.direction == sync.MANYTOONE:
            self.uselist = False

        if self.uselist is None:
            self.uselist = True
        
        if not self.viewonly:
            self._dependency_processor = dependency.create_dependency_processor(self)
            
        # primary property handler, set up class attributes
        if self.is_primary():
            # if a backref name is defined, set up an extension to populate 
            # attributes in the other direction
            if self.backref is not None:
                self.attributeext = self.backref.get_extension()
        
            if self.backref is not None:
                self.backref.compile(self)
        elif not sessionlib.attribute_manager.is_class_managed(self.parent.class_, self.key):
            raise exceptions.ArgumentError("Attempting to assign a new relation '%s' to a non-primary mapper on class '%s'.  New relations can only be added to the primary mapper, i.e. the very first mapper created for class '%s' " % (self.key, self.parent.class_.__name__, self.parent.class_.__name__))

        super(PropertyLoader, self).do_init()
        
    def _is_self_referential(self):
        return self.parent.mapped_table is self.target or self.parent.select_table is self.target
        
    def _get_direction(self):
        """determines our 'direction', i.e. do we represent one to many, many to many, etc."""
        if self.secondaryjoin is not None:
            return sync.MANYTOMANY
        elif self._is_self_referential():
            # for a self referential mapper, if the "foreignkey" is a single or composite primary key,
            # then we are "many to one", since the remote site of the relationship identifies a singular entity.
            # otherwise we are "one to many".
            for f in self.foreignkey:
                if not f.primary_key:
                    return sync.ONETOMANY
            else:
                return sync.MANYTOONE
        elif len([c for c in self.foreignkey if self.mapper.unjoined_table.corresponding_column(c, False) is not None]):
            return sync.ONETOMANY
        elif len([c for c in self.foreignkey if self.parent.unjoined_table.corresponding_column(c, False) is not None]):
            return sync.MANYTOONE
        else:
            raise exceptions.ArgumentError("Cant determine relation direction for '%s' in mapper '%s' with primary join\n '%s'" %(self.key, str(self.mapper), str(self.primaryjoin)))
            
    def _find_dependent(self):
        """searches through the primary join condition to determine which side
        has the foreign key - from this we return 
        the "foreign key" for this property which helps determine one-to-many/many-to-one."""
        foreignkeys = util.Set()
        def foo(binary):
            if binary.operator != '=' or not isinstance(binary.left, schema.Column) or not isinstance(binary.right, schema.Column):
                return
            for f in binary.left.foreign_keys:
                if f.references(binary.right.table):
                    foreignkeys.add(binary.left)
            for f in binary.right.foreign_keys:
                if f.references(binary.left.table):
                    foreignkeys.add(binary.right)
        visitor = mapperutil.BinaryVisitor(foo)
        self.primaryjoin.accept_visitor(visitor)
        if len(foreignkeys) == 0:
            raise exceptions.ArgumentError("On relation '%s', can't figure out which side is the foreign key for join condition '%s'.  Specify the 'foreignkey' argument to the relation." % (self.key, str(self.primaryjoin)))
        self.foreignkey = foreignkeys
        
    def get_join(self):
        if self.secondaryjoin is not None:
            return self.primaryjoin & self.secondaryjoin
        else:
            return self.primaryjoin

    def register_dependencies(self, uowcommit):
        if not self.viewonly:
            self._dependency_processor.register_dependencies(uowcommit)
            
PropertyLoader.logger = logging.class_logger(PropertyLoader)

class BackRef(object):
    """stores the name of a backreference property as well as options to 
    be used on the resulting PropertyLoader."""
    def __init__(self, key, **kwargs):
        self.key = key
        self.kwargs = kwargs
    def compile(self, prop):
        """called by the owning PropertyLoader to set up a backreference on the
        PropertyLoader's mapper."""
        # try to set a LazyLoader on our mapper referencing the parent mapper
        mapper = prop.mapper.primary_mapper()
        if not mapper.props.has_key(self.key):
            pj = self.kwargs.pop('primaryjoin', None)
            sj = self.kwargs.pop('secondaryjoin', None)
            # the backref property is set on the primary mapper
            parent = prop.parent.primary_mapper()
            relation = PropertyLoader(parent, prop.secondary, pj, sj, backref=prop.key, is_backref=True, **self.kwargs)
            mapper._compile_property(self.key, relation);
        elif not isinstance(mapper.props[self.key], PropertyLoader):
            raise exceptions.ArgumentError("Cant create backref '%s' on mapper '%s'; an incompatible property of that name already exists" % (self.key, str(mapper)))
        else:
            # else set one of us as the "backreference"
            parent = prop.parent.primary_mapper()
            if parent.class_ is not mapper.props[self.key]._get_target_class():
                raise exceptions.ArgumentError("Backrefs do not match:  backref '%s' expects to connect to %s, but found a backref already connected to %s" % (self.key, str(parent.class_), str(mapper.props[self.key].mapper.class_)))
            if not mapper.props[self.key].is_backref:
                prop.is_backref=True
                if not prop.viewonly:
                    prop._dependency_processor.is_backref=True
    def get_extension(self):
        """returns an attribute extension to use with this backreference."""
        return attributes.GenericBackrefExtension(self.key)

