# interfaces.py
# Copyright (C) 2005,2006 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php


from sqlalchemy import util, logging

class MapperProperty(object):
    """manages the relationship of a Mapper to a single class attribute, as well
    as that attribute as it appears on individual instances of the class, including 
    attribute instrumentation, attribute access, loading behavior, and dependency calculations."""
    def setup(self, querycontext, **kwargs):
        """called when a statement is being constructed.  """
        pass
    def execute(self, selectcontext, instance, row, identitykey, isnew):
        """called when the mapper receives a row.  instance is the parent instance
        corresponding to the row. """
        raise NotImplementedError()
    def cascade_iterator(self, type, object, recursive=None):
        return []
    def cascade_callable(self, type, object, callable_, recursive=None):
        return []
    def get_criterion(self, query, key, value):
        """Returns a WHERE clause suitable for this MapperProperty corresponding to the 
        given key/value pair, where the key is a column or object property name, and value
        is a value to be matched.  This is only picked up by PropertyLoaders.
            
        this is called by a Query's join_by method to formulate a set of key/value pairs into 
        a WHERE criterion that spans multiple tables if needed."""
        return None
    def set_parent(self, parent):
        self.parent = parent
    def init(self, key, parent):
        """called after all mappers are compiled to assemble relationships between 
        mappers, establish instrumented class attributes"""
        self.key = key
        self.do_init()
    def adapt_to_inherited(self, key, newparent):
        """adapt this MapperProperty to a new parent, assuming the new parent is an inheriting
        descendant of the old parent.  """
        newparent._compile_property(key, self, init=False, setparent=False)
    def do_init(self):
        """template method for subclasses"""
        pass
    def register_dependencies(self, *args, **kwargs):
        """called by the Mapper in response to the UnitOfWork calling the Mapper's
        register_dependencies operation.  Should register with the UnitOfWork all 
        inter-mapper dependencies as well as dependency processors (see UOW docs for more details)"""
        pass
    def is_primary(self):
        """return True if this MapperProperty's mapper is the primary mapper for its class.
        
        This flag is used to indicate that the MapperProperty can define attribute instrumentation
        for the class at the class level (as opposed to the individual instance level.)"""
        return self.parent._is_primary_mapper()

class StrategizedProperty(MapperProperty):
    """a MapperProperty which uses selectable strategies to affect loading behavior.
    There is a single default strategy selected, and alternate strategies can be selected
    at selection time through the usage of StrategizedOption objects."""
    def _get_context_strategy(self, context):
        return self._get_strategy(context.attributes.get((LoaderStrategy, self), self.strategy.__class__))
    def _get_strategy(self, cls):
        try:
            return self._all_strategies[cls]
        except KeyError:
            strategy = cls(self)
            strategy.init()
            strategy.is_default = False
            self._all_strategies[cls] = strategy
            return strategy
    def setup(self, querycontext, **kwargs):
        self._get_context_strategy(querycontext).setup_query(querycontext, **kwargs)
    def execute(self, selectcontext, instance, row, identitykey, isnew):
        self._get_context_strategy(selectcontext).process_row(selectcontext, instance, row, identitykey, isnew)
    def do_init(self):
        self._all_strategies = {}
        self.strategy = self.create_strategy()
        self._all_strategies[self.strategy.__class__] = self.strategy
        self.strategy.init()
        if self.is_primary():
            self.strategy.init_class_attribute()

class OperationContext(object):
    """serves as a context during a query construction or instance loading operation.
    accepts MapperOption objects which may modify its state before proceeding."""
    def __init__(self, mapper, options):
        self.mapper = mapper
        self.options = options
        self.attributes = {}
        self.recursion_stack = util.Set()
        for opt in options:
            self.accept_option(opt)
    def accept_option(self, opt):
        pass

class MapperOption(object):
    """describes a modification to an OperationContext."""
    def process_query_context(self, context):
        pass
    def process_selection_context(self, context):
        pass
    def process_query(self, query):
        pass
        
class PropertyOption(MapperOption):
    """a MapperOption that is applied to a property off the mapper
    or one of its child mappers, identified by a dot-separated key."""
    def __init__(self, key):
        self.key = key
    def process_query_property(self, context, property):
        pass
    def process_selection_property(self, context, property):
        pass
    def process_query_context(self, context):
        self.process_query_property(context, self._get_property(context))
    def process_selection_context(self, context):
        self.process_selection_property(context, self._get_property(context))
    def _get_property(self, context):
        try:
            prop = self.__prop
        except AttributeError:
            mapper = context.mapper
            for token in self.key.split('.'):
                prop = mapper.props[token]
                mapper = getattr(prop, 'mapper', None)
            self.__prop = prop
        return prop
PropertyOption.logger = logging.class_logger(PropertyOption)

class StrategizedOption(PropertyOption):
    """a MapperOption that affects which LoaderStrategy will be used for an operation
    by a StrategizedProperty."""
    def process_query_property(self, context, property):
        self.logger.debug("applying option to QueryContext, property key '%s'" % self.key)
        context.attributes[(LoaderStrategy, property)] = self.get_strategy_class()
    def process_selection_property(self, context, property):
        self.logger.debug("applying option to SelectionContext, property key '%s'" % self.key)
        context.attributes[(LoaderStrategy, property)] = self.get_strategy_class()
    def get_strategy_class(self):
        raise NotImplementedError()


class LoaderStrategy(object):
    """describes the loading behavior of a StrategizedProperty object.  The LoaderStrategy
    interacts with the querying process in three ways:
    
      * it controls the configuration of the InstrumentedAttribute placed on a class to 
      handle the behavior of the attribute.  this may involve setting up class-level callable
      functions to fire off a select operation when the attribute is first accessed (i.e. a lazy load)

      * it processes the QueryContext at statement construction time, where it can modify the SQL statement
      that is being produced.  simple column attributes may add their represented column to the list of
      selected columns, "eager loading" properties may add LEFT OUTER JOIN clauses to the statement.

      * it processes the SelectionContext at row-processing time.  This may involve setting instance-level
      lazyloader functions on newly constructed instances, or may involve recursively appending child items 
      to a list in response to additionally eager-loaded objects in the query.
    """
    def __init__(self, parent):
        self.parent_property = parent
        self.is_default = True
    def init(self):
        self.parent = self.parent_property.parent
        self.key = self.parent_property.key
    def init_class_attribute(self):
        pass
    def setup_query(self, context, **kwargs):
        pass
    def process_row(self, selectcontext, instance, row, identitykey, isnew):
        pass

