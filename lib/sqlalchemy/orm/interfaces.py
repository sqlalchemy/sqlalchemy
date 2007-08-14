# interfaces.py
# Copyright (C) 2005, 2006, 2007 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php


from sqlalchemy import util, logging, sql

__all__ = ['EXT_CONTINUE', 'EXT_STOP', 'EXT_PASS', 'MapperExtension',
           'MapperProperty', 'PropComparator', 'StrategizedProperty', 
           'LoaderStack', 'OperationContext', 'MapperOption', 
           'ExtensionOption', 'SynonymProperty', 'PropertyOption', 
           'AttributeExtension', 'StrategizedOption', 'LoaderStrategy' ]

EXT_CONTINUE = EXT_PASS = object()
EXT_STOP = object()

class MapperExtension(object):
    """Base implementation for customizing Mapper behavior.

    For each method in MapperExtension, returning a result of
    EXT_CONTINUE will allow processing to continue to the next
    MapperExtension in line or use the default functionality if there
    are no other extensions.

    Returning EXT_STOP will halt processing of further extensions
    handling that method.  Some methods such as ``load`` have other
    return requirements, see the individual documentation for details.
    Other than these exception cases, any return value other than
    EXT_CONTINUE or EXT_STOP will be interpreted as equivalent to
    EXT_STOP.
    
    EXT_PASS is a synonym for EXT_CONTINUE and is provided for
    backward compatibility.
    """
    
    def instrument_class(self, mapper, class_):
        return EXT_CONTINUE
        
    def init_instance(self, mapper, class_, oldinit, instance, args, kwargs):
        return EXT_CONTINUE

    def init_failed(self, mapper, class_, oldinit, instance, args, kwargs):
        return EXT_CONTINUE

    def get_session(self):
        """Retrieve a contextual Session instance with which to
        register a new object.

        Note: this is not called if a session is provided with the
        `__init__` params (i.e. `_sa_session`).
        """

        return EXT_CONTINUE

    def load(self, query, *args, **kwargs):
        """Override the `load` method of the Query object.

        The return value of this method is used as the result of
        ``query.load()`` if the value is anything other than EXT_CONTINUE.
        """

        return EXT_CONTINUE

    def get(self, query, *args, **kwargs):
        """Override the `get` method of the Query object.

        The return value of this method is used as the result of
        ``query.get()`` if the value is anything other than EXT_CONTINUE.
        """

        return EXT_CONTINUE

    def get_by(self, query, *args, **kwargs):
        """Override the `get_by` method of the Query object.

        The return value of this method is used as the result of
        ``query.get_by()`` if the value is anything other than
        EXT_CONTINUE.
        
        DEPRECATED.
        """

        return EXT_CONTINUE

    def select_by(self, query, *args, **kwargs):
        """Override the `select_by` method of the Query object.

        The return value of this method is used as the result of
        ``query.select_by()`` if the value is anything other than
        EXT_CONTINUE.
        
        DEPRECATED.
        """

        return EXT_CONTINUE

    def select(self, query, *args, **kwargs):
        """Override the `select` method of the Query object.

        The return value of this method is used as the result of
        ``query.select()`` if the value is anything other than
        EXT_CONTINUE.
        
        DEPRECATED.
        """

        return EXT_CONTINUE


    def translate_row(self, mapper, context, row):
        """Perform pre-processing on the given result row and return a
        new row instance.

        This is called as the very first step in the ``_instance()``
        method.
        """

        return EXT_CONTINUE

    def create_instance(self, mapper, selectcontext, row, class_):
        """Receive a row when a new object instance is about to be
        created from that row.

        The method can choose to create the instance itself, or it can
        return None to indicate normal object creation should take
        place.

        mapper
          The mapper doing the operation

        selectcontext
          SelectionContext corresponding to the instances() call

        row
          The result row from the database

        class\_
          The class we are mapping.
        """

        return EXT_CONTINUE

    def append_result(self, mapper, selectcontext, row, instance, result, **flags):
        """Receive an object instance before that instance is appended
        to a result list.

        If this method returns EXT_CONTINUE, result appending will proceed
        normally.  if this method returns any other value or None,
        result appending will not proceed for this instance, giving
        this extension an opportunity to do the appending itself, if
        desired.

        mapper
          The mapper doing the operation.

        selectcontext
          SelectionContext corresponding to the instances() call.

        row
          The result row from the database.

        instance
          The object instance to be appended to the result.

        result
          List to which results are being appended.

        \**flags
          extra information about the row, same as criterion in
          `create_row_processor()` method of [sqlalchemy.orm.interfaces#MapperProperty]
        """

        return EXT_CONTINUE

    def populate_instance(self, mapper, selectcontext, row, instance, **flags):
        """Receive a newly-created instance before that instance has
        its attributes populated.

        The normal population of attributes is according to each
        attribute's corresponding MapperProperty (which includes
        column-based attributes as well as relationships to other
        classes).  If this method returns EXT_CONTINUE, instance
        population will proceed normally.  If any other value or None
        is returned, instance population will not proceed, giving this
        extension an opportunity to populate the instance itself, if
        desired.
        """

        return EXT_CONTINUE

    def before_insert(self, mapper, connection, instance):
        """Receive an object instance before that instance is INSERTed
        into its table.

        This is a good place to set up primary key values and such
        that aren't handled otherwise.
        """

        return EXT_CONTINUE

    def after_insert(self, mapper, connection, instance):
        """Receive an object instance after that instance is INSERTed."""

        return EXT_CONTINUE

    def before_update(self, mapper, connection, instance):
        """Receive an object instance before that instance is UPDATEed."""

        return EXT_CONTINUE

    def after_update(self, mapper, connection, instance):
        """Receive an object instance after that instance is UPDATEed."""

        return EXT_CONTINUE

    def before_delete(self, mapper, connection, instance):
        """Receive an object instance before that instance is DELETEed."""

        return EXT_CONTINUE

    def after_delete(self, mapper, connection, instance):
        """Receive an object instance after that instance is DELETEed."""

        return EXT_CONTINUE

class MapperProperty(object):
    """Manage the relationship of a ``Mapper`` to a single class
    attribute, as well as that attribute as it appears on individual
    instances of the class, including attribute instrumentation,
    attribute access, loading behavior, and dependency calculations.
    """

    def setup(self, querycontext, **kwargs):
        """Called by Query for the purposes of constructing a SQL statement.
        
        Each MapperProperty associated with the target mapper processes the
        statement referenced by the query context, adding columns and/or
        criterion as appropriate.
        """

        pass

    def create_row_processor(self, selectcontext, mapper, row):
        """return a 2-tuple consiting of a row processing function and an instance post-processing function.
        
        Input arguments are the query.SelectionContext and the *first*
        applicable row of a result set obtained within query.Query.instances(), called
        only the first time a particular mapper.populate_instance() is invoked for the 
        overal result.

        The settings contained within the SelectionContext as well as the columns present
        in the row (which will be the same columns present in all rows) are used to determine
        the behavior of the returned callables.  The callables will then be used to process
        all rows and to post-process all instances, respectively.
        
        callables are of the following form::
        
            def execute(instance, row, **flags):
                # process incoming instance and given row.
                # flags is a dictionary containing at least the following attributes:
                #   isnew - indicates if the instance was newly created as a result of reading this row
                #   instancekey - identity key of the instance
                # optional attribute:
                #   ispostselect - indicates if this row resulted from a 'post' select of additional tables/columns
                
            def post_execute(instance, **flags):
                # process instance after all result rows have been processed.  this
                # function should be used to issue additional selections in order to
                # eagerly load additional properties.
                
            return (execute, post_execute)
            
        either tuple value can also be ``None`` in which case no function is called.
        
        """
        
        raise NotImplementedError()
        
    def cascade_iterator(self, type, object, recursive=None, halt_on=None):
        """return an iterator of objects which are child objects of the given object,
        as attached to the attribute corresponding to this MapperProperty."""
        
        return []

    def cascade_callable(self, type, object, callable_, recursive=None, halt_on=None):
        """run the given callable across all objects which are child objects of 
        the given object, as attached to the attribute corresponding to this MapperProperty."""
        
        return []

    def get_criterion(self, query, key, value):
        """Return a ``WHERE`` clause suitable for this
        ``MapperProperty`` corresponding to the given key/value pair,
        where the key is a column or object property name, and value
        is a value to be matched.  This is only picked up by
        ``PropertyLoaders``.

        This is called by a ``Query``'s ``join_by`` method to
        formulate a set of key/value pairs into a ``WHERE`` criterion
        that spans multiple tables if needed.
        """

        return None

    def set_parent(self, parent):
        self.parent = parent

    def init(self, key, parent):
        """Called after all mappers are compiled to assemble
        relationships between mappers, establish instrumented class
        attributes.
        """

        self.key = key
        self.do_init()

    def do_init(self):
        """Perform subclass-specific initialization steps.
        
        This is a *template* method called by the 
        ``MapperProperty`` object's init() method."""
        
        pass

    def register_dependencies(self, *args, **kwargs):
        """Called by the ``Mapper`` in response to the UnitOfWork
        calling the ``Mapper``'s register_dependencies operation.
        Should register with the UnitOfWork all inter-mapper
        dependencies as well as dependency processors (see UOW docs
        for more details).
        """

        pass

    def is_primary(self):
        """Return True if this ``MapperProperty``'s mapper is the
        primary mapper for its class.

        This flag is used to indicate that the ``MapperProperty`` can
        define attribute instrumentation for the class at the class
        level (as opposed to the individual instance level).
        """

        return self.parent._is_primary_mapper()

    def merge(self, session, source, dest):
        """Merge the attribute represented by this ``MapperProperty``
        from source to destination object"""

        raise NotImplementedError()

    def compare(self, operator, value):
        """Return a compare operation for the columns represented by
        this ``MapperProperty`` to the given value, which may be a
        column value or an instance.  'operator' is an operator from
        the operators module, or from sql.Comparator.
        
        By default uses the PropComparator attached to this MapperProperty
        under the attribute name "comparator".
        """

        return operator(self.comparator, value)

class PropComparator(sql.ColumnOperators):
    """defines comparison operations for MapperProperty objects"""
    
    def expression_element(self):
        return self.clause_element()
        
    def contains_op(a, b):
        return a.contains(b)
    contains_op = staticmethod(contains_op)
    
    def any_op(a, b, **kwargs):
        return a.any(b, **kwargs)
    any_op = staticmethod(any_op)
    
    def has_op(a, b, **kwargs):
        return a.has(b, **kwargs)
    has_op = staticmethod(has_op)
    
    def __init__(self, prop):
        self.prop = prop

    def contains(self, other):
        """return true if this collection contains other"""
        return self.operate(PropComparator.contains_op, other)

    def any(self, criterion=None, **kwargs):
        """return true if this collection contains any member that meets the given criterion.
        
            criterion
                an optional ClauseElement formulated against the member class' table or attributes.
                
            \**kwargs
                key/value pairs corresponding to member class attribute names which will be compared
                via equality to the corresponding values.
        """

        return self.operate(PropComparator.any_op, criterion, **kwargs)
    
    def has(self, criterion=None, **kwargs):
        """return true if this element references a member which meets the given criterion.
        
    
        criterion
            an optional ClauseElement formulated against the member class' table or attributes.
            
        \**kwargs
            key/value pairs corresponding to member class attribute names which will be compared
            via equality to the corresponding values.
        """

        return self.operate(PropComparator.has_op, criterion, **kwargs)
        
class StrategizedProperty(MapperProperty):
    """A MapperProperty which uses selectable strategies to affect
    loading behavior.

    There is a single default strategy selected by default.  Alternate
    strategies can be selected at Query time through the usage of
    ``StrategizedOption`` objects via the Query.options() method.
    """

    def _get_context_strategy(self, context):
        return self._get_strategy(context.attributes.get(("loaderstrategy", self), self.strategy.__class__))

    def _get_strategy(self, cls):
        try:
            return self._all_strategies[cls]
        except KeyError:
            # cache the located strategy per class for faster re-lookup
            strategy = cls(self)
            strategy.init()
            self._all_strategies[cls] = strategy
            return strategy

    def setup(self, querycontext, **kwargs):
        self._get_context_strategy(querycontext).setup_query(querycontext, **kwargs)

    def create_row_processor(self, selectcontext, mapper, row):
        return self._get_context_strategy(selectcontext).create_row_processor(selectcontext, mapper, row)

    def do_init(self):
        self._all_strategies = {}
        self.strategy = self.create_strategy()
        self._all_strategies[self.strategy.__class__] = self.strategy
        self.strategy.init()
        if self.is_primary():
            self.strategy.init_class_attribute()

class LoaderStack(object):
    """a stack object used during load operations to track the 
    current position among a chain of mappers to eager loaders."""
    
    def __init__(self):
        self.__stack = []
        
    def push_property(self, key):
        self.__stack.append(key)
        
    def push_mapper(self, mapper):
        self.__stack.append(mapper.base_mapper)
        
    def pop(self):
        self.__stack.pop()
        
    def snapshot(self):
        """return an 'snapshot' of this stack.
        
        this is a tuple form of the stack which can be used as a hash key.
        """
        
        return tuple(self.__stack)
        
    def __str__(self):
        return "->".join([str(s) for s in self.__stack])
        
class OperationContext(object):
    """Serve as a context during a query construction or instance
    loading operation.

    Accept ``MapperOption`` objects which may modify its state before proceeding.
    """

    def __init__(self, mapper, options):
        self.mapper = mapper
        self.options = options
        self.attributes = {}
        self.recursion_stack = util.Set()
        for opt in util.flatten_iterator(options):
            self.accept_option(opt)

    def accept_option(self, opt):
        pass

class MapperOption(object):
    """Describe a modification to an OperationContext or Query."""

    def process_query_context(self, context):
        pass

    def process_selection_context(self, context):
        pass

    def process_query(self, query):
        pass

class ExtensionOption(MapperOption):
    """a MapperOption that applies a MapperExtension to a query operation."""
    
    def __init__(self, ext):
        self.ext = ext

    def process_query(self, query):
        query._extension = query._extension.copy()
        query._extension.append(self.ext)

class SynonymProperty(MapperProperty):
    def __init__(self, name, proxy=False):
        self.name = name
        self.proxy = proxy

    def setup(self, querycontext, **kwargs):
        pass

    def create_row_processor(self, selectcontext, mapper, row):
        return (None, None)

    def do_init(self):
        if not self.proxy:
            return
        class SynonymProp(object):
            def __set__(s, obj, value):
                setattr(obj, self.name, value)
            def __delete__(s, obj):
                delattr(obj, self.name)
            def __get__(s, obj, owner):
                if obj is None:
                    return s
                return getattr(obj, self.name)
        setattr(self.parent.class_, self.key, SynonymProp())

    def merge(self, session, source, dest, _recursive):
        pass

class PropertyOption(MapperOption):
    """A MapperOption that is applied to a property off the mapper or
    one of its child mappers, identified by a dot-separated key.
    """

    def __init__(self, key):
        self.key = key

    def process_query_property(self, context, properties):
        pass

    def process_selection_property(self, context, properties):
        pass

    def process_query_context(self, context):
        self.process_query_property(context, self._get_properties(context))

    def process_selection_context(self, context):
        self.process_selection_property(context, self._get_properties(context))

    def _get_properties(self, context):
        try:
            l = self.__prop
        except AttributeError:
            l = []
            mapper = context.mapper
            for token in self.key.split('.'):
                prop = mapper.get_property(token, resolve_synonyms=True)
                l.append(prop)
                mapper = getattr(prop, 'mapper', None)
            self.__prop = l
        return l

PropertyOption.logger = logging.class_logger(PropertyOption)


class AttributeExtension(object):
    """An abstract class which specifies `append`, `delete`, and `set`
    event handlers to be attached to an object property.
    """

    def append(self, obj, child, initiator):
        pass

    def remove(self, obj, child, initiator):
        pass

    def set(self, obj, child, oldchild, initiator):
        pass


class StrategizedOption(PropertyOption):
    """A MapperOption that affects which LoaderStrategy will be used
    for an operation by a StrategizedProperty.
    """

    def is_chained(self):
        return False
        
    def process_query_property(self, context, properties):
        self.logger.debug("applying option to QueryContext, property key '%s'" % self.key)
        if self.is_chained():
            for prop in properties:
                context.attributes[("loaderstrategy", prop)] = self.get_strategy_class()
        else:
            context.attributes[("loaderstrategy", properties[-1])] = self.get_strategy_class()

    def process_selection_property(self, context, properties):
        self.logger.debug("applying option to SelectionContext, property key '%s'" % self.key)
        if self.is_chained():
            for prop in properties:
                context.attributes[("loaderstrategy", prop)] = self.get_strategy_class()
        else:     
            context.attributes[("loaderstrategy", properties[-1])] = self.get_strategy_class()

    def get_strategy_class(self):
        raise NotImplementedError()


class LoaderStrategy(object):
    """Describe the loading behavior of a StrategizedProperty object.

    The ``LoaderStrategy`` interacts with the querying process in three
    ways:

    * it controls the configuration of the ``InstrumentedAttribute``
      placed on a class to handle the behavior of the attribute.  this
      may involve setting up class-level callable functions to fire
      off a select operation when the attribute is first accessed
      (i.e. a lazy load)

    * it processes the ``QueryContext`` at statement construction time,
      where it can modify the SQL statement that is being produced.
      simple column attributes may add their represented column to the
      list of selected columns, *eager loading* properties may add
      ``LEFT OUTER JOIN`` clauses to the statement.

    * it processes the ``SelectionContext`` at row-processing time.  This 
      includes straight population of attributes corresponding to rows,
      setting instance-level lazyloader callables on newly
      constructed instances, and appending child items to scalar/collection 
      attributes in response to eagerly-loaded relations.
    """

    def __init__(self, parent):
        self.parent_property = parent
        self.is_class_level = False

    def init(self):
        self.parent = self.parent_property.parent
        self.key = self.parent_property.key
    
    def init_class_attribute(self):
        pass

    def setup_query(self, context, **kwargs):
        pass

    def create_row_processor(self, selectcontext, mapper, row):
        """return row processing functions which fulfill the contract specified
        by MapperProperty.create_row_processor.
        
        
        StrategizedProperty delegates its create_row_processor method
        directly to this method.
        """

        raise NotImplementedError()
