# interfaces.py
# Copyright (C) 2005, 2006, 2007, 2008 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Semi-private implementation objects which form the basis
of ORM-mapped attributes, query options and mapper extension.

Defines the [sqlalchemy.orm.interfaces#MapperExtension] class,
which can be end-user subclassed to add event-based functionality
to mappers.  The remainder of this module is generally private to the
ORM.
"""

from itertools import chain
from sqlalchemy import exceptions, logging, util
from sqlalchemy.sql import expression
class_mapper = None

__all__ = ['EXT_CONTINUE', 'EXT_STOP', 'EXT_PASS', 'MapperExtension',
           'MapperProperty', 'PropComparator', 'StrategizedProperty',
           'build_path', 'MapperOption',
           'ExtensionOption', 'PropertyOption',
           'AttributeExtension', 'StrategizedOption', 'LoaderStrategy' ]

EXT_CONTINUE = EXT_PASS = util.symbol('EXT_CONTINUE')
EXT_STOP = util.symbol('EXT_STOP')

ONETOMANY = util.symbol('ONETOMANY')
MANYTOONE = util.symbol('MANYTOONE')
MANYTOMANY = util.symbol('MANYTOMANY')

class MapperExtension(object):
    """Base implementation for customizing Mapper behavior.

    For each method in MapperExtension, returning a result of EXT_CONTINUE
    will allow processing to continue to the next MapperExtension in line or
    use the default functionality if there are no other extensions.

    Returning EXT_STOP will halt processing of further extensions handling
    that method.  Some methods such as ``load`` have other return
    requirements, see the individual documentation for details.  Other than
    these exception cases, any return value other than EXT_CONTINUE or
    EXT_STOP will be interpreted as equivalent to EXT_STOP.

    EXT_PASS is a synonym for EXT_CONTINUE and is provided for backward
    compatibility.
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

        The method can choose to create the instance itself, or it can return
        EXT_CONTINUE to indicate normal object creation should take place.

        mapper
          The mapper doing the operation

        selectcontext
          SelectionContext corresponding to the instances() call

        row
          The result row from the database

        class\_
          The class we are mapping.

        return value
          A new object instance, or EXT_CONTINUE
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

        Column-based attributes can be modified within this method which will
        result in the new value being inserted.  However *no* changes to the overall
        flush plan can be made; this means any collection modification or
        save() operations which occur within this method will not take effect
        until the next flush call.

        """

        return EXT_CONTINUE

    def after_insert(self, mapper, connection, instance):
        """Receive an object instance after that instance is INSERTed."""

        return EXT_CONTINUE

    def before_update(self, mapper, connection, instance):
        """Receive an object instance before that instance is UPDATEed.

        Note that this method is called for all instances that are marked as
        "dirty", even those which have no net changes to their column-based
        attributes.  An object is marked as dirty when any of its column-based
        attributes have a "set attribute" operation called or when any of its
        collections are modified.  If, at update time, no column-based attributes
        have any net changes, no UPDATE statement will be issued.  This means
        that an instance being sent to before_update is *not* a guarantee that
        an UPDATE statement will be issued (although you can affect the outcome
        here).

        To detect if the column-based attributes on the object have net changes,
        and will therefore generate an UPDATE statement, use
        ``object_session(instance).is_modified(instance, include_collections=False)``.

        Column-based attributes can be modified within this method which will
        result in their being updated.  However *no* changes to the overall
        flush plan can be made; this means any collection modification or
        save() operations which occur within this method will not take effect
        until the next flush call.

        """

        return EXT_CONTINUE

    def after_update(self, mapper, connection, instance):
        """Receive an object instance after that instance is UPDATEed."""

        return EXT_CONTINUE

    def before_delete(self, mapper, connection, instance):
        """Receive an object instance before that instance is DELETEed.

        Note that *no* changes to the overall
        flush plan can be made here; this means any collection modification,
        save() or delete() operations which occur within this method will
        not take effect until the next flush call.

        """

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
        """Return a 3-tuple consiting of two row processing functions and an instance post-processing function.

        Input arguments are the query.SelectionContext and the *first*
        applicable row of a result set obtained within
        query.Query.instances(), called only the first time a particular
        mapper's populate_instance() method is invoked for the overall result.

        The settings contained within the SelectionContext as well as the
        columns present in the row (which will be the same columns present in
        all rows) are used to determine the presence and behavior of the
        returned callables.  The callables will then be used to process all
        rows and to post-process all instances, respectively.

        Callables are of the following form::

            def new_execute(instance, row, **flags):
                # process incoming instance and given row.  the instance is
                # "new" and was just created upon receipt of this row.
                # flags is a dictionary containing at least the following
                # attributes:
                #   isnew - indicates if the instance was newly created as a
                #           result of reading this row
                #   instancekey - identity key of the instance
                # optional attribute:
                #   ispostselect - indicates if this row resulted from a
                #                  'post' select of additional tables/columns

            def existing_execute(instance, row, **flags):
                # process incoming instance and given row.  the instance is
                # "existing" and was created based on a previous row.

            def post_execute(instance, **flags):
                # process instance after all result rows have been processed.
                # this function should be used to issue additional selections
                # in order to eagerly load additional properties.

            return (new_execute, existing_execute, post_execute)

        Either of the three tuples can be ``None`` in which case no function
        is called.
        """

        raise NotImplementedError()

    def cascade_iterator(self, type_, state, visited_instances=None, halt_on=None):
        """Iterate through instances related to the given instance for
        a particular 'cascade', starting with this MapperProperty.

        See PropertyLoader for the related instance implementation.
        """

        return iter([])

    def get_criterion(self, query, key, value):
        """Return a ``WHERE`` clause suitable for this
        ``MapperProperty`` corresponding to the given key/value pair,
        where the key is a column or object property name, and value
        is a value to be matched.  This is only picked up by
        ``PropertyLoaders``.

        This is called by a ``Query``'s ``join_by`` method to formulate a set
        of key/value pairs into a ``WHERE`` criterion that spans multiple
        tables if needed.
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

        return not self.parent.non_primary

    def merge(self, session, source, dest, dont_load, _recursive):
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

class PropComparator(expression.ColumnOperators):
    """defines comparison operations for MapperProperty objects.
    
    PropComparator instances should also define an accessor 'property'
    which returns the MapperProperty associated with this
    PropComparator.
    """

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
        self.prop = self.property = prop
    
    def of_type_op(a, class_):
        return a.of_type(class_)
    of_type_op = staticmethod(of_type_op)
    
    def of_type(self, class_):
        """Redefine this object in terms of a polymorphic subclass.
        
        Returns a new PropComparator from which further criterion can be evaluated.

        e.g.::
        
            query.join(Company.employees.of_type(Engineer)).\\
               filter(Engineer.name=='foo')
              
        \class_
            a class or mapper indicating that criterion will be against
            this specific subclass.

         
        """
        
        return self.operate(PropComparator.of_type_op, class_)
        
    def contains(self, other):
        """Return true if this collection contains other"""
        return self.operate(PropComparator.contains_op, other)

    def any(self, criterion=None, **kwargs):
        """Return true if this collection contains any member that meets the given criterion.

        criterion
          an optional ClauseElement formulated against the member class' table
          or attributes.

        \**kwargs
          key/value pairs corresponding to member class attribute names which
          will be compared via equality to the corresponding values.
        """

        return self.operate(PropComparator.any_op, criterion, **kwargs)

    def has(self, criterion=None, **kwargs):
        """Return true if this element references a member which meets the given criterion.

        criterion
          an optional ClauseElement formulated against the member class' table
          or attributes.

        \**kwargs
          key/value pairs corresponding to member class attribute names which
          will be compared via equality to the corresponding values.
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
        path = context.path
        return self._get_strategy(context.attributes.get(("loaderstrategy", path), self.strategy.__class__))

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
        self.strategy = self._get_strategy(self.strategy_class)
        if self.is_primary():
            self.strategy.init_class_attribute()

def build_path(mapper, key, prev=None):
    if prev:
        return prev + (mapper.base_mapper, key)
    else:
        return (mapper.base_mapper, key)

def serialize_path(path):
    if path is None:
        return None

    return [
        (mapper.class_, mapper.entity_name, key)
        for mapper, key in [(path[i], path[i+1]) for i in range(0, len(path)-1, 2)]
    ]

def deserialize_path(path):
    if path is None:
        return None

    global class_mapper
    if class_mapper is None:
        from sqlalchemy.orm import class_mapper

    return tuple(
        chain(*[(class_mapper(cls, entity), key) for cls, entity, key in path])
    )

class MapperOption(object):
    """Describe a modification to a Query."""

    def process_query(self, query):
        pass

    def process_query_conditionally(self, query):
        """same as process_query(), except that this option may not apply
        to the given query.

        Used when secondary loaders resend existing options to a new
        Query."""
        self.process_query(query)

class ExtensionOption(MapperOption):
    """a MapperOption that applies a MapperExtension to a query operation."""

    def __init__(self, ext):
        self.ext = ext

    def process_query(self, query):
        query._extension = query._extension.copy()
        query._extension.insert(self.ext)


class PropertyOption(MapperOption):
    """A MapperOption that is applied to a property off the mapper or
    one of its child mappers, identified by a dot-separated key.
    """

    def __init__(self, key, mapper=None):
        self.key = key
        self.mapper = mapper

    def process_query(self, query):
        self._process(query, True)

    def process_query_conditionally(self, query):
        self._process(query, False)

    def _process(self, query, raiseerr):
        if self._should_log_debug:
            self.logger.debug("applying option to Query, property key '%s'" % self.key)
        paths = self._get_paths(query, raiseerr)
        if paths:
            self.process_query_property(query, paths)

    def process_query_property(self, query, paths):
        pass

    def _get_paths(self, query, raiseerr):
        path = None
        l = []
        current_path = list(query._current_path)

        if self.mapper:
            global class_mapper
            if class_mapper is None:
                from sqlalchemy.orm import class_mapper
            mapper = self.mapper
            if isinstance(self.mapper, type):
                mapper = class_mapper(mapper)
            if mapper is not query.mapper and mapper not in [q.mapper for q in query._entities]:
                raise exceptions.ArgumentError("Can't find entity %s in Query.  Current list: %r" % (str(mapper), [str(m) for m in query._entities]))
        else:
            mapper = query.mapper
        if isinstance(self.key, basestring):
            tokens = self.key.split('.')
        else:
            tokens = util.to_list(self.key)
            
        for token in tokens:
            if isinstance(token, basestring):
                prop = mapper.get_property(token, resolve_synonyms=True, raiseerr=raiseerr)
            elif isinstance(token, PropComparator):
                prop = token.property
                token = prop.key
                    
            else:
                raise exceptions.ArgumentError("mapper option expects string key or list of attributes")
                
            if current_path and token == current_path[1]:
                current_path = current_path[2:]
                continue
                
            if prop is None:
                return []
            path = build_path(mapper, prop.key, path)
            l.append(path)
            if getattr(token, '_of_type', None):
                mapper = token._of_type
            else:
                mapper = getattr(prop, 'mapper', None)
        return l

PropertyOption.logger = logging.class_logger(PropertyOption)
PropertyOption._should_log_debug = logging.is_debug_enabled(PropertyOption.logger)

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

    def process_query_property(self, query, paths):
        if self.is_chained():
            for path in paths:
                query._attributes[("loaderstrategy", path)] = self.get_strategy_class()
        else:
            query._attributes[("loaderstrategy", paths[-1])] = self.get_strategy_class()

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
        """Return row processing functions which fulfill the contract specified
        by MapperProperty.create_row_processor.

        StrategizedProperty delegates its create_row_processor method directly
        to this method.
        """

        raise NotImplementedError()
