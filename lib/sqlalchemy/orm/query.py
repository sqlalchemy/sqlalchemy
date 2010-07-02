# orm/query.py
# Copyright (C) 2005, 2006, 2007, 2008, 2009, 2010 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""The Query class and support.

Defines the :class:`~sqlalchemy.orm.query.Query` class, the central 
construct used by the ORM to construct database queries.

The ``Query`` class should not be confused with the
:class:`~sqlalchemy.sql.expression.Select` class, which defines database 
SELECT operations at the SQL (non-ORM) level.  ``Query`` differs from 
``Select`` in that it returns ORM-mapped objects and interacts with an 
ORM session, whereas the ``Select`` construct interacts directly with the 
database to return iterable result sets.

"""

from itertools import chain
from operator import itemgetter

from sqlalchemy import sql, util, log, schema
from sqlalchemy import exc as sa_exc
from sqlalchemy.orm import exc as orm_exc
from sqlalchemy.sql import util as sql_util
from sqlalchemy.sql import expression, visitors, operators
from sqlalchemy.orm import (
    attributes, interfaces, mapper, object_mapper, evaluator,
    )
from sqlalchemy.orm.util import (
    AliasedClass, ORMAdapter, _entity_descriptor, _entity_info,
    _is_aliased_class, _is_mapped_class, _orm_columns, _orm_selectable,
    join as orm_join,
    )


__all__ = ['Query', 'QueryContext', 'aliased']


aliased = AliasedClass

def _generative(*assertions):
    """Mark a method as generative."""

    @util.decorator
    def generate(fn, *args, **kw):
        self = args[0]._clone()
        for assertion in assertions:
            assertion(self, fn.func_name)
        fn(self, *args[1:], **kw)
        return self
    return generate

class Query(object):
    """ORM-level SQL construction object."""
    
    _enable_eagerloads = True
    _enable_assertions = True
    _with_labels = False
    _criterion = None
    _yield_per = None
    _lockmode = None
    _order_by = False
    _group_by = False
    _having = None
    _distinct = False
    _offset = None
    _limit = None
    _statement = None
    _correlate = frozenset()
    _populate_existing = False
    _version_check = False
    _autoflush = True
    _current_path = ()
    _only_load_props = None
    _refresh_state = None
    _from_obj = ()
    _filter_aliases = None
    _from_obj_alias = None
    _joinpath = _joinpoint = util.frozendict()
    _execution_options = util.frozendict()
    _params = util.frozendict()
    _attributes = util.frozendict()
    _with_options = ()
    _with_hints = ()
    
    def __init__(self, entities, session=None):
        self.session = session
        self._polymorphic_adapters = {}
        self._set_entities(entities)

    def _set_entities(self, entities, entity_wrapper=None):
        if entity_wrapper is None:
            entity_wrapper = _QueryEntity
        self._entities = []
        for ent in util.to_list(entities):
            entity_wrapper(self, ent)

        self._setup_aliasizers(self._entities)

    def _setup_aliasizers(self, entities):
        if hasattr(self, '_mapper_adapter_map'):
            # usually safe to share a single map, but copying to prevent
            # subtle leaks if end-user is reusing base query with arbitrary
            # number of aliased() objects
            self._mapper_adapter_map = d = self._mapper_adapter_map.copy()
        else:
            self._mapper_adapter_map = d = {}

        for ent in entities:
            for entity in ent.entities:
                if entity not in d:
                    mapper, selectable, is_aliased_class = _entity_info(entity)
                    if not is_aliased_class and mapper.with_polymorphic:
                        with_polymorphic = mapper._with_polymorphic_mappers
                        if mapper.mapped_table not in self._polymorphic_adapters:
                            self.__mapper_loads_polymorphically_with(mapper, 
                                sql_util.ColumnAdapter(
                                            selectable, 
                                            mapper._equivalent_columns))
                        adapter = None
                    elif is_aliased_class:
                        adapter = sql_util.ColumnAdapter(
                                            selectable, 
                                            mapper._equivalent_columns)
                        with_polymorphic = None
                    else:
                        with_polymorphic = adapter = None

                    d[entity] = (mapper, adapter, selectable, 
                                        is_aliased_class, with_polymorphic)
                ent.setup_entity(entity, *d[entity])

    def __mapper_loads_polymorphically_with(self, mapper, adapter):
        for m2 in mapper._with_polymorphic_mappers:
            self._polymorphic_adapters[m2] = adapter
            for m in m2.iterate_to_root():
                self._polymorphic_adapters[m.mapped_table] = \
                                self._polymorphic_adapters[m.local_table] = \
                                adapter

    def _set_select_from(self, *obj):

        fa = []
        for from_obj in obj:
            if isinstance(from_obj, expression._SelectBaseMixin):
                from_obj = from_obj.alias()
            fa.append(from_obj)

        self._from_obj = tuple(fa)

        if len(self._from_obj) == 1 and \
            isinstance(self._from_obj[0], expression.Alias):
            equivs = self.__all_equivs()
            self._from_obj_alias = sql_util.ColumnAdapter(
                                                self._from_obj[0], equivs)
        
    def _get_polymorphic_adapter(self, entity, selectable):
        self.__mapper_loads_polymorphically_with(entity.mapper, 
                    sql_util.ColumnAdapter(selectable, 
                            entity.mapper._equivalent_columns))

    def _reset_polymorphic_adapter(self, mapper):
        for m2 in mapper._with_polymorphic_mappers:
            self._polymorphic_adapters.pop(m2, None)
            for m in m2.iterate_to_root():
                self._polymorphic_adapters.pop(m.mapped_table, None)
                self._polymorphic_adapters.pop(m.local_table, None)

    def __adapt_polymorphic_element(self, element):
        if isinstance(element, expression.FromClause):
            search = element
        elif hasattr(element, 'table'):
            search = element.table
        else:
            search = None

        if search is not None:
            alias = self._polymorphic_adapters.get(search, None)
            if alias:
                return alias.adapt_clause(element)

    def __replace_element(self, adapters):
        def replace(elem):
            if '_halt_adapt' in elem._annotations:
                return elem

            for adapter in adapters:
                e = adapter(elem)
                if e is not None:
                    return e
        return replace

    def __replace_orm_element(self, adapters):
        def replace(elem):
            if '_halt_adapt' in elem._annotations:
                return elem

            if "_orm_adapt" in elem._annotations \
                    or "parententity" in elem._annotations:
                for adapter in adapters:
                    e = adapter(elem)
                    if e is not None:
                        return e
        return replace

    @_generative()
    def _adapt_all_clauses(self):
        self._disable_orm_filtering = True
    
    def _adapt_col_list(self, cols):
        return [
                    self._adapt_clause(
                                expression._literal_as_text(o), 
                                True, True) 
                    for o in cols
                ]
        
    def _adapt_clause(self, clause, as_filter, orm_only):
        adapters = []
        if as_filter and self._filter_aliases:
            for fa in self._filter_aliases._visitor_iterator:
                adapters.append(fa.replace)

        if self._from_obj_alias:
            adapters.append(self._from_obj_alias.replace)

        if self._polymorphic_adapters:
            adapters.append(self.__adapt_polymorphic_element)

        if not adapters:
            return clause

        if getattr(self, '_disable_orm_filtering', not orm_only):
            return visitors.replacement_traverse(
                                clause, 
                                {'column_collections':False}, 
                                self.__replace_element(adapters)
                            )
        else:
            return visitors.replacement_traverse(
                                clause, 
                                {'column_collections':False}, 
                                self.__replace_orm_element(adapters)
                            )

    def _entity_zero(self):
        return self._entities[0]

    def _mapper_zero(self):
        return self._entity_zero().entity_zero

    def _extension_zero(self):
        ent = self._entity_zero()
        return getattr(ent, 'extension', ent.mapper.extension)

    @property
    def _mapper_entities(self):
        # TODO: this is wrong, its hardcoded to "priamry entity" when
        # for the case of __all_equivs() it should not be
        # the name of this accessor is wrong too
        for ent in self._entities:
            if hasattr(ent, 'primary_entity'):
                yield ent

    def _joinpoint_zero(self):
        return self._joinpoint.get(
                            '_joinpoint_entity',
                            self._entity_zero().entity_zero)

    def _mapper_zero_or_none(self):
        if not getattr(self._entities[0], 'primary_entity', False):
            return None
        return self._entities[0].mapper

    def _only_mapper_zero(self, rationale=None):
        if len(self._entities) > 1:
            raise sa_exc.InvalidRequestError(
                    rationale or 
                    "This operation requires a Query against a single mapper."
                )
        return self._mapper_zero()

    def _only_entity_zero(self, rationale=None):
        if len(self._entities) > 1:
            raise sa_exc.InvalidRequestError(
                    rationale or 
                    "This operation requires a Query against a single mapper."
                )
        return self._entity_zero()

    def _generate_mapper_zero(self):
        if not getattr(self._entities[0], 'primary_entity', False):
            raise sa_exc.InvalidRequestError(
                            "No primary mapper set up for this Query.")
        entity = self._entities[0]._clone()
        self._entities = [entity] + self._entities[1:]
        return entity

    def __all_equivs(self):
        equivs = {}
        for ent in self._mapper_entities:
            equivs.update(ent.mapper._equivalent_columns)
        return equivs

    def _get_condition(self):
        self._order_by = self._distinct = False
        return self._no_criterion_condition("get")
        
    def _no_criterion_condition(self, meth):
        if not self._enable_assertions:
            return
        if self._criterion is not None or \
                self._statement is not None or self._from_obj or \
                self._limit is not None or self._offset is not None or \
                self._group_by or self._order_by or self._distinct:
            raise sa_exc.InvalidRequestError(
                                "Query.%s() being called on a "
                                "Query with existing criterion. " % meth)

        self._from_obj = ()
        self._statement = self._criterion = None
        self._order_by = self._group_by = self._distinct = False

    def _no_clauseelement_condition(self, meth):
        if not self._enable_assertions:
            return
        if self._order_by:
            raise sa_exc.InvalidRequestError(
                                "Query.%s() being called on a "
                                "Query with existing criterion. " % meth)
        self._no_criterion_condition(meth)

    def _no_statement_condition(self, meth):
        if not self._enable_assertions:
            return
        if self._statement:
            raise sa_exc.InvalidRequestError(
                ("Query.%s() being called on a Query with an existing full "
                 "statement - can't apply criterion.") % meth)

    def _no_limit_offset(self, meth):
        if not self._enable_assertions:
            return
        if self._limit is not None or self._offset is not None:
            raise sa_exc.InvalidRequestError(
                "Query.%s() being called on a Query which already has LIMIT "
                "or OFFSET applied. To modify the row-limited results of a "
                " Query, call from_self() first.  "
                "Otherwise, call %s() before limit() or offset() are applied."
                % (meth, meth)
            )

    def _no_select_modifiers(self, meth):
        if not self._enable_assertions:
            return
        for attr, methname, notset in (
            ('_limit', 'limit()', None),
            ('_offset', 'offset()', None),
            ('_order_by', 'order_by()', False),
            ('_group_by', 'group_by()', False),
            ('_distinct', 'distinct()', False),
        ):
            if getattr(self, attr) is not notset:
                raise sa_exc.InvalidRequestError(
                    "Can't call Query.%s() when %s has been called" % 
                    (meth, methname)
                )

    def _get_options(self, populate_existing=None, 
                            version_check=None, 
                            only_load_props=None, 
                            refresh_state=None):
        if populate_existing:
            self._populate_existing = populate_existing
        if version_check:
            self._version_check = version_check
        if refresh_state:
            self._refresh_state = refresh_state
        if only_load_props:
            self._only_load_props = set(only_load_props)
        return self

    def _clone(self):
        cls = self.__class__
        q = cls.__new__(cls)
        q.__dict__ = self.__dict__.copy()
        return q

    @property
    def statement(self):
        """The full SELECT statement represented by this Query.
        
        The statement by default will not have disambiguating labels
        applied to the construct unless with_labels(True) is called
        first.
        
        """

        stmt = self._compile_context(labels=self._with_labels).\
                        statement
        if self._params:
            stmt = stmt.params(self._params)
        return stmt._annotate({'_halt_adapt': True})

    def subquery(self):
        """return the full SELECT statement represented by this Query, 
        embedded within an Alias.

        Eager JOIN generation within the query is disabled.

        The statement by default will not have disambiguating labels
        applied to the construct unless with_labels(True) is called
        first.

        """
        return self.enable_eagerloads(False).statement.alias()

    def __clause_element__(self):
        return self.enable_eagerloads(False).with_labels().statement

    @_generative()
    def enable_eagerloads(self, value):
        """Control whether or not eager joins and subqueries are 
        rendered.

        When set to False, the returned Query will not render
        eager joins regardless of :func:`~sqlalchemy.orm.joinedload`,
        :func:`~sqlalchemy.orm.subqueryload` options
        or mapper-level ``lazy='joined'``/``lazy='subquery'``
        configurations.

        This is used primarily when nesting the Query's
        statement into a subquery or other
        selectable.

        """
        self._enable_eagerloads = value

    @_generative()
    def with_labels(self):
        """Apply column labels to the return value of Query.statement.

        Indicates that this Query's `statement` accessor should return
        a SELECT statement that applies labels to all columns in the
        form <tablename>_<columnname>; this is commonly used to
        disambiguate columns from multiple tables which have the same
        name.

        When the `Query` actually issues SQL to load rows, it always
        uses column labeling.

        """
        self._with_labels = True
    
    @_generative()
    def enable_assertions(self, value):
        """Control whether assertions are generated.
        
        When set to False, the returned Query will 
        not assert its state before certain operations, 
        including that LIMIT/OFFSET has not been applied
        when filter() is called, no criterion exists
        when get() is called, and no "from_statement()"
        exists when filter()/order_by()/group_by() etc.
        is called.  This more permissive mode is used by 
        custom Query subclasses to specify criterion or 
        other modifiers outside of the usual usage patterns.
        
        Care should be taken to ensure that the usage 
        pattern is even possible.  A statement applied
        by from_statement() will override any criterion
        set by filter() or order_by(), for example.
        
        """
        self._enable_assertions = value
        
    @property
    def whereclause(self):
        """The WHERE criterion for this Query."""
        return self._criterion

    @_generative()
    def _with_current_path(self, path):
        """indicate that this query applies to objects loaded 
        within a certain path.

        Used by deferred loaders (see strategies.py) which transfer 
        query options from an originating query to a newly generated 
        query intended for the deferred load.

        """
        self._current_path = path

    @_generative(_no_clauseelement_condition)
    def with_polymorphic(self, 
                                    cls_or_mappers, 
                                    selectable=None, discriminator=None):
        """Load columns for descendant mappers of this Query's mapper.

        Using this method will ensure that each descendant mapper's
        tables are included in the FROM clause, and will allow filter()
        criterion to be used against those tables.  The resulting
        instances will also have those columns already loaded so that
        no "post fetch" of those columns will be required.

        :param cls_or_mappers: a single class or mapper, or list of
            class/mappers, which inherit from this Query's mapper.
            Alternatively, it may also be the string ``'*'``, in which case
            all descending mappers will be added to the FROM clause.

        :param selectable: a table or select() statement that will
            be used in place of the generated FROM clause. This argument is
            required if any of the desired mappers use concrete table
            inheritance, since SQLAlchemy currently cannot generate UNIONs
            among tables automatically. If used, the ``selectable`` argument
            must represent the full set of tables and columns mapped by every
            desired mapper. Otherwise, the unaccounted mapped columns will
            result in their table being appended directly to the FROM clause
            which will usually lead to incorrect results.

        :param discriminator: a column to be used as the "discriminator"
            column for the given selectable. If not given, the polymorphic_on
            attribute of the mapper will be used, if any. This is useful for
            mappers that don't have polymorphic loading behavior by default,
            such as concrete table mappers.

        """
        entity = self._generate_mapper_zero()
        entity.set_with_polymorphic(self, 
                                        cls_or_mappers, 
                                        selectable=selectable,
                                        discriminator=discriminator)

    @_generative()
    def yield_per(self, count):
        """Yield only ``count`` rows at a time.

        WARNING: use this method with caution; if the same instance is present
        in more than one batch of rows, end-user changes to attributes will be
        overwritten.

        In particular, it's usually impossible to use this setting with
        eagerly loaded collections (i.e. any lazy='joined' or 'subquery') 
        since those collections will be cleared for a new load when 
        encountered in a subsequent result batch.   In the case of 'subquery'
        loading, the full result for all rows is fetched which generally
        defeats the purpose of :meth:`~sqlalchemy.orm.query.Query.yield_per`.

        Also note that many DBAPIs do not "stream" results, pre-buffering
        all rows before making them available, including mysql-python and 
        psycopg2.  :meth:`~sqlalchemy.orm.query.Query.yield_per` will also 
        set the ``stream_results`` execution
        option to ``True``, which currently is only understood by psycopg2
        and causes server side cursors to be used.
        
        """
        self._yield_per = count
        self._execution_options = self._execution_options.copy()
        self._execution_options['stream_results'] = True
        
    def get(self, ident):
        """Return an instance of the object based on the 
        given identifier, or None if not found.

        The `ident` argument is a scalar or tuple of primary key column values
        in the order of the table def's primary key columns.

        """

        # convert composite types to individual args
        if hasattr(ident, '__composite_values__'):
            ident = ident.__composite_values__()

        key = self._only_mapper_zero(
                    "get() can only be used against a single mapped class."
                ).identity_key_from_primary_key(ident)
        return self._get(key, ident)

    @_generative()
    def correlate(self, *args):
        self._correlate = self._correlate.union(
                                        _orm_selectable(s) 
                                        for s in args)

    @_generative()
    def autoflush(self, setting):
        """Return a Query with a specific 'autoflush' setting.

        Note that a Session with autoflush=False will
        not autoflush, even if this flag is set to True at the
        Query level.  Therefore this flag is usually used only
        to disable autoflush for a specific Query.

        """
        self._autoflush = setting

    @_generative()
    def populate_existing(self):
        """Return a Query that will refresh all instances loaded.

        This includes all entities accessed from the database, including
        secondary entities, eagerly-loaded collection items.

        All changes present on entities which are already present in the
        session will be reset and the entities will all be marked "clean".

        An alternative to populate_existing() is to expire the Session
        fully using session.expire_all().

        """
        self._populate_existing = True

    def with_parent(self, instance, property=None):
        """Add a join criterion corresponding to a relationship to the given
        parent instance.

        instance
          a persistent or detached instance which is related to class
          represented by this query.

        property
          string name of the property which relates this query's class to the
          instance.  if None, the method will attempt to find a suitable
          property.

        Currently, this method only works with immediate parent relationships,
        but in the future may be enhanced to work across a chain of parent
        mappers.

        """
        from sqlalchemy.orm import properties
        mapper = object_mapper(instance)
        if property is None:
            for prop in mapper.iterate_properties:
                if isinstance(prop, properties.PropertyLoader) and \
                    prop.mapper is self._mapper_zero():
                    break
            else:
                raise sa_exc.InvalidRequestError(
                        "Could not locate a property which relates instances "
                        "of class '%s' to instances of class '%s'" % 
                        (
                            self._mapper_zero().class_.__name__,
                            instance.__class__.__name__)
                        )
        else:
            prop = mapper.get_property(property, resolve_synonyms=True)
        return self.filter(prop.compare(
                                operators.eq, 
                                instance, value_is_parent=True))

    @_generative()
    def add_entity(self, entity, alias=None):
        """add a mapped entity to the list of result columns 
        to be returned."""

        if alias is not None:
            entity = aliased(entity, alias)

        self._entities = list(self._entities)
        m = _MapperEntity(self, entity)
        self._setup_aliasizers([m])

    def from_self(self, *entities):
        """return a Query that selects from this Query's 
        SELECT statement.

        \*entities - optional list of entities which will replace
        those being selected.

        """
        fromclause = self.with_labels().enable_eagerloads(False).\
                                    statement.correlate(None)
        q = self._from_selectable(fromclause)
        if entities:
            q._set_entities(entities)
        return q
    
    @_generative()
    def _from_selectable(self, fromclause):
        for attr in ('_statement', '_criterion', '_order_by', '_group_by',
                '_limit', '_offset', '_joinpath', '_joinpoint', 
                '_distinct'
        ):
            self.__dict__.pop(attr, None)
        self._set_select_from(fromclause)
        old_entities = self._entities
        self._entities = []
        for e in old_entities:
            e.adapt_to_selectable(self, self._from_obj[0])

    def values(self, *columns):
        """Return an iterator yielding result tuples corresponding 
        to the given list of columns"""

        if not columns:
            return iter(())
        q = self._clone()
        q._set_entities(columns, entity_wrapper=_ColumnEntity)
        if not q._yield_per:
            q._yield_per = 10
        return iter(q)
    _values = values

    def value(self, column):
        """Return a scalar result corresponding to the given 
        column expression."""
        try:
            # Py3K
            #return self.values(column).__next__()[0]
            # Py2K
            return self.values(column).next()[0]
            # end Py2K
        except StopIteration:
            return None

    @_generative()
    def add_columns(self, *column):
        """Add one or more column expressions to the list 
        of result columns to be returned."""

        self._entities = list(self._entities)
        l = len(self._entities)
        for c in column:
            _ColumnEntity(self, c)
        # _ColumnEntity may add many entities if the
        # given arg is a FROM clause
        self._setup_aliasizers(self._entities[l:])

    @util.pending_deprecation("add_column() superceded by add_columns()")
    def add_column(self, column):
        """Add a column expression to the list of result columns
        to be returned."""
        
        return self.add_columns(column)

    def options(self, *args):
        """Return a new Query object, applying the given list of
        MapperOptions.

        """
        return self._options(False, *args)

    def _conditional_options(self, *args):
        return self._options(True, *args)

    @_generative()
    def _options(self, conditional, *args):
        # most MapperOptions write to the '_attributes' dictionary,
        # so copy that as well
        self._attributes = self._attributes.copy()
        opts = tuple(util.flatten_iterator(args))
        self._with_options = self._with_options + opts
        if conditional:
            for opt in opts:
                opt.process_query_conditionally(self)
        else:
            for opt in opts:
                opt.process_query(self)

    @_generative()
    def with_hint(self, selectable, text, dialect_name=None):
        """Add an indexing hint for the given entity or selectable to 
        this :class:`Query`.
        
        Functionality is passed straight through to 
        :meth:`~sqlalchemy.sql.expression.Select.with_hint`, 
        with the addition that ``selectable`` can be a 
        :class:`Table`, :class:`Alias`, or ORM entity / mapped class 
        /etc.
        """
        mapper, selectable, is_aliased_class = _entity_info(selectable)
        
        self._with_hints += ((selectable, text, dialect_name),)
        
    @_generative()
    def execution_options(self, **kwargs):
        """ Set non-SQL options which take effect during execution.
        
        The options are the same as those accepted by 
        :meth:`sqlalchemy.sql.expression.Executable.execution_options`.
        
        Note that the ``stream_results`` execution option is enabled
        automatically if the :meth:`~sqlalchemy.orm.query.Query.yield_per()`
        method is used.

        """
        self._execution_options = self._execution_options.union(kwargs)

    @_generative()
    def with_lockmode(self, mode):
        """Return a new Query object with the specified locking mode."""

        self._lockmode = mode

    @_generative()
    def params(self, *args, **kwargs):
        """add values for bind parameters which may have been 
        specified in filter().

        parameters may be specified using \**kwargs, or optionally a single
        dictionary as the first positional argument. The reason for both is
        that \**kwargs is convenient, however some parameter dictionaries
        contain unicode keys in which case \**kwargs cannot be used.

        """
        if len(args) == 1:
            kwargs.update(args[0])
        elif len(args) > 0:
            raise sa_exc.ArgumentError(
                            "params() takes zero or one positional argument, "
                            "which is a dictionary.")
        self._params = self._params.copy()
        self._params.update(kwargs)

    @_generative(_no_statement_condition, _no_limit_offset)
    def filter(self, criterion):
        """apply the given filtering criterion to the query and return 
        the newly resulting ``Query``

        the criterion is any sql.ClauseElement applicable to the WHERE clause
        of a select.

        """
        if isinstance(criterion, basestring):
            criterion = sql.text(criterion)

        if criterion is not None and \
                not isinstance(criterion, sql.ClauseElement):
            raise sa_exc.ArgumentError(
                        "filter() argument must be of type "
                        "sqlalchemy.sql.ClauseElement or string")

        criterion = self._adapt_clause(criterion, True, True)

        if self._criterion is not None:
            self._criterion = self._criterion & criterion
        else:
            self._criterion = criterion

    def filter_by(self, **kwargs):
        """apply the given filtering criterion to the query and return 
        the newly resulting ``Query``."""

        clauses = [_entity_descriptor(self._joinpoint_zero(), key)[0] == value
            for key, value in kwargs.iteritems()]

        return self.filter(sql.and_(*clauses))

    @_generative(_no_statement_condition, _no_limit_offset)
    @util.accepts_a_list_as_starargs(list_deprecation='deprecated')
    def order_by(self, *criterion):
        """apply one or more ORDER BY criterion to the query and return 
        the newly resulting ``Query``
        
        All existing ORDER BY settings can be suppressed by 
        passing ``None`` - this will suppress any ORDER BY configured
        on mappers as well.
        
        Alternatively, an existing ORDER BY setting on the Query
        object can be entirely cancelled by passing ``False`` 
        as the value - use this before calling methods where
        an ORDER BY is invalid.
        
        """

        if len(criterion) == 1:
            if criterion[0] is False:
                if '_order_by' in self.__dict__:
                    del self._order_by
                return
            if criterion[0] is None:
                self._order_by = None
                return
                
        criterion = self._adapt_col_list(criterion)

        if self._order_by is False or self._order_by is None:
            self._order_by = criterion
        else:
            self._order_by = self._order_by + criterion

    @_generative(_no_statement_condition, _no_limit_offset)
    @util.accepts_a_list_as_starargs(list_deprecation='deprecated')
    def group_by(self, *criterion):
        """apply one or more GROUP BY criterion to the query and return 
        the newly resulting ``Query``"""

        criterion = list(chain(*[_orm_columns(c) for c in criterion]))

        criterion = self._adapt_col_list(criterion)

        if self._group_by is False:
            self._group_by = criterion
        else:
            self._group_by = self._group_by + criterion

    @_generative(_no_statement_condition, _no_limit_offset)
    def having(self, criterion):
        """apply a HAVING criterion to the query and return the 
        newly resulting ``Query``."""

        if isinstance(criterion, basestring):
            criterion = sql.text(criterion)

        if criterion is not None and \
                not isinstance(criterion, sql.ClauseElement):
            raise sa_exc.ArgumentError(
                    "having() argument must be of type "
                    "sqlalchemy.sql.ClauseElement or string")

        criterion = self._adapt_clause(criterion, True, True)

        if self._having is not None:
            self._having = self._having & criterion
        else:
            self._having = criterion

    def union(self, *q):
        """Produce a UNION of this Query against one or more queries.

        e.g.::

            q1 = sess.query(SomeClass).filter(SomeClass.foo=='bar')
            q2 = sess.query(SomeClass).filter(SomeClass.bar=='foo')

            q3 = q1.union(q2)

        The method accepts multiple Query objects so as to control
        the level of nesting.  A series of ``union()`` calls such as::

            x.union(y).union(z).all()

        will nest on each ``union()``, and produces::

            SELECT * FROM (SELECT * FROM (SELECT * FROM X UNION 
                            SELECT * FROM y) UNION SELECT * FROM Z)

        Whereas::

            x.union(y, z).all()

        produces::

            SELECT * FROM (SELECT * FROM X UNION SELECT * FROM y UNION 
                            SELECT * FROM Z)

        """
        
        
        return self._from_selectable(
                    expression.union(*([self]+ list(q))))

    def union_all(self, *q):
        """Produce a UNION ALL of this Query against one or more queries.

        Works the same way as :meth:`~sqlalchemy.orm.query.Query.union`. See
        that method for usage examples.

        """
        return self._from_selectable(
                    expression.union_all(*([self]+ list(q)))
                )

    def intersect(self, *q):
        """Produce an INTERSECT of this Query against one or more queries.

        Works the same way as :meth:`~sqlalchemy.orm.query.Query.union`. See
        that method for usage examples.

        """
        return self._from_selectable(
                    expression.intersect(*([self]+ list(q)))
                )

    def intersect_all(self, *q):
        """Produce an INTERSECT ALL of this Query against one or more queries.

        Works the same way as :meth:`~sqlalchemy.orm.query.Query.union`. See
        that method for usage examples.

        """
        return self._from_selectable(
                    expression.intersect_all(*([self]+ list(q)))
                )

    def except_(self, *q):
        """Produce an EXCEPT of this Query against one or more queries.

        Works the same way as :meth:`~sqlalchemy.orm.query.Query.union`. See
        that method for usage examples.

        """
        return self._from_selectable(
                    expression.except_(*([self]+ list(q)))
                )

    def except_all(self, *q):
        """Produce an EXCEPT ALL of this Query against one or more queries.

        Works the same way as :meth:`~sqlalchemy.orm.query.Query.union`. See
        that method for usage examples.

        """
        return self._from_selectable(
                    expression.except_all(*([self]+ list(q)))
                )

    @util.accepts_a_list_as_starargs(list_deprecation='deprecated')
    def join(self, *props, **kwargs):
        """Create a join against this ``Query`` object's criterion
        and apply generatively, returning the newly resulting ``Query``.

        Each element in \*props may be:

          * a string property name, i.e. "rooms".  This will join along the
            relationship of the same name from this Query's "primary" mapper,
            if one is present.

          * a class-mapped attribute, i.e. Houses.rooms.  This will create a
            join from "Houses" table to that of the "rooms" relationship.

          * a 2-tuple containing a target class or selectable, and an "ON"
            clause.  The ON clause can be the property name/ attribute like
            above, or a SQL expression.

        e.g.::

            # join along string attribute names
            session.query(Company).join('employees')
            session.query(Company).join('employees', 'tasks')

            # join the Person entity to an alias of itself,
            # along the "friends" relationship
            PAlias = aliased(Person)
            session.query(Person).join((Palias, Person.friends))

            # join from Houses to the "rooms" attribute on the
            # "Colonials" subclass of Houses, then join to the
            # "closets" relationship on Room
            session.query(Houses).join(Colonials.rooms, Room.closets)

            # join from Company entities to the "employees" collection,
            # using "people JOIN engineers" as the target.  Then join
            # to the "computers" collection on the Engineer entity.
            session.query(Company).\
                        join((people.join(engineers), 'employees'),
                        Engineer.computers)

            # join from Articles to Keywords, using the "keywords" attribute.
            # assume this is a many-to-many relationship.
            session.query(Article).join(Article.keywords)

            # same thing, but spelled out entirely explicitly
            # including the association table.
            session.query(Article).join(
                (article_keywords,
                Articles.id==article_keywords.c.article_id),
                (Keyword, Keyword.id==article_keywords.c.keyword_id)
                )

        \**kwargs include:

            aliased - when joining, create anonymous aliases of each table.
            This is used for self-referential joins or multiple joins to the
            same table. Consider usage of the aliased(SomeClass) construct as
            a more explicit approach to this.

            from_joinpoint - when joins are specified using string property
            names, locate the property from the mapper found in the most
            recent previous join() call, instead of from the root entity.

        """
        aliased, from_joinpoint = kwargs.pop('aliased', False),\
                                    kwargs.pop('from_joinpoint', False)
        if kwargs:
            raise TypeError("unknown arguments: %s" %
                                ','.join(kwargs.iterkeys()))
        return self._join(props, 
                            outerjoin=False, create_aliases=aliased, 
                            from_joinpoint=from_joinpoint)

    @util.accepts_a_list_as_starargs(list_deprecation='deprecated')
    def outerjoin(self, *props, **kwargs):
        """Create a left outer join against this ``Query`` object's criterion
        and apply generatively, retunring the newly resulting ``Query``.

        Usage is the same as the ``join()`` method.

        """
        aliased, from_joinpoint = kwargs.pop('aliased', False), \
                                kwargs.pop('from_joinpoint', False)
        if kwargs:
            raise TypeError("unknown arguments: %s" %
                    ','.join(kwargs.iterkeys()))
        return self._join(props, 
                            outerjoin=True, create_aliases=aliased, 
                            from_joinpoint=from_joinpoint)

    @_generative(_no_statement_condition, _no_limit_offset)
    def _join(self, keys, outerjoin, create_aliases, from_joinpoint):
        """consumes arguments from join() or outerjoin(), places them into a
        consistent format with which to form the actual JOIN constructs.
        
        """
        self._polymorphic_adapters = self._polymorphic_adapters.copy()

        if not from_joinpoint:
            self._reset_joinpoint()
        
        if len(keys) >= 2 and \
                isinstance(keys[1], expression.ClauseElement) and \
                not isinstance(keys[1], expression.FromClause):
            raise sa_exc.ArgumentError(
                        "You appear to be passing a clause expression as the second "
                        "argument to query.join().   Did you mean to use the form "
                        "query.join((target, onclause))?  Note the tuple.")
            
        for arg1 in util.to_list(keys):
            if isinstance(arg1, tuple):
                arg1, arg2 = arg1
            else:
                arg2 = None

            # determine onclause/right_entity.  there
            # is a little bit of legacy behavior still at work here
            # which means they might be in either order.  may possibly
            # lock this down to (right_entity, onclause) in 0.6.
            if isinstance(arg1, (interfaces.PropComparator, basestring)):
                right_entity, onclause = arg2, arg1
            else:
                right_entity, onclause = arg1, arg2

            left_entity = prop = None
            
            if isinstance(onclause, basestring):
                left_entity = self._joinpoint_zero()

                descriptor, prop = _entity_descriptor(left_entity, onclause)
                onclause = descriptor
            
            # check for q.join(Class.propname, from_joinpoint=True)
            # and Class is that of the current joinpoint
            elif from_joinpoint and \
                        isinstance(onclause, interfaces.PropComparator):
                left_entity = onclause.parententity
                
                left_mapper, left_selectable, left_is_aliased = \
                                    _entity_info(self._joinpoint_zero())
                if left_mapper is left_entity:
                    left_entity = self._joinpoint_zero()
                    descriptor, prop = _entity_descriptor(left_entity,
                                                            onclause.key)
                    onclause = descriptor

            if isinstance(onclause, interfaces.PropComparator):
                if right_entity is None:
                    right_entity = onclause.property.mapper
                    of_type = getattr(onclause, '_of_type', None)
                    if of_type:
                        right_entity = of_type
                    else:
                        right_entity = onclause.property.mapper
            
                left_entity = onclause.parententity
                
                prop = onclause.property
                if not isinstance(onclause,  attributes.QueryableAttribute):
                    onclause = prop

                if not create_aliases:
                    # check for this path already present.
                    # don't render in that case.
                    if (left_entity, right_entity, prop.key) in \
                                    self._joinpoint:
                        self._joinpoint = \
                                    self._joinpoint[
                                    (left_entity, right_entity, prop.key)]
                        continue

            elif onclause is not None and right_entity is None:
                # TODO: no coverage here
                raise NotImplementedError("query.join(a==b) not supported.")
            
            self._join_left_to_right(
                                left_entity, 
                                right_entity, onclause, 
                                outerjoin, create_aliases, prop)

    def _join_left_to_right(self, left, right, 
                            onclause, outerjoin, create_aliases, prop):
        """append a JOIN to the query's from clause."""
        
        if left is None:
            left = self._joinpoint_zero()

        if left is right and \
                not create_aliases:
            raise sa_exc.InvalidRequestError(
                        "Can't construct a join from %s to %s, they "
                        "are the same entity" % 
                        (left, right))
            
        left_mapper, left_selectable, left_is_aliased = _entity_info(left)
        right_mapper, right_selectable, is_aliased_class = _entity_info(right)

        if right_mapper and prop and \
                not right_mapper.common_parent(prop.mapper):
            raise sa_exc.InvalidRequestError(
                    "Join target %s does not correspond to "
                    "the right side of join condition %s" % (right, onclause)
            )

        if not right_mapper and prop:
            right_mapper = prop.mapper

        need_adapter = False

        if right_mapper and right is right_selectable:
            if not right_selectable.is_derived_from(
                                    right_mapper.mapped_table):
                raise sa_exc.InvalidRequestError(
                    "Selectable '%s' is not derived from '%s'" %
                    (right_selectable.description,
                    right_mapper.mapped_table.description))

            if not isinstance(right_selectable, expression.Alias):
                right_selectable = right_selectable.alias()

            right = aliased(right_mapper, right_selectable)
            need_adapter = True

        aliased_entity = right_mapper and \
                            not is_aliased_class and \
                            (
                                right_mapper.with_polymorphic or
                                isinstance(
                                    right_mapper.mapped_table,
                                    expression.Join)
                            )

        if not need_adapter and (create_aliases or aliased_entity):
            right = aliased(right)
            need_adapter = True

        # if joining on a MapperProperty path,
        # track the path to prevent redundant joins
        if not create_aliases and prop:

            self._joinpoint = jp = {
                '_joinpoint_entity':right,
                'prev':((left, right, prop.key), self._joinpoint)
            }

            # copy backwards to the root of the _joinpath
            # dict, so that no existing dict in the path is mutated
            while 'prev' in jp:
                f, prev = jp['prev']
                prev = prev.copy()
                prev[f] = jp
                jp['prev'] = (f, prev)
                jp = prev

            self._joinpath = jp

        else:
            self._joinpoint = {
                '_joinpoint_entity':right
            }
        
        # if an alias() of the right side was generated here,
        # apply an adapter to all subsequent filter() calls
        # until reset_joinpoint() is called.
        if need_adapter:
            self._filter_aliases = ORMAdapter(right,
                        equivalents=right_mapper._equivalent_columns,
                        chain_to=self._filter_aliases)

        # if the onclause is a ClauseElement, adapt it with any 
        # adapters that are in place right now
        if isinstance(onclause, expression.ClauseElement):
            onclause = self._adapt_clause(onclause, True, True)
        
        # if an alias() on the right side was generated,
        # which is intended to wrap a the right side in a subquery,
        # ensure that columns retrieved from this target in the result
        # set are also adapted.
        if aliased_entity:
            self.__mapper_loads_polymorphically_with(
                        right_mapper,
                        ORMAdapter(
                            right, 
                            equivalents=right_mapper._equivalent_columns
                        )
                    )
        
        join_to_left = not is_aliased_class and not left_is_aliased

        if self._from_obj:
            replace_clause_index, clause = sql_util.find_join_source(
                                                    self._from_obj, 
                                                    left_selectable)
            if clause is not None:
                # the entire query's FROM clause is an alias of itself (i.e.
                # from_self(), similar). if the left clause is that one,
                # ensure it aliases to the left side.
                if self._from_obj_alias and clause is self._from_obj[0]:
                    join_to_left = True

                clause = orm_join(clause, 
                                    right, 
                                    onclause, isouter=outerjoin, 
                                    join_to_left=join_to_left)

                self._from_obj = \
                        self._from_obj[:replace_clause_index] + \
                        (clause, ) + \
                        self._from_obj[replace_clause_index + 1:]
                return

        if left_mapper:
            for ent in self._entities:
                if ent.corresponds_to(left):
                    clause = ent.selectable
                    break
            else:
                clause = left
        else:
            clause = None

        if clause is None:
            raise sa_exc.InvalidRequestError(
                    "Could not find a FROM clause to join from")

        clause = orm_join(clause, right, onclause, 
                                isouter=outerjoin, join_to_left=join_to_left)
            
        self._from_obj = self._from_obj + (clause,)

    def _reset_joinpoint(self):
        self._joinpoint = self._joinpath
        self._filter_aliases = None

    @_generative(_no_statement_condition)
    def reset_joinpoint(self):
        """return a new Query reset the 'joinpoint' of this Query reset
        back to the starting mapper.  Subsequent generative calls will
        be constructed from the new joinpoint.

        Note that each call to join() or outerjoin() also starts from
        the root.

        """
        self._reset_joinpoint()

    @_generative(_no_clauseelement_condition)
    def select_from(self, *from_obj):
        """Set the `from_obj` parameter of the query and return the newly
        resulting ``Query``.  This replaces the table which this Query selects
        from with the given table.
        
        ``select_from()`` also accepts class arguments. Though usually not
        necessary, can ensure that the full selectable of the given mapper is
        applied, e.g. for joined-table mappers.

        """
        
        obj = []
        for fo in from_obj:
            if _is_mapped_class(fo):
                mapper, selectable, is_aliased_class = _entity_info(fo)
                obj.append(selectable)
            elif not isinstance(fo, expression.FromClause):
                raise sa_exc.ArgumentError(
                            "select_from() accepts FromClause objects only.")
            else:
                obj.append(fo)  
                
        self._set_select_from(*obj)

    def __getitem__(self, item):
        if isinstance(item, slice):
            start, stop, step = util.decode_slice(item)

            if isinstance(stop, int) and \
                isinstance(start, int) and \
                stop - start <= 0:
                return []

            # perhaps we should execute a count() here so that we
            # can still use LIMIT/OFFSET ?
            elif (isinstance(start, int) and start < 0) \
                or (isinstance(stop, int) and stop < 0):
                return list(self)[item]

            res = self.slice(start, stop)
            if step is not None:
                return list(res)[None:None:item.step]
            else:
                return list(res)
        else:
            return list(self[item:item+1])[0]

    @_generative(_no_statement_condition)
    def slice(self, start, stop):
        """apply LIMIT/OFFSET to the ``Query`` based on a "
        "range and return the newly resulting ``Query``."""
        
        if start is not None and stop is not None:
            self._offset = (self._offset or 0) + start
            self._limit = stop - start
        elif start is None and stop is not None:
            self._limit = stop
        elif start is not None and stop is None:
            self._offset = (self._offset or 0) + start

    @_generative(_no_statement_condition)
    def limit(self, limit):
        """Apply a ``LIMIT`` to the query and return the newly resulting

        ``Query``.

        """
        self._limit = limit

    @_generative(_no_statement_condition)
    def offset(self, offset):
        """Apply an ``OFFSET`` to the query and return the newly resulting
        ``Query``.

        """
        self._offset = offset

    @_generative(_no_statement_condition)
    def distinct(self):
        """Apply a ``DISTINCT`` to the query and return the newly resulting
        ``Query``.

        """
        self._distinct = True

    def all(self):
        """Return the results represented by this ``Query`` as a list.

        This results in an execution of the underlying query.

        """
        return list(self)

    @_generative(_no_clauseelement_condition)
    def from_statement(self, statement):
        """Execute the given SELECT statement and return results.

        This method bypasses all internal statement compilation, and the
        statement is executed without modification.

        The statement argument is either a string, a ``select()`` construct,
        or a ``text()`` construct, and should return the set of columns
        appropriate to the entity class represented by this ``Query``.

        Also see the ``instances()`` method.

        """
        if isinstance(statement, basestring):
            statement = sql.text(statement)

        if not isinstance(statement, 
                            (expression._TextClause,
                            expression._SelectBaseMixin)):
            raise sa_exc.ArgumentError(
                            "from_statement accepts text(), select(), "
                            "and union() objects only.")

        self._statement = statement

    def first(self):
        """Return the first result of this ``Query`` or 
        None if the result doesn't contain any row.
           
        first() applies a limit of one within the generated SQL, so that
        only one primary entity row is generated on the server side 
        (note this may consist of multiple result rows if join-loaded 
        collections are present).

        Calling ``first()`` results in an execution of the underlying query.

        """
        if self._statement is not None:
            ret = list(self)[0:1]
        else:
            ret = list(self[0:1])
        if len(ret) > 0:
            return ret[0]
        else:
            return None

    def one(self):
        """Return exactly one result or raise an exception.

        Raises ``sqlalchemy.orm.exc.NoResultFound`` if the query selects 
        no rows.  Raises ``sqlalchemy.orm.exc.MultipleResultsFound`` 
        if multiple object identities are returned, or if multiple
        rows are returned for a query that does not return object
        identities.
        
        Note that an entity query, that is, one which selects one or
        more mapped classes as opposed to individual column attributes,
        may ultimately represent many rows but only one row of 
        unique entity or entities - this is a successful result for one().

        Calling ``one()`` results in an execution of the underlying query.
        As of 0.6, ``one()`` fully fetches all results instead of applying 
        any kind of limit, so that the "unique"-ing of entities does not 
        conceal multiple object identities.

        """
        ret = list(self)
        
        l = len(ret)
        if l == 1:
            return ret[0]
        elif l == 0:
            raise orm_exc.NoResultFound("No row was found for one()")
        else:
            raise orm_exc.MultipleResultsFound(
                "Multiple rows were found for one()")

    def scalar(self):
        """Return the first element of the first result or None
        if no rows present.  If multiple rows are returned,
        raises MultipleResultsFound.

          >>> session.query(Item).scalar()
          <Item>
          >>> session.query(Item.id).scalar()
          1
          >>> session.query(Item.id).filter(Item.id < 0).scalar()
          None
          >>> session.query(Item.id, Item.name).scalar()
          1
          >>> session.query(func.count(Parent.id)).scalar()
          20

        This results in an execution of the underlying query.

        """
        try:
            ret = self.one()
            if not isinstance(ret, tuple):
                return ret
            return ret[0]
        except orm_exc.NoResultFound:
            return None

    def __iter__(self):
        context = self._compile_context()
        context.statement.use_labels = True
        if self._autoflush and not self._populate_existing:
            self.session._autoflush()
        return self._execute_and_instances(context)

    def _execute_and_instances(self, querycontext):
        result = self.session.execute(
                        querycontext.statement, params=self._params,
                        mapper=self._mapper_zero_or_none())
        return self.instances(result, querycontext)

    def instances(self, cursor, __context=None):
        """Given a ResultProxy cursor as returned by connection.execute(),
        return an ORM result as an iterator.

        e.g.::

            result = engine.execute("select * from users")
            for u in session.query(User).instances(result):
                print u
        """
        session = self.session

        context = __context
        if context is None:
            context = QueryContext(self)

        context.runid = _new_runid()

        filtered = bool(list(self._mapper_entities))
        single_entity = filtered and len(self._entities) == 1

        if filtered:
            if single_entity:
                filter = lambda x: util.unique_list(x, util.IdentitySet)
            else:
                filter = util.unique_list
        else:
            filter = None

        custom_rows = single_entity and \
                        'append_result' in self._entities[0].extension

        (process, labels) = \
                    zip(*[
                        query_entity.row_processor(self, context, custom_rows)
                        for query_entity in self._entities
                    ])

        if not single_entity:
            labels = [l for l in labels if l]

        while True:
            context.progress = {}
            context.partials = {}

            if self._yield_per:
                fetch = cursor.fetchmany(self._yield_per)
                if not fetch:
                    break
            else:
                fetch = cursor.fetchall()

            if custom_rows:
                rows = []
                for row in fetch:
                    process[0](row, rows)
            elif single_entity:
                rows = [process[0](row, None) for row in fetch]
            else:
                rows = [util.NamedTuple([proc(row, None) for proc in process],
                                        labels) for row in fetch]

            if filter:
                rows = filter(rows)

            if context.refresh_state and self._only_load_props \
                        and context.refresh_state in context.progress:
                context.refresh_state.commit(
                        context.refresh_state.dict, self._only_load_props)
                context.progress.pop(context.refresh_state)

            session._finalize_loaded(context.progress)

            for ii, (dict_, attrs) in context.partials.iteritems():
                ii.commit(dict_, attrs)

            for row in rows:
                yield row

            if not self._yield_per:
                break

    def merge_result(self, iterator, load=True):
        """Merge a result into this Query's Session.
        
        Given an iterator returned by a Query of the same structure as this
        one, return an identical iterator of results, with all mapped
        instances merged into the session using Session.merge(). This is an
        optimized method which will merge all mapped instances, preserving the
        structure of the result rows and unmapped columns with less method
        overhead than that of calling Session.merge() explicitly for each
        value.
        
        The structure of the results is determined based on the column list of
        this Query - if these do not correspond, unchecked errors will occur.
        
        The 'load' argument is the same as that of Session.merge().
        
        """
        
        session = self.session
        if load:
            # flush current contents if we expect to load data
            session._autoflush()
            
        autoflush = session.autoflush
        try:
            session.autoflush = False
            single_entity = len(self._entities) == 1
            if single_entity:
                if isinstance(self._entities[0], _MapperEntity):
                    result = [session._merge(
                            attributes.instance_state(instance), 
                            attributes.instance_dict(instance), 
                            load=load, _recursive={})
                            for instance in iterator]
                else:
                    result = list(iterator)
            else:
                mapped_entities = [i for i, e in enumerate(self._entities) 
                                        if isinstance(e, _MapperEntity)]
                result = []
                for row in iterator:
                    newrow = list(row)
                    for i in mapped_entities:
                        newrow[i] = session._merge(
                                attributes.instance_state(newrow[i]), 
                                attributes.instance_dict(newrow[i]), 
                                load=load, _recursive={})
                    result.append(util.NamedTuple(newrow, row._labels))  
            
            return iter(result)
        finally:
            session.autoflush = autoflush
        
        
    def _get(self, key=None, ident=None, refresh_state=None, lockmode=None,
                                        only_load_props=None, passive=None):
        lockmode = lockmode or self._lockmode
        
        mapper = self._mapper_zero()
        if not self._populate_existing and \
                not refresh_state and \
                not mapper.always_refresh and \
                lockmode is None:
            instance = self.session.identity_map.get(key)
            if instance:
                # item present in identity map with a different class
                if not issubclass(instance.__class__, mapper.class_):
                    return None
                    
                state = attributes.instance_state(instance)
                
                # expired - ensure it still exists
                if state.expired:
                    if passive is attributes.PASSIVE_NO_FETCH:
                        return attributes.PASSIVE_NO_RESULT
                    try:
                        state()
                    except orm_exc.ObjectDeletedError:
                        self.session._remove_newly_deleted(state)
                        return None
                return instance
            elif passive is attributes.PASSIVE_NO_FETCH:
                return attributes.PASSIVE_NO_RESULT

        if ident is None:
            if key is not None:
                ident = key[1]
        else:
            ident = util.to_list(ident)

        if refresh_state is None:
            q = self._clone()
            q._get_condition()
        else:
            q = self._clone()

        if ident is not None:
            (_get_clause, _get_params) = mapper._get_clause
            
            # None present in ident - turn those comparisons
            # into "IS NULL"
            if None in ident:
                nones = set([
                            _get_params[col].key for col, value in
                             zip(mapper.primary_key, ident) if value is None
                            ])
                _get_clause = sql_util.adapt_criterion_to_null(
                                                _get_clause, nones)
                
            _get_clause = q._adapt_clause(_get_clause, True, False)
            q._criterion = _get_clause

            params = dict([
                (_get_params[primary_key].key, id_val)
                for id_val, primary_key in zip(ident, mapper.primary_key)
            ])

            if len(params) != len(mapper.primary_key):
                raise sa_exc.InvalidRequestError(
                "Incorrect number of values in identifier to formulate "
                "primary key for query.get(); primary key columns are %s" %
                ','.join("'%s'" % c for c in mapper.primary_key))
                        
            q._params = params

        if lockmode is not None:
            q._lockmode = lockmode
        q._get_options(
            populate_existing=bool(refresh_state),
            version_check=(lockmode is not None),
            only_load_props=only_load_props,
            refresh_state=refresh_state)
        q._order_by = None

        try:
            return q.one()
        except orm_exc.NoResultFound:
            return None

    @property
    def _select_args(self):
        return {
            'limit':self._limit,
            'offset':self._offset,
            'distinct':self._distinct,
            'group_by':self._group_by or None,
            'having':self._having
        }

    @property
    def _should_nest_selectable(self):
        kwargs = self._select_args
        return (kwargs.get('limit') is not None or
                kwargs.get('offset') is not None or
                kwargs.get('distinct', False))

    def count(self):
        """Return a count of rows this Query would return.
        
        For simple entity queries, count() issues
        a SELECT COUNT, and will specifically count the primary
        key column of the first entity only.  If the query uses 
        LIMIT, OFFSET, or DISTINCT, count() will wrap the statement 
        generated by this Query in a subquery, from which a SELECT COUNT
        is issued, so that the contract of "how many rows
        would be returned?" is honored.
        
        For queries that request specific columns or expressions, 
        count() again makes no assumptions about those expressions
        and will wrap everything in a subquery.  Therefore,
        ``Query.count()`` is usually not what you want in this case.   
        To count specific columns, often in conjunction with 
        GROUP BY, use ``func.count()`` as an individual column expression
        instead of ``Query.count()``.  See the ORM tutorial
        for an example.

        """
        should_nest = [self._should_nest_selectable]
        def ent_cols(ent):
            if isinstance(ent, _MapperEntity):
                return ent.mapper.primary_key
            else:
                should_nest[0] = True
                return [ent.column]

        return self._col_aggregate(sql.literal_column('1'), sql.func.count,
            nested_cols=chain(*[ent_cols(ent) for ent in self._entities]),
            should_nest = should_nest[0]
        )

    def _col_aggregate(self, col, func, nested_cols=None, should_nest=False):
        context = QueryContext(self)

        for entity in self._entities:
            entity.setup_context(self, context)

        if context.from_clause:
            from_obj = list(context.from_clause)
        else:
            from_obj = context.froms

        self._adjust_for_single_inheritance(context)

        whereclause  = context.whereclause

        if should_nest:
            if not nested_cols:
                nested_cols = [col]
            else:
                nested_cols = list(nested_cols)
            s = sql.select(nested_cols, whereclause, 
                        from_obj=from_obj, use_labels=True,
                        **self._select_args)
            s = s.alias()
            s = sql.select(
                [func(s.corresponding_column(col) or col)]).select_from(s)
        else:
            s = sql.select([func(col)], whereclause, from_obj=from_obj,
            **self._select_args)

        if self._autoflush and not self._populate_existing:
            self.session._autoflush()
        return self.session.scalar(s, params=self._params,
            mapper=self._mapper_zero())

    def delete(self, synchronize_session='evaluate'):
        """Perform a bulk delete query.

        Deletes rows matched by this query from the database.

        :param synchronize_session: chooses the strategy for the removal of
            matched objects from the session. Valid values are:
        
            False - don't synchronize the session. This option is the most
            efficient and is reliable once the session is expired, which
            typically occurs after a commit(), or explicitly using
            expire_all(). Before the expiration, objects may still remain in
            the session which were in fact deleted which can lead to confusing
            results if they are accessed via get() or already loaded
            collections.

            'fetch' - performs a select query before the delete to find
            objects that are matched by the delete query and need to be
            removed from the session. Matched objects are removed from the
            session.

            'evaluate' - Evaluate the query's criteria in Python straight on
            the objects in the session. If evaluation of the criteria isn't
            implemented, an error is raised.  In that case you probably 
            want to use the 'fetch' strategy as a fallback.
          
            The expression evaluator currently doesn't account for differing
            string collations between the database and Python.

        Returns the number of rows deleted, excluding any cascades.

        The method does *not* offer in-Python cascading of relationships - it
        is assumed that ON DELETE CASCADE is configured for any foreign key
        references which require it. The Session needs to be expired (occurs
        automatically after commit(), or call expire_all()) in order for the
        state of dependent objects subject to delete or delete-orphan cascade
        to be correctly represented.

        Also, the ``before_delete()`` and ``after_delete()``
        :class:`~sqlalchemy.orm.interfaces.MapperExtension` methods are not
        called from this method. For a delete hook here, use the
        ``after_bulk_delete()``
        :class:`~sqlalchemy.orm.interfaces.MapperExtension` method.

        """
        #TODO: lots of duplication and ifs - probably needs to be 
        # refactored to strategies
        #TODO: cascades need handling.

        if synchronize_session not in [False, 'evaluate', 'fetch']:
            raise sa_exc.ArgumentError(
                            "Valid strategies for session "
                            "synchronization are False, 'evaluate' and "
                            "'fetch'")
        self._no_select_modifiers("delete")

        self = self.enable_eagerloads(False)

        context = self._compile_context()
        if len(context.statement.froms) != 1 or \
                    not isinstance(context.statement.froms[0], schema.Table):
            raise sa_exc.ArgumentError("Only deletion via a single table "
                                        "query is currently supported")
        primary_table = context.statement.froms[0]

        session = self.session

        if synchronize_session == 'evaluate':
            try:
                evaluator_compiler = evaluator.EvaluatorCompiler()
                if self.whereclause is not None:
                    eval_condition = evaluator_compiler.process(
                                                            self.whereclause)
                else:
                    def eval_condition(obj):
                        return True
                    
            except evaluator.UnevaluatableError:
                raise sa_exc.InvalidRequestError(
                    "Could not evaluate current criteria in Python.  "
                    "Specify 'fetch' or False for the synchronize_session "
                    "parameter.")

        delete_stmt = sql.delete(primary_table, context.whereclause)

        if synchronize_session == 'fetch':
            #TODO: use RETURNING when available
            select_stmt = context.statement.with_only_columns(
                                                primary_table.primary_key)
            matched_rows = session.execute(
                                        select_stmt,
                                        params=self._params).fetchall()

        if self._autoflush:
            session._autoflush()
        result = session.execute(delete_stmt, params=self._params)

        if synchronize_session == 'evaluate':
            target_cls = self._mapper_zero().class_

            #TODO: detect when the where clause is a trivial primary key match
            objs_to_expunge = [
                                obj for (cls, pk),obj in
                                session.identity_map.iteritems()
                                if issubclass(cls, target_cls) and
                                eval_condition(obj)]
            for obj in objs_to_expunge:
                session._remove_newly_deleted(attributes.instance_state(obj))
        elif synchronize_session == 'fetch':
            target_mapper = self._mapper_zero()
            for primary_key in matched_rows:
                identity_key = target_mapper.identity_key_from_primary_key(
                                                            list(primary_key))
                if identity_key in session.identity_map:
                    session._remove_newly_deleted(
                        attributes.instance_state(
                            session.identity_map[identity_key]
                        )
                    )

        for ext in session.extensions:
            ext.after_bulk_delete(session, self, context, result)

        return result.rowcount

    def update(self, values, synchronize_session='evaluate'):
        """Perform a bulk update query.

        Updates rows matched by this query in the database.

        :param values: a dictionary with attributes names as keys and literal
          values or sql expressions as values.

        :param synchronize_session: chooses the strategy to update the
            attributes on objects in the session. Valid values are:

            False - don't synchronize the session. This option is the most
            efficient and is reliable once the session is expired, which
            typically occurs after a commit(), or explicitly using
            expire_all(). Before the expiration, updated objects may still
            remain in the session with stale values on their attributes, which
            can lead to confusing results.
              
            'fetch' - performs a select query before the update to find
            objects that are matched by the update query. The updated
            attributes are expired on matched objects.

            'evaluate' - Evaluate the Query's criteria in Python straight on
            the objects in the session. If evaluation of the criteria isn't
            implemented, an exception is raised.

            The expression evaluator currently doesn't account for differing
            string collations between the database and Python.

        Returns the number of rows matched by the update.

        The method does *not* offer in-Python cascading of relationships - it
        is assumed that ON UPDATE CASCADE is configured for any foreign key
        references which require it.

        The Session needs to be expired (occurs automatically after commit(),
        or call expire_all()) in order for the state of dependent objects
        subject foreign key cascade to be correctly represented.

        Also, the ``before_update()`` and ``after_update()``
        :class:`~sqlalchemy.orm.interfaces.MapperExtension` methods are not
        called from this method. For an update hook here, use the
        ``after_bulk_update()``
        :class:`~sqlalchemy.orm.interfaces.SessionExtension` method.

        """

        #TODO: value keys need to be mapped to corresponding sql cols and
        # instr.attr.s to string keys
        #TODO: updates of manytoone relationships need to be converted to 
        # fk assignments
        #TODO: cascades need handling.

        if synchronize_session == 'expire':
            util.warn_deprecated("The 'expire' value as applied to "
                                    "the synchronize_session argument of "
                                    "query.update() is now called 'fetch'")
            synchronize_session = 'fetch'
            
        if synchronize_session not in [False, 'evaluate', 'fetch']:
            raise sa_exc.ArgumentError(
                            "Valid strategies for session synchronization "
                            "are False, 'evaluate' and 'fetch'")
        self._no_select_modifiers("update")

        self = self.enable_eagerloads(False)

        context = self._compile_context()
        if len(context.statement.froms) != 1 or \
                    not isinstance(context.statement.froms[0], schema.Table):
            raise sa_exc.ArgumentError(
                            "Only update via a single table query is "
                            "currently supported")
        primary_table = context.statement.froms[0]

        session = self.session

        if synchronize_session == 'evaluate':
            try:
                evaluator_compiler = evaluator.EvaluatorCompiler()
                if self.whereclause is not None:
                    eval_condition = evaluator_compiler.process(
                                                    self.whereclause)
                else:
                    def eval_condition(obj):
                        return True

                value_evaluators = {}
                for key,value in values.iteritems():
                    key = expression._column_as_key(key)
                    value_evaluators[key] = evaluator_compiler.process(expression._literal_as_binds(value))
            except evaluator.UnevaluatableError:
                raise sa_exc.InvalidRequestError(
                        "Could not evaluate current criteria in Python. "
                        "Specify 'fetch' or False for the "
                        "synchronize_session parameter.")

        update_stmt = sql.update(primary_table, context.whereclause, values)

        if synchronize_session == 'fetch':
            select_stmt = context.statement.with_only_columns(
                                                primary_table.primary_key)
            matched_rows = session.execute(
                                        select_stmt,
                                        params=self._params).fetchall()

        if self._autoflush:
            session._autoflush()
        result = session.execute(update_stmt, params=self._params)

        if synchronize_session == 'evaluate':
            target_cls = self._mapper_zero().class_

            for (cls, pk),obj in session.identity_map.iteritems():
                evaluated_keys = value_evaluators.keys()

                if issubclass(cls, target_cls) and eval_condition(obj):
                    state, dict_ = attributes.instance_state(obj),\
                                            attributes.instance_dict(obj)

                    # only evaluate unmodified attributes
                    to_evaluate = state.unmodified.intersection(
                                                            evaluated_keys)
                    for key in to_evaluate:
                        dict_[key] = value_evaluators[key](obj)

                    state.commit(dict_, list(to_evaluate))

                    # expire attributes with pending changes 
                    # (there was no autoflush, so they are overwritten)
                    state.expire_attributes(dict_,
                                    set(evaluated_keys).
                                        difference(to_evaluate))

        elif synchronize_session == 'fetch':
            target_mapper = self._mapper_zero()

            for primary_key in matched_rows:
                identity_key = target_mapper.identity_key_from_primary_key(
                                                            list(primary_key))
                if identity_key in session.identity_map:
                    session.expire(
                                session.identity_map[identity_key], 
                                [expression._column_as_key(k) for k in values]
                                )

        for ext in session.extensions:
            ext.after_bulk_update(session, self, context, result)

        return result.rowcount

    def _compile_context(self, labels=True):
        context = QueryContext(self)

        if context.statement is not None:
            return context

        if self._lockmode:
            try:
                for_update = {'read': 'read',
                              'update': True,
                              'update_nowait': 'nowait',
                              None: False}[self._lockmode]
            except KeyError:
                raise sa_exc.ArgumentError(
                            "Unknown lockmode %r" % self._lockmode)
        else:
            for_update = False

        for entity in self._entities:
            entity.setup_context(self, context)
        
        for rec in context.create_eager_joins:
            strategy = rec[0]
            strategy(*rec[1:])
            
        eager_joins = context.eager_joins.values()

        if context.from_clause:
            # "load from explicit FROMs" mode, 
            # i.e. when select_from() or join() is used
            froms = list(context.from_clause)  
        else:
            # "load from discrete FROMs" mode, 
            # i.e. when each _MappedEntity has its own FROM
            froms = context.froms   

        self._adjust_for_single_inheritance(context)

        if not context.primary_columns:
            if self._only_load_props:
                raise sa_exc.InvalidRequestError(
                            "No column-based properties specified for "
                            "refresh operation. Use session.expire() "
                            "to reload collections and related items.")
            else:
                raise sa_exc.InvalidRequestError(
                            "Query contains no columns with which to "
                            "SELECT from.")

        if context.multi_row_eager_loaders and self._should_nest_selectable:
            # for eager joins present and LIMIT/OFFSET/DISTINCT, 
            # wrap the query inside a select,
            # then append eager joins onto that

            if context.order_by:
                order_by_col_expr = list(
                                        chain(*[
                                            sql_util.find_columns(o) 
                                            for o in context.order_by
                                        ])
                                    )
            else:
                context.order_by = None
                order_by_col_expr = []

            inner = sql.select(
                        context.primary_columns + order_by_col_expr,
                        context.whereclause,
                        from_obj=froms,
                        use_labels=labels,
                        correlate=False,
                        order_by=context.order_by,
                        **self._select_args
                    )
            
            for hint in self._with_hints:
                inner = inner.with_hint(*hint)
                
            if self._correlate:
                inner = inner.correlate(*self._correlate)

            inner = inner.alias()

            equivs = self.__all_equivs()

            context.adapter = sql_util.ColumnAdapter(inner, equivs)

            statement = sql.select(
                                [inner] + context.secondary_columns, 
                                for_update=for_update, 
                                use_labels=labels)
                                
            if self._execution_options:
                statement = statement.execution_options(
                                                **self._execution_options)

            from_clause = inner
            for eager_join in eager_joins:
                # EagerLoader places a 'stop_on' attribute on the join,
                # giving us a marker as to where the "splice point" of 
                # the join should be
                from_clause = sql_util.splice_joins(
                                            from_clause, 
                                            eager_join, eager_join.stop_on)

            statement.append_from(from_clause)

            if context.order_by:
                    statement.append_order_by(
                        *context.adapter.copy_and_process(
                            context.order_by
                        )
                    )

            statement.append_order_by(*context.eager_order_by)
        else:
            if not context.order_by:
                context.order_by = None

            if self._distinct and context.order_by:
                order_by_col_expr = list(
                                        chain(*[
                                            sql_util.find_columns(o) 
                                            for o in context.order_by
                                        ])
                                    )
                context.primary_columns += order_by_col_expr

            froms += tuple(context.eager_joins.values())

            statement = sql.select(
                            context.primary_columns +
                                    context.secondary_columns,
                            context.whereclause,
                            from_obj=froms,
                            use_labels=labels,
                            for_update=for_update,
                            correlate=False,
                            order_by=context.order_by,
                            **self._select_args
                        )

            for hint in self._with_hints:
                statement = statement.with_hint(*hint)
                        
            if self._execution_options:
                statement = statement.execution_options(
                                            **self._execution_options)

            if self._correlate:
                statement = statement.correlate(*self._correlate)

            if context.eager_order_by:
                statement.append_order_by(*context.eager_order_by)

        context.statement = statement

        return context

    def _adjust_for_single_inheritance(self, context):
        """Apply single-table-inheritance filtering.

        For all distinct single-table-inheritance mappers represented in the
        columns clause of this query, add criterion to the WHERE clause of the
        given QueryContext such that only the appropriate subtypes are
        selected from the total results.

        """
        for entity, (mapper, adapter, s, i, w) in \
                            self._mapper_adapter_map.iteritems():
            single_crit = mapper._single_table_criterion
            if single_crit is not None:
                if adapter:
                    single_crit = adapter.traverse(single_crit)
                single_crit = self._adapt_clause(single_crit, False, False)
                context.whereclause = sql.and_(
                                            context.whereclause, single_crit)

    def __str__(self):
        return str(self._compile_context().statement)


class _QueryEntity(object):
    """represent an entity column returned within a Query result."""

    def __new__(cls, *args, **kwargs):
        if cls is _QueryEntity:
            entity = args[1]
            if not isinstance(entity, basestring) and \
                        _is_mapped_class(entity):
                cls = _MapperEntity
            else:
                cls = _ColumnEntity
        return object.__new__(cls)

    def _clone(self):
        q = self.__class__.__new__(self.__class__)
        q.__dict__ = self.__dict__.copy()
        return q

class _MapperEntity(_QueryEntity):
    """mapper/class/AliasedClass entity"""

    def __init__(self, query, entity):
        self.primary_entity = not query._entities
        query._entities.append(self)

        self.entities = [entity]
        self.entity_zero = entity

    def setup_entity(self, entity, mapper, adapter, 
                        from_obj, is_aliased_class, with_polymorphic):
        self.mapper = mapper
        self.extension = self.mapper.extension
        self.adapter = adapter
        self.selectable  = from_obj
        self._with_polymorphic = with_polymorphic
        self._polymorphic_discriminator = None
        self.is_aliased_class = is_aliased_class
        if is_aliased_class:
            self.path_entity = self.entity = self.entity_zero = entity
        else:
            self.path_entity = mapper
            self.entity = self.entity_zero = mapper

    def set_with_polymorphic(self, query, cls_or_mappers, 
                                selectable, discriminator):
        if cls_or_mappers is None:
            query._reset_polymorphic_adapter(self.mapper)
            return

        mappers, from_obj = self.mapper._with_polymorphic_args(
                                                cls_or_mappers, selectable)
        self._with_polymorphic = mappers
        self._polymorphic_discriminator = discriminator

        # TODO: do the wrapped thing here too so that 
        # with_polymorphic() can be applied to aliases
        if not self.is_aliased_class:
            self.selectable = from_obj
            self.adapter = query._get_polymorphic_adapter(self, from_obj)

    def corresponds_to(self, entity):
        if _is_aliased_class(entity) or self.is_aliased_class:
            return entity is self.path_entity
        else:
            return entity.common_parent(self.path_entity)

    def adapt_to_selectable(self, query, sel):
        query._entities.append(self)

    def _get_entity_clauses(self, query, context):
            
        adapter = None
        if not self.is_aliased_class and query._polymorphic_adapters:
            adapter = query._polymorphic_adapters.get(self.mapper, None)

        if not adapter and self.adapter:
            adapter = self.adapter
        
        if adapter:
            if query._from_obj_alias:
                ret = adapter.wrap(query._from_obj_alias)
            else:
                ret = adapter
        else:
            ret = query._from_obj_alias

        return ret

    def row_processor(self, query, context, custom_rows):
        adapter = self._get_entity_clauses(query, context)

        if context.adapter and adapter:
            adapter = adapter.wrap(context.adapter)
        elif not adapter:
            adapter = context.adapter

        # polymorphic mappers which have concrete tables in 
        # their hierarchy usually
        # require row aliasing unconditionally.
        if not adapter and self.mapper._requires_row_aliasing:
            adapter = sql_util.ColumnAdapter(
                                        self.selectable,
                                        self.mapper._equivalent_columns)

        if self.primary_entity:
            _instance = self.mapper._instance_processor(
                                context, 
                                (self.path_entity,), 
                                adapter,
                                extension=self.extension,
                                only_load_props=query._only_load_props,
                                refresh_state=context.refresh_state,
                                polymorphic_discriminator=
                                    self._polymorphic_discriminator
            )
        else:
            _instance = self.mapper._instance_processor(
                                context, 
                                (self.path_entity,), 
                                adapter,
                                polymorphic_discriminator=
                                    self._polymorphic_discriminator)

        if self.is_aliased_class:
            entname = self.entity._sa_label_name
        else:
            entname = self.mapper.class_.__name__
        
        return _instance, entname

    def setup_context(self, query, context):
        adapter = self._get_entity_clauses(query, context)

        context.froms += (self.selectable,)

        if context.order_by is False and self.mapper.order_by:
            context.order_by = self.mapper.order_by

            # apply adaptation to the mapper's order_by if needed.
            if adapter:
                context.order_by = adapter.adapt_list(
                                        util.to_list(
                                            context.order_by
                                        )
                                    )

        for value in self.mapper._iterate_polymorphic_properties(
                                            self._with_polymorphic):
            if query._only_load_props and \
                    value.key not in query._only_load_props:
                continue
            value.setup(
                context,
                self,
                (self.path_entity,),
                adapter,
                only_load_props=query._only_load_props,
                column_collection=context.primary_columns
            )

        if self._polymorphic_discriminator is not None:
            if adapter:
                pd = adapter.columns[self._polymorphic_discriminator]
            else:
                pd = self._polymorphic_discriminator
            context.primary_columns.append(pd)

    def __str__(self):
        return str(self.mapper)

class _ColumnEntity(_QueryEntity):
    """Column/expression based entity."""

    def __init__(self, query, column):
        if isinstance(column, basestring):
            column = sql.literal_column(column)
            self._result_label = column.name
        elif isinstance(column, attributes.QueryableAttribute):
            self._result_label = column.key
            column = column.__clause_element__()
        else:
            self._result_label = getattr(column, 'key', None)

        if not isinstance(column, expression.ColumnElement) and \
                            hasattr(column, '_select_iterable'):
            for c in column._select_iterable:
                if c is column:
                    break
                _ColumnEntity(query, c)

            if c is not column:
                return

        if not isinstance(column, sql.ColumnElement):
            raise sa_exc.InvalidRequestError(
                "SQL expression, column, or mapped entity "
                "expected - got '%r'" % column
            )

        # if the Column is unnamed, give it a
        # label() so that mutable column expressions
        # can be located in the result even
        # if the expression's identity has been changed
        # due to adaption
        if not column._label:
            column = column.label(None)

        query._entities.append(self)

        self.column = column
        self.froms = set()

        # look for ORM entities represented within the
        # given expression.  Try to count only entities
        # for columns whos FROM object is in the actual list
        # of FROMs for the overall expression - this helps
        # subqueries which were built from ORM constructs from
        # leaking out their entities into the main select construct
        actual_froms = set(column._from_objects)

        self.entities = util.OrderedSet(
            elem._annotations['parententity']
            for elem in visitors.iterate(column, {})
            if 'parententity' in elem._annotations
            and actual_froms.intersection(elem._from_objects)
            )

        if self.entities:
            self.entity_zero = list(self.entities)[0]
        else:
            self.entity_zero = None
    
    def adapt_to_selectable(self, query, sel):
        _ColumnEntity(query, sel.corresponding_column(self.column))
        
    def setup_entity(self, entity, mapper, adapter, from_obj,
                                is_aliased_class, with_polymorphic):
        self.selectable = from_obj
        self.froms.add(from_obj)

    def corresponds_to(self, entity):
        if self.entity_zero is None:
            return False
        elif _is_aliased_class(entity):
            return entity is self.entity_zero
        else:
            return not _is_aliased_class(self.entity_zero) and \
                    entity.common_parent(self.entity_zero)

    def _resolve_expr_against_query_aliases(self, query, expr, context):
        return query._adapt_clause(expr, False, True)

    def row_processor(self, query, context, custom_rows):
        column = self._resolve_expr_against_query_aliases(
                                            query, self.column, context)

        if context.adapter:
            column = context.adapter.columns[column]

        def proc(row, result):
            return row[column]

        return (proc, self._result_label)

    def setup_context(self, query, context):
        column = self._resolve_expr_against_query_aliases(
                                            query, self.column, context)
        context.froms += tuple(self.froms)
        context.primary_columns.append(column)

    def __str__(self):
        return str(self.column)

log.class_logger(Query)

class QueryContext(object):
    multi_row_eager_loaders = False
    adapter = None
    froms = ()
    
    def __init__(self, query):

        if query._statement is not None:
            if isinstance(query._statement, expression._SelectBaseMixin) and \
                                not query._statement.use_labels:
                self.statement = query._statement.apply_labels()
            else:
                self.statement = query._statement
        else:
            self.statement = None
            self.from_clause = query._from_obj
            self.whereclause = query._criterion
            self.order_by = query._order_by

        self.query = query
        self.session = query.session
        self.populate_existing = query._populate_existing
        self.version_check = query._version_check
        self.refresh_state = query._refresh_state
        self.primary_columns = []
        self.secondary_columns = []
        self.eager_order_by = []
        self.eager_joins = {}
        self.create_eager_joins = []
        self.propagate_options = set(o for o in query._with_options if
                                        o.propagate_to_loaders)
        self.attributes = query._attributes.copy()

class AliasOption(interfaces.MapperOption):

    def __init__(self, alias):
        self.alias = alias

    def process_query(self, query):
        if isinstance(self.alias, basestring):
            alias = query._mapper_zero().mapped_table.alias(self.alias)
        else:
            alias = self.alias
        query._from_obj_alias = sql_util.ColumnAdapter(alias)


_runid = 1L
_id_lock = util.threading.Lock()

def _new_runid():
    global _runid
    _id_lock.acquire()
    try:
        _runid += 1
        return _runid
    finally:
        _id_lock.release()
