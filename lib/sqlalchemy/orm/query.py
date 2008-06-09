# orm/query.py
# Copyright (C) 2005, 2006, 2007, 2008 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""The Query class and support.

Defines the [sqlalchemy.orm.query#Query] class, the central construct used by
the ORM to construct database queries.

The ``Query`` class should not be confused with the
[sqlalchemy.sql.expression#Select] class, which defines database SELECT
operations at the SQL (non-ORM) level.  ``Query`` differs from ``Select`` in
that it returns ORM-mapped objects and interacts with an ORM session, whereas
the ``Select`` construct interacts directly with the database to return
iterable result sets.
"""

from itertools import chain
from sqlalchemy import sql, util, exceptions, logging
from sqlalchemy.sql import util as sql_util
from sqlalchemy.sql import expression, visitors, operators
from sqlalchemy.orm import mapper, object_mapper

from sqlalchemy.orm.util import _state_mapper, _class_to_mapper, _is_mapped_class, _is_aliased_class
from sqlalchemy.orm import util as mapperutil
from sqlalchemy.orm import interfaces
from sqlalchemy.orm import attributes
from sqlalchemy.orm.util import AliasedClass

aliased = AliasedClass

__all__ = ['Query', 'QueryContext', 'aliased']


class Query(object):
    """Encapsulates the object-fetching operations provided by Mappers."""

    def __init__(self, class_or_mapper, session=None, entity_name=None):
        self._session = session
        
        self._with_options = []
        self._lockmode = None
        
        self._entities = []
        self._order_by = False
        self._group_by = False
        self._distinct = False
        self._offset = None
        self._limit = None
        self._statement = None
        self._params = {}
        self._yield_per = None
        self._criterion = None
        self.__joinable_tables = None
        self._having = None
        self._column_aggregate = None
        self._populate_existing = False
        self._version_check = False
        self._autoflush = True
        
        self._attributes = {}
        self._current_path = ()
        self._only_load_props = None
        self._refresh_instance = None
        
        self.__init_mapper(_class_to_mapper(class_or_mapper, entity_name=entity_name))

    def __init_mapper(self, mapper):
        """populate all instance variables derived from this Query's mapper."""
        
        self.mapper = mapper
        self.table = self._from_obj = self.mapper.mapped_table
        self._eager_loaders = util.Set(chain(*[mp._eager_loaders for mp in [m for m in self.mapper.iterate_to_root()]]))
        self._extension = self.mapper.extension
        self._aliases_head = self._aliases_tail = None
        self._alias_ids = {}
        self._joinpoint = self.mapper
        self._entities.append(_PrimaryMapperEntity(self.mapper))
        if self.mapper.with_polymorphic:
            self.__set_with_polymorphic(*self.mapper.with_polymorphic)
        else:
            self._with_polymorphic = []

    def __generate_alias_ids(self):
        self._alias_ids = dict([
            (k, list(v)) for k, v in self._alias_ids.iteritems()
        ])

    def __no_criterion(self, meth):
        return self.__conditional_clone(meth, [self.__no_criterion_condition])

    def __no_statement(self, meth):
        return self.__conditional_clone(meth, [self.__no_statement_condition])

    def __reset_all(self, mapper, meth):
        q = self.__conditional_clone(meth, [self.__no_criterion_condition])
        q.__init_mapper(mapper, mapper)
        return q

    def __set_select_from(self, from_obj):
        if isinstance(from_obj, expression._SelectBaseMixin):
            # alias SELECTs and unions
            from_obj = from_obj.alias()

        self._from_obj = from_obj
        self._alias_ids = {}
        
        if self.table not in self._get_joinable_tables():
            self._aliases_head = self._aliases_tail = mapperutil.AliasedClauses(self._from_obj, equivalents=self.mapper._equivalent_columns)
            self._alias_ids.setdefault(self.table, []).append(self._aliases_head)
        else:
            self._aliases_head = self._aliases_tail = None

    def __set_with_polymorphic(self, cls_or_mappers, selectable=None):
        mappers, from_obj = self.mapper._with_polymorphic_args(cls_or_mappers, selectable)
        self._with_polymorphic = mappers
        self.__set_select_from(from_obj)

    def __no_criterion_condition(self, q, meth):
        if q._criterion or q._statement:
            util.warn(
                ("Query.%s() being called on a Query with existing criterion; "
                 "criterion is being ignored.") % meth)

        q._joinpoint = self.mapper
        q._statement = q._criterion = None
        q._order_by = q._group_by = q._distinct = False
        q._aliases_tail = q._aliases_head
        q.table = q._from_obj = q.mapper.mapped_table
        if q.mapper.with_polymorphic:
            q.__set_with_polymorphic(*q.mapper.with_polymorphic)

    def __no_entities(self, meth):
        q = self.__no_statement(meth)
        if len(q._entities) > 1 and not isinstance(q._entities[0], _PrimaryMapperEntity):
            raise exceptions.InvalidRequestError(
                ("Query.%s() being called on a Query with existing  "
                 "additional entities or columns - can't replace columns") % meth)
        q._entities = []
        return q

    def __no_statement_condition(self, q, meth):
        if q._statement:
            raise exceptions.InvalidRequestError(
                ("Query.%s() being called on a Query with an existing full "
                 "statement - can't apply criterion.") % meth)

    def __conditional_clone(self, methname=None, conditions=None):
        q = self._clone()
        if conditions:
            for condition in conditions:
                condition(q, methname)
        return q

    def __get_options(self, populate_existing=None, version_check=None, only_load_props=None, refresh_instance=None):
        if populate_existing:
            self._populate_existing = populate_existing
        if version_check:
            self._version_check = version_check
        if refresh_instance:
            self._refresh_instance = refresh_instance
        if only_load_props:
            self._only_load_props = util.Set(only_load_props)
        return self

    def _clone(self):
        q = Query.__new__(Query)
        q.__dict__ = self.__dict__.copy()
        return q

    def session(self):
        if self._session is None:
            return self.mapper.get_session()
        else:
            return self._session
    session = property(session)

    def statement(self):
        """return the full SELECT statement represented by this Query."""
        return self._compile_context().statement
    statement = property(statement)

    def whereclause(self):
        """return the WHERE criterion for this Query."""
        return self._criterion
    whereclause = property(whereclause)

    def _with_current_path(self, path):
        """indicate that this query applies to objects loaded within a certain path.
        
        Used by deferred loaders (see strategies.py) which transfer query 
        options from an originating query to a newly generated query intended
        for the deferred load.
        
        """
        q = self._clone()
        q._current_path = path
        return q

    def with_polymorphic(self, cls_or_mappers, selectable=None):
        """Load columns for descendant mappers of this Query's mapper.
        
        Using this method will ensure that each descendant mapper's
        tables are included in the FROM clause, and will allow filter() 
        criterion to be used against those tables.  The resulting 
        instances will also have those columns already loaded so that
        no "post fetch" of those columns will be required.
        
        ``cls_or_mappers`` is a single class or mapper, or list of class/mappers,
        which inherit from this Query's mapper.  Alternatively, it
        may also be the string ``'*'``, in which case all descending 
        mappers will be added to the FROM clause.
        
        ``selectable`` is a table or select() statement that will 
        be used in place of the generated FROM clause.  This argument
        is required if any of the desired mappers use concrete table 
        inheritance, since SQLAlchemy currently cannot generate UNIONs 
        among tables automatically.  If used, the ``selectable`` 
        argument must represent the full set of tables and columns mapped 
        by every desired mapper.  Otherwise, the unaccounted mapped columns
        will result in their table being appended directly to the FROM 
        clause which will usually lead to incorrect results.

        """
        q = self.__no_criterion('with_polymorphic')

        q.__set_with_polymorphic(cls_or_mappers, selectable=selectable)

        return q
    
        
    def yield_per(self, count):
        """Yield only ``count`` rows at a time.

        WARNING: use this method with caution; if the same instance is present
        in more than one batch of rows, end-user changes to attributes will be
        overwritten.

        In particular, it's usually impossible to use this setting with
        eagerly loaded collections (i.e. any lazy=False) since those
        collections will be cleared for a new load when encountered in a
        subsequent result batch.
        """

        q = self._clone()
        q._yield_per = count
        return q

    def get(self, ident, **kwargs):
        """Return an instance of the object based on the given identifier, or None if not found.

        The `ident` argument is a scalar or tuple of primary key column values
        in the order of the table def's primary key columns.
        """

        ret = self._extension.get(self, ident, **kwargs)
        if ret is not mapper.EXT_CONTINUE:
            return ret

        # convert composite types to individual args
        # TODO: account for the order of columns in the
        # ColumnProperty it corresponds to
        if hasattr(ident, '__composite_values__'):
            ident = ident.__composite_values__()

        key = self.mapper.identity_key_from_primary_key(ident)
        return self._get(key, ident, **kwargs)

    def load(self, ident, raiseerr=True, **kwargs):
        """Return an instance of the object based on the given identifier.

        If not found, raises an exception.  The method will **remove all
        pending changes** to the object already existing in the Session.  The
        `ident` argument is a scalar or tuple of primary key column values in
        the order of the table def's primary key columns.
        
        DEPRECATED.  Use query.populate_existing().get() instead.
        """

        ret = self._extension.load(self, ident, **kwargs)
        if ret is not mapper.EXT_CONTINUE:
            return ret
        key = self.mapper.identity_key_from_primary_key(ident)
        instance = self.populate_existing()._get(key, ident, **kwargs)
        if instance is None and raiseerr:
            raise exceptions.InvalidRequestError("No instance found for identity %s" % repr(ident))
        return instance

    def query_from_parent(cls, instance, property, **kwargs):
        """Return a new Query with criterion corresponding to a parent instance.

        Return a newly constructed Query object, with criterion corresponding
        to a relationship to the given parent instance.

        instance
          a persistent or detached instance which is related to class
          represented by this query.

         property
           string name of the property which relates this query's class to the
           instance.

         \**kwargs
           all extra keyword arguments are propagated to the constructor of
           Query.
        """

        mapper = object_mapper(instance)
        prop = mapper.get_property(property, resolve_synonyms=True)
        target = prop.mapper
        criterion = prop.compare(operators.eq, instance, value_is_parent=True)
        return Query(target, **kwargs).filter(criterion)
    query_from_parent = classmethod(query_from_parent)

    def autoflush(self, setting):
        """Return a Query with a specific 'autoflush' setting.

        Note that a Session with autoflush=False will
        not autoflush, even if this flag is set to True at the 
        Query level.  Therefore this flag is usually used only
        to disable autoflush for a specific Query.
        
        """
        q = self._clone()
        q._autoflush = setting
        return q

    def populate_existing(self):
        """Return a Query that will refresh all instances loaded.

        This includes all entities accessed from the database, including
        secondary entities, eagerly-loaded collection items.

        All changes present on entities which are already present in the
        session will be reset and the entities will all be marked "clean".

        An alternative to populate_existing() is to expire the Session
        fully using session.expire_all().
        
        """
        q = self._clone()
        q._populate_existing = True
        return q

    def with_parent(self, instance, property=None):
        """add a join criterion corresponding to a relationship to the given parent instance.

            instance
                a persistent or detached instance which is related to class represented
                by this query.

            property
                string name of the property which relates this query's class to the
                instance.  if None, the method will attempt to find a suitable property.

        currently, this method only works with immediate parent relationships, but in the
        future may be enhanced to work across a chain of parent mappers.

        """
        from sqlalchemy.orm import properties
        mapper = object_mapper(instance)
        if property is None:
            for prop in mapper.iterate_properties:
                if isinstance(prop, properties.PropertyLoader) and prop.mapper is self.mapper:
                    break
            else:
                raise exceptions.InvalidRequestError("Could not locate a property which relates instances of class '%s' to instances of class '%s'" % (self.mapper.class_.__name__, instance.__class__.__name__))
        else:
            prop = mapper.get_property(property, resolve_synonyms=True)
        return self.filter(prop.compare(operators.eq, instance, value_is_parent=True))

    def add_entity(self, entity, alias=None, id=None):
        """add a mapped entity to the list of result columns to be returned.

        This will have the effect of all result-returning methods returning a tuple
        of results, the first element being an instance of the primary class for this
        Query, and subsequent elements matching columns or entities which were
        specified via add_column or add_entity.

        When adding entities to the result, its generally desirable to add
        limiting criterion to the query which can associate the primary entity
        of this Query along with the additional entities.  The Query selects
        from all tables with no joining criterion by default.

            entity
                a class or mapper which will be added to the results.

            alias
                a sqlalchemy.sql.Alias object which will be used to select rows.  this
                will match the usage of the given Alias in filter(), order_by(), etc. expressions

            id
                a string ID matching that given to query.join() or query.outerjoin(); rows will be
                selected from the aliased join created via those methods.

        """
        q = self._clone()

        if not alias and _is_aliased_class(entity):
            alias = entity.alias

        if isinstance(entity, type):
            entity = mapper.class_mapper(entity)

        if alias is not None:
            alias = mapperutil.AliasedClauses(alias)

        q._entities = q._entities + [_MapperEntity(mapper=entity, alias=alias, id=id)]
        return q
    
    def _from_self(self):
        """return a Query that selects from this Query's SELECT statement.
        
        The API for this method hasn't been decided yet and is subject to change.

        """
        q = self._clone()
        q._eager_loaders = util.Set()
        fromclause = q.compile().correlate(None)
        return Query(self.mapper, self.session).select_from(fromclause)
        
    def values(self, *columns):
        """Return an iterator yielding result tuples corresponding to the given list of columns"""
        
        q = self.__no_entities('_values')
        q._only_load_props = q._eager_loaders = util.Set()
        q._no_filters = True
        for column in columns:
            q._entities.append(self._add_column(column, None, False))
        if not q._yield_per:
            q = q.yield_per(10)
        return iter(q)
    _values = values
    
    def add_column(self, column, id=None):
        """Add a SQL ColumnElement to the list of result columns to be returned.

        This will have the effect of all result-returning methods returning a
        tuple of results, the first element being an instance of the primary
        class for this Query, and subsequent elements matching columns or
        entities which were specified via add_column or add_entity.

        When adding columns to the result, its generally desirable to add
        limiting criterion to the query which can associate the primary entity
        of this Query along with the additional columns, if the column is
        based on a table or selectable that is not the primary mapped
        selectable.  The Query selects from all tables with no joining
        criterion by default.

        column
          a string column name or sql.ColumnElement to be added to the results.

        """
        q = self._clone()
        q._entities = q._entities + [self._add_column(column, id, True)]
        return q
    
    def _add_column(self, column, id, looks_for_aliases):
        if isinstance(column, interfaces.PropComparator):
            column = column.clause_element()

        elif not isinstance(column, (sql.ColumnElement, basestring)):
            raise exceptions.InvalidRequestError("Invalid column expression '%r'" % column)

        return _ColumnEntity(column, id)
        
    def options(self, *args):
        """Return a new Query object, applying the given list of
        MapperOptions.

        """
        return self._options(False, *args)

    def _conditional_options(self, *args):
        return self._options(True, *args)

    def _options(self, conditional, *args):
        q = self._clone()
        # most MapperOptions write to the '_attributes' dictionary,
        # so copy that as well
        q._attributes = q._attributes.copy()
        opts = [o for o in util.flatten_iterator(args)]
        q._with_options = q._with_options + opts
        if conditional:
            for opt in opts:
                opt.process_query_conditionally(q)
        else:
            for opt in opts:
                opt.process_query(q)
        return q

    def with_lockmode(self, mode):
        """Return a new Query object with the specified locking mode."""
        
        q = self._clone()
        q._lockmode = mode
        return q

    def params(self, *args, **kwargs):
        """add values for bind parameters which may have been specified in filter().

        parameters may be specified using \**kwargs, or optionally a single dictionary
        as the first positional argument.  The reason for both is that \**kwargs is
        convenient, however some parameter dictionaries contain unicode keys in which case
        \**kwargs cannot be used.

        """
        q = self._clone()
        if len(args) == 1:
            kwargs.update(args[0])
        elif len(args) > 0:
            raise exceptions.ArgumentError("params() takes zero or one positional argument, which is a dictionary.")
        q._params = q._params.copy()
        q._params.update(kwargs)
        return q

    def filter(self, criterion):
        """apply the given filtering criterion to the query and return the newly resulting ``Query``

        the criterion is any sql.ClauseElement applicable to the WHERE clause of a select.

        """
        if isinstance(criterion, basestring):
            criterion = sql.text(criterion)

        if criterion is not None and not isinstance(criterion, sql.ClauseElement):
            raise exceptions.ArgumentError("filter() argument must be of type sqlalchemy.sql.ClauseElement or string")

        if self._aliases_tail:
            criterion = self._aliases_tail.adapt_clause(criterion)

        q = self.__no_statement("filter")
        if q._criterion is not None:
            q._criterion = q._criterion & criterion
        else:
            q._criterion = criterion
        return q

    def filter_by(self, **kwargs):
        """apply the given filtering criterion to the query and return the newly resulting ``Query``."""

        clauses = [self._joinpoint.get_property(key, resolve_synonyms=True).compare(operators.eq, value)
            for key, value in kwargs.iteritems()]

        return self.filter(sql.and_(*clauses))


    def min(self, col):
        """Execute the SQL ``min()`` function against the given column."""

        return self._col_aggregate(col, sql.func.min)

    def max(self, col):
        """Execute the SQL ``max()`` function against the given column."""

        return self._col_aggregate(col, sql.func.max)

    def sum(self, col):
        """Execute the SQL ``sum()`` function against the given column."""

        return self._col_aggregate(col, sql.func.sum)

    def avg(self, col):
        """Execute the SQL ``avg()`` function against the given column."""

        return self._col_aggregate(col, sql.func.avg)

    def order_by(self, *criterion):
        """apply one or more ORDER BY criterion to the query and return the newly resulting ``Query``"""

        q = self.__no_statement("order_by")

        if self._aliases_tail:
            criterion = tuple(self._aliases_tail.adapt_list(
                    [expression._literal_as_text(o) for o in criterion]
                    ))

        if q._order_by is False:
            q._order_by = criterion
        else:
            q._order_by = q._order_by + criterion
        return q
    order_by = util.array_as_starargs_decorator(order_by)
    
    def group_by(self, *criterion):
        """apply one or more GROUP BY criterion to the query and return the newly resulting ``Query``"""

        q = self.__no_statement("group_by")
        if q._group_by is False:
            q._group_by = criterion
        else:
            q._group_by = q._group_by + criterion
        return q
    group_by = util.array_as_starargs_decorator(group_by)
    
    def having(self, criterion):
        """apply a HAVING criterion to the query and return the newly resulting ``Query``."""

        if isinstance(criterion, basestring):
            criterion = sql.text(criterion)

        if criterion is not None and not isinstance(criterion, sql.ClauseElement):
            raise exceptions.ArgumentError("having() argument must be of type sqlalchemy.sql.ClauseElement or string")

        if self._aliases_tail:
            criterion = self._aliases_tail.adapt_clause(criterion)

        q = self.__no_statement("having")
        if q._having is not None:
            q._having = q._having & criterion
        else:
            q._having = criterion
        return q

    def join(self, prop, id=None, aliased=False, from_joinpoint=False):
        """Create a join against this ``Query`` object's criterion
        and apply generatively, retunring the newly resulting ``Query``.

        'prop' may be one of:
          * a string property name, i.e. "rooms"
          * a class-mapped attribute, i.e. Houses.rooms
          * a 2-tuple containing one of the above, combined with a selectable
            which derives from the properties' mapped table
          * a list (not a tuple) containing a combination of any of the above.

        e.g.::

            session.query(Company).join('employees')
            session.query(Company).join(['employees', 'tasks'])
            session.query(Houses).join([Colonials.rooms, Room.closets])
            session.query(Company).join([('employees', people.join(engineers)), Engineer.computers])

        """
        return self._join(prop, id=id, outerjoin=False, aliased=aliased, from_joinpoint=from_joinpoint)

    def outerjoin(self, prop, id=None, aliased=False, from_joinpoint=False):
        """Create a left outer join against this ``Query`` object's criterion
        and apply generatively, retunring the newly resulting ``Query``.

        'prop' may be one of:
          * a string property name, i.e. "rooms"
          * a class-mapped attribute, i.e. Houses.rooms
          * a 2-tuple containing one of the above, combined with a selectable
            which derives from the properties' mapped table
          * a list (not a tuple) containing a combination of any of the above.

        e.g.::

            session.query(Company).outerjoin('employees')
            session.query(Company).outerjoin(['employees', 'tasks'])
            session.query(Houses).outerjoin([Colonials.rooms, Room.closets])
            session.query(Company).join([('employees', people.join(engineers)), Engineer.computers])

        """
        return self._join(prop, id=id, outerjoin=True, aliased=aliased, from_joinpoint=from_joinpoint)
    
    def _join(self, prop, id, outerjoin, aliased, from_joinpoint):
        (clause, mapper, aliases) = self._join_to(prop, outerjoin=outerjoin, start=from_joinpoint and self._joinpoint or self.mapper, create_aliases=aliased)
        # TODO: improve the generative check here to look for primary mapped entity, etc.
        q = self.__no_statement("join")
        q._from_obj = clause
        q._joinpoint = mapper
        q._aliases = aliases
        q.__generate_alias_ids()
        
        if aliases:
            q._aliases_tail = aliases

        a = aliases
        while a is not None:
            if isinstance(a, mapperutil.PropertyAliasedClauses):
                q._alias_ids.setdefault(a.mapper, []).append(a)
                q._alias_ids.setdefault(a.table, []).append(a)
                a = a.parentclauses
            else:
                break

        if id:
            q._alias_ids[id] = [aliases]
        return q

    def _get_joinable_tables(self):
        if not self.__joinable_tables or self.__joinable_tables[0] is not self._from_obj:
            currenttables = [self._from_obj]
            def visit_join(join):
                currenttables.append(join.left)
                currenttables.append(join.right)
            visitors.traverse(self._from_obj, visit_join=visit_join, traverse_options={'column_collections':False, 'aliased_selectables':False})
            self.__joinable_tables = (self._from_obj, currenttables)
            return currenttables
        else:
            return self.__joinable_tables[1]

    def _join_to(self, keys, outerjoin=False, start=None, create_aliases=True):
        if start is None:
            start = self._joinpoint

        clause = self._from_obj

        currenttables = self._get_joinable_tables()

        # determine if generated joins need to be aliased on the left
        # hand side.
        if self._aliases_head is self._aliases_tail is not None:
            adapt_against = self._aliases_tail.alias
        elif start is not self.mapper and self._aliases_tail:
            adapt_against = self._aliases_tail.alias
        else:
            adapt_against = None

        mapper = start
        alias = self._aliases_tail

        if not isinstance(keys, list):
            keys = [keys]
            
        for key in keys:
            use_selectable = None
            of_type = None
            is_aliased_class = False
            
            if isinstance(key, tuple):
                key, use_selectable = key

            if isinstance(key, interfaces.PropComparator):
                prop = key.property
                if getattr(key, '_of_type', None):
                    of_type = key._of_type
                    if not use_selectable:
                        use_selectable = key._of_type.mapped_table
            else:
                prop = mapper.get_property(key, resolve_synonyms=True)

            if use_selectable:
                if _is_aliased_class(use_selectable):
                    use_selectable = use_selectable.alias
                    is_aliased_class = True
                if not use_selectable.is_derived_from(prop.mapper.mapped_table):
                    raise exceptions.InvalidRequestError("Selectable '%s' is not derived from '%s'" % (use_selectable.description, prop.mapper.mapped_table.description))
                if not isinstance(use_selectable, expression.Alias):
                    use_selectable = use_selectable.alias()
            elif prop.mapper.with_polymorphic:
                use_selectable = prop.mapper._with_polymorphic_selectable()
                if not isinstance(use_selectable, expression.Alias):
                    use_selectable = use_selectable.alias()

            if prop._is_self_referential() and not create_aliases and not use_selectable:
                raise exceptions.InvalidRequestError("Self-referential query on '%s' property requires aliased=True argument." % str(prop))

            if prop.table not in currenttables or create_aliases or use_selectable:
                
                if use_selectable or create_aliases:
                    alias = mapperutil.PropertyAliasedClauses(prop,
                        prop.primaryjoin, 
                        prop.secondaryjoin, 
                        alias,
                        alias=use_selectable,
                        should_adapt=not is_aliased_class
                    )
                    crit = alias.primaryjoin
                    if prop.secondary:
                        clause = clause.join(alias.secondary, crit, isouter=outerjoin)
                        clause = clause.join(alias.alias, alias.secondaryjoin, isouter=outerjoin)
                    else:
                        clause = clause.join(alias.alias, crit, isouter=outerjoin)
                else:
                    assert not prop.mapper.with_polymorphic
                    pj, sj, source, dest, target_adapter = prop._create_joins(source_selectable=adapt_against)
                    if sj:
                        clause = clause.join(prop.secondary, pj, isouter=outerjoin)
                        clause = clause.join(prop.table, sj, isouter=outerjoin)
                    else:
                        clause = clause.join(prop.table, pj, isouter=outerjoin)
                        
            elif not create_aliases and prop.secondary is not None and prop.secondary not in currenttables:
                # TODO: this check is not strong enough for different paths to the same endpoint which
                # does not use secondary tables
                raise exceptions.InvalidRequestError("Can't join to property '%s'; a path to this table along a different secondary table already exists.  Use the `alias=True` argument to `join()`." % prop.key)

            mapper = of_type or prop.mapper

            if use_selectable:
                adapt_against = use_selectable
        
        return (clause, mapper, alias)


    def reset_joinpoint(self):
        """return a new Query reset the 'joinpoint' of this Query reset
        back to the starting mapper.  Subsequent generative calls will
        be constructed from the new joinpoint.

        Note that each call to join() or outerjoin() also starts from
        the root.

        """
        q = self.__no_statement("reset_joinpoint")
        q._joinpoint = q.mapper
        if q.table not in q._get_joinable_tables():
            q._aliases_head = q._aliases_tail = mapperutil.AliasedClauses(q._from_obj, equivalents=q.mapper._equivalent_columns)
        else:
            q._aliases_head = q._aliases_tail = None
        return q

    def select_from(self, from_obj):
        """Set the `from_obj` parameter of the query and return the newly
        resulting ``Query``.  This replaces the table which this Query selects
        from with the given table.


        `from_obj` is a single table or selectable.

        """
        new = self.__no_criterion('select_from')
        if isinstance(from_obj, (tuple, list)):
            util.warn_deprecated("select_from() now accepts a single Selectable as its argument, which replaces any existing FROM criterion.")
            from_obj = from_obj[-1]

        new.__set_select_from(from_obj)
        return new
    
    def __getitem__(self, item):
        if isinstance(item, slice):
            start = item.start
            stop = item.stop
            # if we slice from the end we need to execute the query
            if (isinstance(start, int) and start < 0) or \
               (isinstance(stop, int) and stop < 0):
                return list(self)[item]
            else:
                res = self._clone()
                if start is not None and stop is not None:
                    res._offset = (self._offset or 0) + start
                    res._limit = stop - start
                elif start is None and stop is not None:
                    res._limit = stop
                elif start is not None and stop is None:
                    res._offset = (self._offset or 0) + start
                if item.step is not None:
                    return list(res)[None:None:item.step]
                else:
                    return res
        else:
            return list(self[item:item+1])[0]

    def limit(self, limit):
        """Apply a ``LIMIT`` to the query and return the newly resulting

        ``Query``.

        """
        return self[:limit]

    def offset(self, offset):
        """Apply an ``OFFSET`` to the query and return the newly resulting
        ``Query``.

        """
        return self[offset:]

    def distinct(self):
        """Apply a ``DISTINCT`` to the query and return the newly resulting
        ``Query``.

        """
        new = self.__no_statement("distinct")
        new._distinct = True
        return new

    def all(self):
        """Return the results represented by this ``Query`` as a list.

        This results in an execution of the underlying query.

        """
        return list(self)


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
        q = self.__no_criterion('from_statement')
        q._statement = statement
        return q

    def first(self):
        """Return the first result of this ``Query`` or None if the result doesn't contain any row.

        This results in an execution of the underlying query.

        """
        if self._column_aggregate is not None:
            return self._col_aggregate(*self._column_aggregate)

        ret = list(self[0:1])
        if len(ret) > 0:
            return ret[0]
        else:
            return None

    def one(self):
        """Return the first result, raising an exception unless exactly one row exists.

        This results in an execution of the underlying query.

        """
        if self._column_aggregate is not None:
            return self._col_aggregate(*self._column_aggregate)

        ret = list(self[0:2])

        if len(ret) == 1:
            return ret[0]
        elif len(ret) == 0:
            raise exceptions.InvalidRequestError('No rows returned for one()')
        else:
            raise exceptions.InvalidRequestError('Multiple rows returned for one()')

    def __iter__(self):
        context = self._compile_context()
        context.statement.use_labels = True
        if self._autoflush and not self._populate_existing:
            self.session._autoflush()
        return self._execute_and_instances(context)

    def _execute_and_instances(self, querycontext):
        result = self.session.execute(querycontext.statement, params=self._params, mapper=self.mapper, instance=self._refresh_instance)
        return self.iterate_instances(result, querycontext=querycontext)

    def instances(self, cursor, *mappers_or_columns, **kwargs):
        """Given a ResultProxy cursor as returned by connection.execute(), return an ORM result as a list.

        e.g.::
        
            result = engine.execute("select * from users")
            users = session.query(User).instances(result)

        """
        return list(self.iterate_instances(cursor, *mappers_or_columns, **kwargs))

    def iterate_instances(self, cursor, *mappers_or_columns, **kwargs):
        """Given a ResultProxy cursor as returned by connection.execute(), return an ORM result as an iterator.
        
        e.g.::
        
            result = engine.execute("select * from users")
            for u in session.query(User).iterate_instances(result):
                print u

        """
        session = self.session

        context = kwargs.pop('querycontext', None)
        if context is None:
            context = QueryContext(self)

        context.runid = _new_runid()

        entities = self._entities + [_QueryEntity.legacy_guess_type(mc) for mc in mappers_or_columns]
        
        if getattr(self, '_no_filters', False):
            filter = None
            single_entity = custom_rows = False
        else:
            single_entity = isinstance(entities[0], _PrimaryMapperEntity) and len(entities) == 1
            custom_rows = single_entity and 'append_result' in context.extension.methods
            
            if single_entity:
                filter = util.OrderedIdentitySet
            else:
                filter = util.OrderedSet
        
        process = [query_entity.row_processor(self, context, single_entity) for query_entity in entities]

        while True:
            context.progress = util.Set()
            context.partials = {}

            if self._yield_per:
                fetch = cursor.fetchmany(self._yield_per)
                if not fetch:
                    return
            else:
                fetch = cursor.fetchall()
            
            if custom_rows:
                rows = []
                for row in fetch:
                    process[0](context, row, rows)
            elif single_entity:
                rows = [process[0](context, row) for row in fetch]
            else:
                rows = [tuple([proc(context, row) for proc in process]) for row in fetch]

            if filter:
                rows = filter(rows)

            if context.refresh_instance and context.only_load_props and context.refresh_instance in context.progress:
                context.refresh_instance.commit(context.only_load_props)
                context.progress.remove(context.refresh_instance)

            for ii in context.progress:
                context.attributes.get(('populating_mapper', ii), _state_mapper(ii))._post_instance(context, ii)
                ii.commit_all()
                
            for ii, attrs in context.partials.items():
                context.attributes.get(('populating_mapper', ii), _state_mapper(ii))._post_instance(context, ii, only_load_props=attrs)
                ii.commit(attrs)
                
            for row in rows:
                yield row

            if not self._yield_per:
                break

    def _get(self, key=None, ident=None, refresh_instance=None, lockmode=None, only_load_props=None):
        lockmode = lockmode or self._lockmode
        if not self._populate_existing and not refresh_instance and not self.mapper.always_refresh and lockmode is None:
            try:
                return self.session.identity_map[key]
            except KeyError:
                pass

        if ident is None:
            if key is not None:
                ident = key[1]
        else:
            ident = util.to_list(ident)

        q = self
        
        # dont use 'polymorphic' mapper if we are refreshing an instance
        if refresh_instance and q.mapper is not q.mapper:
            q = q.__reset_all(q.mapper, '_get')

        if ident is not None:
            q = q.__no_criterion('get')
            params = {}
            (_get_clause, _get_params) = q.mapper._get_clause
            q = q.filter(_get_clause)
            for i, primary_key in enumerate(q.mapper.primary_key):
                try:
                    params[_get_params[primary_key].key] = ident[i]
                except IndexError:
                    raise exceptions.InvalidRequestError("Could not find enough values to formulate primary key for query.get(); primary key columns are %s" % ', '.join(["'%s'" % str(c) for c in q.mapper.primary_key]))
            q = q.params(params)

        if lockmode is not None:
            q = q.with_lockmode(lockmode)
        q = q.__get_options(populate_existing=bool(refresh_instance), version_check=(lockmode is not None), only_load_props=only_load_props, refresh_instance=refresh_instance)
        q._order_by = None
        try:
            # call using all() to avoid LIMIT compilation complexity
            return q.all()[0]
        except IndexError:
            return None

    def _select_args(self):
        return {'limit':self._limit, 'offset':self._offset, 'distinct':self._distinct, 'group_by':self._group_by or None, 'having':self._having or None}
    _select_args = property(_select_args)
    
    def _should_nest_selectable(self):
        kwargs = self._select_args
        return (kwargs.get('limit') is not None or kwargs.get('offset') is not None or kwargs.get('distinct', False))
    _should_nest_selectable = property(_should_nest_selectable)

    def count(self, whereclause=None, params=None, **kwargs):
        """Apply this query's criterion to a SELECT COUNT statement.

        the whereclause, params and \**kwargs arguments are deprecated.  use filter()
        and other generative methods to establish modifiers.

        """
        q = self
        if whereclause is not None:
            q = q.filter(whereclause)
        if params is not None:
            q = q.params(params)
        q = q._legacy_select_kwargs(**kwargs)
        return q._count()

    def _count(self):
        """Apply this query's criterion to a SELECT COUNT statement.

        this is the purely generative version which will become
        the public method in version 0.5.

        """
        return self._col_aggregate(sql.literal_column('1'), sql.func.count, nested_cols=list(self.mapper.primary_key))

    def _col_aggregate(self, col, func, nested_cols=None):
        
        context = QueryContext(self)
        context.whereclause = self._criterion
        self._adjust_for_single_inheritance(context)
        whereclause = context.whereclause 
        
        from_obj = self._from_obj

        if self._should_nest_selectable:
            if not nested_cols:
                nested_cols = [col]
            s = sql.select(nested_cols, whereclause, from_obj=from_obj, **self._select_args)
            s = s.alias()
            s = sql.select([func(s.corresponding_column(col) or col)]).select_from(s)
        else:
            s = sql.select([func(col)], whereclause, from_obj=from_obj, **self._select_args)
            
        if self._autoflush and not self._populate_existing:
            self.session._autoflush()
        return self.session.scalar(s, params=self._params, mapper=self.mapper)

    def compile(self):
        """compiles and returns a SQL statement based on the criterion and conditions within this Query."""

        return self._compile_context().statement

    def _compile_context(self):

        context = QueryContext(self)

        if self._statement:
            self._statement.use_labels = True
            context.statement = self._statement
            return context

        from_obj = self._from_obj
        adapter = self._aliases_head
        
        if self._lockmode:
            try:
                for_update = {'read':'read','update':True,'update_nowait':'nowait',None:False}[self._lockmode]
            except KeyError:
                raise exceptions.ArgumentError("Unknown lockmode '%s'" % self._lockmode)
        else:
            for_update = False
            
        context.from_clause = from_obj
        context.whereclause = self._criterion
        context.order_by = self._order_by

        self._adjust_for_single_inheritance(context)
        
        for entity in self._entities:
            entity.setup_context(self, context)
            
        if self._eager_loaders and self._should_nest_selectable:
            # eager loaders are present, and the SELECT has limiting criterion
            # produce a "wrapped" selectable.
            
            if context.order_by:
                context.order_by = [expression._literal_as_text(o) for o in util.to_list(context.order_by) or []]
                if adapter:
                    context.order_by = adapter.adapt_list(context.order_by)
                # locate all embedded Column clauses so they can be added to the
                # "inner" select statement where they'll be available to the enclosing
                # statement's "order by"
                # TODO: this likely doesn't work with very involved ORDER BY expressions,
                # such as those including subqueries
                order_by_col_expr = list(chain(*[sql_util.find_columns(o) for o in context.order_by]))
            else:
                context.order_by = None
                order_by_col_expr = []
                
            if adapter:
                context.primary_columns = adapter.adapt_list(context.primary_columns)
            
            inner = sql.select(context.primary_columns + order_by_col_expr, context.whereclause, from_obj=context.from_clause, use_labels=True, correlate=False, order_by=context.order_by, **self._select_args).alias()
            local_adapter = sql_util.ClauseAdapter(inner)

            context.row_adapter = mapperutil.create_row_adapter(inner, equivalent_columns=self.mapper._equivalent_columns)

            statement = sql.select([inner] + context.secondary_columns, for_update=for_update, use_labels=True)

            if context.eager_joins:
                eager_joins = local_adapter.traverse(context.eager_joins)
                statement.append_from(eager_joins)

            if context.order_by:
                statement.append_order_by(*local_adapter.copy_and_process(context.order_by))

            statement.append_order_by(*context.eager_order_by)
        else:
            if context.order_by:
                context.order_by = [expression._literal_as_text(o) for o in util.to_list(context.order_by) or []]
                if adapter:
                    context.order_by = adapter.adapt_list(context.order_by)
            else:
                context.order_by = None
            
            if adapter:
                context.primary_columns = adapter.adapt_list(context.primary_columns)
                context.row_adapter = mapperutil.create_row_adapter(adapter.alias, equivalent_columns=self.mapper._equivalent_columns)
                
            if self._distinct and context.order_by:
                order_by_col_expr = list(chain(*[sql_util.find_columns(o) for o in context.order_by]))
                context.primary_columns += order_by_col_expr

            statement = sql.select(context.primary_columns + context.secondary_columns, context.whereclause, from_obj=from_obj, use_labels=True, for_update=for_update, order_by=context.order_by, **self._select_args)

            if context.eager_joins:
                if adapter:
                    context.eager_joins = adapter.adapt_clause(context.eager_joins)
                statement.append_from(context.eager_joins)

            if context.eager_order_by:
                if adapter:
                    context.eager_order_by = adapter.adapt_list(context.eager_order_by)
                statement.append_order_by(*context.eager_order_by)

        # polymorphic mappers which have concrete tables in their hierarchy usually
        # require row aliasing unconditionally.  
        if not context.row_adapter and self.mapper._requires_row_aliasing:
            context.row_adapter = mapperutil.create_row_adapter(self.table, equivalent_columns=self.mapper._equivalent_columns)
            
        context.statement = statement

        return context

    def _adjust_for_single_inheritance(self, context):
        """Apply single-table-inheritance filtering.
        
        For the base mapper of this query, add criterion to the WHERE clause of the given QueryContext
        such that only the appropriate subtypes are selected from the total results.

        A more sophisticated version of this, which works with multiple mappers and column expressions,
        is present in 0.5.

        """
        # if single-table inheritance mapper, add "typecol IN (polymorphic)" criterion so
        # that we only load the appropriate types
        if self.mapper.single and self.mapper.inherits is not None and self.mapper.polymorphic_on is not None and self.mapper.polymorphic_identity is not None:
            context.whereclause = sql.and_(context.whereclause, self.mapper.polymorphic_on.in_([m.polymorphic_identity for m in self.mapper.polymorphic_iterator()]))


    def __log_debug(self, msg):
        self.logger.debug(msg)

    def __str__(self):
        return str(self.compile())

    # DEPRECATED LAND !

    def _generative_col_aggregate(self, col, func):
        """apply the given aggregate function to the query and return the newly
        resulting ``Query``. (deprecated)
        """
        if self._column_aggregate is not None:
            raise exceptions.InvalidRequestError("Query already contains an aggregate column or function")
        q = self.__no_statement("aggregate")
        q._column_aggregate = (col, func)
        return q

    def apply_min(self, col):
        """apply the SQL ``min()`` function against the given column to the
        query and return the newly resulting ``Query``.
        
        DEPRECATED.
        """
        return self._generative_col_aggregate(col, sql.func.min)

    def apply_max(self, col):
        """apply the SQL ``max()`` function against the given column to the
        query and return the newly resulting ``Query``.

        DEPRECATED.
        """
        return self._generative_col_aggregate(col, sql.func.max)

    def apply_sum(self, col):
        """apply the SQL ``sum()`` function against the given column to the
        query and return the newly resulting ``Query``.

        DEPRECATED.
        """
        return self._generative_col_aggregate(col, sql.func.sum)

    def apply_avg(self, col):
        """apply the SQL ``avg()`` function against the given column to the
        query and return the newly resulting ``Query``.

        DEPRECATED.
        """
        return self._generative_col_aggregate(col, sql.func.avg)

    def list(self): #pragma: no cover
        """DEPRECATED.  use all()"""

        return list(self)

    def scalar(self): #pragma: no cover
        """DEPRECATED.  use first()"""

        return self.first()

    def _legacy_filter_by(self, *args, **kwargs): #pragma: no cover
        return self.filter(self._legacy_join_by(args, kwargs, start=self._joinpoint))

    def count_by(self, *args, **params): #pragma: no cover
        """DEPRECATED.  use query.filter_by(\**params).count()"""

        return self.count(self.join_by(*args, **params))


    def select_whereclause(self, whereclause=None, params=None, **kwargs): #pragma: no cover
        """DEPRECATED.  use query.filter(whereclause).all()"""

        q = self.filter(whereclause)._legacy_select_kwargs(**kwargs)
        if params is not None:
            q = q.params(params)
        return list(q)

    def _legacy_select_from(self, from_obj):
        q = self._clone()
        if len(from_obj) > 1:
            raise exceptions.ArgumentError("Multiple-entry from_obj parameter no longer supported")
        q._from_obj = from_obj[0]
        return q

    def _legacy_select_kwargs(self, **kwargs): #pragma: no cover
        q = self
        if "order_by" in kwargs and kwargs['order_by']:
            q = q.order_by(kwargs['order_by'])
        if "group_by" in kwargs:
            q = q.group_by(kwargs['group_by'])
        if "from_obj" in kwargs:
            q = q._legacy_select_from(kwargs['from_obj'])
        if "lockmode" in kwargs:
            q = q.with_lockmode(kwargs['lockmode'])
        if "distinct" in kwargs:
            q = q.distinct()
        if "limit" in kwargs:
            q = q.limit(kwargs['limit'])
        if "offset" in kwargs:
            q = q.offset(kwargs['offset'])
        return q


    def get_by(self, *args, **params): #pragma: no cover
        """DEPRECATED.  use query.filter_by(\**params).first()"""

        ret = self._extension.get_by(self, *args, **params)
        if ret is not mapper.EXT_CONTINUE:
            return ret

        return self._legacy_filter_by(*args, **params).first()

    def select_by(self, *args, **params): #pragma: no cover
        """DEPRECATED. use use query.filter_by(\**params).all()."""

        ret = self._extension.select_by(self, *args, **params)
        if ret is not mapper.EXT_CONTINUE:
            return ret

        return self._legacy_filter_by(*args, **params).list()

    def join_by(self, *args, **params): #pragma: no cover
        """DEPRECATED. use join() to construct joins based on attribute names."""

        return self._legacy_join_by(args, params, start=self._joinpoint)

    def _build_select(self, arg=None, params=None, **kwargs): #pragma: no cover
        if isinstance(arg, sql.FromClause) and arg.supports_execution():
            return self.from_statement(arg)
        elif arg is not None:
            return self.filter(arg)._legacy_select_kwargs(**kwargs)
        else:
            return self._legacy_select_kwargs(**kwargs)

    def selectfirst(self, arg=None, **kwargs): #pragma: no cover
        """DEPRECATED.  use query.filter(whereclause).first()"""

        return self._build_select(arg, **kwargs).first()

    def selectone(self, arg=None, **kwargs): #pragma: no cover
        """DEPRECATED.  use query.filter(whereclause).one()"""

        return self._build_select(arg, **kwargs).one()

    def select(self, arg=None, **kwargs): #pragma: no cover
        """DEPRECATED.  use query.filter(whereclause).all(), or query.from_statement(statement).all()"""

        ret = self._extension.select(self, arg=arg, **kwargs)
        if ret is not mapper.EXT_CONTINUE:
            return ret
        return self._build_select(arg, **kwargs).all()

    def execute(self, clauseelement, params=None, *args, **kwargs): #pragma: no cover
        """DEPRECATED.  use query.from_statement().all()"""

        return self._select_statement(clauseelement, params, **kwargs)

    def select_statement(self, statement, **params): #pragma: no cover
        """DEPRECATED.  Use query.from_statement(statement)"""

        return self._select_statement(statement, params)

    def select_text(self, text, **params): #pragma: no cover
        """DEPRECATED.  Use query.from_statement(statement)"""

        return self._select_statement(text, params)

    def _select_statement(self, statement, params=None, **kwargs): #pragma: no cover
        q = self.from_statement(statement)
        if params is not None:
            q = q.params(params)
        q.__get_options(**kwargs)
        return list(q)

    def join_to(self, key): #pragma: no cover
        """DEPRECATED. use join() to create joins based on property names."""

        [keys, p] = self._locate_prop(key)
        return self.join_via(keys)

    def join_via(self, keys): #pragma: no cover
        """DEPRECATED. use join() to create joins based on property names."""

        mapper = self._joinpoint
        clause = None
        for key in keys:
            prop = mapper.get_property(key, resolve_synonyms=True)
            if clause is None:
                clause = prop._get_join(mapper)
            else:
                clause &= prop._get_join(mapper)
            mapper = prop.mapper

        return clause

    def _legacy_join_by(self, args, params, start=None): #pragma: no cover
        import properties

        clause = None
        for arg in args:
            if clause is None:
                clause = arg
            else:
                clause &= arg

        for key, value in params.iteritems():
            (keys, prop) = self._locate_prop(key, start=start)
            if isinstance(prop, properties.PropertyLoader):
                c = prop.compare(operators.eq, value) & self.join_via(keys[:-1])
            else:
                c = prop.compare(operators.eq, value) & self.join_via(keys)
            if clause is None:
                clause =  c
            else:
                clause &= c
        return clause

    def _locate_prop(self, key, start=None): #pragma: no cover
        import properties
        keys = []
        seen = util.Set()
        def search_for_prop(mapper_):
            if mapper_ in seen:
                return None
            seen.add(mapper_)

            prop = mapper_.get_property(key, resolve_synonyms=True, raiseerr=False)
            if prop is not None:
                if isinstance(prop, properties.PropertyLoader):
                    keys.insert(0, prop.key)
                return prop
            else:
                for prop in mapper_.iterate_properties:
                    if not isinstance(prop, properties.PropertyLoader):
                        continue
                    x = search_for_prop(prop.mapper)
                    if x:
                        keys.insert(0, prop.key)
                        return x
                else:
                    return None
        p = search_for_prop(start or self.mapper)
        if p is None:
            raise exceptions.InvalidRequestError("Can't locate property named '%s'" % key)
        return [keys, p]

    def selectfirst_by(self, *args, **params): #pragma: no cover
        """DEPRECATED. Use query.filter_by(\**kwargs).first()"""

        return self._legacy_filter_by(*args, **params).first()

    def selectone_by(self, *args, **params): #pragma: no cover
        """DEPRECATED. Use query.filter_by(\**kwargs).one()"""

        return self._legacy_filter_by(*args, **params).one()

    for deprecated_method in ('list', 'scalar', 'count_by',
                              'select_whereclause', 'get_by', 'select_by',
                              'join_by', 'selectfirst', 'selectone', 'select',
                              'execute', 'select_statement', 'select_text',
                              'join_to', 'join_via', 'selectfirst_by',
                              'selectone_by', 'apply_max', 'apply_min',
                              'apply_avg', 'apply_sum'):
        locals()[deprecated_method] = \
            util.deprecated(None, False)(locals()[deprecated_method])

class _QueryEntity(object):
    """represent an entity column returned within a Query result."""
    
    def legacy_guess_type(self, e):
        if isinstance(e, type):
            return _MapperEntity(mapper=mapper.class_mapper(e))
        elif isinstance(e, mapper.Mapper):
            return _MapperEntity(mapper=e)
        else:
            return _ColumnEntity(column=e)
    legacy_guess_type=classmethod(legacy_guess_type)

class _MapperEntity(_QueryEntity):
    """entity column corresponding to mapped ORM instances."""
    
    def __init__(self, mapper, alias=None, id=None):
        self.mapper = mapper
        self.alias = alias
        self.alias_id = id

    def _get_entity_clauses(self, query):
        if self.alias:
            return self.alias
        elif self.alias_id:
            try:
                return query._alias_ids[self.alias_id][0]
            except KeyError:
                raise exceptions.InvalidRequestError("Query has no alias identified by '%s'" % self.alias_id)

        l = query._alias_ids.get(self.mapper)
        if l:
            if len(l) > 1:
                raise exceptions.InvalidRequestError("Ambiguous join for entity '%s'; specify id=<someid> to query.join()/query.add_entity()" % str(self.mapper))
            return l[0]
        else:
            return None
            
    def row_processor(self, query, context, single_entity):
        clauses = self._get_entity_clauses(query) 
        if clauses:
            def proc(context, row):
                return self.mapper._instance(context, clauses.row_decorator(row), None)
        else:
            def proc(context, row):
                return self.mapper._instance(context, row, None)
            
        return proc
    
    def setup_context(self, query, context):
        clauses = self._get_entity_clauses(query)
        for value in self.mapper.iterate_properties:
            context.exec_with_path(self.mapper, value.key, value.setup, context, parentclauses=clauses)

    def __str__(self):
        return str(self.mapper)

class _PrimaryMapperEntity(_MapperEntity):
    """entity column corresponding to the 'primary' (first) mapped ORM instance."""

    def row_processor(self, query, context, single_entity):
        if single_entity and 'append_result' in context.extension.methods:    
            def main(context, row, result):
                if context.row_adapter:
                    row = context.row_adapter(row)
                self.mapper._instance(context, row, result,
                    extension=context.extension, only_load_props=context.only_load_props, refresh_instance=context.refresh_instance
                )
        elif context.row_adapter:
            def main(context, row):
                return self.mapper._instance(context, context.row_adapter(row), None,
                    extension=context.extension, only_load_props=context.only_load_props, refresh_instance=context.refresh_instance
                )
        else:
            def main(context, row):
                return self.mapper._instance(context, row, None,
                    extension=context.extension, only_load_props=context.only_load_props, refresh_instance=context.refresh_instance
                )
        
        return main

    def setup_context(self, query, context):
        
        if context.order_by is False:
            if self.mapper.order_by:
                context.order_by = self.mapper.order_by
            elif context.from_clause.default_order_by():
                context.order_by = context.from_clause.default_order_by()
                
        for value in self.mapper._iterate_polymorphic_properties(query._with_polymorphic, context.from_clause):
            if query._only_load_props and value.key not in query._only_load_props:
                continue
            context.exec_with_path(self.mapper, value.key, value.setup, context, only_load_props=query._only_load_props)

class _ColumnEntity(_QueryEntity):
    """entity column corresponding to Table or selectable columns."""

    def __init__(self, column, id):
        if isinstance(column, basestring):
            column = sql.literal_column(column)
            
        if column and isinstance(column, sql.ColumnElement) and not hasattr(column, '_label'):
            column = column.label(None)
        self.column = column
        self.alias_id = id

    def __resolve_expr_against_query_aliases(self, query, expr, context):
        if not query._alias_ids:
            return expr
            
        if ('_ColumnEntity', expr) in context.attributes:
            return context.attributes[('_ColumnEntity', expr)]
        
        if self.alias_id:
            try:
                aliases = query._alias_ids[self.alias_id][0]
            except KeyError:
                raise exceptions.InvalidRequestError("Query has no alias identified by '%s'" % self.alias_id)

            def _locate_aliased(element):
                if element in query._alias_ids:
                    return aliases
        else:
            def _locate_aliased(element):
                if element in query._alias_ids:
                    aliases = query._alias_ids[element]
                    if len(aliases) > 1:
                        raise exceptions.InvalidRequestError("Ambiguous join for entity '%s'; specify id=<someid> to query.join()/query.add_column(), or use the aliased() function to use explicit class aliases." % expr)
                    return aliases[0]
                return None

        class Adapter(visitors.ClauseVisitor):
            def before_clone(self, element):
                if isinstance(element, expression.FromClause):
                    alias = _locate_aliased(element)
                    if alias:
                        return alias.alias
                
                if hasattr(element, 'table'):
                    alias = _locate_aliased(element.table)
                    if alias:
                        return alias.aliased_column(element)

                return None

        context.attributes[('_ColumnEntity', expr)] = ret = Adapter().traverse(expr, clone=True)
        return ret
        
    def row_processor(self, query, context, single_entity):
        column = self.__resolve_expr_against_query_aliases(query, self.column, context)
        def proc(context, row):
            return row[column]
        return proc
    
    def setup_context(self, query, context):
        column = self.__resolve_expr_against_query_aliases(query, self.column, context)
        context.secondary_columns.append(column)
    
    def __str__(self):
        return str(self.column)

        
Query.logger = logging.class_logger(Query)

class QueryContext(object):
    def __init__(self, query):
        self.query = query
        self.mapper = query.mapper
        self.session = query.session
        self.extension = query._extension
        self.statement = None
        self.row_adapter = None
        self.populate_existing = query._populate_existing
        self.version_check = query._version_check
        self.only_load_props = query._only_load_props
        self.refresh_instance = query._refresh_instance
        self.path = ()
        self.primary_columns = []
        self.secondary_columns = []
        self.eager_order_by = []
        self.eager_joins = None
        self.options = query._with_options
        self.attributes = query._attributes.copy()

    def exec_with_path(self, mapper, propkey, fn, *args, **kwargs):
        oldpath = self.path
        self.path += (mapper.base_mapper, propkey)
        try:
            return fn(*args, **kwargs)
        finally:
            self.path = oldpath



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
