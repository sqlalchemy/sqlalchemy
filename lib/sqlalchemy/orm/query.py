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
from sqlalchemy.orm.mapper import _state_mapper
from sqlalchemy.orm import util as mapperutil
from sqlalchemy.orm import interfaces

__all__ = ['Query', 'QueryContext']


class Query(object):
    """Encapsulates the object-fetching operations provided by Mappers."""

    def __init__(self, class_or_mapper, session=None, entity_name=None):
        if isinstance(class_or_mapper, type):
            self.mapper = mapper.class_mapper(class_or_mapper, entity_name=entity_name)
        else:
            self.mapper = class_or_mapper.compile()
        self.select_mapper = self.mapper.get_select_mapper().compile()

        self._session = session

        self._with_options = []
        self._lockmode = None
        self._extension = self.mapper.extension
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
        self._joinable_tables = None
        self._having = None
        self._column_aggregate = None
        self._joinpoint = self.mapper
        self._aliases = None
        self._alias_ids = {}
        self._from_obj = self.table
        self._populate_existing = False
        self._version_check = False
        self._autoflush = True
        self._eager_loaders = util.Set(chain(*[mp._eager_loaders for mp in [m for m in self.mapper.iterate_to_root()]]))
        self._attributes = {}
        self._current_path = ()
        self._only_load_props = None
        self._refresh_instance = None

        self._adapter = self.select_mapper._clause_adapter

    def _no_criterion(self, meth):
        q = self._clone()

        if q._criterion or q._statement or q._from_obj is not self.table:
            util.warn(
                ("Query.%s() being called on a Query with existing criterion; "
                 "criterion is being ignored.") % meth)

        q._from_obj = self.table
        q._adapter = self.select_mapper._clause_adapter
        q._alias_ids = {}
        q._joinpoint = self.mapper
        q._statement = q._aliases = q._criterion = None
        q._order_by = q._group_by = q._distinct = False
        return q

    def _no_statement(self, meth):
        q = self._clone()
        if q._statement:
            raise exceptions.InvalidRequestError(
                ("Query.%s() being called on a Query with an existing full "
                 "statement - can't apply criterion.") % meth)
        return q

    def _clone(self):
        q = Query.__new__(Query)
        q.__dict__ = self.__dict__.copy()
        return q

    def _get_session(self):
        if self._session is None:
            return self.mapper.get_session()
        else:
            return self._session

    table = property(lambda s:s.select_mapper.mapped_table)
    primary_key_columns = property(lambda s:s.select_mapper.primary_key)
    session = property(_get_session)

    def _with_current_path(self, path):
        q = self._clone()
        q._current_path = path
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
        q = self._clone()
        q._autoflush = setting
        return q

    def populate_existing(self):
        """Return a Query that will refresh all instances loaded.

        This includes all entities accessed from the database, including
        secondary entities, eagerly-loaded collection items.

        All changes present on entities which are already present in the
        session will be reset and the entities will all be marked "clean".

        This is essentially the en-masse version of load().
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

        if isinstance(entity, type):
            entity = mapper.class_mapper(entity)
        if alias is not None:
            alias = mapperutil.AliasedClauses(entity.mapped_table, alias=alias)

        q._entities = q._entities + [(entity, alias, id)]
        return q

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

        # duck type to get a ClauseElement
        if hasattr(column, 'clause_element'):
            column = column.clause_element()

        # alias non-labeled column elements.
        if isinstance(column, sql.ColumnElement) and not hasattr(column, '_label'):
            column = column.label(None)

        q._entities = q._entities + [(column, None, id)]
        return q

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
            d = args[0]
            kwargs.update(d)
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

        if self._adapter is not None:
            criterion = self._adapter.traverse(criterion)

        q = self._no_statement("filter")
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

    def _get_joinable_tables(self):
        if not self._joinable_tables or self._joinable_tables[0] is not self._from_obj:
            currenttables = [self._from_obj]
            def visit_join(join):
                currenttables.append(join.left)
                currenttables.append(join.right)
            visitors.traverse(self._from_obj, visit_join=visit_join, traverse_options={'column_collections':False, 'aliased_selectables':False})
            self._joinable_tables = (self._from_obj, currenttables)
            return currenttables
        else:
            return self._joinable_tables[1]

    def _join_to(self, keys, outerjoin=False, start=None, create_aliases=True):
        if start is None:
            start = self._joinpoint

        clause = self._from_obj

        currenttables = self._get_joinable_tables()

        # determine if generated joins need to be aliased on the left
        # hand side.
        if self._adapter and not self._aliases:  # at the beginning of a join, look at leftmost adapter
            adapt_against = self._adapter.selectable
        elif start.select_table is not start.mapped_table: # in the middle of a join, look for a polymorphic mapper
            adapt_against = start.select_table
        else:
            adapt_against = None

        mapper = start
        alias = self._aliases
        if not isinstance(keys, list):
            keys = [keys]
        for key in keys:
            use_selectable = None
            if isinstance(key, tuple):
                key, use_selectable = key

            if isinstance(key, interfaces.PropComparator):
                prop = key.property
            else:
                prop = mapper.get_property(key, resolve_synonyms=True)

            if use_selectable:
                if not use_selectable.is_derived_from(prop.mapper.mapped_table):
                    raise exceptions.InvalidRequestError("Selectable '%s' is not derived from '%s'" % (use_selectable.description, prop.mapper.mapped_table.description))
                if not isinstance(use_selectable, expression.Alias):
                    use_selectable = use_selectable.alias()

            if prop._is_self_referential() and not create_aliases and not use_selectable:
                raise exceptions.InvalidRequestError("Self-referential query on '%s' property requires create_aliases=True argument." % str(prop))

            if prop.select_table not in currenttables or create_aliases or use_selectable:
                if prop.secondary:
                    if use_selectable:
                        alias = mapperutil.PropertyAliasedClauses(prop,
                            prop.primary_join_against(mapper, adapt_against),
                            prop.secondary_join_against(mapper),
                            alias,
                            alias=use_selectable
                        )
                        crit = alias.primaryjoin
                        clause = clause.join(alias.secondary, crit, isouter=outerjoin).join(alias.alias, alias.secondaryjoin, isouter=outerjoin)
                    elif create_aliases:
                        alias = mapperutil.PropertyAliasedClauses(prop,
                            prop.primary_join_against(mapper, adapt_against),
                            prop.secondary_join_against(mapper),
                            alias
                        )
                        crit = alias.primaryjoin
                        clause = clause.join(alias.secondary, crit, isouter=outerjoin).join(alias.alias, alias.secondaryjoin, isouter=outerjoin)
                    else:
                        crit = prop.primary_join_against(mapper, adapt_against)
                        clause = clause.join(prop.secondary, crit, isouter=outerjoin)
                        clause = clause.join(prop.select_table, prop.secondary_join_against(mapper), isouter=outerjoin)
                else:
                    if use_selectable:
                        alias = mapperutil.PropertyAliasedClauses(prop,
                            prop.primary_join_against(mapper, adapt_against),
                            None,
                            alias,
                            alias=use_selectable
                        )
                        crit = alias.primaryjoin
                        clause = clause.join(alias.alias, crit, isouter=outerjoin)
                    elif create_aliases:
                        alias = mapperutil.PropertyAliasedClauses(prop,
                            prop.primary_join_against(mapper, adapt_against),
                            None,
                            alias
                        )
                        crit = alias.primaryjoin
                        clause = clause.join(alias.alias, crit, isouter=outerjoin)
                    else:
                        crit = prop.primary_join_against(mapper, adapt_against)
                        clause = clause.join(prop.select_table, crit, isouter=outerjoin)
            elif not create_aliases and prop.secondary is not None and prop.secondary not in currenttables:
                # TODO: this check is not strong enough for different paths to the same endpoint which
                # does not use secondary tables
                raise exceptions.InvalidRequestError("Can't join to property '%s'; a path to this table along a different secondary table already exists.  Use the `alias=True` argument to `join()`." % prop.key)

            mapper = prop.mapper

            if use_selectable:
                adapt_against = use_selectable
            elif mapper.select_table is not mapper.mapped_table:
                adapt_against = mapper.select_table

        return (clause, mapper, alias)

    def _generative_col_aggregate(self, col, func):
        """apply the given aggregate function to the query and return the newly
        resulting ``Query``.
        """
        if self._column_aggregate is not None:
            raise exceptions.InvalidRequestError("Query already contains an aggregate column or function")
        q = self._no_statement("aggregate")
        q._column_aggregate = (col, func)
        return q

    def apply_min(self, col):
        """apply the SQL ``min()`` function against the given column to the
        query and return the newly resulting ``Query``.
        """
        return self._generative_col_aggregate(col, sql.func.min)

    def apply_max(self, col):
        """apply the SQL ``max()`` function against the given column to the
        query and return the newly resulting ``Query``.
        """
        return self._generative_col_aggregate(col, sql.func.max)

    def apply_sum(self, col):
        """apply the SQL ``sum()`` function against the given column to the
        query and return the newly resulting ``Query``.
        """
        return self._generative_col_aggregate(col, sql.func.sum)

    def apply_avg(self, col):
        """apply the SQL ``avg()`` function against the given column to the
        query and return the newly resulting ``Query``.
        """
        return self._generative_col_aggregate(col, sql.func.avg)

    def _col_aggregate(self, col, func):
        """Execute ``func()`` function against the given column.

        For performance, only use subselect if `order_by` attribute is set.
        """

        ops = {'distinct':self._distinct, 'order_by':self._order_by or None, 'from_obj':self._from_obj}

        if self._autoflush and not self._populate_existing:
            self.session._autoflush()

        if self._order_by is not False:
            s1 = sql.select([col], self._criterion, **ops).alias('u')
            return self.session.execute(sql.select([func(s1.corresponding_column(col))]), mapper=self.mapper).scalar()
        else:
            return self.session.execute(sql.select([func(col)], self._criterion, **ops), mapper=self.mapper).scalar()

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

    def order_by(self, criterion):
        """apply one or more ORDER BY criterion to the query and return the newly resulting ``Query``"""

        q = self._no_statement("order_by")

        if self._adapter:
            criterion = [expression._literal_as_text(o) for o in util.to_list(criterion) or []]
            criterion = self._adapter.copy_and_process(criterion)

        if q._order_by is False:
            q._order_by = util.to_list(criterion)
        else:
            q._order_by = q._order_by + util.to_list(criterion)
        return q

    def group_by(self, criterion):
        """apply one or more GROUP BY criterion to the query and return the newly resulting ``Query``"""

        q = self._no_statement("group_by")
        if q._group_by is False:
            q._group_by = util.to_list(criterion)
        else:
            q._group_by = q._group_by + util.to_list(criterion)
        return q

    def having(self, criterion):
        """apply a HAVING criterion to the query and return the newly resulting ``Query``."""

        if isinstance(criterion, basestring):
            criterion = sql.text(criterion)

        if criterion is not None and not isinstance(criterion, sql.ClauseElement):
            raise exceptions.ArgumentError("having() argument must be of type sqlalchemy.sql.ClauseElement or string")

        if self._adapter is not None:
            criterion = self._adapter.traverse(criterion)

        q = self._no_statement("having")
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
        q = self._no_statement("join")
        q._from_obj = clause
        q._joinpoint = mapper
        q._aliases = aliases

        if aliases:
            q._adapter = sql_util.ClauseAdapter(aliases.alias).copy_and_chain(q._adapter)
        else:
            select_mapper = mapper.get_select_mapper()
            if select_mapper._clause_adapter:
                q._adapter = select_mapper._clause_adapter.copy_and_chain(q._adapter)

        a = aliases
        while a is not None:
            q._alias_ids.setdefault(a.mapper, []).append(a)
            q._alias_ids.setdefault(a.table, []).append(a)
            q._alias_ids.setdefault(a.alias, []).append(a)
            a = a.parentclauses

        if id:
            q._alias_ids[id] = aliases
        return q

    def reset_joinpoint(self):
        """return a new Query reset the 'joinpoint' of this Query reset
        back to the starting mapper.  Subsequent generative calls will
        be constructed from the new joinpoint.

        Note that each call to join() or outerjoin() also starts from
        the root.
        """

        q = self._no_statement("reset_joinpoint")
        q._joinpoint = q.mapper
        q._aliases = None
        if q.table not in q._get_joinable_tables():
            q._adapter = sql_util.ClauseAdapter(q._from_obj, equivalents=q.mapper._equivalent_columns)
        return q


    def select_from(self, from_obj):
        """Set the `from_obj` parameter of the query and return the newly
        resulting ``Query``.  This replaces the table which this Query selects
        from with the given table.


        `from_obj` is a single table or selectable.
        """

        new = self._no_criterion('select_from')
        if isinstance(from_obj, (tuple, list)):
            util.warn_deprecated("select_from() now accepts a single Selectable as its argument, which replaces any existing FROM criterion.")
            from_obj = from_obj[-1]

        if isinstance(from_obj, expression._SelectBaseMixin):
            # alias SELECTs and unions
            from_obj = from_obj.alias()

        new._from_obj = from_obj

        if new.table not in new._get_joinable_tables():
            new._adapter = sql_util.ClauseAdapter(new._from_obj, equivalents=new.mapper._equivalent_columns)
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

        new = self._no_statement("distinct")
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
        q = self._no_criterion('from_statement')
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
        """Return the first result of this ``Query``, raising an exception if more than one row exists.

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
        return list(self.iterate_instances(cursor, *mappers_or_columns, **kwargs))

    def iterate_instances(self, cursor, *mappers_or_columns, **kwargs):
        session = self.session

        context = kwargs.pop('querycontext', None)
        if context is None:
            context = QueryContext(self)

        context.runid = _new_runid()

        mappers_or_columns = tuple(self._entities) + mappers_or_columns
        tuples = bool(mappers_or_columns)

        if context.row_adapter:
            def main(context, row):
                return self.select_mapper._instance(context, context.row_adapter(row), None,
                    extension=context.extension, only_load_props=context.only_load_props, refresh_instance=context.refresh_instance
                )
        else:
            def main(context, row):
                return self.select_mapper._instance(context, row, None,
                    extension=context.extension, only_load_props=context.only_load_props, refresh_instance=context.refresh_instance
                )

        if tuples:
            process = []
            process.append(main)
            for tup in mappers_or_columns:
                if isinstance(tup, tuple):
                    (m, alias, alias_id) = tup
                    clauses = self._get_entity_clauses(tup)
                else:
                    clauses = alias = alias_id = None
                    m = tup

                if isinstance(m, type):
                    m = mapper.class_mapper(m)

                if isinstance(m, mapper.Mapper):
                    def x(m):
                        row_adapter = clauses is not None and clauses.row_decorator or (lambda row: row)
                        def proc(context, row):
                            return m._instance(context, row_adapter(row), None)
                        process.append(proc)
                    x(m)
                elif isinstance(m, (sql.ColumnElement, basestring)):
                    def y(m):
                        row_adapter = clauses is not None and clauses.row_decorator or (lambda row: row)
                        def proc(context, row):
                            return row_adapter(row)[m]
                        process.append(proc)
                    y(m)
                else:
                    raise exceptions.InvalidRequestError("Invalid column expression '%r'" % m)

        while True:
            context.progress = util.Set()
            context.partials = {}

            if self._yield_per:
                fetch = cursor.fetchmany(self._yield_per)
                if not fetch:
                    return
            else:
                fetch = cursor.fetchall()

            if tuples:
                rows = util.OrderedSet()
                for row in fetch:
                    rows.add(tuple([proc(context, row) for proc in process]))
            else:
                rows = util.UniqueAppender([])
                for row in fetch:
                    rows.append(main(context, row))

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
        if ident is not None:
            q = q._no_criterion('get')
            params = {}
            (_get_clause, _get_params) = self.select_mapper._get_clause
            q = q.filter(_get_clause)
            for i, primary_key in enumerate(self.primary_key_columns):
                try:
                    params[_get_params[primary_key].key] = ident[i]
                except IndexError:
                    raise exceptions.InvalidRequestError("Could not find enough values to formulate primary key for query.get(); primary key columns are %s" % ', '.join(["'%s'" % str(c) for c in self.primary_key_columns]))
            q = q.params(params)

        if lockmode is not None:
            q = q.with_lockmode(lockmode)
        q = q._select_context_options(populate_existing=bool(refresh_instance), version_check=(lockmode is not None), only_load_props=only_load_props, refresh_instance=refresh_instance)
        q._order_by = None
        try:
            # call using all() to avoid LIMIT compilation complexity
            return q.all()[0]
        except IndexError:
            return None

    def _nestable(self, **kwargs):
        """Return true if the given statement options imply it should be nested."""

        return (kwargs.get('limit') is not None or kwargs.get('offset') is not None or kwargs.get('distinct', False))

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

        whereclause = self._criterion

        context = QueryContext(self)
        from_obj = self._from_obj

        if self._nestable(**self._select_args()):
            s = sql.select([self.table], whereclause, from_obj=from_obj, **self._select_args()).alias('getcount').count()
        else:
            primary_key = self.primary_key_columns
            s = sql.select([sql.func.count(list(primary_key)[0])], whereclause, from_obj=from_obj, **self._select_args())
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

        whereclause = self._criterion

        from_obj = self._from_obj

        # if the query's ClauseAdapter is present, and its
        # specifically adapting against a modified "select_from"
        # argument, apply adaptation to the
        # individually selected columns as well as "eager" clauses added;
        # otherwise its currently not needed
        if self._adapter and self.table not in self._get_joinable_tables():
            adapter = self._adapter
        else:
            adapter = None

        adapter = self._adapter

        # TODO: mappers added via add_entity(), adapt their queries also,
        # if those mappers are polymorphic

        order_by = self._order_by
        if order_by is False:
            order_by = self.select_mapper.order_by
        if order_by is False:
            order_by = []
            if self.table.default_order_by() is not None:
                order_by = self.table.default_order_by()
            if from_obj.default_order_by() is not None:
                order_by = from_obj.default_order_by()

        try:
            for_update = {'read':'read','update':True,'update_nowait':'nowait',None:False}[self._lockmode]
        except KeyError:
            raise exceptions.ArgumentError("Unknown lockmode '%s'" % self._lockmode)

        # if single-table inheritance mapper, add "typecol IN (polymorphic)" criterion so
        # that we only load the appropriate types
        if self.select_mapper.single and self.select_mapper.polymorphic_on is not None and self.select_mapper.polymorphic_identity is not None:
            whereclause = sql.and_(whereclause, self.select_mapper.polymorphic_on.in_([m.polymorphic_identity for m in self.select_mapper.polymorphic_iterator()]))

        context.from_clause = from_obj

        # give all the attached properties a chance to modify the query
        # TODO: doing this off the select_mapper.  if its the polymorphic mapper, then
        # it has no relations() on it.  should we compile those too into the query ?  (i.e. eagerloads)
        for value in self.select_mapper.iterate_properties:
            if self._only_load_props and value.key not in self._only_load_props:
                continue
            context.exec_with_path(self.select_mapper, value.key, value.setup, context, only_load_props=self._only_load_props)

        # additional entities/columns, add those to selection criterion
        for tup in self._entities:
            (m, alias, alias_id) = tup
            clauses = self._get_entity_clauses(tup)
            if isinstance(m, mapper.Mapper):
                for value in m.iterate_properties:
                    context.exec_with_path(m, value.key, value.setup, context, parentclauses=clauses)
            elif isinstance(m, sql.ColumnElement):
                if clauses is not None:
                    m = clauses.aliased_column(m)
                context.secondary_columns.append(m)

        if self._eager_loaders and self._nestable(**self._select_args()):
            # eager loaders are present, and the SELECT has limiting criterion
            # produce a "wrapped" selectable.

            # ensure all 'order by' elements are ClauseElement instances
            # (since they will potentially be aliased)
            # locate all embedded Column clauses so they can be added to the
            # "inner" select statement where they'll be available to the enclosing
            # statement's "order by"

            cf = util.Set()
            if order_by:
                order_by = [expression._literal_as_text(o) for o in util.to_list(order_by) or []]
                for o in order_by:
                    cf.update(sql_util.find_columns(o))

            if adapter:
                # TODO: make usage of the ClauseAdapter here to create the list
                # of primary columns
                context.primary_columns = [from_obj.corresponding_column(c) or c for c in context.primary_columns]
                cf = [from_obj.corresponding_column(c) or c for c in cf]

            s2 = sql.select(context.primary_columns + list(cf), whereclause, from_obj=context.from_clause, use_labels=True, correlate=False, order_by=util.to_list(order_by), **self._select_args())

            s3 = s2.alias()

            context.row_adapter = mapperutil.create_row_adapter(s3, self.table)

            statement = sql.select([s3] + context.secondary_columns, for_update=for_update, use_labels=True)

            if context.eager_joins:
                eager_joins = sql_util.ClauseAdapter(s3).traverse(context.eager_joins)
                statement.append_from(eager_joins, _copy_collection=False)

            if order_by:
                statement.append_order_by(*sql_util.ClauseAdapter(s3).copy_and_process(order_by))

            statement.append_order_by(*context.eager_order_by)
        else:
            if adapter:
                # TODO: make usage of the ClauseAdapter here to create row adapter, list
                # of primary columns
                context.primary_columns = [from_obj.corresponding_column(c) or c for c in context.primary_columns]
                context.row_adapter = mapperutil.create_row_adapter(from_obj, self.table)

            if self._distinct:
                if order_by:
                    order_by = [expression._literal_as_text(o) for o in util.to_list(order_by) or []]

                if self._distinct and order_by:
                    cf = util.Set()
                    for o in order_by:
                        cf.update(sql_util.find_columns(o))
                    for c in cf:
                        context.primary_columns.append(c)

            statement = sql.select(context.primary_columns + context.secondary_columns, whereclause, from_obj=from_obj, use_labels=True, for_update=for_update, order_by=util.to_list(order_by), **self._select_args())

            if context.eager_joins:
                if adapter:
                    context.eager_joins = adapter.traverse(context.eager_joins)
                statement.append_from(context.eager_joins, _copy_collection=False)

            if context.eager_order_by:
                if adapter:
                    context.eager_order_by = adapter.copy_and_process(context.eager_order_by)
                statement.append_order_by(*context.eager_order_by)

        context.statement = statement

        return context

    def _select_args(self):
        """Return a dictionary of attributes that can be applied to a ``sql.Select`` statement.
        """
        return {'limit':self._limit, 'offset':self._offset, 'distinct':self._distinct, 'group_by':self._group_by or None, 'having':self._having or None}


    def _get_entity_clauses(self, m):
        """for tuples added via add_entity() or add_column(), attempt to locate
        an AliasedClauses object which should be used to formulate the query as well
        as to process result rows."""

        (m, alias, alias_id) = m
        if alias is not None:
            return alias
        if alias_id is not None:
            try:
                return self._alias_ids[alias_id]
            except KeyError:
                raise exceptions.InvalidRequestError("Query has no alias identified by '%s'" % alias_id)

        if isinstance(m, type):
            m = mapper.class_mapper(m)
        if isinstance(m, mapper.Mapper):
            l = self._alias_ids.get(m)
            if l:
                if len(l) > 1:
                    raise exceptions.InvalidRequestError("Ambiguous join for entity '%s'; specify id=<someid> to query.join()/query.add_entity()" % str(m))
                else:
                    return l[0]
            else:
                return None
        elif isinstance(m, sql.ColumnElement):
            aliases = []
            for table in sql_util.find_tables(m, check_columns=True):
                for a in self._alias_ids.get(table, []):
                    aliases.append(a)
            if len(aliases) > 1:
                raise exceptions.InvalidRequestError("Ambiguous join for entity '%s'; specify id=<someid> to query.join()/query.add_column()" % str(m))
            elif len(aliases) == 1:
                return aliases[0]
            else:
                return None

    def __log_debug(self, msg):
        self.logger.debug(msg)

    def __str__(self):
        return str(self.compile())

    # DEPRECATED LAND !

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
        else:
            return self.filter(arg)._legacy_select_kwargs(**kwargs)

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
        q._select_context_options(**kwargs)
        return list(q)

    def _select_context_options(self, populate_existing=None, version_check=None, only_load_props=None, refresh_instance=None): #pragma: no cover
        if populate_existing:
            self._populate_existing = populate_existing
        if version_check:
            self._version_check = version_check
        if refresh_instance:
            self._refresh_instance = refresh_instance
        if only_load_props:
            self._only_load_props = util.Set(only_load_props)
        return self

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
                clause = prop.get_join(mapper)
            else:
                clause &= prop.get_join(mapper)
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
                          'selectone_by'):
    setattr(Query, deprecated_method,
            util.deprecated(getattr(Query, deprecated_method),
                            add_deprecation_to_docstring=False))

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

    def exec_with_path(self, mapper, propkey, func, *args, **kwargs):
        oldpath = self.path
        self.path += (mapper.base_mapper, propkey)
        try:
            return func(*args, **kwargs)
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
