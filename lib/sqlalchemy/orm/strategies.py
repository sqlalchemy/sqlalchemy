# strategies.py
# Copyright (C) 2005, 2006, 2007 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""sqlalchemy.orm.interfaces.LoaderStrategy implementations, and related MapperOptions."""

from sqlalchemy import sql, schema, util, exceptions, sql_util, logging
from sqlalchemy.orm import mapper, query
from sqlalchemy.orm.interfaces import *
from sqlalchemy.orm import session as sessionlib
from sqlalchemy.orm import util as mapperutil
import random


class ColumnLoader(LoaderStrategy):
    def init(self):
        super(ColumnLoader, self).init()
        self.columns = self.parent_property.columns
        self._should_log_debug = logging.is_debug_enabled(self.logger)
        
    def setup_query(self, context, eagertable=None, parentclauses=None, **kwargs):
        for c in self.columns:
            if parentclauses is not None:
                context.statement.append_column(parentclauses.aliased_column(c))
            else:
                context.statement.append_column(c)
        
    def init_class_attribute(self):
        self.logger.info("register managed attribute %s on class %s" % (self.key, self.parent.class_.__name__))
        coltype = self.columns[0].type
        sessionlib.attribute_manager.register_attribute(self.parent.class_, self.key, uselist=False, copy_function=coltype.copy_value, compare_function=coltype.compare_values, mutable_scalars=self.columns[0].type.is_mutable())

    def process_row(self, selectcontext, instance, row, identitykey, isnew):
        if isnew:
            if self._should_log_debug:
                self.logger.debug("populating %s with %s/%s" % (mapperutil.attribute_str(instance, self.key), row.__class__.__name__, self.columns[0].key))
            instance.__dict__[self.key] = row[self.columns[0]]
        
ColumnLoader.logger = logging.class_logger(ColumnLoader)

class DeferredColumnLoader(LoaderStrategy):
    """Describes an object attribute that corresponds to a table
    column, which also will *lazy load* its value from the table.

    This is per-column lazy loading.
    """
    
    def init(self):
        super(DeferredColumnLoader, self).init()
        self.columns = self.parent_property.columns
        self.group = self.parent_property.group
        self._should_log_debug = logging.is_debug_enabled(self.logger)

    def init_class_attribute(self):
        self.logger.info("register managed attribute %s on class %s" % (self.key, self.parent.class_.__name__))
        sessionlib.attribute_manager.register_attribute(self.parent.class_, self.key, uselist=False, callable_=lambda i:self.setup_loader(i), copy_function=lambda x: self.columns[0].type.copy_value(x), compare_function=lambda x,y:self.columns[0].type.compare_values(x,y), mutable_scalars=self.columns[0].type.is_mutable())

    def setup_query(self, context, **kwargs):
        pass
        
    def process_row(self, selectcontext, instance, row, identitykey, isnew):
        if isnew:
            if not self.is_default or len(selectcontext.options):
                sessionlib.attribute_manager.init_instance_attribute(instance, self.key, False, callable_=self.setup_loader(instance, selectcontext.options))
            else:
                sessionlib.attribute_manager.reset_instance_attribute(instance, self.key)

    def setup_loader(self, instance, options=None):
        if not mapper.has_mapper(instance):
            return None
        else:
            prop = mapper.object_mapper(instance).props[self.key]
            if prop is not self.parent_property:
                return prop._get_strategy(DeferredColumnLoader).setup_loader(instance)
        def lazyload():
            if self._should_log_debug:
                self.logger.debug("deferred load %s group %s" % (mapperutil.attribute_str(instance, self.key), str(self.group)))

            if not mapper.has_identity(instance):
                return None

            try:
                pk = self.parent.pks_by_table[self.columns[0].table]
            except KeyError:
                pk = self.columns[0].table.primary_key

            clause = sql.and_()
            for primary_key in pk:
                attr = self.parent.get_attr_by_column(instance, primary_key)
                if not attr:
                    return None
                clause.clauses.append(primary_key == attr)

            session = sessionlib.object_session(instance)
            if session is None:
                raise exceptions.InvalidRequestError("Parent instance %s is not bound to a Session; deferred load operation of attribute '%s' cannot proceed" % (instance.__class__, self.key))

            localparent = mapper.object_mapper(instance)
            if self.group is not None:
                groupcols = [p for p in localparent.props.values() if isinstance(p.strategy, DeferredColumnLoader) and p.group==self.group]
                result = session.execute(localparent, sql.select([g.columns[0] for g in groupcols], clause, use_labels=True), None)
                try:
                    row = result.fetchone()
                    for prop in groupcols:
                        if prop is self:
                            continue
                        # set a scalar object instance directly on the object, 
                        # bypassing SmartProperty event handlers.
                        sessionlib.attribute_manager.init_instance_attribute(instance, prop.key, uselist=False)
                        instance.__dict__[prop.key] = row[prop.columns[0]]
                    return row[self.columns[0]]    
                finally:
                    result.close()
            else:
                return session.scalar(localparent, sql.select([self.columns[0]], clause, use_labels=True),None)

        return lazyload
                
DeferredColumnLoader.logger = logging.class_logger(DeferredColumnLoader)

class DeferredOption(StrategizedOption):
    def __init__(self, key, defer=False):
        super(DeferredOption, self).__init__(key)
        self.defer = defer

    def get_strategy_class(self):
        if self.defer:
            return DeferredColumnLoader
        else:
            return ColumnLoader

class AbstractRelationLoader(LoaderStrategy):
    def init(self):
        super(AbstractRelationLoader, self).init()
        for attr in ['primaryjoin', 'secondaryjoin', 'secondary', 'foreign_keys', 'mapper', 'select_mapper', 'target', 'select_table', 'loads_polymorphic', 'uselist', 'cascade', 'attributeext', 'order_by', 'remote_side', 'polymorphic_primaryjoin', 'polymorphic_secondaryjoin', 'direction']:
            setattr(self, attr, getattr(self.parent_property, attr))
        self._should_log_debug = logging.is_debug_enabled(self.logger)
        
    def _init_instance_attribute(self, instance, callable_=None):
        return sessionlib.attribute_manager.init_instance_attribute(instance, self.key, self.uselist, cascade=self.cascade,  trackparent=True, callable_=callable_)
        
    def _register_attribute(self, class_, callable_=None):
        self.logger.info("register managed %s attribute %s on class %s" % ((self.uselist and "list-holding" or "scalar"), self.key, self.parent.class_.__name__))
        sessionlib.attribute_manager.register_attribute(class_, self.key, uselist = self.uselist, extension=self.attributeext, cascade=self.cascade,  trackparent=True, typecallable=self.parent_property.collection_class, callable_=callable_)

class NoLoader(AbstractRelationLoader):
    def init_class_attribute(self):
        self._register_attribute(self.parent.class_)

    def process_row(self, selectcontext, instance, row, identitykey, isnew):
        if isnew:
            if not self.is_default or len(selectcontext.options):
                if self._should_log_debug:
                    self.logger.debug("set instance-level no loader on %s" % mapperutil.attribute_str(instance, self.key))
                self._init_instance_attribute(instance)

NoLoader.logger = logging.class_logger(NoLoader)
        
class LazyLoader(AbstractRelationLoader):
    def init(self):
        super(LazyLoader, self).init()
        (self.lazywhere, self.lazybinds, self.lazyreverse) = self._create_lazy_clause(self)
        
        self.logger.info(str(self.parent_property) + " lazy loading clause " + str(self.lazywhere))

        # determine if our "lazywhere" clause is the same as the mapper's
        # get() clause.  then we can just use mapper.get()
        self.use_get = not self.uselist and query.Query(self.mapper)._get_clause.compare(self.lazywhere)
        if self.use_get:
            self.logger.info(str(self.parent_property) + " will use query.get() to optimize instance loads")

    def init_class_attribute(self):
        self._register_attribute(self.parent.class_, callable_=lambda i: self.setup_loader(i))

    def setup_loader(self, instance, options=None):
        if not mapper.has_mapper(instance):
            return None
        else:
            prop = mapper.object_mapper(instance).props[self.key]
            if prop is not self.parent_property:
                return prop._get_strategy(LazyLoader).setup_loader(instance)
        def lazyload():
            self.logger.debug("lazy load attribute %s on instance %s" % (self.key, mapperutil.instance_str(instance)))
            params = {}
            allparams = True
            # if the instance wasnt loaded from the database, then it cannot lazy load
            # child items.  one reason for this is that a bi-directional relationship
            # will not update properly, since bi-directional uses lazy loading functions
            # in both directions, and this instance will not be present in the lazily-loaded
            # results of the other objects since its not in the database
            if not mapper.has_identity(instance):
                return None
            #print "setting up loader, lazywhere", str(self.lazywhere), "binds", self.lazybinds
            for col, bind in self.lazybinds.iteritems():
                params[bind.key] = self.parent.get_attr_by_column(instance, col)
                if params[bind.key] is None:
                    allparams = False
                    break

            if not allparams:
                return None

            session = sessionlib.object_session(instance)
            if session is None:
                try:
                    session = mapper.object_mapper(instance).get_session()
                except exceptions.InvalidRequestError:
                    raise exceptions.InvalidRequestError("Parent instance %s is not bound to a Session, and no contextual session is established; lazy load operation of attribute '%s' cannot proceed" % (instance.__class__, self.key))

            # if we have a simple straight-primary key load, use mapper.get()
            # to possibly save a DB round trip
            if self.use_get:
                ident = []
                for primary_key in self.select_mapper.pks_by_table[self.select_mapper.mapped_table]:
                    bind = self.lazyreverse[primary_key]
                    ident.append(params[bind.key])
                return session.query(self.mapper).get(ident)
            elif self.order_by is not False:
                order_by = self.order_by
            elif self.secondary is not None and self.secondary.default_order_by() is not None:
                order_by = self.secondary.default_order_by()
            else:
                order_by = False
            result = session.query(self.mapper, with_options=options).select_whereclause(self.lazywhere, order_by=order_by, params=params)

            if self.uselist:
                return result
            else:
                if len(result):
                    return result[0]
                else:
                    return None
        return lazyload

    def process_row(self, selectcontext, instance, row, identitykey, isnew):
        if isnew:
            # new object instance being loaded from a result row
            if not self.is_default or len(selectcontext.options):
                self.logger.debug("set instance-level lazy loader on %s" % mapperutil.attribute_str(instance, self.key))
                # we are not the primary manager for this attribute on this class - set up a per-instance lazyloader,
                # which will override the clareset_instance_attributess-level behavior
                self._init_instance_attribute(instance, callable_=self.setup_loader(instance, selectcontext.options))
            else:
                self.logger.debug("set class-level lazy loader on %s" % mapperutil.attribute_str(instance, self.key))
                # we are the primary manager for this attribute on this class - reset its per-instance attribute state, 
                # so that the class-level lazy loader is executed when next referenced on this instance.
                # this usually is not needed unless the constructor of the object referenced the attribute before we got 
                # to load data into it.
                sessionlib.attribute_manager.reset_instance_attribute(instance, self.key)

    def _create_lazy_clause(cls, prop, reverse_direction=False):
        (primaryjoin, secondaryjoin, remote_side) = (prop.polymorphic_primaryjoin, prop.polymorphic_secondaryjoin, prop.remote_side)
        
        binds = {}
        reverse = {}

        def should_bind(targetcol, othercol):
            if reverse_direction and not secondaryjoin:
                return targetcol in remote_side
            else:
                return othercol in remote_side

        def find_column_in_expr(expr):
            if not isinstance(expr, sql.ColumnElement):
                return None
            columns = []
            class FindColumnInColumnClause(sql.ClauseVisitor):
                def visit_column(self, c):
                    columns.append(c)
            FindColumnInColumnClause().traverse(expr)
            return len(columns) and columns[0] or None
        
        def bind_label():
            # TODO: make this generation deterministic
            return "lazy_" + hex(random.randint(0, 65535))[2:]

        def visit_binary(binary):
            leftcol = find_column_in_expr(binary.left)
            rightcol = find_column_in_expr(binary.right)
            if leftcol is None or rightcol is None:
                return
            if should_bind(leftcol, rightcol):
                col = leftcol
                binary.left = binds.setdefault(leftcol,
                        sql.bindparam(bind_label(), None, shortname=leftcol.name, type=binary.right.type, unique=True))
                reverse[rightcol] = binds[col]

            # the "left is not right" compare is to handle part of a join clause that is "table.c.col1==table.c.col1",
            # which can happen in rare cases (test/orm/relationships.py RelationTest2)
            if leftcol is not rightcol and should_bind(rightcol, leftcol):
                col = rightcol
                binary.right = binds.setdefault(rightcol,
                        sql.bindparam(bind_label(), None, shortname=rightcol.name, type=binary.left.type, unique=True))
                reverse[leftcol] = binds[col]

        lazywhere = primaryjoin.copy_container()
        li = mapperutil.BinaryVisitor(visit_binary)
        
        if not secondaryjoin or not reverse_direction:
            li.traverse(lazywhere)
        
        if secondaryjoin is not None:
            secondaryjoin = secondaryjoin.copy_container()
            if reverse_direction:
                li.traverse(secondaryjoin)
            lazywhere = sql.and_(lazywhere, secondaryjoin)

        return (lazywhere, binds, reverse)
    _create_lazy_clause = classmethod(_create_lazy_clause)
    
LazyLoader.logger = logging.class_logger(LazyLoader)


class EagerLoader(AbstractRelationLoader):
    """Loads related objects inline with a parent query."""
    
    def init(self):
        super(EagerLoader, self).init()
        if self.parent.isa(self.mapper):
            raise exceptions.ArgumentError(
                "Error creating eager relationship '%s' on parent class '%s' "
                "to child class '%s': Cant use eager loading on a self "
                "referential relationship." %
                (self.key, repr(self.parent.class_), repr(self.mapper.class_)))
        if self.is_default:
            self.parent._eager_loaders.add(self.parent_property)

        self.clauses = {}
        self.clauses_by_lead_mapper = {}

    class AliasedClauses(object):
        """Defines a set of join conditions and table aliases which
        are aliased on a randomly-generated alias name, corresponding
        to the connection of an optional parent AliasedClauses object
        and a target mapper.
        
        EagerLoader has a distinct AliasedClauses object per parent
        AliasedClauses object, so that all paths from one mapper to
        another across a chain of eagerloaders generates a distinct
        chain of joins.  The AliasedClauses objects are generated and
        cached on an as-needed basis.
        
        E.g.::
        
            mapper A -->
                (EagerLoader 'items') --> 
                    mapper B --> 
                        (EagerLoader 'keywords') --> 
                            mapper C
            
        will generate::
            
            EagerLoader 'items' --> {
                None : AliasedClauses(items, None, alias_suffix='AB34')        # mappera JOIN mapperb_AB34
            }
            
            EagerLoader 'keywords' --> [
                None : AliasedClauses(keywords, None, alias_suffix='43EF')     # mapperb JOIN mapperc_43EF
                AliasedClauses(items, None, alias_suffix='AB34') : 
                        AliasedClauses(keywords, items, alias_suffix='8F44')   # mapperb_AB34 JOIN mapperc_8F44
            ]
        """
        
        def __init__(self, eagerloader, parentclauses=None):
            self.id = (parentclauses is not None and (parentclauses.id + "/") or '') + str(eagerloader.parent_property)
            self.parent = eagerloader
            self.target = eagerloader.select_table
            self.eagertarget = eagerloader.select_table.alias(self._aliashash("/target"))
            self.extra_cols = {}

            if eagerloader.secondary:
                self.eagersecondary = eagerloader.secondary.alias(self._aliashash("/secondary"))
                if parentclauses is not None:
                    aliasizer = sql_util.ClauseAdapter(self.eagertarget).\
                            chain(sql_util.ClauseAdapter(self.eagersecondary)).\
                            chain(sql_util.ClauseAdapter(parentclauses.eagertarget))
                else:
                    aliasizer = sql_util.ClauseAdapter(self.eagertarget).\
                        chain(sql_util.ClauseAdapter(self.eagersecondary))
                self.eagersecondaryjoin = eagerloader.polymorphic_secondaryjoin.copy_container()
                aliasizer.traverse(self.eagersecondaryjoin)
                self.eagerprimary = eagerloader.polymorphic_primaryjoin.copy_container()
                aliasizer.traverse(self.eagerprimary)
            else:
                self.eagerprimary = eagerloader.polymorphic_primaryjoin.copy_container()
                if parentclauses is not None: 
                    aliasizer = sql_util.ClauseAdapter(self.eagertarget)
                    aliasizer.chain(sql_util.ClauseAdapter(parentclauses.eagertarget, exclude=eagerloader.parent_property.remote_side))
                else:
                    aliasizer = sql_util.ClauseAdapter(self.eagertarget)
                aliasizer.traverse(self.eagerprimary)

            if eagerloader.order_by:
                self.eager_order_by = sql_util.ClauseAdapter(self.eagertarget).copy_and_process(util.to_list(eagerloader.order_by))
            else:
                self.eager_order_by = None

            self._row_decorator = self._create_decorator_row()
        
        def aliased_column(self, column):
            """return the aliased version of the given column, creating a new label for it if not already
            present in this AliasedClauses eagertable."""

            conv = self.eagertarget.corresponding_column(column, raiseerr=False)
            if conv:
                return conv

            if column in self.extra_cols:
                return self.extra_cols[column]
            
            aliased_column = column.copy_container()
            sql_util.ClauseAdapter(self.eagertarget).traverse(aliased_column)
            alias = self._aliashash(column.name)
            aliased_column = aliased_column.label(alias)
            self._row_decorator.map[column] = alias
            self.extra_cols[column] = aliased_column
            return aliased_column
            
        def _aliashash(self, extra):
            """return a deterministic 4 digit hash value for this AliasedClause's id + extra."""
            # use the first 4 digits of an MD5 hash
            return "anon_" + util.hash(self.id + extra)[0:4]
            
        def _create_decorator_row(self):
            class EagerRowAdapter(object):
                def __init__(self, row):
                    self.row = row
                def has_key(self, key):
                    return map.has_key(key) or self.row.has_key(key)
                def __getitem__(self, key):
                    if map.has_key(key):
                        key = map[key]
                    return self.row[key]
                def keys(self):
                    return map.keys()
            map = {}        
            for c in self.eagertarget.c:
                parent = self.target.corresponding_column(c)
                map[parent] = c
                map[parent._label] = c
                map[parent.name] = c
            EagerRowAdapter.map = map
            return EagerRowAdapter

        def _decorate_row(self, row):
            # adapts a row at row iteration time to transparently
            # convert plain columns into the aliased columns that were actually
            # added to the column clause of the SELECT.
            return self._row_decorator(row)

    def init_class_attribute(self):
        self.parent_property._get_strategy(LazyLoader).init_class_attribute()
        
    def setup_query(self, context, eagertable=None, parentclauses=None, parentmapper=None, **kwargs):
        """Add a left outer join to the statement thats being constructed."""
        
        if parentmapper is None:
            localparent = context.mapper
        else:
            localparent = parentmapper
        
        if self.mapper in context.recursion_stack:
            return
        else:
            context.recursion_stack.add(self.parent)

        statement = context.statement
        
        if hasattr(statement, '_outerjoin'):
            towrap = statement._outerjoin
        elif isinstance(localparent.mapped_table, sql.Join):
            towrap = localparent.mapped_table
        else:
            # look for the mapper's selectable expressed within the current "from" criterion.
            # this will locate the selectable inside of any containers it may be a part of (such
            # as a join).  if its inside of a join, we want to outer join on that join, not the 
            # selectable.
            for fromclause in statement.froms:
                if fromclause is localparent.mapped_table:
                    towrap = fromclause
                    break
                elif isinstance(fromclause, sql.Join):
                    if localparent.mapped_table in sql_util.TableFinder(fromclause, include_aliases=True):
                        towrap = fromclause
                        break
            else:
                raise exceptions.InvalidRequestError("EagerLoader cannot locate a clause with which to outer join to, in query '%s' %s" % (str(statement), localparent.mapped_table))
        
        try:
            clauses = self.clauses[parentclauses]
        except KeyError:
            clauses = EagerLoader.AliasedClauses(self, parentclauses)
            self.clauses[parentclauses] = clauses
            
        if context.mapper not in self.clauses_by_lead_mapper:
            self.clauses_by_lead_mapper[context.mapper] = clauses

        if self.secondaryjoin is not None:
            statement._outerjoin = sql.outerjoin(towrap, clauses.eagersecondary, clauses.eagerprimary).outerjoin(clauses.eagertarget, clauses.eagersecondaryjoin)
            if self.order_by is False and self.secondary.default_order_by() is not None:
                statement.order_by(*clauses.eagersecondary.default_order_by())
        else:
            statement._outerjoin = towrap.outerjoin(clauses.eagertarget, clauses.eagerprimary)
            if self.order_by is False and clauses.eagertarget.default_order_by() is not None:
                statement.order_by(*clauses.eagertarget.default_order_by())

        if clauses.eager_order_by:
            statement.order_by(*util.to_list(clauses.eager_order_by))
                
        statement.append_from(statement._outerjoin)
        for value in self.select_mapper.props.values():
            value.setup(context, eagertable=clauses.eagertarget, parentclauses=clauses, parentmapper=self.select_mapper)

    def _create_row_processor(self, selectcontext, row):
        """Create a *row processing* function that will apply eager
        aliasing to the row.
        
        Also check that an identity key can be retrieved from the row,
        else return None.
        """
        
        # check for a user-defined decorator in the SelectContext (which was set up by the contains_eager() option)
        if selectcontext.attributes.has_key((EagerLoader, self.parent_property)):
            # custom row decoration function, placed in the selectcontext by the 
            # contains_eager() mapper option
            decorator = selectcontext.attributes[(EagerLoader, self.parent_property)]
            if decorator is None:
                decorator = lambda row: row
        else:
            try:
                # decorate the row according to the stored AliasedClauses for this eager load
                clauses = self.clauses_by_lead_mapper[selectcontext.mapper]
                decorator = clauses._row_decorator
            except KeyError, k:
                # no stored AliasedClauses: eager loading was not set up in the query and
                # AliasedClauses never got initialized
                return None

        try:
            decorated_row = decorator(row)
            # check for identity key
            identity_key = self.mapper.identity_key_from_row(decorated_row)
            # and its good
            return decorator
        except KeyError, k:
            # no identity key - dont return a row processor, will cause a degrade to lazy
            if self._should_log_debug:
                self.logger.debug("could not locate identity key from row '%s'; missing column '%s'" % (repr(decorated_row), str(k)))
            return None

    def process_row(self, selectcontext, instance, row, identitykey, isnew):
        """Receive a row.

        Tell our mapper to look for a new object instance in the row,
        and attach it to a list on the parent instance.
        """
        
        if self in selectcontext.recursion_stack:
            return
        
        try:
            # check for row processor
            row_processor = selectcontext.attributes[id(self)]
        except KeyError:
            # create a row processor function and cache it in the context
            row_processor = self._create_row_processor(selectcontext, row)
            selectcontext.attributes[id(self)] = row_processor
            
        if row_processor is not None:
            decorated_row = row_processor(row)
        else:
            # row_processor was None: degrade to a lazy loader
            if self._should_log_debug:
                self.logger.debug("degrade to lazy loader on %s" % mapperutil.attribute_str(instance, self.key))
            self.parent_property._get_strategy(LazyLoader).process_row(selectcontext, instance, row, identitykey, isnew)
            return
            
        # TODO: recursion check a speed hit...?  try to get a "termination point" into the AliasedClauses
        # or EagerRowAdapter ?
        selectcontext.recursion_stack.add(self)
        try:
            if not self.uselist:
                if self._should_log_debug:
                    self.logger.debug("eagerload scalar instance on %s" % mapperutil.attribute_str(instance, self.key))
                if isnew:
                    # set a scalar object instance directly on the parent object, 
                    # bypassing SmartProperty event handlers.
                    instance.__dict__[self.key] = self.mapper._instance(selectcontext, decorated_row, None)
                else:
                    # call _instance on the row, even though the object has been created,
                    # so that we further descend into properties
                    self.mapper._instance(selectcontext, decorated_row, None)
            else:
                if isnew:
                    if self._should_log_debug:
                        self.logger.debug("initialize UniqueAppender on %s" % mapperutil.attribute_str(instance, self.key))
                    # call the SmartProperty's initialize() method to create a new, blank list
                    l = getattr(instance.__class__, self.key).initialize(instance)
                
                    # create an appender object which will add set-like semantics to the list
                    appender = util.UniqueAppender(l.data)
                
                    # store it in the "scratch" area, which is local to this load operation.
                    selectcontext.attributes[(instance, self.key)] = appender
                result_list = selectcontext.attributes[(instance, self.key)]
                if self._should_log_debug:
                    self.logger.debug("eagerload list instance on %s" % mapperutil.attribute_str(instance, self.key))
                self.select_mapper._instance(selectcontext, decorated_row, result_list)
        finally:
            selectcontext.recursion_stack.remove(self)

EagerLoader.logger = logging.class_logger(EagerLoader)

class EagerLazyOption(StrategizedOption):
    def __init__(self, key, lazy=True):
        super(EagerLazyOption, self).__init__(key)
        self.lazy = lazy

    def process_query_property(self, context, prop):
        if self.lazy:
            if prop in context.eager_loaders:
                context.eager_loaders.remove(prop)
        else:
            context.eager_loaders.add(prop)
        super(EagerLazyOption, self).process_query_property(context, prop)

    def get_strategy_class(self):
        if self.lazy:
            return LazyLoader
        elif self.lazy is False:
            return EagerLoader
        elif self.lazy is None:
            return NoLoader

EagerLazyOption.logger = logging.class_logger(EagerLazyOption)

class RowDecorateOption(PropertyOption):
    def __init__(self, key, decorator=None, alias=None):
        super(RowDecorateOption, self).__init__(key)
        self.decorator = decorator
        self.alias = alias

    def process_selection_property(self, context, property):
        if self.alias is not None and self.decorator is None:
            if isinstance(self.alias, basestring):
                self.alias = property.target.alias(self.alias)
            def decorate(row):
                d = {}
                for c in property.target.columns:
                    d[c] = row[self.alias.corresponding_column(c)]
                return d
            self.decorator = decorate
        context.attributes[(EagerLoader, property)] = self.decorator

RowDecorateOption.logger = logging.class_logger(RowDecorateOption)
        

