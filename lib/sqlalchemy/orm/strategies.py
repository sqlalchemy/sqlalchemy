# strategies.py
# Copyright (C) 2005, 2006, 2007 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""sqlalchemy.orm.interfaces.LoaderStrategy implementations, and related MapperOptions."""

from sqlalchemy import sql, util, exceptions, logging
from sqlalchemy.sql import util as sql_util
from sqlalchemy.sql import visitors
from sqlalchemy.orm import mapper, attributes
from sqlalchemy.orm.interfaces import LoaderStrategy, StrategizedOption, MapperOption, PropertyOption
from sqlalchemy.orm import session as sessionlib
from sqlalchemy.orm import util as mapperutil


class ColumnLoader(LoaderStrategy):
    def init(self):
        super(ColumnLoader, self).init()
        self.columns = self.parent_property.columns
        self._should_log_debug = logging.is_debug_enabled(self.logger)
        self.is_composite = hasattr(self.parent_property, 'composite_class')
        
    def setup_query(self, context, parentclauses=None, **kwargs):
        for c in self.columns:
            if parentclauses is not None:
                context.statement.append_column(parentclauses.aliased_column(c))
            else:
                context.statement.append_column(c)
        
    def init_class_attribute(self):
        self.is_class_level = True
        if self.is_composite:
            self._init_composite_attribute()
        else:
            self._init_scalar_attribute()

    def _init_composite_attribute(self):
        self.logger.info("register managed composite attribute %s on class %s" % (self.key, self.parent.class_.__name__))
        def copy(obj):
            return self.parent_property.composite_class(
                *obj.__composite_values__())
        def compare(a, b):
            for col, aprop, bprop in zip(self.columns,
                                         a.__composite_values__(),
                                         b.__composite_values__()):
                if not col.type.compare_values(aprop, bprop):
                    return False
            else:
                return True
        sessionlib.attribute_manager.register_attribute(self.parent.class_, self.key, uselist=False, copy_function=copy, compare_function=compare, mutable_scalars=True, comparator=self.parent_property.comparator)

    def _init_scalar_attribute(self):
        self.logger.info("register managed attribute %s on class %s" % (self.key, self.parent.class_.__name__))
        coltype = self.columns[0].type
        sessionlib.attribute_manager.register_attribute(self.parent.class_, self.key, uselist=False, copy_function=coltype.copy_value, compare_function=coltype.compare_values, mutable_scalars=self.columns[0].type.is_mutable(), comparator=self.parent_property.comparator)
        
    def create_row_processor(self, selectcontext, mapper, row):
        if self.is_composite:
            for c in self.columns:
                if c not in row:
                    break
            else:
                def execute(instance, row, isnew, ispostselect=None, **flags):
                    if isnew or ispostselect:
                        if self._should_log_debug:
                            self.logger.debug("populating %s with %s/%s..." % (mapperutil.attribute_str(instance, self.key), row.__class__.__name__, self.columns[0].key))
                        instance.__dict__[self.key] = self.parent_property.composite_class(*[row[c] for c in self.columns])
                self.logger.debug("Returning active composite column fetcher for %s %s" % (mapper, self.key))
                return (execute, None)
                
        elif self.columns[0] in row:
            def execute(instance, row, isnew, ispostselect=None, **flags):
                if isnew or ispostselect:
                    if self._should_log_debug:
                        self.logger.debug("populating %s with %s/%s" % (mapperutil.attribute_str(instance, self.key), row.__class__.__name__, self.columns[0].key))
                    instance.__dict__[self.key] = row[self.columns[0]]
            self.logger.debug("Returning active column fetcher for %s %s" % (mapper, self.key))
            return (execute, None)

        # our mapped column is not present in the row.  check if we need to initialize a polymorphic
        # row fetcher used by inheritance.
        (hosted_mapper, needs_tables) = selectcontext.attributes.get(('polymorphic_fetch', mapper), (None, None))
        if hosted_mapper is None:
            return (None, None)
        
        if hosted_mapper.polymorphic_fetch == 'deferred':
            # 'deferred' polymorphic row fetcher, put a callable on the property.
            def execute(instance, row, isnew, **flags):
                if isnew:
                    sessionlib.attribute_manager.init_instance_attribute(instance, self.key, callable_=self._get_deferred_inheritance_loader(instance, mapper, needs_tables))
            self.logger.debug("Returning deferred column fetcher for %s %s" % (mapper, self.key))
            return (execute, None)
        else:  
            # immediate polymorphic row fetcher.  no processing needed for this row.
            self.logger.debug("Returning no column fetcher for %s %s" % (mapper, self.key))
            return (None, None)

    def _get_deferred_inheritance_loader(self, instance, mapper, needs_tables):
        def create_statement():
            cond, param_names = mapper._deferred_inheritance_condition(needs_tables)
            statement = sql.select(needs_tables, cond, use_labels=True)
            params = {}
            for c in param_names:
                params[c.name] = mapper.get_attr_by_column(instance, c)
            return (statement, params)
            
        strategy = self.parent_property._get_strategy(DeferredColumnLoader)

        props = [p for p in mapper.iterate_properties if isinstance(p.strategy, ColumnLoader) and p.columns[0].table in needs_tables]
        return strategy.setup_loader(instance, props=props, create_statement=create_statement)


ColumnLoader.logger = logging.class_logger(ColumnLoader)

class DeferredColumnLoader(LoaderStrategy):
    """Describes an object attribute that corresponds to a table
    column, which also will *lazy load* its value from the table.

    This is per-column lazy loading.
    """
    
    def create_row_processor(self, selectcontext, mapper, row):
        if self.group is not None and selectcontext.attributes.get(('undefer', self.group), False):
            return self.parent_property._get_strategy(ColumnLoader).create_row_processor(selectcontext, mapper, row)
        elif not self.is_class_level or len(selectcontext.options):
            def execute(instance, row, isnew, **flags):
                if isnew:
                    if self._should_log_debug:
                        self.logger.debug("set deferred callable on %s" % mapperutil.attribute_str(instance, self.key))
                    sessionlib.attribute_manager.init_instance_attribute(instance, self.key, callable_=self.setup_loader(instance))
            return (execute, None)
        else:
            def execute(instance, row, isnew, **flags):
                if isnew:
                    if self._should_log_debug:
                        self.logger.debug("set deferred callable on %s" % mapperutil.attribute_str(instance, self.key))
                    sessionlib.attribute_manager.reset_instance_attribute(instance, self.key)
            return (execute, None)

    def init(self):
        super(DeferredColumnLoader, self).init()
        if hasattr(self.parent_property, 'composite_class'):
            raise NotImplementedError("Deferred loading for composite types not implemented yet")
        self.columns = self.parent_property.columns
        self.group = self.parent_property.group
        self._should_log_debug = logging.is_debug_enabled(self.logger)

    def init_class_attribute(self):
        self.is_class_level = True
        self.logger.info("register managed attribute %s on class %s" % (self.key, self.parent.class_.__name__))
        sessionlib.attribute_manager.register_attribute(self.parent.class_, self.key, uselist=False, callable_=self.setup_loader, copy_function=self.columns[0].type.copy_value, compare_function=self.columns[0].type.compare_values, mutable_scalars=self.columns[0].type.is_mutable(), comparator=self.parent_property.comparator)

    def setup_query(self, context, **kwargs):
        if self.group is not None and context.attributes.get(('undefer', self.group), False):
            self.parent_property._get_strategy(ColumnLoader).setup_query(context, **kwargs)
        
    def setup_loader(self, instance, props=None, create_statement=None):
        localparent = mapper.object_mapper(instance, raiseerror=False)
        if localparent is None:
            return None

        # adjust for the ColumnProperty associated with the instance
        # not being our own ColumnProperty.  This can occur when entity_name
        # mappers are used to map different versions of the same ColumnProperty
        # to the class.
        prop = localparent.get_property(self.key)
        if prop is not self.parent_property:
            return prop._get_strategy(DeferredColumnLoader).setup_loader(instance)
            
        def lazyload():
            if not mapper.has_identity(instance):
                return None
            
            if props is not None:
                group = props
            elif self.group is not None:
                group = [p for p in localparent.iterate_properties if isinstance(p.strategy, DeferredColumnLoader) and p.group==self.group]
            else:
                group = [self.parent_property]
                
            if self._should_log_debug:
                self.logger.debug("deferred load %s group %s" % (mapperutil.attribute_str(instance, self.key), group and ','.join([p.key for p in group]) or 'None'))

            session = sessionlib.object_session(instance)
            if session is None:
                raise exceptions.InvalidRequestError("Parent instance %s is not bound to a Session; deferred load operation of attribute '%s' cannot proceed" % (instance.__class__, self.key))

            if create_statement is None:
                clause = localparent._get_clause
                ident = instance._instance_key[1]
                params = {}
                for i, primary_key in enumerate(localparent.primary_key):
                    params[primary_key._label] = ident[i]
                statement = sql.select([p.columns[0] for p in group], clause, from_obj=[localparent.mapped_table], use_labels=True)
            else:
                statement, params = create_statement()
                
            result = session.execute(statement, params, mapper=localparent)
            try:
                row = result.fetchone()
                for prop in group:
                    sessionlib.attribute_manager.get_attribute(instance, prop.key).set_committed_value(instance, row[prop.columns[0]])
                return attributes.ATTR_WAS_SET
            finally:
                result.close()

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

class UndeferGroupOption(MapperOption):
    def __init__(self, group):
        self.group = group
    def process_query_context(self, context):
        context.attributes[('undefer', self.group)] = True

    def process_selection_context(self, context):
        context.attributes[('undefer', self.group)] = True

class AbstractRelationLoader(LoaderStrategy):
    def init(self):
        super(AbstractRelationLoader, self).init()
        for attr in ['primaryjoin', 'secondaryjoin', 'secondary', 'foreign_keys', 'mapper', 'select_mapper', 'target', 'select_table', 'loads_polymorphic', 'uselist', 'cascade', 'attributeext', 'order_by', 'remote_side', 'polymorphic_primaryjoin', 'polymorphic_secondaryjoin', 'direction']:
            setattr(self, attr, getattr(self.parent_property, attr))
        self._should_log_debug = logging.is_debug_enabled(self.logger)
        
    def _init_instance_attribute(self, instance, callable_=None):
        return sessionlib.attribute_manager.init_instance_attribute(instance, self.key, callable_=callable_)
        
    def _register_attribute(self, class_, callable_=None, **kwargs):
        self.logger.info("register managed %s attribute %s on class %s" % ((self.uselist and "list-holding" or "scalar"), self.key, self.parent.class_.__name__))
        sessionlib.attribute_manager.register_attribute(class_, self.key, uselist = self.uselist, extension=self.attributeext, cascade=self.cascade,  trackparent=True, typecallable=self.parent_property.collection_class, callable_=callable_, comparator=self.parent_property.comparator, **kwargs)

class DynaLoader(AbstractRelationLoader):
    def init_class_attribute(self):
        self.is_class_level = True
        self._register_attribute(self.parent.class_, dynamic=True, target_mapper=self.parent_property.mapper)

    def create_row_processor(self, selectcontext, mapper, row):
        return (None, None)

DynaLoader.logger = logging.class_logger(DynaLoader)
        
class NoLoader(AbstractRelationLoader):
    def init_class_attribute(self):
        self.is_class_level = True
        self._register_attribute(self.parent.class_)

    def create_row_processor(self, selectcontext, mapper, row):
        def execute(instance, row, isnew, **flags):
            if isnew:
                if self._should_log_debug:
                    self.logger.debug("initializing blank scalar/collection on %s" % mapperutil.attribute_str(instance, self.key))
                self._init_instance_attribute(instance)
        return (execute, None)

NoLoader.logger = logging.class_logger(NoLoader)
        
class LazyLoader(AbstractRelationLoader):
    def init(self):
        super(LazyLoader, self).init()
        (self.lazywhere, self.lazybinds, self.lazyreverse) = self._create_lazy_clause(self)
        
        self.logger.info(str(self.parent_property) + " lazy loading clause " + str(self.lazywhere))

        # determine if our "lazywhere" clause is the same as the mapper's
        # get() clause.  then we can just use mapper.get()
        #from sqlalchemy.orm import query
        self.use_get = not self.uselist and self.mapper._get_clause.compare(self.lazywhere)
        if self.use_get:
            self.logger.info(str(self.parent_property) + " will use query.get() to optimize instance loads")

    def init_class_attribute(self):
        self.is_class_level = True
        self._register_attribute(self.parent.class_, callable_=lambda i: self.setup_loader(i))

    def lazy_clause(self, instance, reverse_direction=False):
        if not reverse_direction:
            (criterion, lazybinds, rev) = (self.lazywhere, self.lazybinds, self.lazyreverse)
        else:
            (criterion, lazybinds, rev) = LazyLoader._create_lazy_clause(self.parent_property, reverse_direction=reverse_direction)
        bind_to_col = dict([(lazybinds[col].key, col) for col in lazybinds])

        class Visitor(visitors.ClauseVisitor):
            def visit_bindparam(s, bindparam):
                mapper = reverse_direction and self.parent_property.mapper or self.parent_property.parent
                if bindparam.key in bind_to_col:
                    bindparam.value = mapper.get_attr_by_column(instance, bind_to_col[bindparam.key])
        return Visitor().traverse(criterion, clone=True)
    
    def setup_loader(self, instance, options=None):
        if not mapper.has_mapper(instance):
            return None
        else:
            # adjust for the PropertyLoader associated with the instance
            # not being our own PropertyLoader.  This can occur when entity_name
            # mappers are used to map different versions of the same PropertyLoader
            # to the class.
            prop = mapper.object_mapper(instance).get_property(self.key)
            if prop is not self.parent_property:
                return prop._get_strategy(LazyLoader).setup_loader(instance)

        def lazyload():
            self.logger.debug("lazy load attribute %s on instance %s" % (self.key, mapperutil.instance_str(instance)))

            if not mapper.has_identity(instance):
                return None

            session = sessionlib.object_session(instance)
            if session is None:
                try:
                    session = mapper.object_mapper(instance).get_session()
                except exceptions.InvalidRequestError:
                    raise exceptions.InvalidRequestError("Parent instance %s is not bound to a Session, and no contextual session is established; lazy load operation of attribute '%s' cannot proceed" % (instance.__class__, self.key))

            # if we have a simple straight-primary key load, use mapper.get()
            # to possibly save a DB round trip
            q = session.query(self.mapper).autoflush(False)
            if self.use_get:
                params = {}
                for col, bind in self.lazybinds.iteritems():
                    params[bind.key] = self.parent.get_attr_by_column(instance, col)
                ident = []
                for primary_key in self.select_mapper.primary_key: 
                    bind = self.lazyreverse[primary_key]
                    ident.append(params[bind.key])
                if options:
                    q = q.options(*options)
                return q.get(ident)
            elif self.order_by is not False:
                q = q.order_by(self.order_by)
            elif self.secondary is not None and self.secondary.default_order_by() is not None:
                q = q.order_by(self.secondary.default_order_by())

            if options:
                q = q.options(*options)
            q = q.filter(self.lazy_clause(instance))

            result = q.all()
            if self.uselist:
                return result
            else:
                if result:
                    return result[0]
                else:
                    return None

        return lazyload

    def create_row_processor(self, selectcontext, mapper, row):
        if not self.is_class_level or len(selectcontext.options):
            def execute(instance, row, isnew, **flags):
                if isnew:
                    if self._should_log_debug:
                        self.logger.debug("set instance-level lazy loader on %s" % mapperutil.attribute_str(instance, self.key))
                    # we are not the primary manager for this attribute on this class - set up a per-instance lazyloader,
                    # which will override the class-level behavior
                    self._init_instance_attribute(instance, callable_=self.setup_loader(instance, selectcontext.options))
            return (execute, None)
        else:
            def execute(instance, row, isnew, **flags):
                if isnew:
                    if self._should_log_debug:
                        self.logger.debug("set class-level lazy loader on %s" % mapperutil.attribute_str(instance, self.key))
                    # we are the primary manager for this attribute on this class - reset its per-instance attribute state, 
                    # so that the class-level lazy loader is executed when next referenced on this instance.
                    # this usually is not needed unless the constructor of the object referenced the attribute before we got 
                    # to load data into it.
                    sessionlib.attribute_manager.reset_instance_attribute(instance, self.key)
            return (execute, None)

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
            class FindColumnInColumnClause(visitors.ClauseVisitor):
                def visit_column(self, c):
                    columns.append(c)
            FindColumnInColumnClause().traverse(expr)
            return len(columns) and columns[0] or None
        
        def visit_binary(binary):
            leftcol = find_column_in_expr(binary.left)
            rightcol = find_column_in_expr(binary.right)
            if leftcol is None or rightcol is None:
                return
            
            if should_bind(leftcol, rightcol):
                col = leftcol
                binary.left = binds.setdefault(leftcol,
                        sql.bindparam(None, None, shortname=leftcol.name, type_=binary.right.type, unique=True))
                reverse[rightcol] = binds[col]

            # the "left is not right" compare is to handle part of a join clause that is "table.c.col1==table.c.col1",
            # which can happen in rare cases (test/orm/relationships.py RelationTest2)
            if leftcol is not rightcol and should_bind(rightcol, leftcol):
                col = rightcol
                binary.right = binds.setdefault(rightcol,
                        sql.bindparam(None, None, shortname=rightcol.name, type_=binary.left.type, unique=True))
                reverse[leftcol] = binds[col]

        lazywhere = primaryjoin
        li = mapperutil.BinaryVisitor(visit_binary)
        
        if not secondaryjoin or not reverse_direction:
            lazywhere = li.traverse(lazywhere, clone=True)
        
        if secondaryjoin is not None:
            if reverse_direction:
                secondaryjoin = li.traverse(secondaryjoin, clone=True)
            lazywhere = sql.and_(lazywhere, secondaryjoin)
        return (lazywhere, binds, reverse)
    _create_lazy_clause = classmethod(_create_lazy_clause)
    
LazyLoader.logger = logging.class_logger(LazyLoader)


class EagerLoader(AbstractRelationLoader):
    """Loads related objects inline with a parent query."""
    
    def init(self):
        super(EagerLoader, self).init()
        self.clauses = {}
        self.join_depth = self.parent_property.join_depth

    def init_class_attribute(self):
        # class-level eager strategy; add the PropertyLoader
        # to the parent's list of "eager loaders"; this tells the Query
        # that eager loaders will be used in a normal query
        self.parent._eager_loaders.add(self.parent_property)
        
        # initialize a lazy loader on the class level attribute
        self.parent_property._get_strategy(LazyLoader).init_class_attribute()
        
    def setup_query(self, context, parentclauses=None, parentmapper=None, **kwargs):
        """Add a left outer join to the statement thats being constructed."""
        
        # build a path as we setup the query.  the format of this path
        # matches that of interfaces.LoaderStack, and will be used in the 
        # row-loading phase to match up AliasedClause objects with the current
        # LoaderStack position.
        if parentclauses:
            path = parentclauses.path + (self.parent.base_mapper, self.key)
        else:
            path = (self.parent.base_mapper, self.key)
        
        if self.join_depth:
            if len(path) / 2 > self.join_depth:
                return
        else:
            if self.mapper in path:
                return
        
        #print "CREATING EAGER PATH FOR", "->".join([str(s) for s in path])
        
        if parentmapper is None:
            localparent = context.mapper
        else:
            localparent = parentmapper
        
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
            clauses = self.clauses[path]
        except KeyError:
            clauses = mapperutil.PropertyAliasedClauses(self.parent_property, self.parent_property.polymorphic_primaryjoin, self.parent_property.polymorphic_secondaryjoin, parentclauses)
            self.clauses[path] = clauses
        
        if self.secondaryjoin is not None:
            statement._outerjoin = sql.outerjoin(towrap, clauses.secondary, clauses.primaryjoin).outerjoin(clauses.alias, clauses.secondaryjoin)
            if self.order_by is False and self.secondary.default_order_by() is not None:
                statement.append_order_by(*clauses.secondary.default_order_by())
        else:
            statement._outerjoin = towrap.outerjoin(clauses.alias, clauses.primaryjoin)
            if self.order_by is False and clauses.alias.default_order_by() is not None:
                statement.append_order_by(*clauses.alias.default_order_by())

        if clauses.order_by:
            statement.append_order_by(*util.to_list(clauses.order_by))
        
        statement.append_from(statement._outerjoin)

        for value in self.select_mapper.iterate_properties:
            value.setup(context, parentclauses=clauses, parentmapper=self.select_mapper)
        
    def _create_row_decorator(self, selectcontext, row, path):
        """Create a *row decorating* function that will apply eager
        aliasing to the row.
        
        Also check that an identity key can be retrieved from the row,
        else return None.
        """
        
        #print "creating row decorator for path ", "->".join([str(s) for s in path])
        
        # check for a user-defined decorator in the SelectContext (which was set up by the contains_eager() option)
        if ("eager_row_processor", self.parent_property) in selectcontext.attributes:
            # custom row decoration function, placed in the selectcontext by the 
            # contains_eager() mapper option
            decorator = selectcontext.attributes[("eager_row_processor", self.parent_property)]
            if decorator is None:
                decorator = lambda row: row
        else:
            try:
                # decorate the row according to the stored AliasedClauses for this eager load
                clauses = self.clauses[path]
                decorator = clauses.row_decorator
            except KeyError, k:
                # no stored AliasedClauses: eager loading was not set up in the query and
                # AliasedClauses never got initialized
                if self._should_log_debug:
                    self.logger.debug("Could not locate aliased clauses for key: " + str(path))
                return None

        try:
            decorated_row = decorator(row)
            # check for identity key
            identity_key = self.select_mapper.identity_key_from_row(decorated_row)
            # and its good
            return decorator
        except KeyError, k:
            # no identity key - dont return a row processor, will cause a degrade to lazy
            if self._should_log_debug:
                self.logger.debug("could not locate identity key from row '%s'; missing column '%s'" % (repr(decorated_row), str(k)))
            return None

    def create_row_processor(self, selectcontext, mapper, row):
        selectcontext.stack.push_property(self.key)
        path = selectcontext.stack.snapshot()

        row_decorator = self._create_row_decorator(selectcontext, row, path)
        if row_decorator is not None:
            def execute(instance, row, isnew, **flags):
                decorated_row = row_decorator(row)

                selectcontext.stack.push_property(self.key)
                
                if not self.uselist:
                    if self._should_log_debug:
                        self.logger.debug("eagerload scalar instance on %s" % mapperutil.attribute_str(instance, self.key))
                    if isnew:
                        # set a scalar object instance directly on the
                        # parent object, bypassing InstrumentedAttribute
                        # event handlers.
                        #
                        # FIXME: instead of...
                        sessionlib.attribute_manager.get_attribute(instance, self.key).set_raw_value(instance, self.select_mapper._instance(selectcontext, decorated_row, None))
                        # bypass and set directly:
                        #instance.__dict__[self.key] = ...
                    else:
                        # call _instance on the row, even though the object has been created,
                        # so that we further descend into properties
                        self.select_mapper._instance(selectcontext, decorated_row, None)
                else:
                    if isnew:
                        if self._should_log_debug:
                            self.logger.debug("initialize UniqueAppender on %s" % mapperutil.attribute_str(instance, self.key))

                        collection = sessionlib.attribute_manager.init_collection(instance, self.key)
                        appender = util.UniqueAppender(collection, 'append_without_event')

                        # store it in the "scratch" area, which is local to this load operation.
                        selectcontext.attributes[(instance, self.key)] = appender
                    result_list = selectcontext.attributes[(instance, self.key)]
                    if self._should_log_debug:
                        self.logger.debug("eagerload list instance on %s" % mapperutil.attribute_str(instance, self.key))
                        
                    self.select_mapper._instance(selectcontext, decorated_row, result_list)
                selectcontext.stack.pop()

            selectcontext.stack.pop()
            return (execute, None)
        else:
            self.logger.debug("eager loader %s degrading to lazy loader" % str(self))
            selectcontext.stack.pop()
            return self.parent_property._get_strategy(LazyLoader).create_row_processor(selectcontext, mapper, row)
        
            
    def __str__(self):
        return str(self.parent) + "." + self.key
        
EagerLoader.logger = logging.class_logger(EagerLoader)

class EagerLazyOption(StrategizedOption):
    def __init__(self, key, lazy=True, chained=False):
        super(EagerLazyOption, self).__init__(key)
        self.lazy = lazy
        self.chained = chained
        
    def is_chained(self):
        return not self.lazy and self.chained
        
    def process_query_property(self, context, properties):
        if self.lazy:
            if properties[-1] in context.eager_loaders:
                context.eager_loaders.remove(properties[-1])
        else:
            for prop in properties:
                context.eager_loaders.add(prop)
        super(EagerLazyOption, self).process_query_property(context, properties)

    def get_strategy_class(self):
        if self.lazy:
            return LazyLoader
        elif self.lazy is False:
            return EagerLoader
        elif self.lazy is None:
            return NoLoader

EagerLazyOption.logger = logging.class_logger(EagerLazyOption)

# TODO: enable FetchMode option.  currently 
# this class does nothing.  will require Query
# to swich between using its "polymorphic" selectable
# and its regular selectable in order to make decisions
# (therefore might require that FetchModeOperation is performed
# only as the first operation on a Query.)
class FetchModeOption(PropertyOption):
    def __init__(self, key, type):
        super(FetchModeOption, self).__init__(key)
        if type not in ('join', 'select'):
            raise exceptions.ArgumentError("Fetchmode must be one of 'join' or 'select'")
        self.type = type
        
    def process_selection_property(self, context, properties):
        context.attributes[('fetchmode', properties[-1])] = self.type
        
class RowDecorateOption(PropertyOption):
    def __init__(self, key, decorator=None, alias=None):
        super(RowDecorateOption, self).__init__(key)
        self.decorator = decorator
        self.alias = alias

    def process_selection_property(self, context, properties):
        if self.alias is not None and self.decorator is None:
            if isinstance(self.alias, basestring):
                self.alias = properties[-1].target.alias(self.alias)
            def decorate(row):
                d = {}
                for c in properties[-1].target.columns:
                    d[c] = row[self.alias.corresponding_column(c)]
                return d
            self.decorator = decorate
        context.attributes[("eager_row_processor", properties[-1])] = self.decorator

RowDecorateOption.logger = logging.class_logger(RowDecorateOption)
        
