# properties.py
# Copyright (C) 2005, 2006, 2007 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Defines a set of mapper.MapperProperty objects, including basic
column properties as well as relationships.  The objects rely upon the
LoaderStrategy objects in the strategies.py module to handle load
operations.  PropertyLoader also relies upon the dependency.py module
to handle flush-time dependency sorting and processing.
"""

from sqlalchemy import sql, schema, util, exceptions, logging
from sqlalchemy.sql import util as sql_util
from sqlalchemy.orm import mapper, sync, strategies, attributes, dependency
from sqlalchemy.orm import session as sessionlib
from sqlalchemy.orm import util as mapperutil
import operator
from sqlalchemy.orm.interfaces import StrategizedProperty, PropComparator
from sqlalchemy.exceptions import ArgumentError

__all__ = ['ColumnProperty', 'CompositeProperty', 'PropertyLoader', 'BackRef']

class ColumnProperty(StrategizedProperty):
    """Describes an object attribute that corresponds to a table column."""

    def __init__(self, *columns, **kwargs):
        """The list of `columns` describes a single object
        property. If there are multiple tables joined together for the
        mapper, this list represents the equivalent column as it
        appears across each table.
        """

        self.columns = list(columns)
        self.group = kwargs.pop('group', None)
        self.deferred = kwargs.pop('deferred', False)
        self.comparator = ColumnProperty.ColumnComparator(self)
        # sanity check
        for col in columns:
            if not hasattr(col, 'name'):
                if hasattr(col, 'label'):
                    raise ArgumentError('ColumnProperties must be named for the mapper to work with them.  Try .label() to fix this')
                raise ArgumentError('%r is not a valid candidate for ColumnProperty' % col)
        
    def create_strategy(self):
        if self.deferred:
            return strategies.DeferredColumnLoader(self)
        else:
            return strategies.ColumnLoader(self)
    
    def copy(self):
        return ColumnProperty(deferred=self.deferred, group=self.group, *self.columns)
        
    def getattr(self, object, column):
        return getattr(object, self.key)

    def setattr(self, object, value, column):
        setattr(object, self.key, value)

    def get_history(self, obj, passive=False):
        return sessionlib.attribute_manager.get_history(obj, self.key, passive=passive)

    def merge(self, session, source, dest, dont_load, _recursive):
        setattr(dest, self.key, getattr(source, self.key, None))

    def get_col_value(self, column, value):
        return value

    class ColumnComparator(PropComparator):
        def clause_element(self):
            return self.prop.columns[0]
            
        def operate(self, op, *other):
            return op(self.prop.columns[0], *other)

        def reverse_operate(self, op, other):
            col = self.prop.columns[0]
            return op(col._bind_param(other), col)

            
ColumnProperty.logger = logging.class_logger(ColumnProperty)


class CompositeProperty(ColumnProperty):
    """subclasses ColumnProperty to provide composite type support."""
    
    def __init__(self, class_, *columns, **kwargs):
        super(CompositeProperty, self).__init__(*columns, **kwargs)
        self.composite_class = class_
        self.comparator = kwargs.pop('comparator', CompositeProperty.Comparator)(self)
        
    def copy(self):
        return CompositeProperty(deferred=self.deferred, group=self.group, composite_class=self.composite_class, *self.columns)

    def getattr(self, object, column):
        obj = getattr(object, self.key)
        return self.get_col_value(column, obj)

    def setattr(self, object, value, column):
        obj = getattr(object, self.key, None)
        if obj is None:
            obj = self.composite_class(*[None for c in self.columns])
        for a, b in zip(self.columns, value.__composite_values__()):
            if a is column:
                setattr(obj, b, value)

    def get_col_value(self, column, value):
        for a, b in zip(self.columns, value.__composite_values__()):
            if a is column:
                return b

    class Comparator(PropComparator):
        def __eq__(self, other):
            if other is None:
                return sql.and_(*[a==None for a in self.prop.columns])
            else:
                return sql.and_(*[a==b for a, b in
                                  zip(self.prop.columns,
                                      other.__composite_values__())])

        def __ne__(self, other):
            return sql.or_(*[a!=b for a, b in
                             zip(self.prop.columns,
                                 other.__composite_values__())])

class PropertyLoader(StrategizedProperty):
    """Describes an object property that holds a single item or list
    of items that correspond to a related database table.
    """

    def __init__(self, argument, secondary=None, primaryjoin=None, secondaryjoin=None, entity_name=None, foreign_keys=None, foreignkey=None, uselist=None, private=False, association=None, order_by=False, attributeext=None, backref=None, is_backref=False, post_update=False, cascade=None, viewonly=False, lazy=True, collection_class=None, passive_deletes=False, remote_side=None, enable_typechecks=True, join_depth=None, strategy_class=None):
        self.uselist = uselist
        self.argument = argument
        self.entity_name = entity_name
        self.secondary = secondary
        self.primaryjoin = primaryjoin
        self.secondaryjoin = secondaryjoin
        self.post_update = post_update
        self.direction = None
        self.viewonly = viewonly
        self.lazy = lazy
        self.foreign_keys = util.to_set(foreign_keys)
        self._legacy_foreignkey = util.to_set(foreignkey)
        if foreignkey:
            util.warn_deprecated('foreignkey option is deprecated; see docs for details')
        self.collection_class = collection_class
        self.passive_deletes = passive_deletes
        self.remote_side = util.to_set(remote_side)
        self.enable_typechecks = enable_typechecks
        self._parent_join_cache = {}
        self.comparator = PropertyLoader.Comparator(self)
        self.join_depth = join_depth
        self.strategy_class = strategy_class

        if cascade is not None:
            self.cascade = mapperutil.CascadeOptions(cascade)
        else:
            if private:
                util.warn_deprecated('private option is deprecated; see docs for details')
                self.cascade = mapperutil.CascadeOptions("all, delete-orphan")
            else:
                self.cascade = mapperutil.CascadeOptions("save-update, merge")
        
        if self.passive_deletes == 'all' and ("delete" in self.cascade or "delete-orphan" in self.cascade):
            raise exceptions.ArgumentError("Can't set passive_deletes='all' in conjunction with 'delete' or 'delete-orphan' cascade")

        self.association = association
        if association:
            util.warn_deprecated('association option is deprecated; see docs for details')
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

    class Comparator(PropComparator):
        def __eq__(self, other):
            if other is None:
                return ~sql.exists([1], self.prop.primaryjoin)
            elif self.prop.uselist:
                if not hasattr(other, '__iter__'):
                    raise exceptions.InvalidRequestError("Can only compare a collection to an iterable object.  Use contains().")
                else:
                    j = self.prop.primaryjoin
                    if self.prop.secondaryjoin:
                        j = j & self.prop.secondaryjoin
                    clauses = []
                    for o in other:
                        clauses.append(
                            sql.exists([1], j & sql.and_(*[x==y for (x, y) in zip(self.prop.mapper.primary_key, self.prop.mapper.primary_key_from_instance(o))]))
                        )
                    return sql.and_(*clauses)
            else:  
                return self.prop._optimized_compare(other)
        
        def any(self, criterion=None, **kwargs):
            if not self.prop.uselist:
                raise exceptions.InvalidRequestError("'any()' not implemented for scalar attributes. Use has().")
            j = self.prop.primaryjoin
            if self.prop.secondaryjoin:
                j = j & self.prop.secondaryjoin
            for k in kwargs:
                crit = (getattr(self.prop.mapper.class_, k) == kwargs[k])
                if criterion is None:
                    criterion = crit
                else:
                    criterion = criterion & crit
            return sql.exists([1], j & criterion)
        
        def has(self, criterion=None, **kwargs):
            if self.prop.uselist:
                raise exceptions.InvalidRequestError("'has()' not implemented for collections.  Use any().")
            j = self.prop.primaryjoin
            if self.prop.secondaryjoin:
                j = j & self.prop.secondaryjoin
            for k in kwargs:
                crit = (getattr(self.prop.mapper.class_, k) == kwargs[k])
                if criterion is None:
                    criterion = crit
                else:
                    criterion = criterion & crit
            return sql.exists([1], j & criterion)
                
        def contains(self, other):
            if not self.prop.uselist:
                raise exceptions.InvalidRequestError("'contains' not implemented for scalar attributes.  Use ==")
            clause = self.prop._optimized_compare(other)

            if self.prop.secondaryjoin:
                j = self.prop.primaryjoin
                j = j & self.prop.secondaryjoin
                clause.negation_clause = ~sql.exists([1], j & sql.and_(*[x==y for (x, y) in zip(self.prop.mapper.primary_key, self.prop.mapper.primary_key_from_instance(other))]))

            return clause

        def __ne__(self, other):
            if self.prop.uselist and not hasattr(other, '__iter__'):
                raise exceptions.InvalidRequestError("Can only compare a collection to an iterable object")
                
            j = self.prop.primaryjoin
            if self.prop.secondaryjoin:
                j = j & self.prop.secondaryjoin
            return ~sql.exists([1], j & sql.and_(*[x==y for (x, y) in zip(self.prop.mapper.primary_key, self.prop.mapper.primary_key_from_instance(other))]))
            
    def compare(self, op, value, value_is_parent=False):
        if op == operator.eq:
            if value is None:
                return ~sql.exists([1], self.prop.mapper.mapped_table, self.prop.primaryjoin)
            else:
                return self._optimized_compare(value, value_is_parent=value_is_parent)
        else:
            return op(self.comparator, value)
    
    def _optimized_compare(self, value, value_is_parent=False):
        return self._get_strategy(strategies.LazyLoader).lazy_clause(value, reverse_direction=not value_is_parent)
        
    private = property(lambda s:s.cascade.delete_orphan)

    def create_strategy(self):
        if self.strategy_class:
            return self.strategy_class(self)
        elif self.lazy == 'dynamic':
            return strategies.DynaLoader(self)
        elif self.lazy:
            return strategies.LazyLoader(self)
        elif self.lazy is False:
            return strategies.EagerLoader(self)
        elif self.lazy is None:
            return strategies.NoLoader(self)

    def __str__(self):
        return str(self.parent.class_.__name__) + "." + self.key + " (" + str(self.mapper.class_.__name__)  + ")"

    def merge(self, session, source, dest, dont_load, _recursive):
        if not "merge" in self.cascade or self.mapper in _recursive:
            return
        childlist = sessionlib.attribute_manager.get_history(source, self.key, passive=True)
        if childlist is None:
            return
        if self.uselist:
            # sets a blank collection according to the correct list class
            dest_list = sessionlib.attribute_manager.init_collection(dest, self.key)
            for current in list(childlist):
                obj = session.merge(current, entity_name=self.mapper.entity_name, dont_load=dont_load, _recursive=_recursive)
                if obj is not None:
                    dest_list.append_with_event(obj)
        else:
            current = list(childlist)[0]
            if current is not None:
                obj = session.merge(current, entity_name=self.mapper.entity_name, dont_load=dont_load, _recursive=_recursive)
                if obj is not None:
                    setattr(dest, self.key, obj)


    def cascade_iterator(self, type, object, recursive, halt_on=None):
        if not type in self.cascade:
            return
        passive = type != 'delete' or self.passive_deletes
        mapper = self.mapper.primary_mapper()
        for c in sessionlib.attribute_manager.get_as_list(object, self.key, passive=passive):
            if c is not None and c not in recursive and (halt_on is None or not halt_on(c)):
                if not isinstance(c, self.mapper.class_):
                    raise exceptions.AssertionError("Attribute '%s' on class '%s' doesn't handle objects of type '%s'" % (self.key, str(self.parent.class_), str(c.__class__)))
                recursive.add(c)
                yield c
                for c2 in mapper.cascade_iterator(type, c, recursive):
                    yield c2

    def cascade_callable(self, type, object, callable_, recursive, halt_on=None):
        if not type in self.cascade:
            return

        mapper = self.mapper.primary_mapper()
        passive = type != 'delete' or self.passive_deletes
        for c in sessionlib.attribute_manager.get_as_list(object, self.key, passive=passive):
            if c is not None and c not in recursive and (halt_on is None or not halt_on(c)):
                if not isinstance(c, self.mapper.class_):
                    raise exceptions.AssertionError("Attribute '%s' on class '%s' doesn't handle objects of type '%s'" % (self.key, str(self.parent.class_), str(c.__class__)))
                recursive.add(c)
                callable_(c, mapper.entity_name)
                mapper.cascade_callable(type, c, callable_, recursive)

    def _get_target_class(self):
        """Return the target class of the relation, even if the
        property has not been initialized yet.
        """

        if isinstance(self.argument, type):
            return self.argument
        else:
            return self.argument.class_

    def do_init(self):
        self._determine_targets()
        self._determine_joins()
        self._determine_fks()
        self._determine_direction()
        self._determine_remote_side()
        self._create_polymorphic_joins()
        self._post_init()

    def _determine_targets(self):
        if isinstance(self.argument, type):
            self.mapper = mapper.class_mapper(self.argument, entity_name=self.entity_name, compile=False)._check_compile()
        elif isinstance(self.argument, mapper.Mapper):
            self.mapper = self.argument._check_compile()
        else:
            raise exceptions.ArgumentError("relation '%s' expects a class or a mapper argument (received: %s)" % (self.key, type(self.argument)))

        # ensure the "select_mapper", if different from the regular target mapper, is compiled.
        self.mapper.get_select_mapper()._check_compile()

        if self.association is not None:
            if isinstance(self.association, type):
                self.association = mapper.class_mapper(self.association, entity_name=self.entity_name, compile=False)._check_compile()

        self.target = self.mapper.mapped_table
        self.select_mapper = self.mapper.get_select_mapper()
        self.select_table = self.mapper.select_table
        self.loads_polymorphic = self.target is not self.select_table

        if self.cascade.delete_orphan:
            if self.parent.class_ is self.mapper.class_:
                raise exceptions.ArgumentError("In relationship '%s', can't establish 'delete-orphan' cascade rule on a self-referential relationship.  You probably want cascade='all', which includes delete cascading but not orphan detection." %(str(self)))
            self.mapper.primary_mapper().delete_orphans.append((self.key, self.parent.class_))

    def _determine_joins(self):
        if self.secondaryjoin is not None and self.secondary is None:
            raise exceptions.ArgumentError("Property '" + self.key + "' specified with secondary join condition but no secondary argument")
        # if join conditions were not specified, figure them out based on foreign keys
        
        def _search_for_join(mapper, table):
            """find a join between the given mapper's mapped table and the given table.
            will try the mapper's local table first for more specificity, then if not 
            found will try the more general mapped table, which in the case of inheritance
            is a join."""
            try:
                return sql.join(mapper.local_table, table)
            except exceptions.ArgumentError, e:
                return sql.join(mapper.mapped_table, table)
        
        try:
            if self.secondary is not None:
                if self.secondaryjoin is None:
                    self.secondaryjoin = _search_for_join(self.mapper, self.secondary).onclause
                if self.primaryjoin is None:
                    self.primaryjoin = _search_for_join(self.parent, self.secondary).onclause
            else:
                if self.primaryjoin is None:
                    self.primaryjoin = _search_for_join(self.parent, self.target).onclause
        except exceptions.ArgumentError, e:
            raise exceptions.ArgumentError("""Error determining primary and/or secondary join for relationship '%s'. If the underlying error cannot be corrected, you should specify the 'primaryjoin' (and 'secondaryjoin', if there is an association table present) keyword arguments to the relation() function (or for backrefs, by specifying the backref using the backref() function with keyword arguments) to explicitly specify the join conditions. Nested error is \"%s\"""" % (str(self), str(e)))

        # if using polymorphic mapping, the join conditions must be agasint the base tables of the mappers,
        # as the loader strategies expect to be working with those now (they will adapt the join conditions
        # to the "polymorphic" selectable as needed).  since this is an API change, put an explicit check/
        # error message in case its the "old" way.
        if self.loads_polymorphic:
            vis = sql_util.ColumnsInClause(self.mapper.select_table)
            vis.traverse(self.primaryjoin)
            if self.secondaryjoin:
                vis.traverse(self.secondaryjoin)
            if vis.result:
                raise exceptions.ArgumentError("In relationship '%s', primary and secondary join conditions must not include columns from the polymorphic 'select_table' argument as of SA release 0.3.4.  Construct join conditions using the base tables of the related mappers." % (str(self)))

    def _determine_fks(self):
        if self._legacy_foreignkey and not self._is_self_referential():
            self.foreign_keys = self._legacy_foreignkey

        def col_is_part_of_mappings(col):
            if self.secondary is None:
                return self.parent.mapped_table.corresponding_column(col, raiseerr=False) is not None or \
                    self.target.corresponding_column(col, raiseerr=False) is not None
            else:
                return self.parent.mapped_table.corresponding_column(col, raiseerr=False) is not None or \
                    self.target.corresponding_column(col, raiseerr=False) is not None or \
                    self.secondary.corresponding_column(col, raiseerr=False) is not None

        if self.foreign_keys:
            self._opposite_side = util.Set()
            def visit_binary(binary):
                if binary.operator != operator.eq or not isinstance(binary.left, schema.Column) or not isinstance(binary.right, schema.Column):
                    return
                if binary.left in self.foreign_keys:
                    self._opposite_side.add(binary.right)
                if binary.right in self.foreign_keys:
                    self._opposite_side.add(binary.left)
            mapperutil.BinaryVisitor(visit_binary).traverse(self.primaryjoin)
            if self.secondaryjoin is not None:
                mapperutil.BinaryVisitor(visit_binary).traverse(self.secondaryjoin)
        else:
            self.foreign_keys = util.Set()
            self._opposite_side = util.Set()
            def visit_binary(binary):
                if binary.operator != operator.eq or not isinstance(binary.left, schema.Column) or not isinstance(binary.right, schema.Column):
                    return

                # this check is for when the user put the "view_only" flag on and has tables that have nothing
                # to do with the relationship's parent/child mappings in the join conditions.  we dont want cols
                # or clauses related to those external tables dealt with.  see orm.relationships.ViewOnlyTest
                if not col_is_part_of_mappings(binary.left) or not col_is_part_of_mappings(binary.right):
                    return
                        
                for f in binary.left.foreign_keys:
                    if f.references(binary.right.table):
                        self.foreign_keys.add(binary.left)
                        self._opposite_side.add(binary.right)
                for f in binary.right.foreign_keys:
                    if f.references(binary.left.table):
                        self.foreign_keys.add(binary.right)
                        self._opposite_side.add(binary.left)
            mapperutil.BinaryVisitor(visit_binary).traverse(self.primaryjoin)

            if len(self.foreign_keys) == 0:
                raise exceptions.ArgumentError(
                    "Can't locate any foreign key columns in primary join "
                    "condition '%s' for relationship '%s'.  Specify "
                    "'foreign_keys' argument to indicate which columns in "
                    "the join condition are foreign." %(str(self.primaryjoin), str(self)))
            if self.secondaryjoin is not None:
                mapperutil.BinaryVisitor(visit_binary).traverse(self.secondaryjoin)


    def _determine_direction(self):
        """Determine our *direction*, i.e. do we represent one to
        many, many to many, etc.
        """

        if self.secondaryjoin is not None:
            self.direction = sync.MANYTOMANY
        elif self._is_self_referential():
            # for a self referential mapper, if the "foreignkey" is a single or composite primary key,
            # then we are "many to one", since the remote site of the relationship identifies a singular entity.
            # otherwise we are "one to many".
            if self._legacy_foreignkey:
                for f in self._legacy_foreignkey:
                    if not f.primary_key:
                        self.direction = sync.ONETOMANY
                    else:
                        self.direction = sync.MANYTOONE

            elif self.remote_side:
                for f in self.foreign_keys:
                    if f in self.remote_side:
                        self.direction = sync.ONETOMANY
                        return
                else:
                    self.direction = sync.MANYTOONE
            else:
                self.direction = sync.ONETOMANY
        else:
            for mappedtable, parenttable in [(self.mapper.mapped_table, self.parent.mapped_table), (self.mapper.local_table, self.parent.local_table)]:
                onetomany = len([c for c in self.foreign_keys if mappedtable.c.contains_column(c)])
                manytoone = len([c for c in self.foreign_keys if parenttable.c.contains_column(c)])

                if not onetomany and not manytoone:
                    raise exceptions.ArgumentError(
                        "Can't determine relation direction for relationship '%s' "
                        "- foreign key columns are present in neither the "
                        "parent nor the child's mapped tables" %(str(self)))
                elif onetomany and manytoone:
                    continue
                elif onetomany:
                    self.direction = sync.ONETOMANY
                    break
                elif manytoone:
                    self.direction = sync.MANYTOONE
                    break
            else:
                raise exceptions.ArgumentError(
                    "Can't determine relation direction for relationship '%s' "
                    "- foreign key columns are present in both the parent and "
                    "the child's mapped tables.  Specify 'foreign_keys' "
                    "argument." % (str(self)))

    def _determine_remote_side(self):
        if not self.remote_side:
            if self.direction is sync.MANYTOONE:
                self.remote_side = util.Set(self._opposite_side)
            elif self.direction is sync.ONETOMANY or self.direction is sync.MANYTOMANY:
                self.remote_side = util.Set(self.foreign_keys)

        self.local_side = util.Set(self._opposite_side).union(util.Set(self.foreign_keys)).difference(self.remote_side)

    def _create_polymorphic_joins(self):
        # get ready to create "polymorphic" primary/secondary join clauses.
        # these clauses represent the same join between parent/child tables that the primary
        # and secondary join clauses represent, except they reference ColumnElements that are specifically
        # in the "polymorphic" selectables.  these are used to construct joins for both Query as well as
        # eager loading, and also are used to calculate "lazy loading" clauses.

        # as we will be using the polymorphic selectables (i.e. select_table argument to Mapper) to figure this out,
        # first create maps of all the "equivalent" columns, since polymorphic selectables will often munge
        # several "equivalent" columns (such as parent/child fk cols) into just one column.

        target_equivalents = self.mapper._get_equivalent_columns()
            
        # if the target mapper loads polymorphically, adapt the clauses to the target's selectable
        if self.loads_polymorphic:
            if self.secondaryjoin:
                self.polymorphic_secondaryjoin = sql_util.ClauseAdapter(self.mapper.select_table).traverse(self.secondaryjoin, clone=True)
                self.polymorphic_primaryjoin = self.primaryjoin
            else:
                if self.direction is sync.ONETOMANY:
                    self.polymorphic_primaryjoin = sql_util.ClauseAdapter(self.mapper.select_table, include=self.foreign_keys, equivalents=target_equivalents).traverse(self.primaryjoin, clone=True)
                elif self.direction is sync.MANYTOONE:
                    self.polymorphic_primaryjoin = sql_util.ClauseAdapter(self.mapper.select_table, exclude=self.foreign_keys, equivalents=target_equivalents).traverse(self.primaryjoin, clone=True)
                self.polymorphic_secondaryjoin = None
            # load "polymorphic" versions of the columns present in "remote_side" - this is
            # important for lazy-clause generation which goes off the polymorphic target selectable
            for c in list(self.remote_side):
                if self.secondary and self.secondary.columns.contains_column(c):
                    continue
                for equiv in [c] + (c in target_equivalents and list(target_equivalents[c]) or []): 
                    corr = self.mapper.select_table.corresponding_column(equiv, raiseerr=False)
                    if corr:
                        self.remote_side.add(corr)
                        break
                else:
                    raise exceptions.AssertionError(str(self) + ": Could not find corresponding column for " + str(c) + " in selectable "  + str(self.mapper.select_table))
        else:
            self.polymorphic_primaryjoin = self.primaryjoin
            self.polymorphic_secondaryjoin = self.secondaryjoin

    def _post_init(self):
        if logging.is_info_enabled(self.logger):
            self.logger.info(str(self) + " setup primary join " + str(self.primaryjoin))
            self.logger.info(str(self) + " setup polymorphic primary join " + str(self.polymorphic_primaryjoin))
            self.logger.info(str(self) + " setup secondary join " + str(self.secondaryjoin))
            self.logger.info(str(self) + " setup polymorphic secondary join " + str(self.polymorphic_secondaryjoin))
            self.logger.info(str(self) + " foreign keys " + str([str(c) for c in self.foreign_keys]))
            self.logger.info(str(self) + " remote columns " + str([str(c) for c in self.remote_side]))
            self.logger.info(str(self) + " relation direction " + (self.direction is sync.ONETOMANY and "one-to-many" or (self.direction is sync.MANYTOONE and "many-to-one" or "many-to-many")))

        if self.uselist is None and self.direction is sync.MANYTOONE:
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
        elif not mapper.class_mapper(self.parent.class_).get_property(self.key, raiseerr=False):
            raise exceptions.ArgumentError("Attempting to assign a new relation '%s' to a non-primary mapper on class '%s'.  New relations can only be added to the primary mapper, i.e. the very first mapper created for class '%s' " % (self.key, self.parent.class_.__name__, self.parent.class_.__name__))

        super(PropertyLoader, self).do_init()

    def _is_self_referential(self):
        return self.parent.mapped_table is self.target or self.parent.select_table is self.target

    def get_join(self, parent, primary=True, secondary=True, polymorphic_parent=True):
        """return a join condition from the given parent mapper to this PropertyLoader's mapper.
        
           The resulting ClauseElement object is cached and should not be modified directly.
        
            parent
              a mapper which has a relation() to this PropertyLoader.  A PropertyLoader can 
              have multiple "parents" when its actual parent mapper has inheriting mappers.
              
            primary
              include the primary join condition in the resulting join.
              
            secondary
              include the secondary join condition in the resulting join.  If both primary
              and secondary are returned, they are joined via AND.
              
            polymorphic_parent
              if True, use the parent's 'select_table' instead of its 'mapped_table' to produce the join.
        """
        
        try:
            return self._parent_join_cache[(parent, primary, secondary, polymorphic_parent)]
        except KeyError:
            parent_equivalents = parent._get_equivalent_columns()
            secondaryjoin = self.polymorphic_secondaryjoin
            if polymorphic_parent:
                # adapt the "parent" side of our join condition to the "polymorphic" select of the parent
                if self.direction is sync.ONETOMANY:
                    primaryjoin = sql_util.ClauseAdapter(parent.select_table, exclude=self.foreign_keys, equivalents=parent_equivalents).traverse(self.polymorphic_primaryjoin, clone=True)
                elif self.direction is sync.MANYTOONE:
                    primaryjoin = sql_util.ClauseAdapter(parent.select_table, include=self.foreign_keys, equivalents=parent_equivalents).traverse(self.polymorphic_primaryjoin, clone=True)
                elif self.secondaryjoin:
                    primaryjoin = sql_util.ClauseAdapter(parent.select_table, exclude=self.foreign_keys, equivalents=parent_equivalents).traverse(self.polymorphic_primaryjoin, clone=True)

            if secondaryjoin is not None:
                if secondary and not primary:
                    j = secondaryjoin
                elif primary and secondary:
                    j = primaryjoin & secondaryjoin
                elif primary and not secondary:
                    j = primaryjoin
            else:
                j = primaryjoin
            self._parent_join_cache[(parent, primary, secondary, polymorphic_parent)] = j
            return j

    def register_dependencies(self, uowcommit):
        if not self.viewonly:
            self._dependency_processor.register_dependencies(uowcommit)

PropertyLoader.logger = logging.class_logger(PropertyLoader)

class BackRef(object):
    """Attached to a PropertyLoader to indicate a complementary reverse relationship.
    
    Can optionally create the complementing PropertyLoader if one does not exist already."""

    def __init__(self, key, _prop=None, **kwargs):
        self.key = key
        self.kwargs = kwargs
        self.prop = _prop

    def compile(self, prop):
        if self.prop:
            return
        
        self.prop = prop
        
        mapper = prop.mapper.primary_mapper()
        if mapper.get_property(self.key, raiseerr=False) is None:
            pj = self.kwargs.pop('primaryjoin', None)
            sj = self.kwargs.pop('secondaryjoin', None)

            parent = prop.parent.primary_mapper()
            self.kwargs.setdefault('viewonly', prop.viewonly)
            self.kwargs.setdefault('post_update', prop.post_update)
                
            relation = PropertyLoader(parent, prop.secondary, pj, sj,
                                      backref=BackRef(prop.key, _prop=prop), 
                                      is_backref=True,
                                      **self.kwargs)
                                      
            mapper._compile_property(self.key, relation);

            prop.reverse_property = mapper.get_property(self.key)
            mapper.get_property(self.key).reverse_property = prop

        else:
            raise exceptions.ArgumentError("Error creating backref '%s' on relation '%s': property of that name exists on mapper '%s'" % (self.key, prop, mapper))

    def get_extension(self):
        """Return an attribute extension to use with this backreference."""

        return attributes.GenericBackrefExtension(self.key)

mapper.ColumnProperty = ColumnProperty
        
