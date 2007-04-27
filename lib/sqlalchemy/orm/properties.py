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

from sqlalchemy import sql, schema, util, exceptions, sql_util, logging
from sqlalchemy.orm import mapper, sync, strategies, attributes, dependency
from sqlalchemy.orm import session as sessionlib
from sqlalchemy.orm import util as mapperutil
import sets, random
from sqlalchemy.orm.interfaces import *

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

    def create_strategy(self):
        if self.deferred:
            return strategies.DeferredColumnLoader(self)
        else:
            return strategies.ColumnLoader(self)

    def getattr(self, object):
        return getattr(object, self.key)

    def setattr(self, object, value):
        setattr(object, self.key, value)

    def get_history(self, obj, passive=False):
        return sessionlib.attribute_manager.get_history(obj, self.key, passive=passive)

    def merge(self, session, source, dest, _recursive):
        setattr(dest, self.key, getattr(source, self.key, None))

    def compare(self, value):
        return self.columns[0] == value

ColumnProperty.logger = logging.class_logger(ColumnProperty)

mapper.ColumnProperty = ColumnProperty

class PropertyLoader(StrategizedProperty):
    """Describes an object property that holds a single item or list
    of items that correspond to a related database table.
    """

    def __init__(self, argument, secondary, primaryjoin, secondaryjoin, entity_name=None, foreign_keys=None, foreignkey=None, uselist=None, private=False, association=None, order_by=False, attributeext=None, backref=None, is_backref=False, post_update=False, cascade=None, viewonly=False, lazy=True, collection_class=None, passive_deletes=False, remote_side=None, enable_typechecks=True):
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
        self.collection_class = collection_class
        self.passive_deletes = passive_deletes
        self.remote_side = util.to_set(remote_side)
        self.enable_typechecks = enable_typechecks
        self._parent_join_cache = {}

        if cascade is not None:
            self.cascade = mapperutil.CascadeOptions(cascade)
        else:
            if private:
                self.cascade = mapperutil.CascadeOptions("all, delete-orphan")
            else:
                self.cascade = mapperutil.CascadeOptions("save-update, merge")

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

    def compare(self, value):
        return sql.and_(*[x==y for (x, y) in zip(self.mapper.primary_key, self.mapper.primary_key_from_instance(value))])

    private = property(lambda s:s.cascade.delete_orphan)

    def create_strategy(self):
        if self.lazy:
            return strategies.LazyLoader(self)
        elif self.lazy is False:
            return strategies.EagerLoader(self)
        elif self.lazy is None:
            return strategies.NoLoader(self)

    def __str__(self):
        return str(self.parent.class_.__name__) + "." + self.key + " (" + str(self.mapper.class_.__name__)  + ")"

    def merge(self, session, source, dest, _recursive):
        if not "merge" in self.cascade or self.mapper in _recursive:
            return
        childlist = sessionlib.attribute_manager.get_history(source, self.key, passive=True)
        if childlist is None:
            return
        if self.uselist:
            # sets a blank list according to the correct list class
            dest_list = getattr(self.parent.class_, self.key).initialize(dest)
            for current in list(childlist):
                obj = session.merge(current, entity_name=self.mapper.entity_name, _recursive=_recursive)
                if obj is not None:
                    dest_list.append(obj)
        else:
            current = list(childlist)[0]
            if current is not None:
                obj = session.merge(current, entity_name=self.mapper.entity_name, _recursive=_recursive)
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
        if len(self._legacy_foreignkey) and not self._is_self_referential():
            self.foreign_keys = self._legacy_foreignkey

        def col_is_part_of_mappings(col):
            if self.secondary is None:
                return self.parent.mapped_table.corresponding_column(col, raiseerr=False) is not None or \
                    self.target.corresponding_column(col, raiseerr=False) is not None
            else:
                return self.parent.mapped_table.corresponding_column(col, raiseerr=False) is not None or \
                    self.target.corresponding_column(col, raiseerr=False) is not None or \
                    self.secondary.corresponding_column(col, raiseerr=False) is not None

        if len(self.foreign_keys):
            self._opposite_side = util.Set()
            def visit_binary(binary):
                if binary.operator != '=' or not isinstance(binary.left, schema.Column) or not isinstance(binary.right, schema.Column):
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
                if binary.operator != '=' or not isinstance(binary.left, schema.Column) or not isinstance(binary.right, schema.Column):
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
            if len(self._legacy_foreignkey):
                for f in self._legacy_foreignkey:
                    if not f.primary_key:
                        self.direction = sync.ONETOMANY
                    else:
                        self.direction = sync.MANYTOONE

            elif len(self.remote_side):
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
        if len(self.remote_side):
            return
        self.remote_side = util.Set()

        if self.direction is sync.MANYTOONE:
            for c in self._opposite_side:
                self.remote_side.add(c)
        elif self.direction is sync.ONETOMANY or self.direction is sync.MANYTOMANY:
            for c in self.foreign_keys:
                self.remote_side.add(c)

    def _create_polymorphic_joins(self):
        # get ready to create "polymorphic" primary/secondary join clauses.
        # these clauses represent the same join between parent/child tables that the primary
        # and secondary join clauses represent, except they reference ColumnElements that are specifically
        # in the "polymorphic" selectables.  these are used to construct joins for both Query as well as
        # eager loading, and also are used to calculate "lazy loading" clauses.

        # as we will be using the polymorphic selectables (i.e. select_table argument to Mapper) to figure this out,
        # first create maps of all the "equivalent" columns, since polymorphic selectables will often munge
        # several "equivalent" columns (such as parent/child fk cols) into just one column.
        target_equivalents = self.mapper._get_inherited_column_equivalents()

        # if the target mapper loads polymorphically, adapt the clauses to the target's selectable
        if self.loads_polymorphic:
            if self.secondaryjoin:
                self.polymorphic_secondaryjoin = self.secondaryjoin.copy_container()
                sql_util.ClauseAdapter(self.mapper.select_table).traverse(self.polymorphic_secondaryjoin)
                self.polymorphic_primaryjoin = self.primaryjoin.copy_container()
            else:
                self.polymorphic_primaryjoin = self.primaryjoin.copy_container()
                if self.direction is sync.ONETOMANY:
                    sql_util.ClauseAdapter(self.mapper.select_table, include=self.foreign_keys, equivalents=target_equivalents).traverse(self.polymorphic_primaryjoin)
                elif self.direction is sync.MANYTOONE:
                    sql_util.ClauseAdapter(self.mapper.select_table, exclude=self.foreign_keys, equivalents=target_equivalents).traverse(self.polymorphic_primaryjoin)
                self.polymorphic_secondaryjoin = None
            # load "polymorphic" versions of the columns present in "remote_side" - this is
            # important for lazy-clause generation which goes off the polymorphic target selectable
            for c in list(self.remote_side):
                if self.secondary and c in self.secondary.columns:
                    continue
                for equiv in [c] + (c in target_equivalents and target_equivalents[c] or []): 
                    corr = self.mapper.select_table.corresponding_column(equiv, raiseerr=False)
                    if corr:
                        self.remote_side.add(corr)
                        break
                else:
                    raise exceptions.AssertionError(str(self) + ": Could not find corresponding column for " + str(c) + " in selectable "  + str(self.mapper.select_table))
        else:
            self.polymorphic_primaryjoin = self.primaryjoin.copy_container()
            self.polymorphic_secondaryjoin = self.secondaryjoin and self.secondaryjoin.copy_container() or None

    def _post_init(self):
        if logging.is_info_enabled(self.logger):
            self.logger.info(str(self) + " setup primary join " + str(self.primaryjoin))
            self.logger.info(str(self) + " setup polymorphic primary join " + str(self.polymorphic_primaryjoin))
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
        elif not sessionlib.attribute_manager.is_class_managed(self.parent.class_, self.key):
            raise exceptions.ArgumentError("Attempting to assign a new relation '%s' to a non-primary mapper on class '%s'.  New relations can only be added to the primary mapper, i.e. the very first mapper created for class '%s' " % (self.key, self.parent.class_.__name__, self.parent.class_.__name__))

        super(PropertyLoader, self).do_init()

    def _is_self_referential(self):
        return self.parent.mapped_table is self.target or self.parent.select_table is self.target

    def get_join(self, parent, primary=True, secondary=True):
        try:
            return self._parent_join_cache[(parent, primary, secondary)]
        except KeyError:
            parent_equivalents = parent._get_inherited_column_equivalents()
            primaryjoin = self.polymorphic_primaryjoin.copy_container()
            if self.secondaryjoin is not None:
                secondaryjoin = self.polymorphic_secondaryjoin.copy_container()
            else:
                secondaryjoin = None
            if self.direction is sync.ONETOMANY:
                sql_util.ClauseAdapter(parent.select_table, exclude=self.foreign_keys, equivalents=parent_equivalents).traverse(primaryjoin)
            elif self.direction is sync.MANYTOONE:
                sql_util.ClauseAdapter(parent.select_table, include=self.foreign_keys, equivalents=parent_equivalents).traverse(primaryjoin)
            elif self.secondaryjoin:
                sql_util.ClauseAdapter(parent.select_table, exclude=self.foreign_keys, equivalents=parent_equivalents).traverse(primaryjoin)

            if secondaryjoin is not None:
                if secondary and not primary:
                    j = secondaryjoin
                elif primary and secondary:
                    j = primaryjoin & secondaryjoin
                elif primary and not secondary:
                    j = primaryjoin
            else:
                j = primaryjoin
            self._parent_join_cache[(parent, primary, secondary)] = j
            return j

    def register_dependencies(self, uowcommit):
        if not self.viewonly:
            self._dependency_processor.register_dependencies(uowcommit)

PropertyLoader.logger = logging.class_logger(PropertyLoader)

class BackRef(object):
    """Stores the name of a backreference property as well as options
    to be used on the resulting PropertyLoader.
    """

    def __init__(self, key, **kwargs):
        self.key = key
        self.kwargs = kwargs

    def compile(self, prop):
        """Called by the owning PropertyLoader to set up a
        backreference on the PropertyLoader's mapper.
        """

        # try to set a LazyLoader on our mapper referencing the parent mapper
        mapper = prop.mapper.primary_mapper()
        if not mapper.props.has_key(self.key):
            pj = self.kwargs.pop('primaryjoin', None)
            sj = self.kwargs.pop('secondaryjoin', None)
            # the backref property is set on the primary mapper
            parent = prop.parent.primary_mapper()
            self.kwargs.setdefault('viewonly', prop.viewonly)
            self.kwargs.setdefault('post_update', prop.post_update)
            relation = PropertyLoader(parent, prop.secondary, pj, sj,
                                      backref=prop.key, is_backref=True,
                                      **self.kwargs)
            mapper._compile_property(self.key, relation);
        elif not isinstance(mapper.props[self.key], PropertyLoader):
            raise exceptions.ArgumentError(
                "Can't create backref '%s' on mapper '%s'; an incompatible "
                "property of that name already exists" % (self.key, str(mapper)))
        else:
            # else set one of us as the "backreference"
            parent = prop.parent.primary_mapper()
            if parent.class_ is not mapper.props[self.key]._get_target_class():
                raise exceptions.ArgumentError(
                    "Backrefs do not match:  backref '%s' expects to connect to %s, "
                    "but found a backref already connected to %s" %
                    (self.key, str(parent.class_), str(mapper.props[self.key].mapper.class_)))
            if not mapper.props[self.key].is_backref:
                prop.is_backref=True
                if not prop.viewonly:
                    prop._dependency_processor.is_backref=True
                    # reverse_property used by dependencies.ManyToManyDP to check
                    # association table operations
                    prop.reverse_property = mapper.props[self.key]
                    mapper.props[self.key].reverse_property = prop

    def get_extension(self):
        """Return an attribute extension to use with this backreference."""

        return attributes.GenericBackrefExtension(self.key)
