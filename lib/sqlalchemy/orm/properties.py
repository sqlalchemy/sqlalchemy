# properties.py
# Copyright (C) 2005,2006 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""defines a set of mapper.MapperProperty objects, including basic column properties as 
well as relationships.  also defines some MapperOptions that can be used with the
properties."""

from sqlalchemy import sql, schema, util, attributes, exceptions
import sync
import mapper
import session as sessionlib
import dependency
import util as mapperutil
import sets, random

class ColumnProperty(mapper.MapperProperty):
    """describes an object attribute that corresponds to a table column."""
    def __init__(self, *columns):
        """the list of columns describes a single object property. if there
        are multiple tables joined together for the mapper, this list represents
        the equivalent column as it appears across each table."""
        self.columns = list(columns)
    def getattr(self, object):
        return getattr(object, self.key, None)
    def setattr(self, object, value):
        setattr(object, self.key, value)
    def get_history(self, obj, passive=False):
        return sessionlib.global_attributes.get_history(obj, self.key, passive=passive)
    def copy(self):
        return ColumnProperty(*self.columns)
    def setup(self, key, statement, eagertable=None, **options):
        for c in self.columns:
            if eagertable is not None:
                statement.append_column(eagertable.corresponding_column(c))
            else:
                statement.append_column(c)
    def do_init(self, key, parent):
        # establish a SmartProperty property manager on the object for this key
        if parent._is_primary_mapper():
            #print "regiser col on class %s key %s" % (parent.class_.__name__, key)
            sessionlib.global_attributes.register_attribute(parent.class_, key, uselist = False)
    def execute(self, session, instance, row, identitykey, imap, isnew):
        if isnew:
            #print "POPULATING OBJ", instance.__class__.__name__, "COL", self.columns[0]._label, "WITH DATA", row[self.columns[0]], "ROW IS A", row.__class__.__name__, "COL ID", id(self.columns[0])
            instance.__dict__[self.key] = row[self.columns[0]]
    def __repr__(self):
        return "ColumnProperty(%s)" % repr([str(c) for c in self.columns])
        
class DeferredColumnProperty(ColumnProperty):
    """describes an object attribute that corresponds to a table column, which also
    will "lazy load" its value from the table.  this is per-column lazy loading."""
    def __init__(self, *columns, **kwargs):
        self.group = kwargs.get('group', None)
        ColumnProperty.__init__(self, *columns)
    def copy(self):
        return DeferredColumnProperty(*self.columns)
    def do_init(self, key, parent):
        # establish a SmartProperty property manager on the object for this key, 
        # containing a callable to load in the attribute
        if self.is_primary():
            sessionlib.global_attributes.register_attribute(parent.class_, key, uselist=False, callable_=lambda i:self.setup_loader(i))
    def setup_loader(self, instance):
        if not self.localparent.is_assigned(instance):
            return mapper.object_mapper(instance).props[self.key].setup_loader(instance)
        def lazyload():
            session = sessionlib.object_session(instance)
            connection = session.connection(self.parent)
            clause = sql.and_()
            try:
                pk = self.parent.pks_by_table[self.columns[0].table]
            except KeyError:
                pk = self.columns[0].table.primary_key
            for primary_key in pk:
                attr = self.parent._getattrbycolumn(instance, primary_key)
                if not attr:
                    return None
                clause.clauses.append(primary_key == attr)
            
            try:
                if self.group is not None:
                    groupcols = [p for p in self.localparent.props.values() if isinstance(p, DeferredColumnProperty) and p.group==self.group]
                    row = connection.execute(sql.select([g.columns[0] for g in groupcols], clause, use_labels=True), None).fetchone()
                    for prop in groupcols:
                        if prop is self:
                            continue
                        instance.__dict__[prop.key] = row[prop.columns[0]]
                        sessionlib.global_attributes.create_history(instance, prop.key, uselist=False)
                    return row[self.columns[0]]    
                else:
                    return connection.scalar(sql.select([self.columns[0]], clause, use_labels=True),None)
            finally:
                connection.close()
        return lazyload
    def setup(self, key, statement, **options):
        pass
    def execute(self, session, instance, row, identitykey, imap, isnew):
        if isnew:
            if not self.is_primary():
                sessionlib.global_attributes.create_history(instance, self.key, False, callable_=self.setup_loader(instance))
            else:
                sessionlib.global_attributes.reset_history(instance, self.key)

mapper.ColumnProperty = ColumnProperty

class PropertyLoader(mapper.MapperProperty):
    ONETOMANY = 0
    MANYTOONE = 1
    MANYTOMANY = 2

    """describes an object property that holds a single item or list of items that correspond
    to a related database table."""
    def __init__(self, argument, secondary, primaryjoin, secondaryjoin, foreignkey=None, uselist=None, private=False, association=None, order_by=False, attributeext=None, backref=None, is_backref=False, post_update=False, cascade=None):
        self.uselist = uselist
        self.argument = argument
        self.secondary = secondary
        self.primaryjoin = primaryjoin
        self.secondaryjoin = secondaryjoin
        self.post_update = post_update
        self.direction = None
        
        # would like to have foreignkey be a list.
        # however, have to figure out how to do 
        # <column> in <list>, since column overrides the == operator 
        # and it doesnt work
        self.foreignkey = foreignkey  #util.to_set(foreignkey)
        if foreignkey:
            self.foreigntable = foreignkey.table
        else:
            self.foreigntable = None
        
        if cascade is not None:
            self.cascade = mapperutil.CascadeOptions(cascade)
        else:
            if private:
                self.cascade = mapperutil.CascadeOptions("all, delete-orphan")
            else:
                self.cascade = mapperutil.CascadeOptions("save-update")

        self.association = association
        self.order_by = order_by
        self.attributeext=attributeext
        if isinstance(backref, str):
            self.backref = BackRef(backref)
        else:
            self.backref = backref
        self.is_backref = is_backref

    private = property(lambda s:s.cascade.delete_orphan)
    
    def cascade_iterator(self, type, object, recursive):
        if not type in self.cascade:
            return
        childlist = sessionlib.global_attributes.get_history(object, self.key, passive=True)
        
        mapper = self.mapper.primary_mapper()
        for c in childlist.added_items() + childlist.deleted_items() + childlist.unchanged_items():
            if c is not None and c not in recursive:
                recursive.add(c)
                yield c
                for c2 in mapper.cascade_iterator(type, c, recursive):
                    yield c2

    def cascade_callable(self, type, object, callable_, recursive):
        if not type in self.cascade:
            return
        childlist = sessionlib.global_attributes.get_history(object, self.key, passive=True)
        mapper = self.mapper.primary_mapper()
        for c in childlist.added_items() + childlist.deleted_items() + childlist.unchanged_items():
            if c is not None and c not in recursive:
                recursive.add(c)
                callable_(c, mapper.entity_name)
                mapper.cascade_callable(type, c, callable_, recursive)

    def copy(self):
        x = self.__class__.__new__(self.__class__)
        x.__dict__.update(self.__dict__)
        return x
        
    def do_init_subclass(self, key, parent):
        """template method for subclasses of PropertyLoader"""
        pass
    
    def do_init(self, key, parent):
        import sqlalchemy.orm
        if isinstance(self.argument, type):
            self.mapper = mapper.class_mapper(self.argument)
        else:
            self.mapper = self.argument

        self.mapper = self.mapper.get_select_mapper()
        
        if self.association is not None:
            if isinstance(self.association, type):
                self.association = mapper.class_mapper(self.association)
        
        self.target = self.mapper.mapped_table

        if self.secondaryjoin is not None and self.secondary is None:
            raise exceptions.ArgumentError("Property '" + self.key + "' specified with secondary join condition but no secondary argument")
        # if join conditions were not specified, figure them out based on foreign keys
        if self.secondary is not None:
            if self.secondaryjoin is None:
                self.secondaryjoin = sql.join(self.mapper.unjoined_table, self.secondary).onclause
            if self.primaryjoin is None:
                self.primaryjoin = sql.join(parent.unjoined_table, self.secondary).onclause
        else:
            if self.primaryjoin is None:
                self.primaryjoin = sql.join(parent.unjoined_table, self.target).onclause

        # if the foreign key wasnt specified and theres no assocaition table, try to figure
        # out who is dependent on who. we dont need all the foreign keys represented in the join,
        # just one of them.  
        if self.foreignkey is None and self.secondaryjoin is None:
            # else we usually will have a one-to-many where the secondary depends on the primary
            # but its possible that its reversed
            self._find_dependent()

        # if we are re-initializing, as in a copy made for an inheriting 
        # mapper, dont re-evaluate the direction.
        if self.direction is None:
            self.direction = self._get_direction()
        
        if self.uselist is None and self.direction == sync.MANYTOONE:
            self.uselist = False

        if self.uselist is None:
            self.uselist = True

        self._compile_synchronizers()
        self._dependency_processor = dependency.create_dependency_processor(self.key, self.syncrules, self.cascade, secondary=self.secondary, association=self.association, is_backref=self.is_backref, post_update=self.post_update)

        # primary property handler, set up class attributes
        if self.is_primary():
            # if a backref name is defined, set up an extension to populate 
            # attributes in the other direction
            if self.backref is not None:
                self.attributeext = self.backref.get_extension()
        
            # set our class attribute
            self._set_class_attribute(parent.class_, key)

            if self.backref is not None:
                self.backref.compile(self)
        elif not sessionlib.global_attributes.is_class_managed(parent.class_, key):
            raise exceptions.ArgumentError("Attempting to assign a new relation '%s' to a non-primary mapper on class '%s'.  New relations can only be added to the primary mapper, i.e. the very first mapper created for class '%s' " % (key, parent.class_.__name__, parent.class_.__name__))

        self.do_init_subclass(key, parent)

    def _register_attribute(self, class_, callable_=None):
        sessionlib.global_attributes.register_attribute(class_, self.key, uselist = self.uselist, extension=self.attributeext, cascade=self.cascade,  trackparent=True, callable_=callable_)

    def _create_history(self, instance, callable_=None):
        return sessionlib.global_attributes.create_history(instance, self.key, self.uselist, cascade=self.cascade,  trackparent=True, callable_=callable_)
        
    def _set_class_attribute(self, class_, key):
        """sets attribute behavior on our target class."""
        self._register_attribute(class_)
        
    def _get_direction(self):
        """determines our 'direction', i.e. do we represent one to many, many to many, etc."""
        if self.secondaryjoin is not None:
            return sync.MANYTOMANY
        elif self.parent.mapped_table is self.target or self.parent.select_table is self.target:
            if self.foreignkey.primary_key:
                return sync.MANYTOONE
            else:
                return sync.ONETOMANY
        elif self.foreigntable == self.mapper.unjoined_table:
            return sync.ONETOMANY
        elif self.foreigntable == self.parent.unjoined_table:
            return sync.MANYTOONE
        else:
            raise exceptions.ArgumentError("Cant determine relation direction")
            
    def _find_dependent(self):
        """searches through the primary join condition to determine which side
        has the primary key and which has the foreign key - from this we return 
        the "foreign key" for this property which helps determine one-to-many/many-to-one."""
        
        # set as a reference to allow assignment from inside a first-class function
        dependent = [None]
        def foo(binary):
            if binary.operator != '=' or not isinstance(binary.left, schema.Column) or not isinstance(binary.right, schema.Column):
                return
            if binary.left.primary_key:
                if dependent[0] is binary.left.table:
                    raise exceptions.ArgumentError("Could not determine the parent/child relationship for property '%s', based on join condition '%s' (table '%s' appears on both sides of the relationship, or in an otherwise ambiguous manner). please specify the 'foreignkey' keyword parameter to the relation() function indicating a column on the remote side of the relationship" % (self.key, str(self.primaryjoin), binary.left.table.name))
                dependent[0] = binary.right.table
                self.foreignkey= binary.right
            elif binary.right.primary_key:
                if dependent[0] is binary.right.table:
                    raise exceptions.ArgumentError("Could not determine the parent/child relationship for property '%s', based on join condition '%s' (table '%s' appears on both sides of the relationship, or in an otherwise ambiguous manner). please specify the 'foreignkey' keyword parameter to the relation() function indicating a column on the remote side of the relationship" % (self.key, str(self.primaryjoin), binary.right.table.name))
                dependent[0] = binary.left.table
                self.foreignkey = binary.left
        visitor = BinaryVisitor(foo)
        self.primaryjoin.accept_visitor(visitor)
        if dependent[0] is None:
            raise exceptions.ArgumentError("Could not determine the parent/child relationship for property '%s', based on join condition '%s' (no relationships joining tables '%s' and '%s' could be located). please specify the 'foreignkey' keyword parameter to the relation() function indicating a column on the remote side of the relationship" % (self.key, str(self.primaryjoin), str(binary.left.table), binary.right.table.name))
        else:
            self.foreigntable = dependent[0]

    def get_join(self):
        if self.secondaryjoin is not None:
            return self.primaryjoin & self.secondaryjoin
        else:
            return self.primaryjoin

    def execute(self, session, instance, row, identitykey, imap, isnew):
        if self.is_primary():
            return
        #print "PLAIN PROPLOADER EXEC NON-PRIAMRY", repr(id(self)), repr(self.mapper.class_), self.key
        self._create_history(instance)

    def register_dependencies(self, uowcommit):
        self._dependency_processor.register_dependencies(uowcommit)

    def _compile_synchronizers(self):
        """assembles a list of 'synchronization rules', which are instructions on how to populate
        the objects on each side of a relationship.  This is done when a PropertyLoader is 
        first initialized.
        
        The list of rules is used within commits by the _synchronize() method when dependent 
        objects are processed."""
        parent_tables = util.HashSet(self.parent.tables + [self.parent.mapped_table])
        target_tables = util.HashSet(self.mapper.tables + [self.mapper.mapped_table])

        self.syncrules = sync.ClauseSynchronizer(self.parent, self.mapper, self.direction)
        if self.direction == sync.MANYTOMANY:
            self.syncrules.compile(self.primaryjoin, parent_tables, [self.secondary], False)
            self.syncrules.compile(self.secondaryjoin, target_tables, [self.secondary], True)
        else:
            self.syncrules.compile(self.primaryjoin, parent_tables, target_tables)

class LazyLoader(PropertyLoader):
    def do_init_subclass(self, key, parent):
        (self.lazywhere, self.lazybinds, self.lazyreverse) = create_lazy_clause(self.parent.unjoined_table, self.primaryjoin, self.secondaryjoin, self.foreignkey)
        # determine if our "lazywhere" clause is the same as the mapper's
        # get() clause.  then we can just use mapper.get()
        self.use_get = not self.uselist and self.mapper.query()._get_clause.compare(self.lazywhere)
        
    def _set_class_attribute(self, class_, key):
        # establish a class-level lazy loader on our class
        #print "SETCLASSATTR LAZY", repr(class_), key
        self._register_attribute(class_, callable_=lambda i: self.setup_loader(i))

    def setup_loader(self, instance):
        # make sure our parent mapper is the one thats assigned to this instance, else call that one
        if not self.localparent.is_assigned(instance):
            # if no mapper association with this instance (i.e. not in a session, not loaded by a mapper),
            # then we cant set up a lazy loader
            if not mapper.has_mapper(instance):
                return None
            else:
                return mapper.object_mapper(instance).props[self.key].setup_loader(instance)
        def lazyload():
            params = {}
            allparams = True
            session = sessionlib.object_session(instance)
            #print "setting up loader, lazywhere", str(self.lazywhere), "binds", self.lazybinds
            if session is not None:
                for col, bind in self.lazybinds.iteritems():
                    params[bind.key] = self.parent._getattrbycolumn(instance, col)
                    if params[bind.key] is None:
                        allparams = False
                        break
            else:
                allparams = False
            if allparams:
                # if we have a simple straight-primary key load, use mapper.get()
                # to possibly save a DB round trip
                if self.use_get:
                    ident = []
                    for primary_key in self.mapper.pks_by_table[self.mapper.mapped_table]:
                        bind = self.lazyreverse[primary_key]
                        ident.append(params[bind.key])
                    return self.mapper.using(session).get(ident)
                elif self.order_by is not False:
                    order_by = self.order_by
                elif self.secondary is not None and self.secondary.default_order_by() is not None:
                    order_by = self.secondary.default_order_by()
                else:
                    order_by = False
                result = self.mapper.using(session).select_whereclause(self.lazywhere, order_by=order_by, params=params)
            else:
                result = []
            if self.uselist:
                return result
            else:
                if len(result):
                    return result[0]
                else:
                    return None
        return lazyload
        
    def execute(self, session, instance, row, identitykey, imap, isnew):
        if isnew:
            # new object instance being loaded from a result row
            if not self.is_primary():
                #print "EXEC NON-PRIAMRY", repr(self.mapper.class_), self.key
                # we are not the primary manager for this attribute on this class - set up a per-instance lazyloader,
                # which will override the class-level behavior
                self._create_history(instance, callable_=self.setup_loader(instance))
            else:
                #print "EXEC PRIMARY", repr(self.mapper.class_), self.key
                # we are the primary manager for this attribute on this class - reset its per-instance attribute state, 
                # so that the class-level lazy loader is executed when next referenced on this instance.
                # this usually is not needed unless the constructor of the object referenced the attribute before we got 
                # to load data into it.
                sessionlib.global_attributes.reset_history(instance, self.key)
 
def create_lazy_clause(table, primaryjoin, secondaryjoin, foreignkey):
    binds = {}
    reverse = {}
    def bind_label():
        return "lazy_" + hex(random.randint(0, 65535))[2:]
    
    def visit_binary(binary):
        circular = isinstance(binary.left, schema.Column) and isinstance(binary.right, schema.Column) and binary.left.table is binary.right.table
        if isinstance(binary.left, schema.Column) and isinstance(binary.right, schema.Column) and ((not circular and binary.left.table is table) or (circular and binary.right is foreignkey)):
            col = binary.left
            binary.left = binds.setdefault(binary.left,
                    sql.BindParamClause(bind_label(), None, shortname = binary.left.name))
            reverse[binary.right] = binds[col]

        if isinstance(binary.right, schema.Column) and isinstance(binary.left, schema.Column) and ((not circular and binary.right.table is table) or (circular and binary.left is foreignkey)):
            col = binary.right
            binary.right = binds.setdefault(binary.right,
                    sql.BindParamClause(bind_label(), None, shortname = binary.right.name))
            reverse[binary.left] = binds[col]
            
    lazywhere = primaryjoin.copy_container()
    li = BinaryVisitor(visit_binary)
    lazywhere.accept_visitor(li)
    if secondaryjoin is not None:
        lazywhere = sql.and_(lazywhere, secondaryjoin)
    return (lazywhere, binds, reverse)
        

class EagerLoader(LazyLoader):
    """loads related objects inline with a parent query."""
    def do_init_subclass(self, key, parent, recursion_stack=None):
        if recursion_stack is None:
            LazyLoader.do_init_subclass(self, key, parent)
        parent._has_eager = True

        self.eagertarget = self.target.alias()
#        print "ALIAS", str(self.eagertarget.select()) #selectable.__class__.__name__
        if self.secondary:
            self.eagersecondary = self.secondary.alias()
            self.aliasizer = Aliasizer(self.target, self.secondary, aliases={
                    self.target:self.eagertarget,
                    self.secondary:self.eagersecondary
                    })
            #print "TARGET", self.target
            self.eagersecondaryjoin = self.secondaryjoin.copy_container()
            self.eagersecondaryjoin.accept_visitor(self.aliasizer)
            self.eagerprimary = self.primaryjoin.copy_container()
            self.eagerprimary.accept_visitor(self.aliasizer)
            #print "JOINS:", str(self.eagerprimary), "|", str(self.eagersecondaryjoin)
        else:
            self.aliasizer = Aliasizer(self.target, aliases={self.target:self.eagertarget})
            self.eagerprimary = self.primaryjoin.copy_container()
            self.eagerprimary.accept_visitor(self.aliasizer)
        
        if self.order_by:
            self.eager_order_by = self._aliasize_orderby(self.order_by)
        else:
            self.eager_order_by = None


    def _create_eager_chain(self, in_chain=False, recursion_stack=None):
        if not in_chain and getattr(self, '_eager_chained', False):
            return
            
        if recursion_stack is None:
            recursion_stack = {}

        eagerprops = []
        # create a new "eager chain", starting from this eager loader and descending downwards
        # through all sub-eagerloaders.  this will copy all those eagerloaders and have them set up
        # aliases distinct to this eager chain.  if a recursive relationship to any of the tables is detected,
        # the chain will terminate by copying that eager loader into a lazy loader.
        for key, prop in self.mapper.props.iteritems():
            if isinstance(prop, EagerLoader):
                eagerprops.append(prop)
        if len(eagerprops):
            recursion_stack[self.localparent.mapped_table] = True
            self.mapper = self.mapper.copy()
            try:
                for prop in eagerprops:
                    if recursion_stack.has_key(prop.target):
                        # recursion - set the relationship as a LazyLoader
                        p = EagerLazyOption(None, False).create_prop(self.mapper, prop.key)
                        continue
                    p = prop.copy()
                    self.mapper.props[prop.key] = p
#                    print "we are:", id(self), self.target.name, (self.secondary and self.secondary.name or "None"), self.parent.mapped_table.name
#                    print "prop is",id(prop), prop.target.name, (prop.secondary and prop.secondary.name or "None"), prop.parent.mapped_table.name
                    p.do_init_subclass(prop.key, prop.parent, recursion_stack)
                    p._create_eager_chain(in_chain=True, recursion_stack=recursion_stack)
                    p.eagerprimary = p.eagerprimary.copy_container()
#                    aliasizer = Aliasizer(p.parent.mapped_table, aliases={p.parent.mapped_table:self.eagertarget})
                    p.eagerprimary.accept_visitor(self.aliasizer)
                    #print "new eagertqarget", p.eagertarget.name, (p.secondary and p.secondary.name or "none"), p.parent.mapped_table.name
            finally:
                del recursion_stack[self.localparent.mapped_table]

        self._row_decorator = self._create_decorator_row()
        
        self._eager_chained = True
                
    def _aliasize_orderby(self, orderby, copy=True):
        if copy:
            orderby = [o.copy_container() for o in util.to_list(orderby)]
        else:
            orderby = util.to_list(orderby)
        for i in range(0, len(orderby)):
            if isinstance(orderby[i], schema.Column):
                orderby[i] = self.eagertarget.corresponding_column(orderby[i])
            else:
                orderby[i].accept_visitor(self.aliasizer)
        return orderby
        
    def setup(self, key, statement, eagertable=None, **options):
        """add a left outer join to the statement thats being constructed"""

        # initialize the eager chains late in the game
        self._create_eager_chain()

        if hasattr(statement, '_outerjoin'):
            towrap = statement._outerjoin
        else:
            towrap = self.localparent.mapped_table

 #       print "hello, towrap", str(towrap)
        if self.secondaryjoin is not None:
            statement._outerjoin = sql.outerjoin(towrap, self.eagersecondary, self.eagerprimary).outerjoin(self.eagertarget, self.eagersecondaryjoin)
            if self.order_by is False and self.secondary.default_order_by() is not None:
                statement.order_by(*self.eagersecondary.default_order_by())
        else:
            statement._outerjoin = towrap.outerjoin(self.eagertarget, self.eagerprimary)
            if self.order_by is False and self.eagertarget.default_order_by() is not None:
                statement.order_by(*self.eagertarget.default_order_by())

        if self.eager_order_by:
            statement.order_by(*util.to_list(self.eager_order_by))
        elif getattr(statement, 'order_by_clause', None):
            self._aliasize_orderby(statement.order_by_clause, False)
                
        statement.append_from(statement._outerjoin)
        for key, value in self.mapper.props.iteritems():
            value.setup(key, statement, eagertable=self.eagertarget)
            
        
    def execute(self, session, instance, row, identitykey, imap, isnew):
        """receive a row.  tell our mapper to look for a new object instance in the row, and attach
        it to a list on the parent instance."""
        
        decorated_row = self._decorate_row(row)
        try:
            # check for identity key
            identity_key = self.mapper._row_identity_key(decorated_row)
        except KeyError:
            # else degrade to a lazy loader
            LazyLoader.execute(self, session, instance, row, identitykey, imap, isnew)
            return
                
        if isnew:
            # new row loaded from the database.  initialize a blank container on the instance.
            # this will override any per-class lazyloading type of stuff.
            h = self._create_history(instance)
            
        if not self.uselist:
            if isnew:
                h.setattr_clean(self.mapper._instance(session, decorated_row, imap, None))
            else:
                # call _instance on the row, even though the object has been created,
                # so that we further descend into properties
                self.mapper._instance(session, decorated_row, imap, None)
                
            return
        elif isnew:
            result_list = h
        else:
            result_list = getattr(instance, self.key)
        self.mapper._instance(session, decorated_row, imap, result_list)

    def _create_decorator_row(self):
        class DecoratorDict(object):
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
        return DecoratorDict

    def _decorate_row(self, row):
        # since the EagerLoader makes an Alias of its mapper's table,
        # we translate the actual result columns back to what they 
        # would normally be into a "virtual row" which is passed to the child mapper.
        # that way the mapper doesnt have to know about the modified column name
        # (neither do any MapperExtensions).  The row is keyed off the Column object
        # (which is what mappers use) as well as its "label" (which might be what
        # user-defined code is using)
        try:
            return self._row_decorator(row)
        except AttributeError:
            self._create_eager_chain()
            return self._row_decorator(row)

class GenericOption(mapper.MapperOption):
    """a mapper option that can handle dotted property names,
    descending down through the relations of a mapper until it
    reaches the target."""
    def __init__(self, key):
        self.key = key
    def process(self, mapper):
        self.process_by_key(mapper, self.key)
    def process_by_key(self, mapper, key):
        tokens = key.split('.', 1)
        if len(tokens) > 1:
            oldprop = mapper.props[tokens[0]]
            newprop = oldprop.copy()
            newprop.argument = self.process_by_key(oldprop.mapper.copy(), tokens[1])
            mapper.set_property(tokens[0], newprop)
        else:
            self.create_prop(mapper, tokens[0])
        return mapper
        
    def create_prop(self, mapper, key):
        kwargs = util.constructor_args(oldprop)
        mapper.set_property(key, class_(**kwargs ))

class BackRef(object):
    """stores the name of a backreference property as well as options to 
    be used on the resulting PropertyLoader."""
    def __init__(self, key, **kwargs):
        self.key = key
        self.kwargs = kwargs
    def compile(self, prop):
        """called by the owning PropertyLoader to set up a backreference on the
        PropertyLoader's mapper."""
        # try to set a LazyLoader on our mapper referencing the parent mapper
        mapper = prop.mapper.primary_mapper()
        if not mapper.props.has_key(self.key):
            pj = self.kwargs.pop('primaryjoin', None)
            sj = self.kwargs.pop('secondaryjoin', None)
            # TODO: we are going to have the newly backref'd property create its 
            # primary/secondary join through normal means, and only override if they are
            # specified to the constructor.  think about if this is really going to work
            # all the way.
            #if pj is None:
            #    if prop.secondaryjoin is not None:
            #        # if setting up a backref to a many-to-many, reverse the order
            #        # of the "primary" and "secondary" joins
            #        pj = prop.secondaryjoin
            #        sj = prop.primaryjoin
            #    else:
            #        pj = prop.primaryjoin
            #        sj = None
            lazy = self.kwargs.pop('lazy', True)
            if lazy:
                cls = LazyLoader
            else:
                cls = EagerLoader
            # the backref property is set on the primary mapper
            parent = prop.parent.primary_mapper()
            relation = cls(parent, prop.secondary, pj, sj, backref=prop.key, is_backref=True, **self.kwargs)
            mapper.add_property(self.key, relation);
        else:
            # else set one of us as the "backreference"
            if not mapper.props[self.key].is_backref:
                prop.is_backref=True
                prop._dependency_processor.is_backref=True
    def get_extension(self):
        """returns an attribute extension to use with this backreference."""
        return attributes.GenericBackrefExtension(self.key)
        
class EagerLazyOption(GenericOption):
    """an option that switches a PropertyLoader to be an EagerLoader or LazyLoader"""
    def __init__(self, key, toeager = True, **kwargs):
        self.key = key
        self.toeager = toeager
        self.kwargs = kwargs

    def hash_key(self):
        return "EagerLazyOption(%s, %s)" % (repr(self.key), repr(self.toeager))

    def create_prop(self, mapper, key):
        if self.toeager:
            class_ = EagerLoader
        elif self.toeager is None:
            class_ = PropertyLoader
        else:
            class_ = LazyLoader

        oldprop = mapper.props[key]
        newprop = class_.__new__(class_)
        newprop.__dict__.update(oldprop.__dict__)
        newprop.do_init_subclass(key, mapper)
        mapper.set_property(key, newprop)

class DeferredOption(GenericOption):
    def __init__(self, key, defer=False, **kwargs):
        self.key = key
        self.defer = defer
        self.kwargs = kwargs
    def hash_key(self):
        return "DeferredOption(%s,%s)" % (self.key, self.defer)
    def create_prop(self, mapper, key):
        oldprop = mapper.props[key]
        if self.defer:
            prop = DeferredColumnProperty(*oldprop.columns, **self.kwargs)
        else:
            prop = ColumnProperty(*oldprop.columns, **self.kwargs)
        mapper.set_property(key, prop)
        
class Aliasizer(sql.ClauseVisitor):
    """converts a table instance within an expression to be an alias of that table."""
    def __init__(self, *tables, **kwargs):
        self.tables = {}
        self.aliases = kwargs.get('aliases', {})
        for t in tables:
            self.tables[t] = t
            if not self.aliases.has_key(t):
                self.aliases[t] = sql.alias(t)
            if isinstance(t, sql.Join):
                for t2 in t.columns:
                    self.tables[t2.table] = t2
                    self.aliases[t2.table] = self.aliases[t]
        self.binary = None
    def get_alias(self, table):
        return self.aliases[table]
    def visit_compound(self, compound):
        self.visit_clauselist(compound)
    def visit_clauselist(self, clist):
        for i in range(0, len(clist.clauses)):
            if isinstance(clist.clauses[i], schema.Column) and self.tables.has_key(clist.clauses[i].table):
                orig = clist.clauses[i]
                clist.clauses[i] = self.get_alias(clist.clauses[i].table).corresponding_column(clist.clauses[i])
                if clist.clauses[i] is None:
                    raise "cant get orig for " + str(orig) + " against table " + orig.table.name + " " + self.get_alias(orig.table).name
    def visit_binary(self, binary):
        if isinstance(binary.left, schema.Column) and self.tables.has_key(binary.left.table):
            binary.left = self.get_alias(binary.left.table).corresponding_column(binary.left)
        if isinstance(binary.right, schema.Column) and self.tables.has_key(binary.right.table):
            binary.right = self.get_alias(binary.right.table).corresponding_column(binary.right)

class BinaryVisitor(sql.ClauseVisitor):
    def __init__(self, func):
        self.func = func
    def visit_binary(self, binary):
        self.func(binary)
