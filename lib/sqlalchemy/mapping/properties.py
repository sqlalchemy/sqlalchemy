# properties.py
# Copyright (C) 2005,2006 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""defines a set of MapperProperty objects, including basic column properties as 
well as relationships.  also defines some MapperOptions that can be used with the
properties."""

from mapper import *
import sqlalchemy.sql as sql
import sqlalchemy.schema as schema
import sqlalchemy.engine as engine
import sqlalchemy.util as util
import sqlalchemy.attributes as attributes
import sync
import mapper
import objectstore
from sqlalchemy.exceptions import *

class ColumnProperty(MapperProperty):
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
        return objectstore.global_attributes.get_history(obj, self.key, passive=passive)
    def copy(self):
        return ColumnProperty(*self.columns)
    def setup(self, key, statement, eagertable=None, **options):
        for c in self.columns:
            if eagertable is not None:
                statement.append_column(eagertable._get_col_by_original(c))
            else:
                statement.append_column(c)
    def do_init(self, key, parent):
        self.key = key
        # establish a SmartProperty property manager on the object for this key
        if parent._is_primary_mapper():
            #print "regiser col on class %s key %s" % (parent.class_.__name__, key)
            objectstore.uow().register_attribute(parent.class_, key, uselist = False)
    def execute(self, instance, row, identitykey, imap, isnew):
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
        self.key = key
        self.parent = parent
        # establish a SmartProperty property manager on the object for this key, 
        # containing a callable to load in the attribute
        if self.is_primary():
            objectstore.uow().register_attribute(parent.class_, key, uselist=False, callable_=lambda i:self.setup_loader(i))
    def setup_loader(self, instance):
        def lazyload():
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
            
            if self.group is not None:
                groupcols = [p for p in self.parent.props.values() if isinstance(p, DeferredColumnProperty) and p.group==self.group]
                row = sql.select([g.columns[0] for g in groupcols], clause, use_labels=True).execute().fetchone()
                for prop in groupcols:
                    if prop is self:
                        continue
                    instance.__dict__[prop.key] = row[prop.columns[0]]
                    objectstore.global_attributes.create_history(instance, prop.key, uselist=False)
                return row[self.columns[0]]    
            else:
                return sql.select([self.columns[0]], clause, use_labels=True).scalar()
        return lazyload
    def setup(self, key, statement, **options):
        pass
    def execute(self, instance, row, identitykey, imap, isnew):
        if isnew:
            if not self.is_primary():
                objectstore.global_attributes.create_history(instance, self.key, False, callable_=self.setup_loader(instance))
            else:
                objectstore.global_attributes.reset_history(instance, self.key)

mapper.ColumnProperty = ColumnProperty

class PropertyLoader(MapperProperty):
    ONETOMANY = 0
    MANYTOONE = 1
    MANYTOMANY = 2

    """describes an object property that holds a single item or list of items that correspond
    to a related database table."""
    def __init__(self, argument, secondary, primaryjoin, secondaryjoin, foreignkey=None, uselist=None, private=False, association=None, use_alias=None, selectalias=None, order_by=False, attributeext=None, backref=None, is_backref=False, post_update=False):
        self.uselist = uselist
        self.argument = argument
        self.secondary = secondary
        self.primaryjoin = primaryjoin
        self.secondaryjoin = secondaryjoin
        self.post_update = post_update
        
        # would like to have foreignkey be a list.
        # however, have to figure out how to do 
        # <column> in <list>, since column overrides the == operator or somethign
        # and it doesnt work
        self.foreignkey = foreignkey  #util.to_set(foreignkey)
        if foreignkey:
            self.foreigntable = foreignkey.table
        else:
            self.foreigntable = None
            
        self.private = private
        self.association = association
        if selectalias is not None:
            print "'selectalias' argument to relation() is deprecated.  eager loads automatically alias-ize tables now."
        if use_alias is not None:
            print "'use_alias' argument to relation() is deprecated.  eager loads automatically alias-ize tables now."
        self.order_by = order_by
        self.attributeext=attributeext
        if isinstance(backref, str):
            self.backref = BackRef(backref)
        else:
            self.backref = backref
        self.is_backref = is_backref

    def copy(self):
        x = self.__class__.__new__(self.__class__)
        x.__dict__.update(self.__dict__)
        return x
        
    def do_init_subclass(self, key, parent):
        """template method for subclasses of PropertyLoader"""
        pass
        
    def do_init(self, key, parent):
        import sqlalchemy.mapping
        if isinstance(self.argument, type):
            self.mapper = sqlalchemy.mapping.class_mapper(self.argument)
        else:
            self.mapper = self.argument

        if self.association is not None:
            if isinstance(self.association, type):
                self.association = sqlalchemy.mapping.class_mapper(self.association)
        
        self.target = self.mapper.table
        self.key = key
        self.parent = parent

        if self.secondaryjoin is not None and self.secondary is None:
            raise ArgumentError("Property '" + self.key + "' specified with secondary join condition but no secondary argument")
        # if join conditions were not specified, figure them out based on foreign keys
        if self.secondary is not None:
            if self.secondaryjoin is None:
                self.secondaryjoin = sql.join(self.mapper.noninherited_table, self.secondary).onclause
            if self.primaryjoin is None:
                self.primaryjoin = sql.join(parent.noninherited_table, self.secondary).onclause
        else:
            if self.primaryjoin is None:
                self.primaryjoin = sql.join(parent.noninherited_table, self.target).onclause
        # if the foreign key wasnt specified and theres no assocaition table, try to figure
        # out who is dependent on who. we dont need all the foreign keys represented in the join,
        # just one of them.  
        if self.foreignkey is None and self.secondaryjoin is None:
            # else we usually will have a one-to-many where the secondary depends on the primary
            # but its possible that its reversed
            self._find_dependent()

        self.direction = self._get_direction()
        
        if self.uselist is None and self.direction == PropertyLoader.MANYTOONE:
            self.uselist = False

        if self.uselist is None:
            self.uselist = True

        self._compile_synchronizers()

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
        elif not objectstore.global_attributes.is_class_managed(parent.class_, key):
            raise ArgumentError("Non-primary property created for attribute '%s' on class '%s', but that attribute is not managed! Insure that the primary mapper for this class defines this property" % (key, parent.class_.__name__))

        self.do_init_subclass(key, parent)
        
    def _set_class_attribute(self, class_, key):
        """sets attribute behavior on our target class."""
        objectstore.uow().register_attribute(class_, key, uselist = self.uselist, deleteremoved = self.private, extension=self.attributeext)
        
    def _get_direction(self):
        """determines our 'direction', i.e. do we represent one to many, many to many, etc."""
        #print self.key, repr(self.parent.table.name), repr(self.parent.primarytable.name), repr(self.foreignkey.table.name), repr(self.target), repr(self.foreigntable.name)
        
        if self.parent.table is self.target:
            if self.foreignkey.primary_key:
                return PropertyLoader.MANYTOONE
            else:
                return PropertyLoader.ONETOMANY
        elif self.secondaryjoin is not None:
            return PropertyLoader.MANYTOMANY
        elif self.foreigntable == self.target:
        #elif self.foreigntable is self.target or self.foreigntable in self.mapper.tables:
            return PropertyLoader.ONETOMANY
        elif self.foreigntable == self.parent.table:
        #elif self.foreigntable is self.parent.table or self.foreigntable in self.parent.tables:
            return PropertyLoader.MANYTOONE
        else:
            raise ArgumentError("Cant determine relation direction")
            
    def _find_dependent(self):
        """searches through the primary join condition to determine which side
        has the primary key and which has the foreign key - from this we return 
        the "foreign key" for this property which helps determine one-to-many/many-to-one."""
        
        # set as a reference to allow assignment from inside a first-class function
        dependent = [None]
        def foo(binary):
            if binary.operator != '=':
                return
            if isinstance(binary.left, schema.Column) and binary.left.primary_key:
                if dependent[0] is binary.left.table:
                    raise ArgumentError("bidirectional dependency not supported...specify foreignkey")
                dependent[0] = binary.right.table
                self.foreignkey= binary.right
            elif isinstance(binary.right, schema.Column) and binary.right.primary_key:
                if dependent[0] is binary.right.table:
                    raise ArgumentError("bidirectional dependency not supported...specify foreignkey")
                dependent[0] = binary.left.table
                self.foreignkey = binary.left
        visitor = BinaryVisitor(foo)
        self.primaryjoin.accept_visitor(visitor)
        if dependent[0] is None:
            raise ArgumentError("cant determine primary foreign key in the join relationship....specify foreignkey=<column> or foreignkey=[<columns>]")
        else:
            self.foreigntable = dependent[0]

            
    def get_criterion(self, key, value):
        """given a key/value pair, determines if this PropertyLoader's mapper contains a key of the
        given name in its property list, or if this PropertyLoader's association mapper, if any, 
        contains a key of the given name in its property list, and returns a WHERE clause against
        the given value if found.
        
        this is called by a mappers select_by method to formulate a set of key/value pairs into 
        a WHERE criterion that spans multiple tables if needed."""
        # TODO: optimization: change mapper to accept a WHERE clause with separate bind parameters
        # then cache the generated WHERE clauses here, since the creation + the copy_container 
        # is an extra expense
        if self.mapper.props.has_key(key):
            if self.secondaryjoin is not None:
                c = (self.mapper.props[key].columns[0]==value) & self.primaryjoin & self.secondaryjoin
            else:
                c = (self.mapper.props[key].columns[0]==value) & self.primaryjoin
            return c.copy_container()
        elif self.mapper.table.c.has_key(key):
            if self.secondaryjoin is not None:
                c = (self.mapper.table.c[key].columns[0]==value) & self.primaryjoin & self.secondaryjoin
            else:
                c = (self.mapper.table.c[key].columns[0]==value) & self.primaryjoin
            return c.copy_container()
        elif self.association is not None:
            c = self.mapper._get_criterion(key, value) & self.primaryjoin
            return c.copy_container()
        return None

    def register_deleted(self, obj, uow):
        if not self.private:
            return

        if self.uselist:
            childlist = uow.attributes.get_history(obj, self.key, passive = False)
        else: 
            childlist = uow.attributes.get_history(obj, self.key)
        for child in childlist.deleted_items() + childlist.unchanged_items():
            if child is not None:
                uow.register_deleted(child)

    class MapperStub(object):
        """poses as a Mapper representing the association table in a many-to-many
        join, when performing a commit().  

        The Task objects in the objectstore module treat it just like
        any other Mapper, but in fact it only serves as a "dependency" placeholder
        for the many-to-many update task."""
        def __init__(self, mapper):
            self.mapper = mapper
        def save_obj(self, *args, **kwargs):
            pass
        def delete_obj(self, *args, **kwargs):
            pass
        def _primary_mapper(self):
            return self
        
    def register_dependencies(self, uowcommit):
        """tells a UOWTransaction what mappers are dependent on which, with regards
        to the two or three mappers handled by this PropertyLoader.
        
        Also registers itself as a "processor" for one of its mappers, which
        will be executed after that mapper's objects have been saved or before
        they've been deleted.  The process operation manages attributes and dependent
        operations upon the objects of one of the involved mappers."""
        if self.association is not None:
            # association object.  our mapper should be dependent on both
            # the parent mapper and the association object mapper.
            # this is where we put the "stub" as a marker, so we get
            # association/parent->stub->self, then we process the child
            # elments after the 'stub' save, which is before our own
            # mapper's save.
            stub = PropertyLoader.MapperStub(self.association)
            uowcommit.register_dependency(self.parent, stub)
            uowcommit.register_dependency(self.association, stub)
            uowcommit.register_dependency(stub, self.mapper)
            uowcommit.register_processor(stub, self, self.parent, False)
            uowcommit.register_processor(stub, self, self.parent, True)

        elif self.direction == PropertyLoader.MANYTOMANY:
            # many-to-many.  create a "Stub" mapper to represent the
            # "middle table" in the relationship.  This stub mapper doesnt save
            # or delete any objects, but just marks a dependency on the two
            # related mappers.  its dependency processor then populates the
            # association table.
            
            if self.is_backref:
                # if we are the "backref" half of a two-way backref 
                # relationship, let the other mapper handle inserting the rows
                return
            stub = PropertyLoader.MapperStub(self.mapper)
            uowcommit.register_dependency(self.parent, stub)
            uowcommit.register_dependency(self.mapper, stub)
            uowcommit.register_processor(stub, self, self.parent, False)
            uowcommit.register_processor(stub, self, self.parent, True)
        elif self.direction == PropertyLoader.ONETOMANY:
            if self.post_update:
                stub = PropertyLoader.MapperStub(self.mapper)
                uowcommit.register_dependency(self.mapper, stub)
                uowcommit.register_dependency(self.parent, stub)
                uowcommit.register_processor(stub, self, self.parent, False)
                uowcommit.register_processor(stub, self, self.parent, True)
            else:
                uowcommit.register_dependency(self.parent, self.mapper)
                uowcommit.register_processor(self.parent, self, self.parent, False)
                uowcommit.register_processor(self.parent, self, self.parent, True)
        elif self.direction == PropertyLoader.MANYTOONE:
            if self.post_update:
                stub = PropertyLoader.MapperStub(self.mapper)
                uowcommit.register_dependency(self.mapper, stub)
                uowcommit.register_dependency(self.parent, stub)
                uowcommit.register_processor(stub, self, self.parent, False)
                uowcommit.register_processor(stub, self, self.parent, True)
            else:
                uowcommit.register_dependency(self.mapper, self.parent)
                uowcommit.register_processor(self.mapper, self, self.parent, False)
        else:
            raise AssertionError(" no foreign key ?")

    def get_object_dependencies(self, obj, uowcommit, passive = True):
        return uowcommit.uow.attributes.get_history(obj, self.key, passive = passive)

    def whose_dependent_on_who(self, obj1, obj2):
        """given an object pair assuming obj2 is a child of obj1, returns a tuple
        with the dependent object second, or None if they are equal.  
        used by objectstore's object-level topological sort (i.e. cyclical 
        table dependency)."""
        if obj1 is obj2:
            return None
        elif self.direction == PropertyLoader.ONETOMANY:
            return (obj1, obj2)
        else:
            return (obj2, obj1)

    def process_dependencies(self, task, deplist, uowcommit, delete = False):
        """this method is called during a commit operation to synchronize data between a parent and child object.  
        it also can establish child or parent objects within the unit of work as "to be saved" or "deleted" 
        in some cases."""
        #print self.mapper.table.name + " " + self.key + " " + repr(len(deplist)) + " process_dep isdelete " + repr(delete) + " direction " + repr(self.direction)

        def getlist(obj, passive=True):
            l = self.get_object_dependencies(obj, uowcommit, passive)
            uowcommit.register_saved_history(l)
            return l

        # plugin point
        
        if self.direction == PropertyLoader.MANYTOMANY:
            secondary_delete = []
            secondary_insert = []
            if delete:
                for obj in deplist:
                    childlist = getlist(obj, False)
                    for child in childlist.deleted_items() + childlist.unchanged_items():
                        associationrow = {}
                        self._synchronize(obj, child, associationrow, False)
                        secondary_delete.append(associationrow)
            else:
                for obj in deplist:
                    childlist = getlist(obj)
                    if childlist is None: continue
                    for child in childlist.added_items():
                        associationrow = {}
                        self._synchronize(obj, child, associationrow, False)
                        secondary_insert.append(associationrow)
                    for child in childlist.deleted_items():
                        associationrow = {}
                        self._synchronize(obj, child, associationrow, False)
                        secondary_delete.append(associationrow)
            if len(secondary_delete):
                # TODO: precompile the delete/insert queries and store them as instance variables
                # on the PropertyLoader
                statement = self.secondary.delete(sql.and_(*[c == sql.bindparam(c.key) for c in self.secondary.c]))
                statement.execute(*secondary_delete)
            if len(secondary_insert):
                statement = self.secondary.insert()
                statement.execute(*secondary_insert)
        elif self.direction == PropertyLoader.MANYTOONE and delete:
            if self.post_update:
                # post_update means we have to update our row to not reference the child object
                # before we can DELETE the row
                for obj in deplist:
                    self._synchronize(obj, None, None, True)
                    uowcommit.register_object(obj, postupdate=True)
        elif self.direction == PropertyLoader.ONETOMANY and delete:
            # head object is being deleted, and we manage its list of child objects
            # the child objects have to have their foreign key to the parent set to NULL
            if self.private and not self.post_update:
                # if we are privately managed, then all our objects should
                # have been marked as "todelete" already and no attribute adjustment is needed
                return
            for obj in deplist:
                childlist = getlist(obj, False)
                for child in childlist.deleted_items() + childlist.unchanged_items():
                    if child is not None:
                        self._synchronize(obj, child, None, True)
                        uowcommit.register_object(child, postupdate=self.post_update)
        elif self.association is not None:
            # manage association objects.
            for obj in deplist:
                childlist = getlist(obj, passive=True)
                if childlist is None: continue
                
                #print "DIRECTION", self.direction
                d = {}
                for child in childlist:
                    self._synchronize(obj, child, None, False)
                    key = self.mapper.instance_key(child)
                    #print "SYNCHRONIZED", child, "INSTANCE KEY", key
                    d[key] = child
                    uowcommit.unregister_object(child)

                for child in childlist.added_items():
                    uowcommit.register_object(child)
                    key = self.mapper.instance_key(child)
                    #print "ADDED, INSTANCE KEY", key
                    d[key] = child
                    
                for child in childlist.unchanged_items():
                    key = self.mapper.instance_key(child)
                    o = d[key]
                    o._instance_key= key
                    
                for child in childlist.deleted_items():
                    key = self.mapper.instance_key(child)
                    #print "DELETED, INSTANCE KEY", key
                    if d.has_key(key):
                        o = d[key]
                        o._instance_key = key
                        uowcommit.unregister_object(child)
                    else:
                        #print "DELETE ASSOC OBJ", repr(child)
                        uowcommit.register_object(child, isdelete=True)
        else:
            for obj in deplist:
                childlist = getlist(obj, passive=True)
                if childlist is not None:
                    for child in childlist.added_items():
                        self._synchronize(obj, child, None, False)
                        if self.direction == PropertyLoader.ONETOMANY and child is not None:
                            uowcommit.register_object(child, postupdate=self.post_update)
                if self.direction == PropertyLoader.MANYTOONE:
                    uowcommit.register_object(obj, postupdate=self.post_update)
                if self.direction != PropertyLoader.MANYTOONE:
                    for child in childlist.deleted_items():
                        if not self.private:
                            self._synchronize(obj, child, None, True)
                        if self.direction == PropertyLoader.ONETOMANY:
                            # for a cyclical task, this registration is handled by the objectstore
                            uowcommit.register_object(child, isdelete=self.private)

    def execute(self, instance, row, identitykey, imap, isnew):
        if self.is_primary():
            return
        #print "PLAIN PROPLOADER EXEC NON-PRIAMRY", repr(id(self)), repr(self.mapper.class_), self.key
        objectstore.global_attributes.create_history(instance, self.key, self.uselist)

    def _compile_synchronizers(self):
        """assembles a list of 'synchronization rules', which are instructions on how to populate
        the objects on each side of a relationship.  This is done when a PropertyLoader is 
        first initialized.
        
        The list of rules is used within commits by the _synchronize() method when dependent 
        objects are processed."""

        parent_tables = util.HashSet(self.parent.tables + [self.parent.primarytable])
        target_tables = util.HashSet(self.mapper.tables + [self.mapper.primarytable])

        self.syncrules = sync.ClauseSynchronizer(self.parent, self.mapper, self.direction)
        if self.direction == PropertyLoader.MANYTOMANY:
            #print "COMPILING p/c", self.parent, self.mapper
            self.syncrules.compile(self.primaryjoin, parent_tables, [self.secondary], False)
            self.syncrules.compile(self.secondaryjoin, target_tables, [self.secondary], True)
        else:
            self.syncrules.compile(self.primaryjoin, parent_tables, target_tables)

    def _synchronize(self, obj, child, associationrow, clearkeys):
        """called during a commit to execute the full list of syncrules on the 
        given object/child/optional association row"""
        if self.direction == PropertyLoader.ONETOMANY:
            source = obj
            dest = child
        elif self.direction == PropertyLoader.MANYTOONE:
            source = child
            dest = obj
        elif self.direction == PropertyLoader.MANYTOMANY:
            dest = associationrow
            source = None
            
        if dest is None:
            return

        self.syncrules.execute(source, dest, obj, child, clearkeys)

class LazyLoader(PropertyLoader):
    def do_init_subclass(self, key, parent):
        (self.lazywhere, self.lazybinds) = create_lazy_clause(self.parent.table, self.primaryjoin, self.secondaryjoin, self.foreignkey)
        # determine if our "lazywhere" clause is the same as the mapper's
        # get() clause.  then we can just use mapper.get()
        self.use_get = not self.uselist and self.mapper._get_clause.compare(self.lazywhere)
        
    def _set_class_attribute(self, class_, key):
        # establish a class-level lazy loader on our class
        #print "SETCLASSATTR LAZY", repr(class_), key
        objectstore.global_attributes.register_attribute(class_, key, uselist = self.uselist, deleteremoved = self.private, callable_=lambda i: self.setup_loader(i), extension=self.attributeext)

    def setup_loader(self, instance):
        def lazyload():
            params = {}
            allparams = True
            #print "setting up loader, lazywhere", str(self.lazywhere)
            for col, bind in self.lazybinds.iteritems():
                params[bind.key] = self.parent._getattrbycolumn(instance, col)
                if params[bind.key] is None:
                    allparams = False
                    break
            if allparams:
                # if we have a simple straight-primary key load, use mapper.get()
                # to possibly save a DB round trip
                if self.use_get:
                    ident = []
                    for primary_key in self.mapper.pks_by_table[self.mapper.table]:
                        ident.append(params[self.mapper.table.name + "_" + primary_key.name])
                    return self.mapper.get(*ident)
                elif self.order_by is not False:
                    order_by = self.order_by
                elif self.secondary is not None and self.secondary.default_order_by() is not None:
                    order_by = self.secondary.default_order_by()
                else:
                    order_by = False
                result = self.mapper.select(self.lazywhere, order_by=order_by, params=params)
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
        
    def execute(self, instance, row, identitykey, imap, isnew):
        if isnew:
            # new object instance being loaded from a result row
            if not self.is_primary():
                #print "EXEC NON-PRIAMRY", repr(self.mapper.class_), self.key
                # we are not the primary manager for this attribute on this class - set up a per-instance lazyloader,
                # which will override the class-level behavior
                objectstore.global_attributes.create_history(instance, self.key, self.uselist, callable_=self.setup_loader(instance))
            else:
                #print "EXEC PRIMARY", repr(self.mapper.class_), self.key
                # we are the primary manager for this attribute on this class - reset its per-instance attribute state, 
                # so that the class-level lazy loader is executed when next referenced on this instance.
                # this usually is not needed unless the constructor of the object referenced the attribute before we got 
                # to load data into it.
                objectstore.global_attributes.reset_history(instance, self.key)
 
def create_lazy_clause(table, primaryjoin, secondaryjoin, foreignkey):
    binds = {}
    def visit_binary(binary):
        circular = isinstance(binary.left, schema.Column) and isinstance(binary.right, schema.Column) and binary.left.table is binary.right.table
        if isinstance(binary.left, schema.Column) and isinstance(binary.right, schema.Column) and ((not circular and binary.left.table is table) or (circular and binary.right is foreignkey)):
            binary.left = binds.setdefault(binary.left,
                    sql.BindParamClause(binary.right.table.name + "_" + binary.right.name, None, shortname = binary.left.name))
            binary.swap()

        if isinstance(binary.right, schema.Column) and isinstance(binary.left, schema.Column) and ((not circular and binary.right.table is table) or (circular and binary.left is foreignkey)):
            binary.right = binds.setdefault(binary.right,
                    sql.BindParamClause(binary.left.table.name + "_" + binary.left.name, None, shortname = binary.right.name))
                    
    if secondaryjoin is not None:
        lazywhere = sql.and_(primaryjoin, secondaryjoin)
    else:
        lazywhere = primaryjoin
    lazywhere = lazywhere.copy_container()
    li = BinaryVisitor(visit_binary)
    lazywhere.accept_visitor(li)
    return (lazywhere, binds)
        

class EagerLoader(PropertyLoader):
    """loads related objects inline with a parent query."""
    def do_init_subclass(self, key, parent, recursion_stack=None):
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
            recursion_stack[self.parent.table] = True
            self.mapper = self.mapper.copy()
            try:
                for prop in eagerprops:
                    if recursion_stack.has_key(prop.target):
                        # recursion - set the relationship as a LazyLoader
                        p = EagerLazyOption(None, False).create_prop(self.mapper, prop.key)
                        continue
                    p = prop.copy()
                    self.mapper.props[prop.key] = p
#                    print "we are:", id(self), self.target.name, (self.secondary and self.secondary.name or "None"), self.parent.table.name
#                    print "prop is",id(prop), prop.target.name, (prop.secondary and prop.secondary.name or "None"), prop.parent.table.name
                    p.do_init_subclass(prop.key, prop.parent, recursion_stack)
                    p._create_eager_chain(in_chain=True, recursion_stack=recursion_stack)
                    p.eagerprimary = p.eagerprimary.copy_container()
#                    aliasizer = Aliasizer(p.parent.table, aliases={p.parent.table:self.eagertarget})
                    p.eagerprimary.accept_visitor(self.aliasizer)
                    #print "new eagertqarget", p.eagertarget.name, (p.secondary and p.secondary.name or "none"), p.parent.table.name
            finally:
                del recursion_stack[self.parent.table]
        self._eager_chained = True
                
    def _aliasize_orderby(self, orderby, copy=True):
        if copy:
            orderby = [o.copy_container() for o in util.to_list(orderby)]
        else:
            orderby = util.to_list(orderby)
        for i in range(0, len(orderby)):
            if isinstance(orderby[i], schema.Column):
                orderby[i] = self.eagertarget._get_col_by_original(orderby[i])
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
            towrap = self.parent.table

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
            
    def execute(self, instance, row, identitykey, imap, isnew):
        """receive a row.  tell our mapper to look for a new object instance in the row, and attach
        it to a list on the parent instance."""
        
        if isnew:
            # new row loaded from the database.  initialize a blank container on the instance.
            # this will override any per-class lazyloading type of stuff.
            h = objectstore.global_attributes.create_history(instance, self.key, self.uselist)
            
        if not self.uselist:
            if isnew:
                h.setattr_clean(self._instance(row, imap))
            else:
                # call _instance on the row, even though the object has been created,
                # so that we further descend into properties
                self._instance(row, imap)
                
            return
        elif isnew:
            result_list = h
        else:
            result_list = getattr(instance, self.key)
    
        self._instance(row, imap, result_list)

    def _instance(self, row, imap, result_list=None):
        """gets an instance from a row, via this EagerLoader's mapper."""
        fakerow = util.DictDecorator(row)
        for c in self.eagertarget.c:
            parent = self.target._get_col_by_original(c.original)
            fakerow[parent] = row[c]
        row = fakerow
        return self.mapper._instance(row, imap, result_list)

class GenericOption(MapperOption):
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
        if not prop.mapper.props.has_key(self.key):
            if prop.secondaryjoin is not None:
                # if setting up a backref to a many-to-many, reverse the order
                # of the "primary" and "secondary" joins
                pj = prop.secondaryjoin
                sj = prop.primaryjoin
            else:
                pj = prop.primaryjoin
                sj = None
            lazy = self.kwargs.pop('lazy', True)
            if lazy:
                cls = LazyLoader
            else:
                cls = EagerLoader
            relation = cls(prop.parent, prop.secondary, pj, sj, backref=prop.key, is_backref=True, **self.kwargs)
            prop.mapper.add_property(self.key, relation);
        else:
            # else set one of us as the "backreference"
            if not prop.mapper.props[self.key].is_backref:
                prop.is_backref=True
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
                clist.clauses[i] = self.get_alias(clist.clauses[i].table)._get_col_by_original(clist.clauses[i])
                if clist.clauses[i] is None:
                    raise "cant get orig for " + str(orig) + " against table " + orig.table.name + " " + self.get_alias(orig.table).name
    def visit_binary(self, binary):
        if isinstance(binary.left, schema.Column) and self.tables.has_key(binary.left.table):
            binary.left = self.get_alias(binary.left.table)._get_col_by_original(binary.left)
        if isinstance(binary.right, schema.Column) and self.tables.has_key(binary.right.table):
            binary.right = self.get_alias(binary.right.table)._get_col_by_original(binary.right)

class BinaryVisitor(sql.ClauseVisitor):
    def __init__(self, func):
        self.func = func
    def visit_binary(self, binary):
        self.func(binary)
