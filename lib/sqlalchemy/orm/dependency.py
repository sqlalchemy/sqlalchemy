# orm/dependency.py
# Copyright (C) 2005,2006 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php


"""bridges the PropertyLoader (i.e. a relation()) and the UOWTransaction 
together to allow processing of scalar- and list-based dependencies at flush time."""

from sync import ONETOMANY,MANYTOONE,MANYTOMANY
from sqlalchemy import sql, util
import session as sessionlib

def create_dependency_processor(key, syncrules, cascade, secondary=None, association=None, is_backref=False, post_update=False):
    types = {
        ONETOMANY : OneToManyDP,
        MANYTOONE: ManyToOneDP,
        MANYTOMANY : ManyToManyDP,
    }
    if association is not None:
        return AssociationDP(key, syncrules, cascade, secondary, association, is_backref, post_update)
    else:
        return types[syncrules.direction](key, syncrules, cascade, secondary, association, is_backref, post_update)

class DependencyProcessor(object):
    def __init__(self, key, syncrules, cascade, secondary=None, association=None, is_backref=False, post_update=False):
        # TODO: update instance variable names to be more meaningful
        self.syncrules = syncrules
        self.cascade = cascade
        self.mapper = syncrules.child_mapper
        self.parent = syncrules.parent_mapper
        self.association = association
        self.secondary = secondary
        self.direction = syncrules.direction
        self.is_backref = is_backref
        self.post_update = post_update
        self.key = key

    def register_dependencies(self, uowcommit):
        """tells a UOWTransaction what mappers are dependent on which, with regards
        to the two or three mappers handled by this PropertyLoader.

        Also registers itself as a "processor" for one of its mappers, which
        will be executed after that mapper's objects have been saved or before
        they've been deleted.  The process operation manages attributes and dependent
        operations upon the objects of one of the involved mappers."""
        raise NotImplementedError()

    def whose_dependent_on_who(self, obj1, obj2):
        """given an object pair assuming obj2 is a child of obj1, returns a tuple
        with the dependent object second, or None if they are equal.  
        used by objectstore's object-level topological sort (i.e. cyclical 
        table dependency)."""
        if obj1 is obj2:
            return None
        elif self.direction == ONETOMANY:
            return (obj1, obj2)
        else:
            return (obj2, obj1)

    def process_dependencies(self, task, deplist, uowcommit, delete = False):
        """this method is called during a flush operation to synchronize data between a parent and child object.
        it is called within the context of the various mappers and sometimes individual objects sorted according to their
        insert/update/delete order (topological sort)."""
        raise NotImplementedError()

    # TODO: all of these preproc rules need to take dependencies into account
    def preprocess_dependencies(self, task, deplist, uowcommit, delete = False):
        """used before the flushes' topological sort to traverse through related objects and insure every 
        instance which will require save/update/delete is properly added to the UOWTransaction."""
        raise NotImplementedError()

    def _synchronize(self, obj, child, associationrow, clearkeys):
        """called during a flush to synchronize primary key identifier values between a parent/child object, as well as 
        to an associationrow in the case of many-to-many."""
        raise NotImplementedError()
        
    def get_object_dependencies(self, obj, uowcommit, passive = True):
        """returns the list of objects that are dependent on the given object, as according to the relationship
        this dependency processor represents"""
        return sessionlib.attribute_manager.get_history(obj, self.key, passive = passive)


class OneToManyDP(DependencyProcessor):
    def register_dependencies(self, uowcommit):
        if self.post_update:
            stub = MapperStub(self.parent, self.mapper, self.key)
            uowcommit.register_dependency(self.mapper, stub)
            uowcommit.register_dependency(self.parent, stub)
            uowcommit.register_processor(stub, self, self.parent)
        else:
            uowcommit.register_dependency(self.parent, self.mapper)
            uowcommit.register_processor(self.parent, self, self.parent)
    def process_dependencies(self, task, deplist, uowcommit, delete = False):
        #print self.mapper.mapped_table.name + " " + self.key + " " + repr(len(deplist)) + " process_dep isdelete " + repr(delete) + " direction " + repr(self.direction)
        if delete:
            # head object is being deleted, and we manage its list of child objects
            # the child objects have to have their foreign key to the parent set to NULL
            if not self.cascade.delete_orphan or self.post_update:
                for obj in deplist:
                    childlist = self.get_object_dependencies(obj, uowcommit, passive=False)
                    for child in childlist.deleted_items():
                        if child is not None and childlist.hasparent(child) is False:
                            self._synchronize(obj, child, None, True)
                            if self.post_update:
                                uowcommit.register_object(child, postupdate=True)
                    for child in childlist.unchanged_items():
                        if child is not None:
                            self._synchronize(obj, child, None, True)
                            if self.post_update:
                                uowcommit.register_object(child, postupdate=True)
        else:
            for obj in deplist:
                childlist = self.get_object_dependencies(obj, uowcommit, passive=True)
                if childlist is not None:
                    for child in childlist.added_items():
                        self._synchronize(obj, child, None, False)
                        if child is not None and self.post_update:
                            uowcommit.register_object(child, postupdate=True)
                    for child in childlist.deleted_items():
                        if not self.cascade.delete_orphan:
                            self._synchronize(obj, child, None, True)

    def preprocess_dependencies(self, task, deplist, uowcommit, delete = False):
        #print self.mapper.mapped_table.name + " " + self.key + " " + repr(len(deplist)) + " preprocess_dep isdelete " + repr(delete) + " direction " + repr(self.direction)

        if delete:
            # head object is being deleted, and we manage its list of child objects
            # the child objects have to have their foreign key to the parent set to NULL
            if self.post_update:
                # TODO: post_update instructions should be established in this step as well
                # (and executed in the regular traversal)
                pass
            elif self.cascade.delete_orphan:
                for obj in deplist:
                    childlist = self.get_object_dependencies(obj, uowcommit, passive=False)
                    for child in childlist.deleted_items():
                        if child is not None and childlist.hasparent(child) is False:
                            uowcommit.register_object(child, isdelete=True)
                            for c in self.mapper.cascade_iterator('delete', child):
                                uowcommit.register_object(c, isdelete=True)
                    for child in childlist.unchanged_items():
                        if child is not None:
                            uowcommit.register_object(child, isdelete=True)
                            for c in self.mapper.cascade_iterator('delete', child):
                                uowcommit.register_object(c, isdelete=True)
            else:
                for obj in deplist:
                    childlist = self.get_object_dependencies(obj, uowcommit, passive=False)
                    for child in childlist.deleted_items():
                        if child is not None and childlist.hasparent(child) is False:
                            uowcommit.register_object(child)
                    for child in childlist.unchanged_items():
                        if child is not None:
                            uowcommit.register_object(child)
        else:
            for obj in deplist:
                childlist = self.get_object_dependencies(obj, uowcommit, passive=True)
                if childlist is not None:
                    for child in childlist.added_items():
                        if child is not None:
                            uowcommit.register_object(child)
                    for child in childlist.deleted_items():
                        if not self.cascade.delete_orphan:
                            uowcommit.register_object(child, isdelete=False)
                        elif childlist.hasparent(child) is False:
                            uowcommit.register_object(child, isdelete=True)
                            for c in self.mapper.cascade_iterator('delete', child):
                                uowcommit.register_object(c, isdelete=True)
            
    def _synchronize(self, obj, child, associationrow, clearkeys):
        source = obj
        dest = child
        if dest is None:
            return
        self.syncrules.execute(source, dest, obj, child, clearkeys)
    
class ManyToOneDP(DependencyProcessor):
    def register_dependencies(self, uowcommit):
        if self.post_update:
            stub = MapperStub(self.parent, self.mapper, self.key)
            uowcommit.register_dependency(self.mapper, stub)
            uowcommit.register_dependency(self.parent, stub)
            uowcommit.register_processor(stub, self, self.parent)
        else:
            uowcommit.register_dependency(self.mapper, self.parent)
            uowcommit.register_processor(self.mapper, self, self.parent)
    def process_dependencies(self, task, deplist, uowcommit, delete = False):
        #print self.mapper.mapped_table.name + " " + self.key + " " + repr(len(deplist)) + " process_dep isdelete " + repr(delete) + " direction " + repr(self.direction)
        if delete:
            if self.post_update and not self.cascade.delete_orphan:
                # post_update means we have to update our row to not reference the child object
                # before we can DELETE the row
                for obj in deplist:
                    self._synchronize(obj, None, None, True)
                    uowcommit.register_object(obj, postupdate=True)
        else:
            for obj in deplist:
                childlist = self.get_object_dependencies(obj, uowcommit, passive=True)
                if childlist is not None:
                    for child in childlist.added_items():
                        self._synchronize(obj, child, None, False)
                if self.post_update:
                    uowcommit.register_object(obj, postupdate=True)
            
    def preprocess_dependencies(self, task, deplist, uowcommit, delete = False):
        #print self.mapper.mapped_table.name + " " + self.key + " " + repr(len(deplist)) + " PRE process_dep isdelete " + repr(delete) + " direction " + repr(self.direction)
        # TODO: post_update instructions should be established in this step as well
        # (and executed in the regular traversal)
        if self.post_update:
            return
        if delete:
            if self.cascade.delete:
                for obj in deplist:
                    childlist = self.get_object_dependencies(obj, uowcommit, passive=False)
                    for child in childlist.deleted_items() + childlist.unchanged_items():
                        if child is not None and childlist.hasparent(child) is False:
                            uowcommit.register_object(child, isdelete=True)
                            for c in self.mapper.cascade_iterator('delete', child):
                                uowcommit.register_object(c, isdelete=True)
        else:
            for obj in deplist:
                uowcommit.register_object(obj)
                if self.cascade.delete_orphan:
                    childlist = self.get_object_dependencies(obj, uowcommit, passive=False)
                    for child in childlist.deleted_items():
                        if childlist.hasparent(child) is False:
                            uowcommit.register_object(child, isdelete=True)
                            for c in self.mapper.cascade_iterator('delete', child):
                                uowcommit.register_object(c, isdelete=True)
                        
    def _synchronize(self, obj, child, associationrow, clearkeys):
        source = child
        dest = obj
        if dest is None:
            return
        self.syncrules.execute(source, dest, obj, child, clearkeys)

class ManyToManyDP(DependencyProcessor):
    def register_dependencies(self, uowcommit):
        # many-to-many.  create a "Stub" mapper to represent the
        # "middle table" in the relationship.  This stub mapper doesnt save
        # or delete any objects, but just marks a dependency on the two
        # related mappers.  its dependency processor then populates the
        # association table.

        if self.is_backref:
            # if we are the "backref" half of a two-way backref 
            # relationship, let the other mapper handle inserting the rows
            return
        stub = MapperStub(self.parent, self.mapper, self.key)
        uowcommit.register_dependency(self.parent, stub)
        uowcommit.register_dependency(self.mapper, stub)
        uowcommit.register_processor(stub, self, self.parent)
        
    def process_dependencies(self, task, deplist, uowcommit, delete = False):
        #print self.mapper.table.name + " " + self.key + " " + repr(len(deplist)) + " process_dep isdelete " + repr(delete) + " direction " + repr(self.direction)
        connection = uowcommit.transaction.connection(self.mapper)
        secondary_delete = []
        secondary_insert = []
        if delete:
            for obj in deplist:
                childlist = self.get_object_dependencies(obj, uowcommit, passive=False)
                for child in childlist.deleted_items() + childlist.unchanged_items():
                    associationrow = {}
                    self._synchronize(obj, child, associationrow, False)
                    secondary_delete.append(associationrow)
        else:
            for obj in deplist:
                childlist = self.get_object_dependencies(obj, uowcommit)
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
            secondary_delete.sort()
            # TODO: precompile the delete/insert queries and store them as instance variables
            # on the PropertyLoader
            statement = self.secondary.delete(sql.and_(*[c == sql.bindparam(c.key) for c in self.secondary.c if c.key in associationrow]))
            connection.execute(statement, secondary_delete)
        if len(secondary_insert):
            statement = self.secondary.insert()
            connection.execute(statement, secondary_insert)

    def preprocess_dependencies(self, task, deplist, uowcommit, delete = False):
        pass
    def _synchronize(self, obj, child, associationrow, clearkeys):
        dest = associationrow
        source = None
        if dest is None:
            return
        self.syncrules.execute(source, dest, obj, child, clearkeys)

class AssociationDP(OneToManyDP):
    def register_dependencies(self, uowcommit):
        # association object.  our mapper should be dependent on both
        # the parent mapper and the association object mapper.
        # this is where we put the "stub" as a marker, so we get
        # association/parent->stub->self, then we process the child
        # elments after the 'stub' save, which is before our own
        # mapper's save.
        stub = MapperStub(self.parent, self.association, self.key)
        uowcommit.register_dependency(self.parent, stub)
        uowcommit.register_dependency(self.association, stub)
        uowcommit.register_dependency(stub, self.mapper)
        uowcommit.register_processor(stub, self, self.parent)
    def process_dependencies(self, task, deplist, uowcommit, delete = False):
        #print self.mapper.table.name + " " + self.key + " " + repr(len(deplist)) + " process_dep isdelete " + repr(delete) + " direction " + repr(self.direction)
        for obj in deplist:
            childlist = self.get_object_dependencies(obj, uowcommit, passive=True)
            if childlist is None: continue

            # for the association mapper, the list of association objects is organized into a unique list based on the
            # "primary key".  newly added association items which correspond to existing association items are "merged"
            # into the existing one by moving the "_instance_key" over to the added item, so instead of insert/delete you
            # just get an update operation.
            if not delete:
                tosave = util.OrderedDict()
                for child in childlist:
                    self._synchronize(obj, child, None, False)
                    key = self.mapper.instance_key(child)
                    tosave[key] = child
                    uowcommit.unregister_object(child)

                todelete = {}
                for child in childlist.deleted_items():
                    self._synchronize(obj, child, None, False)
                    key = self.mapper.instance_key(child)
                    if not tosave.has_key(key):
                        todelete[key] = child
                    else:
                        tosave[key]._instance_key = key
                    uowcommit.unregister_object(child)
                
                for child in childlist.unchanged_items():
                    key = self.mapper.instance_key(child)
                    tosave[key]._instance_key = key
                    
                #print "OK for the save", [(o, getattr(o, '_instance_key', None)) for o in tosave.values()]
                #print "OK for the delete", [(o, getattr(o, '_instance_key', None)) for o in todelete.values()]
                
                for obj in tosave.values():
                    uowcommit.register_object(obj)
                for obj in todelete.values():
                    uowcommit.register_object(obj, isdelete=True)
            else:
                todelete = {}
                for child in childlist.unchanged_items() + childlist.deleted_items():
                    self._synchronize(obj, child, None, False)
                    key = self.mapper.instance_key(child)
                    todelete[key] = child
                for obj in todelete.values():
                    uowcommit.register_object(obj, isdelete=True)
                    
                
    def preprocess_dependencies(self, task, deplist, uowcommit, delete = False):
        # TODO: clean up the association step in process_dependencies and move the
        # appropriate sections of it to here
        pass
        

class MapperStub(object):
    """poses as a Mapper representing the association table in a many-to-many
    join, when performing a flush().  

    The Task objects in the objectstore module treat it just like
    any other Mapper, but in fact it only serves as a "dependency" placeholder
    for the many-to-many update task."""
    __metaclass__ = util.ArgSingleton
    def __init__(self, parent, mapper, key):
        self.mapper = mapper
        self._inheriting_mappers = []
    def register_dependencies(self, uowcommit):
        pass
    def save_obj(self, *args, **kwargs):
        pass
    def delete_obj(self, *args, **kwargs):
        pass
    def primary_mapper(self):
        return self
    def base_mapper(self):
        return self