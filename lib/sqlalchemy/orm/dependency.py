# orm/dependency.py
# Copyright (C) 2005, 2006, 2007 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php


"""Bridge the ``PropertyLoader`` (i.e. a ``relation()``) and the
``UOWTransaction`` together to allow processing of scalar- and
list-based dependencies at flush time.
"""

from sqlalchemy.orm import sync
from sqlalchemy.orm.sync import ONETOMANY,MANYTOONE,MANYTOMANY
from sqlalchemy import sql, util, exceptions
from sqlalchemy.orm import session as sessionlib

def create_dependency_processor(prop):
    types = {
        ONETOMANY : OneToManyDP,
        MANYTOONE: ManyToOneDP,
        MANYTOMANY : ManyToManyDP,
    }
    if prop.association is not None:
        return AssociationDP(prop)
    else:
        return types[prop.direction](prop)

class DependencyProcessor(object):
    def __init__(self, prop):
        self.prop = prop
        self.cascade = prop.cascade
        self.mapper = prop.mapper
        self.parent = prop.parent
        self.association = prop.association
        self.secondary = prop.secondary
        self.direction = prop.direction
        self.is_backref = prop.is_backref
        self.post_update = prop.post_update
        self.foreign_keys = prop.foreign_keys
        self.passive_deletes = prop.passive_deletes
        self.enable_typechecks = prop.enable_typechecks
        self.key = prop.key

        self._compile_synchronizers()

    def _get_instrumented_attribute(self):
        """Return the ``InstrumentedAttribute`` handled by this
        ``DependencyProecssor``.
        """

        return getattr(self.parent.class_, self.key)

    def register_dependencies(self, uowcommit):
        """Tell a ``UOWTransaction`` what mappers are dependent on
        which, with regards to the two or three mappers handled by
        this ``PropertyLoader``.

        Also register itself as a *processor* for one of its mappers,
        which will be executed after that mapper's objects have been
        saved or before they've been deleted.  The process operation
        manages attributes and dependent operations upon the objects
        of one of the involved mappers.
        """

        raise NotImplementedError()

    def whose_dependent_on_who(self, obj1, obj2):
        """Given an object pair assuming `obj2` is a child of `obj1`,
        return a tuple with the dependent object second, or None if
        they are equal.

        Used by objectstore's object-level topological sort (i.e. cyclical
        table dependency).
        """

        if obj1 is obj2:
            return None
        elif self.direction == ONETOMANY:
            return (obj1, obj2)
        else:
            return (obj2, obj1)

    def process_dependencies(self, task, deplist, uowcommit, delete = False):
        """This method is called during a flush operation to
        synchronize data between a parent and child object.

        It is called within the context of the various mappers and
        sometimes individual objects sorted according to their
        insert/update/delete order (topological sort).
        """

        raise NotImplementedError()

    def preprocess_dependencies(self, task, deplist, uowcommit, delete = False):
        """Used before the flushes' topological sort to traverse
        through related objects and ensure every instance which will
        require save/update/delete is properly added to the
        UOWTransaction.
        """

        raise NotImplementedError()

    def _verify_canload(self, child):
        if not self.enable_typechecks:
            return
        if child is not None and not self.mapper.canload(child):
            raise exceptions.FlushError("Attempting to flush an item of type %s on collection '%s', which is handled by mapper '%s' and does not load items of that type.  Did you mean to use a polymorphic mapper for this relationship ?  Set 'enable_typechecks=False' on the relation() to disable this exception.  Mismatched typeloading may cause bi-directional relationships (backrefs) to not function properly." % (child.__class__, self.prop, self.mapper))
        
    def _synchronize(self, obj, child, associationrow, clearkeys, uowcommit):
        """Called during a flush to synchronize primary key identifier
        values between a parent/child object, as well as to an
        associationrow in the case of many-to-many.
        """

        raise NotImplementedError()

    def _compile_synchronizers(self):
        """Assemble a list of *synchronization rules*, which are
        instructions on how to populate the objects on each side of a
        relationship.  This is done when a ``DependencyProcessor`` is
        first initialized.

        The list of rules is used within commits by the ``_synchronize()``
        method when dependent objects are processed.
        """

        self.syncrules = sync.ClauseSynchronizer(self.parent, self.mapper, self.direction)
        if self.direction == sync.MANYTOMANY:
            self.syncrules.compile(self.prop.primaryjoin, issecondary=False, foreign_keys=self.foreign_keys)
            self.syncrules.compile(self.prop.secondaryjoin, issecondary=True, foreign_keys=self.foreign_keys)
        else:
            self.syncrules.compile(self.prop.primaryjoin, foreign_keys=self.foreign_keys)

    def get_object_dependencies(self, obj, uowcommit, passive = True):
        """Return the list of objects that are dependent on the given
        object, as according to the relationship this dependency
        processor represents.
        """

        return sessionlib.attribute_manager.get_history(obj, self.key, passive = passive)

    def _conditional_post_update(self, obj, uowcommit, related):
        """Execute a post_update call.

        For relations that contain the post_update flag, an additional
        ``UPDATE`` statement may be associated after an ``INSERT`` or
        before a ``DELETE`` in order to resolve circular row
        dependencies.

        This method will check for the post_update flag being set on a
        particular relationship, and given a target object and list of
        one or more related objects, and execute the ``UPDATE`` if the
        given related object list contains ``INSERT``s or ``DELETE``s.
        """

        if obj is not None and self.post_update:
            for x in related:
                if x is not None:
                    uowcommit.register_object(obj, postupdate=True, post_update_cols=self.syncrules.dest_columns())
                    break

    def __str__(self):
        return "%s(%s)" % (self.__class__.__name__, str(self.prop))

class OneToManyDP(DependencyProcessor):
    def register_dependencies(self, uowcommit):
        if self.post_update:
            if not self.is_backref:
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
            # this phase can be called safely for any cascade but is unnecessary if delete cascade
            # is on.
            if not self.cascade.delete or self.post_update:
                for obj in deplist:
                    childlist = self.get_object_dependencies(obj, uowcommit, passive=self.passive_deletes)
                    if childlist is not None:
                        for child in childlist.deleted_items():
                            if child is not None and childlist.hasparent(child) is False:
                                self._synchronize(obj, child, None, True, uowcommit)
                                self._conditional_post_update(child, uowcommit, [obj])
                        for child in childlist.unchanged_items():
                            if child is not None:
                                self._synchronize(obj, child, None, True, uowcommit)
                                self._conditional_post_update(child, uowcommit, [obj])
        else:
            for obj in deplist:
                childlist = self.get_object_dependencies(obj, uowcommit, passive=True)
                if childlist is not None:
                    for child in childlist.added_items():
                        self._synchronize(obj, child, None, False, uowcommit)
                        self._conditional_post_update(child, uowcommit, [obj])
                    for child in childlist.deleted_items():
                        if not self.cascade.delete_orphan and not self._get_instrumented_attribute().hasparent(child):
                            self._synchronize(obj, child, None, True, uowcommit)

    def preprocess_dependencies(self, task, deplist, uowcommit, delete = False):
        #print self.mapper.mapped_table.name + " " + self.key + " " + repr(len(deplist)) + " preprocess_dep isdelete " + repr(delete) + " direction " + repr(self.direction)

        if delete:
            # head object is being deleted, and we manage its list of child objects
            # the child objects have to have their foreign key to the parent set to NULL
            if not self.post_update and not self.cascade.delete:
                for obj in deplist:
                    childlist = self.get_object_dependencies(obj, uowcommit, passive=self.passive_deletes)
                    if childlist is not None:
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

    def _synchronize(self, obj, child, associationrow, clearkeys, uowcommit):
        source = obj
        dest = child
        if dest is None or (not self.post_update and uowcommit.is_deleted(dest)):
            return
        self._verify_canload(child)
        self.syncrules.execute(source, dest, obj, child, clearkeys)

class ManyToOneDP(DependencyProcessor):
    def register_dependencies(self, uowcommit):
        if self.post_update:
            if not self.is_backref:
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
                    self._synchronize(obj, None, None, True, uowcommit)
                    childlist = self.get_object_dependencies(obj, uowcommit, passive=self.passive_deletes)
                    if childlist is not None:
                        self._conditional_post_update(obj, uowcommit, childlist.deleted_items() + childlist.unchanged_items() + childlist.added_items())
        else:
            for obj in deplist:
                childlist = self.get_object_dependencies(obj, uowcommit, passive=True)
                if childlist is not None:
                    for child in childlist.added_items():
                        self._synchronize(obj, child, None, False, uowcommit)
                    self._conditional_post_update(obj, uowcommit, childlist.deleted_items() + childlist.unchanged_items() + childlist.added_items())

    def preprocess_dependencies(self, task, deplist, uowcommit, delete = False):
        #print self.mapper.mapped_table.name + " " + self.key + " " + repr(len(deplist)) + " PRE process_dep isdelete " + repr(delete) + " direction " + repr(self.direction)
        if self.post_update:
            return
        if delete:
            if self.cascade.delete:
                for obj in deplist:
                    childlist = self.get_object_dependencies(obj, uowcommit, passive=self.passive_deletes)
                    if childlist is not None:
                        for child in childlist.deleted_items() + childlist.unchanged_items():
                            if child is not None and childlist.hasparent(child) is False:
                                uowcommit.register_object(child, isdelete=True)
                                for c in self.mapper.cascade_iterator('delete', child):
                                    uowcommit.register_object(c, isdelete=True)
        else:
            for obj in deplist:
                uowcommit.register_object(obj)
                if self.cascade.delete_orphan:
                    childlist = self.get_object_dependencies(obj, uowcommit, passive=self.passive_deletes)
                    if childlist is not None:
                        for child in childlist.deleted_items():
                            if childlist.hasparent(child) is False:
                                uowcommit.register_object(child, isdelete=True)
                                for c in self.mapper.cascade_iterator('delete', child):
                                    uowcommit.register_object(c, isdelete=True)

    def _synchronize(self, obj, child, associationrow, clearkeys, uowcommit):
        source = child
        dest = obj
        if dest is None or (not self.post_update and uowcommit.is_deleted(dest)):
            return
        self._verify_canload(child)
        self.syncrules.execute(source, dest, obj, child, clearkeys)

class ManyToManyDP(DependencyProcessor):
    def register_dependencies(self, uowcommit):
        # many-to-many.  create a "Stub" mapper to represent the
        # "middle table" in the relationship.  This stub mapper doesnt save
        # or delete any objects, but just marks a dependency on the two
        # related mappers.  its dependency processor then populates the
        # association table.

        stub = MapperStub(self.parent, self.mapper, self.key)
        uowcommit.register_dependency(self.parent, stub)
        uowcommit.register_dependency(self.mapper, stub)
        uowcommit.register_processor(stub, self, self.parent)

    def process_dependencies(self, task, deplist, uowcommit, delete = False):
        #print self.mapper.mapped_table.name + " " + self.key + " " + repr(len(deplist)) + " process_dep isdelete " + repr(delete) + " direction " + repr(self.direction)
        connection = uowcommit.transaction.connection(self.mapper)
        secondary_delete = []
        secondary_insert = []

        if hasattr(self.prop, 'reverse_property'):
            reverse_dep = getattr(self.prop.reverse_property, '_dependency_processor', None)
        else:
            reverse_dep = None
            
        if delete:
            for obj in deplist:
                childlist = self.get_object_dependencies(obj, uowcommit, passive=self.passive_deletes)
                if childlist is not None:
                    for child in childlist.deleted_items() + childlist.unchanged_items():
                        if child is None or (reverse_dep and (reverse_dep, "manytomany", child, obj) in uowcommit.attributes):
                            continue
                        associationrow = {}
                        self._synchronize(obj, child, associationrow, False, uowcommit)
                        secondary_delete.append(associationrow)
                        uowcommit.attributes[(self, "manytomany", obj, child)] = True
        else:
            for obj in deplist:
                childlist = self.get_object_dependencies(obj, uowcommit)
                if childlist is None: continue
                for child in childlist.added_items():
                    if child is None or (reverse_dep and (reverse_dep, "manytomany", child, obj) in uowcommit.attributes):
                        continue
                    associationrow = {}
                    self._synchronize(obj, child, associationrow, False, uowcommit)
                    uowcommit.attributes[(self, "manytomany", obj, child)] = True
                    secondary_insert.append(associationrow)
                for child in childlist.deleted_items():
                    if child is None or (reverse_dep and (reverse_dep, "manytomany", child, obj) in uowcommit.attributes):
                        continue
                    associationrow = {}
                    self._synchronize(obj, child, associationrow, False, uowcommit)
                    uowcommit.attributes[(self, "manytomany", obj, child)] = True
                    secondary_delete.append(associationrow)

        if len(secondary_delete):
            secondary_delete.sort()
            # TODO: precompile the delete/insert queries?
            statement = self.secondary.delete(sql.and_(*[c == sql.bindparam(c.key, type=c.type) for c in self.secondary.c if c.key in associationrow]))
            result = connection.execute(statement, secondary_delete)
            if result.supports_sane_rowcount() and result.rowcount != len(secondary_delete):
                raise exceptions.ConcurrentModificationError("Deleted rowcount %d does not match number of objects deleted %d" % (result.rowcount, len(secondary_delete)))

        if len(secondary_insert):
            statement = self.secondary.insert()
            connection.execute(statement, secondary_insert)

    def preprocess_dependencies(self, task, deplist, uowcommit, delete = False):
        #print self.mapper.mapped_table.name + " " + self.key + " " + repr(len(deplist)) + " preprocess_dep isdelete " + repr(delete) + " direction " + repr(self.direction)
        if not delete:
            for obj in deplist:
                childlist = self.get_object_dependencies(obj, uowcommit, passive=True)
                if childlist is not None:
                    for child in childlist.deleted_items():
                        if self.cascade.delete_orphan and childlist.hasparent(child) is False:
                            uowcommit.register_object(child, isdelete=True)
                            for c in self.mapper.cascade_iterator('delete', child):
                                uowcommit.register_object(c, isdelete=True)

    def _synchronize(self, obj, child, associationrow, clearkeys, uowcommit):
        dest = associationrow
        source = None
        if dest is None:
            return
        self._verify_canload(child)
        self.syncrules.execute(source, dest, obj, child, clearkeys)

class AssociationDP(OneToManyDP):
    def __init__(self, *args, **kwargs):
        super(AssociationDP, self).__init__(*args, **kwargs)
        self.cascade.delete = True
        self.cascade.delete_orphan = True

class MapperStub(object):
    """Pose as a Mapper representing the association table in a
    many-to-many join, when performing a ``flush()``.

    The ``Task`` objects in the objectstore module treat it just like
    any other ``Mapper``, but in fact it only serves as a *dependency*
    placeholder for the many-to-many update task.
    """

    __metaclass__ = util.ArgSingleton

    def __init__(self, parent, mapper, key):
        self.mapper = mapper
        self.class_ = mapper.class_
        self._inheriting_mappers = []

    def polymorphic_iterator(self):
        return iter([self])
        
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
