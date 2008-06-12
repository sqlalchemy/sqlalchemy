# orm/dependency.py
# Copyright (C) 2005, 2006, 2007, 2008 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php


"""Bridge the ``PropertyLoader`` (i.e. a ``relation()``) and the
``UOWTransaction`` together to allow processing of relation()-based
 dependencies at flush time.
"""

from sqlalchemy.orm import sync
from sqlalchemy import sql, util, exceptions
from sqlalchemy.orm.interfaces import ONETOMANY, MANYTOONE, MANYTOMANY


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
    no_dependencies = False

    def __init__(self, prop):
        self.prop = prop
        self.cascade = prop.cascade
        self.mapper = prop.mapper
        self.parent = prop.parent
        self.secondary = prop.secondary
        self.direction = prop.direction
        self.is_backref = prop.is_backref
        self.post_update = prop.post_update
        self.foreign_keys = prop.foreign_keys
        self.passive_deletes = prop.passive_deletes
        self.passive_updates = prop.passive_updates
        self.enable_typechecks = prop.enable_typechecks
        self.key = prop.key
        if not self.prop.synchronize_pairs:
            raise exceptions.ArgumentError("Can't build a DependencyProcessor for relation %s.  No target attributes to populate between parent and child are present" % self.prop)

    def _get_instrumented_attribute(self):
        """Return the ``InstrumentedAttribute`` handled by this
        ``DependencyProecssor``.
        """

        return getattr(self.parent.class_, self.key)

    def hasparent(self, state):
        """return True if the given object instance has a parent,
        according to the ``InstrumentedAttribute`` handled by this ``DependencyProcessor``."""

        # TODO: use correct API for this
        return self._get_instrumented_attribute().impl.hasparent(state)

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

    def whose_dependent_on_who(self, state1, state2):
        """Given an object pair assuming `obj2` is a child of `obj1`,
        return a tuple with the dependent object second, or None if
        there is no dependency.
        """

        if state1 is state2:
            return None
        elif self.direction == ONETOMANY:
            return (state1, state2)
        else:
            return (state2, state1)

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

    def _verify_canload(self, state):
        if state is not None and not self.mapper._canload(state, allow_subtypes=not self.enable_typechecks):
            if self.mapper._canload(state, allow_subtypes=True):
                raise exceptions.FlushError("Attempting to flush an item of type %s on collection '%s', which is not the expected type %s.  Configure mapper '%s' to load this subtype polymorphically, or set enable_typechecks=False to allow subtypes.  Mismatched typeloading may cause bi-directional relationships (backrefs) to not function properly." % (state.class_, self.prop, self.mapper.class_, self.mapper))
            else:
                raise exceptions.FlushError("Attempting to flush an item of type %s on collection '%s', whose mapper does not inherit from that of %s." % (state.class_, self.prop, self.mapper.class_))
            
    def _synchronize(self, state, child, associationrow, clearkeys, uowcommit):
        """Called during a flush to synchronize primary key identifier
        values between a parent/child object, as well as to an
        associationrow in the case of many-to-many.
        """

        raise NotImplementedError()


    def _conditional_post_update(self, state, uowcommit, related):
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

        if state is not None and self.post_update:
            for x in related:
                if x is not None:
                    uowcommit.register_object(state, postupdate=True, post_update_cols=[r for l, r in self.prop.synchronize_pairs])
                    break

    def _pks_changed(self, uowcommit, state):
        raise NotImplementedError()

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
            if self.post_update or not self.passive_deletes=='all':
                for state in deplist:
                    (added, unchanged, deleted) = uowcommit.get_attribute_history(state, self.key,passive=self.passive_deletes)
                    if unchanged or deleted:
                        for child in deleted:
                            if child is not None and self.hasparent(child) is False:
                                self._synchronize(state, child, None, True, uowcommit)
                                self._conditional_post_update(child, uowcommit, [state])
                        if self.post_update or not self.cascade.delete:
                            for child in unchanged:
                                if child is not None:
                                    self._synchronize(state, child, None, True, uowcommit)
                                    self._conditional_post_update(child, uowcommit, [state])
        else:
            for state in deplist:
                (added, unchanged, deleted) = uowcommit.get_attribute_history(state, self.key, passive=True)
                if added or deleted:
                    for child in added:
                        self._synchronize(state, child, None, False, uowcommit)
                        if child is not None:
                            self._conditional_post_update(child, uowcommit, [state])
                    for child in deleted:
                        if not self.cascade.delete_orphan and not self.hasparent(child):
                            self._synchronize(state, child, None, True, uowcommit)

                if self._pks_changed(uowcommit, state):
                    if unchanged:
                        for child in unchanged:
                            self._synchronize(state, child, None, False, uowcommit)

    def preprocess_dependencies(self, task, deplist, uowcommit, delete = False):
        #print self.mapper.mapped_table.name + " " + self.key + " " + repr(len(deplist)) + " preprocess_dep isdelete " + repr(delete) + " direction " + repr(self.direction)

        if delete:
            # head object is being deleted, and we manage its list of child objects
            # the child objects have to have their foreign key to the parent set to NULL
            if not self.post_update:
                should_null_fks = not self.cascade.delete and not self.passive_deletes=='all'
                for state in deplist:
                    (added, unchanged, deleted) = uowcommit.get_attribute_history(state, self.key,passive=self.passive_deletes)
                    if unchanged or deleted:
                        for child in deleted:
                            if child is not None and self.hasparent(child) is False:
                                if self.cascade.delete_orphan:
                                    uowcommit.register_object(child, isdelete=True)
                                else:
                                    uowcommit.register_object(child)
                        if should_null_fks:
                            for child in unchanged:
                                if child is not None:
                                    uowcommit.register_object(child)
        else:
            for state in deplist:
                (added, unchanged, deleted) = uowcommit.get_attribute_history(state, self.key,passive=True)
                if added or deleted:
                    for child in added:
                        if child is not None:
                            uowcommit.register_object(child)
                    for child in deleted:
                        if not self.cascade.delete_orphan:
                            uowcommit.register_object(child, isdelete=False)
                        elif self.hasparent(child) is False:
                            uowcommit.register_object(child, isdelete=True)
                            for c, m in self.mapper.cascade_iterator('delete', child):
                                uowcommit.register_object(c._state, isdelete=True)
                if not self.passive_updates and self._pks_changed(uowcommit, state):
                    if not unchanged:
                        (added, unchanged, deleted) = uowcommit.get_attribute_history(state, self.key, passive=False)
                    if unchanged:
                        for child in unchanged:
                            uowcommit.register_object(child)

    def _synchronize(self, state, child, associationrow, clearkeys, uowcommit):
        source = state
        dest = child
        if dest is None or (not self.post_update and uowcommit.is_deleted(dest)):
            return
        self._verify_canload(child)
        if clearkeys:
            sync.clear(dest, self.mapper, self.prop.synchronize_pairs)
        else:
            sync.populate(source, self.parent, dest, self.mapper, self.prop.synchronize_pairs)

    def _pks_changed(self, uowcommit, state):
        return sync.source_changes(uowcommit, state, self.parent, self.prop.synchronize_pairs)

class DetectKeySwitch(DependencyProcessor):
    """a special DP that works for many-to-one relations, fires off for
    child items who have changed their referenced key."""

    no_dependencies = True

    def register_dependencies(self, uowcommit):
        uowcommit.register_processor(self.parent, self, self.mapper)

    def preprocess_dependencies(self, task, deplist, uowcommit, delete=False):
        # for non-passive updates, register in the preprocess stage
        # so that mapper save_obj() gets a hold of changes
        if not delete and not self.passive_updates:
            self._process_key_switches(deplist, uowcommit)

    def process_dependencies(self, task, deplist, uowcommit, delete=False):
        # for passive updates, register objects in the process stage
        # so that we avoid ManyToOneDP's registering the object without
        # the listonly flag in its own preprocess stage (results in UPDATE)
        # statements being emitted
        if not delete and self.passive_updates:
            self._process_key_switches(deplist, uowcommit)

    def _process_key_switches(self, deplist, uowcommit):
        switchers = util.Set([s for s in deplist if self._pks_changed(uowcommit, s)])
        if switchers:
            # yes, we're doing a linear search right now through the UOW.  only
            # takes effect when primary key values have actually changed.
            # a possible optimization might be to enhance the "hasparents" capability of
            # attributes to actually store all parent references, but this introduces
            # more complicated attribute accounting.
            for s in [elem for elem in uowcommit.session.identity_map.all_states()
                if issubclass(elem.class_, self.parent.class_) and
                    self.key in elem.dict and
                    elem.dict[self.key]._state in switchers
                ]:
                uowcommit.register_object(s, listonly=self.passive_updates)
                sync.populate(s.dict[self.key]._state, self.mapper, s, self.parent, self.prop.synchronize_pairs)
                #self.syncrules.execute(s.dict[self.key]._state, s, None, None, False)

    def _pks_changed(self, uowcommit, state):
        return sync.source_changes(uowcommit, state, self.mapper, self.prop.synchronize_pairs)

class ManyToOneDP(DependencyProcessor):
    def __init__(self, prop):
        DependencyProcessor.__init__(self, prop)
        self.mapper._dependency_processors.append(DetectKeySwitch(prop))

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
            if self.post_update and not self.cascade.delete_orphan and not self.passive_deletes=='all':
                # post_update means we have to update our row to not reference the child object
                # before we can DELETE the row
                for state in deplist:
                    self._synchronize(state, None, None, True, uowcommit)
                    (added, unchanged, deleted) = uowcommit.get_attribute_history(state, self.key,passive=self.passive_deletes)
                    if added or unchanged or deleted:
                        self._conditional_post_update(state, uowcommit, deleted + unchanged + added)
        else:
            for state in deplist:
                (added, unchanged, deleted) = uowcommit.get_attribute_history(state, self.key,passive=True)
                if added or deleted or unchanged:
                    for child in added:
                        self._synchronize(state, child, None, False, uowcommit)
                    self._conditional_post_update(state, uowcommit, deleted + unchanged + added)

    def preprocess_dependencies(self, task, deplist, uowcommit, delete = False):
        #print self.mapper.mapped_table.name + " " + self.key + " " + repr(len(deplist)) + " PRE process_dep isdelete " + repr(delete) + " direction " + repr(self.direction)
        if self.post_update:
            return
        if delete:
            if self.cascade.delete or self.cascade.delete_orphan:
                for state in deplist:
                    (added, unchanged, deleted) = uowcommit.get_attribute_history(state, self.key,passive=self.passive_deletes)
                    if self.cascade.delete_orphan:
                        todelete = added + unchanged + deleted
                    else:
                        todelete = added + unchanged
                    for child in todelete:
                        if child is None:
                            continue
                        uowcommit.register_object(child, isdelete=True)
                        for c, m in self.mapper.cascade_iterator('delete', child):
                            uowcommit.register_object(c._state, isdelete=True)
        else:
            for state in deplist:
                uowcommit.register_object(state)
                if self.cascade.delete_orphan:
                    (added, unchanged, deleted) = uowcommit.get_attribute_history(state, self.key,passive=self.passive_deletes)
                    if deleted:
                        for child in deleted:
                            if self.hasparent(child) is False:
                                uowcommit.register_object(child, isdelete=True)
                                for c, m in self.mapper.cascade_iterator('delete', child):
                                    uowcommit.register_object(c._state, isdelete=True)


    def _synchronize(self, state, child, associationrow, clearkeys, uowcommit):
        if state is None or (not self.post_update and uowcommit.is_deleted(state)):
            return

        if clearkeys or child is None:
            sync.clear(state, self.parent, self.prop.synchronize_pairs)
        else:
            self._verify_canload(child)
            sync.populate(child, self.mapper, state, self.parent, self.prop.synchronize_pairs)

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
        secondary_update = []

        if self.prop._reverse_property:
            reverse_dep = getattr(self.prop._reverse_property, '_dependency_processor', None)
        else:
            reverse_dep = None

        if delete:
            for state in deplist:
                (added, unchanged, deleted) = uowcommit.get_attribute_history(state, self.key,passive=self.passive_deletes)
                if deleted or unchanged:
                    for child in deleted + unchanged:
                        if child is None or (reverse_dep and (reverse_dep, "manytomany", child, state) in uowcommit.attributes):
                            continue
                        associationrow = {}
                        self._synchronize(state, child, associationrow, False, uowcommit)
                        secondary_delete.append(associationrow)
                        uowcommit.attributes[(self, "manytomany", state, child)] = True
        else:
            for state in deplist:
                (added, unchanged, deleted) = uowcommit.get_attribute_history(state, self.key)
                if added or deleted:
                    for child in added:
                        if child is None or (reverse_dep and (reverse_dep, "manytomany", child, state) in uowcommit.attributes):
                            continue
                        associationrow = {}
                        self._synchronize(state, child, associationrow, False, uowcommit)
                        uowcommit.attributes[(self, "manytomany", state, child)] = True
                        secondary_insert.append(associationrow)
                    for child in deleted:
                        if child is None or (reverse_dep and (reverse_dep, "manytomany", child, state) in uowcommit.attributes):
                            continue
                        associationrow = {}
                        self._synchronize(state, child, associationrow, False, uowcommit)
                        uowcommit.attributes[(self, "manytomany", state, child)] = True
                        secondary_delete.append(associationrow)

                if not self.passive_updates and unchanged and self._pks_changed(uowcommit, state):
                    for child in unchanged:
                        associationrow = {}
                        sync.update(state, self.parent, associationrow, "old_", self.prop.synchronize_pairs)
                        sync.update(child, self.mapper, associationrow, "old_", self.prop.secondary_synchronize_pairs)

                        #self.syncrules.update(associationrow, state, child, "old_")
                        secondary_update.append(associationrow)

        if secondary_delete:
            secondary_delete.sort()
            # TODO: precompile the delete/insert queries?
            statement = self.secondary.delete(sql.and_(*[c == sql.bindparam(c.key, type_=c.type) for c in self.secondary.c if c.key in associationrow]))
            result = connection.execute(statement, secondary_delete)
            if result.supports_sane_multi_rowcount() and result.rowcount != len(secondary_delete):
                raise exceptions.ConcurrentModificationError("Deleted rowcount %d does not match number of secondary table rows deleted from table '%s': %d" % (result.rowcount, self.secondary.description, len(secondary_delete)))

        if secondary_update:
            statement = self.secondary.update(sql.and_(*[c == sql.bindparam("old_" + c.key, type_=c.type) for c in self.secondary.c if c.key in associationrow]))
            result = connection.execute(statement, secondary_update)
            if result.supports_sane_multi_rowcount() and result.rowcount != len(secondary_update):
                raise exceptions.ConcurrentModificationError("Updated rowcount %d does not match number of secondary table rows updated from table '%s': %d" % (result.rowcount, self.secondary.description, len(secondary_update)))

        if secondary_insert:
            statement = self.secondary.insert()
            connection.execute(statement, secondary_insert)

    def preprocess_dependencies(self, task, deplist, uowcommit, delete = False):
        #print self.mapper.mapped_table.name + " " + self.key + " " + repr(len(deplist)) + " preprocess_dep isdelete " + repr(delete) + " direction " + repr(self.direction)
        if not delete:
            for state in deplist:
                (added, unchanged, deleted) = uowcommit.get_attribute_history(state, self.key,passive=True)
                if deleted:
                    for child in deleted:
                        if self.cascade.delete_orphan and self.hasparent(child) is False:
                            uowcommit.register_object(child, isdelete=True)
                            for c, m in self.mapper.cascade_iterator('delete', child):
                                uowcommit.register_object(c._state, isdelete=True)

    def _synchronize(self, state, child, associationrow, clearkeys, uowcommit):
        if associationrow is None:
            return
        self._verify_canload(child)
        
        sync.populate_dict(state, self.parent, associationrow, self.prop.synchronize_pairs)
        sync.populate_dict(child, self.mapper, associationrow, self.prop.secondary_synchronize_pairs)

    def _pks_changed(self, uowcommit, state):
        return sync.source_changes(uowcommit, state, self.parent, self.prop.synchronize_pairs)

class AssociationDP(OneToManyDP):
    def __init__(self, *args, **kwargs):
        super(AssociationDP, self).__init__(*args, **kwargs)
        self.cascade.delete = True
        self.cascade.delete_orphan = True

class MapperStub(object):
    """Pose as a Mapper representing the association table in a
    many-to-many join, when performing a ``flush()``.

    The ``Task`` objects in the objectstore module treat it just like
    any other ``Mapper``, but in fact it only serves as a dependency
    placeholder for the many-to-many update task.
    """

    __metaclass__ = util.ArgSingleton

    def __init__(self, parent, mapper, key):
        self.mapper = mapper
        self.base_mapper = self
        self.class_ = mapper.class_
        self._inheriting_mappers = []

    def polymorphic_iterator(self):
        return iter([self])

    def _register_dependencies(self, uowcommit):
        pass

    def _save_obj(self, *args, **kwargs):
        pass

    def _delete_obj(self, *args, **kwargs):
        pass

    def primary_mapper(self):
        return self
