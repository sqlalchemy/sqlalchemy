# orm/dependency.py
# Copyright (C) 2005, 2006, 2007, 2008, 2009, 2010 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Relationship dependencies.

Bridges the ``PropertyLoader`` (i.e. a ``relationship()``) and the
``UOWTransaction`` together to allow processing of relationship()-based
dependencies at flush time.

"""

from sqlalchemy import sql, util
import sqlalchemy.exceptions as sa_exc
from sqlalchemy.orm import attributes, exc, sync, unitofwork
from sqlalchemy.orm.interfaces import ONETOMANY, MANYTOONE, MANYTOMANY


def create_dependency_processor(prop):
    types = {
        ONETOMANY : OneToManyDP,
        MANYTOONE: ManyToOneDP,
        MANYTOMANY : ManyToManyDP,
    }
    return types[prop.direction](prop)

class DependencyProcessor(object):
    has_dependencies = True

    def __init__(self, prop):
        self.prop = prop
        self.cascade = prop.cascade
        self.mapper = prop.mapper
        self.parent = prop.parent
        self.secondary = prop.secondary
        self.direction = prop.direction
        self.post_update = prop.post_update
        self.passive_deletes = prop.passive_deletes
        self.passive_updates = prop.passive_updates
        self.enable_typechecks = prop.enable_typechecks
        self.key = prop.key
        self.dependency_marker = MapperStub(self.parent, self.mapper, self.key)
        if not self.prop.synchronize_pairs:
            raise sa_exc.ArgumentError("Can't build a DependencyProcessor for relationship %s.  "
                    "No target attributes to populate between parent and child are present" % self.prop)

    def _get_instrumented_attribute(self):
        """Return the ``InstrumentedAttribute`` handled by this
        ``DependencyProecssor``.
        
        """
        return self.parent.class_manager.get_impl(self.key)

    def hasparent(self, state):
        """return True if the given object instance has a parent,
        according to the ``InstrumentedAttribute`` handled by this ``DependencyProcessor``.
        
        """
        # TODO: use correct API for this
        return self._get_instrumented_attribute().hasparent(state)

    def per_property_flush_actions(self, uow):
        pass
        
    def presort_deletes(self, uowcommit, states):
        pass
        
    def presort_saves(self, uowcommit, states):
        pass
        
    def process_deletes(self, uowcommit, states):
        pass
        
    def process_saves(self, uowcommit, states):
        pass

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

    def _verify_canload(self, state):
        if state is not None and \
            not self.mapper._canload(state, allow_subtypes=not self.enable_typechecks):
            if self.mapper._canload(state, allow_subtypes=True):
                raise exc.FlushError(
                    "Attempting to flush an item of type %s on collection '%s', "
                    "which is not the expected type %s.  Configure mapper '%s' to "
                    "load this subtype polymorphically, or set "
                    "enable_typechecks=False to allow subtypes. "
                    "Mismatched typeloading may cause bi-directional relationships "
                    "(backrefs) to not function properly." % 
                    (state.class_, self.prop, self.mapper.class_, self.mapper))
            else:
                raise exc.FlushError(
                    "Attempting to flush an item of type %s on collection '%s', "
                    "whose mapper does not inherit from that of %s." % 
                    (state.class_, self.prop, self.mapper.class_))
            
    def _synchronize(self, state, child, associationrow, clearkeys, uowcommit):
        """Called during a flush to synchronize primary key identifier
        values between a parent/child object, as well as to an
        associationrow in the case of many-to-many.
        
        """
        raise NotImplementedError()

    def _check_reverse_action(self, uowcommit, parent, child, action):
        """Determine if an action has been performed by the 'reverse' property of this property.
        
        this is used to ensure that only one side of a bidirectional relationship
        issues a certain operation for a parent/child pair.
        
        """
        for r in self.prop._reverse_property:
            if not r.viewonly and (r._dependency_processor, action, parent, child) in uowcommit.attributes:
                return True
        return False
    
    def _performed_action(self, uowcommit, parent, child, action):
        """Establish that an action has been performed for a certain parent/child pair.
        
        Used only for actions that are sensitive to bidirectional double-action,
        i.e. manytomany, post_update.
        
        """
        uowcommit.attributes[(self, action, parent, child)] = True
        
    def _conditional_post_update(self, state, uowcommit, related):
        """Execute a post_update call.

        For relationships that contain the post_update flag, an additional
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
                if x is not None and not self._check_reverse_action(uowcommit, x, state, "postupdate"):
                    uowcommit.register_object(state, postupdate=True, 
                                    post_update_cols=[r for l, r in self.prop.synchronize_pairs])
                    self._performed_action(uowcommit, x, state, "postupdate")
                    break

    def _pks_changed(self, uowcommit, state):
        raise NotImplementedError()

    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, self.prop)

class OneToManyDP(DependencyProcessor):
    
    def per_property_flush_actions(self, uow):
        unitofwork.GetDependentObjects(uow, self, False, True)
        
        after_save = unitofwork.ProcessAll(uow, self, False, True)
        before_delete = unitofwork.ProcessAll(uow, self, True, True)
        
        parent_saves = unitofwork.SaveUpdateAll(uow, self.parent)
        child_saves = unitofwork.SaveUpdateAll(uow, self.mapper)
        
        parent_deletes = unitofwork.DeleteAll(uow, self.parent)
        child_deletes = unitofwork.DeleteAll(uow, self.mapper)

        if self.post_update:
            uow.dependencies.update([
                (child_saves, after_save),
                (parent_saves, after_save),
                (before_delete, parent_deletes),
                (before_delete, child_deletes),
            ])
        else:
            unitofwork.GetDependentObjects(uow, self, True, True)
            
            uow.dependencies.update([
                (parent_saves, after_save),
                (after_save, child_saves),
                
                (child_saves, parent_deletes),
                (before_delete, child_saves),
                
                (child_deletes, parent_deletes)
            ])
    
    def per_saved_state_flush_actions(self, uow, state):
        if True:
            parent_saves = unitofwork.SaveUpdateAll(uow, self.parent)
            child_saves = unitofwork.SaveUpdateAll(uow, self.mapper)
            assert parent_saves in uow.cycles
            assert child_saves in uow.cycles
        
        added, unchanged, deleted = uow.get_attribute_history(state, self.key, passive=True)
        if not added and not unchanged and not deleted:
            return
        
        save_parent = unitofwork.SaveUpdateState(uow, state)
        after_save = unitofwork.ProcessState(uow, self, False, state)

        for child_state in added + unchanged + deleted:
            if child_state is None:
                continue
            
            (deleted, listonly) = uow.states[child_state]
            if deleted:
                child_action = unitofwork.DeleteState(uow, child_state)
            else:
                child_action = unitofwork.SaveUpdateState(uow, child_state)
            
            uow.dependencies.update([
                (save_parent, after_save),
                (after_save, child_action),
            ])

    def per_deleted_state_flush_actions(self, uow, state):
        if True:
            parent_deletes = unitofwork.DeleteAll(uow, self.parent)
            child_deletes = unitofwork.DeleteAll(uow, self.mapper)
            assert parent_deletes in uow.cycles
            assert child_deletes in uow.cycles

        added, unchanged, deleted = uow.get_attribute_history(state, self.key, passive=True)
        if not added and not unchanged and not deleted:
            return

        delete_parent = unitofwork.DeleteState(uow, state)
        before_delete = unitofwork.ProcessState(uow, self, True, state)

        for child_state in added + unchanged + deleted:
            if child_state is None:
                continue

            (deleted, listonly) = uow.states[child_state]
            if deleted:
                child_action = unitofwork.DeleteState(uow, child_state)
            else:
                child_action = unitofwork.SaveUpdateState(uow, child_state)

            uow.dependencies.update([
                (child_action, before_delete),
                (before_delete, delete_parent),
            ])
        
        
    def presort_deletes(self, uowcommit, states):
        # head object is being deleted, and we manage its list of child objects
        # the child objects have to have their foreign key to the parent set to NULL
        should_null_fks = not self.cascade.delete and not self.passive_deletes == 'all'
        for state in states:
            history = uowcommit.get_attribute_history(
                                        state, self.key, passive=self.passive_deletes)
            if history:
                for child in history.deleted:
                    if child is not None and self.hasparent(child) is False:
                        if self.cascade.delete_orphan:
                            uowcommit.register_object(child, isdelete=True)
                        else:
                            uowcommit.register_object(child)
                if should_null_fks:
                    for child in history.unchanged:
                        if child is not None:
                            uowcommit.register_object(child)
    
    def presort_saves(self, uowcommit, states):
        for state in states:
            history = uowcommit.get_attribute_history(state, self.key, passive=True)
            if history:
                for child in history.added:
                    if child is not None:
                        uowcommit.register_object(child)
                for child in history.deleted:
                    if not self.cascade.delete_orphan:
                        uowcommit.register_object(child, isdelete=False)
                    elif self.hasparent(child) is False:
                        uowcommit.register_object(child, isdelete=True)
                        for c, m in self.mapper.cascade_iterator('delete', child):
                            uowcommit.register_object(
                                attributes.instance_state(c),
                                isdelete=True)
            if self._pks_changed(uowcommit, state):
                if not history:
                    history = uowcommit.get_attribute_history(
                                        state, self.key, passive=self.passive_updates)
                if history:
                    for child in history.unchanged:
                        if child is not None:
                            uowcommit.register_object(child)
    
    def process_deletes(self, uowcommit, states):
        # head object is being deleted, and we manage its list of child objects
        # the child objects have to have their foreign key to the parent set to NULL
        # this phase can be called safely for any cascade but is unnecessary if delete cascade
        # is on.
        if self.post_update or not self.passive_deletes == 'all':
            for state in states:
                history = uowcommit.get_attribute_history(state, self.key, passive=self.passive_deletes)
                if history:
                    for child in history.deleted:
                        if child is not None and self.hasparent(child) is False:
                            self._synchronize(state, child, None, True, uowcommit)
                            self._conditional_post_update(child, uowcommit, [state])
                    if self.post_update or not self.cascade.delete:
                        for child in history.unchanged:
                            if child is not None:
                                self._synchronize(state, child, None, True, uowcommit)
                                self._conditional_post_update(child, uowcommit, [state])
    
    def process_saves(self, uowcommit, states):
        for state in states:
            history = uowcommit.get_attribute_history(state, self.key, passive=True)
            if history:
                for child in history.added:
                    self._synchronize(state, child, None, False, uowcommit)
                    if child is not None:
                        self._conditional_post_update(child, uowcommit, [state])

                for child in history.deleted:
                    if not self.cascade.delete_orphan and not self.hasparent(child):
                        self._synchronize(state, child, None, True, uowcommit)

                if self._pks_changed(uowcommit, state):
                    for child in history.unchanged:
                        self._synchronize(state, child, None, False, uowcommit)
        
    def _synchronize(self, state, child, associationrow, clearkeys, uowcommit):
        source = state
        dest = child
        if dest is None or (not self.post_update and uowcommit.is_deleted(dest)):
            return
        self._verify_canload(child)
        if clearkeys:
            sync.clear(dest, self.mapper, self.prop.synchronize_pairs)
        else:
            sync.populate(source, self.parent, dest, self.mapper, 
                                    self.prop.synchronize_pairs, uowcommit,
                                    self.passive_updates)

    def _pks_changed(self, uowcommit, state):
        return sync.source_modified(uowcommit, state, self.parent, self.prop.synchronize_pairs)

class ManyToOneDP(DependencyProcessor):
    def __init__(self, prop):
        DependencyProcessor.__init__(self, prop)
        self._key_switch = DetectKeySwitch(prop)

    def per_property_flush_actions(self, uow):
        #self._key_switch.per_property_flush_actions(uow)
        
        after_save = unitofwork.ProcessAll(uow, self, False, True)
        before_delete = unitofwork.ProcessAll(uow, self, True, True)

        parent_saves = unitofwork.SaveUpdateAll(uow, self.parent)
        child_saves = unitofwork.SaveUpdateAll(uow, self.mapper)

        parent_deletes = unitofwork.DeleteAll(uow, self.parent)
        child_deletes = unitofwork.DeleteAll(uow, self.mapper)

        if self.post_update:
            uow.dependencies.update([
                (child_saves, after_save),
                (parent_saves, after_save),
                (before_delete, parent_deletes),
                (before_delete, child_deletes),
            ])
        else:
            unitofwork.GetDependentObjects(uow, self, False, True)
            unitofwork.GetDependentObjects(uow, self, True, True)
            
            uow.dependencies.update([
                (child_saves, after_save),
                (after_save, parent_saves),
                (parent_saves, child_deletes),
                (parent_deletes, child_deletes)
            ])

    def presort_deletes(self, uowcommit, states):
        if self.cascade.delete or self.cascade.delete_orphan:
            for state in states:
                history = uowcommit.get_attribute_history(state, self.key, passive=self.passive_deletes)
                if history:
                    if self.cascade.delete_orphan:
                        todelete = history.sum()
                    else:
                        todelete = history.non_deleted()
                    for child in todelete:
                        if child is None:
                            continue
                        uowcommit.register_object(child, isdelete=True)
                        for c, m in self.mapper.cascade_iterator('delete', child):
                            uowcommit.register_object(
                                attributes.instance_state(c), isdelete=True)

    def presort_saves(self, uowcommit, states):
        for state in states:
            uowcommit.register_object(state)
            if self.cascade.delete_orphan:
                history = uowcommit.get_attribute_history(state, self.key, passive=self.passive_deletes)
                if history:
                    for child in history.deleted:
                        if self.hasparent(child) is False:
                            uowcommit.register_object(child, isdelete=True)
                            for c, m in self.mapper.cascade_iterator('delete', child):
                                uowcommit.register_object(
                                    attributes.instance_state(c),
                                    isdelete=True)

    def process_deletes(self, uowcommit, states):
        if self.post_update and \
                not self.cascade.delete_orphan and \
                not self.passive_deletes == 'all':
            # post_update means we have to update our row to not reference the child object
            # before we can DELETE the row
            for state in states:
                self._synchronize(state, None, None, True, uowcommit)
                history = uowcommit.get_attribute_history(state, self.key, passive=self.passive_deletes)
                if history:
                    self._conditional_post_update(state, uowcommit, history.sum())

    def process_saves(self, uowcommit, states):
        for state in states:
            history = uowcommit.get_attribute_history(state, self.key, passive=True)
            if history:
                for child in history.added:
                    self._synchronize(state, child, None, False, uowcommit)
                
                self._conditional_post_update(state, uowcommit, history.sum())


    def _synchronize(self, state, child, associationrow, clearkeys, uowcommit):
        if state is None or (not self.post_update and uowcommit.is_deleted(state)):
            return

        if clearkeys or child is None:
            sync.clear(state, self.parent, self.prop.synchronize_pairs)
        else:
            self._verify_canload(child)
            sync.populate(child, self.mapper, state, 
                            self.parent, self.prop.synchronize_pairs, uowcommit,
                            self.passive_updates
                            )

class DetectKeySwitch(DependencyProcessor):
    """a special DP that works for many-to-one relationships, fires off for
    child items who have changed their referenced key."""

    has_dependencies = False

    def register_dependencies(self, uowcommit):
        pass

    def register_processors(self, uowcommit):
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
        switchers = set(s for s in deplist if self._pks_changed(uowcommit, s))
        if switchers:
            # yes, we're doing a linear search right now through the UOW.  only
            # takes effect when primary key values have actually changed.
            # a possible optimization might be to enhance the "hasparents" capability of
            # attributes to actually store all parent references, but this introduces
            # more complicated attribute accounting.
            for s in [elem for elem in uowcommit.session.identity_map.all_states()
                if issubclass(elem.class_, self.parent.class_) and
                    self.key in elem.dict and
                    elem.dict[self.key] is not None and 
                    attributes.instance_state(elem.dict[self.key]) in switchers
                ]:
                uowcommit.register_object(s)
                sync.populate(
                            attributes.instance_state(s.dict[self.key]), 
                            self.mapper, s, self.parent, self.prop.synchronize_pairs, 
                            uowcommit, self.passive_updates)

    def _pks_changed(self, uowcommit, state):
        return sync.source_modified(uowcommit, state, self.mapper, self.prop.synchronize_pairs)


class ManyToManyDP(DependencyProcessor):
    def register_dependencies(self, uowcommit):
        # many-to-many.  create a "Stub" mapper to represent the
        # "middle table" in the relationship.  This stub mapper doesnt save
        # or delete any objects, but just marks a dependency on the two
        # related mappers.  its dependency processor then populates the
        # association table.

        uowcommit.register_dependency(self.parent, self.dependency_marker)
        uowcommit.register_dependency(self.mapper, self.dependency_marker)

    def register_processors(self, uowcommit):
        uowcommit.register_processor(self.dependency_marker, self, self.parent)
        
    def process_dependencies(self, task, deplist, uowcommit, delete = False):
        connection = uowcommit.transaction.connection(self.mapper)
        secondary_delete = []
        secondary_insert = []
        secondary_update = []

        if delete:
            for state in deplist:
                history = uowcommit.get_attribute_history(state, self.key, passive=self.passive_deletes)
                if history:
                    for child in history.non_added():
                        if child is None or self._check_reverse_action(uowcommit, child, state, "manytomany"):
                            continue
                        associationrow = {}
                        self._synchronize(state, child, associationrow, False, uowcommit)
                        secondary_delete.append(associationrow)
                        self._performed_action(uowcommit, state, child, "manytomany")
        else:
            for state in deplist:
                history = uowcommit.get_attribute_history(state, self.key)
                if history:
                    for child in history.added:
                        if child is None or self._check_reverse_action(uowcommit, child, state, "manytomany"):
                            continue
                        associationrow = {}
                        self._synchronize(state, child, associationrow, False, uowcommit)
                        self._performed_action(uowcommit, state, child, "manytomany")
                        secondary_insert.append(associationrow)
                    for child in history.deleted:
                        if child is None or self._check_reverse_action(uowcommit, child, state, "manytomany"):
                            continue
                        associationrow = {}
                        self._synchronize(state, child, associationrow, False, uowcommit)
                        self._performed_action(uowcommit, state, child, "manytomany")
                        secondary_delete.append(associationrow)

                if not self.passive_updates and self._pks_changed(uowcommit, state):
                    if not history:
                        history = uowcommit.get_attribute_history(state, self.key, passive=False)
                    
                    for child in history.unchanged:
                        associationrow = {}
                        sync.update(state, self.parent, associationrow, "old_", self.prop.synchronize_pairs)
                        sync.update(child, self.mapper, associationrow, "old_", self.prop.secondary_synchronize_pairs)

                        #self.syncrules.update(associationrow, state, child, "old_")
                        secondary_update.append(associationrow)

        if secondary_delete:
            statement = self.secondary.delete(sql.and_(*[
                                c == sql.bindparam(c.key, type_=c.type) for c in self.secondary.c if c.key in associationrow
                            ]))
            result = connection.execute(statement, secondary_delete)
            if result.supports_sane_multi_rowcount() and result.rowcount != len(secondary_delete):
                raise exc.ConcurrentModificationError("Deleted rowcount %d does not match number of "
                            "secondary table rows deleted from table '%s': %d" % 
                            (result.rowcount, self.secondary.description, len(secondary_delete)))

        if secondary_update:
            statement = self.secondary.update(sql.and_(*[
                                c == sql.bindparam("old_" + c.key, type_=c.type) for c in self.secondary.c if c.key in associationrow
                            ]))
            result = connection.execute(statement, secondary_update)
            if result.supports_sane_multi_rowcount() and result.rowcount != len(secondary_update):
                raise exc.ConcurrentModificationError("Updated rowcount %d does not match number of "
                            "secondary table rows updated from table '%s': %d" % 
                            (result.rowcount, self.secondary.description, len(secondary_update)))

        if secondary_insert:
            statement = self.secondary.insert()
            connection.execute(statement, secondary_insert)

    def preprocess_dependencies(self, task, deplist, uowcommit, delete = False):
        if not delete:
            for state in deplist:
                history = uowcommit.get_attribute_history(state, self.key, passive=True)
                if history:
                    for child in history.deleted:
                        if self.cascade.delete_orphan and self.hasparent(child) is False:
                            uowcommit.register_object(child, isdelete=True)
                            for c, m in self.mapper.cascade_iterator('delete', child):
                                uowcommit.register_object(
                                    attributes.instance_state(c), isdelete=True)

    def _synchronize(self, state, child, associationrow, clearkeys, uowcommit):
        if associationrow is None:
            return
        self._verify_canload(child)
        
        sync.populate_dict(state, self.parent, associationrow, 
                                        self.prop.synchronize_pairs)
        sync.populate_dict(child, self.mapper, associationrow,
                                        self.prop.secondary_synchronize_pairs)

    def _pks_changed(self, uowcommit, state):
        return sync.source_modified(uowcommit, state, self.parent, self.prop.synchronize_pairs)

class MapperStub(object):
    """Represent a many-to-many dependency within a flush 
    context. 
     
    The UOWTransaction corresponds dependencies to mappers.   
    MapperStub takes the place of the "association table" 
    so that a depedendency can be corresponded to it.

    """
    
    def __init__(self, parent, mapper, key):
        self.mapper = mapper
        self.base_mapper = self
        self.class_ = mapper.class_
        self._inheriting_mappers = []

    def polymorphic_iterator(self):
        return iter((self,))

    def _register_dependencies(self, uowcommit):
        pass

    def _register_procesors(self, uowcommit):
        pass

    def _save_obj(self, *args, **kwargs):
        pass

    def _delete_obj(self, *args, **kwargs):
        pass

    def primary_mapper(self):
        return self
