# mapper/sync.py
# Copyright (C) 2005, 2006, 2007, 2008 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Contains the ClauseSynchronizer class, which is used to map
attributes between two objects in a manner corresponding to a SQL
clause that compares column values.
"""

from sqlalchemy import schema, exceptions, util
from sqlalchemy.sql import visitors, operators
from sqlalchemy import logging
from sqlalchemy.orm import util as mapperutil, attributes

ONETOMANY = 0
MANYTOONE = 1
MANYTOMANY = 2

class ClauseSynchronizer(object):
    """Given a SQL clause, usually a series of one or more binary
    expressions between columns, and a set of 'source' and
    'destination' mappers, compiles a set of SyncRules corresponding
    to that information.

    The ClauseSynchronizer can then be executed given a set of
    parent/child objects or destination dictionary, which will iterate
    through each of its SyncRules and execute them.  Each SyncRule
    will copy the value of a single attribute from the parent to the
    child, corresponding to the pair of columns in a particular binary
    expression, using the source and destination mappers to map those
    two columns to object attributes within parent and child.
    """

    def __init__(self, parent_mapper, child_mapper, direction):
        self.parent_mapper = parent_mapper
        self.child_mapper = child_mapper
        self.direction = direction
        self.syncrules = []

    def compile(self, sqlclause, foreign_keys=None, issecondary=None):
        def compile_binary(binary):
            """Assemble a SyncRule given a single binary condition."""

            if binary.operator != operators.eq or not isinstance(binary.left, schema.Column) or not isinstance(binary.right, schema.Column):
                return

            source_column = None
            dest_column = None

            if foreign_keys is None:
                if binary.left.table == binary.right.table:
                    raise exceptions.ArgumentError("need foreign_keys argument for self-referential sync")

                if binary.left in util.Set([f.column for f in binary.right.foreign_keys]):
                    dest_column = binary.right
                    source_column = binary.left
                elif binary.right in util.Set([f.column for f in binary.left.foreign_keys]):
                    dest_column = binary.left
                    source_column = binary.right
            else:
                if binary.left in foreign_keys:
                    source_column = binary.right
                    dest_column = binary.left
                elif binary.right in foreign_keys:
                    source_column = binary.left
                    dest_column = binary.right

            if source_column and dest_column:
                if self.direction == ONETOMANY:
                    self.syncrules.append(SyncRule(self.parent_mapper, source_column, dest_column, dest_mapper=self.child_mapper))
                elif self.direction == MANYTOONE:
                    self.syncrules.append(SyncRule(self.child_mapper, source_column, dest_column, dest_mapper=self.parent_mapper))
                else:
                    if not issecondary:
                        self.syncrules.append(SyncRule(self.parent_mapper, source_column, dest_column, dest_mapper=self.child_mapper, issecondary=issecondary))
                    else:
                        self.syncrules.append(SyncRule(self.child_mapper, source_column, dest_column, dest_mapper=self.parent_mapper, issecondary=issecondary))

        rules_added = len(self.syncrules)
        visitors.traverse(sqlclause, visit_binary=compile_binary)
        if len(self.syncrules) == rules_added:
            raise exceptions.ArgumentError("No syncrules generated for join criterion " + str(sqlclause))

    def dest_columns(self):
        return [r.dest_column for r in self.syncrules if r.dest_column is not None]

    def update(self, dest, parent, child, old_prefix):
        for rule in self.syncrules:
            rule.update(dest, parent, child, old_prefix)
        
    def execute(self, source, dest, obj=None, child=None, clearkeys=None):
        for rule in self.syncrules:
            rule.execute(source, dest, obj, child, clearkeys)
    
    def source_changes(self, uowcommit, source):
        for rule in self.syncrules:
            if rule.source_changes(uowcommit, source):
                return True
        else:
            return False
            
class SyncRule(object):
    """An instruction indicating how to populate the objects on each
    side of a relationship.

    E.g. if table1 column A is joined against table2 column
    B, and we are a one-to-many from table1 to table2, a syncrule
    would say *take the A attribute from object1 and assign it to the
    B attribute on object2*.
    """

    def __init__(self, source_mapper, source_column, dest_column, dest_mapper=None, issecondary=None):
        self.source_mapper = source_mapper
        self.source_column = source_column
        self.issecondary = issecondary
        self.dest_mapper = dest_mapper
        self.dest_column = dest_column

        #print "SyncRule", source_mapper, source_column, dest_column, dest_mapper

    def dest_primary_key(self):
        # late-evaluating boolean since some syncs are created
        # before the mapper has assembled pks
        try:
            return self._dest_primary_key
        except AttributeError:
            self._dest_primary_key = self.dest_mapper is not None and self.dest_column in self.dest_mapper._pks_by_table[self.dest_column.table] and not self.dest_mapper.allow_null_pks
            return self._dest_primary_key
    
    def source_changes(self, uowcommit, source):
        prop = self.source_mapper._columntoproperty[self.source_column]
        (added, unchanged, deleted) = uowcommit.get_attribute_history(source, prop.key, passive=True)
        return bool(added and deleted)
    
    def update(self, dest, parent, child, old_prefix):
        if self.issecondary is False:
            source = parent
        elif self.issecondary is True:
            source = child
        oldvalue = self.source_mapper._get_committed_attr_by_column(source.obj(), self.source_column)
        value = self.source_mapper._get_state_attr_by_column(source, self.source_column)
        dest[self.dest_column.key] = value
        dest[old_prefix + self.dest_column.key] = oldvalue
        
    def execute(self, source, dest, parent, child, clearkeys):
        # TODO: break the "dictionary" case into a separate method like 'update' above,
        # reduce conditionals
        if source is None:
            if self.issecondary is False:
                source = parent
            elif self.issecondary is True:
                source = child
        if clearkeys or source is None:
            value = None
            clearkeys = True
        else:
            value = self.source_mapper._get_state_attr_by_column(source, self.source_column)
        if isinstance(dest, dict):
            dest[self.dest_column.key] = value
        else:
            if clearkeys and self.dest_primary_key():
                raise exceptions.AssertionError("Dependency rule tried to blank-out primary key column '%s' on instance '%s'" % (str(self.dest_column), mapperutil.state_str(dest)))

            if logging.is_debug_enabled(self.logger):
                self.logger.debug("execute() instances: %s(%s)->%s(%s) ('%s')" % (mapperutil.state_str(source), str(self.source_column), mapperutil.state_str(dest), str(self.dest_column), value))
            self.dest_mapper._set_state_attr_by_column(dest, self.dest_column, value)

SyncRule.logger = logging.class_logger(SyncRule)

