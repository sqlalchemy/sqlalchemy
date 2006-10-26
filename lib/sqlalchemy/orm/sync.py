# mapper/sync.py
# Copyright (C) 2005,2006 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php



from sqlalchemy import sql, schema, exceptions
from sqlalchemy import logging
from sqlalchemy.orm import util as mapperutil

"""contains the ClauseSynchronizer class, which is used to map attributes between two objects
in a manner corresponding to a SQL clause that compares column values."""

ONETOMANY = 0
MANYTOONE = 1
MANYTOMANY = 2

class ClauseSynchronizer(object):
    """Given a SQL clause, usually a series of one or more binary 
    expressions between columns, and a set of 'source' and 'destination' mappers, compiles a set of SyncRules
    corresponding to that information.  The ClauseSynchronizer can then be executed given a set of parent/child 
    objects or destination dictionary, which will iterate through each of its SyncRules and execute them.
    Each SyncRule will copy the value of a single attribute from the parent
    to the child, corresponding to the pair of columns in a particular binary expression, using the source and
    destination mappers to map those two columns to object attributes within parent and child."""
    def __init__(self, parent_mapper, child_mapper, direction):
        self.parent_mapper = parent_mapper
        self.child_mapper = child_mapper
        self.direction = direction
        self.syncrules = []

    def compile(self, sqlclause, issecondary=None, foreignkey=None):
        def compile_binary(binary):
            """assemble a SyncRule given a single binary condition"""
            if binary.operator != '=' or not isinstance(binary.left, schema.Column) or not isinstance(binary.right, schema.Column):
                return

            source_column = None
            dest_column = None
            if foreignkey is not None:
                # for self-referential relationships,
                # the best we can do right now is figure out which side
                # is the primary key
                # TODO: need some better way for this
                if binary.left.table == binary.right.table:
                    if binary.left.primary_key:
                        source_column = binary.left
                        dest_column = binary.right
                    elif binary.right.primary_key:
                        source_column = binary.right
                        dest_column = binary.left
                    else:
                        raise exceptions.ArgumentError("Can't locate a primary key column in self-referential equality clause '%s'" % str(binary))
                # for other relationships we are more flexible
                # and go off the 'foreignkey' property
                elif binary.left in foreignkey:
                    dest_column = binary.left
                    source_column = binary.right
                elif binary.right in foreignkey:
                    dest_column = binary.right
                    source_column = binary.left
                else:
                    return
            else:
                if binary.left in [f.column for f in binary.right.foreign_keys]:
                    dest_column = binary.right
                    source_column = binary.left
                elif binary.right in [f.column for f in binary.left.foreign_keys]:
                    dest_column = binary.left
                    source_column = binary.right
            
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
        processor = BinaryVisitor(compile_binary)
        sqlclause.accept_visitor(processor)
        if len(self.syncrules) == rules_added:
            raise exceptions.ArgumentError("No syncrules generated for join criterion " + str(sqlclause))
    
    def dest_columns(self):
        return [r.dest_column for r in self.syncrules if r.dest_column is not None]

    def execute(self, source, dest, obj=None, child=None, clearkeys=None):
        for rule in self.syncrules:
            rule.execute(source, dest, obj, child, clearkeys)
        
class SyncRule(object):
    """An instruction indicating how to populate the objects on each side of a relationship.  
    i.e. if table1 column A is joined against
    table2 column B, and we are a one-to-many from table1 to table2, a syncrule would say 
    'take the A attribute from object1 and assign it to the B attribute on object2'.  
    
    A rule contains the source mapper, the source column, destination column, 
    destination mapper in the case of a one/many relationship, and
    the integer direction of this mapper relative to the association in the case
    of a many to many relationship.
    """
    def __init__(self, source_mapper, source_column, dest_column, dest_mapper=None, issecondary=None):
        self.source_mapper = source_mapper
        self.source_column = source_column
        self.issecondary = issecondary
        self.dest_mapper = dest_mapper
        self.dest_column = dest_column
            
        #print "SyncRule", source_mapper, source_column, dest_column, dest_mapper
    def dest_primary_key(self):
        try:
            return self._dest_primary_key
        except AttributeError:
            self._dest_primary_key = self.dest_mapper is not None and self.dest_column in self.dest_mapper.pks_by_table[self.dest_column.table]
            return self._dest_primary_key
        
    def execute(self, source, dest, obj, child, clearkeys):
        if source is None:
            if self.issecondary is False:
                source = obj
            elif self.issecondary is True:
                source = child
        if clearkeys or source is None:
            value = None
        else:
            value = self.source_mapper.get_attr_by_column(source, self.source_column)
        if isinstance(dest, dict):
            dest[self.dest_column.key] = value
        else:
            if clearkeys and self.dest_primary_key():
                raise exceptions.AssertionError("Dependency rule tried to blank-out primary key column '%s' on instance '%s'" % (str(self.dest_column), mapperutil.instance_str(dest)))
                
            if logging.is_debug_enabled(self.logger):
                self.logger.debug("execute() instances: %s(%s)->%s(%s) ('%s')" % (mapperutil.instance_str(source), str(self.source_column), mapperutil.instance_str(dest), str(self.dest_column), value))
            self.dest_mapper.set_attr_by_column(dest, self.dest_column, value)

SyncRule.logger = logging.class_logger(SyncRule)
            
class BinaryVisitor(sql.ClauseVisitor):
    def __init__(self, func):
        self.func = func
    def visit_binary(self, binary):
        self.func(binary)

