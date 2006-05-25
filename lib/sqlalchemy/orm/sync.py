# mapper/sync.py
# Copyright (C) 2005,2006 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php



import sqlalchemy.sql as sql
import sqlalchemy.schema as schema
from sqlalchemy.exceptions import *

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

    def compile(self, sqlclause, source_tables, target_tables, issecondary=None):
        def check_for_table(binary, list1, list2):
            #print "check for table", str(binary), [str(c) for c in l]
            if binary.left.table in list1 and binary.right.table in list2:
                return (binary.left, binary.right)
            elif binary.right.table in list1 and binary.left.table in list2:
                return (binary.right, binary.left)
            else:
                return (None, None)
                
        def compile_binary(binary):
            """assembles a SyncRule given a single binary condition"""
            if binary.operator != '=' or not isinstance(binary.left, schema.Column) or not isinstance(binary.right, schema.Column):
                return

            if binary.left.table == binary.right.table:
                # self-cyclical relation
                if binary.left.primary_key:
                    source = binary.left
                    dest = binary.right
                elif binary.right.primary_key:
                    source = binary.right
                    dest = binary.left
                else:
                    raise ArgumentError("Cant determine direction for relationship %s = %s" % (binary.left.fullname, binary.right.fullname))
                if self.direction == ONETOMANY:
                    self.syncrules.append(SyncRule(self.parent_mapper, source, dest, dest_mapper=self.child_mapper))
                elif self.direction == MANYTOONE:
                    self.syncrules.append(SyncRule(self.child_mapper, source, dest, dest_mapper=self.parent_mapper))
                else:
                    raise AssertionError("assert failed")
            else:
                (pt, tt) = check_for_table(binary, source_tables, target_tables)
                #print "OK", binary, [t.name for t in source_tables], [t.name for t in target_tables]
                if pt and tt:
                    if self.direction == ONETOMANY:
                        self.syncrules.append(SyncRule(self.parent_mapper, pt, tt, dest_mapper=self.child_mapper))
                    elif self.direction == MANYTOONE:
                        self.syncrules.append(SyncRule(self.child_mapper, tt, pt, dest_mapper=self.parent_mapper))
                    else:
                        if not issecondary:
                            self.syncrules.append(SyncRule(self.parent_mapper, pt, tt, dest_mapper=self.child_mapper, issecondary=issecondary))
                        else:
                            self.syncrules.append(SyncRule(self.child_mapper, pt, tt, dest_mapper=self.parent_mapper, issecondary=issecondary))
                            
        rules_added = len(self.syncrules)
        processor = BinaryVisitor(compile_binary)
        sqlclause.accept_visitor(processor)
        if len(self.syncrules) == rules_added:
            raise ArgumentError("No syncrules generated for join criterion " + str(sqlclause))
        
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

    def execute(self, source, dest, obj, child, clearkeys):
        if source is None:
            if self.issecondary is False:
                source = obj
            elif self.issecondary is True:
                source = child
        if clearkeys or source is None:
            value = None
        else:
            value = self.source_mapper._getattrbycolumn(source, self.source_column)
        if isinstance(dest, dict):
            dest[self.dest_column.key] = value
        else:
            #print "SYNC VALUE", value, "TO", dest, self.source_column, self.dest_column
            self.dest_mapper._setattrbycolumn(dest, self.dest_column, value)
            
class BinaryVisitor(sql.ClauseVisitor):
    def __init__(self, func):
        self.func = func
    def visit_binary(self, binary):
        self.func(binary)

