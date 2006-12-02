# topological.py
# Copyright (C) 2005,2006 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""topological sorting algorithms.  the key to the unit of work is to assemble a list
of dependencies amongst all the different mappers that have been defined for classes.
Related tables with foreign key constraints have a definite insert order, deletion order, 
objects need dependent properties from parent objects set up before saved, etc.  
These are all encoded as dependencies, in the form "mapper X is dependent on mapper Y", 
meaning mapper Y's objects must be saved before those of mapper X, and mapper X's objects 
must be deleted before those of mapper Y.

The topological sort is an algorithm that receives this list of dependencies as a "partial
ordering", that is a list of pairs which might say, "X is dependent on Y", "Q is dependent
on Z", but does not necessarily tell you anything about Q being dependent on X. Therefore,
its not a straight sort where every element can be compared to another...only some of the
elements have any sorting preference, and then only towards just some of the other elements.
For a particular partial ordering, there can be many possible sorts that satisfy the
conditions.

An intrinsic "gotcha" to this algorithm is that since there are many possible outcomes
to sorting a partial ordering, the algorithm can return any number of different results for the
same input; just running it on a different machine architecture, or just random differences
in the ordering of dictionaries, can change the result that is returned.  While this result
is guaranteed to be true to the incoming partial ordering, if the partial ordering itself
does not properly represent the dependencies, code that works fine will suddenly break, then
work again, then break, etc.   Most of the bugs I've chased down while developing the "unit of work"
have been of this nature - very tricky to reproduce and track down, particularly before I 
realized this characteristic of the algorithm.
"""
import string, StringIO
from sqlalchemy import util
from sqlalchemy.exceptions import *

class _Node(object):
    """represents each item in the sort.  While the topological sort
    produces a straight ordered list of items, _Node ultimately stores a tree-structure
    of those items which are organized so that non-dependent nodes are siblings."""
    def __init__(self, item):
        self.item = item
        self.dependencies = util.Set()
        self.children = []
        self.cycles = None
    def __str__(self):
        return self.safestr()
    def safestr(self, indent=0):
        return (' ' * indent * 2) + \
            str(self.item) + \
            (self.cycles is not None and (" (cycles: " + repr([x for x in self.cycles]) + ")") or "") + \
            "\n" + \
            string.join([n.safestr(indent + 1) for n in self.children], '')
    def __repr__(self):
        return "%s" % (str(self.item))
    def is_dependent(self, child):
        if self.cycles is not None:
            for c in self.cycles:
                if child in c.dependencies:
                    return True
        if child.cycles is not None:
            for c in child.cycles:
                if c in self.dependencies:
                    return True
        return child in self.dependencies

class _EdgeCollection(object):
    """a collection of directed edges."""
    def __init__(self):
        self.parent_to_children = {}
        self.child_to_parents = {}
    def add(self, edge):
        """add an edge to this collection."""
        (parentnode, childnode) = edge
        if not self.parent_to_children.has_key(parentnode):
            self.parent_to_children[parentnode] = util.Set()
        self.parent_to_children[parentnode].add(childnode)
        if not self.child_to_parents.has_key(childnode):
            self.child_to_parents[childnode] = util.Set()
        self.child_to_parents[childnode].add(parentnode)
        parentnode.dependencies.add(childnode)
    def remove(self, edge):
        """remove an edge from this collection.  return the childnode if it has no other parents"""
        (parentnode, childnode) = edge
        self.parent_to_children[parentnode].remove(childnode)
        self.child_to_parents[childnode].remove(parentnode)
        if len(self.child_to_parents[childnode]) == 0:
            return childnode
        else:
            return None
    def has_parents(self, node):
        return self.child_to_parents.has_key(node) and len(self.child_to_parents[node]) > 0
    def edges_by_parent(self, node):
        if self.parent_to_children.has_key(node):
            return [(node, child) for child in self.parent_to_children[node]]
        else:
            return []
    def get_parents(self):
        return self.parent_to_children.keys()
    def pop_node(self, node):
        """remove all edges where the given node is a parent.  
        
        returns the collection of all nodes which were children of the given node, and have
        no further parents."""
        children = self.parent_to_children.pop(node, None)
        if children is not None:
            for child in children:
                self.child_to_parents[child].remove(node)
                if not len(self.child_to_parents[child]):
                    yield child
    def __len__(self):
        return sum([len(x) for x in self.parent_to_children.values()])
    def __iter__(self):
        for parent, children in self.parent_to_children.iteritems():
            for child in children:
                yield (parent, child)
    def __str__(self):
        return repr(list(self))
    def __repr__(self):
        return repr(list(self))
        
class QueueDependencySorter(object):
    """topological sort adapted from wikipedia's article on the subject.  it creates a straight-line
    list of elements, then a second pass groups non-dependent actions together to build
    more of a tree structure with siblings."""
            
    def __init__(self, tuples, allitems):
        self.tuples = tuples
        self.allitems = allitems

    def sort(self, allow_self_cycles=True, allow_all_cycles=False):
        (tuples, allitems) = (self.tuples, self.allitems)
        #print "\n---------------------------------\n"        
        #print repr([t for t in tuples])
        #print repr([a for a in allitems])
        #print "\n---------------------------------\n"        

        nodes = {}
        edges = _EdgeCollection()
        for item in allitems + [t[0] for t in tuples] + [t[1] for t in tuples]:
            if not nodes.has_key(item):
                node = _Node(item)
                nodes[item] = node
        
        for t in tuples:
            if t[0] is t[1]:
                if allow_self_cycles:
                    n = nodes[t[0]]
                    n.cycles = util.Set([n])
                    continue
                else:
                    raise FlushError("Self-referential dependency detected " + repr(t))
            childnode = nodes[t[1]]
            parentnode = nodes[t[0]]
            edges.add((parentnode, childnode))

        queue = []
        for n in nodes.values():
            if not edges.has_parents(n):
                queue.append(n)
        cycles = {}
        output = []
        while len(nodes) > 0:
            if len(queue) == 0:
                # edges remain but no edgeless nodes to remove; this indicates
                # a cycle
                if allow_all_cycles:
                    x = self._find_cycles(edges)
                    for cycle in self._find_cycles(edges):
                        lead = cycle[0][0]
                        lead.cycles = util.Set()
                        for edge in cycle:
                            n = edges.remove(edge)
                            lead.cycles.add(edge[0])
                            lead.cycles.add(edge[1])
                            if n is not None:
                                queue.append(n)
                        for n in lead.cycles:
                            if n is not lead:
                                n._cyclical = True
                                for (n,k) in list(edges.edges_by_parent(n)):
                                    edges.add((lead, k))
                                    edges.remove((n,k))
                    continue
                else:
                    # long cycles not allowed
                    raise FlushError("Circular dependency detected " + repr(edges) + repr(queue))
            node = queue.pop()
            if not hasattr(node, '_cyclical'):
                output.append(node)
            del nodes[node.item]
            for childnode in edges.pop_node(node):
                queue.append(childnode)
        return self._create_batched_tree(output)
        

    def _create_batched_tree(self, nodes):
        """given a list of nodes from a topological sort, organizes the nodes into a tree structure,
        with as many non-dependent nodes set as silbings to each other as possible."""
        def sort(index=None, l=None):
            if index is None:
                index = 0
            
            if index >= len(nodes):
                return None
            
            node = nodes[index]
            l2 = []
            sort(index + 1, l2)
            for n in l2:
                if l is None or search_dep(node, n):
                    node.children.append(n)
                else:
                    l.append(n)
            if l is not None:
                l.append(node)
            return node
            
        def search_dep(parent, child):
            if child is None:
                return False
            elif parent.is_dependent(child):
                return True
            else:
                for c in child.children:
                    x = search_dep(parent, c)
                    if x is True:
                        return True
                else:
                    return False
        return sort()
        
    def _find_cycles(self, edges):
        involved_in_cycles = util.Set()
        cycles = {}
        def traverse(node, goal=None, cycle=None):
            if goal is None:
                goal = node
                cycle = []
            elif node is goal:
                return True
                
            for (n, key) in edges.edges_by_parent(node):
                if key in cycle:
                    continue
                cycle.append(key)
                if traverse(key, goal, cycle):
                    cycset = util.Set(cycle)
                    for x in cycle:
                        involved_in_cycles.add(x)
                        if cycles.has_key(x):
                            existing_set = cycles[x]
                            [existing_set.add(y) for y in cycset]
                            for y in existing_set:
                                cycles[y] = existing_set
                            cycset = existing_set
                        else:
                            cycles[x] = cycset
                cycle.pop()
                    
        for parent in edges.get_parents():
            traverse(parent)

        for cycle in dict([(id(s), s) for s in cycles.values()]).values():
            edgecollection = []
            for edge in edges:
                if edge[0] in cycle and edge[1] in cycle:
                    edgecollection.append(edge)
            yield edgecollection
    
