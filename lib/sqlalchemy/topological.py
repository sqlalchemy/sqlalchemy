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
from sets import *
import sqlalchemy.util as util
from sqlalchemy.exceptions import *

class QueueDependencySorter(object):
    """topological sort adapted from wikipedia's article on the subject.  it creates a straight-line
    list of elements, then a second pass groups non-dependent actions together to build
    more of a tree structure with siblings."""
    class Node:
        """represents a node in a tree.  stores an 'item' which represents the 
        dependent thing we are talking about.  if node 'a' is an ancestor node of 
        node 'b', it means 'a's item is *not* dependent on that of 'b'."""
        def __init__(self, item):
            self.item = item
            self.edges = {}
            self.dependencies = {}
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
                
        def describe(self):
            return "%s" % (str(self.item))
        def __repr__(self):
            return self.describe()
        def is_dependent(self, child):
            if self.cycles is not None:
                for c in self.cycles:
                    if c.dependencies.has_key(child):
                        return True
            if child.cycles is not None:
                for c in child.cycles:
                    if self.dependencies.has_key(c):
                        return True
            return self.dependencies.has_key(child)
            
    def __init__(self, tuples, allitems):
        self.tuples = tuples
        self.allitems = allitems

    def _dump_edges(self, edges):
        s = StringIO.StringIO()
        for key, value in edges.iteritems():
            for c in value.keys():
                s.write("%s->%s\n" % (repr(key), repr(c)))
        return s.getvalue()
        
    def sort(self, allow_self_cycles=True, allow_all_cycles=False):
        (tuples, allitems) = (self.tuples, self.allitems)

        #print "\n---------------------------------\n"        
        #print repr([t for t in tuples])
        #print repr([a for a in allitems])
        #print "\n---------------------------------\n"        

        nodes = {}
        edges = {}
        for item in allitems + [t[0] for t in tuples] + [t[1] for t in tuples]:
            if not nodes.has_key(item):
                node = QueueDependencySorter.Node(item)
                nodes[item] = node
                edges[node] = {}
        
        for t in tuples:
            if t[0] is t[1]:
                if allow_self_cycles:
                    n = nodes[t[0]]
                    n.cycles = Set([n])
                    continue
                else:
                    raise FlushError("Self-referential dependency detected " + repr(t))
            childnode = nodes[t[1]]
            parentnode = nodes[t[0]]
            self._add_edge(edges, (parentnode, childnode))

        queue = []
        for n in nodes.values():
            if len(n.edges) == 0:
                queue.append(n)
        cycles = {}
        output = []
        while len(edges) > 0:
            #print self._dump_edges(edges)
            if len(queue) == 0:
                # edges remain but no edgeless nodes to remove; this indicates
                # a cycle
                if allow_all_cycles:
                    cycle = self._find_cycle(edges)
                    lead = cycle[0][0]
                    lead.cycles = Set()
                    for edge in cycle:
                        n = self._remove_edge(edges, edge)
                        lead.cycles.add(edge[0])
                        lead.cycles.add(edge[1])
                        if n is not None:
                            queue.append(n)
                            if n is not lead:
                                n._cyclical = True
                    # loop through cycle
                    # remove edges from the edge dictionary
                    # install the cycled nodes in the "cycle" list of one of the nodes
                    continue
                else:
                    # long cycles not allowed
                    raise FlushError("Circular dependency detected " + repr(edges) + repr(queue))
            node = queue.pop()
            if not hasattr(node, '_cyclical'):
                output.append(node)
            nodeedges = edges.pop(node, None)
            if nodeedges is None:
                continue
            for childnode in nodeedges.keys():
                del childnode.edges[node]
                if len(childnode.edges) == 0:
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
        
        
    def _add_edge(self, edges, edge):
        (parentnode, childnode) = edge
        edges[parentnode][childnode] = True
        parentnode.dependencies[childnode] = True
        childnode.edges[parentnode] = True

    def _remove_edge(self, edges, edge):
        (parentnode, childnode) = edge
        del edges[parentnode][childnode]
        del childnode.edges[parentnode]
        del parentnode.dependencies[childnode]
        if len(childnode.edges) == 0:
            return childnode
        
    def _find_cycle(self, edges):
        """given a structure of edges, locates a cycle in the strucure and returns 
        as a list of tuples representing edges involved in the cycle."""
        seen = Set()
        cycled_edges = []
        def traverse(d, parent=None):
            for key in d.keys():
                if not edges.has_key(key):
                    continue
                if key in seen:
                    if parent is not None:
                        cycled_edges.append((parent, key))
                    return key
                seen.add(key)
                x = traverse(edges[key], parent=key)
                if x is None:
                    seen.remove(key)
                else:
                    if parent is not None:
                        cycled_edges.append((parent, key))
                    return x
            else:
                return None
        s = traverse(edges)
        if s is None:
            return None
        else:
            return cycled_edges

