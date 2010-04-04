# topological.py
# Copyright (C) 2005, 2006, 2007, 2008, 2009, 2010 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Topological sorting algorithms.

The topological sort is an algorithm that receives this list of
dependencies as a *partial ordering*, that is a list of pairs which
might say, *X is dependent on Y*, *Q is dependent on Z*, but does not
necessarily tell you anything about Q being dependent on X. Therefore,
its not a straight sort where every element can be compared to
another... only some of the elements have any sorting preference, and
then only towards just some of the other elements.  For a particular
partial ordering, there can be many possible sorts that satisfy the
conditions.

"""

from sqlalchemy.exc import CircularDependencyError
from sqlalchemy import util

__all__ = ['sort']

class _EdgeCollection(object):
    """A collection of directed edges."""

    def __init__(self):
        self.parent_to_children = util.defaultdict(set)
        self.child_to_parents = util.defaultdict(set)

    def add(self, edge):
        """Add an edge to this collection."""

        parentnode, childnode = edge
        self.parent_to_children[parentnode].add(childnode)
        self.child_to_parents[childnode].add(parentnode)

    def has_parents(self, node):
        return node in self.child_to_parents and bool(self.child_to_parents[node])

    def edges_by_parent(self, node):
        if node in self.parent_to_children:
            return [(node, child) for child in self.parent_to_children[node]]
        else:
            return []
    
    def outgoing(self, node):
        """an iterable returning all nodes reached via node's outgoing edges"""
        
        return self.parent_to_children[node]
        
    def pop_node(self, node):
        """Remove all edges where the given node is a parent.

        Return the collection of all nodes which were children of the
        given node, and have no further parents.
        """

        children = self.parent_to_children.pop(node, None)
        if children is not None:
            for child in children:
                self.child_to_parents[child].remove(node)
                if not self.child_to_parents[child]:
                    yield child

    def __iter__(self):
        for parent, children in self.parent_to_children.iteritems():
            for child in children:
                yield (parent, child)

    def __repr__(self):
        return repr(list(self))

def sort(tuples, allitems):
    """sort the given list of items by dependency.

    'tuples' is a list of tuples representing a partial ordering.
    """

    edges = _EdgeCollection()
    nodes = set(allitems)

    for t in tuples:
        nodes.update(t)
        edges.add(t)

    queue = []
    for n in nodes:
        if not edges.has_parents(n):
            queue.append(n)

    output = []
    while nodes:
        if not queue:
            raise CircularDependencyError("Circular dependency detected: cycles: %r all edges: %r" % 
                                                    (find_cycles(tuples, allitems), edges))
        node = queue.pop()
        output.append(node)
        nodes.remove(node)
        for childnode in edges.pop_node(node):
            queue.append(childnode)
    return output

def find_cycles(tuples, allitems):
    # straight from gvr with some mods
    todo = set(allitems)
    edges = _EdgeCollection()

    for t in tuples:
        todo.update(t)
        edges.add(t)
    
    output = set()
    
    while todo:
        node = todo.pop()
        stack = [node]
        while stack:
            top = stack[-1]
            for node in edges.outgoing(top):
                if node in stack:
                    cyc = stack[stack.index(node):]
                    todo.difference_update(cyc)
                    output.update(cyc)
                    
                if node in todo:
                    stack.append(node)
                    todo.remove(node)
                    break
            else:
                node = stack.pop()
    return output
