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

__all__ = ['sort', 'sort_with_cycles', 'sort_as_tree']

# TODO: obviate the need for a _Node class.
# a straight tuple should be used.
class _Node(tuple):
    """Represent each item in the sort."""
    
    def __new__(cls, item):
        children = []
        t = tuple.__new__(cls, [item, children])
        t.item = item
        t.children = children
        return t
    
    def __hash__(self):
        return id(self)
    
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

    def remove(self, edge):
        """Remove an edge from this collection.

        Return the childnode if it has no other parents.
        """

        (parentnode, childnode) = edge
        self.parent_to_children[parentnode].remove(childnode)
        self.child_to_parents[childnode].remove(parentnode)
        if not self.child_to_parents[childnode]:
            return childnode
        else:
            return None

    def has_parents(self, node):
        return node in self.child_to_parents and bool(self.child_to_parents[node])

    def edges_by_parent(self, node):
        if node in self.parent_to_children:
            return [(node, child) for child in self.parent_to_children[node]]
        else:
            return []

    def get_parents(self):
        return self.parent_to_children.keys()

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

    def __len__(self):
        return sum(len(x) for x in self.parent_to_children.values())

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
    nodes = {}
    edges = _EdgeCollection()

    for item in list(allitems) + [t[0] for t in tuples] + [t[1] for t in tuples]:
        item_id = id(item)
        if item_id not in nodes:
            nodes[item_id] = _Node(item)

    for t in tuples:
        id0, id1 = id(t[0]), id(t[1])
        if t[0] is t[1]:
            continue
        childnode = nodes[id1]
        parentnode = nodes[id0]
        edges.add((parentnode, childnode))

    queue = []
    for n in nodes.values():
        if not edges.has_parents(n):
            queue.append(n)

    output = []
    while nodes:
        if not queue:
            raise CircularDependencyError("Circular dependency detected " + 
                                repr(edges) + repr(queue))
        node = queue.pop()
        output.append(node.item)
        del nodes[id(node.item)]
        for childnode in edges.pop_node(node):
            queue.append(childnode)
    return output


def _find_cycles(edges):
    cycles = {}

    def traverse(node, cycle, goal):
        for (n, key) in edges.edges_by_parent(node):
            if key in cycle:
                continue
            cycle.add(key)
            if key is goal:
                cycset = set(cycle)
                for x in cycle:
                    if x in cycles:
                        existing_set = cycles[x]
                        existing_set.update(cycset)
                        for y in existing_set:
                            cycles[y] = existing_set
                        cycset = existing_set
                    else:
                        cycles[x] = cycset
            else:
                traverse(key, cycle, goal)
            cycle.pop()

    for parent in edges.get_parents():
        traverse(parent, set(), parent)

    unique_cycles = set(tuple(s) for s in cycles.values())
    
    for cycle in unique_cycles:
        edgecollection = [edge for edge in edges
                          if edge[0] in cycle and edge[1] in cycle]
        yield edgecollection
