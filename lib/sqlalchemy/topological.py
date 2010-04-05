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

    def __init__(self, edges):
        self.parent_to_children = util.defaultdict(set)
        self.child_to_parents = util.defaultdict(set)
        for parentnode, childnode in edges:
            self.parent_to_children[parentnode].add(childnode)
            self.child_to_parents[childnode].add(parentnode)
            
    def has_parents(self, node):
        return node in self.child_to_parents and bool(self.child_to_parents[node])

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

    edges = _EdgeCollection(tuples)
    nodes = set(allitems)

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
    edges = _EdgeCollection(tuples)

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


def organize_as_tree(tuples, allitems):
    """Given a list of sorted nodes from a topological sort, organize the
    nodes into a tree structure, with as many non-dependent nodes
    set as siblings to each other as possible.

    returns nodes as tuples (item, children).
    """

    nodes = allitems
    edges = _EdgeCollection(tuples)
    children = util.defaultdict(list)
    
    if not nodes:
        return None
    # a list of all currently independent subtrees as a tuple of
    # (root_node, set_of_all_tree_nodes, set_of_all_cycle_nodes_in_tree)
    # order of the list has no semantics for the algorithmic
    independents = []
    # in reverse topological order
    for node in reversed(nodes):
        # nodes subtree and cycles contain the node itself
        subtree = set([node])
        # get a set of dependent nodes of node and its cycles
        nodealldeps = edges.outgoing(node)
        if nodealldeps:
            # iterate over independent node indexes in reverse order so we can efficiently remove them
            for index in xrange(len(independents) - 1, -1, -1):
                child, childsubtree = independents[index]
                # if there is a dependency between this node and an independent node
                if (childsubtree.intersection(nodealldeps)):
                    # prepend child to nodes children
                    # (append should be fine, but previous implemetation used prepend)
                    children[node][0:0] = [(child, children[child])]
                    # merge childs subtree and cycles
                    subtree.update(childsubtree)
                    # remove the child from list of independent subtrees
                    independents[index:index+1] = []
        # add node as a new independent subtree
        independents.append((node, subtree))
    # choose an arbitrary node from list of all independent subtrees
    head = independents.pop()[0]
    # add all other independent subtrees as a child of the chosen root
    # used prepend [0:0] instead of extend to maintain exact behaviour of previous implementation
    children[head][0:0] = [(i[0], children[i[0]]) for i in independents]
    return (head, children[head])
