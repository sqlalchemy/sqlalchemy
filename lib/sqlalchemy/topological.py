# topological.py
# Copyright (C) 2005, 2006, 2007, 2008, 2009, 2010 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Topological sorting algorithms."""

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
            
    def outgoing(self, node):
        return self.parent_to_children[node]
    
    def incoming(self, node):
        return self.child_to_parents[node]
        
    def __iter__(self):
        for parent in self.parent_to_children:
            for child in self.outgoing(parent):
                yield (parent, child)

    def __repr__(self):
        return repr(list(self))

def sort_as_subsets(tuples, allitems):
    output = set()

    todo = set(allitems)
    edges = _EdgeCollection(tuples)
    while todo:
        for node in list(todo):
            if not todo.intersection(edges.incoming(node)):
                output.add(node)

        if not output:
            raise CircularDependencyError(
                    "Circular dependency detected: cycles: %r all edges: %r" % 
                    (find_cycles(tuples, allitems), edges))

        todo.difference_update(output)
        yield output
        output = set()

def sort(tuples, allitems):
    """sort the given list of items by dependency.

    'tuples' is a list of tuples representing a partial ordering.
    """

    for set_ in sort_as_subsets(tuples, allitems):
        for s in set_:
            yield s

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

