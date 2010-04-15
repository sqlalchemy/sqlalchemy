# topological.py
# Copyright (C) 2005, 2006, 2007, 2008, 2009, 2010 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Topological sorting algorithms."""

from sqlalchemy.exc import CircularDependencyError
from sqlalchemy import util

__all__ = ['sort', 'sort_as_subsets', 'find_cycles']

def sort_as_subsets(tuples, allitems):

    edges = util.defaultdict(set)
    for parent, child in tuples:
        edges[child].add(parent)
    
    todo = set(allitems)

    while todo:
        output = set()
        for node in list(todo):
            if not todo.intersection(edges[node]):
                output.add(node)

        if not output:
            raise CircularDependencyError(
                    "Circular dependency detected: cycles: %r all edges: %s" % 
                    (find_cycles(tuples, allitems), _dump_edges(edges, True)))

        todo.difference_update(output)
        yield output

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

    edges = util.defaultdict(set)
    for parent, child in tuples:
        edges[parent].add(child)

    output = set()
    
    while todo:
        node = todo.pop()
        stack = [node]
        while stack:
            top = stack[-1]
            for node in edges[top]:
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

def _dump_edges(edges, reverse):
    l = []
    for left in edges:
        for right in edges[left]:
            if reverse:
                l.append((right, left))
            else:
                l.append((left, right))
    return repr(l)


