# sqlalchemy/topological.py
# Copyright (C) 2005-2011 the SQLAlchemy authors and contributors <see AUTHORS file>
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
                    "Circular dependency detected",
                    find_cycles(tuples, allitems), 
                    _gen_edges(edges)
                )

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

def _gen_edges(edges):
    return set([
                    (right, left) 
                    for left in edges 
                    for right in edges[left] 
                ])
