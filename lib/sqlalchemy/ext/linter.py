import collections
import itertools

from sqlalchemy import util

from sqlalchemy.sql import visitors
from sqlalchemy.sql.expression import Select


def before_execute_hook(conn, clauseelement, multiparams, params):
    if isinstance(clauseelement, Select):
        warn_for_cartesian(clauseelement)
    else:
        raise NotImplementedError


def find_unmatching_froms(element, start_with=None):
    # TODO: It would be nicer to use OrderedSet, but it seems to not be too much optimize, so let's skip for now
    froms = set(element.froms)
    if not froms:
        return
    edges = set()

    # find all "a <operator> b", add that as edges
    def visit_binary(binary_element):
        edges.update(
            itertools.product(
                binary_element.left._from_objects,
                binary_element.right._from_objects,
            )
        )

    # find all "a JOIN b", add "a" and "b" as froms
    def visit_join(join_element):
        if join_element in froms:
            froms.remove(join_element)
            froms.update((join_element.left, join_element.right))

    # unwrap "FromGrouping" objects, e.g. parentheized froms
    def visit_grouping(grouping_element):
        if grouping_element in froms:
            froms.remove(grouping_element)

            # the enclosed element will often be a JOIN.  The visitors.traverse
            # does a depth-first outside-in traversal so the next
            # call will be visit_join() of this element :)
            froms.add(grouping_element.element)

    visitors.traverse(
        element,
        {},
        {
            "binary": visit_binary,
            "join": visit_join,
            "grouping": visit_grouping,
        },
    )

    # take any element from the list of FROMS.
    # then traverse all the edges and ensure we can reach
    # all other FROMS
    if start_with is not None:
        assert start_with in froms
    else:
        start_with = next(iter(froms))
    froms.remove(start_with)
    the_rest = froms
    stack = collections.deque([start_with])
    while stack and the_rest:
        node = stack.popleft()
        # the_rest.pop(node, None)
        the_rest.discard(node)
        for edge in list(edges):
            if edge not in edges:
                continue
            elif edge[0] is node:
                edges.remove(edge)
                stack.appendleft(edge[1])
            elif edge[1] is node:
                edges.remove(edge)
                stack.appendleft(edge[0])

    # FROMS left over?  boom
    if the_rest:
        return the_rest, start_with
    else:
        return None, None

def warn_for_cartesian(element):
    froms, start_with = find_unmatching_froms(element)
    if froms:
        util.warn(
            'for stmt %s FROM elements %s are not joined up to FROM element "%r"'
            % (
                id(element),  # defeat the warnings filter
                ", ".join('"%r"' % f for f in froms),
                start_with,
            )
        )
