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

# from sqlalchemy.ext.compiler import compiles
# @compiles(Select)
# def select_warn_for_cartesian_compiler(element, compiler, **kw):
#     warn_for_cartesian(element)
#     return compiler.visit_select(element, **kw)


def warn_for_cartesian(element):
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
    start_with = froms.pop()
    the_rest = froms
    stack = collections.deque([start_with])
    while stack and the_rest:
        node = stack.popleft()
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
        util.warn(
            'for stmt %s FROM elements %s are not joined up to FROM element "%r"'
            % (
                id(element),  # defeat the warnings filter
                ", ".join('"%r"' % f for f in the_rest),
                start_with,
            )
        )