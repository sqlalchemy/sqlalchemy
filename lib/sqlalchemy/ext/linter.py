import collections
import itertools

from .. import util
from ..sql import visitors
from ..sql.expression import Select


def _indent(text, indent):
    return "\n".join(indent + line for line in text.split("\n"))


def before_execute_hook(conn, clauseelement, multiparams, params):
    if isinstance(clauseelement, Select):
        lint(clauseelement)


def find_unmatching_froms(query, start_with=None):
    # type: (Select, Optional[FromClause]) -> Tuple[Set[FromClause], FromClause]  # noqa
    # TODO: It would be nicer to use OrderedSet, but it seems to not
    #  be too much optimized, so let's skip for now
    froms = set(query.froms)
    if not froms:
        return None, None
    edges = set()

    # find all "a <operator> b", add that as edges
    def visit_binary(binary_element):
        # type: (BinaryExpression) -> None
        edges.update(
            itertools.product(
                binary_element.left._from_objects,
                binary_element.right._from_objects,
            )
        )

    # find all "a JOIN b", add "a" and "b" as froms
    def visit_join(join_element):
        # type: (Join) -> None
        if join_element in froms:
            froms.remove(join_element)
            froms.update((join_element.left, join_element.right))

    # unwrap "FromGrouping" objects, e.g. parentheized froms
    def visit_grouping(grouping_element):
        # type: (FromGrouping) -> None
        if grouping_element in froms:
            froms.remove(grouping_element)

            # the enclosed element will often be a JOIN.  The visitors.traverse
            # does a depth-first outside-in traversal so the next
            # call will be visit_join() of this element :)
            froms.add(grouping_element.element)

    visitors.traverse(
        query,
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


def warn_for_unmatching_froms(query):
    # type: (Select) -> None
    froms, start_with = find_unmatching_froms(query)
    if froms:
        template = 'Query\n{query}\nhas FROM elements:\n{froms}\nthat ' \
                   'are not joined up to FROM element\n{start}'''
        indent = '    '
        froms_str = '\n'.join('* {elem}'.format(elem=from_) for from_ in froms)
        message = template.format(
            query=_indent(str(query), indent),
            froms=_indent(froms_str, indent),
            start=_indent('* {elem}'.format(elem=start_with), indent),
        )
        util.warn(message)


def lint(query):
    # type: (Select) -> None
    warn_for_unmatching_froms(query)
