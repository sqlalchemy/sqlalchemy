from collections import deque
from collections import namedtuple
import operator

from . import operators
from .visitors import ExtendedInternalTraversal
from .visitors import InternalTraversal
from .. import util
from ..inspection import inspect

SKIP_TRAVERSE = util.symbol("skip_traverse")
COMPARE_FAILED = False
COMPARE_SUCCEEDED = True
NO_CACHE = util.symbol("no_cache")
CACHE_IN_PLACE = util.symbol("cache_in_place")
CALL_GEN_CACHE_KEY = util.symbol("call_gen_cache_key")
STATIC_CACHE_KEY = util.symbol("static_cache_key")


def compare(obj1, obj2, **kw):
    if kw.get("use_proxies", False):
        strategy = ColIdentityComparatorStrategy()
    else:
        strategy = TraversalComparatorStrategy()

    return strategy.compare(obj1, obj2, **kw)


class HasCacheKey(object):
    _cache_key_traversal = NO_CACHE

    __slots__ = ()

    def _gen_cache_key(self, anon_map, bindparams):
        """return an optional cache key.

        The cache key is a tuple which can contain any series of
        objects that are hashable and also identifies
        this object uniquely within the presence of a larger SQL expression
        or statement, for the purposes of caching the resulting query.

        The cache key should be based on the SQL compiled structure that would
        ultimately be produced.   That is, two structures that are composed in
        exactly the same way should produce the same cache key; any difference
        in the structures that would affect the SQL string or the type handlers
        should result in a different cache key.

        If a structure cannot produce a useful cache key, it should raise
        NotImplementedError, which will result in the entire structure
        for which it's part of not being useful as a cache key.


        """

        idself = id(self)

        if anon_map is not None:
            if idself in anon_map:
                return (anon_map[idself], self.__class__)
            else:
                # inline of
                # id_ = anon_map[idself]
                anon_map[idself] = id_ = str(anon_map.index)
                anon_map.index += 1
        else:
            id_ = None

        _cache_key_traversal = self._cache_key_traversal
        if _cache_key_traversal is None:
            try:
                _cache_key_traversal = self._traverse_internals
            except AttributeError:
                _cache_key_traversal = NO_CACHE

        if _cache_key_traversal is NO_CACHE:
            if anon_map is not None:
                anon_map[NO_CACHE] = True
            return None

        result = (id_, self.__class__)

        # inline of _cache_key_traversal_visitor.run_generated_dispatch()
        try:
            dispatcher = self.__class__.__dict__[
                "_generated_cache_key_traversal"
            ]
        except KeyError:
            dispatcher = _cache_key_traversal_visitor.generate_dispatch(
                self, _cache_key_traversal, "_generated_cache_key_traversal"
            )

        for attrname, obj, meth in dispatcher(
            self, _cache_key_traversal_visitor
        ):
            if obj is not None:
                if meth is CACHE_IN_PLACE:
                    # cache in place is always going to be a Python
                    # tuple, dict, list, etc. so we can do a boolean check
                    if obj:
                        result += (attrname, obj)
                elif meth is STATIC_CACHE_KEY:
                    result += (attrname, obj._static_cache_key)
                elif meth is CALL_GEN_CACHE_KEY:
                    result += (
                        attrname,
                        obj._gen_cache_key(anon_map, bindparams),
                    )
                elif meth is InternalTraversal.dp_clauseelement_list:
                    if obj:
                        result += (
                            attrname,
                            tuple(
                                [
                                    elem._gen_cache_key(anon_map, bindparams)
                                    for elem in obj
                                ]
                            ),
                        )
                else:
                    # note that all the "ClauseElement" standalone cases
                    # here have been handled by inlines above; so we can
                    # safely assume the object is a standard list/tuple/dict
                    # which we can skip if it evaluates to false.
                    # improvement would be to have this as a flag delivered
                    # up front in the dispatcher list
                    if obj:
                        result += meth(
                            attrname, obj, self, anon_map, bindparams
                        )

        return result

    def _generate_cache_key(self):
        """return a cache key.

        The cache key is a tuple which can contain any series of
        objects that are hashable and also identifies
        this object uniquely within the presence of a larger SQL expression
        or statement, for the purposes of caching the resulting query.

        The cache key should be based on the SQL compiled structure that would
        ultimately be produced.   That is, two structures that are composed in
        exactly the same way should produce the same cache key; any difference
        in the structures that would affect the SQL string or the type handlers
        should result in a different cache key.

        The cache key returned by this method is an instance of
        :class:`.CacheKey`, which consists of a tuple representing the
        cache key, as well as a list of :class:`.BindParameter` objects
        which are extracted from the expression.   While two expressions
        that produce identical cache key tuples will themselves generate
        identical SQL strings, the list of :class:`.BindParameter` objects
        indicates the bound values which may have different values in
        each one; these bound parameters must be consulted in order to
        execute the statement with the correct parameters.

        a :class:`.ClauseElement` structure that does not implement
        a :meth:`._gen_cache_key` method and does not implement a
        :attr:`.traverse_internals` attribute will not be cacheable; when
        such an element is embedded into a larger structure, this method
        will return None, indicating no cache key is available.

        """
        bindparams = []

        _anon_map = anon_map()
        key = self._gen_cache_key(_anon_map, bindparams)
        if NO_CACHE in _anon_map:
            return None
        else:
            return CacheKey(key, bindparams)


class CacheKey(namedtuple("CacheKey", ["key", "bindparams"])):
    def __hash__(self):
        return hash(self.key)

    def __eq__(self, other):
        return self.key == other.key


def _clone(element, **kw):
    return element._clone()


class _CacheKey(ExtendedInternalTraversal):
    # very common elements are inlined into the main _get_cache_key() method
    # to produce a dramatic savings in Python function call overhead

    visit_has_cache_key = visit_clauseelement = CALL_GEN_CACHE_KEY
    visit_clauseelement_list = InternalTraversal.dp_clauseelement_list
    visit_string = (
        visit_boolean
    ) = visit_operator = visit_plain_obj = CACHE_IN_PLACE
    visit_statement_hint_list = CACHE_IN_PLACE
    visit_type = STATIC_CACHE_KEY

    def visit_inspectable(self, attrname, obj, parent, anon_map, bindparams):
        return self.visit_has_cache_key(
            attrname, inspect(obj), parent, anon_map, bindparams
        )

    def visit_multi(self, attrname, obj, parent, anon_map, bindparams):
        return (
            attrname,
            obj._gen_cache_key(anon_map, bindparams)
            if isinstance(obj, HasCacheKey)
            else obj,
        )

    def visit_multi_list(self, attrname, obj, parent, anon_map, bindparams):
        return (
            attrname,
            tuple(
                elem._gen_cache_key(anon_map, bindparams)
                if isinstance(elem, HasCacheKey)
                else elem
                for elem in obj
            ),
        )

    def visit_has_cache_key_tuples(
        self, attrname, obj, parent, anon_map, bindparams
    ):
        if not obj:
            return ()
        return (
            attrname,
            tuple(
                tuple(
                    elem._gen_cache_key(anon_map, bindparams)
                    for elem in tup_elem
                )
                for tup_elem in obj
            ),
        )

    def visit_has_cache_key_list(
        self, attrname, obj, parent, anon_map, bindparams
    ):
        if not obj:
            return ()
        return (
            attrname,
            tuple(elem._gen_cache_key(anon_map, bindparams) for elem in obj),
        )

    def visit_inspectable_list(
        self, attrname, obj, parent, anon_map, bindparams
    ):
        return self.visit_has_cache_key_list(
            attrname, [inspect(o) for o in obj], parent, anon_map, bindparams
        )

    def visit_clauseelement_tuples(
        self, attrname, obj, parent, anon_map, bindparams
    ):
        return self.visit_has_cache_key_tuples(
            attrname, obj, parent, anon_map, bindparams
        )

    def visit_anon_name(self, attrname, obj, parent, anon_map, bindparams):
        from . import elements

        name = obj
        if isinstance(name, elements._anonymous_label):
            name = name.apply_map(anon_map)

        return (attrname, name)

    def visit_fromclause_ordered_set(
        self, attrname, obj, parent, anon_map, bindparams
    ):
        if not obj:
            return ()
        return (
            attrname,
            tuple([elem._gen_cache_key(anon_map, bindparams) for elem in obj]),
        )

    def visit_clauseelement_unordered_set(
        self, attrname, obj, parent, anon_map, bindparams
    ):
        if not obj:
            return ()
        cache_keys = [
            elem._gen_cache_key(anon_map, bindparams) for elem in obj
        ]
        return (
            attrname,
            tuple(
                sorted(cache_keys)
            ),  # cache keys all start with (id_, class)
        )

    def visit_named_ddl_element(
        self, attrname, obj, parent, anon_map, bindparams
    ):
        return (attrname, obj.name)

    def visit_prefix_sequence(
        self, attrname, obj, parent, anon_map, bindparams
    ):
        if not obj:
            return ()
        return (
            attrname,
            tuple(
                [
                    (clause._gen_cache_key(anon_map, bindparams), strval)
                    for clause, strval in obj
                ]
            ),
        )

    def visit_table_hint_list(
        self, attrname, obj, parent, anon_map, bindparams
    ):
        if not obj:
            return ()

        return (
            attrname,
            tuple(
                [
                    (
                        clause._gen_cache_key(anon_map, bindparams),
                        dialect_name,
                        text,
                    )
                    for (clause, dialect_name), text in obj.items()
                ]
            ),
        )

    def visit_plain_dict(self, attrname, obj, parent, anon_map, bindparams):
        return (attrname, tuple([(key, obj[key]) for key in sorted(obj)]))

    def visit_string_clauseelement_dict(
        self, attrname, obj, parent, anon_map, bindparams
    ):
        return (
            attrname,
            tuple(
                (key, obj[key]._gen_cache_key(anon_map, bindparams))
                for key in sorted(obj)
            ),
        )

    def visit_string_multi_dict(
        self, attrname, obj, parent, anon_map, bindparams
    ):
        return (
            attrname,
            tuple(
                (
                    key,
                    value._gen_cache_key(anon_map, bindparams)
                    if isinstance(value, HasCacheKey)
                    else value,
                )
                for key, value in [(key, obj[key]) for key in sorted(obj)]
            ),
        )

    def visit_fromclause_canonical_column_collection(
        self, attrname, obj, parent, anon_map, bindparams
    ):
        return (
            attrname,
            tuple(col._gen_cache_key(anon_map, bindparams) for col in obj),
        )

    def visit_unknown_structure(
        self, attrname, obj, parent, anon_map, bindparams
    ):
        anon_map[NO_CACHE] = True
        return ()


_cache_key_traversal_visitor = _CacheKey()


class _CopyInternals(InternalTraversal):
    """Generate a _copy_internals internal traversal dispatch for classes
    with a _traverse_internals collection."""

    def visit_clauseelement(self, parent, element, clone=_clone, **kw):
        return clone(element, **kw)

    def visit_clauseelement_list(self, parent, element, clone=_clone, **kw):
        return [clone(clause, **kw) for clause in element]

    def visit_clauseelement_tuples(self, parent, element, clone=_clone, **kw):
        return [
            tuple(clone(tup_elem, **kw) for tup_elem in elem)
            for elem in element
        ]

    def visit_string_clauseelement_dict(
        self, parent, element, clone=_clone, **kw
    ):
        return dict(
            (key, clone(value, **kw)) for key, value in element.items()
        )


_copy_internals = _CopyInternals()


class _GetChildren(InternalTraversal):
    """Generate a _children_traversal internal traversal dispatch for classes
    with a _traverse_internals collection."""

    def visit_has_cache_key(self, element, **kw):
        return (element,)

    def visit_clauseelement(self, element, **kw):
        return (element,)

    def visit_clauseelement_list(self, element, **kw):
        return tuple(element)

    def visit_clauseelement_tuples(self, element, **kw):
        tup = ()
        for elem in element:
            tup += elem
        return tup

    def visit_fromclause_canonical_column_collection(self, element, **kw):
        if kw.get("column_collections", False):
            return tuple(element)
        else:
            return ()

    def visit_string_clauseelement_dict(self, element, **kw):
        return tuple(element.values())

    def visit_fromclause_ordered_set(self, element, **kw):
        return tuple(element)

    def visit_clauseelement_unordered_set(self, element, **kw):
        return tuple(element)


_get_children = _GetChildren()


@util.dependencies("sqlalchemy.sql.elements")
def _resolve_name_for_compare(elements, element, name, anon_map, **kw):
    if isinstance(name, elements._anonymous_label):
        name = name.apply_map(anon_map)

    return name


class anon_map(dict):
    """A map that creates new keys for missing key access.

    Produces an incrementing sequence given a series of unique keys.

    This is similar to the compiler prefix_anon_map class although simpler.

    Inlines the approach taken by :class:`sqlalchemy.util.PopulateDict` which
    is otherwise usually used for this type of operation.

    """

    def __init__(self):
        self.index = 0

    def __missing__(self, key):
        self[key] = val = str(self.index)
        self.index += 1
        return val


class TraversalComparatorStrategy(InternalTraversal, util.MemoizedSlots):
    __slots__ = "stack", "cache", "anon_map"

    def __init__(self):
        self.stack = deque()
        self.cache = set()

    def _memoized_attr_anon_map(self):
        return (anon_map(), anon_map())

    def compare(self, obj1, obj2, **kw):
        stack = self.stack
        cache = self.cache

        compare_annotations = kw.get("compare_annotations", False)

        stack.append((obj1, obj2))

        while stack:
            left, right = stack.popleft()

            if left is right:
                continue
            elif left is None or right is None:
                # we know they are different so no match
                return False
            elif (left, right) in cache:
                continue
            cache.add((left, right))

            visit_name = left.__visit_name__
            if visit_name != right.__visit_name__:
                return False

            meth = getattr(self, "compare_%s" % visit_name, None)

            if meth:
                attributes_compared = meth(left, right, **kw)
                if attributes_compared is COMPARE_FAILED:
                    return False
                elif attributes_compared is SKIP_TRAVERSE:
                    continue

                # attributes_compared is returned as a list of attribute
                # names that were "handled" by the comparison method above.
                # remaining attribute names in the _traverse_internals
                # will be compared.
            else:
                attributes_compared = ()

            for (
                (left_attrname, left_visit_sym),
                (right_attrname, right_visit_sym),
            ) in util.zip_longest(
                left._traverse_internals,
                right._traverse_internals,
                fillvalue=(None, None),
            ):
                if not compare_annotations and (
                    (left_attrname == "_annotations_cache_key")
                    or (right_attrname == "_annotations_cache_key")
                ):
                    continue

                if (
                    left_attrname != right_attrname
                    or left_visit_sym is not right_visit_sym
                ):
                    return False
                elif left_attrname in attributes_compared:
                    continue

                dispatch = self.dispatch(left_visit_sym)
                left_child = operator.attrgetter(left_attrname)(left)
                right_child = operator.attrgetter(right_attrname)(right)
                if left_child is None:
                    if right_child is not None:
                        return False
                    else:
                        continue

                comparison = dispatch(
                    left, left_child, right, right_child, **kw
                )
                if comparison is COMPARE_FAILED:
                    return False

        return True

    def compare_inner(self, obj1, obj2, **kw):
        comparator = self.__class__()
        return comparator.compare(obj1, obj2, **kw)

    def visit_has_cache_key(
        self, left_parent, left, right_parent, right, **kw
    ):
        if left._gen_cache_key(self.anon_map[0], []) != right._gen_cache_key(
            self.anon_map[1], []
        ):
            return COMPARE_FAILED

    def visit_clauseelement(
        self, left_parent, left, right_parent, right, **kw
    ):
        self.stack.append((left, right))

    def visit_fromclause_canonical_column_collection(
        self, left_parent, left, right_parent, right, **kw
    ):
        for lcol, rcol in util.zip_longest(left, right, fillvalue=None):
            self.stack.append((lcol, rcol))

    def visit_fromclause_derived_column_collection(
        self, left_parent, left, right_parent, right, **kw
    ):
        pass

    def visit_string_clauseelement_dict(
        self, left_parent, left, right_parent, right, **kw
    ):
        for lstr, rstr in util.zip_longest(
            sorted(left), sorted(right), fillvalue=None
        ):
            if lstr != rstr:
                return COMPARE_FAILED
            self.stack.append((left[lstr], right[rstr]))

    def visit_clauseelement_tuples(
        self, left_parent, left, right_parent, right, **kw
    ):
        for ltup, rtup in util.zip_longest(left, right, fillvalue=None):
            if ltup is None or rtup is None:
                return COMPARE_FAILED

            for l, r in util.zip_longest(ltup, rtup, fillvalue=None):
                self.stack.append((l, r))

    def visit_clauseelement_list(
        self, left_parent, left, right_parent, right, **kw
    ):
        for l, r in util.zip_longest(left, right, fillvalue=None):
            self.stack.append((l, r))

    def _compare_unordered_sequences(self, seq1, seq2, **kw):
        if seq1 is None:
            return seq2 is None

        completed = set()
        for clause in seq1:
            for other_clause in set(seq2).difference(completed):
                if self.compare_inner(clause, other_clause, **kw):
                    completed.add(other_clause)
                    break
        return len(completed) == len(seq1) == len(seq2)

    def visit_clauseelement_unordered_set(
        self, left_parent, left, right_parent, right, **kw
    ):
        return self._compare_unordered_sequences(left, right, **kw)

    def visit_fromclause_ordered_set(
        self, left_parent, left, right_parent, right, **kw
    ):
        for l, r in util.zip_longest(left, right, fillvalue=None):
            self.stack.append((l, r))

    def visit_string(self, left_parent, left, right_parent, right, **kw):
        return left == right

    def visit_anon_name(self, left_parent, left, right_parent, right, **kw):
        return _resolve_name_for_compare(
            left_parent, left, self.anon_map[0], **kw
        ) == _resolve_name_for_compare(
            right_parent, right, self.anon_map[1], **kw
        )

    def visit_boolean(self, left_parent, left, right_parent, right, **kw):
        return left == right

    def visit_operator(self, left_parent, left, right_parent, right, **kw):
        return left is right

    def visit_type(self, left_parent, left, right_parent, right, **kw):
        return left._compare_type_affinity(right)

    def visit_plain_dict(self, left_parent, left, right_parent, right, **kw):
        return left == right

    def visit_plain_obj(self, left_parent, left, right_parent, right, **kw):
        return left == right

    def visit_named_ddl_element(
        self, left_parent, left, right_parent, right, **kw
    ):
        if left is None:
            if right is not None:
                return COMPARE_FAILED

        return left.name == right.name

    def visit_prefix_sequence(
        self, left_parent, left, right_parent, right, **kw
    ):
        for (l_clause, l_str), (r_clause, r_str) in util.zip_longest(
            left, right, fillvalue=(None, None)
        ):
            if l_str != r_str:
                return COMPARE_FAILED
            else:
                self.stack.append((l_clause, r_clause))

    def visit_table_hint_list(
        self, left_parent, left, right_parent, right, **kw
    ):
        left_keys = sorted(left, key=lambda elem: (elem[0].fullname, elem[1]))
        right_keys = sorted(
            right, key=lambda elem: (elem[0].fullname, elem[1])
        )
        for (ltable, ldialect), (rtable, rdialect) in util.zip_longest(
            left_keys, right_keys, fillvalue=(None, None)
        ):
            if ldialect != rdialect:
                return COMPARE_FAILED
            elif left[(ltable, ldialect)] != right[(rtable, rdialect)]:
                return COMPARE_FAILED
            else:
                self.stack.append((ltable, rtable))

    def visit_statement_hint_list(
        self, left_parent, left, right_parent, right, **kw
    ):
        return left == right

    def visit_unknown_structure(
        self, left_parent, left, right_parent, right, **kw
    ):
        raise NotImplementedError()

    def compare_clauselist(self, left, right, **kw):
        if left.operator is right.operator:
            if operators.is_associative(left.operator):
                if self._compare_unordered_sequences(
                    left.clauses, right.clauses, **kw
                ):
                    return ["operator", "clauses"]
                else:
                    return COMPARE_FAILED
            else:
                return ["operator"]
        else:
            return COMPARE_FAILED

    def compare_binary(self, left, right, **kw):
        if left.operator == right.operator:
            if operators.is_commutative(left.operator):
                if (
                    compare(left.left, right.left, **kw)
                    and compare(left.right, right.right, **kw)
                ) or (
                    compare(left.left, right.right, **kw)
                    and compare(left.right, right.left, **kw)
                ):
                    return ["operator", "negate", "left", "right"]
                else:
                    return COMPARE_FAILED
            else:
                return ["operator", "negate"]
        else:
            return COMPARE_FAILED

    def compare_bindparam(self, left, right, **kw):
        compare_values = kw.pop("compare_values", True)
        if compare_values:
            return []
        else:
            # this means, "skip these, we already compared"
            return ["callable", "value"]


class ColIdentityComparatorStrategy(TraversalComparatorStrategy):
    def compare_column_element(
        self, left, right, use_proxies=True, equivalents=(), **kw
    ):
        """Compare ColumnElements using proxies and equivalent collections.

        This is a comparison strategy specific to the ORM.
        """

        to_compare = (right,)
        if equivalents and right in equivalents:
            to_compare = equivalents[right].union(to_compare)

        for oth in to_compare:
            if use_proxies and left.shares_lineage(oth):
                return SKIP_TRAVERSE
            elif hash(left) == hash(right):
                return SKIP_TRAVERSE
        else:
            return COMPARE_FAILED

    def compare_column(self, left, right, **kw):
        return self.compare_column_element(left, right, **kw)

    def compare_label(self, left, right, **kw):
        return self.compare_column_element(left, right, **kw)

    def compare_table(self, left, right, **kw):
        # tables compare on identity, since it's not really feasible to
        # compare them column by column with the above rules
        return SKIP_TRAVERSE if left is right else COMPARE_FAILED
