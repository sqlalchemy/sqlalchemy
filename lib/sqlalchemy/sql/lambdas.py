# sql/lambdas.py
# Copyright (C) 2005-2019 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

import itertools
import operator
import sys
import weakref

from . import coercions
from . import elements
from . import roles
from . import schema
from . import traversals
from . import type_api
from . import visitors
from .operators import ColumnOperators
from .. import exc
from .. import inspection
from .. import util
from ..util import collections_abc

_trackers = weakref.WeakKeyDictionary()


_TRACKERS = 0
_STALE_CHECK = 1
_REAL_FN = 2
_EXPR = 3
_IS_SEQUENCE = 4
_PROPAGATE_ATTRS = 5


def lambda_stmt(lmb):
    """Produce a SQL statement that is cached as a lambda.

    This SQL statement will only be constructed if element has not been
    compiled yet.    The approach is used to save on Python function overhead
    when constructing statements that will be cached.

    E.g.::

        from sqlalchemy import lambda_stmt

        stmt = lambda_stmt(lambda: table.select())
        stmt += lambda s: s.where(table.c.id == 5)

        result = connection.execute(stmt)

    The object returned is an instance of :class:`_sql.StatementLambdaElement`.

    .. versionadded:: 1.4

    .. seealso::

        :ref:`engine_lambda_caching`


    """
    return coercions.expect(roles.CoerceTextStatementRole, lmb)


class LambdaElement(elements.ClauseElement):
    """A SQL construct where the state is stored as an un-invoked lambda.

    The :class:`_sql.LambdaElement` is produced transparently whenever
    passing lambda expressions into SQL constructs, such as::

        stmt = select(table).where(lambda: table.c.col == parameter)

    The :class:`_sql.LambdaElement` is the base of the
    :class:`_sql.StatementLambdaElement` which represents a full statement
    within a lambda.

    .. versionadded:: 1.4

    .. seealso::

        :ref:`engine_lambda_caching`

    """

    __visit_name__ = "lambda_element"

    _is_lambda_element = True

    _resolved_bindparams = ()

    _traverse_internals = [
        ("_resolved", visitors.InternalTraversal.dp_clauseelement)
    ]

    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self.fn.__code__)

    def __init__(self, fn, role, apply_propagate_attrs=None, **kw):
        self.fn = fn
        self.role = role
        self.parent_lambda = None

        if apply_propagate_attrs is None and (
            role is roles.CoerceTextStatementRole
        ):
            apply_propagate_attrs = self

        if fn.__code__ not in _trackers:
            rec = self._initialize_var_trackers(
                role, apply_propagate_attrs, kw
            )
        else:
            rec = _trackers[self.fn.__code__]
            closure = fn.__closure__

            # check if the objects fixed inside the lambda that we've cached
            # have been changed.   This can apply to things like mappers that
            # were recreated in test suites.    if so, re-initialize.
            #
            # this is a small performance hit on every use for a not very
            # common situation, however it's very hard to debug if the
            # condition does occur.
            for idx, obj in rec[_STALE_CHECK]:
                if closure[idx].cell_contents is not obj:
                    rec = self._initialize_var_trackers(
                        role, apply_propagate_attrs, kw
                    )
                    break
        self._rec = rec

        if apply_propagate_attrs is not None:
            propagate_attrs = rec[_PROPAGATE_ATTRS]
            if propagate_attrs:
                apply_propagate_attrs._propagate_attrs = propagate_attrs

        if rec[_TRACKERS]:
            self._resolved_bindparams = bindparams = []
            for tracker in rec[_TRACKERS]:
                tracker(self.fn, bindparams)

    def __getattr__(self, key):
        return getattr(self._rec[_EXPR], key)

    @property
    def _is_sequence(self):
        return self._rec[_IS_SEQUENCE]

    @property
    def _select_iterable(self):
        if self._is_sequence:
            return itertools.chain.from_iterable(
                [element._select_iterable for element in self._resolved]
            )

        else:
            return self._resolved._select_iterable

    @property
    def _from_objects(self):
        if self._is_sequence:
            return itertools.chain.from_iterable(
                [element._from_objects for element in self._resolved]
            )

        else:
            return self._resolved._from_objects

    def _param_dict(self):
        return {b.key: b.value for b in self._resolved_bindparams}

    @util.memoized_property
    def _resolved(self):
        bindparam_lookup = {b.key: b for b in self._resolved_bindparams}

        def replace(thing):
            if (
                isinstance(thing, elements.BindParameter)
                and thing.key in bindparam_lookup
            ):
                bind = bindparam_lookup[thing.key]
                # TODO: consider
                # if we should clone the bindparam here, re-cache the new
                # version, etc.  also we make an assumption about "expanding"
                # in this case.
                if thing.expanding:
                    bind.expanding = True
                return bind

        expr = self._rec[_EXPR]

        if self._rec[_IS_SEQUENCE]:
            expr = [
                visitors.replacement_traverse(sub_expr, {}, replace)
                for sub_expr in expr
            ]
        elif getattr(expr, "is_clause_element", False):
            expr = visitors.replacement_traverse(expr, {}, replace)

        return expr

    def _gen_cache_key(self, anon_map, bindparams):

        cache_key = (self.fn.__code__, self.__class__)

        if self._resolved_bindparams:
            bindparams.extend(self._resolved_bindparams)

        return cache_key

    def _invoke_user_fn(self, fn, *arg):
        return fn()

    def _initialize_var_trackers(self, role, apply_propagate_attrs, coerce_kw):
        fn = self.fn

        # track objects referenced inside of lambdas, create bindparams
        # ahead of time for literal values.   If bindparams are produced,
        # then rewrite the function globals and closure as necessary so that
        # it refers to the bindparams, then invoke the function
        new_closure = {}
        new_globals = fn.__globals__.copy()
        tracker_collection = []
        check_closure_for_stale = []

        for name in fn.__code__.co_names:
            if name not in new_globals:
                continue

            bound_value = _roll_down_to_literal(new_globals[name])

            if coercions._is_literal(bound_value):
                new_globals[name] = bind = PyWrapper(name, bound_value)
                tracker_collection.append(_globals_tracker(name, bind))

        if fn.__closure__:
            for closure_index, (fv, cell) in enumerate(
                zip(fn.__code__.co_freevars, fn.__closure__)
            ):

                bound_value = _roll_down_to_literal(cell.cell_contents)

                if coercions._is_literal(bound_value):
                    new_closure[fv] = bind = PyWrapper(fv, bound_value)
                    tracker_collection.append(
                        _closure_tracker(fv, bind, closure_index)
                    )
                else:
                    new_closure[fv] = cell.cell_contents
                    # for normal cell contents, add them to a list that
                    # we can compare later when we get new lambdas.  if
                    # any identities have changed, then we will recalculate
                    # the whole lambda and run it again.
                    check_closure_for_stale.append(
                        (closure_index, cell.cell_contents)
                    )

        if tracker_collection:
            new_fn = _rewrite_code_obj(
                fn,
                [new_closure[name] for name in fn.__code__.co_freevars],
                new_globals,
            )
            expr = self._invoke_user_fn(new_fn)

        else:
            new_fn = fn
            expr = self._invoke_user_fn(new_fn)
            tracker_collection = []

        if self.parent_lambda is None:
            if isinstance(expr, collections_abc.Sequence):
                expected_expr = [
                    coercions.expect(
                        role,
                        sub_expr,
                        apply_propagate_attrs=apply_propagate_attrs,
                        **coerce_kw
                    )
                    for sub_expr in expr
                ]
                is_sequence = True
            else:
                expected_expr = coercions.expect(
                    role,
                    expr,
                    apply_propagate_attrs=apply_propagate_attrs,
                    **coerce_kw
                )
                is_sequence = False
        else:
            expected_expr = expr
            is_sequence = False

        if apply_propagate_attrs is not None:
            propagate_attrs = apply_propagate_attrs._propagate_attrs
        else:
            propagate_attrs = util.immutabledict()

        rec = _trackers[self.fn.__code__] = (
            tracker_collection,
            check_closure_for_stale,
            new_fn,
            expected_expr,
            is_sequence,
            propagate_attrs,
        )
        return rec


class StatementLambdaElement(roles.AllowsLambdaRole, LambdaElement):
    """Represent a composable SQL statement as a :class:`_sql.LambdaElement`.

    The :class:`_sql.StatementLambdaElement` is constructed using the
    :func:`_sql.lambda_stmt` function::


        from sqlalchemy import lambda_stmt

        stmt = lambda_stmt(lambda: select(table))

    Once constructed, additional criteria can be built onto the statement
    by adding subsequent lambdas, which accept the existing statement
    object as a single parameter::

        stmt += lambda s: s.where(table.c.col == parameter)


    .. versionadded:: 1.4

    .. seealso::

        :ref:`engine_lambda_caching`

    """

    def __add__(self, other):
        return LinkedLambdaElement(other, parent_lambda=self)

    def _execute_on_connection(
        self, connection, multiparams, params, execution_options
    ):
        if self._rec[_EXPR].supports_execution:
            return connection._execute_clauseelement(
                self, multiparams, params, execution_options
            )
        else:
            raise exc.ObjectNotExecutableError(self)

    @property
    def _with_options(self):
        return self._rec[_EXPR]._with_options

    @property
    def _effective_plugin_target(self):
        return self._rec[_EXPR]._effective_plugin_target

    @property
    def _is_future(self):
        return self._rec[_EXPR]._is_future

    @property
    def _execution_options(self):
        return self._rec[_EXPR]._execution_options


class LinkedLambdaElement(StatementLambdaElement):
    def __init__(self, fn, parent_lambda, **kw):
        self.fn = fn
        self.parent_lambda = parent_lambda
        role = None

        apply_propagate_attrs = self

        if fn.__code__ not in _trackers:
            rec = self._initialize_var_trackers(
                role, apply_propagate_attrs, kw
            )
        else:
            rec = _trackers[self.fn.__code__]

            closure = fn.__closure__

            # check if objects referred to by the lambda have changed and
            # re-scan the lambda if so. see comments for this same section in
            # LambdaElement.
            for idx, obj in rec[_STALE_CHECK]:
                if closure[idx].cell_contents is not obj:
                    rec = self._initialize_var_trackers(
                        role, apply_propagate_attrs, kw
                    )
                    break

        self._rec = rec

        self._propagate_attrs = parent_lambda._propagate_attrs

        self._resolved_bindparams = bindparams = []
        rec = self._rec
        while True:
            if rec[_TRACKERS]:
                for tracker in rec[_TRACKERS]:
                    tracker(self.fn, bindparams)
            if self.parent_lambda is not None:
                self = self.parent_lambda
                rec = self._rec
            else:
                break

    def _invoke_user_fn(self, fn, *arg):
        return fn(self.parent_lambda._rec[_EXPR])

    def _gen_cache_key(self, anon_map, bindparams):
        if self._resolved_bindparams:
            bindparams.extend(self._resolved_bindparams)

        cache_key = (self.fn.__code__, self.__class__)

        parent = self.parent_lambda
        while parent is not None:
            cache_key = (parent.fn.__code__,) + cache_key
            parent = parent.parent_lambda

        return cache_key


class PyWrapper(ColumnOperators):
    def __init__(self, name, to_evaluate, getter=None):
        self._name = name
        self._to_evaluate = to_evaluate
        self._param = None
        self._bind_paths = {}
        self._getter = getter

    def __call__(self, *arg, **kw):
        elem = object.__getattribute__(self, "_to_evaluate")
        value = elem(*arg, **kw)
        if coercions._is_literal(value) and not isinstance(
            # TODO: coverage where an ORM option or similar is here
            value,
            traversals.HasCacheKey,
        ):
            # TODO: we can instead scan the arguments and make sure they
            # are all Python literals

            # TODO: coverage
            name = object.__getattribute__(self, "_name")
            raise exc.InvalidRequestError(
                "Can't invoke Python callable %s() inside of lambda "
                "expression argument; lambda cache keys should not call "
                "regular functions since the caching "
                "system does not track the values of the arguments passed "
                "to the functions.  Call the function outside of the lambda "
                "and assign to a local variable that is used in the lambda."
                % (name)
            )
        else:
            return value

    def operate(self, op, *other, **kwargs):
        elem = object.__getattribute__(self, "__clause_element__")()
        return op(elem, *other, **kwargs)

    def reverse_operate(self, op, other, **kwargs):
        elem = object.__getattribute__(self, "__clause_element__")()
        return op(other, elem, **kwargs)

    def _extract_bound_parameters(self, starting_point, result_list):
        param = object.__getattribute__(self, "_param")
        if param is not None:
            param = param._with_value(starting_point, maintain_key=True)
            result_list.append(param)
        for pywrapper in object.__getattribute__(self, "_bind_paths").values():
            getter = object.__getattribute__(pywrapper, "_getter")
            element = getter(starting_point)
            pywrapper._sa__extract_bound_parameters(element, result_list)

    def __clause_element__(self):
        param = object.__getattribute__(self, "_param")
        to_evaluate = object.__getattribute__(self, "_to_evaluate")
        if param is None:
            name = object.__getattribute__(self, "_name")
            self._param = param = elements.BindParameter(name, unique=True)
            param.type = type_api._resolve_value_to_type(to_evaluate)

        return param._with_value(to_evaluate, maintain_key=True)

    def __getattribute__(self, key):
        if key.startswith("_sa_"):
            return object.__getattribute__(self, key[4:])
        elif key in ("__clause_element__", "operate", "reverse_operate"):
            return object.__getattribute__(self, key)

        if key.startswith("__"):
            elem = object.__getattribute__(self, "_to_evaluate")
            return getattr(elem, key)
        else:
            return self._sa__add_getter(key, operator.attrgetter)

    def __getitem__(self, key):
        if isinstance(key, PyWrapper):
            # TODO: coverage
            raise exc.InvalidRequestError(
                "Dictionary keys / list indexes inside of a cached "
                "lambda must be Python literals only"
            )
        return self._sa__add_getter(key, operator.itemgetter)

    def _add_getter(self, key, getter_fn):

        bind_paths = object.__getattribute__(self, "_bind_paths")

        bind_path_key = (key, getter_fn)
        if bind_path_key in bind_paths:
            return bind_paths[bind_path_key]

        getter = getter_fn(key)
        elem = object.__getattribute__(self, "_to_evaluate")
        value = getter(elem)

        if coercions._is_literal(value):
            wrapper = PyWrapper(key, value, getter)
            bind_paths[bind_path_key] = wrapper
            return wrapper
        else:
            return value


def _roll_down_to_literal(element):
    is_clause_element = hasattr(element, "__clause_element__")

    if is_clause_element:
        while not isinstance(
            element, (elements.ClauseElement, schema.SchemaItem)
        ):
            try:
                element = element.__clause_element__()
            except AttributeError:
                break

    if not is_clause_element:
        insp = inspection.inspect(element, raiseerr=False)
        if insp is not None:
            try:
                return insp.__clause_element__()
            except AttributeError:
                return insp

        # TODO: should we coerce consts None/True/False here?
        return element
    else:
        return element


def _globals_tracker(name, wrapper):
    def extract_parameter_value(current_fn, result):
        object.__getattribute__(wrapper, "_extract_bound_parameters")(
            current_fn.__globals__[name], result
        )

    return extract_parameter_value


def _closure_tracker(name, wrapper, closure_index):
    def extract_parameter_value(current_fn, result):
        object.__getattribute__(wrapper, "_extract_bound_parameters")(
            current_fn.__closure__[closure_index].cell_contents, result
        )

    return extract_parameter_value


def _rewrite_code_obj(f, cell_values, globals_):
    """Return a copy of f, with a new closure and new globals

    yes it works in pypy :P

    """

    argrange = range(len(cell_values))

    code = "def make_cells():\n"
    if cell_values:
        code += "    (%s) = (%s)\n" % (
            ", ".join("i%d" % i for i in argrange),
            ", ".join("o%d" % i for i in argrange),
        )
    code += "    def closure():\n"
    code += "        return %s\n" % ", ".join("i%d" % i for i in argrange)
    code += "    return closure.__closure__"
    vars_ = {"o%d" % i: cell_values[i] for i in argrange}
    exec(code, vars_, vars_)
    closure = vars_["make_cells"]()

    func = type(f)(f.__code__, globals_, f.__name__, f.__defaults__, closure)
    if sys.version_info >= (3,):
        func.__annotations__ = f.__annotations__
        func.__kwdefaults__ = f.__kwdefaults__
    func.__doc__ = f.__doc__
    func.__module__ = f.__module__

    return func


@inspection._inspects(LambdaElement)
def insp(lmb):
    return inspection.inspect(lmb._resolved)
