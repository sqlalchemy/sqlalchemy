# sql/lambdas.py
# Copyright (C) 2005-2020 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

import itertools
import operator
import sys
import types
import weakref

from . import coercions
from . import elements
from . import roles
from . import schema
from . import traversals
from . import type_api
from . import visitors
from .base import _clone
from .operators import ColumnOperators
from .. import exc
from .. import inspection
from .. import util
from ..util import collections_abc

_closure_per_cache_key = util.LRUCache(1000)


def lambda_stmt(lmb, **opts):
    """Produce a SQL statement that is cached as a lambda.

    The Python code object within the lambda is scanned for both Python
    literals that will become bound parameters as well as closure variables
    that refer to Core or ORM constructs that may vary.   The lambda itself
    will be invoked only once per particular set of constructs detected.

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

    return StatementLambdaElement(lmb, roles.CoerceTextStatementRole, **opts)


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

    _traverse_internals = [
        ("_resolved", visitors.InternalTraversal.dp_clauseelement)
    ]

    _transforms = ()

    parent_lambda = None

    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self.fn.__code__)

    def __init__(self, fn, role, apply_propagate_attrs=None, **kw):
        self.fn = fn
        self.role = role
        self.tracker_key = (fn.__code__,)

        if apply_propagate_attrs is None and (
            role is roles.CoerceTextStatementRole
        ):
            apply_propagate_attrs = self

        rec = self._retrieve_tracker_rec(fn, apply_propagate_attrs, kw)

        if apply_propagate_attrs is not None:
            propagate_attrs = rec.propagate_attrs
            if propagate_attrs:
                apply_propagate_attrs._propagate_attrs = propagate_attrs

    def _retrieve_tracker_rec(self, fn, apply_propagate_attrs, kw):
        lambda_cache = kw.get("lambda_cache", _closure_per_cache_key)

        tracker_key = self.tracker_key

        fn = self.fn
        closure = fn.__closure__

        tracker = AnalyzedCode.get(
            fn,
            self,
            kw,
            track_bound_values=kw.get("track_bound_values", True),
            enable_tracking=kw.get("enable_tracking", True),
            track_on=kw.get("track_on", None),
        )

        self._resolved_bindparams = bindparams = []

        anon_map = traversals.anon_map()
        cache_key = tuple(
            [
                getter(closure, kw, anon_map, bindparams)
                for getter in tracker.closure_trackers
            ]
        )
        if self.parent_lambda is not None:
            cache_key = self.parent_lambda.closure_cache_key + cache_key

        self.closure_cache_key = cache_key

        try:
            rec = lambda_cache[tracker_key + cache_key]
        except KeyError:
            rec = None

        if rec is None:
            rec = AnalyzedFunction(
                tracker, self, apply_propagate_attrs, kw, fn
            )
            rec.closure_bindparams = bindparams
            lambda_cache[tracker_key + cache_key] = rec
        else:
            bindparams[:] = [
                orig_bind._with_value(new_bind.value, maintain_key=True)
                for orig_bind, new_bind in zip(
                    rec.closure_bindparams, bindparams
                )
            ]

        if self.parent_lambda is not None:
            bindparams[:0] = self.parent_lambda._resolved_bindparams

        self._rec = rec

        lambda_element = self
        while lambda_element is not None:
            rec = lambda_element._rec
            if rec.bindparam_trackers:
                tracker_instrumented_fn = rec.tracker_instrumented_fn
                for tracker in rec.bindparam_trackers:
                    tracker(
                        lambda_element.fn, tracker_instrumented_fn, bindparams
                    )
            lambda_element = lambda_element.parent_lambda

        return rec

    def __getattr__(self, key):
        return getattr(self._rec.expected_expr, key)

    @property
    def _is_sequence(self):
        return self._rec.is_sequence

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

    def _setup_binds_for_tracked_expr(self, expr):
        bindparam_lookup = {b.key: b for b in self._resolved_bindparams}

        def replace(thing):
            if (
                isinstance(thing, elements.BindParameter)
                and thing.key in bindparam_lookup
            ):
                bind = bindparam_lookup[thing.key]
                if thing.expanding:
                    bind.expanding = True
                return bind

        if self._rec.is_sequence:
            expr = [
                visitors.replacement_traverse(sub_expr, {}, replace)
                for sub_expr in expr
            ]
        elif getattr(expr, "is_clause_element", False):
            expr = visitors.replacement_traverse(expr, {}, replace)

        return expr

    def _copy_internals(
        self, clone=_clone, deferred_copy_internals=None, **kw
    ):
        # TODO: this needs A LOT of tests
        self._resolved = clone(
            self._resolved,
            deferred_copy_internals=deferred_copy_internals,
            **kw
        )

    @util.memoized_property
    def _resolved(self):
        expr = self._rec.expected_expr

        if self._resolved_bindparams:
            expr = self._setup_binds_for_tracked_expr(expr)

        return expr

    def _gen_cache_key(self, anon_map, bindparams):

        cache_key = (
            self.fn.__code__,
            self.__class__,
        ) + self.closure_cache_key

        parent = self.parent_lambda
        while parent is not None:
            cache_key = (
                (parent.fn.__code__,) + parent.closure_cache_key + cache_key
            )

            parent = parent.parent_lambda

        if self._resolved_bindparams:
            bindparams.extend(self._resolved_bindparams)

        return cache_key

    def _invoke_user_fn(self, fn, *arg):
        return fn()


class DeferredLambdaElement(LambdaElement):
    """A LambdaElement where the lambda accepts arguments and is
    invoked within the compile phase with special context.

    This lambda doesn't normally produce its real SQL expression outside of the
    compile phase.  It is passed a fixed set of initial arguments
    so that it can generate a sample expression.

    """

    def __init__(self, fn, role, lambda_args=(), **kw):
        self.lambda_args = lambda_args
        self.coerce_kw = kw
        super(DeferredLambdaElement, self).__init__(fn, role, **kw)

    def _invoke_user_fn(self, fn, *arg):
        return fn(*self.lambda_args)

    def _resolve_with_args(self, *lambda_args):
        tracker_fn = self._rec.tracker_instrumented_fn
        expr = tracker_fn(*lambda_args)

        expr = coercions.expect(self.role, expr, **self.coerce_kw)

        if self._resolved_bindparams:
            expr = self._setup_binds_for_tracked_expr(expr)

        # TODO: TEST TEST TEST, this is very out there
        for deferred_copy_internals in self._transforms:
            expr = deferred_copy_internals(expr)

        return expr

    def _copy_internals(
        self, clone=_clone, deferred_copy_internals=None, **kw
    ):
        super(DeferredLambdaElement, self)._copy_internals(
            clone=clone, deferred_copy_internals=deferred_copy_internals, **kw
        )

        # TODO: A LOT A LOT of tests.   for _resolve_with_args, we don't know
        # our expression yet.   so hold onto the replacement
        if deferred_copy_internals:
            self._transforms += (deferred_copy_internals,)


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

    def __init__(self, fn, parent_lambda, **kw):
        self._default_kw = default_kw = {}
        global_track_bound_values = kw.pop("global_track_bound_values", None)
        if global_track_bound_values is not None:
            default_kw["track_bound_values"] = global_track_bound_values
            kw["track_bound_values"] = global_track_bound_values

        if "lambda_cache" in kw:
            default_kw["lambda_cache"] = kw["lambda_cache"]

        super(StatementLambdaElement, self).__init__(fn, parent_lambda, **kw)

    def __add__(self, other):
        return LinkedLambdaElement(
            other, parent_lambda=self, **self._default_kw
        )

    def add_criteria(self, other, **kw):
        if self._default_kw:
            if kw:
                default_kw = self._default_kw.copy()
                default_kw.update(kw)
                kw = default_kw
            else:
                kw = self._default_kw

        return LinkedLambdaElement(other, parent_lambda=self, **kw)

    def _execute_on_connection(
        self, connection, multiparams, params, execution_options
    ):
        if self._rec.expected_expr.supports_execution:
            return connection._execute_clauseelement(
                self, multiparams, params, execution_options
            )
        else:
            raise exc.ObjectNotExecutableError(self)

    @property
    def _with_options(self):
        return self._rec.expected_expr._with_options

    @property
    def _effective_plugin_target(self):
        return self._rec.expected_expr._effective_plugin_target

    @property
    def _is_future(self):
        return self._rec.expected_expr._is_future

    @property
    def _execution_options(self):
        return self._rec.expected_expr._execution_options

    def spoil(self):
        """Return a new :class:`.StatementLambdaElement` that will run
        all lambdas unconditionally each time.

        """
        return NullLambdaStatement(self.fn())


class NullLambdaStatement(roles.AllowsLambdaRole, elements.ClauseElement):
    """Provides the :class:`.StatementLambdaElement` API but does not
    cache or analyze lambdas.

    the lambdas are instead invoked immediately.

    The intended use is to isolate issues that may arise when using
    lambda statements.

    """

    __visit_name__ = "lambda_element"

    _is_lambda_element = True

    _traverse_internals = [
        ("_resolved", visitors.InternalTraversal.dp_clauseelement)
    ]

    def __init__(self, statement):
        self._resolved = statement
        self._propagate_attrs = statement._propagate_attrs

    def __getattr__(self, key):
        return getattr(self._resolved, key)

    def __add__(self, other):
        statement = other(self._resolved)

        return NullLambdaStatement(statement)

    def add_criteria(self, other, **kw):
        statement = other(self._resolved)

        return NullLambdaStatement(statement)

    def _execute_on_connection(
        self, connection, multiparams, params, execution_options
    ):
        if self._resolved.supports_execution:
            return connection._execute_clauseelement(
                self, multiparams, params, execution_options
            )
        else:
            raise exc.ObjectNotExecutableError(self)


class LinkedLambdaElement(StatementLambdaElement):
    """Represent subsequent links of a :class:`.StatementLambdaElement`."""

    role = None

    def __init__(self, fn, parent_lambda, **kw):
        self._default_kw = parent_lambda._default_kw

        self.fn = fn
        self.parent_lambda = parent_lambda

        self.tracker_key = parent_lambda.tracker_key + (fn.__code__,)
        self._retrieve_tracker_rec(fn, self, kw)
        self._propagate_attrs = parent_lambda._propagate_attrs

    def _invoke_user_fn(self, fn, *arg):
        return fn(self.parent_lambda._resolved)


class AnalyzedCode(object):
    __slots__ = (
        "track_closure_variables",
        "track_bound_values",
        "bindparam_trackers",
        "closure_trackers",
        "build_py_wrappers",
    )
    _fns = weakref.WeakKeyDictionary()

    @classmethod
    def get(cls, fn, lambda_element, lambda_kw, **kw):
        try:
            # TODO: validate kw haven't changed?
            return cls._fns[fn.__code__]
        except KeyError:
            pass
        cls._fns[fn.__code__] = analyzed = AnalyzedCode(
            fn, lambda_element, lambda_kw, **kw
        )
        return analyzed

    def __init__(
        self,
        fn,
        lambda_element,
        lambda_kw,
        track_bound_values=True,
        enable_tracking=True,
        track_on=None,
    ):
        closure = fn.__closure__

        self.track_closure_variables = not track_on

        self.track_bound_values = track_bound_values

        # a list of callables generated from _bound_parameter_getter_*
        # functions.  Each of these uses a PyWrapper object to retrieve
        # a parameter value
        self.bindparam_trackers = []

        # a list of callables generated from _cache_key_getter_* functions
        # these callables work to generate a cache key for the lambda
        # based on what's inside its closure variables.
        self.closure_trackers = []

        self.build_py_wrappers = []

        if enable_tracking:
            if track_on:
                self._init_track_on(track_on)

            self._init_globals(fn)

            if closure:
                self._init_closure(fn)

        self._setup_additional_closure_trackers(fn, lambda_element, lambda_kw)

    def _init_track_on(self, track_on):
        self.closure_trackers.extend(
            self._cache_key_getter_track_on(idx, elem)
            for idx, elem in enumerate(track_on)
        )

    def _init_globals(self, fn):
        build_py_wrappers = self.build_py_wrappers
        bindparam_trackers = self.bindparam_trackers
        track_bound_values = self.track_bound_values

        for name in fn.__code__.co_names:
            if name not in fn.__globals__:
                continue

            _bound_value = self._roll_down_to_literal(fn.__globals__[name])

            if coercions._deep_is_literal(_bound_value):
                build_py_wrappers.append((name, None))
                if track_bound_values:
                    bindparam_trackers.append(
                        self._bound_parameter_getter_func_globals(name)
                    )

    def _init_closure(self, fn):
        build_py_wrappers = self.build_py_wrappers
        closure = fn.__closure__

        track_bound_values = self.track_bound_values
        track_closure_variables = self.track_closure_variables
        bindparam_trackers = self.bindparam_trackers
        closure_trackers = self.closure_trackers

        for closure_index, (fv, cell) in enumerate(
            zip(fn.__code__.co_freevars, closure)
        ):
            _bound_value = self._roll_down_to_literal(cell.cell_contents)

            if coercions._deep_is_literal(_bound_value):
                build_py_wrappers.append((fv, closure_index))
                if track_bound_values:
                    bindparam_trackers.append(
                        self._bound_parameter_getter_func_closure(
                            fv, closure_index
                        )
                    )
            else:
                # for normal cell contents, add them to a list that
                # we can compare later when we get new lambdas.  if
                # any identities have changed, then we will
                # recalculate the whole lambda and run it again.

                if track_closure_variables:
                    closure_trackers.append(
                        self._cache_key_getter_closure_variable(
                            closure_index, cell.cell_contents
                        )
                    )

    def _setup_additional_closure_trackers(
        self, fn, lambda_element, lambda_kw
    ):
        # an additional step is to actually run the function, then
        # go through the PyWrapper objects that were set up to catch a bound
        # parameter.   then if they *didn't* make a param, oh they're another
        # object in the closure we have to track for our cache key.  so
        # create trackers to catch those.

        analyzed_function = AnalyzedFunction(
            self,
            lambda_element,
            None,
            lambda_kw,
            fn,
        )

        closure_trackers = self.closure_trackers

        for pywrapper in analyzed_function.closure_pywrappers:
            if not pywrapper._sa__has_param:
                closure_trackers.append(
                    self._cache_key_getter_tracked_literal(pywrapper)
                )

    @classmethod
    def _roll_down_to_literal(cls, element):
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

    def _bound_parameter_getter_func_globals(self, name):
        """Return a getter that will extend a list of bound parameters
        with new entries from the ``__globals__`` collection of a particular
        lambda.

        """

        def extract_parameter_value(
            current_fn, tracker_instrumented_fn, result
        ):
            wrapper = tracker_instrumented_fn.__globals__[name]
            object.__getattribute__(wrapper, "_extract_bound_parameters")(
                current_fn.__globals__[name], result
            )

        return extract_parameter_value

    def _bound_parameter_getter_func_closure(self, name, closure_index):
        """Return a getter that will extend a list of bound parameters
        with new entries from the ``__closure__`` collection of a particular
        lambda.

        """

        def extract_parameter_value(
            current_fn, tracker_instrumented_fn, result
        ):
            wrapper = tracker_instrumented_fn.__closure__[
                closure_index
            ].cell_contents
            object.__getattribute__(wrapper, "_extract_bound_parameters")(
                current_fn.__closure__[closure_index].cell_contents, result
            )

        return extract_parameter_value

    def _cache_key_getter_track_on(self, idx, elem):
        """Return a getter that will extend a cache key with new entries
        from the "track_on" parameter passed to a :class:`.LambdaElement`.

        """
        if isinstance(elem, traversals.HasCacheKey):

            def get(closure, kw, anon_map, bindparams):
                return kw["track_on"][idx]._gen_cache_key(anon_map, bindparams)

        else:

            def get(closure, kw, anon_map, bindparams):
                return kw["track_on"][idx]

        return get

    def _cache_key_getter_closure_variable(self, idx, cell_contents):
        """Return a getter that will extend a cache key with new entries
        from the ``__closure__`` collection of a particular lambda.

        """

        if isinstance(cell_contents, traversals.HasCacheKey):

            def get(closure, kw, anon_map, bindparams):
                return closure[idx].cell_contents._gen_cache_key(
                    anon_map, bindparams
                )

        elif isinstance(cell_contents, types.FunctionType):

            def get(closure, kw, anon_map, bindparams):
                return closure[idx].cell_contents.__code__

        elif cell_contents.__hash__ is None:
            # this covers dict, etc.
            def get(closure, kw, anon_map, bindparams):
                return ()

        else:

            def get(closure, kw, anon_map, bindparams):
                return closure[idx].cell_contents

        return get

    def _cache_key_getter_tracked_literal(self, pytracker):
        """Return a getter that will extend a cache key with new entries
        from the ``__closure__`` collection of a particular lambda.

        this getter differs from _cache_key_getter_closure_variable
        in that these are detected after the function is run, and PyWrapper
        objects have recorded that a particular literal value is in fact
        not being interpreted as a bound parameter.

        """

        elem = pytracker._sa__to_evaluate
        closure_index = pytracker._sa__closure_index

        if isinstance(elem, set):
            raise exc.ArgumentError(
                "Can't create a cache key for lambda closure variable "
                '"%s" because it\'s a set.  try using a list'
                % pytracker._sa__name
            )

        elif isinstance(elem, list):

            def get(closure, kw, anon_map, bindparams):
                return tuple(
                    elem._gen_cache_key(anon_map, bindparams)
                    for elem in closure[closure_index].cell_contents
                )

        elif elem.__hash__ is None:
            # this covers dict, etc.
            def get(closure, kw, anon_map, bindparams):
                return ()

        else:

            def get(closure, kw, anon_map, bindparams):
                return closure[closure_index].cell_contents

        return get


class AnalyzedFunction(object):
    __slots__ = (
        "analyzed_code",
        "fn",
        "closure_pywrappers",
        "tracker_instrumented_fn",
        "expr",
        "bindparam_trackers",
        "expected_expr",
        "is_sequence",
        "propagate_attrs",
        "closure_bindparams",
    )

    def __init__(
        self,
        analyzed_code,
        lambda_element,
        apply_propagate_attrs,
        kw,
        fn,
    ):
        self.analyzed_code = analyzed_code
        self.fn = fn

        self.bindparam_trackers = analyzed_code.bindparam_trackers

        self._instrument_and_run_function(lambda_element)

        self._coerce_expression(lambda_element, apply_propagate_attrs, kw)

    def _instrument_and_run_function(self, lambda_element):
        analyzed_code = self.analyzed_code

        fn = self.fn
        self.closure_pywrappers = closure_pywrappers = []

        build_py_wrappers = analyzed_code.build_py_wrappers

        if not build_py_wrappers:
            self.tracker_instrumented_fn = tracker_instrumented_fn = fn
            self.expr = lambda_element._invoke_user_fn(tracker_instrumented_fn)
        else:
            track_closure_variables = analyzed_code.track_closure_variables
            closure = fn.__closure__

            # will form the __closure__ of the function when we rebuild it
            if closure:
                new_closure = {
                    fv: cell.cell_contents
                    for fv, cell in zip(fn.__code__.co_freevars, closure)
                }
            else:
                new_closure = {}

            # will form the __globals__ of the function when we rebuild it
            new_globals = fn.__globals__.copy()

            for name, closure_index in build_py_wrappers:
                if closure_index is not None:
                    value = closure[closure_index].cell_contents
                    new_closure[name] = bind = PyWrapper(
                        name, value, closure_index=closure_index
                    )
                    if track_closure_variables:
                        closure_pywrappers.append(bind)
                else:
                    value = fn.__globals__[name]
                    new_globals[name] = bind = PyWrapper(name, value)

            # rewrite the original fn.   things that look like they will
            # become bound parameters are wrapped in a PyWrapper.
            self.tracker_instrumented_fn = (
                tracker_instrumented_fn
            ) = self._rewrite_code_obj(
                fn,
                [new_closure[name] for name in fn.__code__.co_freevars],
                new_globals,
            )

            # now invoke the function.  This will give us a new SQL
            # expression, but all the places that there would be a bound
            # parameter, the PyWrapper in its place will give us a bind
            # with a predictable name we can match up later.

            # additionally, each PyWrapper will log that it did in fact
            # create a parameter, otherwise, it's some kind of Python
            # object in the closure and we want to track that, to make
            # sure it doesn't change to somehting else, or if it does,
            # that we create a different tracked function with that
            # variable.
            self.expr = lambda_element._invoke_user_fn(tracker_instrumented_fn)

    def _coerce_expression(self, lambda_element, apply_propagate_attrs, kw):
        """Run the tracker-generated expression through coercion rules.

        After the user-defined lambda has been invoked to produce a statement
        for re-use, run it through coercion rules to both check that it's the
        correct type of object and also to coerce it to its useful form.

        """

        parent_lambda = lambda_element.parent_lambda
        expr = self.expr

        if parent_lambda is None:
            if isinstance(expr, collections_abc.Sequence):
                self.expected_expr = [
                    coercions.expect(
                        lambda_element.role,
                        sub_expr,
                        apply_propagate_attrs=apply_propagate_attrs,
                        **kw
                    )
                    for sub_expr in expr
                ]
                self.is_sequence = True
            else:
                self.expected_expr = coercions.expect(
                    lambda_element.role,
                    expr,
                    apply_propagate_attrs=apply_propagate_attrs,
                    **kw
                )
                self.is_sequence = False
        else:
            self.expected_expr = expr
            self.is_sequence = False

        if apply_propagate_attrs is not None:
            self.propagate_attrs = apply_propagate_attrs._propagate_attrs
        else:
            self.propagate_attrs = util.EMPTY_DICT

    def _rewrite_code_obj(self, f, cell_values, globals_):
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

        func = type(f)(
            f.__code__, globals_, f.__name__, f.__defaults__, closure
        )
        if sys.version_info >= (3,):
            func.__annotations__ = f.__annotations__
            func.__kwdefaults__ = f.__kwdefaults__
        func.__doc__ = f.__doc__
        func.__module__ = f.__module__

        return func


class PyWrapper(ColumnOperators):
    """A wrapper object that is injected into the ``__globals__`` and
    ``__closure__`` of a Python function.

    When the function is instrumented with :class:`.PyWrapper` objects, it is
    then invoked just once in order to set up the wrappers.  We look through
    all the :class:`.PyWrapper` objects we made to find the ones that generated
    a :class:`.BindParameter` object, e.g. the expression system interpreted
    something as a literal.   Those positions in the globals/closure are then
    ones that we will look at, each time a new lambda comes in that refers to
    the same ``__code__`` object.   In this way, we keep a single version of
    the SQL expression that this lambda produced, without calling upon the
    Python function that created it more than once, unless its other closure
    variables have changed.   The expression is then transformed to have the
    new bound values embedded into it.

    """

    def __init__(self, name, to_evaluate, closure_index=None, getter=None):
        self._name = name
        self._to_evaluate = to_evaluate
        self._param = None
        self._has_param = False
        self._bind_paths = {}
        self._getter = getter
        self._closure_index = closure_index

    def __call__(self, *arg, **kw):
        elem = object.__getattribute__(self, "_to_evaluate")
        value = elem(*arg, **kw)
        if coercions._deep_is_literal(value) and not isinstance(
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
            self._has_param = True
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

    def __iter__(self):
        elem = object.__getattribute__(self, "_to_evaluate")
        return iter(elem)

    def __getitem__(self, key):
        elem = object.__getattribute__(self, "_to_evaluate")
        if not hasattr(elem, "__getitem__"):
            raise AttributeError("__getitem__")

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

        if coercions._deep_is_literal(value):
            wrapper = PyWrapper(key, value, getter=getter)
            bind_paths[bind_path_key] = wrapper
            return wrapper
        else:
            return value


@inspection._inspects(LambdaElement)
def insp(lmb):
    return inspection.inspect(lmb._resolved)
