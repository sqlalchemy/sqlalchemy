# util/_preloaded.py
# Copyright (C) 2005-2020 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Legacy routines to resolve circular module imports at runtime.

These routines are replaced in 1.4.

"""

from functools import update_wrapper

from . import compat


class _memoized_property(object):
    """vendored version of langhelpers.memoized_property.

    not needed in the 1.4 version of preloaded.

    """

    def __init__(self, fget, doc=None):
        self.fget = fget
        self.__doc__ = doc or fget.__doc__
        self.__name__ = fget.__name__

    def __get__(self, obj, cls):
        if obj is None:
            return self
        obj.__dict__[self.__name__] = result = self.fget(obj)
        return result


def _format_argspec_plus(fn, grouped=True):
    """vendored version of langhelpers._format_argspec_plus.

    not needed in the 1.4 version of preloaded.

    """
    if compat.callable(fn):
        spec = compat.inspect_getfullargspec(fn)
    else:
        spec = fn

    args = compat.inspect_formatargspec(*spec)
    if spec[0]:
        self_arg = spec[0][0]
    elif spec[1]:
        self_arg = "%s[0]" % spec[1]
    else:
        self_arg = None

    apply_pos = compat.inspect_formatargspec(
        spec[0], spec[1], spec[2], None, spec[4]
    )
    num_defaults = 0
    if spec[3]:
        num_defaults += len(spec[3])
    if spec[4]:
        num_defaults += len(spec[4])
    name_args = spec[0] + spec[4]

    if num_defaults:
        defaulted_vals = name_args[0 - num_defaults :]
    else:
        defaulted_vals = ()

    apply_kw = compat.inspect_formatargspec(
        name_args,
        spec[1],
        spec[2],
        defaulted_vals,
        formatvalue=lambda x: "=" + x,
    )
    if grouped:
        return dict(
            args=args,
            self_arg=self_arg,
            apply_pos=apply_pos,
            apply_kw=apply_kw,
        )
    else:
        return dict(
            args=args[1:-1],
            self_arg=self_arg,
            apply_pos=apply_pos[1:-1],
            apply_kw=apply_kw[1:-1],
        )


class dependencies(object):
    """Apply imported dependencies as arguments to a function.

    E.g.::

        @util.dependencies(
            "sqlalchemy.sql.widget",
            "sqlalchemy.engine.default"
        );
        def some_func(self, widget, default, arg1, arg2, **kw):
            # ...

    Rationale is so that the impact of a dependency cycle can be
    associated directly with the few functions that cause the cycle,
    and not pollute the module-level namespace.

    """

    def __init__(self, *deps):
        self.import_deps = []
        for dep in deps:
            tokens = dep.split(".")
            self.import_deps.append(
                dependencies._importlater(".".join(tokens[0:-1]), tokens[-1])
            )

    def __call__(self, fn):
        import_deps = self.import_deps
        spec = compat.inspect_getfullargspec(fn)

        spec_zero = list(spec[0])
        hasself = spec_zero[0] in ("self", "cls")

        for i in range(len(import_deps)):
            spec[0][i + (1 if hasself else 0)] = "import_deps[%r]" % i

        inner_spec = _format_argspec_plus(spec, grouped=False)

        for impname in import_deps:
            del spec_zero[1 if hasself else 0]
        spec[0][:] = spec_zero

        outer_spec = _format_argspec_plus(spec, grouped=False)

        code = "lambda %(args)s: fn(%(apply_kw)s)" % {
            "args": outer_spec["args"],
            "apply_kw": inner_spec["apply_kw"],
        }

        decorated = eval(code, locals())
        decorated.__defaults__ = getattr(fn, "im_func", fn).__defaults__
        return update_wrapper(decorated, fn)

    @classmethod
    def resolve_all(cls, path):
        for m in list(dependencies._unresolved):
            if m._full_path.startswith(path):
                m._resolve()

    _unresolved = set()
    _by_key = {}

    class _importlater(object):
        _unresolved = set()

        _by_key = {}

        def __new__(cls, path, addtl):
            key = path + "." + addtl
            if key in dependencies._by_key:
                return dependencies._by_key[key]
            else:
                dependencies._by_key[key] = imp = object.__new__(cls)
                return imp

        def __init__(self, path, addtl):
            self._il_path = path
            self._il_addtl = addtl
            dependencies._unresolved.add(self)

        @property
        def _full_path(self):
            return self._il_path + "." + self._il_addtl

        @_memoized_property
        def module(self):
            if self in dependencies._unresolved:
                raise ImportError(
                    "importlater.resolve_all() hasn't "
                    "been called (this is %s %s)"
                    % (self._il_path, self._il_addtl)
                )

            return getattr(self._initial_import, self._il_addtl)

        def _resolve(self):
            dependencies._unresolved.discard(self)
            self._initial_import = compat.import_(
                self._il_path, globals(), locals(), [self._il_addtl]
            )

        def __getattr__(self, key):
            if key == "module":
                raise ImportError(
                    "Could not resolve module %s" % self._full_path
                )
            try:
                attr = getattr(self.module, key)
            except AttributeError:
                raise AttributeError(
                    "Module %s has no attribute '%s'" % (self._full_path, key)
                )
            self.__dict__[key] = attr
            return attr
