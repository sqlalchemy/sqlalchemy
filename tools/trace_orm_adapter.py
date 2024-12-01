"""
Debug ORMAdapter calls within ORM runs.

Demos::

    $ python tools/trace_orm_adapter.py -m pytest \
        test/orm/inheritance/test_polymorphic_rel.py::PolymorphicAliasedJoinsTest::test_primary_eager_aliasing_joinedload

    $ python tools/trace_orm_adapter.py -m pytest \
        test/orm/test_eager_relations.py::LazyLoadOptSpecificityTest::test_pathed_joinedload_aliased_abs_bcs

    $ python tools/trace_orm_adapter.py my_test_script.py


The above two tests should spit out a ton of debug output.  If a test or program
has no debug output at all, that's a good thing!  it means ORMAdapter isn't
used for that case.

You can then set a breakpoint at the end of any adapt step::

    $ python tools/trace_orm_adapter.py -d 10 -m pytest -s \
        test/orm/test_eager_relations.py::LazyLoadOptSpecificityTest::test_pathed_joinedload_aliased_abs_bcs


"""  # noqa: E501

# mypy: ignore-errors


from __future__ import annotations

import argparse
import contextlib
import contextvars
import sys
from typing import TYPE_CHECKING

from sqlalchemy.orm import util


if TYPE_CHECKING:
    from typing import Any
    from typing import List
    from typing import Optional

    from sqlalchemy.sql.elements import ColumnElement


class _ORMAdapterTrace:
    def _locate_col(
        self, col: ColumnElement[Any]
    ) -> Optional[ColumnElement[Any]]:
        with self._tracer("_locate_col") as tracer:
            return tracer(super()._locate_col, col)

    def replace(self, col, _include_singleton_constants: bool = False):
        with self._tracer("replace") as tracer:
            return tracer(super().replace, col)

    _orm_adapter_trace_context = contextvars.ContextVar("_tracer")

    @contextlib.contextmanager
    def _tracer(self, meth):
        adapter = self
        ctx = self._orm_adapter_trace_context.get(
            {"stack": [], "last_depth": 0, "line_no": 0}
        )
        self._orm_adapter_trace_context.set(ctx)

        stack: List[Any] = ctx["stack"]  # type: ignore
        last_depth = len(stack)
        line_no: int = ctx["line_no"]  # type: ignore
        ctx["last_depth"] = last_depth
        stack.append((adapter, meth))
        indent = "    " * last_depth

        if hasattr(adapter, "mapper"):
            adapter_desc = (
                f"{adapter.__class__.__name__}"
                f"({adapter.role.name}, mapper={adapter.mapper})"
            )
        else:
            adapter_desc = f"{adapter.__class__.__name__}({adapter.role.name})"

        def tracer_fn(fn, arg):
            nonlocal line_no

            line_no += 1

            print(f"{indent} {line_no} {adapter_desc}", file=REAL_STDOUT)
            sub_indent = " " * len(f"{line_no} ")

            print(
                f"{indent}{sub_indent} -> "
                f"{meth} {_orm_adapter_trace_print(arg)}",
                file=REAL_STDOUT,
            )
            ctx["line_no"] = line_no
            ret = fn(arg)

            if DEBUG_ADAPT_STEP == line_no:
                breakpoint()

            if ret is arg:
                print(f"{indent} {line_no} <- same object", file=REAL_STDOUT)
            else:
                print(
                    f"{indent} {line_no} <- {_orm_adapter_trace_print(ret)}",
                    file=REAL_STDOUT,
                )

            if last_depth == 0:
                print("", file=REAL_STDOUT)
            return ret

        try:
            yield tracer_fn
        finally:
            stack.pop(-1)


util.ORMAdapter.__bases__ = (_ORMAdapterTrace,) + util.ORMAdapter.__bases__
util.ORMStatementAdapter.__bases__ = (
    _ORMAdapterTrace,
) + util.ORMStatementAdapter.__bases__


def _orm_adapter_trace_print(obj):
    if obj is None:
        return "None"

    t_print = _orm_adapter_trace_printers.get(obj.__visit_name__, None)
    if t_print:
        return t_print(obj)
    else:
        return f"{obj!r}"


_orm_adapter_trace_printers = {
    "table": lambda t: (
        f'Table("{t.name}", '
        f"entity={t._annotations.get('parentmapper', None)})"
    ),
    "column": lambda c: (
        f'Column("{c.name}", {_orm_adapter_trace_print(c.table)} '
        f"entity={c._annotations.get('parentmapper', None)})"
    ),
    "join": lambda j: (
        f"{j.__class__.__name__}({_orm_adapter_trace_print(j.left)}, "
        f"{_orm_adapter_trace_print(j.right)})"
    ),
    "label": lambda l: f"Label({_orm_adapter_trace_print(l.element)})",
}

DEBUG_ADAPT_STEP = None
REAL_STDOUT = sys.__stdout__


def main():
    global DEBUG_ADAPT_STEP

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-d", "--debug", type=int, help="breakpoint at this adaptation step"
    )
    parser.add_argument(
        "-m",
        "--module",
        type=str,
        help="import module name instead of running a script",
    )
    parser.add_argument(
        "args", metavar="N", type=str, nargs="*", help="additional arguments"
    )

    argparse_args = []
    sys_argv = list(sys.argv)

    progname = sys_argv.pop(0)

    # this is a little crazy, works at the moment for:
    # module w args:
    #    python tools/trace_orm_adapter.py -m  pytest test/orm/test_query.py -s
    # script:
    #   python tools/trace_orm_adapter.py test3.py
    has_module = False
    while sys_argv:
        arg = sys_argv.pop(0)
        if arg in ("-m", "--module", "-d", "--debug"):
            argparse_args.append(arg)
            argparse_args.append(sys_argv.pop(0))
            has_module = arg in ("-m", "--module")
        else:
            if not has_module:
                argparse_args.append(arg)
            else:
                sys_argv.insert(0, arg)
            break

    options = parser.parse_args(argparse_args)
    sys.argv = ["program.py"] + sys_argv

    if options.module == "pytest":
        sys.argv.extend(["--capture", "sys"])

    import runpy

    if options.debug:
        DEBUG_ADAPT_STEP = options.debug

    if options.module:
        runpy.run_module(options.module, run_name="__main__")
    else:
        progname = options.args[0]

        runpy.run_path(progname)


if __name__ == "__main__":
    main()
