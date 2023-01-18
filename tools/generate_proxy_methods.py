"""Generate static proxy code for SQLAlchemy classes that proxy other
objects.

This tool is run at source code authoring / commit time whenever we add new
methods to engines/connections/sessions that need to be generically proxied by
scoped_session or asyncio.   The generated code is part of what's committed
to source just as though we typed it all by hand.

The original "proxy" class was scoped_session. Then with asyncio, all the
asyncio objects are essentially "proxy" objects as well; while all the methods
that are "async" needed to be written by hand, there's lots of other attributes
and methods that are proxied exactly.

To eliminate redundancy, all of these classes made use of the
@langhelpers.create_proxy_methods() decorator which at runtime would read a
selected list of methods and attributes from the proxied class and generate new
methods and properties descriptors on the proxying class; this way the proxy
would have all the same methods signatures / attributes / docstrings consumed
by Sphinx and look just like the proxied class.

Then mypy and typing came along, which don't care about runtime generated code
and never will. So this script takes that same
@langhelpers.create_proxy_methods() decorator, keeps its public interface just
as is, and uses it to generate all the code and docs in those proxy classes
statically, as though we sat there and spent seven hours typing it all by hand.
The runtime code generation part is removed from ``create_proxy_methods()``.
Now we have static code that is perfectly consumable by all the typing tools
and we also reduce import time a bit.

A similar approach is used in Alembic where a dynamic approach towards creating
alembic "ops" was enhanced to generate a .pyi stubs file statically for
consumption by typing tools.

Note that the usual OO approach of having a common interface class with
concrete subtypes doesn't really solve any problems here; the concrete subtypes
must still list out all methods, arguments, typing annotations, and docstrings,
all of which is copied by this script rather than requiring it all be
typed by hand.

.. versionadded:: 2.0

"""
# mypy: ignore-errors

from __future__ import annotations

import collections
import importlib
import inspect
import os
from pathlib import Path
import re
import sys
from tempfile import NamedTemporaryFile
import textwrap
from typing import Any
from typing import Callable
from typing import Dict
from typing import Iterable
from typing import TextIO
from typing import Tuple
from typing import Type
from typing import TypeVar

from sqlalchemy import util
from sqlalchemy.util import compat
from sqlalchemy.util import langhelpers
from sqlalchemy.util.langhelpers import format_argspec_plus
from sqlalchemy.util.langhelpers import inject_docstring_text
from sqlalchemy.util.tool_support import code_writer_cmd

is_posix = os.name == "posix"


sys.path.append(str(Path(__file__).parent.parent))


class _repr_sym:
    __slots__ = ("sym",)

    def __init__(self, sym: str):
        self.sym = sym

    def __repr__(self) -> str:
        return self.sym


classes: collections.defaultdict[
    str, Dict[str, Tuple[Any, ...]]
] = collections.defaultdict(dict)

_T = TypeVar("_T", bound="Any")


def create_proxy_methods(
    target_cls: Type[Any],
    target_cls_sphinx_name: str,
    proxy_cls_sphinx_name: str,
    classmethods: Iterable[str] = (),
    methods: Iterable[str] = (),
    attributes: Iterable[str] = (),
) -> Callable[[Type[_T]], Type[_T]]:
    """A class decorator that will copy attributes to a proxy class.

    The class to be instrumented must define a single accessor "_proxied".

    """

    def decorate(cls: Type[_T]) -> Type[_T]:
        # collect the class as a separate step.  since the decorator
        # is called as a result of imports, the order in which classes
        # are collected (like in asyncio) can't be well controlled.  however,
        # the proxies (specifically asyncio session and asyncio scoped_session)
        # have to be generated in dependency order, so run them in order in a
        # second step.
        classes[cls.__module__][cls.__name__] = (
            target_cls,
            target_cls_sphinx_name,
            proxy_cls_sphinx_name,
            classmethods,
            methods,
            attributes,
            cls,
        )
        return cls

    return decorate


def _grab_overloads(fn):
    """grab @overload entries for a function, assuming black-formatted
    code ;) so that we can do a simple regex

    """

    # functions that use @util.deprecated and whatnot will have a string
    # generated fn.  we can look at __wrapped__ but these functions don't
    # have any overloads in any case right now so skip
    if fn.__code__.co_filename == "<string>":
        return []

    with open(fn.__code__.co_filename) as f:
        lines = [l for i, l in zip(range(fn.__code__.co_firstlineno), f)]

        lines.reverse()

    output = []

    current_ov = []
    for line in lines[1:]:
        current_ov.append(line)
        outside_block_match = re.match(r"^\w", line)
        if outside_block_match:
            current_ov[:] = []
            break

        fn_match = re.match(r"^    (?:    )?(?:async )?def (.*)\(", line)
        if fn_match and fn_match.group(1) != fn.__name__:
            current_ov[:] = []
            break

        ov_match = re.match(r"^    (?:    )?@overload$", line)
        if ov_match:
            output.append("".join(reversed(current_ov)))
            current_ov[:] = []

        if re.match(r"^    if (?:typing\.)?TYPE_CHECKING:", line):
            output.append(line)
            current_ov[:] = []

    output.reverse()
    return output


def process_class(
    buf: TextIO,
    target_cls: Type[Any],
    target_cls_sphinx_name: str,
    proxy_cls_sphinx_name: str,
    classmethods: Iterable[str],
    methods: Iterable[str],
    attributes: Iterable[str],
    cls: Type[Any],
):

    sphinx_symbol_match = re.match(r":class:`(.+)`", target_cls_sphinx_name)
    if not sphinx_symbol_match:
        raise Exception(
            f"Couldn't match sphinx class identifier from "
            f"target_cls_sphinx_name f{target_cls_sphinx_name!r}.  Currently "
            'this program expects the form ":class:`_<prefix>.<clsname>`"'
        )

    sphinx_symbol = sphinx_symbol_match.group(1)

    def instrument(buf: TextIO, name: str, clslevel: bool = False) -> None:
        fn = getattr(target_cls, name)

        overloads = _grab_overloads(fn)

        for overload in overloads:
            buf.write(overload)

        spec = compat.inspect_getfullargspec(fn)

        iscoroutine = inspect.iscoroutinefunction(fn)

        if spec.defaults or spec.kwonlydefaults:
            elem = list(spec)

            if spec.defaults:
                new_defaults = tuple(
                    _repr_sym("util.EMPTY_DICT")
                    if df is util.EMPTY_DICT
                    else df
                    for df in spec.defaults
                )
                elem[3] = new_defaults

            if spec.kwonlydefaults:
                new_kwonlydefaults = {
                    name: _repr_sym("util.EMPTY_DICT")
                    if df is util.EMPTY_DICT
                    else df
                    for name, df in spec.kwonlydefaults.items()
                }
                elem[5] = new_kwonlydefaults

            spec = compat.FullArgSpec(*elem)

        caller_argspec = format_argspec_plus(spec, grouped=False)

        metadata = {
            "name": fn.__name__,
            "async": "async " if iscoroutine else "",
            "await": "await " if iscoroutine else "",
            "apply_pos_proxied": caller_argspec["apply_pos_proxied"],
            "target_cls_name": target_cls.__name__,
            "apply_kw_proxied": caller_argspec["apply_kw_proxied"],
            "grouped_args": caller_argspec["grouped_args"],
            "self_arg": caller_argspec["self_arg"],
            "doc": textwrap.indent(
                inject_docstring_text(
                    fn.__doc__,
                    textwrap.indent(
                        ".. container:: class_bases\n\n"
                        f"    Proxied for the {target_cls_sphinx_name} "
                        "class on \n"
                        f"    behalf of the {proxy_cls_sphinx_name} "
                        "class.",
                        "    ",
                    ),
                    1,
                ),
                "    ",
            ).lstrip(),
        }

        if clslevel:
            code = (
                "@classmethod\n"
                "%(async)sdef %(name)s%(grouped_args)s:\n"
                '    r"""%(doc)s\n    """  # noqa: E501\n\n'
                "    return %(await)s%(target_cls_name)s.%(name)s(%(apply_kw_proxied)s)\n\n"  # noqa: E501
                % metadata
            )
        else:
            code = (
                "%(async)sdef %(name)s%(grouped_args)s:\n"
                '    r"""%(doc)s\n    """  # noqa: E501\n\n'
                "    return %(await)s%(self_arg)s._proxied.%(name)s(%(apply_kw_proxied)s)\n\n"  # noqa: E501
                % metadata
            )

        buf.write(textwrap.indent(code, "    "))

    def makeprop(buf: TextIO, name: str) -> None:
        attr = target_cls.__dict__.get(name, None)

        return_type = target_cls.__annotations__.get(name, "Any")
        assert isinstance(return_type, str), (
            "expected string annotations, is from __future__ "
            "import annotations set up?"
        )

        if attr is not None:
            if isinstance(attr, property):
                readonly = attr.fset is None
            elif isinstance(attr, langhelpers.generic_fn_descriptor):
                readonly = True
            else:
                readonly = not hasattr(attr, "__set__")
            doc = textwrap.indent(
                inject_docstring_text(
                    attr.__doc__,
                    textwrap.indent(
                        ".. container:: class_bases\n\n"
                        f"    Proxied for the {target_cls_sphinx_name} "
                        "class \n"
                        f"    on behalf of the {proxy_cls_sphinx_name} "
                        "class.",
                        "    ",
                    ),
                    1,
                ),
                "    ",
            ).lstrip()
        else:
            readonly = False
            doc = (
                f"Proxy for the :attr:`{sphinx_symbol}.{name}` "
                "attribute \n"
                f"        on behalf of the {proxy_cls_sphinx_name} "
                "class.\n"
            )

        code = (
            "@property\n"
            "def %(name)s(self) -> %(return_type)s:\n"
            '    r"""%(doc)s\n    """  # noqa: E501\n\n'
            "    return self._proxied.%(name)s\n\n"
        ) % {"name": name, "doc": doc, "return_type": return_type}

        if not readonly:
            code += (
                "@%(name)s.setter\n"
                "def %(name)s(self, attr: %(return_type)s) -> None:\n"
                "    self._proxied.%(name)s = attr\n\n"
            ) % {"name": name, "return_type": return_type}

        buf.write(textwrap.indent(code, "    "))

    for meth in methods:
        instrument(buf, meth)

    for prop in attributes:
        makeprop(buf, prop)

    for prop in classmethods:
        instrument(buf, prop, clslevel=True)


def process_module(modname: str, filename: str, cmd: code_writer_cmd) -> str:

    class_entries = classes[modname]

    # use tempfile in same path as the module, or at least in the
    # current working directory, so that black / zimports use
    # local pyproject.toml
    with NamedTemporaryFile(
        mode="w",
        delete=False,
        suffix=".py",
    ) as buf, open(filename) as orig_py:

        in_block = False
        current_clsname = None
        for line in orig_py:
            m = re.match(r"    # START PROXY METHODS (.+)$", line)
            if m:
                current_clsname = m.group(1)
                args = class_entries[current_clsname]
                cmd.write_status(
                    f"Generating attributes for class {current_clsname}\n"
                )
                in_block = True
                buf.write(line)
                buf.write(
                    "\n    # code within this block is "
                    "**programmatically, \n"
                    "    # statically generated** by"
                    f" tools/{os.path.basename(__file__)}\n\n"
                )

                process_class(buf, *args)
            if line.startswith(f"    # END PROXY METHODS {current_clsname}"):
                in_block = False

            if not in_block:
                buf.write(line)
    return buf.name


def run_module(modname: str, cmd: code_writer_cmd) -> None:

    cmd.write_status(f"importing module {modname}\n")
    mod = importlib.import_module(modname)
    destination_path = mod.__file__
    assert destination_path is not None

    tempfile = process_module(modname, destination_path, cmd)

    cmd.run_zimports(tempfile)
    cmd.run_black(tempfile)
    cmd.write_output_file_from_tempfile(tempfile, destination_path)


def main(cmd: code_writer_cmd) -> None:
    from sqlalchemy import util
    from sqlalchemy.util import langhelpers

    util.create_proxy_methods = (
        langhelpers.create_proxy_methods
    ) = create_proxy_methods

    for entry in entries:
        if cmd.args.module in {"all", entry}:
            run_module(entry, cmd)


entries = [
    "sqlalchemy.orm.scoping",
    "sqlalchemy.ext.asyncio.engine",
    "sqlalchemy.ext.asyncio.session",
    "sqlalchemy.ext.asyncio.scoping",
]

if __name__ == "__main__":

    cmd = code_writer_cmd(__file__)

    with cmd.add_arguments() as parser:
        parser.add_argument(
            "--module",
            choices=entries + ["all"],
            default="all",
            help="Which file to generate. Default is to regenerate all files",
        )

    with cmd.run_program():
        main(cmd)
