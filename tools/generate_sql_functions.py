"""Generate inline stubs for generic functions on func"""

# mypy: ignore-errors

from __future__ import annotations

import inspect
import re
from tempfile import NamedTemporaryFile
import textwrap
import typing

import typing_extensions

from sqlalchemy.sql.functions import _registry
from sqlalchemy.sql.functions import ReturnTypeFromArgs
from sqlalchemy.sql.functions import ReturnTypeFromOptionalArgs
from sqlalchemy.types import TypeEngine
from sqlalchemy.util.tool_support import code_writer_cmd


def _fns_in_deterministic_order():
    reg = _registry["_default"]
    for key in sorted(reg):
        cls = reg[key]
        if cls is ReturnTypeFromArgs or cls is ReturnTypeFromOptionalArgs:
            continue
        yield key, cls


def process_functions(filename: str, cmd: code_writer_cmd) -> str:
    with (
        NamedTemporaryFile(
            mode="w",
            delete=False,
            suffix=".py",
        ) as buf,
        open(filename) as orig_py,
    ):
        indent = ""
        in_block = False
        alias_mapping: dict[str, str] = {}

        for line in orig_py:
            m = re.match(
                r"^( *)# START GENERATED FUNCTION ACCESSORS",
                line,
            )
            if m:
                in_block = True
                buf.write(line)
                indent = m.group(1)
                buf.write(
                    textwrap.indent(
                        """
# code within this block is **programmatically,
# statically generated** by tools/generate_sql_functions.py
""",
                        indent,
                    )
                )

                builtins = set(dir(__builtins__))
                for key, fn_class in _fns_in_deterministic_order():
                    is_reserved_word = key in builtins

                    class_name = f"_{fn_class.__name__}_func"
                    if issubclass(fn_class, ReturnTypeFromArgs):
                        guess_its_generic = True
                        if issubclass(fn_class, ReturnTypeFromOptionalArgs):
                            _TEE = "Optional[_T]"
                        else:
                            _TEE = "_T"

                        buf.write(
                            textwrap.indent(
                                f"""

# set ColumnElement[_T] as a separate overload, to appease
# mypy which seems to not want to accept _T from
# _ColumnExpressionArgument. Seems somewhat related to the covariant
# _HasClauseElement as of mypy 1.15

@overload
def {key}( {'  # noqa: A001' if is_reserved_word else ''}
    self,
    col: ColumnElement[_T],
    *args: _ColumnExpressionOrLiteralArgument[Any],
    **kwargs: Any,
) -> {class_name}[_T]:
    ...

@overload
def {key}( {'  # noqa: A001' if is_reserved_word else ''}
    self,
    col: _ColumnExpressionArgument[{_TEE}],
    *args: _ColumnExpressionOrLiteralArgument[Any],
    **kwargs: Any,
) -> {class_name}[_T]:
        ...

@overload
def {key}( {'  # noqa: A001' if is_reserved_word else ''}
    self,
    col: {_TEE},
    *args: _ColumnExpressionOrLiteralArgument[Any],
    **kwargs: Any,
) -> {class_name}[_T]:
        ...

def {key}( {'  # noqa: A001' if is_reserved_word else ''}
    self,
    col: _ColumnExpressionOrLiteralArgument[{_TEE}],
    *args: _ColumnExpressionOrLiteralArgument[Any],
    **kwargs: Any,
) -> {class_name}[_T]:
    ...

    """,
                                indent,
                            )
                        )
                    else:
                        guess_its_generic = bool(fn_class.__parameters__)

                        # the latest flake8 is quite broken here:
                        # 1. it insists on linting f-strings, no option
                        #    to turn it off
                        # 2. the f-string indentation rules are either broken
                        #    or completely impossible to figure out
                        # 3. there's no way to E501 a too-long f-string,
                        #    so I can't even put the expressions all one line
                        #    to get around the indentation errors
                        # 4. Therefore here I have to concat part of the
                        #    string outside of the f-string
                        _type = class_name
                        _type += "[Any]" if guess_its_generic else ""
                        _reserved_word = (
                            "  # noqa: A001" if is_reserved_word else ""
                        )

                        # now the f-string
                        buf.write(
                            textwrap.indent(
                                f"""
@property
def {key}(self) -> Type[{_type}]:{_reserved_word}
    ...

""",
                                indent,
                            )
                        )
                    orig_name = fn_class.__name__
                    alias_name = class_name
                    if guess_its_generic:
                        orig_name += "[_T]"
                    alias_mapping[orig_name] = alias_name

            m = re.match(
                r"^( *)# START GENERATED FUNCTION TYPING TESTS",
                line,
            )
            if m:
                in_block = True
                buf.write(line)
                indent = m.group(1)

                buf.write(
                    textwrap.indent(
                        """
# code within this block is **programmatically,
# statically generated** by tools/generate_sql_functions.py
""",
                        indent,
                    )
                )

                count = 0
                for key, fn_class in _fns_in_deterministic_order():
                    if issubclass(fn_class, ReturnTypeFromArgs):
                        count += 1

                        # Would be ReturnTypeFromArgs
                        (orig_base,) = typing_extensions.get_original_bases(
                            fn_class
                        )
                        # Type parameter of ReturnTypeFromArgs
                        (rtype,) = typing.get_args(orig_base)
                        # The origin type, if rtype is a generic
                        orig_type = typing.get_origin(rtype)
                        if orig_type is not None:
                            coltype = rf".*{orig_type.__name__}\[.*int\]"
                        else:
                            coltype = ".*int"

                        buf.write(
                            textwrap.indent(
                                rf"""

# test the {key}() function.
# this function is a ReturnTypeFromArgs type.

fn{count} = func.{key}(column('x', Integer))
assert_type(fn{count}, functions.{key}[int])

stmt{count} = select(func.{key}(column('x', Integer)))
# EXPECTED_RE_TYPE: .*Select\[Tuple\[{coltype}\]\]
reveal_type(stmt{count})


""",
                                indent,
                            )
                        )
                    elif fn_class.__name__ == "aggregate_strings":
                        count += 1
                        buf.write(
                            textwrap.indent(
                                rf"""

# test the aggregate_strings() function.
# this function is somewhat special case.

stmt{count} = select(func.{key}(column('x', String), ','))
# EXPECTED_RE_TYPE: .*Select\[Tuple\[.*str\]\]
reveal_type(stmt{count})

""",
                                indent,
                            )
                        )

                    elif hasattr(fn_class, "type") and isinstance(
                        fn_class.type, TypeEngine
                    ):
                        python_type = fn_class.type.python_type
                        python_expr = rf"Tuple\[.*{python_type.__name__}\]"
                        argspec = inspect.getfullargspec(fn_class)
                        if fn_class.__name__ == "next_value":
                            args = "Sequence('x_seq')"
                        else:
                            args = ", ".join(
                                'column("x")' for elem in argspec.args[1:]
                            )
                        count += 1

                        buf.write(
                            textwrap.indent(
                                rf"""

# test the {key}() function.
# this function is fixed to the SQL {fn_class.type} class, or the {python_expr} type.

fn{count} = func.{key}({args})
assert_type(fn{count}, functions.{key})

stmt{count} = select(func.{key}({args}))
# EXPECTED_RE_TYPE: .*Select\[{python_expr}\]
reveal_type(stmt{count})

""",  # noqa: E501
                                indent,
                            )
                        )

            m = re.match(
                r"^( *)# START GENERATED FUNCTION ALIASES",
                line,
            )
            if m:
                in_block = True
                buf.write(line)
                indent = m.group(1)

                for name, alias in alias_mapping.items():
                    buf.write(f"{indent}{alias}: TypeAlias = {name}\n")

            if in_block and line.startswith(
                f"{indent}# END GENERATED FUNCTION"
            ):
                in_block = False

            if not in_block:
                buf.write(line)
    return buf.name


def main(cmd: code_writer_cmd) -> None:
    for path in [functions_py, test_functions_py]:
        destination_path = path
        tempfile = process_functions(destination_path, cmd)
        cmd.run_zimports(tempfile)
        cmd.run_black(tempfile)
        cmd.write_output_file_from_tempfile(tempfile, destination_path)


functions_py = "lib/sqlalchemy/sql/functions.py"
test_functions_py = "test/typing/plain_files/sql/functions.py"


if __name__ == "__main__":
    cmd = code_writer_cmd(__file__)

    with cmd.run_program():
        main(cmd)
