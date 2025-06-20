r"""Generate tuple mapping overloads.

the problem solved by this script is that of there's no way in current
pep-484 typing to unpack \*args: _T into Tuple[_T].  pep-646 is the first
pep to provide this, but it doesn't work for the actual Tuple class
and also mypy does not have support for pep-646 as of yet.  Better pep-646
support would allow us to use a TypeVarTuple with Unpack, but TypeVarTuple
does not have support for sequence operations like ``__getitem__`` and
iteration; there's also no way for TypeVarTuple to be translated back to a
Tuple which does have those things without a combinatoric hardcoding approach
to each length of tuple.

So here, the script creates a map from `*args` to a Tuple directly using a
combinatoric generated code approach.

.. versionadded:: 2.0

"""

# mypy: ignore-errors

from __future__ import annotations

import importlib
import os
from pathlib import Path
import re
import sys
from tempfile import NamedTemporaryFile
import textwrap

from sqlalchemy.util.tool_support import code_writer_cmd

is_posix = os.name == "posix"


sys.path.append(str(Path(__file__).parent.parent))


def process_module(
    modname: str, filename: str, expected_number: int, cmd: code_writer_cmd
) -> str:
    # use tempfile in same path as the module, or at least in the
    # current working directory, so that black / zimports use
    # local pyproject.toml
    found = 0
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
        current_fnname = given_fnname = None
        for line in orig_py:
            m = re.match(
                r"^( *)# START OVERLOADED FUNCTIONS ([\.\w_]+) ([\w_]+) (\d+)-(\d+)(?: \"(.+)\")?",  # noqa: E501
                line,
            )
            if m:
                found += 1
                indent = m.group(1)
                given_fnname = current_fnname = m.group(2)
                if current_fnname.startswith("self."):
                    use_self = True
                    current_fnname = current_fnname.split(".")[1]
                else:
                    use_self = False
                return_type = m.group(3)
                start_index = int(m.group(4))
                end_index = int(m.group(5))
                extra_args = m.group(6) or ""

                cmd.write_status(
                    f"Generating {start_index}-{end_index} overloads "
                    f"attributes for "
                    f"class {'self.' if use_self else ''}{current_fnname} "
                    f"-> {return_type}\n"
                )
                in_block = True
                buf.write(line)
                buf.write(
                    "\n    # code within this block is "
                    "**programmatically, \n"
                    "    # statically generated** by"
                    f" tools/{os.path.basename(__file__)}\n\n"
                )

                for num_args in range(start_index, end_index + 1):
                    ret_suffix = ""
                    combinations = [
                        f"__ent{arg}: _TCCA[_T{arg}]"
                        for arg in range(num_args)
                    ]

                    if num_args == end_index:
                        ret_suffix = ", Unpack[TupleAny]"
                        extra_args = (
                            f", *entities: _ColumnsClauseArgument[Any]"
                            f"{extra_args.replace(', *', '')}"
                        )

                    buf.write(
                        textwrap.indent(
                            f"""
@overload
def {current_fnname}(
    {'self, ' if use_self else ''}{", ".join(combinations)},/{extra_args}
) -> {return_type}[{', '.join(f'_T{i}' for i in range(num_args))}{ret_suffix}]:
    ...

""",  # noqa: E501
                            indent,
                        )
                    )

            if in_block and line.startswith(
                f"{indent}# END OVERLOADED FUNCTIONS {given_fnname}"
            ):
                in_block = False

            if not in_block:
                buf.write(line)
    if found != expected_number:
        raise Exception(
            f"{modname} processed {found}. expected {expected_number}"
        )
    return buf.name


def run_module(modname: str, count: int, cmd: code_writer_cmd) -> None:
    cmd.write_status(f"importing module {modname}\n")
    mod = importlib.import_module(modname)
    destination_path = mod.__file__
    assert destination_path is not None

    tempfile = process_module(modname, destination_path, count, cmd)

    cmd.run_zimports(tempfile)
    cmd.run_black(tempfile)
    cmd.write_output_file_from_tempfile(tempfile, destination_path)


def main(cmd: code_writer_cmd) -> None:
    for modname, count in entries:
        if cmd.args.module in {"all", modname}:
            run_module(modname, count, cmd)


entries = [
    ("sqlalchemy.sql._selectable_constructors", 1),
    ("sqlalchemy.orm.session", 1),
    ("sqlalchemy.orm.query", 1),
    ("sqlalchemy.sql.selectable", 1),
    ("sqlalchemy.sql.dml", 3),
]

if __name__ == "__main__":
    cmd = code_writer_cmd(__file__)

    with cmd.add_arguments() as parser:
        parser.add_argument(
            "--module",
            choices=[n for n, _ in entries] + ["all"],
            default="all",
            help="Which file to generate. Default is to regenerate all files",
        )

    with cmd.run_program():
        main(cmd)
