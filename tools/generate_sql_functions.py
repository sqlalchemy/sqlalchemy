"""Generate inline stubs for generic functions on func

"""
# mypy: ignore-errors

from __future__ import annotations

import inspect
import re
from tempfile import NamedTemporaryFile
import textwrap

from sqlalchemy.sql.functions import _registry
from sqlalchemy.types import TypeEngine
from sqlalchemy.util.tool_support import code_writer_cmd


def _fns_in_deterministic_order():
    reg = _registry["_default"]
    for key in sorted(reg):
        yield key, reg[key]


def process_functions(filename: str, cmd: code_writer_cmd) -> str:
    with NamedTemporaryFile(
        mode="w",
        delete=False,
        suffix=".py",
    ) as buf, open(filename) as orig_py:
        indent = ""
        in_block = False

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

                    guess_its_generic = bool(fn_class.__parameters__)

                    buf.write(
                        textwrap.indent(
                            f"""
@property
def {key}(self) -> Type[{fn_class.__name__}{
    '[Any]' if guess_its_generic else ''
}]:{
     '  # noqa: A001' if is_reserved_word else ''
}
    ...

""",
                            indent,
                        )
                    )

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
                    if hasattr(fn_class, "type") and isinstance(
                        fn_class.type, TypeEngine
                    ):
                        python_type = fn_class.type.python_type
                        python_expr = rf"Tuple\[.*{python_type.__name__}\]"
                        argspec = inspect.getfullargspec(fn_class)
                        args = ", ".join(
                            'column("x")' for elem in argspec.args[1:]
                        )
                        count += 1

                        buf.write(
                            textwrap.indent(
                                rf"""
stmt{count} = select(func.{key}({args}))

# EXPECTED_RE_TYPE: .*Select\[{python_expr}\]
reveal_type(stmt{count})

""",
                                indent,
                            )
                        )

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
test_functions_py = "test/ext/mypy/plain_files/functions.py"


if __name__ == "__main__":
    cmd = code_writer_cmd(__file__)

    with cmd.run_program():
        main(cmd)
