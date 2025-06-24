"""Format the code blocks in the documentation using black.

this script parses the documentation files and runs black on the code blocks
that it extracts from the documentation.

.. versionadded:: 2.0

"""

# mypy: ignore-errors

from argparse import ArgumentParser
from argparse import RawDescriptionHelpFormatter
from collections.abc import Iterator
import dataclasses
from functools import partial
from itertools import chain
from pathlib import Path
import re
from typing import NamedTuple

from black import format_str
from black.const import DEFAULT_LINE_LENGTH
from black.files import parse_pyproject_toml
from black.mode import Mode
from black.mode import TargetVersion


home = Path(__file__).parent.parent
ignore_paths = (
    re.compile(r"changelog/unreleased_\d{2}"),
    re.compile(r"README\.unittests\.rst"),
    re.compile(r"\.tox"),
    re.compile(rf"{home.as_posix()}/build"),
)

CUSTOM_TARGET_VERSIONS = {"declarative_tables.rst": "PY312"}


class BlockLine(NamedTuple):
    line: str
    line_no: int
    code: str
    padding: str | None = None  # relevant only on first line of block
    sql_marker: str | None = None


_Block = list[BlockLine]


def _format_block(
    input_block: _Block,
    exit_on_error: bool,
    errors: list[tuple[int, str, Exception]],
    is_doctest: bool,
    file: str,
    is_python_file: bool,
) -> list[str]:
    if not is_doctest:
        # The first line may have additional padding. Remove then restore later
        add_padding = start_space.match(input_block[0].code).groups()[0]
        skip = len(add_padding)
        code = "\n".join(
            l.code[skip:] if l.code.startswith(add_padding) else l.code
            for l in input_block
        )
    else:
        add_padding = None
        code = "\n".join(l.code for l in input_block)

    mode = PYTHON_BLACK_MODE if is_python_file else RST_BLACK_MODE
    custom_target = CUSTOM_TARGET_VERSIONS.get(Path(file).name)
    if custom_target:
        mode = dataclasses.replace(
            mode, target_versions={TargetVersion[custom_target]}
        )

    try:
        formatted = format_str(code, mode=mode)
    except Exception as e:
        start_line = input_block[0].line_no
        first_error = not errors
        if not REPORT_ONLY_DOCTEST or is_doctest:
            type_ = "doctest" if is_doctest else "plain"
            errors.append((start_line, code, e))
            if first_error:
                print()  # add newline
            print(
                f"--- {file}:{start_line} Could not format {type_} code "
                f"block:\n{code}\n---Error: {e}"
            )
            if exit_on_error:
                print("Exiting since --exit-on-error was passed")
                raise
            else:
                print("Ignoring error")
        return [l.line for l in input_block]
    else:
        formatted_code_lines = formatted.splitlines()
        padding = input_block[0].padding
        sql_prefix = input_block[0].sql_marker or ""

        if is_doctest:
            formatted_lines = [
                f"{padding}{sql_prefix}>>> {formatted_code_lines[0]}",
                *(
                    f"{padding}...{' ' if fcl else ''}{fcl}"
                    for fcl in formatted_code_lines[1:]
                ),
            ]
        else:
            formatted_lines = [
                f"{padding}{add_padding}{sql_prefix}{formatted_code_lines[0]}",
                *(
                    f"{padding}{add_padding}{fcl}" if fcl else fcl
                    for fcl in formatted_code_lines[1:]
                ),
            ]
            if not input_block[-1].line and formatted_lines[-1]:
                # last line was empty and black removed it. restore it
                formatted_lines.append("")
        return formatted_lines


format_directive = re.compile(r"^\.\.\s*format\s*:\s*(on|off)\s*$")

doctest_code_start = re.compile(
    r"^(\s+)({(?:opensql|execsql|printsql|sql|stop)})?>>>\s?(.+)"
)
doctest_code_continue = re.compile(r"^\s+\.\.\.\s?(\s*.*)")

sql_code_start = re.compile(r"^(\s+)({(?:open|print|exec)?sql})")
sql_code_stop = re.compile(r"^(\s+){stop}")

start_code_section = re.compile(
    r"^(((?!\.\.).+::)|(\.\.\s*sourcecode::(.*py.*)?)|(::))$"
)
start_space = re.compile(r"^(\s*)[^ ]?")
not_python_line = re.compile(r"^\s+[$:]")


def format_file(
    file: Path, exit_on_error: bool, check: bool
) -> tuple[bool, int]:
    buffer = []
    if not check:
        print(f"Running file {file} ..", end="")
    original = file.read_text("utf-8")
    doctest_block: _Block | None = None
    plain_block: _Block | None = None

    is_python_file = file.suffix == ".py"

    plain_code_section = False
    plain_padding = None
    plain_padding_len = None
    sql_section = False

    errors = []

    do_doctest_format = partial(
        _format_block,
        exit_on_error=exit_on_error,
        errors=errors,
        is_doctest=True,
        file=str(file),
        is_python_file=is_python_file,
    )

    def doctest_format():
        nonlocal doctest_block
        if doctest_block:
            buffer.extend(do_doctest_format(doctest_block))
            doctest_block = None

    do_plain_format = partial(
        _format_block,
        exit_on_error=exit_on_error,
        errors=errors,
        is_doctest=False,
        file=str(file),
        is_python_file=is_python_file,
    )

    def plain_format():
        nonlocal plain_block
        if plain_block:
            buffer.extend(do_plain_format(plain_block))
            plain_block = None

    disable_format = False
    for line_no, line in enumerate(original.splitlines(), 1):
        if (
            line
            and not disable_format
            and start_code_section.match(line.strip())
        ):
            # start_code_section regexp requires no spaces at the start
            plain_format()
            plain_code_section = True
            assert not sql_section
            plain_padding = start_space.match(line).groups()[0]
            plain_padding_len = len(plain_padding)
            buffer.append(line)
            continue
        elif (
            plain_code_section
            and line.strip()
            and not line.startswith(" " * (plain_padding_len + 1))
        ):
            plain_code_section = sql_section = False
        elif match := format_directive.match(line):
            assert not plain_code_section
            disable_format = match.groups()[0] == "off"

        if doctest_block:
            assert not plain_block
            if match := doctest_code_continue.match(line):
                doctest_block.append(
                    BlockLine(line, line_no, match.groups()[0])
                )
                continue
            else:
                doctest_format()
        elif plain_block:
            if (
                plain_code_section
                and not doctest_code_start.match(line)
                and not sql_code_start.match(line)
            ):
                plain_block.append(
                    BlockLine(line, line_no, line[plain_padding_len:])
                )
                continue
            else:
                plain_format()

        if line and (match := doctest_code_start.match(line)):
            # the line is in a doctest
            plain_code_section = sql_section = False
            plain_format()
            padding, sql_marker, code = match.groups()
            doctest_block = [
                BlockLine(line, line_no, code, padding, sql_marker)
            ]
        elif line and plain_code_section:
            assert not disable_format
            assert not doctest_block
            if match := sql_code_start.match(line):
                plain_format()
                sql_section = True
                buffer.append(line)
            elif sql_section:
                if match := sql_code_stop.match(line):
                    sql_section = False
                    no_stop_line = line.replace("{stop}", "")
                    # start of a plain block
                    if no_stop_line.strip():
                        assert not plain_block
                        plain_block = [
                            BlockLine(
                                line,
                                line_no,
                                no_stop_line[plain_padding_len:],
                                plain_padding,
                                "{stop}",
                            )
                        ]
                        continue
                buffer.append(line)
            elif (
                is_python_file
                and not plain_block
                and not_python_line.match(line)
            ):
                # not a python block. ignore it
                plain_code_section = False
                buffer.append(line)
            else:
                # start of a plain block
                assert not doctest_block
                plain_block = [
                    BlockLine(
                        line,
                        line_no,
                        line[plain_padding_len:],
                        plain_padding,
                    )
                ]
        else:
            buffer.append(line)

    doctest_format()
    plain_format()
    if buffer:
        buffer.append("")
        updated = "\n".join(buffer)
        equal = original == updated
        if not check:
            print(
                f"..done. {len(errors)} error(s).",
                "No changes" if equal else "Changes detected",
            )
            if not equal:
                # write only if there are changes to write
                file.write_text(updated, "utf-8", newline="\n")
    else:
        # if there is nothing in the buffer something strange happened so
        # don't do anything
        if not check:
            print(".. Nothing to write")
        equal = bool(original) is False

    if check:
        if not equal:
            print(f"File {file} would be formatted")
    return equal, len(errors)


def iter_files(directory: str) -> Iterator[Path]:
    dir_path = home / directory
    yield from (
        file
        for file in chain(
            dir_path.glob("./**/*.rst"), dir_path.glob("./**/*.py")
        )
        if not any(pattern.search(file.as_posix()) for pattern in ignore_paths)
    )


def main(
    file: list[str] | None, directory: str, exit_on_error: bool, check: bool
):
    if file is not None:
        result = [format_file(Path(f), exit_on_error, check) for f in file]
    else:
        result = [
            format_file(doc, exit_on_error, check)
            for doc in iter_files(directory)
        ]

    if check:
        formatting_error_counts = [e for _, e in result if e]
        to_reformat = len([b for b, _ in result if not b])

        if not to_reformat and not formatting_error_counts:
            print("All files are correctly formatted")
            exit(0)
        else:
            print(
                f"{to_reformat} file(s) would be reformatted;",
                (
                    (
                        f"{sum(formatting_error_counts)} formatting errors "
                        f"reported in {len(formatting_error_counts)} files"
                    )
                    if formatting_error_counts
                    else "no formatting errors reported"
                ),
            )

            exit(1)


if __name__ == "__main__":
    parser = ArgumentParser(
        description="""Formats code inside docs using black. Supports \
doctest code blocks and plain code block identified as indented sections \
that are preceded by ``::`` or ``.. sourcecode:: py``.

To disable formatting on a file section the comment ``.. format: off`` \
disables formatting until ``.. format: on`` is encountered or the file ends.

Use --report-doctest to ignore errors on plain code blocks.
""",
        formatter_class=RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "-f",
        "--file",
        help="Format only this file instead of all docs",
        nargs="+",
    )
    parser.add_argument(
        "-d",
        "--directory",
        help="Find documents in this directory and its sub dirs",
        default=".",
    )
    parser.add_argument(
        "-c",
        "--check",
        help="Don't write the files back, just return the "
        "status. Return code 0 means nothing would change. "
        "Return code 1 means some files would be reformatted",
        action="store_true",
    )
    parser.add_argument(
        "-e",
        "--exit-on-error",
        help="Exit in case of black format error instead of ignoring it",
        action="store_true",
    )
    parser.add_argument(
        "-l",
        "--project-line-length",
        help="Configure the line length to the project value instead "
        "of using the black default of 88. Python files always use the"
        "project line length",
        action="store_true",
    )
    parser.add_argument(
        "-rd",
        "--report-doctest",
        help="Report errors only when running doctest blocks. When active "
        "exit-on-error will be valid only on doctest blocks",
        action="store_true",
    )
    args = parser.parse_args()

    config = parse_pyproject_toml(home / "pyproject.toml")
    target_versions = {
        TargetVersion[val.upper()]
        for val in config.get("target_version", [])
        if val != "py27"
    }

    RST_BLACK_MODE = Mode(
        target_versions=target_versions,
        line_length=(
            config.get("line_length", DEFAULT_LINE_LENGTH)
            if args.project_line_length
            else DEFAULT_LINE_LENGTH
        ),
    )
    PYTHON_BLACK_MODE = Mode(
        target_versions=target_versions,
        # Remove a few char to account for normal indent
        line_length=(config.get("line_length", 4) - 4 or DEFAULT_LINE_LENGTH),
    )
    REPORT_ONLY_DOCTEST = args.report_doctest

    main(args.file, args.directory, args.exit_on_error, args.check)
