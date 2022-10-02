from argparse import ArgumentParser
from argparse import RawDescriptionHelpFormatter
from collections.abc import Iterator
from pathlib import Path
import re

from black import format_str
from black.const import DEFAULT_LINE_LENGTH
from black.files import parse_pyproject_toml
from black.mode import Mode
from black.mode import TargetVersion


home = Path(__file__).parent.parent

_Block = list[
    tuple[
        str,
        int,
        str | None,
        str | None,
        str,
    ]
]


def _format_block(
    input_block: _Block,
    exit_on_error: bool,
    errors: list[tuple[int, str, Exception]],
    is_doctest: bool,
) -> list[str]:
    if not is_doctest:
        # The first line may have additional padding. Remove then restore later
        add_padding = start_space.match(input_block[0][4]).groups()[0]
        skip = len(add_padding)
        code = "\n".join(
            c[skip:] if c.startswith(add_padding) else c
            for *_, c in input_block
        )
    else:
        add_padding = None
        code = "\n".join(c for *_, c in input_block)

    try:
        formatted = format_str(code, mode=BLACK_MODE)
    except Exception as e:
        start_line = input_block[0][1]
        errors.append((start_line, code, e))
        if is_doctest:
            print(
                "Could not format code block starting at "
                f"line {start_line}:\n{code}\nError: {e}"
            )
            if exit_on_error:
                print("Exiting since --exit-on-error was passed")
                raise
            else:
                print("Ignoring error")
        elif VERBOSE:
            print(
                "Could not format code block starting at "
                f"line {start_line}:\n---\n{code}\n---Error: {e}"
            )
        return [line for line, *_ in input_block]
    else:
        formatted_code_lines = formatted.splitlines()
        padding = input_block[0][2]
        sql_prefix = input_block[0][3] or ""

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
            if not input_block[-1][0] and formatted_lines[-1]:
                # last line was empty and black removed it. restore it
                formatted_lines.append("")
        return formatted_lines


format_directive = re.compile(r"^\.\.\s*format\s*:\s*(on|off)\s*$")

doctest_code_start = re.compile(r"^(\s+)({(?:opensql|sql|stop)})?>>>\s?(.+)")
doctest_code_continue = re.compile(r"^\s+\.\.\.\s?(\s*.*)")
sql_code_start = re.compile(r"^(\s+){(?:open)?sql}")
sql_code_stop = re.compile(r"^(\s+){stop}")

start_code_section = re.compile(
    r"^(((?!\.\.).+::)|(\.\.\s*sourcecode::(.*py.*)?)|(::))$"
)
start_space = re.compile(r"^(\s*)[^ ]?")


def format_file(
    file: Path, exit_on_error: bool, check: bool, no_plain: bool
) -> tuple[bool, int]:
    buffer = []
    if not check:
        print(f"Running file {file} ..", end="")
    original = file.read_text("utf-8")
    doctest_block: _Block | None = None
    plain_block: _Block | None = None

    plain_code_section = False
    plain_padding = None
    plain_padding_len = None
    sql_section = False

    errors = []

    disable_format = False
    for line_no, line in enumerate(original.splitlines(), 1):
        # start_code_section requires no spaces at the start

        if start_code_section.match(line.strip()):
            if plain_block:
                buffer.extend(
                    _format_block(
                        plain_block, exit_on_error, errors, is_doctest=False
                    )
                )
                plain_block = None
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
            disable_format = match.groups()[0] == "off"

        if doctest_block:
            assert not plain_block
            if match := doctest_code_continue.match(line):
                doctest_block.append(
                    (line, line_no, None, None, match.groups()[0])
                )
                continue
            else:
                buffer.extend(
                    _format_block(
                        doctest_block, exit_on_error, errors, is_doctest=True
                    )
                )
                doctest_block = None
        elif plain_block:
            if (
                plain_code_section
                and not doctest_code_start.match(line)
                and not sql_code_start.match(line)
            ):
                plain_block.append(
                    (line, line_no, None, None, line[plain_padding_len:])
                )
                continue
            else:
                buffer.extend(
                    _format_block(
                        plain_block, exit_on_error, errors, is_doctest=False
                    )
                )
                plain_block = None

        if line and (match := doctest_code_start.match(line)):
            plain_code_section = sql_section = False
            if plain_block:
                buffer.extend(
                    _format_block(
                        plain_block, exit_on_error, errors, is_doctest=False
                    )
                )
                plain_block = None
            padding, code = match.group(1, 3)
            doctest_block = [(line, line_no, padding, match.group(2), code)]
        elif (
            line
            and plain_code_section
            and (match := sql_code_start.match(line))
        ):
            if plain_block:
                buffer.extend(
                    _format_block(
                        plain_block, exit_on_error, errors, is_doctest=False
                    )
                )
                plain_block = None

            sql_section = True
            buffer.append(line)
        elif line and sql_section and (match := sql_code_stop.match(line)):
            sql_section = False
            orig_line = line
            line = line.replace("{stop}", "")
            assert not doctest_block
            # start of a plain block
            if line.strip():
                plain_block = [
                    (
                        line,
                        line_no,
                        plain_padding,
                        "{stop}",
                        line[plain_padding_len:],
                    )
                ]
            else:
                buffer.append(orig_line)

        elif (
            line
            and not no_plain
            and not disable_format
            and plain_code_section
            and not sql_section
        ):
            assert not doctest_block
            # start of a plain block
            plain_block = [
                (line, line_no, plain_padding, None, line[plain_padding_len:])
            ]
        else:
            buffer.append(line)

    if doctest_block:
        buffer.extend(
            _format_block(
                doctest_block, exit_on_error, errors, is_doctest=True
            )
        )
    if plain_block:
        buffer.extend(
            _format_block(plain_block, exit_on_error, errors, is_doctest=False)
        )
    if buffer:
        # if there is nothing in the buffer something strange happened so
        # don't do anything
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
        if not check:
            print(".. Nothing to write")
        equal = bool(original) is False

    if check:
        if not equal:
            print(f"File {file} would be formatted")
    return equal, len(errors)


def iter_files(directory) -> Iterator[Path]:
    yield from (home / directory).glob("./**/*.rst")


def main(
    file: str | None,
    directory: str,
    exit_on_error: bool,
    check: bool,
    no_plain: bool,
):
    if file is not None:
        result = [format_file(Path(file), exit_on_error, check, no_plain)]
    else:
        result = [
            format_file(doc, exit_on_error, check, no_plain)
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
                    f"{sum(formatting_error_counts)} formatting errors "
                    f"reported in {len(formatting_error_counts)} files"
                )
                if formatting_error_counts
                else "no formatting errors reported",
            )

            # interim, until we fix all formatting errors
            if not to_reformat:
                exit(0)
            exit(1)


if __name__ == "__main__":
    parser = ArgumentParser(
        description="""Formats code inside docs using black. Supports \
doctest code blocks and also tries to format plain code block identifies as \
all indented blocks of at least 4 spaces, unless '--no-plain' is specified.

Plain code block may lead to false positive. To disable formatting on a \
file section the comment ``.. format: off`` disables formatting until \
``.. format: on`` is encountered or the file ends.
Another alterative is to use less than 4 spaces to indent the code block.
""",
        formatter_class=RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "-f", "--file", help="Format only this file instead of all docs"
    )
    parser.add_argument(
        "-d",
        "--directory",
        help="Find documents in this directory and its sub dirs",
        default="doc/build",
    )
    parser.add_argument(
        "-c",
        "--check",
        help="Don't write the files back, just return the "
        "status. Return code 0 means nothing would change. "
        "Return code 1 means some files would be reformatted.",
        action="store_true",
    )
    parser.add_argument(
        "-e",
        "--exit-on-error",
        help="Exit in case of black format error instead of ignoring it. "
        "This option is only valid for doctest code blocks",
        action="store_true",
    )
    parser.add_argument(
        "-l",
        "--project-line-length",
        help="Configure the line length to the project value instead "
        "of using the black default of 88",
        action="store_true",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        help="Increase verbosity",
        action="store_true",
    )
    parser.add_argument(
        "-n",
        "--no-plain",
        help="Disable plain code blocks formatting that's more difficult "
        "to parse compared to doctest code blocks",
        action="store_true",
    )
    args = parser.parse_args()

    config = parse_pyproject_toml(home / "pyproject.toml")
    BLACK_MODE = Mode(
        target_versions=set(
            TargetVersion[val.upper()]
            for val in config.get("target_version", [])
            if val != "py27"
        ),
        line_length=config.get("line_length", DEFAULT_LINE_LENGTH)
        if args.project_line_length
        else DEFAULT_LINE_LENGTH,
    )
    VERBOSE = args.verbose

    main(
        args.file,
        args.directory,
        args.exit_on_error,
        args.check,
        args.no_plain,
    )
