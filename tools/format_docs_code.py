from argparse import ArgumentParser
from argparse import RawDescriptionHelpFormatter
from collections.abc import Iterator
from pathlib import Path
import re

from black import DEFAULT_LINE_LENGTH
from black import format_str
from black import Mode
from black import parse_pyproject_toml
from black import TargetVersion


home = Path(__file__).parent.parent

_Block = list[tuple[str, int, str | None, str]]


def _format_block(
    input_block: _Block, exit_on_error: bool, is_doctest: bool
) -> list[str]:
    code = "\n".join(c for *_, c in input_block)
    try:
        formatted = format_str(code, mode=BLACK_MODE)
    except Exception as e:
        if is_doctest:
            start_line = input_block[0][1]
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
            start_line = input_block[0][1]
            print(
                "Could not format code block starting at "
                f"line {start_line}:\n---\n{code}\n---Error: {e}"
            )
        return [line for line, *_ in input_block]
    else:
        formatted_code_lines = formatted.splitlines()
        padding = input_block[0][2]
        if is_doctest:
            formatted_lines = [
                f"{padding}>>> {formatted_code_lines[0]}",
                *(f"{padding}... {fcl}" for fcl in formatted_code_lines[1:]),
            ]
        else:
            # The first line may have additional padding.
            # If it does restore it
            additionalPadding = re.match(
                r"^(\s*)[^ ]?", input_block[0][3]
            ).groups()[0]
            formatted_lines = [
                f"{padding}{additionalPadding}{fcl}" if fcl else fcl
                for fcl in formatted_code_lines
            ]
            if not input_block[-1][0] and formatted_lines[-1]:
                # last line was empty and black removed it. restore it
                formatted_lines.append("")
        return formatted_lines


doctest_code_start = re.compile(r"^(\s+)>>>\s?(.+)")
doctest_code_continue = re.compile(r"^\s+\.\.\.\s?(\s*.*)")
plain_indent = re.compile(r"^(\s{4})(\s*[^: ].*)")
format_directive = re.compile(r"^\.\.\s*format\s*:\s*(on|off)\s*$")
dont_format_under_directive = re.compile(r"^\.\. (?:toctree)::\s*$")


def format_file(
    file: Path, exit_on_error: bool, check: bool, no_plain: bool
) -> bool | None:
    buffer = []
    if not check:
        print(f"Running file {file} ..", end="")
    original = file.read_text("utf-8")
    doctest_block: _Block | None = None
    plain_block: _Block | None = None
    last_line = None
    disable_format = False
    non_code_directive = False
    for line_no, line in enumerate(original.splitlines(), 1):
        if match := format_directive.match(line):
            disable_format = match.groups()[0] == "off"
        elif match := dont_format_under_directive.match(line):
            non_code_directive = True

        if doctest_block:
            assert not plain_block
            if match := doctest_code_continue.match(line):
                doctest_block.append((line, line_no, None, match.groups()[0]))
                continue
            else:
                buffer.extend(
                    _format_block(
                        doctest_block, exit_on_error, is_doctest=True
                    )
                )
                doctest_block = None

        if plain_block:
            assert not doctest_block
            if not line:
                plain_block.append((line, line_no, None, line))
                continue
            elif match := plain_indent.match(line):
                plain_block.append((line, line_no, None, match.groups()[1]))
                continue
            else:
                if non_code_directive:
                    buffer.extend(line for line, _, _, _ in plain_block)
                else:
                    buffer.extend(
                        _format_block(
                            plain_block, exit_on_error, is_doctest=False
                        )
                    )
                plain_block = None
                non_code_directive = False

        if match := doctest_code_start.match(line):
            if plain_block:
                buffer.extend(
                    _format_block(plain_block, exit_on_error, is_doctest=False)
                )
                plain_block = None
            padding, code = match.groups()
            doctest_block = [(line, line_no, padding, code)]
        elif (
            not no_plain
            and not disable_format
            and not last_line
            and (match := plain_indent.match(line))
        ):
            # print('start plain', line)
            assert not doctest_block
            # start of a plain block
            padding, code = match.groups()
            plain_block = [(line, line_no, padding, code)]
        else:
            buffer.append(line)
        last_line = line

    if doctest_block:
        buffer.extend(
            _format_block(doctest_block, exit_on_error, is_doctest=True)
        )
    if plain_block:
        if non_code_directive:
            buffer.extend(line for line, _, _, _ in plain_block)
        else:
            buffer.extend(
                _format_block(plain_block, exit_on_error, is_doctest=False)
            )
    if buffer:
        # if there is nothing in the buffer something strange happened so
        # don't do anything
        buffer.append("")
        updated = "\n".join(buffer)
        equal = original == updated
        if not check:
            print("..done. ", "No changes" if equal else "Changes detected")
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
        return equal
    else:
        return None


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
        if all(result):
            print("All files are correctly formatted")
            exit(0)
        else:
            print("Some file would be reformated")
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
