from datetime import date
from pathlib import Path
import re

from sqlalchemy.util.tool_support import code_writer_cmd

sa_path = Path(__file__).parent.parent / "lib/sqlalchemy"


file_re = re.compile(r"^# [\w+/]+.(?:pyx?|pxd)$", re.MULTILINE)
license_re = re.compile(
    r"Copyright .C. (\d+)-\d+ the SQLAlchemy authors and contributors"
)

this_year = date.today().year
license_ = f"""
# Copyright (C) 2005-{this_year} the SQLAlchemy authors and \
contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php
"""


def run_file(cmd: code_writer_cmd, file: Path, update_year: bool):
    content = file.read_text("utf-8")
    path = str(file.relative_to(sa_path)).replace("\\", "/")  # handle windows
    path_comment = f"# {path}"
    has_license = bool(license_re.search(content))
    if file_re.match(content.strip()):
        if has_license:
            to_sub = path_comment
        else:
            to_sub = path_comment + license_
        content = file_re.sub(to_sub, content, count=1)
    else:
        content = path_comment + ("\n" if has_license else license_) + content

    if has_license and update_year:
        content = license_re.sub(
            rf"Copyright (C) \1-{this_year} the SQLAlchemy "
            "authors and contributors",
            content,
            1,
        )
    cmd.write_output_file_from_text(content, file)


def run(cmd: code_writer_cmd, update_year: bool):
    i = 0
    for ext in ("py", "pyx", "pxd"):
        for file in sa_path.glob(f"**/*.{ext}"):
            run_file(cmd, file, update_year)
            i += 1
    cmd.write_status(f"\nDone. Processed {i} files.")


if __name__ == "__main__":
    cmd = code_writer_cmd(__file__)
    with cmd.add_arguments() as parser:
        parser.add_argument(
            "--update-year",
            action="store_true",
            help="Update the year in the license files",
        )

    with cmd.run_program():
        run(cmd, cmd.args.update_year)
