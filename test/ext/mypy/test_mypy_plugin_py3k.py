import os
import re
import shutil
import sys
import tempfile
from typing import Any
from typing import cast
from typing import List
from typing import Tuple

from sqlalchemy import testing
from sqlalchemy.testing import config
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures


def _file_combinations(dirname):
    path = os.path.join(os.path.dirname(__file__), dirname)
    files = []
    for f in os.listdir(path):
        if f.endswith(".py"):
            files.append(os.path.join(os.path.dirname(__file__), dirname, f))

    for extra_dir in testing.config.options.mypy_extra_test_paths:
        if extra_dir and os.path.isdir(extra_dir):
            for f in os.listdir(os.path.join(extra_dir, dirname)):
                if f.endswith(".py"):
                    files.append(os.path.join(extra_dir, dirname, f))
    return files


def _incremental_dirs():
    path = os.path.join(os.path.dirname(__file__), "incremental")
    files = []
    for d in os.listdir(path):
        if os.path.isdir(os.path.join(path, d)):
            files.append(
                os.path.join(os.path.dirname(__file__), "incremental", d)
            )

    for extra_dir in testing.config.options.mypy_extra_test_paths:
        if extra_dir and os.path.isdir(extra_dir):
            for d in os.listdir(os.path.join(extra_dir, "incremental")):
                if os.path.isdir(os.path.join(path, d)):
                    files.append(os.path.join(extra_dir, "incremental", d))
    return files


@testing.add_to_marker.mypy
class MypyPluginTest(fixtures.TestBase):
    __tags__ = ("mypy",)
    __requires__ = ("no_sqlalchemy2_stubs",)

    @testing.fixture(scope="function")
    def per_func_cachedir(self):
        yield from self._cachedir()

    @testing.fixture(scope="class")
    def cachedir(self):
        yield from self._cachedir()

    def _cachedir(self):
        # as of mypy 0.971 i think we need to keep mypy_path empty
        mypy_path = ""

        with tempfile.TemporaryDirectory() as cachedir:
            with open(
                os.path.join(cachedir, "sqla_mypy_config.cfg"), "w"
            ) as config_file:
                config_file.write(
                    f"""
                    [mypy]\n
                    plugins = sqlalchemy.ext.mypy.plugin\n
                    show_error_codes = True\n
                    {mypy_path}
                    disable_error_code = no-untyped-call

                    [mypy-sqlalchemy.*]
                    ignore_errors = True

                    """
                )
            with open(
                os.path.join(cachedir, "plain_mypy_config.cfg"), "w"
            ) as config_file:
                config_file.write(
                    f"""
                    [mypy]\n
                    show_error_codes = True\n
                    {mypy_path}
                    disable_error_code = var-annotated,no-untyped-call
                    [mypy-sqlalchemy.*]
                    ignore_errors = True

                    """
                )
            yield cachedir

    @testing.fixture()
    def mypy_runner(self, cachedir):
        from mypy import api

        def run(path, use_plugin=True, incremental=False):
            args = [
                "--strict",
                "--raise-exceptions",
                "--cache-dir",
                cachedir,
                "--config-file",
                os.path.join(
                    cachedir,
                    "sqla_mypy_config.cfg"
                    if use_plugin
                    else "plain_mypy_config.cfg",
                ),
            ]

            # mypy as of 0.990 is more aggressively blocking messaging
            # for paths that are in sys.path, and as pytest puts currdir,
            # test/ etc in sys.path, just copy the source file to the
            # tempdir we are working in so that we don't have to try to
            # manipulate sys.path and/or guess what mypy is doing
            filename = os.path.basename(path)
            test_program = os.path.join(cachedir, filename)
            shutil.copyfile(path, test_program)
            args.append(test_program)

            # I set this locally but for the suite here needs to be
            # disabled
            os.environ.pop("MYPY_FORCE_COLOR", None)

            result = api.run(args)
            return result

        return run

    @testing.combinations(
        *[
            (pathname, testing.exclusions.closed())
            for pathname in _incremental_dirs()
        ],
        argnames="pathname",
    )
    @testing.requires.patch_library
    def test_incremental(self, mypy_runner, per_func_cachedir, pathname):
        import patch

        cachedir = per_func_cachedir

        dest = os.path.join(cachedir, "mymodel")
        os.mkdir(dest)

        patches = set()

        print("incremental test: %s" % pathname)

        for fname in os.listdir(pathname):
            if fname.endswith(".py"):
                shutil.copy(
                    os.path.join(pathname, fname), os.path.join(dest, fname)
                )
                print("copying to: %s" % os.path.join(dest, fname))
            elif fname.endswith(".testpatch"):
                patches.add(fname)

        for patchfile in [None] + sorted(patches):
            if patchfile is not None:
                print("Applying patchfile %s" % patchfile)
                patch_obj = patch.fromfile(os.path.join(pathname, patchfile))
                assert patch_obj.apply(1, dest), (
                    "pathfile %s failed" % patchfile
                )
            print("running mypy against %s" % dest)
            result = mypy_runner(
                dest,
                use_plugin=True,
                incremental=True,
            )
            eq_(
                result[2],
                0,
                msg="Failure after applying patch %s: %s"
                % (patchfile, result[0]),
            )

    @testing.combinations(
        *(
            cast(
                List[Tuple[Any, ...]],
                [
                    ("w_plugin", os.path.basename(path), path, True)
                    for path in _file_combinations("plugin_files")
                ],
            )
            + cast(
                List[Tuple[Any, ...]],
                [
                    ("plain", os.path.basename(path), path, False)
                    for path in _file_combinations("plain_files")
                ],
            )
        ),
        argnames="filename,path,use_plugin",
        id_="isaa",
    )
    def test_files(self, mypy_runner, filename, path, use_plugin):

        expected_messages = []
        expected_re = re.compile(r"\s*# EXPECTED(_MYPY)?(_RE)?(_TYPE)?: (.+)")
        py_ver_re = re.compile(r"^#\s*PYTHON_VERSION\s?>=\s?(\d+\.\d+)")
        with open(path) as file_:
            current_assert_messages = []
            for num, line in enumerate(file_, 1):
                m = py_ver_re.match(line)
                if m:
                    major, _, minor = m.group(1).partition(".")
                    if sys.version_info < (int(major), int(minor)):
                        config.skip_test(
                            "Requires python >= %s" % (m.group(1))
                        )
                    continue
                if line.startswith("# NOPLUGINS"):
                    use_plugin = False
                    continue

                m = expected_re.match(line)
                if m:
                    is_mypy = bool(m.group(1))
                    is_re = bool(m.group(2))
                    is_type = bool(m.group(3))

                    expected_msg = re.sub(r"# noqa[:]? ?.*", "", m.group(4))
                    if is_type:
                        if not is_re:
                            # the goal here is that we can cut-and-paste
                            # from vscode -> pylance into the
                            # EXPECTED_TYPE: line, then the test suite will
                            # validate that line against what mypy produces
                            expected_msg = re.sub(
                                r"([\[\]])",
                                lambda m: rf"\{m.group(0)}",
                                expected_msg,
                            )

                            # note making sure preceding text matches
                            # with a dot, so that an expect for "Select"
                            # does not match "TypedSelect"
                            expected_msg = re.sub(
                                r"([\w_]+)",
                                lambda m: rf"(?:.*\.)?{m.group(1)}\*?",
                                expected_msg,
                            )

                            expected_msg = re.sub(
                                "List", "builtins.list", expected_msg
                            )

                            expected_msg = re.sub(
                                r"\b(int|str|float|bool)\b",
                                lambda m: rf"builtins.{m.group(0)}\*?",
                                expected_msg,
                            )
                            # expected_msg = re.sub(
                            #     r"(Sequence|Tuple|List|Union)",
                            #     lambda m: fr"typing.{m.group(0)}\*?",
                            #     expected_msg,
                            # )

                        is_mypy = is_re = True
                        expected_msg = f'Revealed type is "{expected_msg}"'
                    current_assert_messages.append(
                        (is_mypy, is_re, expected_msg.strip())
                    )
                elif current_assert_messages:
                    expected_messages.extend(
                        (num, is_mypy, is_re, expected_msg)
                        for (
                            is_mypy,
                            is_re,
                            expected_msg,
                        ) in current_assert_messages
                    )
                    current_assert_messages[:] = []

        result = mypy_runner(path, use_plugin=use_plugin)

        not_located = []

        if expected_messages:
            # mypy 0.990 changed how return codes work, so don't assume a
            # 1 or a 0 return code here, could be either depending on if
            # errors were generated or not

            output = []

            raw_lines = result[0].split("\n")
            while raw_lines:
                e = raw_lines.pop(0)
                if re.match(r".+\.py:\d+: error: .*", e):
                    output.append(("error", e))
                elif re.match(
                    r".+\.py:\d+: note: +(?:Possible overload|def ).*", e
                ):
                    while raw_lines:
                        ol = raw_lines.pop(0)
                        if not re.match(r".+\.py:\d+: note: +def \[.*", ol):
                            break
                elif re.match(
                    r".+\.py:\d+: note: .*(?:perhaps|suggestion)", e, re.I
                ):
                    pass
                elif re.match(r".+\.py:\d+: note: .*", e):
                    output.append(("note", e))

            for num, is_mypy, is_re, msg in expected_messages:
                msg = msg.replace("'", '"')
                prefix = "[SQLAlchemy Mypy plugin] " if not is_mypy else ""
                for idx, (typ, errmsg) in enumerate(output):
                    if is_re:
                        if re.match(
                            rf".*{filename}\:{num}\: {typ}\: {prefix}{msg}",  # noqa: E501
                            errmsg,
                        ):
                            break
                    elif (
                        f"{filename}:{num}: {typ}: {prefix}{msg}"
                        in errmsg.replace("'", '"')
                    ):
                        break
                else:
                    not_located.append(msg)
                    continue
                del output[idx]

            if not_located:
                print(f"Couldn't locate expected messages: {not_located}")
                print("\n".join(msg for _, msg in output))
                assert False, "expected messages not found, see stdout"

            if output:
                print(f"{len(output)} messages from mypy were not consumed:")
                print("\n".join(msg for _, msg in output))
                assert False, "errors and/or notes remain, see stdout"

        else:
            if result[2] != 0:
                print(result[0])

            eq_(result[2], 0, msg=result)
