import os
import re
import tempfile

from sqlalchemy import testing
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures


class MypyPluginTest(fixtures.TestBase):
    __requires__ = ("sqlalchemy2_stubs",)

    @testing.fixture(scope="class")
    def cachedir(self):
        with tempfile.TemporaryDirectory() as cachedir:
            with open(
                os.path.join(cachedir, "sqla_mypy_config.cfg"), "w"
            ) as config_file:
                config_file.write(
                    """
                    [mypy]\n
                    plugins = sqlalchemy.ext.mypy.plugin\n
                    """
                )
            with open(
                os.path.join(cachedir, "plain_mypy_config.cfg"), "w"
            ) as config_file:
                config_file.write(
                    """
                    [mypy]\n
                    """
                )
            yield cachedir

    @testing.fixture()
    def mypy_runner(self, cachedir):
        from mypy import api

        def run(filename, use_plugin=True):
            path = os.path.join(os.path.dirname(__file__), "files", filename)

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

            args.append(path)

            return api.run(args)

        return run

    def _file_combinations():
        path = os.path.join(os.path.dirname(__file__), "files")
        return [f for f in os.listdir(path) if f.endswith(".py")]

    @testing.combinations(
        *[(filename,) for filename in _file_combinations()],
        argnames="filename"
    )
    def test_mypy(self, mypy_runner, filename):
        path = os.path.join(os.path.dirname(__file__), "files", filename)

        use_plugin = True

        expected_errors = []
        with open(path) as file_:
            for num, line in enumerate(file_, 1):
                if line.startswith("# NOPLUGINS"):
                    use_plugin = False
                    continue

                m = re.match(r"\s*# EXPECTED(_MYPY)?: (.+)", line)
                if m:
                    is_mypy = bool(m.group(1))
                    expected_msg = m.group(2)
                    expected_msg = re.sub(r"# noqa ?.*", "", m.group(2))
                    expected_errors.append(
                        (num, is_mypy, expected_msg.strip())
                    )

        result = mypy_runner(filename, use_plugin=use_plugin)

        if expected_errors:
            eq_(result[2], 1)

            print(result[0])

            errors = []
            for e in result[0].split("\n"):
                if re.match(r".+\.py:\d+: error: .*", e):
                    errors.append(e)

            for num, is_mypy, msg in expected_errors:
                prefix = "[SQLAlchemy Mypy plugin] " if not is_mypy else ""
                for idx, errmsg in enumerate(errors):
                    if f"{filename}:{num + 1}: error: {prefix}{msg}" in errmsg:
                        break
                else:
                    continue
                del errors[idx]

            assert not errors, "errors remain: %s" % "\n".join(errors)

        else:
            eq_(result[2], 0, msg=result[0])
