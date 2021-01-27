from distutils.command.build_ext import build_ext
from distutils.errors import CCompilerError
from distutils.errors import DistutilsExecError
from distutils.errors import DistutilsPlatformError
import os
import platform
import re
import sys

from setuptools import Distribution as _Distribution
from setuptools import Extension
from setuptools import setup
from setuptools.command.test import test as TestCommand


cmdclass = {}

cpython = platform.python_implementation() == "CPython"

ext_errors = (CCompilerError, DistutilsExecError, DistutilsPlatformError)
if sys.platform == "win32":
    # Work around issue https://github.com/pypa/setuptools/issues/1902
    ext_errors += (IOError, TypeError)
    extra_compile_args = []
elif sys.platform in ("linux", "linux2"):
    # warn for undefined symbols in .c files
    extra_compile_args = ["-Wundef", "-Werror=implicit-function-declaration"]
else:
    extra_compile_args = []

ext_modules = [
    Extension(
        "sqlalchemy.cprocessors",
        sources=["lib/sqlalchemy/cextension/processors.c"],
        extra_compile_args=extra_compile_args,
    ),
    Extension(
        "sqlalchemy.cresultproxy",
        sources=["lib/sqlalchemy/cextension/resultproxy.c"],
        extra_compile_args=extra_compile_args,
    ),
    Extension(
        "sqlalchemy.cimmutabledict",
        sources=["lib/sqlalchemy/cextension/immutabledict.c"],
        extra_compile_args=extra_compile_args,
    ),
    Extension(
        "sqlalchemy.cutils",
        sources=["lib/sqlalchemy/cextension/utils.c"],
        extra_compile_args=extra_compile_args,
    ),
]


class BuildFailed(Exception):
    def __init__(self):
        self.cause = sys.exc_info()[1]  # work around py 2/3 different syntax


class ve_build_ext(build_ext):
    # This class allows C extension building to fail.

    def run(self):
        try:
            build_ext.run(self)
        except DistutilsPlatformError:
            raise BuildFailed()

    def build_extension(self, ext):
        try:
            build_ext.build_extension(self, ext)
        except ext_errors:
            raise BuildFailed()
        except ValueError:
            # this can happen on Windows 64 bit, see Python issue 7511
            if "'path'" in str(sys.exc_info()[1]):  # works with both py 2/3
                raise BuildFailed()
            raise


cmdclass["build_ext"] = ve_build_ext


class Distribution(_Distribution):
    def has_ext_modules(self):
        # We want to always claim that we have ext_modules. This will be fine
        # if we don't actually have them (such as on PyPy) because nothing
        # will get built, however we don't want to provide an overally broad
        # Wheel package when building a wheel without C support. This will
        # ensure that Wheel knows to treat us as if the build output is
        # platform specific.
        return True


class UseTox(TestCommand):
    RED = 31
    RESET_SEQ = "\033[0m"
    BOLD_SEQ = "\033[1m"
    COLOR_SEQ = "\033[1;%dm"

    def run_tests(self):
        sys.stderr.write(
            "%s%spython setup.py test is deprecated by pypa.  Please invoke "
            "'tox' with no arguments for a basic test run.\n%s"
            % (self.COLOR_SEQ % self.RED, self.BOLD_SEQ, self.RESET_SEQ)
        )
        sys.exit(1)


cmdclass["test"] = UseTox


def status_msgs(*msgs):
    print("*" * 75)
    for msg in msgs:
        print(msg)
    print("*" * 75)


with open(
    os.path.join(os.path.dirname(__file__), "lib", "sqlalchemy", "__init__.py")
) as v_file:
    VERSION = (
        re.compile(r""".*__version__ = ["'](.*?)['"]""", re.S)
        .match(v_file.read())
        .group(1)
    )


def run_setup(with_cext):
    kwargs = {}
    if with_cext:
        kwargs["ext_modules"] = ext_modules
    else:
        if os.environ.get("REQUIRE_SQLALCHEMY_CEXT"):
            raise AssertionError(
                "Can't build on this platform with "
                "REQUIRE_SQLALCHEMY_CEXT set."
            )

        kwargs["ext_modules"] = []

    setup(version=VERSION, cmdclass=cmdclass, distclass=Distribution, **kwargs)


if not cpython:
    run_setup(False)
    status_msgs(
        "WARNING: C extensions are not supported on "
        + "this Python platform, speedups are not enabled.",
        "Plain-Python build succeeded.",
    )
elif os.environ.get("DISABLE_SQLALCHEMY_CEXT"):
    run_setup(False)
    status_msgs(
        "DISABLE_SQLALCHEMY_CEXT is set; "
        + "not attempting to build C extensions.",
        "Plain-Python build succeeded.",
    )

else:
    try:
        run_setup(True)
    except BuildFailed as exc:

        if os.environ.get("REQUIRE_SQLALCHEMY_CEXT"):
            status_msgs(
                "NOTE: C extension build is required because "
                "REQUIRE_SQLALCHEMY_CEXT is set, and the build has failed; "
                "will not degrade to non-C extensions"
            )
            raise

        status_msgs(
            exc.cause,
            "WARNING: The C extension could not be compiled, "
            + "speedups are not enabled.",
            "Failure information, if any, is above.",
            "Retrying the build without the C extension now.",
        )

        run_setup(False)

        status_msgs(
            "WARNING: The C extension could not be compiled, "
            + "speedups are not enabled.",
            "Plain-Python build succeeded.",
        )
