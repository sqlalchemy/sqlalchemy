import os
import platform
import sys

from setuptools import __version__
from setuptools import Distribution as _Distribution
from setuptools import setup

if not int(__version__.partition(".")[0]) >= 47:
    raise RuntimeError(f"Setuptools >= 47 required. Found {__version__}")

# attempt to use pep-632 imports for setuptools symbols; however,
# since these symbols were only added to setuptools as of 59.0.1,
# fall back to the distutils symbols otherwise
try:
    from setuptools.errors import CCompilerError
    from setuptools.errors import DistutilsExecError
    from setuptools.errors import DistutilsPlatformError
except ImportError:
    from distutils.errors import CCompilerError
    from distutils.errors import DistutilsExecError
    from distutils.errors import DistutilsPlatformError

try:
    from Cython.Distutils.old_build_ext import old_build_ext
    from Cython.Distutils.extension import Extension

    CYTHON = True
except ImportError:
    CYTHON = False

cmdclass = {}

cpython = platform.python_implementation() == "CPython"

ext_errors = (CCompilerError, DistutilsExecError, DistutilsPlatformError)
extra_compile_args = []
if sys.platform == "win32":
    # Work around issue https://github.com/pypa/setuptools/issues/1902
    ext_errors += (IOError, TypeError)

cython_files = [
    "collections.pyx",
    "immutabledict.pyx",
    "processors.pyx",
    "resultproxy.pyx",
    "util.pyx",
]
cython_directives = {"language_level": "3"}

if CYTHON:

    def get_ext_modules():
        module_prefix = "sqlalchemy.cyextension."
        source_prefix = "lib/sqlalchemy/cyextension/"

        ext_modules = []
        for file in cython_files:
            name, _ = os.path.splitext(file)
            ext_modules.append(
                Extension(
                    module_prefix + name,
                    sources=[source_prefix + file],
                    extra_compile_args=extra_compile_args,
                    cython_directives=cython_directives,
                )
            )
        return ext_modules

    class BuildFailed(Exception):
        pass

    class ve_build_ext(old_build_ext):
        # This class allows Cython building to fail.

        def run(self):
            try:
                super().run()
            except DistutilsPlatformError:
                raise BuildFailed()

        def build_extension(self, ext):
            try:
                super().build_extension(ext)
            except ext_errors as e:
                raise BuildFailed() from e
            except ValueError as e:
                # this can happen on Windows 64 bit, see Python issue 7511
                if "'path'" in str(e):
                    raise BuildFailed() from e
                raise

    cmdclass["build_ext"] = ve_build_ext
    ext_modules = get_ext_modules()
else:
    ext_modules = []


class Distribution(_Distribution):
    def has_ext_modules(self):
        # We want to always claim that we have ext_modules. This will be fine
        # if we don't actually have them (such as on PyPy) because nothing
        # will get built, however we don't want to provide an overally broad
        # Wheel package when building a wheel without C support. This will
        # ensure that Wheel knows to treat us as if the build output is
        # platform specific.
        return True


def status_msgs(*msgs):
    print("*" * 75)
    for msg in msgs:
        print(msg)
    print("*" * 75)


def run_setup(with_cext):
    kwargs = {}
    if with_cext:
        kwargs["ext_modules"] = ext_modules
    else:
        if os.environ.get("REQUIRE_SQLALCHEMY_CEXT"):
            raise AssertionError(
                "Can't build on this platform with REQUIRE_SQLALCHEMY_CEXT"
                " set. Cython is required to build compiled extensions"
            )

        kwargs["ext_modules"] = []

    setup(cmdclass=cmdclass, distclass=Distribution, **kwargs)


if not cpython:
    run_setup(False)
    status_msgs(
        "WARNING: Cython extensions are not supported on "
        "this Python platform, speedups are not enabled.",
        "Plain-Python build succeeded.",
    )
elif not CYTHON:
    run_setup(False)
    status_msgs(
        "WARNING: Cython is required to build the compiled "
        "extensions, speedups are not enabled.",
        "Plain-Python build succeeded.",
    )
elif os.environ.get("DISABLE_SQLALCHEMY_CEXT"):
    run_setup(False)
    status_msgs(
        "DISABLE_SQLALCHEMY_CEXT is set; "
        "not attempting to build Cython extensions.",
        "Plain-Python build succeeded.",
    )
else:
    try:
        run_setup(True)
    except BuildFailed as exc:

        if os.environ.get("REQUIRE_SQLALCHEMY_CEXT"):
            status_msgs(
                "NOTE: Cython extension build is required because "
                "REQUIRE_SQLALCHEMY_CEXT is set, and the build has failed; "
                "will not degrade to non-C extensions"
            )
            raise

        status_msgs(
            exc.__cause__,
            "WARNING: The Cython extension could not be compiled, "
            "speedups are not enabled.",
            "Failure information, if any, is above.",
            "Retrying the build without the C extension now.",
        )

        run_setup(False)

        status_msgs(
            "WARNING: The Cython extension could not be compiled, "
            "speedups are not enabled.",
            "Plain-Python build succeeded.",
        )
