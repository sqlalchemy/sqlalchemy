"""setup.py

Please see README for basic installation instructions.

"""

import os
import re
import sys
from distutils.command.build_ext import build_ext
from distutils.errors import (CCompilerError, DistutilsExecError,
                              DistutilsPlatformError)

has_feature = False
try:
    from setuptools import setup, Extension
    try:
        # see
        # https://bitbucket.org/pypa/setuptools/issue/65/deprecate-and-remove-features,
        # where they may remove Feature.
        from setuptools import Feature
        has_feature = True
    except ImportError:
        pass
except ImportError:
    from distutils.core import setup, Extension

py3k = False

cmdclass = {}
extra = {}
if sys.version_info < (2, 6):
    raise Exception("SQLAlchemy requires Python 2.6 or higher.")
elif sys.version_info >= (3, 0):
    py3k = True

import platform
cpython = platform.python_implementation() == 'CPython'

ext_modules = [
    Extension('sqlalchemy.cprocessors',
              sources=['lib/sqlalchemy/cextension/processors.c']),
    Extension('sqlalchemy.cresultproxy',
              sources=['lib/sqlalchemy/cextension/resultproxy.c']),
    Extension('sqlalchemy.cutils',
              sources=['lib/sqlalchemy/cextension/utils.c'])
    ]

ext_errors = (CCompilerError, DistutilsExecError, DistutilsPlatformError)
if sys.platform == 'win32':
    # 2.6's distutils.msvc9compiler can raise an IOError when failing to
    # find the compiler
    ext_errors += (IOError,)


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

cmdclass['build_ext'] = ve_build_ext


def status_msgs(*msgs):
    print('*' * 75)
    for msg in msgs:
        print(msg)
    print('*' * 75)


def find_packages(location):
    packages = []
    for pkg in ['sqlalchemy']:
        for _dir, subdirectories, files in (
                os.walk(os.path.join(location, pkg))):
            if '__init__.py' in files:
                tokens = _dir.split(os.sep)[len(location.split(os.sep)):]
                packages.append(".".join(tokens))
    return packages

v_file = open(os.path.join(os.path.dirname(__file__),
                           'lib', 'sqlalchemy', '__init__.py'))
VERSION = re.compile(r".*__version__ = '(.*?)'",
                     re.S).match(v_file.read()).group(1)
v_file.close()

r_file = open(os.path.join(os.path.dirname(__file__), 'README.rst'))
readme = r_file.read()
r_file.close()


def run_setup(with_cext):
    kwargs = extra.copy()
    if with_cext:
        if has_feature:
            kwargs['features'] = {'cextensions': Feature(
                "optional C speed-enhancements",
                standard=True,
                ext_modules=ext_modules
                )}
        else:
            kwargs['ext_modules'] = ext_modules

    setup(name="SQLAlchemy",
          version=VERSION,
          description="Database Abstraction Library",
          author="Mike Bayer",
          author_email="mike_mp@zzzcomputing.com",
          url="http://www.sqlalchemy.org",
          packages=find_packages('lib'),
          package_dir={'': 'lib'},
          license="MIT License",
          cmdclass=cmdclass,
          tests_require=['pytest >= 2.5.2', 'mock', 'pytest-xdist'],
          test_suite="sqlalchemy.testing.distutils_run",
          long_description=readme,
          classifiers=[
              "Development Status :: 5 - Production/Stable",
              "Intended Audience :: Developers",
              "License :: OSI Approved :: MIT License",
              "Programming Language :: Python",
              "Programming Language :: Python :: 3",
              "Programming Language :: Python :: Implementation :: CPython",
              "Programming Language :: Python :: Implementation :: Jython",
              "Programming Language :: Python :: Implementation :: PyPy",
              "Topic :: Database :: Front-Ends",
              "Operating System :: OS Independent",
              ],
          **kwargs
          )

if not cpython:
    run_setup(False)
    status_msgs(
        "WARNING: C extensions are not supported on " +
        "this Python platform, speedups are not enabled.",
        "Plain-Python build succeeded."
    )
elif os.environ.get('DISABLE_SQLALCHEMY_CEXT'):
    run_setup(False)
    status_msgs(
        "DISABLE_SQLALCHEMY_CEXT is set; " +
        "not attempting to build C extensions.",
        "Plain-Python build succeeded."
    )

else:
    try:
        run_setup(True)
    except BuildFailed as exc:
        status_msgs(
            exc.cause,
            "WARNING: The C extension could not be compiled, " +
            "speedups are not enabled.",
            "Failure information, if any, is above.",
            "Retrying the build without the C extension now."
        )

        run_setup(False)

        status_msgs(
            "WARNING: The C extension could not be compiled, " +
            "speedups are not enabled.",
            "Plain-Python build succeeded."
        )
