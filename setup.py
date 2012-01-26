"""setup.py

Please see README for basic installation instructions.

"""

import os
import re
import sys
from distutils.command.build_ext import build_ext
from distutils.errors import (CCompilerError, DistutilsExecError,
                              DistutilsPlatformError)
try:
    from setuptools import setup, Extension, Feature
    has_setuptools = True
except ImportError:
    has_setuptools = False
    from distutils.core import setup, Extension
    Feature = None
    try:  # Python 3
        from distutils.command.build_py import build_py_2to3 as build_py
    except ImportError:  # Python 2
        from distutils.command.build_py import build_py

cmdclass = {}
pypy = hasattr(sys, 'pypy_version_info')
jython = sys.platform.startswith('java')
py3k = False
extra = {}
if sys.version_info < (2, 4):
    raise Exception("SQLAlchemy requires Python 2.4 or higher.")
elif sys.version_info >= (3, 0):
    py3k = True
    # monkeypatch our preprocessor
    # onto the 2to3 tool.
    from sa2to3 import refactor_string
    from lib2to3.refactor import RefactoringTool
    RefactoringTool.refactor_string = refactor_string

    if has_setuptools:
        extra.update(
            use_2to3=True,
        )
    else:
        cmdclass['build_py'] = build_py

ext_modules = [
    Extension('sqlalchemy.cprocessors',
           sources=['lib/sqlalchemy/cextension/processors.c']),
    Extension('sqlalchemy.cresultproxy',
           sources=['lib/sqlalchemy/cextension/resultproxy.c'])
    ]

ext_errors = (CCompilerError, DistutilsExecError, DistutilsPlatformError) 
if sys.platform == 'win32' and sys.version_info > (2, 6):
   # 2.6's distutils.msvc9compiler can raise an IOError when failing to
   # find the compiler
   ext_errors += (IOError,) 

class BuildFailed(Exception):

    def __init__(self):
        self.cause = sys.exc_info()[1] # work around py 2/3 different syntax

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
            if "'path'" in str(sys.exc_info()[1]): # works with both py 2/3
                raise BuildFailed()
            raise

cmdclass['build_ext'] = ve_build_ext

def status_msgs(*msgs):
    print('*' * 75)
    for msg in msgs:
        print(msg)
    print('*' * 75)

def find_packages(dir_):
    packages = []
    for pkg in ['sqlalchemy']:
        for _dir, subdirectories, files in (
                os.walk(os.path.join(dir_, pkg))
            ):
            if '__init__.py' in files:
                lib, fragment = _dir.split(os.sep, 1)
                packages.append(fragment.replace(os.sep, '.'))
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
        if Feature:
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

          tests_require=['nose >= 0.11'],
          test_suite="sqla_nose",
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

if pypy or jython or py3k:
    run_setup(False)
    status_msgs(
        "WARNING: C extensions are not supported on " +
            "this Python platform, speedups are not enabled.",
        "Plain-Python build succeeded."
    )
else:
    try:
        run_setup(True)
    except BuildFailed:
        exc = sys.exc_info()[1] # work around py 2/3 different syntax
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
