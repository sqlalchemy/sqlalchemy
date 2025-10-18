from __future__ import annotations

import os
import platform
import sys
from typing import Any
from typing import cast
from typing import Dict
from typing import TYPE_CHECKING

from setuptools import setup

if TYPE_CHECKING:
    from setuptools import Extension

try:
    from Cython.Build import build_ext as _cy_build_ext
    from Cython.Distutils.extension import Extension as _cy_Extension

    HAS_CYTHON = True
except ImportError:
    _cy_build_ext = _cy_Extension = None
    HAS_CYTHON = False

IS_CPYTHON = platform.python_implementation() == "CPython"
DISABLE_EXTENSION = bool(os.environ.get("DISABLE_SQLALCHEMY_CEXT"))
REQUIRE_EXTENSION = bool(os.environ.get("REQUIRE_SQLALCHEMY_CEXT"))

if DISABLE_EXTENSION and REQUIRE_EXTENSION:
    raise RuntimeError(
        "Cannot set both 'DISABLE_SQLALCHEMY_CEXT' and "
        "'REQUIRE_SQLALCHEMY_CEXT' environment variables"
    )

# when adding a cython module, also update the imports in _has_cython
# it is tested in test_setup_defines_all_files
CYTHON_MODULES = (
    "engine._processors_cy",
    "engine._row_cy",
    "engine._util_cy",
    "sql._util_cy",
    "util._collections_cy",
    "util._immutabledict_cy",
)

if HAS_CYTHON and IS_CPYTHON and not DISABLE_EXTENSION:
    assert _cy_Extension is not None
    assert _cy_build_ext is not None
    from Cython.Compiler import Options

    Options.docstrings = False
    Options.lookup_module_cpdef = True
    Options.clear_to_none = False

    cython_directives: Dict[str, Any] = {
        "language_level": "3",
        "initializedcheck": False,
    }

    if sys.version_info >= (3, 13):
        cython_directives["freethreading_compatible"] = True

    module_prefix = "sqlalchemy."
    source_prefix = "lib/sqlalchemy/"

    ext_modules = cast(
        "list[Extension]",
        [
            _cy_Extension(
                f"{module_prefix}{module}",
                sources=[f"{source_prefix}{module.replace('.', '/')}.py"],
                cython_directives=cython_directives,
                optional=not REQUIRE_EXTENSION,
            )
            for module in CYTHON_MODULES
        ],
    )

    cmdclass = {"build_ext": _cy_build_ext}

elif REQUIRE_EXTENSION:
    reasons = []
    if not HAS_CYTHON:
        reasons.append("Cython is missing")
    if not IS_CPYTHON:
        reasons.append("Not CPython, build is supported only on it")
    raise RuntimeError(
        "Cython extension build is required because REQUIRE_SQLALCHEMY_CEXT "
        f"is set but it was deselected because: {'; '.join(reasons)}; "
        "will not degrade to pure python install"
    )

else:
    ext_modules = []
    cmdclass = {}

setup(cmdclass=cmdclass, ext_modules=ext_modules)
