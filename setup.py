from __future__ import annotations

import os
import platform
from typing import cast
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


if HAS_CYTHON and IS_CPYTHON and not DISABLE_EXTENSION:
    assert _cy_Extension is not None
    assert _cy_build_ext is not None

    # when adding a cython module, also update the imports in _has_cy
    cython_files = [
        "collections.pyx",
        "immutabledict.pyx",
        "processors.pyx",
        "resultproxy.pyx",
        "util.pyx",
    ]
    cython_directives = {"language_level": "3"}

    module_prefix = "sqlalchemy.cyextension."
    source_prefix = "lib/sqlalchemy/cyextension/"

    ext_modules = cast(
        "list[Extension]",
        [
            _cy_Extension(
                f"{module_prefix}{os.path.splitext(file)[0]}",
                sources=[f"{source_prefix}{file}"],
                cython_directives=cython_directives,
                optional=not REQUIRE_EXTENSION,
            )
            for file in cython_files
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
