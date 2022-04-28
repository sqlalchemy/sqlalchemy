# mypy: allow-untyped-defs, allow-untyped-calls

import os
import typing

if not typing.TYPE_CHECKING:
    if os.environ.get("DISABLE_SQLALCHEMY_CEXT_RUNTIME"):
        HAS_CYEXTENSION = False
    else:
        try:
            from ..cyextension import util  # noqa
        except ImportError:
            HAS_CYEXTENSION = False
        else:
            HAS_CYEXTENSION = True
else:
    HAS_CYEXTENSION = False
