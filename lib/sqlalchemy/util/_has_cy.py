import typing

if not typing.TYPE_CHECKING:
    try:
        from ..cyextension import util  # noqa
    except ImportError:
        HAS_CYEXTENSION = False
    else:
        HAS_CYEXTENSION = True
else:
    HAS_CYEXTENSION = False
