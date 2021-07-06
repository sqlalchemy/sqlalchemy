# testing/mock.py
# Copyright (C) 2005-2021 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php

"""Import stub for mock library.
"""
from __future__ import absolute_import

from ..util import py3k


if py3k:
    from unittest.mock import MagicMock
    from unittest.mock import Mock
    from unittest.mock import call
    from unittest.mock import patch
    from unittest.mock import ANY
else:
    try:
        from mock import MagicMock  # noqa
        from mock import Mock  # noqa
        from mock import call  # noqa
        from mock import patch  # noqa
        from mock import ANY  # noqa
    except ImportError:
        raise ImportError(
            "SQLAlchemy's test suite requires the "
            "'mock' library as of 0.8.2."
        )
