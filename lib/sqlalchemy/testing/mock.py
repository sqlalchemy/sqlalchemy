# testing/mock.py
# Copyright (C) 2005-2014 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Import stub for mock library.
"""
from __future__ import absolute_import
from ..util import py33

if py33:
    from unittest.mock import MagicMock, Mock, call, patch
else:
    try:
        from mock import MagicMock, Mock, call, patch
    except ImportError:
        raise ImportError(
                "SQLAlchemy's test suite requires the "
                "'mock' library as of 0.8.2.")

