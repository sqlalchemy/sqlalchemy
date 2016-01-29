# testing/warnings.py
# Copyright (C) 2005-2016 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from __future__ import absolute_import

import warnings
from .. import exc as sa_exc
from . import assertions


def setup_filters():
    """Set global warning behavior for the test suite."""

    warnings.filterwarnings('ignore',
                            category=sa_exc.SAPendingDeprecationWarning)
    warnings.filterwarnings('error', category=sa_exc.SADeprecationWarning)
    warnings.filterwarnings('error', category=sa_exc.SAWarning)


def assert_warnings(fn, warning_msgs, regex=False):
    """Assert that each of the given warnings are emitted by fn.

    Deprecated.  Please use assertions.expect_warnings().

    """

    with assertions._expect_warnings(
            sa_exc.SAWarning, warning_msgs, regex=regex):
        return fn()

