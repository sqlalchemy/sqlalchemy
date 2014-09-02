# testing/warnings.py
# Copyright (C) 2005-2014 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from __future__ import absolute_import

import warnings
from .. import exc as sa_exc
import re


def setup_filters():
    """Set global warning behavior for the test suite."""

    warnings.filterwarnings('ignore',
                            category=sa_exc.SAPendingDeprecationWarning)
    warnings.filterwarnings('error', category=sa_exc.SADeprecationWarning)
    warnings.filterwarnings('error', category=sa_exc.SAWarning)


def assert_warnings(fn, warning_msgs, regex=False):
    """Assert that each of the given warnings are emitted by fn."""

    from .assertions import eq_

    with warnings.catch_warnings(record=True) as log:
        # ensure that nothing is going into __warningregistry__
        warnings.filterwarnings("always")

        result = fn()
    for warning in log:
        popwarn = warning_msgs.pop(0)
        if regex:
            assert re.match(popwarn, str(warning.message))
        else:
            eq_(popwarn, str(warning.message))
    return result
