# testing/warnings.py
# Copyright (C) 2005-2021 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php

from __future__ import absolute_import

import warnings

from . import assertions
from .. import exc as sa_exc
from ..util.langhelpers import _warnings_warn


class SATestSuiteWarning(sa_exc.SAWarning):
    """warning for a condition detected during tests that is non-fatal"""


def warn_test_suite(message):
    _warnings_warn(message, category=SATestSuiteWarning)


def setup_filters():
    """Set global warning behavior for the test suite."""

    warnings.filterwarnings(
        "ignore", category=sa_exc.SAPendingDeprecationWarning
    )
    warnings.filterwarnings("error", category=sa_exc.SADeprecationWarning)
    warnings.filterwarnings("error", category=sa_exc.SAWarning)
    warnings.filterwarnings("always", category=SATestSuiteWarning)

    # some selected deprecations...
    warnings.filterwarnings("error", category=DeprecationWarning)
    warnings.filterwarnings(
        "ignore", category=DeprecationWarning, message=r".*StopIteration"
    )
    warnings.filterwarnings(
        "ignore",
        category=DeprecationWarning,
        message=r".*inspect.get.*argspec",
    )

    warnings.filterwarnings(
        "ignore",
        category=DeprecationWarning,
        message="The loop argument is deprecated",
    )

    # ignore things that are deprecated *as of* 2.0 :)
    warnings.filterwarnings(
        "ignore",
        category=sa_exc.SADeprecationWarning,
        message=r".*\(deprecated since: 2.0\)$",
    )
    warnings.filterwarnings(
        "ignore",
        category=sa_exc.SADeprecationWarning,
        message=r"^The (Sybase|firebird) dialect is deprecated and will be",
    )

    try:
        import pytest
    except ImportError:
        pass
    else:
        warnings.filterwarnings(
            "once", category=pytest.PytestDeprecationWarning
        )


def assert_warnings(fn, warning_msgs, regex=False):
    """Assert that each of the given warnings are emitted by fn.

    Deprecated.  Please use assertions.expect_warnings().

    """

    with assertions._expect_warnings(
        sa_exc.SAWarning, warning_msgs, regex=regex
    ):
        return fn()
