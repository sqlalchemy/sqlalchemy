# testing/warnings.py
# Copyright (C) 2005-2022 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php
from . import assertions
from .. import exc as sa_exc
from ..exc import SATestSuiteWarning
from ..util.langhelpers import _warnings_warn


def warn_test_suite(message):
    _warnings_warn(message, category=SATestSuiteWarning)


def setup_filters():
    """hook for setting up warnings filters.

    Note that when the pytest warnings plugin is in place, that plugin
    overwrites whatever happens here.

    Current SQLAlchemy 2.0 default is to use pytest warnings plugin
    which is configured in pyproject.toml.

    """


def assert_warnings(fn, warning_msgs, regex=False):
    """Assert that each of the given warnings are emitted by fn.

    Deprecated.  Please use assertions.expect_warnings().

    """

    with assertions._expect_warnings(
        sa_exc.SAWarning, warning_msgs, regex=regex
    ):
        return fn()
