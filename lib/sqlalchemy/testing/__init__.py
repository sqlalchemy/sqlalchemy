# testing/__init__.py
# Copyright (C) 2005-2016 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php


from .warnings import assert_warnings

from . import config

from .exclusions import db_spec, _is_excluded, fails_if, skip_if, future,\
    fails_on, fails_on_everything_except, skip, only_on, exclude, \
    against as _against, _server_version, only_if, fails


def against(*queries):
    return _against(config._current, *queries)

from .assertions import emits_warning, emits_warning_on, uses_deprecated, \
    eq_, ne_, le_, is_, is_not_, startswith_, assert_raises, \
    assert_raises_message, AssertsCompiledSQL, ComparesTables, \
    AssertsExecutionResults, expect_deprecated, expect_warnings, \
    in_, not_in_, eq_ignore_whitespace, eq_regex, is_true, is_false

from .util import run_as_contextmanager, rowset, fail, \
    provide_metadata, adict, force_drop_names, \
    teardown_events

crashes = skip

from .config import db
from .config import requirements as requires

from . import mock
