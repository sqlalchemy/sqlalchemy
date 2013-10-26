# testing/__init__.py
# Copyright (C) 2005-2013 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php


from .warnings import testing_warn, assert_warnings, resetwarnings

from . import config

from .exclusions import db_spec, _is_excluded, fails_if, skip_if, future,\
    fails_on, fails_on_everything_except, skip, only_on, exclude, against,\
    _server_version, only_if

from .assertions import emits_warning, emits_warning_on, uses_deprecated, \
        eq_, ne_, is_, is_not_, startswith_, assert_raises, \
        assert_raises_message, AssertsCompiledSQL, ComparesTables, \
        AssertsExecutionResults, expect_deprecated

from .util import run_as_contextmanager, rowset, fail, provide_metadata, adict

crashes = skip

from .config import db, requirements as requires

from . import mock