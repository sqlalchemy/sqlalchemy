# testing/__init__.py
# Copyright (C) 2005-2019 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php


from . import config  # noqa
from . import mock  # noqa
from .assertions import assert_raises  # noqa
from .assertions import assert_raises_message  # noqa
from .assertions import assert_raises_return  # noqa
from .assertions import AssertsCompiledSQL  # noqa
from .assertions import AssertsExecutionResults  # noqa
from .assertions import ComparesTables  # noqa
from .assertions import emits_warning  # noqa
from .assertions import emits_warning_on  # noqa
from .assertions import eq_  # noqa
from .assertions import eq_ignore_whitespace  # noqa
from .assertions import eq_regex  # noqa
from .assertions import expect_deprecated  # noqa
from .assertions import expect_warnings  # noqa
from .assertions import in_  # noqa
from .assertions import is_  # noqa
from .assertions import is_false  # noqa
from .assertions import is_not_  # noqa
from .assertions import is_true  # noqa
from .assertions import le_  # noqa
from .assertions import ne_  # noqa
from .assertions import not_in_  # noqa
from .assertions import startswith_  # noqa
from .assertions import uses_deprecated  # noqa
from .config import combinations  # noqa
from .config import db  # noqa
from .config import fixture  # noqa
from .config import requirements as requires  # noqa
from .exclusions import _is_excluded  # noqa
from .exclusions import _server_version  # noqa
from .exclusions import against as _against  # noqa
from .exclusions import db_spec  # noqa
from .exclusions import exclude  # noqa
from .exclusions import fails  # noqa
from .exclusions import fails_if  # noqa
from .exclusions import fails_on  # noqa
from .exclusions import fails_on_everything_except  # noqa
from .exclusions import future  # noqa
from .exclusions import only_if  # noqa
from .exclusions import only_on  # noqa
from .exclusions import skip  # noqa
from .exclusions import skip_if  # noqa
from .util import adict  # noqa
from .util import fail  # noqa
from .util import flag_combinations  # noqa
from .util import force_drop_names  # noqa
from .util import metadata_fixture  # noqa
from .util import provide_metadata  # noqa
from .util import resolve_lambda  # noqa
from .util import rowset  # noqa
from .util import run_as_contextmanager  # noqa
from .util import teardown_events  # noqa
from .warnings import assert_warnings  # noqa


def against(*queries):
    return _against(config._current, *queries)


crashes = skip
