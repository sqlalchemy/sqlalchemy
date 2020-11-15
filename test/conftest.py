#!/usr/bin/env python
"""
pytest plugin script.

This script is an extension to pytest which
installs SQLAlchemy's testing plugin into the local environment.

"""
import os
import sys

import pytest


os.environ["SQLALCHEMY_WARN_20"] = "true"

collect_ignore_glob = []

# minimum version for a py3k only test is at
# 3.6 because these are asyncio tests anyway
if sys.version_info[0:2] < (3, 6):
    collect_ignore_glob.append("*_py3k.py")

pytest.register_assert_rewrite("sqlalchemy.testing.assertions")


if not sys.flags.no_user_site:
    # this is needed so that test scenarios like "python setup.py test"
    # work correctly, as well as plain "pytest".  These commands assume
    # that the package in question is locally present, but since we have
    # ./lib/, we need to punch that in.
    # We check no_user_site to honor the use of this flag.
    sys.path.insert(
        0,
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"),
    )

# use bootstrapping so that test plugins are loaded
# without touching the main library before coverage starts
bootstrap_file = os.path.join(
    os.path.dirname(__file__),
    "..",
    "lib",
    "sqlalchemy",
    "testing",
    "plugin",
    "bootstrap.py",
)

with open(bootstrap_file) as f:
    code = compile(f.read(), "bootstrap.py", "exec")
    to_bootstrap = "pytest"
    exec(code, globals(), locals())
    from pytestplugin import *  # noqa
