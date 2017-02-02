#!/usr/bin/env python
"""
pytest plugin script.

This script is an extension to py.test which
installs SQLAlchemy's testing plugin into the local environment.

"""
import sys
import os

if not sys.flags.no_user_site:
    # this is needed so that test scenarios like "python setup.py test"
    # work correctly, as well as plain "py.test".  These commands assume
    # that the package in question is locally present, but since we have
    # ./lib/, we need to punch that in.
    # We check no_user_site to honor the use of this flag.
    sys.path.insert(
        0,
        os.path.join(
            os.path.dirname(os.path.abspath(__file__)), '..', 'lib')
    )

# use bootstrapping so that test plugins are loaded
# without touching the main library before coverage starts
bootstrap_file = os.path.join(
    os.path.dirname(__file__), "..", "lib", "sqlalchemy",
    "testing", "plugin", "bootstrap.py"
)

with open(bootstrap_file) as f:
    code = compile(f.read(), "bootstrap.py", 'exec')
    to_bootstrap = "pytest"
    exec(code, globals(), locals())
    from pytestplugin import *  # noqa
