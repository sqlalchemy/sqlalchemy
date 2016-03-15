#!/usr/bin/env python
"""
nose runner script.

This script is a front-end to "nosetests" which
installs SQLAlchemy's testing plugin into the local environment.

"""
import sys
import nose
import os

if not sys.flags.no_user_site:
    sys.path.insert(
        0,
        os.path.join(os.path.dirname(os.path.abspath(__file__)), 'lib')
    )

# use bootstrapping so that test plugins are loaded
# without touching the main library before coverage starts
bootstrap_file = os.path.join(
    os.path.dirname(__file__), "lib", "sqlalchemy",
    "testing", "plugin", "bootstrap.py"
)

with open(bootstrap_file) as f:
    code = compile(f.read(), "bootstrap.py", 'exec')
    to_bootstrap = "nose"
    exec(code, globals(), locals())


from noseplugin import NoseSQLAlchemy
nose.main(addplugins=[NoseSQLAlchemy()])
