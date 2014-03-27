# plugin/noseplugin.py
# Copyright (C) 2005-2014 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Enhance nose with extra options and behaviors for running SQLAlchemy tests.

Must be run via ./sqla_nose.py so that it is imported in the expected
way (e.g. as a package-less import).

"""

import os

from nose.plugins import Plugin
fixtures = None

# no package imports yet!  this prevents us from tripping coverage
# too soon.
import imp
path = os.path.join(os.path.dirname(__file__), "plugin_base.py")
plugin_base = imp.load_source("plugin_base", path)


class NoseSQLAlchemy(Plugin):
    enabled = True

    name = 'sqla_testing'
    score = 100

    def options(self, parser, env=os.environ):
        Plugin.options(self, parser, env)
        opt = parser.add_option

        def make_option(name, **kw):
            callback_ = kw.pop("callback", None)
            if callback_:
                def wrap_(option, opt_str, value, parser):
                    callback_(opt_str, value, parser)
                kw["callback"] = wrap_
            opt(name, **kw)

        plugin_base.setup_options(make_option)
        plugin_base.read_config()

    def configure(self, options, conf):
        super(NoseSQLAlchemy, self).configure(options, conf)
        plugin_base.pre_begin(options)

        plugin_base.set_coverage_flag(options.enable_plugin_coverage)

        global fixtures
        from sqlalchemy.testing import fixtures

    def begin(self):
        plugin_base.post_begin()

    def describeTest(self, test):
        return ""

    def wantFunction(self, fn):
        if fn.__module__ is None:
            return False
        if fn.__module__.startswith('sqlalchemy.testing'):
            return False

    def wantClass(self, cls):
        return plugin_base.want_class(cls)

    def beforeTest(self, test):
        plugin_base.before_test(test,
                        test.test.cls.__module__,
                        test.test.cls, test.test.method.__name__)

    def afterTest(self, test):
        plugin_base.after_test(test)

    def startContext(self, ctx):
        if not isinstance(ctx, type) \
            or not issubclass(ctx, fixtures.TestBase):
            return
        plugin_base.start_test_class(ctx)

    def stopContext(self, ctx):
        if not isinstance(ctx, type) \
            or not issubclass(ctx, fixtures.TestBase):
            return
        plugin_base.stop_test_class(ctx)
