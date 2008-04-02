import testenv; testenv.configure_for_tests()
import unittest

import orm.alltests as orm
import base.alltests as base
import sql.alltests as sql
import engine.alltests as engine
import dialect.alltests as dialect
import ext.alltests as ext
import zblog.alltests as zblog
import profiling.alltests as profiling

# The profiling tests are sensitive to foibles of CPython VM state, so
# run them first.  Ideally, each should be run in a fresh interpreter.

def suite():
    alltests = unittest.TestSuite()
    for suite in (profiling, base, engine, sql, dialect, orm, ext, zblog):
        alltests.addTest(suite.suite())
    return alltests


if __name__ == '__main__':
    testenv.main(suite())
