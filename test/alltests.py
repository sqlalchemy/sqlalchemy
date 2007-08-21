import testbase
import unittest

import orm.alltests as orm
import base.alltests as base
import sql.alltests as sql
import engine.alltests as engine
import dialect.alltests as dialect
import ext.alltests as ext
import zblog.alltests as zblog
import profiling.alltests as profiling

def suite():
    alltests = unittest.TestSuite()
    for suite in (base, engine, sql, dialect, orm, ext, zblog, profiling):
        alltests.addTest(suite.suite())
    return alltests


if __name__ == '__main__':
    testbase.main(suite())
