import testbase
import unittest

import orm.alltests as orm
import base.alltests as base
import sql.alltests as sql
import engine.alltests as engine
import ext.alltests as ext

def suite():
    alltests = unittest.TestSuite()
    for suite in (base, engine, sql, orm, ext):
        alltests.addTest(suite.suite())
    return alltests


if __name__ == '__main__':
    testbase.runTests(suite())
