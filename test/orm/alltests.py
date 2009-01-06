import testenv; testenv.configure_for_tests()
from testlib import sa_unittest as unittest

import inheritance.alltests as inheritance
import sharding.alltests as sharding

def suite():
    modules_to_test = (
        'orm.attributes',
        'orm.bind',
        'orm.extendedattr',
        'orm.instrumentation',
        'orm.query',
        'orm.lazy_relations',
        'orm.eager_relations',
        'orm.mapper',
        'orm.expire',
        'orm.selectable',
        'orm.collection',
        'orm.generative',
        'orm.lazytest1',
        'orm.assorted_eager',

        'orm.naturalpks',
        'orm.unitofwork',
        'orm.session',
        'orm.transaction',
        'orm.scoping',
        'orm.cascade',
        'orm.relationships',
        'orm.association',
        'orm.merge',
        'orm.pickled',
        'orm.utils',

        'orm.cycles',

        'orm.compile',
        'orm.manytomany',
        'orm.onetoone',
        'orm.dynamic',

        'orm.deprecations',
        )
    alltests = unittest.TestSuite()
    for name in modules_to_test:
        mod = __import__(name)
        for token in name.split('.')[1:]:
            mod = getattr(mod, token)
        alltests.addTest(unittest.findTestCases(mod, suiteClass=None))
    alltests.addTest(inheritance.suite())
    alltests.addTest(sharding.suite())
    return alltests


if __name__ == '__main__':
    testenv.main(suite())
