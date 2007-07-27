import testbase
import unittest

import inheritance.alltests as inheritance
import sharding.alltests as sharding

def suite():
    modules_to_test = (
    'orm.attributes',
        'orm.query',
        'orm.lazy_relations',
        'orm.eager_relations',
        'orm.mapper',
        'orm.selectable',
        'orm.collection',
        'orm.generative',
        'orm.lazytest1',
        'orm.assorted_eager',
        
        'orm.sessioncontext', 
        'orm.unitofwork',
        'orm.session',
        'orm.cascade',
        'orm.relationships',
        'orm.association',
        'orm.merge',
        'orm.memusage',
        
        'orm.cycles',

        'orm.entity',
        'orm.compile',
        'orm.manytomany',
        'orm.onetoone',
        'orm.dynamic',
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
    testbase.main(suite())
