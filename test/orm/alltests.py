import testbase
import unittest

import inheritance.alltests as inheritance

def suite():
    modules_to_test = (
	'orm.attributes',
	    'orm.query',
	    'orm.lazy_relations',
        'orm.mapper',
        'orm.generative',
        'orm.lazytest1',
        'orm.eagertest1',
        'orm.eagertest2',
        'orm.eagertest3',
        
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
        )
    alltests = unittest.TestSuite()
    for name in modules_to_test:
        mod = __import__(name)
        for token in name.split('.')[1:]:
            mod = getattr(mod, token)
        alltests.addTest(unittest.findTestCases(mod, suiteClass=None))
    alltests.addTest(inheritance.suite())
    return alltests


if __name__ == '__main__':
    testbase.main(suite())
