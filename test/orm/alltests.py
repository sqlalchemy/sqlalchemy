import testbase
import unittest

def suite():
    modules_to_test = (
        'orm.mapper',
        'orm.selectresults',
        'orm.lazytest1',
        'orm.eagertest1',
        'orm.eagertest2',
        
        'orm.sessioncontext', 
        'orm.objectstore',
	'orm.session',
        'orm.cascade',
        'orm.relationships',
        'orm.association',
        
        'orm.cycles',
        'orm.poly_linked_list',

        'orm.entity',
        'orm.compile',
        'orm.manytomany',
        'orm.onetoone',
        'orm.inheritance',
        'orm.inheritance2',
        'orm.inheritance3',
        'orm.polymorph'        
        )
    alltests = unittest.TestSuite()
    for name in modules_to_test:
        mod = __import__(name)
        for token in name.split('.')[1:]:
            mod = getattr(mod, token)
        alltests.addTest(unittest.findTestCases(mod, suiteClass=None))
    return alltests


if __name__ == '__main__':
    testbase.runTests(suite())
