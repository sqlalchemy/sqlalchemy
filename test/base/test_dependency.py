import sqlalchemy.topological as topological
from sqlalchemy.test import TestBase
from sqlalchemy.test.testing import assert_raises
from sqlalchemy import exc
import collections

class DependencySortTest(TestBase):
    def assert_sort(self, tuples, result):
        
        deps = collections.defaultdict(set)
        for parent, child in tuples:
            deps[parent].add(child)
        
        for i, node in enumerate(result):
            for n in result[i:]:
                assert node not in deps[n]

    def testsort(self):
        rootnode = 'root'
        node2 = 'node2'
        node3 = 'node3'
        node4 = 'node4'
        subnode1 = 'subnode1'
        subnode2 = 'subnode2'
        subnode3 = 'subnode3'
        subnode4 = 'subnode4'
        subsubnode1 = 'subsubnode1'
        tuples = [
            (subnode3, subsubnode1),
            (node2, subnode1),
            (node2, subnode2),
            (rootnode, node2),
            (rootnode, node3),
            (rootnode, node4),
            (node4, subnode3),
            (node4, subnode4)
        ]
        head = topological.sort(tuples, [])
        self.assert_sort(tuples, head)

    def testsort2(self):
        node1 = 'node1'
        node2 = 'node2'
        node3 = 'node3'
        node4 = 'node4'
        node5 = 'node5'
        node6 = 'node6'
        node7 = 'node7'
        tuples = [
            (node1, node2),
            (node3, node4),
            (node4, node5),
            (node5, node6),
            (node6, node2)
        ]
        head = topological.sort(tuples, [node7])
        self.assert_sort(tuples, head)

    def testsort3(self):
        ['Mapper|Keyword|keywords,Mapper|IKAssociation|itemkeywords', 'Mapper|Item|items,Mapper|IKAssociation|itemkeywords']
        node1 = 'keywords'
        node2 = 'itemkeyowrds'
        node3 = 'items'
        tuples = [
            (node1, node2),
            (node3, node2),
            (node1,node3)
        ]
        head1 = topological.sort(tuples, [node1, node2, node3])
        head2 = topological.sort(tuples, [node3, node1, node2])
        head3 = topological.sort(tuples, [node3, node2, node1])

        # TODO: figure out a "node == node2" function
        #self.assert_(str(head1) == str(head2) == str(head3))
        print "\n" + str(head1)
        print "\n" + str(head2)
        print "\n" + str(head3)

    def testsort4(self):
        node1 = 'keywords'
        node2 = 'itemkeyowrds'
        node3 = 'items'
        node4 = 'hoho'
        tuples = [
            (node1, node2),
            (node4, node1),
            (node1, node3),
            (node3, node2)
        ]
        head = topological.sort(tuples, [])
        self.assert_sort(tuples, head)

    def testcircular(self):
        node1 = 'node1'
        node2 = 'node2'
        node3 = 'node3'
        node4 = 'node4'
        node5 = 'node5'
        tuples = [
            (node4, node5),
            (node5, node4),
            (node1, node2),
            (node2, node3),
            (node3, node1),
            (node4, node1)
        ]
        allitems = [node1, node2, node3, node4]
        assert_raises(exc.CircularDependencyError, topological.sort, tuples, allitems)

    def testcircular2(self):
        # this condition was arising from ticket:362
        # and was not treated properly by topological sort
        node1 = 'node1'
        node2 = 'node2'
        node3 = 'node3'
        node4 = 'node4'
        tuples = [
            (node1, node2),
            (node3, node1),
            (node2, node4),
            (node3, node2),
            (node2, node3)
        ]
        assert_raises(exc.CircularDependencyError, topological.sort, tuples, [])

    def testcircular3(self):
        question, issue, providerservice, answer, provider = "Question", "Issue", "ProviderService", "Answer", "Provider"

        tuples = [(question, issue), (providerservice, issue), (provider, question), (question, provider), (providerservice, question), (provider, providerservice), (question, answer), (issue, question)]

        assert_raises(exc.CircularDependencyError, topological.sort, tuples, [])

    def testbigsort(self):
        tuples = [(i, i + 1) for i in range(0, 1500, 2)]
        head = topological.sort(tuples, [])


    def testids(self):
        # ticket:1380 regression: would raise a KeyError
        topological.sort([(id(i), i) for i in range(3)], [])


