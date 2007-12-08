import testbase
import sqlalchemy.topological as topological
from sqlalchemy import util
from testlib import *


class DependencySortTest(PersistTest):
    def assert_sort(self, tuples, node, collection=None):
        print str(node)
        def assert_tuple(tuple, node):
            if node[1]:
                cycles = node[1]
            else:
                cycles = []
            if tuple[0] is node[0] or tuple[0] in cycles:
                tuple.pop()
                if tuple[0] is node[0] or tuple[0] in cycles:
                    return
            elif len(tuple) > 1 and tuple[1] is node[0]:
                assert False, "Tuple not in dependency tree: " + str(tuple) + " " + str(node)
            for c in node[2]:
                assert_tuple(tuple, c)
        
        for tuple in tuples:
            assert_tuple(list(tuple), node)

        if collection is None:
            collection = []
        items = util.Set()
        def assert_unique(node):
            for item in [i for i in node[1] or [node[0]]]:
                assert item not in items
                items.add(item)
                if item in collection:
                    collection.remove(item)
            for c in node[2]:
                assert_unique(c)
        assert_unique(node)
        assert len(collection) == 0
        
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
        head = topological.sort_as_tree(tuples, [])
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
        head = topological.sort_as_tree(tuples, [node7])
        self.assert_sort(tuples, head, [node7])

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
        head1 = topological.sort_as_tree(tuples, [node1, node2, node3])
        head2 = topological.sort_as_tree(tuples, [node3, node1, node2])
        head3 = topological.sort_as_tree(tuples, [node3, node2, node1])
        
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
        head = topological.sort_as_tree(tuples, [])
        self.assert_sort(tuples, head)

    def testsort5(self):
        # this one, depenending on the weather, 
        node1 = 'node1' #'00B94190'
        node2 = 'node2' #'00B94990'
        node3 = 'node3' #'00B9A9B0'
        node4 = 'node4' #'00B4F210'
        tuples = [
            (node4, node1),
            (node1, node2),
            (node4, node3),
            (node2, node3),
            (node4, node2),
            (node3, node3)
        ]
        allitems = [
            node1,
            node2,
            node3,
            node4
        ]
        head = topological.sort_as_tree(tuples, allitems, with_cycles=True)
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
        head = topological.sort_as_tree(tuples, allitems, with_cycles=True)
        self.assert_sort(tuples, head)
        
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
        head = topological.sort_as_tree(tuples, [], with_cycles=True)
        self.assert_sort(tuples, head)
    
    def testcircular3(self):
        nodes = {}
        tuples = [('Question', 'Issue'), ('ProviderService', 'Issue'), ('Provider', 'Question'), ('Question', 'Provider'), ('ProviderService', 'Question'), ('Provider', 'ProviderService'), ('Question', 'Answer'), ('Issue', 'Question')]
        head = topological.sort_as_tree(tuples, [], with_cycles=True)
        self.assert_sort(tuples, head)
        
    def testbigsort(self):
        tuples = []
        for i in range(0,1500, 2):
            tuples.append((i, i+1))
        head = topological.sort_as_tree(tuples, [])
            
            
            
if __name__ == "__main__":
    testbase.main()
