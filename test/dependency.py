from testbase import PersistTest
import sqlalchemy.mapping.topological as topological
import unittest, sys, os


# TODO:  need assertion conditions in this suite


class DependencySorter(topological.QueueDependencySorter):pass
    
class thingy(object):
    def __init__(self, name):
        self.name = name
    def __repr__(self):
        return "thingy(%d, %s)" % (id(self), self.name)
    def __str__(self):
        return repr(self)
        
class DependencySortTest(PersistTest):
    
    def _assert_sort(self, tuples, allnodes, **kwargs):

        head = DependencySorter(tuples, allnodes).sort(**kwargs)

        print "\n" + str(head)
        def findnode(t, n, parent=False):
            if n.item is t[0]:
                parent=True
            elif n.item is t[1]:
                if not parent and t[0] not in [c.item for c in n.cycles]:
                    self.assert_(False, "Node " + str(t[1]) + " not a child of " +str(t[0]))
                else:
                    return
            for c in n.children:
                findnode(t, c, parent)
            
        for t in tuples:
            findnode(t, head)
            
    def testsort(self):
        rootnode = thingy('root')
        node2 = thingy('node2')
        node3 = thingy('node3')
        node4 = thingy('node4')
        subnode1 = thingy('subnode1')
        subnode2 = thingy('subnode2')
        subnode3 = thingy('subnode3')
        subnode4 = thingy('subnode4')
        subsubnode1 = thingy('subsubnode1')
        allnodes = [rootnode, node2,node3,node4,subnode1,subnode2,subnode3,subnode4,subsubnode1]
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

        self._assert_sort(tuples, allnodes)

    def testsort2(self):
        node1 = thingy('node1')
        node2 = thingy('node2')
        node3 = thingy('node3')
        node4 = thingy('node4')
        node5 = thingy('node5')
        node6 = thingy('node6')
        node7 = thingy('node7')
        tuples = [
            (node1, node2),
            (node3, node4),
            (node4, node5),
            (node5, node6),
            (node6, node2)
        ]
        self._assert_sort(tuples, [node1,node2,node3,node4,node5,node6,node7])

    def testsort3(self):
        ['Mapper|Keyword|keywords,Mapper|IKAssociation|itemkeywords', 'Mapper|Item|items,Mapper|IKAssociation|itemkeywords']
        node1 = thingy('keywords')
        node2 = thingy('itemkeyowrds')
        node3 = thingy('items')
        tuples = [
            (node1, node2),
            (node3, node2),
            (node1,node3)
        ]
        self._assert_sort(tuples, [node1, node2, node3])
        self._assert_sort(tuples, [node3, node1, node2])
        self._assert_sort(tuples, [node3, node2, node1])
        

    def testsort4(self):
        node1 = thingy('keywords')
        node2 = thingy('itemkeyowrds')
        node3 = thingy('items')
        node4 = thingy('hoho')
        tuples = [
            (node1, node2),
            (node4, node1),
            (node1, node3),
            (node3, node2)
        ]
        self._assert_sort(tuples, [node1,node2,node3,node4])

    def testsort5(self):
        # this one, depenending on the weather, 
#    thingy(5780972, node4)  (idself=5781292, idparent=None)
#     thingy(5780876, node1)  (idself=5781068, idparent=5781292)
#      thingy(5780908, node2)  (idself=5781164, idparent=5781068)
#       thingy(5780940, node3)  (idself=5781228, idparent=5781164)
   
        node1 = thingy('node1') #thingy('00B94190')
        node2 = thingy('node2') #thingy('00B94990')
        node3 = thingy('node3') #thingy('00B9A9B0')
        node4 = thingy('node4') #thingy('00B4F210')
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
        self._assert_sort(tuples, allitems)

    def testsort6(self):
        #('tbl_c', 'tbl_d'), ('tbl_a', 'tbl_c'), ('tbl_b', 'tbl_d')
        nodea = thingy('tbl_a')
        nodeb = thingy('tbl_b')
        nodec = thingy('tbl_c')
        noded = thingy('tbl_d')
        tuples = [
            (nodec, noded),
            (nodea, nodec),
            (nodeb, noded)
        ]
        allitems = [nodea,nodeb,nodec,noded]
        self._assert_sort(tuples, allitems)

    def testcircular(self):
        node1 = thingy('node1')
        node2 = thingy('node2')
        node3 = thingy('node3')
        node4 = thingy('node4')
        node5 = thingy('node5')
        tuples = [
            (node4, node5),
            (node5, node4),
            (node1, node2),
            (node2, node3),
            (node3, node1),
            (node4, node1)
        ]
        self._assert_sort(tuples, [node1,node2,node3,node4,node5], allow_all_cycles=True)
        

if __name__ == "__main__":
    unittest.main()
