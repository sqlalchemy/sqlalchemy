"""A basic Adjacency List model tree."""

from sqlalchemy import MetaData, Table, Column, Sequence, ForeignKey
from sqlalchemy import Integer, String
from sqlalchemy.orm import create_session, mapper, relation, backref
from sqlalchemy.orm.collections import attribute_mapped_collection

metadata = MetaData('sqlite:///')
metadata.bind.echo = True

trees = Table('treenodes', metadata,
    Column('id', Integer, Sequence('treenode_id_seq', optional=True),
           primary_key=True),
    Column('parent_id', Integer, ForeignKey('treenodes.id'), nullable=True),
    Column('name', String(50), nullable=False))


class TreeNode(object):
    """a rich Tree class which includes path-based operations"""
    def __init__(self, name):
        self.name = name
        self.parent = None
        self.id = None
        self.parent_id = None
    def append(self, node):
        if isinstance(node, str):
            node = TreeNode(node)
        node.parent = self
        self.children[node.name] = node
    def __repr__(self):
        return self._getstring(0, False)
    def __str__(self):
        return self._getstring(0, False)
    def _getstring(self, level, expand = False):
        s = ('  ' * level) + "%s (%s,%s, %d)" % (
            self.name, self.id,self.parent_id,id(self)) + '\n'
        if expand:
            s += ''.join([n._getstring(level+1, True)
                          for n in self.children.values()])
        return s
    def print_nodes(self):
        return self._getstring(0, True)

mapper(TreeNode, trees, properties={
    'children': relation(TreeNode, cascade="all",
                         backref=backref("parent", remote_side=[trees.c.id]),
                         collection_class=attribute_mapped_collection('name'),
                         lazy=False, join_depth=3)})

print "\n\n\n----------------------------"
print "Creating Tree Table:"
print "----------------------------"

trees.create()

node2 = TreeNode('node2')
node2.append('subnode1')
node = TreeNode('rootnode')
node.append('node1')
node.append(node2)
node.append('node3')
node.children['node2'].append('subnode2')

print "\n\n\n----------------------------"
print "Created new tree structure:"
print "----------------------------"

print node.print_nodes()

print "\n\n\n----------------------------"
print "Flushing:"
print "----------------------------"

session = create_session()
session.add(node)
session.flush()

print "\n\n\n----------------------------"
print "Tree After Save:"
print "----------------------------"

print node.print_nodes()

node.append('node4')
node.children['node4'].append('subnode3')
node.children['node4'].append('subnode4')
node.children['node4'].children['subnode3'].append('subsubnode1')
del node.children['node1']

print "\n\n\n----------------------------"
print "Modified the tree"
print "(added node4, node4/subnode3, node4/subnode4,"
print "node4/subnode3/subsubnode1, deleted node1):"
print "----------------------------"

print node.print_nodes()

print "\n\n\n----------------------------"
print "Flushing:"
print "----------------------------"
session.flush()

print "\n\n\n----------------------------"
print "Tree After Save:"
print "----------------------------"

print node.print_nodes()

nodeid = node.id

print "\n\n\n----------------------------"
print "Clearing session, selecting "
print "tree new where node_id=%d:" % nodeid
print "----------------------------"

session.expunge_all()
t = session.query(TreeNode).filter(TreeNode.id==nodeid)[0]

print "\n\n\n----------------------------"
print "Full Tree:"
print "----------------------------"
print t.print_nodes()

print "\n\n\n----------------------------"
print "Marking root node as deleted"
print "and flushing:"
print "----------------------------"
session.delete(t)
session.flush()
