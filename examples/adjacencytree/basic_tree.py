"""a basic Adjacency List model tree."""

from sqlalchemy import *
from sqlalchemy.util import OrderedDict

metadata = BoundMetaData('sqlite:///', echo=True)

trees = Table('treenodes', metadata,
    Column('node_id', Integer, Sequence('treenode_id_seq',optional=False), primary_key=True),
    Column('parent_node_id', Integer, ForeignKey('treenodes.node_id'), nullable=True),
    Column('node_name', String(50), nullable=False),
    )

class NodeList(OrderedDict):
    """subclasses OrderedDict to allow usage as a list-based property."""
    def append(self, node):
        self[node.name] = node
    def __iter__(self):
        return iter(self.values())

class TreeNode(object):
    """a rich Tree class which includes path-based operations"""
    def __init__(self, name):
        self.children = NodeList()
        self.name = name
        self.parent = None
        self.id = None
        self.parent_id = None
    def append(self, node):
        if isinstance(node, str):
            node = TreeNode(node)
        node.parent = self
        self.children.append(node)
    def __repr__(self):
        return self._getstring(0, False)
    def __str__(self):
        return self._getstring(0, False)
    def _getstring(self, level, expand = False):
        s = ('  ' * level) + "%s (%s,%s, %d)" % (self.name, self.id,self.parent_id,id(self)) + '\n'
        if expand:
            s += ''.join([n._getstring(level+1, True) for n in self.children.values()])
        return s
    def print_nodes(self):
        return self._getstring(0, True)
        
mapper(TreeNode, trees, properties=dict(
    id=trees.c.node_id,
    name=trees.c.node_name,
    parent_id=trees.c.parent_node_id,
    children=relation(TreeNode, cascade="all", backref=backref("parent", foreignkey=trees.c.node_id), collection_class=NodeList),
))

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
session.save(node)
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

session.clear()
t = session.query(TreeNode).select(TreeNode.c.id==nodeid)[0]

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
