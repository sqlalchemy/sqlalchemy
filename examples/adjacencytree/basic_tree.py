from sqlalchemy import *
import sqlalchemy.util as util
import tables
import string, sys

"""a basic Adjacency List model tree."""

class NodeList(util.OrderedDict):
    """extends an Ordered Dictionary, which is just a dictionary that returns its keys and values
    in order upon iteration.  Adds functionality to automatically associate 
    the parent of a TreeNode with itself, upon append to the parent's list of child nodes."""
    def __init__(self, parent):
        util.OrderedDict.__init__(self)
        self.parent = parent
    def append(self, node):
        node.parent = self.parent
        self[node.name] = node
    def __iter__(self):
        return iter(self.values())

class TreeNode(object):
    """a rich Tree class which includes path-based operations"""
    def __init__(self, name=None):
        self.children = NodeList(self)
        self.name = name
        self.parent = None
        self.id = None
        self.parent_id = None
    def append(self, node):
        if isinstance(node, str):
            self.children.append(TreeNode(node))
        else:
            self.children.append(node)
    def __repr__(self):
        return self._getstring(0, False)
    def __str__(self):
        return self._getstring(0, False)
    def _getstring(self, level, expand = False):
        s = ('  ' * level) + "%s (%s,%s, %d)" % (self.name, self.id,self.parent_id,id(self)) + '\n'
        if expand:
            s += string.join([n._getstring(level+1, True) for n in self.children.values()], '')
        return s
    def print_nodes(self):
        return self._getstring(0, True)
        
# define the mapper.  we will make "convenient" property
# names vs. the more verbose names in the table definition

assign_mapper(TreeNode, tables.trees, properties=dict(
    id=tables.trees.c.node_id,
    name=tables.trees.c.node_name,
    parent_id=tables.trees.c.parent_node_id,
    root_id=tables.trees.c.root_node_id,
    children=relation(TreeNode, primaryjoin=tables.trees.c.parent_node_id==tables.trees.c.node_id, lazy=True, uselist=True, private=True),
))

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
print "Committing:"
print "----------------------------"

objectstore.commit()

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
print "Committing:"
print "----------------------------"
objectstore.commit()

print "\n\n\n----------------------------"
print "Tree After Save:"
print "----------------------------"

print node.print_nodes()

nodeid = node.id

print "\n\n\n----------------------------"
print "Clearing objectstore, selecting "
print "tree new where node_id=%d:" % nodeid
print "----------------------------"

objectstore.clear()
t = TreeNode.mapper.select(TreeNode.c.node_id==nodeid)[0]

print "\n\n\n----------------------------"
print "Full Tree:"
print "----------------------------"
print t.print_nodes()

print "\n\n\n----------------------------"
print "Marking root node as deleted"
print "and committing:"
print "----------------------------"
objectstore.delete(t)
objectstore.commit()
