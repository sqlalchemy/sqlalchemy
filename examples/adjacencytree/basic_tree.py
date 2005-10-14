from sqlalchemy.mapper import *
from sqlalchemy.schema import *
from sqlalchemy.sql import *
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
    def get_child_by_path(self, path):
        node = self
        try:
            for token in path.split('/'):
                node = node.children[token]
            else:
                return node
        except KeyError:
            return None
    def append(self, node):
        if isinstance(node, str):
            self.children.append(TreeNode(node))
        else:
            self.children.append(node)
    def _get_path(self):
        if self.parent is None:
            return '/'
        else:
            return self.parent._get_path() + self.name + '/'
    path = property(lambda s: s._path())
    def __str__(self):
        return self._getstring(0, False)
    def _getstring(self, level, expand = False):
        s = ('  ' * level) + "%s (%s,%s)" % (self.name, self.id,self.parent_id) + '\n'
        if expand:
            s += string.join([n._getstring(level+1, True) for n in self.children.values()], '')
        return s
    def print_nodes(self):
        return self._getstring(0, True)
        
# define the mapper.  we will make "convenient" property
# names vs. the more verbose names in the table definition

TreeNode.mapper=assignmapper(tables.trees, class_=TreeNode, properties=dict(
    id=tables.trees.c.node_id,
    name=tables.trees.c.node_name,
    parent_id=tables.trees.c.parent_node_id,
    root_id=tables.trees.c.root_node_id,
    children=relation(TreeNode, primaryjoin=tables.trees.c.parent_node_id==tables.trees.c.node_id, thiscol=tables.trees.c.node_id, lazy=True, uselist=True, private=True),
))


node2 = TreeNode('node2')
node2.append('subnode1')
node = TreeNode('rootnode')
node.append('node1')
node.append(node2)
node.append('node3')
node.children['node2'].append('subnode2')
print node.print_nodes()

objectstore.commit()

node.append('node4')
node.children['node4'].append('subnode3')
node.children['node4'].append('subnode4')
node.children['node4'].children['subnode3'].append('subsubnode1')
del node.children['node1']

print node.print_nodes()

objectstore.commit()

id = node.id

objectstore.clear()
print "\n\n\n"

t = TreeNode.mapper.select(TreeNode.c.id == id)[0]

print t.print_nodes()
objectstore.delete(t)
objectstore.commit()




