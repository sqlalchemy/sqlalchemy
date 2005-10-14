from sqlalchemy.mapper import *
from sqlalchemy.schema import *
from sqlalchemy.sql import *
import sqlalchemy.util as util
import tables
import string, sys

"""a more advanced example of basic_tree.py.  illustrates MapperExtension objects which
add application-specific functionality to a Mapper object."""

class NodeList(util.OrderedDict):
    """extends an Ordered Dictionary, which is just a dictionary that returns its keys and values
    in order upon iteration.  Adds functionality to automatically associate 
    the parent of a TreeNode with itself, upon append to the parent's list of child nodes."""
    def __init__(self, parent):
        util.OrderedDict.__init__(self)
        self.parent = parent
    def append(self, node):
        node.parent = self.parent
        node._set_root(self.parent.root)
        self[node.name] = node
    def __iter__(self):
        return iter(self.values())

class TreeNode(object):
    """a rich Tree class which includes path-based operations"""
    def __init__(self, name):
        self.children = NodeList(self)
        self.name = name
        self.root = self
        self.parent = None
        self.id = None
        self.parent_id = None
        self.root_id=None
    def _set_root(self, root):
        self.root = root
        for c in self.children:
            c._set_root(root)
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
        s = ('  ' * level) + "%s (%s,%s,%s, %d)" % (self.name, self.id,self.parent_id,self.root_id, id(self)) + '\n'
        if expand:
            s += string.join([n._getstring(level+1, True) for n in self.children.values()], '')
        return s
        
class TreeLoader(MapperExtension):
    def create_instance(self, mapper, row, imap, class_):
        return TreeNode(row[mapper.c.name.label], _mapper_nohistory=True)
    def after_insert(self, mapper, instance):
        if instance.root is instance:
            mapper.primarytable.update(TreeNode.c.id==instance.id, values=dict(root_node_id=instance.id)).execute()
            instance.root_id = instance.id
    def append_result(self, mapper, row, imap, result, instance, populate_existing=False):
        if instance.parent_id is None:
            result.append(instance)
        else:
            parentnode = imap[mapper.identity_key(instance.parent_id)]
            parentnode.children.append(instance, _mapper_nohistory=True)
        return False
            
# define the mapper.  we will make "convenient" property
# names vs. the more verbose names in the table definition

TreeNode.mapper=assignmapper(tables.trees, properties=dict(
    id=tables.trees.c.node_id,
    name=tables.trees.c.node_name,
    parent_id=tables.trees.c.parent_node_id,
    root_id=tables.trees.c.root_node_id,
    root=relation(TreeNode, primaryjoin=tables.trees.c.root_node_id==tables.trees.c.node_id, thiscol=tables.trees.c.root_node_id, lazy=None, uselist=False),
    children=relation(TreeNode, primaryjoin=tables.trees.c.parent_node_id==tables.trees.c.node_id, thiscol=tables.trees.c.node_id, lazy=None, uselist=True, private=True),
), extension = TreeLoader())
TreeNode.mapper

node2 = TreeNode('node2')
node2.append('subnode1')
node = TreeNode('rootnode')
#node.root = node
node.append('node1')
node.append(node2)
node.append('node3')
node.children['node2'].append('subnode2')
print node._getstring(0, True)
objectstore.commit()
print "\n\n\n"
node.append('node4')
objectstore.commit()

node.children['node4'].append('subnode3')
node.children['node4'].append('subnode4')
node.children['node4'].children['subnode3'].append('subsubnode1')
print node._getstring(0, True)
#raise "hi"
del node.children['node1']
objectstore.commit()

nodeid = node.id

objectstore.clear()
print "\n\n\n"
t = TreeNode.mapper.select(TreeNode.c.root_id==nodeid, order_by=[TreeNode.c.id])[0]

print t._getstring(0, True)
objectstore.delete(t)
objectstore.commit()




