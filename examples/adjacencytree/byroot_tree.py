"""A more advanced example of basic_tree.py.

Treenodes can now reference their "root" node, and introduces a new
selection method which selects an entire tree of nodes at once, taking
advantage of a custom MapperExtension to assemble incoming nodes into their
correct structure.
"""

from sqlalchemy import MetaData, Table, Column, Sequence, ForeignKey
from sqlalchemy import Integer, String
from sqlalchemy.orm import create_session, mapper, relation, backref
from sqlalchemy.orm import MapperExtension
from sqlalchemy.orm.collections import attribute_mapped_collection


metadata = MetaData('sqlite:///')
metadata.bind.echo = True

# Create the `treenodes` table, a basic adjacency list model table.
# One additional column, "root_id", references a "root node" row and is used
# in the 'byroot_tree' example.

trees = Table('treenodes', metadata,
    Column('id', Integer, Sequence('treenode_id_seq', optional=True),
           primary_key=True),
    Column('parent_id', Integer, ForeignKey('treenodes.id'), nullable=True),
    Column('root_id', Integer, ForeignKey('treenodes.id'), nullable=True),
    Column('name', String(50), nullable=False),
    Column('data_id', Integer, ForeignKey('treedata.data_id')))

treedata = Table(
    "treedata", metadata,
    Column('data_id', Integer, primary_key=True),
    Column('value', String(100), nullable=False))


class TreeNode(object):
    """A hierarchical Tree class,

    Adds the concept of a "root node".  The root is the topmost node in a
    tree, or in other words a node whose parent ID is NULL.  All child nodes
    that are decendents of a particular root, as well as a root node itself,
    reference this root node.
    """

    def __init__(self, name):
        self.name = name
        self.root = self

    def _set_root(self, root):
        self.root = root
        for c in self.children.values():
            c._set_root(root)

    def append(self, node):
        if isinstance(node, str):
            node = TreeNode(node)
        node._set_root(self.root)
        self.children.set(node)

    def __repr__(self):
        return self._getstring(0, False)

    def __str__(self):
        return self._getstring(0, False)

    def _getstring(self, level, expand = False):
        s = "%s%s (%s,%s,%s, %d): %s\n" % (
            ('  ' * level), self.name, self.id,self.parent_id,
            self.root_id, id(self), repr(self.data))
        if expand:
            s += ''.join([n._getstring(level+1, True)
                          for n in self.children.values()])
        return s

    def print_nodes(self):
        return self._getstring(0, True)

class TreeLoader(MapperExtension):

    def after_insert(self, mapper, connection, instance):
        """
        Runs after the insert of a new TreeNode row.  The primary key of the
        row is not determined until the insert is complete, since most DB's
        use autoincrementing columns.  If this node is the root node, we
        will take the new primary key and update it as the value of the
        node's "root ID" as well, since its root node is itself.
        """

        if instance.root is instance:
            connection.execute(mapper.mapped_table.update(
                TreeNode.c.id==instance.id, values=dict(root_id=instance.id)))
            instance.root_id = instance.id

    def append_result(self, mapper, selectcontext, row, instance, result, **flags):
        """
        Runs as results from a SELECT statement are processed, and newly
        created or already-existing instances that correspond to each row
        are appended to result lists.  This method will only append root
        nodes to the result list, and will attach child nodes to their
        appropriate parent node as they arrive from the select results.
        This allows a SELECT statement which returns both root and child
        nodes in one query to return a list of "roots".
        """

        isnew = flags.get('isnew', False)

        if instance.parent_id is None:
            result.append(instance)
        else:
            if isnew or selectcontext.populate_existing:
                key = mapper.identity_key_from_primary_key(instance.parent_id)
                parentnode = selectcontext.identity_map[key]
                parentnode.children.set(instance)
        return False

class TreeData(object):
    def __init__(self, value=None):
        self.id = None
        self.value = value
    def __repr__(self):
        return "TreeData(%s, %s)" % (repr(self.id), repr(self.value))


print "\n\n\n----------------------------"
print "Creating Tree Table:"
print "----------------------------"

metadata.create_all()

mapper(TreeNode, trees, properties=dict(
    # 'root' attribute.  has a load-only backref '_descendants' that loads
    # all nodes with the same root ID eagerly, which are intercepted by the
    # TreeLoader extension and populated into the "children" collection.
    root=relation(TreeNode, primaryjoin=trees.c.root_id==trees.c.id,
                  remote_side=trees.c.id, lazy=None,
                  backref=backref('_descendants', lazy=False, join_depth=1,
                                  primaryjoin=trees.c.root_id==trees.c.id,viewonly=True)),

    # 'children' attribute.  collection of immediate child nodes.  this is a
    # non-loading relation which is populated by the TreeLoader extension.
    children=relation(TreeNode, primaryjoin=trees.c.parent_id==trees.c.id,
        lazy=None, cascade="all",
        collection_class=attribute_mapped_collection('name'),
        backref=backref('parent',
                        primaryjoin=trees.c.parent_id==trees.c.id,
                        remote_side=trees.c.id)
        ),

    # 'data' attribute.  A collection of secondary objects which also loads
    # eagerly.
    data=relation(TreeData, cascade="all, delete-orphan", lazy=False)

), extension=TreeLoader())

mapper(TreeData, treedata, properties={'id':treedata.c.data_id})

session = create_session()

node2 = TreeNode('node2')
node2.append('subnode1')
node = TreeNode('rootnode')
node.append('node1')
node.append(node2)
node.append('node3')
node.children['node3'].data = TreeData('node 3s data')
node.children['node2'].append('subnode2')
node.children['node1'].data = TreeData('node 1s data')

print "\n\n\n----------------------------"
print "Created new tree structure:"
print "----------------------------"

print node.print_nodes()

print "\n\n\n----------------------------"
print "flushing:"
print "----------------------------"

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
print "Committing:"
print "----------------------------"
session.flush()

print "\n\n\n----------------------------"
print "Tree After Save:"
print "----------------------------"

print node.print_nodes()

nodeid = node.id

print "\n\n\n----------------------------"
print "Clearing objectstore, selecting "
print "tree new where root_id=%d:" % nodeid
print "----------------------------"

session.clear()

# load some nodes.  we do this based on "root id" which will load an entire
# sub-tree in one pass.  the MapperExtension will assemble the incoming
# nodes into a tree structure.
t = (session.query(TreeNode).
       filter(TreeNode.c.root_id==nodeid).
       order_by([TreeNode.c.id]))[0]

print "\n\n\n----------------------------"
print "Full Tree:"
print "----------------------------"
print t.print_nodes()

print "\n\n\n----------------------------"
print "Marking root node as deleted"
print "and committing:"
print "----------------------------"
session.delete(t)
session.flush()




