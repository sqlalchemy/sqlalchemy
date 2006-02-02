from sqlalchemy import *
import sqlalchemy.util as util
import string, sys, time

"""a more advanced example of basic_tree.py.  illustrates MapperExtension objects which
add application-specific functionality to a Mapper object."""

engine = create_engine('sqlite://', echo = True)
#engine = sqlalchemy.engine.create_engine('mysql', {'db':'test', 'host':'127.0.0.1', 'user':'scott'}, echo=True)
#engine = sqlalchemy.engine.create_engine('postgres', {'database':'test', 'host':'127.0.0.1', 'user':'scott', 'password':'tiger'}, echo=True)
#engine = sqlalchemy.engine.create_engine('oracle', {'dsn':os.environ['DSN'], 'user':os.environ['USER'], 'password':os.environ['PASSWORD']}, echo=True)

"""create the treenodes table.  This is ia basic adjacency list model table.
One additional column, "root_node_id", references a "root node" row and is used
in the 'byroot_tree' example."""

trees = Table('treenodes', engine,
    Column('node_id', Integer, Sequence('treenode_id_seq',optional=False), primary_key=True),
    Column('parent_node_id', Integer, ForeignKey('treenodes.node_id'), nullable=True),
    Column('root_node_id', Integer, ForeignKey('treenodes.node_id'), nullable=True),
    Column('node_name', String(50), nullable=False),
    Column('data_ident', Integer, ForeignKey('treedata.data_id'))
    )

treedata = Table(
    "treedata", engine, 
    Column('data_id', Integer, primary_key=True),
    Column('value', String(100), nullable=False)
)
    


class NodeList(util.OrderedDict):
    """extends an Ordered Dictionary, which is just a dictionary that iterates its keys and values
    in the order they were inserted.  Adds an "append" method, which appends a node to the 
    dictionary as though it were a list, and also within append automatically associates 
    the parent of a TreeNode with itself."""
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
    """a hierarchical Tree class, which adds the concept of a "root node".  The root is 
    the topmost node in a tree, or in other words a node whose parent ID is NULL.  
    All child nodes that are decendents of a particular root, as well as a root node itself, 
    reference this root node.  
    this is useful as a way to identify all nodes in a tree as belonging to a single
    identifiable root.  Any node can return its root node and therefore the "tree" that it 
    belongs to, and entire trees can be selected from the database in one query, by 
    identifying their common root ID."""
    
    def __init__(self, name):
        """for data integrity, a TreeNode requires its name to be passed as a parameter
        to its constructor, so there is no chance of a TreeNode that doesnt have a name."""
        self.name = name
        self.children = NodeList(self)
        self.root = self
        self.parent = None
        self.id = None
        self.data =None
        self.parent_id = None
        self.root_id=None
    def _set_root(self, root):
        self.root = root
        for c in self.children:
            c._set_root(root)
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
        s = ('  ' * level) + "%s (%s,%s,%s, %d): %s" % (self.name, self.id,self.parent_id,self.root_id, id(self), repr(self.data)) + '\n'
        if expand:
            s += string.join([n._getstring(level+1, True) for n in self.children.values()], '')
        return s
    def print_nodes(self):
        return self._getstring(0, True)
        
class TreeLoader(MapperExtension):
    """an extension that will plug-in additional functionality to the Mapper."""
    def create_instance(self, mapper, row, imap, class_):
        """creates an instance of a TreeNode.  since the TreeNode constructor requires
        the 'name' argument, this method pulls the data from the database row directly."""
        return TreeNode(row[mapper.c.name], _mapper_nohistory=True)
    def after_insert(self, mapper, instance):
        """runs after the insert of a new TreeNode row.  The primary key of the row is not determined
        until the insert is complete, since most DB's use autoincrementing columns.  If this node is
        the root node, we will take the new primary key and update it as the value of the node's 
        "root ID" as well, since its root node is itself."""
        if instance.root is instance:
            mapper.primarytable.update(TreeNode.c.id==instance.id, values=dict(root_node_id=instance.id)).execute()
            instance.root_id = instance.id
    def append_result(self, mapper, row, imap, result, instance, isnew, populate_existing=False):
        """runs as results from a SELECT statement are processed, and newly created or already-existing
        instances that correspond to each row are appended to result lists.  This method will only
        append root nodes to the result list, and will attach child nodes to their appropriate parent
        node as they arrive from the select results.  This allows a SELECT statement which returns
        both root and child nodes in one query to return a list of "roots"."""
        if instance.parent_id is None:
            result.append(instance)
        else:
            if isnew or populate_existing:
                parentnode = imap[mapper.identity_key(instance.parent_id)]
                parentnode.children.append(instance, _mapper_nohistory=True)
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

treedata.create()    
trees.create()


assign_mapper(TreeNode, trees, properties=dict(
    id=trees.c.node_id,
    name=trees.c.node_name,
    parent_id=trees.c.parent_node_id,
    root_id=trees.c.root_node_id,
    root=relation(TreeNode, primaryjoin=trees.c.root_node_id==trees.c.node_id, foreignkey=trees.c.node_id, lazy=None, uselist=False),
    children=relation(TreeNode, primaryjoin=trees.c.parent_node_id==trees.c.node_id, lazy=None, uselist=True, private=True),
    data=relation(mapper(TreeData, treedata, properties=dict(id=treedata.c.data_id)), private=True, lazy=False)
    
), extension = TreeLoader())
TreeNode.mapper


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
print "Committing:"
print "----------------------------"

objectstore.commit()
#sys.exit()
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
print "tree new where root_id=%d:" % nodeid
print "----------------------------"

objectstore.clear()
t = TreeNode.mapper.select(TreeNode.c.root_id==nodeid, order_by=[TreeNode.c.id])[0]

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




