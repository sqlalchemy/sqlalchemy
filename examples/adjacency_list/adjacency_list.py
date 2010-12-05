from sqlalchemy import MetaData, Table, Column, Sequence, ForeignKey,\
                        Integer, String, create_engine
                        
from sqlalchemy.orm import sessionmaker, mapper, relationship, backref,\
                                joinedload_all
                                
from sqlalchemy.orm.collections import attribute_mapped_collection

metadata = MetaData()

tree_table = Table('tree', metadata,
    Column('id', Integer, primary_key=True),
    Column('parent_id', Integer, ForeignKey('tree.id')),
    Column('name', String(50), nullable=False)
)

class TreeNode(object):
    def __init__(self, name, parent=None):
        self.name = name
        self.parent = parent
        
    def append(self, nodename):
        self.children[nodename] = TreeNode(nodename, parent=self)
    
    def __repr__(self):
        return "TreeNode(name=%r, id=%r, parent_id=%r)" % (
                    self.name,
                    self.id,
                    self.parent_id
                )
                
def dump_tree(node, indent=0):
    
    return "   " * indent + repr(node) + \
                "\n" + \
                "".join([
                    dump_tree(c, indent +1) 
                    for c in node.children.values()]
                )
                    

mapper(TreeNode, tree_table, properties={
    'children': relationship(TreeNode, 

                        # cascade deletions
                        cascade="all",
    
                        # many to one + adjacency list - remote_side
                        # is required to reference the 'remote' 
                        # column in the join condition.
                        backref=backref("parent", remote_side=tree_table.c.id),
                         
                        # children will be represented as a dictionary
                        # on the "name" attribute.
                        collection_class=attribute_mapped_collection('name'),
                    )
    })

if __name__ == '__main__':
    engine = create_engine('sqlite://', echo=True)

    def msg(msg):
        print "\n\n\n" + "-" * len(msg)
        print msg
        print "-" * len(msg)

    msg("Creating Tree Table:")

    metadata.create_all(engine)

    # session.  using expire_on_commit=False
    # so that the session's contents are not expired
    # after each transaction commit.
    session = sessionmaker(engine, expire_on_commit=False)()

    node = TreeNode('rootnode')
    node.append('node1')
    node.append('node3')

    node2 = TreeNode('node2')
    node2.append('subnode1')
    node.children['node2'] = node2
    node.children['node2'].append('subnode2')

    msg("Created new tree structure:")

    print dump_tree(node)

    msg("flush + commit:")

    session.add(node)
    session.commit()

    msg("Tree After Save:")

    print dump_tree(node)

    node.append('node4')
    node.children['node4'].append('subnode3')
    node.children['node4'].append('subnode4')
    node.children['node4'].children['subnode3'].append('subsubnode1')

    # mark node1 as deleted and remove
    session.delete(node.children['node1'])

    msg("Removed node1.  flush + commit:")
    session.commit()

    print "\n\n\n----------------------------"
    print "Tree After Save:"
    print "----------------------------"

    # expire the "children" collection so that
    # it reflects the deletion of "node1".
    session.expire(node, ['children'])
    print dump_tree(node)

    msg("Emptying out the session entirely, "
        "selecting tree on root, using eager loading to join four levels deep.")
    session.expunge_all()
    node = session.query(TreeNode).\
                        options(joinedload_all("children", "children", 
                                                "children", "children")).\
                        filter(TreeNode.name=="rootnode").\
                        first()

    msg("Full Tree:")
    print dump_tree(node)

    msg( "Marking root node as deleted, flush + commit:" )

    session.delete(node)
    session.commit()
