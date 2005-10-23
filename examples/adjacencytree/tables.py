from sqlalchemy.schema import *
import sqlalchemy.engine
import os

#engine = sqlalchemy.engine.create_engine('sqlite', ':memory:', {}, echo = True)
#engine = sqlalchemy.engine.create_engine('postgres', {'database':'test', 'host':'127.0.0.1', 'user':'scott', 'password':'tiger'}, echo=True)
db = sqlalchemy.engine.create_engine('oracle', {'dsn':os.environ['DSN'], 'user':os.environ['USER'], 'password':os.environ['PASSWORD']}, echo=True)


"""create the treenodes table.  This is ia basic adjacency list model table.
One additional column, "root_node_id", references a "root node" row and is used
in the 'byroot_tree' example."""

trees = Table('treenodes', engine,
    Column('node_id', Integer, Sequence('tree_id_seq', optional=True), primary_key=True),
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
    
print "\n\n\n----------------------------"
print "Creating Tree Table:"
print "----------------------------"

treedata.create()    
trees.create()


