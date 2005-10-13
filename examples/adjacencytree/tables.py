from sqlalchemy.schema import *
import sqlalchemy.engine

engine = sqlalchemy.engine.create_engine('sqlite', ':memory:', {}, echo = True)

"""create the treenodes table.  This is ia basic adjacency list model table.
One additional column, "root_node_id", references a "root node" row and is used
in the 'byroot_tree' example."""

trees = Table('treenodes', engine,
    Column('node_id', Integer, primary_key=True),
    Column('parent_node_id', Integer, ForeignKey('treenodes.node_id'), nullable=True),
    Column('root_node_id', Integer, ForeignKey('treenodes.node_id'), nullable=True),
    Column('node_name', String(50), nullable=False)
    )
    
trees.create()