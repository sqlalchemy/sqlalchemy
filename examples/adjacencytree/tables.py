from sqlalchemy.schema import *
import sqlalchemy.engine

engine = sqlalchemy.engine.create_engine('sqlite', ':memory:', {}, echo = True)

trees = Table('treenodes', engine,
    Column('node_id', Integer, primary_key=True),
    Column('parent_node_id', Integer, ForeignKey('treenodes.node_id'), nullable=True),
    Column('root_node_id', Integer, ForeignKey('treenodes.node_id'), nullable=True),
    Column('node_name', String(50), nullable=False)
    )
    
trees.create()