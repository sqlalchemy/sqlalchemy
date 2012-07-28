"""a directed graph example."""

from sqlalchemy import MetaData, Table, Column, Integer, ForeignKey, \
    create_engine
from sqlalchemy.orm import mapper, relationship, sessionmaker
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Node(Base):
    __tablename__ = 'node'

    node_id = Column(Integer, primary_key=True)

    def __init__(self, id):
        self.node_id = id

    def add_neighbors(self, *nodes):
        for node in nodes:
            Edge(self, node)
        return self

    def higher_neighbors(self):
        return [x.higher_node for x in self.lower_edges]

    def lower_neighbors(self):
        return [x.lower_node for x in self.higher_edges]

class Edge(Base):
    __tablename__ = 'edge'

    lower_id = Column(Integer,
                        ForeignKey('node.node_id'),
                        primary_key=True)

    higher_id = Column(Integer,
                        ForeignKey('node.node_id'),
                        primary_key=True)

    lower_node = relationship(Node,
                                primaryjoin=lower_id==Node.node_id,
                                backref='lower_edges')
    higher_node = relationship(Node,
                                primaryjoin=higher_id==Node.node_id,
                                backref='higher_edges')

    # here we have lower.node_id <= higher.node_id
    def __init__(self, n1, n2):
        if n1.node_id < n2.node_id:
            self.lower_node = n1
            self.higher_node = n2
        else:
            self.lower_node = n2
            self.higher_node = n1

engine = create_engine('sqlite://', echo=True)
Base.metadata.create_all(engine)

session = sessionmaker(engine)()

# create a directed graph like this:
#       n1 -> n2 -> n5
#                -> n7
#          -> n3 -> n6

n1 = Node(1)
n2 = Node(2)
n3 = Node(3)
n4 = Node(4)
n5 = Node(5)
n6 = Node(6)
n7 = Node(7)

n2.add_neighbors(n5, n1)
n3.add_neighbors(n6)
n7.add_neighbors(n2)
n1.add_neighbors(n3)

session.add_all([n1, n2, n3, n4, n5, n6, n7])
session.commit()

assert [x.node_id for x in n3.higher_neighbors()] == [6]
assert [x.node_id for x in n3.lower_neighbors()] == [1]
assert [x.node_id for x in n2.lower_neighbors()] == [1]
assert [x.node_id for x in n2.higher_neighbors()] == [5,7]


