"""a directed graph example."""

from sqlalchemy import Column, Integer, ForeignKey, \
    create_engine
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Node(Base):
    __tablename__ = 'node'

    node_id = Column(Integer, primary_key=True)

    def higher_neighbors(self):
        return [x.higher_node for x in self.lower_edges]

    def lower_neighbors(self):
        return [x.lower_node for x in self.higher_edges]


class Edge(Base):
    __tablename__ = 'edge'

    lower_id = Column(
        Integer,
        ForeignKey('node.node_id'),
        primary_key=True)

    higher_id = Column(
        Integer,
        ForeignKey('node.node_id'),
        primary_key=True)

    lower_node = relationship(
        Node,
        primaryjoin=lower_id == Node.node_id,
        backref='lower_edges')

    higher_node = relationship(
        Node,
        primaryjoin=higher_id == Node.node_id,
        backref='higher_edges')

    def __init__(self, n1, n2):
        self.lower_node = n1
        self.higher_node = n2

engine = create_engine('sqlite://', echo=True)
Base.metadata.create_all(engine)

session = sessionmaker(engine)()

# create a directed graph like this:
#       n1 -> n2 -> n1
#                -> n5
#                -> n7
#          -> n3 -> n6

n1 = Node()
n2 = Node()
n3 = Node()
n4 = Node()
n5 = Node()
n6 = Node()
n7 = Node()

Edge(n1, n2)
Edge(n1, n3)
Edge(n2, n1)
Edge(n2, n5)
Edge(n2, n7)
Edge(n3, n6)

session.add_all([n1, n2, n3, n4, n5, n6, n7])
session.commit()

assert [x for x in n3.higher_neighbors()] == [n6]
assert [x for x in n3.lower_neighbors()] == [n1]
assert [x for x in n2.lower_neighbors()] == [n1]
assert [x for x in n2.higher_neighbors()] == [n1, n5, n7]

