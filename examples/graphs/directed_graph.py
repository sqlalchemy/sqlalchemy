"""a directed graph example."""

from sqlalchemy import MetaData, Table, Column, Integer, ForeignKey
from sqlalchemy.orm import mapper, relationship, create_session

import logging
logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

meta = MetaData('sqlite://')

nodes = Table('nodes', meta,
    Column("nodeid", Integer, primary_key=True)
)
 
# here we have lower.nodeid <= higher.nodeid
edges = Table('edges', meta,
    Column("lower_id", Integer, ForeignKey('nodes.nodeid'), primary_key=True),
    Column("higher_id", Integer, ForeignKey('nodes.nodeid'), primary_key=True)
)

meta.create_all()

class Node(object):
    def __init__(self, id):
        self.nodeid = id
    def add_neighbor(self, othernode):
        Edge(self, othernode)
    def higher_neighbors(self):
        return [x.higher_node for x in self.lower_edges]
    def lower_neighbors(self):
        return [x.lower_node for x in self.higher_edges]

class Edge(object):
    def __init__(self, n1, n2):
        if n1.nodeid < n2.nodeid:
            self.lower_node = n1
            self.higher_node = n2
        else:
            self.lower_node = n2
            self.higher_node = n1

mapper(Node, nodes)
mapper(Edge, edges, properties={
    'lower_node':relationship(Node,
primaryjoin=edges.c.lower_id==nodes.c.nodeid, backref='lower_edges'),
    'higher_node':relationship(Node,
primaryjoin=edges.c.higher_id==nodes.c.nodeid, backref='higher_edges')
    }
)

session = create_session()

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

n2.add_neighbor(n5)
n3.add_neighbor(n6)
n7.add_neighbor(n2)
n1.add_neighbor(n3)
n2.add_neighbor(n1)

[session.add(x) for x in [n1, n2, n3, n4, n5, n6, n7]]
session.flush()

session.expunge_all()

n2 = session.query(Node).get(2)
n3 = session.query(Node).get(3)

assert [x.nodeid for x in n3.higher_neighbors()] == [6]
assert [x.nodeid for x in n3.lower_neighbors()] == [1]
assert [x.nodeid for x in n2.lower_neighbors()] == [1]
assert [x.nodeid for x in n2.higher_neighbors()] == [5,7]


