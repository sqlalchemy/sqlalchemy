"""Illustrates the "materialized paths" pattern.

Materialized paths is a way to represent a tree structure in SQL with fast
descendant and ancestor queries at the expense of moving nodes (which require
O(n) UPDATEs in the worst case, where n is the number of nodes in the tree). It
is a good balance in terms of performance and simplicity between the nested
sets model and the adjacency list model.

It works by storing all nodes in a table with a path column, containing a
string of delimited IDs. Think file system paths:

    1
    1.2
    1.3
    1.3.4
    1.3.5
    1.3.6
    1.7
    1.7.8
    1.7.9
    1.7.9.10
    1.7.11

Descendant queries are simple left-anchored LIKE queries, and ancestors are
already stored in the path itself. Updates require going through all
descendants and changing the prefix.

"""
from sqlalchemy import Column
from sqlalchemy import create_engine
from sqlalchemy import func
from sqlalchemy import Integer
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import foreign
from sqlalchemy.orm import relationship
from sqlalchemy.orm import remote
from sqlalchemy.orm import Session
from sqlalchemy.sql.expression import cast


Base = declarative_base()


class Node(Base):
    __tablename__ = "node"

    id = Column(Integer, primary_key=True, autoincrement=False)
    path = Column(String(500), nullable=False, index=True)

    # To find the descendants of this node, we look for nodes whose path
    # starts with this node's path.
    descendants = relationship(
        "Node",
        viewonly=True,
        order_by=path,
        primaryjoin=remote(foreign(path)).like(path.concat(".%")),
    )

    # Finding the ancestors is a little bit trickier. We need to create a fake
    # secondary table since this behaves like a many-to-many join.
    secondary = select(
        id.label("id"),
        func.unnest(
            cast(
                func.string_to_array(
                    func.regexp_replace(path, r"\.?\d+$", ""), "."
                ),
                ARRAY(Integer),
            )
        ).label("ancestor_id"),
    ).alias()
    ancestors = relationship(
        "Node",
        viewonly=True,
        secondary=secondary,
        primaryjoin=id == secondary.c.id,
        secondaryjoin=secondary.c.ancestor_id == id,
        order_by=path,
    )

    @property
    def depth(self):
        return len(self.path.split(".")) - 1

    def __repr__(self):
        return "Node(id={})".format(self.id)

    def __str__(self):
        root_depth = self.depth
        s = [str(self.id)]
        s.extend(
            ((n.depth - root_depth) * "  " + str(n.id))
            for n in self.descendants
        )
        return "\n".join(s)

    def move_to(self, new_parent):
        new_path = new_parent.path + "." + str(self.id)
        for n in self.descendants:
            n.path = new_path + n.path[len(self.path) :]
        self.path = new_path


if __name__ == "__main__":
    engine = create_engine(
        "postgresql://scott:tiger@localhost/test", echo=True
    )
    Base.metadata.create_all(engine)

    session = Session(engine)

    print("-" * 80)
    print("create a tree")
    session.add_all(
        [
            Node(id=1, path="1"),
            Node(id=2, path="1.2"),
            Node(id=3, path="1.3"),
            Node(id=4, path="1.3.4"),
            Node(id=5, path="1.3.5"),
            Node(id=6, path="1.3.6"),
            Node(id=7, path="1.7"),
            Node(id=8, path="1.7.8"),
            Node(id=9, path="1.7.9"),
            Node(id=10, path="1.7.9.10"),
            Node(id=11, path="1.7.11"),
        ]
    )
    session.flush()
    print(str(session.query(Node).get(1)))

    print("-" * 80)
    print("move 7 under 3")
    session.query(Node).get(7).move_to(session.query(Node).get(3))
    session.flush()
    print(str(session.query(Node).get(1)))

    print("-" * 80)
    print("move 3 under 2")
    session.query(Node).get(3).move_to(session.query(Node).get(2))
    session.flush()
    print(str(session.query(Node).get(1)))

    print("-" * 80)
    print("find the ancestors of 10")
    print([n.id for n in session.query(Node).get(10).ancestors])

    session.close()
    Base.metadata.drop_all(engine)
