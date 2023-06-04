from __future__ import annotations

from typing import Dict
from typing import Optional

from sqlalchemy import create_engine
from sqlalchemy import ForeignKey
from sqlalchemy import select
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import MappedAsDataclass
from sqlalchemy.orm import relationship
from sqlalchemy.orm import selectinload
from sqlalchemy.orm import Session
from sqlalchemy.orm.collections import attribute_keyed_dict


class Base(DeclarativeBase):
    pass


class TreeNode(MappedAsDataclass, Base):
    __tablename__ = "tree"

    id: Mapped[int] = mapped_column(primary_key=True, init=False)
    parent_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("tree.id"), init=False
    )
    name: Mapped[str]

    children: Mapped[Dict[str, TreeNode]] = relationship(
        cascade="all, delete-orphan",
        back_populates="parent",
        collection_class=attribute_keyed_dict("name"),
        init=False,
        repr=False,
    )

    parent: Mapped[Optional[TreeNode]] = relationship(
        back_populates="children", remote_side=id, default=None
    )

    def dump(self, _indent: int = 0) -> str:
        return (
            "   " * _indent
            + repr(self)
            + "\n"
            + "".join([c.dump(_indent + 1) for c in self.children.values()])
        )


if __name__ == "__main__":
    engine = create_engine("sqlite://", echo=True)

    print("Creating Tree Table:")

    Base.metadata.create_all(engine)

    with Session(engine) as session:
        node = TreeNode("rootnode")
        TreeNode("node1", parent=node)
        TreeNode("node3", parent=node)

        node2 = TreeNode("node2")
        TreeNode("subnode1", parent=node2)
        node.children["node2"] = node2
        TreeNode("subnode2", parent=node.children["node2"])

        print(f"Created new tree structure:\n{node.dump()}")

        print("flush + commit:")

        session.add(node)
        session.commit()

        print(f"Tree after save:\n{node.dump()}")

        session.add_all(
            [
                TreeNode("node4", parent=node),
                TreeNode("subnode3", parent=node.children["node4"]),
                TreeNode("subnode4", parent=node.children["node4"]),
                TreeNode(
                    "subsubnode1",
                    parent=node.children["node4"].children["subnode3"],
                ),
            ]
        )

        # remove node1 from the parent, which will trigger a delete
        # via the delete-orphan cascade.
        del node.children["node1"]

        print("Removed node1.  flush + commit:")
        session.commit()

        print("Tree after save, will unexpire all nodes:\n")
        print(f"{node.dump()}")

    with Session(engine) as session:
        print(
            "Perform a full select of the root node, eagerly loading "
            "up to a depth of four"
        )
        node = session.scalars(
            select(TreeNode)
            .options(selectinload(TreeNode.children, recursion_depth=4))
            .filter(TreeNode.name == "rootnode")
        ).one()

        print(f"Full Tree:\n{node.dump()}")

        print("Marking root node as deleted, flush + commit:")

        session.delete(node)
        session.commit()
