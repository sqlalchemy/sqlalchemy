"""Same example as basic_association, adding in
usage of :mod:`sqlalchemy.ext.associationproxy` to make explicit references
to ``OrderItem`` optional.


"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy import ForeignKey
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.associationproxy import AssociationProxy
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session


class Base(DeclarativeBase):
    pass


class Order(Base):
    __tablename__ = "order"

    order_id: Mapped[int] = mapped_column(primary_key=True)
    customer_name: Mapped[str] = mapped_column(String(30))
    order_date: Mapped[datetime] = mapped_column(default=datetime.now())
    order_items: Mapped[list[OrderItem]] = relationship(
        cascade="all, delete-orphan", backref="order"
    )
    items: AssociationProxy[list[Item]] = association_proxy(
        "order_items", "item"
    )

    def __init__(self, customer_name: str) -> None:
        self.customer_name = customer_name


class Item(Base):
    __tablename__ = "item"
    item_id: Mapped[int] = mapped_column(primary_key=True)
    description: Mapped[str] = mapped_column(String(30))
    price: Mapped[float]

    def __init__(self, description: str, price: float) -> None:
        self.description = description
        self.price = price

    def __repr__(self) -> str:
        return "Item({!r}, {!r})".format(self.description, self.price)


class OrderItem(Base):
    __tablename__ = "orderitem"
    order_id: Mapped[int] = mapped_column(
        ForeignKey("order.order_id"), primary_key=True
    )
    item_id: Mapped[int] = mapped_column(
        ForeignKey("item.item_id"), primary_key=True
    )
    price: Mapped[float]

    item: Mapped[Item] = relationship(lazy="joined")

    def __init__(self, item: Item, price: float | None = None):
        self.item = item
        self.price = price or item.price


if __name__ == "__main__":
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)

    with Session(engine) as session:

        # create catalog
        tshirt, mug, hat, crowbar = (
            Item("SA T-Shirt", 10.99),
            Item("SA Mug", 6.50),
            Item("SA Hat", 8.99),
            Item("MySQL Crowbar", 16.99),
        )
        session.add_all([tshirt, mug, hat, crowbar])
        session.commit()

        # create an order
        order = Order("john smith")

        # add items via the association proxy.
        # the OrderItem is created automatically.
        order.items.append(mug)
        order.items.append(hat)

        # add an OrderItem explicitly.
        order.order_items.append(OrderItem(crowbar, 10.99))

        session.add(order)
        session.commit()

        # query the order, print items
        order = session.scalars(
            select(Order).filter_by(customer_name="john smith")
        ).one()

        # print items based on the OrderItem collection directly
        print(
            [
                (assoc.item.description, assoc.price, assoc.item.price)
                for assoc in order.order_items
            ]
        )

        # print items based on the "proxied" items collection
        print([(item.description, item.price) for item in order.items])

        # print customers who bought 'MySQL Crowbar' on sale
        orders_stmt = (
            select(Order)
            .join(OrderItem)
            .join(Item)
            .filter(Item.description == "MySQL Crowbar")
            .filter(Item.price > OrderItem.price)
        )
        print([o.customer_name for o in session.scalars(orders_stmt)])
