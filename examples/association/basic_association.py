"""Illustrate a many-to-many relationship between an
"Order" and a collection of "Item" objects, associating a purchase price
with each via an association object called "OrderItem"

The association object pattern is a form of many-to-many which
associates additional data with each association between parent/child.

The example illustrates an "order", referencing a collection
of "items", with a particular price paid associated with each "item".

"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy import ForeignKey
from sqlalchemy import select
from sqlalchemy import String
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

    def __init__(self, item: Item, price: float | None = None) -> None:
        self.item = item
        self.price = price or item.price

    item: Mapped[Item] = relationship(lazy="joined")


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

        # add three OrderItem associations to the Order and save
        order.order_items.append(OrderItem(mug))
        order.order_items.append(OrderItem(crowbar, 10.99))
        order.order_items.append(OrderItem(hat))
        session.add(order)
        session.commit()

        # query the order, print items
        order = session.scalars(
            select(Order).filter_by(customer_name="john smith")
        ).one()
        print(
            [
                (order_item.item.description, order_item.price)
                for order_item in order.order_items
            ]
        )

        # print customers who bought 'MySQL Crowbar' on sale
        q = (
            select(Order)
            .join(OrderItem)
            .join(Item)
            .where(
                Item.description == "MySQL Crowbar",
                Item.price > OrderItem.price,
            )
        )

        print([order.customer_name for order in session.scalars(q)])
