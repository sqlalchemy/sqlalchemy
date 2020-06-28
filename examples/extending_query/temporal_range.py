"""Illustrates a custom per-query criteria that will be applied
to selected entities.


"""

import datetime

from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import orm


class HasTemporal(object):
    """Mixin that identifies a class as having a timestamp column"""

    timestamp = Column(
        DateTime, default=datetime.datetime.utcnow, nullable=False
    )


def temporal_range(range_lower, range_upper):
    return orm.with_loader_criteria(
        HasTemporal,
        lambda cls: cls.timestamp.between(range_lower, range_upper),
        include_aliases=True,
    )


if __name__ == "__main__":

    from sqlalchemy import Integer, Column, ForeignKey
    from sqlalchemy import select
    from sqlalchemy import create_engine
    from sqlalchemy.orm import relationship, sessionmaker, selectinload
    from sqlalchemy.ext.declarative import declarative_base

    Base = declarative_base()

    class Parent(HasTemporal, Base):
        __tablename__ = "parent"
        id = Column(Integer, primary_key=True)
        children = relationship("Child")

    class Child(HasTemporal, Base):
        __tablename__ = "child"
        id = Column(Integer, primary_key=True)
        parent_id = Column(Integer, ForeignKey("parent.id"), nullable=False)

    engine = create_engine("sqlite://", echo=True)
    Base.metadata.create_all(engine)

    Session = sessionmaker(bind=engine, future=True)

    sess = Session()

    c1, c2, c3, c4, c5 = [
        Child(timestamp=datetime.datetime(2009, 10, 15, 12, 00, 00)),
        Child(timestamp=datetime.datetime(2009, 10, 17, 12, 00, 00)),
        Child(timestamp=datetime.datetime(2009, 10, 20, 12, 00, 00)),
        Child(timestamp=datetime.datetime(2009, 10, 12, 12, 00, 00)),
        Child(timestamp=datetime.datetime(2009, 10, 17, 12, 00, 00)),
    ]

    p1 = Parent(
        timestamp=datetime.datetime(2009, 10, 15, 12, 00, 00),
        children=[c1, c2, c3],
    )
    p2 = Parent(
        timestamp=datetime.datetime(2009, 10, 17, 12, 00, 00),
        children=[c4, c5],
    )

    sess.add_all([p1, p2])
    sess.commit()

    # use populate_existing() to ensure the range option takes
    # place for elements already in the identity map

    parents = (
        sess.query(Parent)
        .populate_existing()
        .options(
            temporal_range(
                datetime.datetime(2009, 10, 16, 12, 00, 00),
                datetime.datetime(2009, 10, 18, 12, 00, 00),
            )
        )
        .all()
    )

    assert parents[0] == p2
    assert parents[0].children == [c5]

    sess.expire_all()

    # try it with eager load
    parents = (
        sess.query(Parent)
        .options(
            temporal_range(
                datetime.datetime(2009, 10, 16, 12, 00, 00),
                datetime.datetime(2009, 10, 18, 12, 00, 00),
            )
        )
        .options(selectinload(Parent.children))
        .all()
    )

    assert parents[0] == p2
    assert parents[0].children == [c5]

    sess.expire_all()

    # illustrate a 2.0 style query
    print("------------------")
    parents = (
        sess.execute(
            select(Parent)
            .execution_options(populate_existing=True)
            .options(
                temporal_range(
                    datetime.datetime(2009, 10, 15, 11, 00, 00),
                    datetime.datetime(2009, 10, 18, 12, 00, 00),
                )
            )
            .join(Parent.children)
            .filter(Child.id == 2)
        )
        .scalars()
        .all()
    )

    assert parents[0] == p1
    print("-------------------")
    assert parents[0].children == [c1, c2]
