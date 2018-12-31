"""Illustrate a "three way join" - where a primary table joins to a remote
table via an association table, but then the primary table also needs
to refer to some columns in the remote table directly.

E.g.::

    first.first_id      -> second.first_id
                           second.other_id --> partitioned.other_id
    first.partition_key ---------------------> partitioned.partition_key

For a relationship like this, "second" is a lot like a "secondary" table,
but the mechanics aren't present within the "secondary" feature to allow
for the join directly between first and partitioned.  Instead, we
will derive a selectable from partitioned and second combined together, then
link first to that derived selectable.

If we define the derived selectable as::

    second JOIN partitioned ON second.other_id = partitioned.other_id

A JOIN from first to this derived selectable is then::

    first JOIN (second JOIN partitioned
                ON second.other_id = partitioned.other_id)
          ON first.first_id = second.first_id AND
             first.partition_key = partitioned.partition_key

We will use the "non primary mapper" feature in order to produce this.
A non primary mapper is essentially an "extra" :func:`.mapper` that we can
use to associate a particular class with some selectable that is
not its usual mapped table.   It is used only when called upon within
a Query (or a :func:`.relationship`).


"""
from sqlalchemy import and_
from sqlalchemy import Column
from sqlalchemy import create_engine
from sqlalchemy import Integer
from sqlalchemy import join
from sqlalchemy import String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import foreign
from sqlalchemy.orm import mapper
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session


Base = declarative_base()


class First(Base):
    __tablename__ = "first"

    first_id = Column(Integer, primary_key=True)
    partition_key = Column(String)

    def __repr__(self):
        return "First(%s, %s)" % (self.first_id, self.partition_key)


class Second(Base):
    __tablename__ = "second"

    first_id = Column(Integer, primary_key=True)
    other_id = Column(Integer, primary_key=True)


class Partitioned(Base):
    __tablename__ = "partitioned"

    other_id = Column(Integer, primary_key=True)
    partition_key = Column(String, primary_key=True)

    def __repr__(self):
        return "Partitioned(%s, %s)" % (self.other_id, self.partition_key)


j = join(Partitioned, Second, Partitioned.other_id == Second.other_id)

partitioned_second = mapper(
    Partitioned,
    j,
    non_primary=True,
    properties={
        # note we need to disambiguate columns here - the join()
        # will provide them as j.c.<tablename>_<colname> for access,
        # but they retain their real names in the mapping
        "other_id": [j.c.partitioned_other_id, j.c.second_other_id]
    },
)

First.partitioned = relationship(
    partitioned_second,
    primaryjoin=and_(
        First.partition_key == partitioned_second.c.partition_key,
        First.first_id == foreign(partitioned_second.c.first_id),
    ),
    innerjoin=True,
)

# when using any database other than SQLite, we will get a nested
# join, e.g. "first JOIN (partitioned JOIN second ON ..) ON ..".
# On SQLite, SQLAlchemy needs to render a full subquery.
e = create_engine("sqlite://", echo=True)

Base.metadata.create_all(e)
s = Session(e)
s.add_all(
    [
        First(first_id=1, partition_key="p1"),
        First(first_id=2, partition_key="p1"),
        First(first_id=3, partition_key="p2"),
        Second(first_id=1, other_id=1),
        Second(first_id=2, other_id=1),
        Second(first_id=3, other_id=2),
        Partitioned(partition_key="p1", other_id=1),
        Partitioned(partition_key="p1", other_id=2),
        Partitioned(partition_key="p2", other_id=2),
    ]
)
s.commit()

for row in s.query(First, Partitioned).join(First.partitioned):
    print(row)

for f in s.query(First):
    for p in f.partitioned:
        print(f.partition_key, p.partition_key)
