from sqlalchemy import CheckConstraint
from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import ForeignKey
from sqlalchemy import Index
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import PrimaryKeyConstraint
from sqlalchemy import String
from sqlalchemy import Table


m = MetaData()


t1 = Table(
    "t1",
    m,
    Column("id", Integer, primary_key=True),
    Column("data", String),
    Column("data2", String(50)),
    Column("timestamp", DateTime()),
    Index(None, "data2"),
)

t2 = Table(
    "t2",
    m,
    Column("t1id", ForeignKey("t1.id")),
    Column("q", Integer, CheckConstraint("q > 5")),
)

t3 = Table(
    "t3",
    m,
    Column("x", Integer),
    Column("y", Integer),
    Column("t1id", ForeignKey(t1.c.id)),
    PrimaryKeyConstraint("x", "y"),
)

# cols w/ no name or type, used by declarative
c1: Column[int] = Column(ForeignKey(t3.c.x))
