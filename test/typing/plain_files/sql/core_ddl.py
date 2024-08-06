from sqlalchemy import Boolean
from sqlalchemy import CheckConstraint
from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import FetchedValue
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import Index
from sqlalchemy import Integer
from sqlalchemy import literal_column
from sqlalchemy import MetaData
from sqlalchemy import PrimaryKeyConstraint
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import text
from sqlalchemy import true
from sqlalchemy import UUID


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

t4 = Table(
    "test_table",
    m,
    Column("i", UUID(as_uuid=True), nullable=False, primary_key=True),
    Column("x", UUID(as_uuid=True), index=True),
    Column("y", UUID(as_uuid=False), index=True),
    Index("ix_xy_unique", "x", "y", unique=True),
)


# cols w/ no name or type, used by declarative
c1: Column[int] = Column(ForeignKey(t3.c.x))
# more colum args
Column("name", Integer, index=True)
Column(None, name="name")
Column(Integer, name="name", index=True)
Column("name", ForeignKey("a.id"))
Column(ForeignKey("a.id"), type_=None, index=True)
Column(ForeignKey("a.id"), name="name", type_=Integer())
Column("name", None)
Column("name", index=True)
Column(ForeignKey("a.id"), name="name", index=True)
Column(type_=None, index=True)
Column(None, ForeignKey("a.id"))
Column("name")
Column(name="name", type_=None, index=True)
Column(ForeignKey("a.id"), name="name", type_=None)
Column(Integer)
Column(ForeignKey("a.id"), type_=Integer())
Column("name", Integer, ForeignKey("a.id"), index=True)
Column("name", None, ForeignKey("a.id"), index=True)
Column(ForeignKey("a.id"), index=True)
Column("name", Integer)
Column(Integer, name="name")
Column(Integer, ForeignKey("a.id"), name="name", index=True)
Column(ForeignKey("a.id"), type_=None)
Column(ForeignKey("a.id"), name="name")
Column(name="name", index=True)
Column(type_=None)
Column(None, index=True)
Column(name="name", type_=None)
Column(type_=Integer(), index=True)
Column("name", Integer, ForeignKey("a.id"))
Column(name="name", type_=Integer(), index=True)
Column(Integer, ForeignKey("a.id"), index=True)
Column("name", None, ForeignKey("a.id"))
Column(index=True)
Column("name", type_=None, index=True)
Column("name", ForeignKey("a.id"), type_=Integer(), index=True)
Column(ForeignKey("a.id"))
Column(Integer, ForeignKey("a.id"))
Column(Integer, ForeignKey("a.id"), name="name")
Column("name", ForeignKey("a.id"), index=True)
Column("name", type_=Integer(), index=True)
Column(ForeignKey("a.id"), name="name", type_=Integer(), index=True)
Column(name="name")
Column("name", None, index=True)
Column("name", ForeignKey("a.id"), type_=None, index=True)
Column("name", type_=Integer())
Column(None)
Column(None, ForeignKey("a.id"), index=True)
Column("name", ForeignKey("a.id"), type_=None)
Column(type_=Integer())
Column(None, ForeignKey("a.id"), name="name", index=True)
Column(Integer, index=True)
Column(ForeignKey("a.id"), name="name", type_=None, index=True)
Column(ForeignKey("a.id"), type_=Integer(), index=True)
Column(name="name", type_=Integer())
Column(None, name="name", index=True)
Column()
Column(None, ForeignKey("a.id"), name="name")
Column("name", type_=None)
Column("name", ForeignKey("a.id"), type_=Integer())

# server_default
Column(Boolean, nullable=False, server_default=true())
Column(DateTime, server_default=func.now(), nullable=False)
Column(Boolean, server_default=func.xyzq(), nullable=False)
# what would be *nice* to emit an error would be this, but this
# is really not important, people don't usually put types in functions
# as they are usually part of a bigger context where the type is known
Column(Boolean, server_default=func.xyzq(type_=DateTime), nullable=False)
Column(DateTime, server_default="now()")
Column(DateTime, server_default=text("now()"))
Column(DateTime, server_default=FetchedValue())
Column(Boolean, server_default=literal_column("false", Boolean))
Column("name", server_default=FetchedValue(), nullable=False)
Column(server_default="now()", nullable=False)
Column("name", Integer, server_default=text("now()"), nullable=False)
Column(Integer, server_default=literal_column("42", Integer), nullable=False)

# server_onupdate
Column(server_onupdate=FetchedValue(), nullable=False)
Column(server_onupdate="now()", nullable=False)
Column("name", server_onupdate=FetchedValue(), nullable=False)
Column("name", Integer, server_onupdate=FetchedValue(), nullable=False)
Column("name", Integer, server_onupdate=text("now()"), nullable=False)
Column(Boolean, nullable=False, server_default=true())
Column(Integer, server_onupdate=FetchedValue(), nullable=False)
Column(DateTime, server_onupdate="now()")
Column(DateTime, server_onupdate=text("now()"))
Column(DateTime, server_onupdate=FetchedValue())
Column(Boolean, server_onupdate=literal_column("false", Boolean))
Column(Integer, server_onupdate=literal_column("42", Integer), nullable=False)

# TypeEngine.with_variant should accept both a TypeEngine instance and the Concrete Type
Integer().with_variant(Integer, "mysql")
Integer().with_variant(Integer(), "mysql")
# Also test Variant.with_variant
Integer().with_variant(Integer, "mysql").with_variant(Integer, "mysql")
Integer().with_variant(Integer, "mysql").with_variant(Integer(), "mysql")
