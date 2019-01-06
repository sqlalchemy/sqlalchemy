"""Illustrates a method to intercept changes on objects, turning
an UPDATE statement on a single row into an INSERT statement, so that a new
row is inserted with the new data, keeping the old row intact.

"""
from sqlalchemy import Column
from sqlalchemy import create_engine
from sqlalchemy import event
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import attributes
from sqlalchemy.orm import backref
from sqlalchemy.orm import make_transient
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker


class Versioned(object):
    def new_version(self, session):
        # make us transient (removes persistent
        # identity).
        make_transient(self)

        # set 'id' to None.
        # a new PK will be generated on INSERT.
        self.id = None


@event.listens_for(Session, "before_flush")
def before_flush(session, flush_context, instances):
    for instance in session.dirty:
        if not isinstance(instance, Versioned):
            continue
        if not session.is_modified(instance, passive=True):
            continue

        if not attributes.instance_state(instance).has_identity:
            continue

        # make it transient
        instance.new_version(session)
        # re-add
        session.add(instance)


Base = declarative_base()

engine = create_engine("sqlite://", echo=True)

Session = sessionmaker(engine)

# example 1, simple versioning


class Example(Versioned, Base):
    __tablename__ = "example"
    id = Column(Integer, primary_key=True)
    data = Column(String)


Base.metadata.create_all(engine)

session = Session()
e1 = Example(data="e1")
session.add(e1)
session.commit()

e1.data = "e2"
session.commit()

assert session.query(Example.id, Example.data).order_by(Example.id).all() == (
    [(1, "e1"), (2, "e2")]
)

# example 2, versioning with a parent


class Parent(Base):
    __tablename__ = "parent"
    id = Column(Integer, primary_key=True)
    child_id = Column(Integer, ForeignKey("child.id"))
    child = relationship("Child", backref=backref("parent", uselist=False))


class Child(Versioned, Base):
    __tablename__ = "child"

    id = Column(Integer, primary_key=True)
    data = Column(String)

    def new_version(self, session):
        # expire parent's reference to us
        session.expire(self.parent, ["child"])

        # create new version
        Versioned.new_version(self, session)

        # re-add ourselves to the parent
        self.parent.child = self


Base.metadata.create_all(engine)

session = Session()

p1 = Parent(child=Child(data="c1"))
session.add(p1)
session.commit()

p1.child.data = "c2"
session.commit()

assert p1.child_id == 2
assert session.query(Child.id, Child.data).order_by(Child.id).all() == (
    [(1, "c1"), (2, "c2")]
)
