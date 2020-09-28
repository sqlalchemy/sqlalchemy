"""Illustrates a method to intercept changes on objects, turning
an UPDATE statement on a single row into an INSERT statement, so that a new
row is inserted with the new data, keeping the old row intact.

This example adds a numerical version_id to the Versioned class as well
as the ability to see which row is the most "current" vesion.

"""
from sqlalchemy import Boolean
from sqlalchemy import Column
from sqlalchemy import create_engine
from sqlalchemy import event
from sqlalchemy import ForeignKeyConstraint
from sqlalchemy import func
from sqlalchemy import Integer
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import attributes
from sqlalchemy.orm import backref
from sqlalchemy.orm import column_property
from sqlalchemy.orm import make_transient
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker


class Versioned(object):
    # we have a composite primary key consisting of "id"
    # and "version_id"
    id = Column(Integer, primary_key=True)
    version_id = Column(Integer, primary_key=True, default=1)

    # optional - add a persisted is_current_version column
    is_current_version = Column(Boolean, default=True)

    # optional - add a calculated is_current_version column
    @classmethod
    def __declare_last__(cls):
        alias = cls.__table__.alias()
        cls.calc_is_current_version = column_property(
            select(func.max(alias.c.version_id) == cls.version_id).where(
                alias.c.id == cls.id
            )
        )

    def new_version(self, session):
        # optional - set previous version to have is_current_version=False
        old_id = self.id
        session.query(self.__class__).filter_by(id=old_id).update(
            values=dict(is_current_version=False), synchronize_session=False
        )

        # make us transient (removes persistent
        # identity).
        make_transient(self)

        # increment version_id, which means we have a new PK.
        self.version_id += 1


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
    data = Column(String)


Base.metadata.create_all(engine)

session = Session()
e1 = Example(id=1, data="e1")
session.add(e1)
session.commit()

e1.data = "e2"
session.commit()

assert (
    session.query(
        Example.id,
        Example.version_id,
        Example.is_current_version,
        Example.calc_is_current_version,
        Example.data,
    )
    .order_by(Example.id, Example.version_id)
    .all()
    == ([(1, 1, False, False, "e1"), (1, 2, True, True, "e2")])
)

# example 2, versioning with a parent


class Parent(Base):
    __tablename__ = "parent"
    id = Column(Integer, primary_key=True)
    child_id = Column(Integer)
    child_version_id = Column(Integer)
    child = relationship("Child", backref=backref("parent", uselist=False))

    __table_args__ = (
        ForeignKeyConstraint(
            ["child_id", "child_version_id"], ["child.id", "child.version_id"]
        ),
    )


class Child(Versioned, Base):
    __tablename__ = "child"

    data = Column(String)

    def new_version(self, session):
        # expire parent's reference to us
        session.expire(self.parent, ["child"])

        # create new version
        Versioned.new_version(self, session)

        # re-add ourselves to the parent.  this causes the
        # parent foreign key to be updated also
        self.parent.child = self


Base.metadata.create_all(engine)

session = Session()

p1 = Parent(child=Child(id=1, data="c1"))
session.add(p1)
session.commit()

p1.child.data = "c2"
session.commit()

assert p1.child_id == 1
assert p1.child.version_id == 2

assert (
    session.query(
        Child.id,
        Child.version_id,
        Child.is_current_version,
        Child.calc_is_current_version,
        Child.data,
    )
    .order_by(Child.id, Child.version_id)
    .all()
    == ([(1, 1, False, False, "c1"), (1, 2, True, True, "c2")])
)
