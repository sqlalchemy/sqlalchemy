"""Illustrates the same UPDATE into INSERT technique of ``versioned_rows.py``,
but also emits an UPDATE on the **old** row to affect a change in timestamp.
Also includes a :meth:`.QueryEvents.before_compile` hook to limit queries
to only the most recent version.

"""

import datetime
import time

from sqlalchemy import Column
from sqlalchemy import create_engine
from sqlalchemy import DateTime
from sqlalchemy import event
from sqlalchemy import inspect
from sqlalchemy import Integer
from sqlalchemy import literal
from sqlalchemy import String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import attributes
from sqlalchemy.orm import backref
from sqlalchemy.orm import make_transient
from sqlalchemy.orm import make_transient_to_detached
from sqlalchemy.orm import Query
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session


Base = declarative_base()

# this will be the current time as the test runs
now = None


# in practice this would be a real "now" function
def current_time():
    return now


class VersionedStartEnd(object):
    def __init__(self, **kw):
        # reduce some verbosity when we make a new object
        kw.setdefault("start", current_time() - datetime.timedelta(days=3))
        kw.setdefault("end", current_time() + datetime.timedelta(days=3))
        super(VersionedStartEnd, self).__init__(**kw)

    def new_version(self, session):

        # our current identity key, which will be used on the "old"
        # version of us to emit an UPDATE. this is just for assertion purposes
        old_identity_key = inspect(self).key

        # make sure self.start / self.end are not expired
        self.id, self.start, self.end

        # turn us into an INSERT
        make_transient(self)

        # make the "old" version of us, which we will turn into an
        # UPDATE
        old_copy_of_us = self.__class__(
            id=self.id, start=self.start, end=self.end
        )

        # turn old_copy_of_us into an UPDATE
        make_transient_to_detached(old_copy_of_us)

        # the "old" object has our old identity key (that we no longer have)
        assert inspect(old_copy_of_us).key == old_identity_key

        # now put it back in the session
        session.add(old_copy_of_us)

        # now update the 'end' - SQLAlchemy sees this as a PK switch
        old_copy_of_us.end = current_time()

        # fun fact!  the new_version() routine is *not* called for
        # old_copy_of_us!  because we are already in the before_flush() hook!
        # this surprised even me.   I was thinking we had to guard against
        # it.  Still might be a good idea to do so.

        self.start = current_time()
        self.end = current_time() + datetime.timedelta(days=2)


@event.listens_for(Session, "before_flush")
def before_flush(session, flush_context, instances):
    for instance in session.dirty:
        if not isinstance(instance, VersionedStartEnd):
            continue
        if not session.is_modified(instance, passive=True):
            continue

        if not attributes.instance_state(instance).has_identity:
            continue

        # make it transient
        instance.new_version(session)
        # re-add
        session.add(instance)


@event.listens_for(Query, "before_compile", retval=True)
def before_compile(query):
    """ensure all queries for VersionedStartEnd include criteria """

    for ent in query.column_descriptions:
        entity = ent["entity"]
        if entity is None:
            continue
        insp = inspect(ent["entity"])
        mapper = getattr(insp, "mapper", None)
        if mapper and issubclass(mapper.class_, VersionedStartEnd):
            query = query.enable_assertions(False).filter(
                # using a literal "now" because SQLite's "between"
                # seems to be inclusive. In practice, this would be
                # ``func.now()`` and we'd be using PostgreSQL
                literal(
                    current_time() + datetime.timedelta(seconds=1)
                ).between(ent["entity"].start, ent["entity"].end)
            )

    return query


class Parent(VersionedStartEnd, Base):
    __tablename__ = "parent"
    id = Column(Integer, primary_key=True)
    start = Column(DateTime, primary_key=True)
    end = Column(DateTime, primary_key=True)
    data = Column(String)

    child_n = Column(Integer)

    child = relationship(
        "Child",
        primaryjoin=("Child.id == foreign(Parent.child_n)"),
        # note the primaryjoin can also be:
        #
        #  "and_(Child.id == foreign(Parent.child_n), "
        #  "func.now().between(Child.start, Child.end))"
        #
        # however the before_compile() above will take care of this for us in
        # all cases except for joinedload.  You *can* use the above primaryjoin
        # as well, it just means the criteria will be present twice for most
        # parent->child load operations
        #
        uselist=False,
        backref=backref("parent", uselist=False),
    )


class Child(VersionedStartEnd, Base):
    __tablename__ = "child"

    id = Column(Integer, primary_key=True)
    start = Column(DateTime, primary_key=True)
    end = Column(DateTime, primary_key=True)
    data = Column(String)

    def new_version(self, session):

        # expire parent's reference to us
        session.expire(self.parent, ["child"])

        # create new version
        VersionedStartEnd.new_version(self, session)

        # re-add ourselves to the parent
        self.parent.child = self


times = []


def time_passes(s):
    """keep track of timestamps in terms of the database and allow time to
    pass between steps."""

    # close the transaction, if any, since PG time doesn't increment in the
    # transaction
    s.commit()

    # get "now" in terms of the DB so we can keep the ranges low and
    # still have our assertions pass
    if times:
        time.sleep(1)

    times.append(datetime.datetime.now())

    if len(times) > 1:
        assert times[-1] > times[-2]
    return times[-1]


e = create_engine("sqlite://", echo="debug")
Base.metadata.create_all(e)

s = Session(e)

now = time_passes(s)

c1 = Child(id=1, data="child 1")
p1 = Parent(id=1, data="c1", child=c1)

s.add(p1)
s.commit()

# assert raw DB data
assert s.query(Parent.__table__).all() == [
    (
        1,
        times[0] - datetime.timedelta(days=3),
        times[0] + datetime.timedelta(days=3),
        "c1",
        1,
    )
]
assert s.query(Child.__table__).all() == [
    (
        1,
        times[0] - datetime.timedelta(days=3),
        times[0] + datetime.timedelta(days=3),
        "child 1",
    )
]

now = time_passes(s)

p1_check = s.query(Parent).first()
assert p1_check is p1
assert p1_check.child is c1

p1.child.data = "elvis presley"

s.commit()

p2_check = s.query(Parent).first()
assert p2_check is p1_check
c2_check = p2_check.child

# same object
assert p2_check.child is c1

# new data
assert c1.data == "elvis presley"

# new end time
assert c1.end == now + datetime.timedelta(days=2)

# assert raw DB data
assert s.query(Parent.__table__).all() == [
    (
        1,
        times[0] - datetime.timedelta(days=3),
        times[0] + datetime.timedelta(days=3),
        "c1",
        1,
    )
]
assert s.query(Child.__table__).order_by(Child.end).all() == [
    (1, times[0] - datetime.timedelta(days=3), times[1], "child 1"),
    (1, times[1], times[1] + datetime.timedelta(days=2), "elvis presley"),
]

now = time_passes(s)

p1.data = "c2 elvis presley"

s.commit()

# assert raw DB data.  now there are two parent rows.
assert s.query(Parent.__table__).order_by(Parent.end).all() == [
    (1, times[0] - datetime.timedelta(days=3), times[2], "c1", 1),
    (
        1,
        times[2],
        times[2] + datetime.timedelta(days=2),
        "c2 elvis presley",
        1,
    ),
]
assert s.query(Child.__table__).order_by(Child.end).all() == [
    (1, times[0] - datetime.timedelta(days=3), times[1], "child 1"),
    (1, times[1], times[1] + datetime.timedelta(days=2), "elvis presley"),
]

# add some more rows to test that these aren't coming back for
# queries
s.add(Parent(id=2, data="unrelated", child=Child(id=2, data="unrelated")))
s.commit()


# Query only knows about one parent for id=1
p3_check = s.query(Parent).filter_by(id=1).one()

assert p3_check is p1
assert p3_check.child is c1

# and one child.
c3_check = s.query(Child).filter(Child.parent == p3_check).one()
assert c3_check is c1

# one child one parent....
c3_check = (
    s.query(Child).join(Parent.child).filter(Parent.id == p3_check.id).one()
)
