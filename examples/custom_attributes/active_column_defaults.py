"""Illustrates use of the :meth:`.AttributeEvents.init_scalar`
event, in conjunction with Core column defaults to provide
ORM objects that automatically produce the default value
when an un-set attribute is accessed.

"""

from sqlalchemy import event


def configure_listener(mapper, class_):
    """Establish attribute setters for every default-holding column on the
    given mapper."""

    # iterate through ColumnProperty objects
    for col_attr in mapper.column_attrs:

        # look at the Column mapped by the ColumnProperty
        # (we look at the first column in the less common case
        # of a property mapped to multiple columns at once)
        column = col_attr.columns[0]

        # if the Column has a "default", set up a listener
        if column.default is not None:
            default_listener(col_attr, column.default)


def default_listener(col_attr, default):
    """Establish a default-setting listener.

    Given a class_, attrname, and a :class:`.DefaultGenerator` instance.
    The default generator should be a :class:`.ColumnDefault` object with a
    plain Python value or callable default; otherwise, the appropriate behavior
    for SQL functions and defaults should be determined here by the
    user integrating this feature.

    """
    @event.listens_for(col_attr, "init_scalar", retval=True, propagate=True)
    def init_scalar(target, value, dict_):

        if default.is_callable:
            # the callable of ColumnDefault always accepts a context
            # argument; we can pass it as None here.
            value = default.arg(None)
        elif default.is_scalar:
            value = default.arg
        else:
            # default is a Sequence, a SQL expression, server
            # side default generator, or other non-Python-evaluable
            # object.  The feature here can't easily support this.   This
            # can be made to return None, rather than raising,
            # or can procure a connection from an Engine
            # or Session and actually run the SQL, if desired.
            raise NotImplementedError(
                "Can't invoke pre-default for a SQL-level column default")

        # set the value in the given dict_; this won't emit any further
        # attribute set events or create attribute "history", but the value
        # will be used in the INSERT statement
        dict_[col_attr.key] = value

        # return the value as well
        return value


if __name__ == '__main__':

    from sqlalchemy import Column, Integer, DateTime, create_engine
    from sqlalchemy.orm import Session
    from sqlalchemy.ext.declarative import declarative_base
    import datetime

    Base = declarative_base()

    event.listen(Base, 'mapper_configured', configure_listener, propagate=True)

    class Widget(Base):
        __tablename__ = 'widget'

        id = Column(Integer, primary_key=True)

        radius = Column(Integer, default=30)
        timestamp = Column(DateTime, default=datetime.datetime.now)

    e = create_engine("sqlite://", echo=True)
    Base.metadata.create_all(e)

    w1 = Widget()

    # not persisted at all, default values are present the moment
    # we access them
    assert w1.radius == 30
    current_time = w1.timestamp
    assert (
        current_time > datetime.datetime.now() - datetime.timedelta(seconds=5)
    )

    # persist
    sess = Session(e)
    sess.add(w1)
    sess.commit()

    # data is persisted.  The timestamp is also the one we generated above;
    # e.g. the default wasn't re-invoked later.
    assert (
        sess.query(Widget.radius, Widget.timestamp).first() ==
        (30, current_time)
    )
