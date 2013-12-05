"""Illustrate a :func:`.relationship` that joins two columns where those
columns are not of the same type, and a CAST must be used on the SQL
side in order to match them.

When complete, we'd like to see a load of the relationship to look like::

    -- load the primary row, a_id is a string
    SELECT a.id AS a_id_1, a.a_id AS a_a_id
    FROM a
    WHERE a.a_id = '2'

    -- then load the collection using CAST, b.a_id is an integer
    SELECT b.id AS b_id, b.a_id AS b_a_id
    FROM b
    WHERE CAST('2' AS INTEGER) = b.a_id

The relationship is essentially configured as follows::

    class B(Base):
        # ...

        a = relationship(A,
                    primaryjoin=cast(A.a_id, Integer) == foreign(B.a_id),
                    backref="bs")

Where above, we are making use of the :func:`.cast` function in order
to produce CAST, as well as the :func:`.foreign` :term:`annotation` function
in order to note to the ORM that ``B.a_id`` should be treated like the
"foreign key" column.

"""
from sqlalchemy import *
from sqlalchemy.orm import *
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class StringAsInt(TypeDecorator):
    """Coerce string->integer type.

    This is needed only if the relationship() from
    int to string is writable, as SQLAlchemy will copy
    the string parent values into the integer attribute
    on the child during a flush.

    """
    impl = Integer
    def process_bind_param(self, value, dialect):
        if value is not None:
            value = int(value)
        return value

class A(Base):
    """Parent. The referenced column is a string type."""

    __tablename__ = 'a'

    id = Column(Integer, primary_key=True)
    a_id = Column(String)

class B(Base):
    """Child.  The column we reference 'A' with is an integer."""

    __tablename__ = 'b'

    id = Column(Integer, primary_key=True)
    a_id = Column(StringAsInt)
    a = relationship("A",
                # specify primaryjoin.  The string form is optional
                # here, but note that Declarative makes available all
                # of the built-in functions we might need, including
                # cast() and foreign().
                primaryjoin="cast(A.a_id, Integer) == foreign(B.a_id)",
                backref="bs")

# we demonstrate with SQLite, but the important part
# is the CAST rendered in the SQL output.

e = create_engine('sqlite://', echo=True)
Base.metadata.create_all(e)

s = Session(e)

s.add_all([
    A(a_id="1"),
    A(a_id="2", bs=[B(), B()]),
    A(a_id="3", bs=[B()]),
])
s.commit()

b1 = s.query(B).filter_by(a_id="2").first()
print(b1.a)

a1 = s.query(A).filter_by(a_id="2").first()
print(a1.bs)