"""Illustrates how to attach events to all instrumented attributes
and listen for change events.

"""

from sqlalchemy import event
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship


def configure_listener(class_, key, inst):
    def append(instance, value, initiator):
        instance.receive_change_event("append", key, value, None)

    def remove(instance, value, initiator):
        instance.receive_change_event("remove", key, value, None)

    def set_(instance, value, oldvalue, initiator):
        instance.receive_change_event("set", key, value, oldvalue)

    event.listen(inst, "append", append)
    event.listen(inst, "remove", remove)
    event.listen(inst, "set", set_)


if __name__ == "__main__":

    class Base(DeclarativeBase):
        def receive_change_event(self, verb, key, value, oldvalue) -> None:
            s = "Value '%s' %s on attribute '%s', " % (value, verb, key)
            if oldvalue:
                s += "which replaced the value '%s', " % oldvalue
            s += "on object %s" % self
            print(s)

    event.listen(Base, "attribute_instrument", configure_listener)

    class MyMappedClass(Base):
        __tablename__ = "mytable"

        id: Mapped[int] = mapped_column(Integer, primary_key=True)
        data: Mapped[str] = mapped_column(String(50))
        related_id: Mapped[int] = mapped_column(
            Integer, ForeignKey("related.id")
        )
        related = relationship("Related", backref="mapped")

        def __str__(self) -> str:
            return "MyMappedClass(data=%r)" % self.data

    class Related(Base):
        __tablename__ = "related"

        id: Mapped[int] = mapped_column(Integer, primary_key=True)
        data: Mapped[str] = mapped_column(String(50))

        def __str__(self) -> str:
            return "Related(data=%r)" % self.data

    # classes are instrumented.  Demonstrate the events !

    m1 = MyMappedClass(data="m1", related=Related(data="r1"))
    m1.data = "m1mod"
    m1.related.mapped.append(MyMappedClass(data="m2"))
    del m1.data
