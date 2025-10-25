from sqlalchemy import inspect
from sqlalchemy import text
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker


class Base(DeclarativeBase):
    pass


class X(Base):
    __tablename__ = "x"
    id: Mapped[int] = mapped_column(primary_key=True)


scoped_session.object_session(object())
scoped_session.identity_key()
ss = scoped_session(sessionmaker())
value: bool = "foo" in ss
list(ss)
ss.add(object())
ss.add_all([])
ss.begin()
ss.begin_nested()
ss.close()
ss.commit()
ss.connection()
ss.delete(object())
ss.execute(text("select 1"))
ss.expire(object())
ss.expire_all()
ss.expunge(object())
ss.expunge_all()
ss.flush()
ss.get(object, 1)
b = ss.get_bind()
ss.is_modified(object())
ss.bulk_save_objects([])
ss.bulk_insert_mappings(inspect(X), [])
ss.bulk_update_mappings(inspect(X), [])
ss.merge(object())
q = (ss.query(object),)
ss.refresh(object())
ss.rollback()
ss.scalar(text("select 1"))
ss.bind
ss.dirty
ss.deleted
ss.new
ss.identity_map
ss.is_active
ss.autoflush
ss.no_autoflush
ss.info
