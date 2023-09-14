from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import registry
from sqlalchemy.orm import Session
from sqlalchemy.sql.functions import now

mapper_registry: registry = registry()
e = create_engine("sqlite:///database.db", echo=True)


@mapper_registry.mapped
class A:
    __tablename__ = "a"
    id: Mapped[int] = mapped_column(primary_key=True)
    date_time: Mapped[datetime]


mapper_registry.metadata.create_all(e)

with Session(e) as s:
    a = A()
    a.date_time = now()
    s.add(a)
    s.commit()
