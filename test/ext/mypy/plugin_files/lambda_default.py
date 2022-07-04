import uuid

from sqlalchemy import Column
from sqlalchemy import String
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class MyClass(Base):
    id = Column(String, default=lambda: uuid.uuid4(), primary_key=True)
