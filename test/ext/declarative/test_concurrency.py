import random
import threading
import time

from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import clear_mappers
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session
from sqlalchemy.testing import fixtures


class ConcurrentUseDeclMappingTest(fixtures.TestBase):
    @classmethod
    def make_a(cls, Base):
        class A(Base):
            __tablename__ = "a"

            id = Column(Integer, primary_key=True)
            data = Column(String)
            bs = relationship("B")

        # need a strong ref so that the class is not gc'ed
        cls.A = A

    @classmethod
    def query_a(cls, Base, result):
        s = Session()
        time.sleep(random.random() / 100)
        A = cls.A
        try:
            s.query(A).join(A.bs)
        except Exception as err:
            result[0] = err
            print(err)
        else:
            result[0] = True
            print("worked")

    @classmethod
    def make_b(cls, Base):
        class B(Base):
            __tablename__ = "b"
            id = Column(Integer, primary_key=True)

            @declared_attr
            def data(cls):
                time.sleep(0.001)
                return Column(String)

            a_id = Column(ForeignKey("a.id"))

        cls.B = B

    def test_concurrent_create(self):
        for i in range(50):
            Base = declarative_base()
            clear_mappers()

            self.make_a(Base)
            result = [False]
            threads = [
                threading.Thread(target=self.make_b, args=(Base,)),
                threading.Thread(target=self.query_a, args=(Base, result)),
            ]

            for t in threads:
                t.start()

            for t in threads:
                t.join()

            if isinstance(result[0], Exception):
                raise result[0]
