"""Test MySQL FOR UPDATE behavior.

See #4246

"""
import contextlib

from sqlalchemy import Column
from sqlalchemy import exc
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import testing
from sqlalchemy import update
from sqlalchemy.orm import joinedload
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session
from sqlalchemy.testing import fixtures


class MySQLForUpdateLockingTest(fixtures.DeclarativeMappedTest):
    __backend__ = True
    __only_on__ = "mysql"
    __requires__ = ("mysql_for_update",)

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class A(Base):
            __tablename__ = "a"
            id = Column(Integer, primary_key=True)
            x = Column(Integer)
            y = Column(Integer)
            bs = relationship("B")
            __table_args__ = {"mysql_engine": "InnoDB"}

        class B(Base):
            __tablename__ = "b"
            id = Column(Integer, primary_key=True)
            a_id = Column(ForeignKey("a.id"))
            x = Column(Integer)
            y = Column(Integer)
            __table_args__ = {"mysql_engine": "InnoDB"}

    @classmethod
    def insert_data(cls, connection):
        A = cls.classes.A
        B = cls.classes.B

        # all the x/y are < 10
        s = Session(connection)
        s.add_all(
            [
                A(x=5, y=5, bs=[B(x=4, y=4), B(x=2, y=8), B(x=7, y=1)]),
                A(x=7, y=5, bs=[B(x=4, y=4), B(x=5, y=8)]),
            ]
        )
        s.commit()

    @contextlib.contextmanager
    def run_test(self):
        connection = testing.db.connect()
        connection.execute("set innodb_lock_wait_timeout=1")
        main_trans = connection.begin()
        try:
            yield Session(bind=connection)
        finally:
            main_trans.rollback()
            connection.close()

    def _assert_a_is_locked(self, should_be_locked):
        A = self.classes.A
        with testing.db.begin() as alt_trans:
            alt_trans.execute("set innodb_lock_wait_timeout=1")
            # set x/y > 10
            try:
                alt_trans.execute(update(A).values(x=15, y=19))
            except (exc.InternalError, exc.OperationalError) as err:
                assert "Lock wait timeout exceeded" in str(err)
                assert should_be_locked
            else:
                assert not should_be_locked

    def _assert_b_is_locked(self, should_be_locked):
        B = self.classes.B
        with testing.db.begin() as alt_trans:
            alt_trans.execute("set innodb_lock_wait_timeout=1")
            # set x/y > 10
            try:
                alt_trans.execute(update(B).values(x=15, y=19))
            except (exc.InternalError, exc.OperationalError) as err:
                assert "Lock wait timeout exceeded" in str(err)
                assert should_be_locked
            else:
                assert not should_be_locked

    def test_basic_lock(self):
        A = self.classes.A
        with self.run_test() as s:
            s.query(A).with_for_update().all()
            # test our fixture
            self._assert_a_is_locked(True)

    def test_basic_not_lock(self):
        A = self.classes.A
        with self.run_test() as s:
            s.query(A).all()
            # test our fixture
            self._assert_a_is_locked(False)

    def test_joined_lock_subquery(self):
        A = self.classes.A
        with self.run_test() as s:
            s.query(A).options(joinedload(A.bs)).with_for_update().first()

            # test for issue #4246, should be locked
            self._assert_a_is_locked(True)
            self._assert_b_is_locked(True)

    def test_joined_lock_subquery_inner_for_update(self):
        A = self.classes.A
        B = self.classes.B
        with self.run_test() as s:
            q = s.query(A).with_for_update().subquery()
            s.query(q).join(B).all()

            # FOR UPDATE is inside the subquery, should be locked
            self._assert_a_is_locked(True)

            # FOR UPDATE is inside the subquery, B is not locked
            self._assert_b_is_locked(False)

    def test_joined_lock_subquery_inner_for_update_outer(self):
        A = self.classes.A
        B = self.classes.B
        with self.run_test() as s:
            q = s.query(A).with_for_update().subquery()
            s.query(q).join(B).with_for_update().all()

            # FOR UPDATE is inside the subquery, should be locked
            self._assert_a_is_locked(True)

            # FOR UPDATE is also outside the subquery, B is locked
            self._assert_b_is_locked(True)

    def test_joined_lock_subquery_order_for_update_outer(self):
        A = self.classes.A
        B = self.classes.B
        with self.run_test() as s:
            q = s.query(A).order_by(A.id).subquery()
            s.query(q).join(B).with_for_update().all()
            # FOR UPDATE is inside the subquery, should not be locked
            self._assert_a_is_locked(False)
            self._assert_b_is_locked(True)

    def test_joined_lock_no_subquery(self):
        A = self.classes.A
        with self.run_test() as s:
            s.query(A).options(joinedload(A.bs)).with_for_update().all()
            # no subquery, should be locked
            self._assert_a_is_locked(True)
            self._assert_b_is_locked(True)
