from sqlalchemy import delete
from sqlalchemy import exc
from sqlalchemy import insert
from sqlalchemy import Integer
from sqlalchemy import literal
from sqlalchemy import literal_column
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy import text
from sqlalchemy import TypeDecorator
from sqlalchemy import update
from sqlalchemy.orm import loading
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_true
from sqlalchemy.testing import mock
from sqlalchemy.testing.assertions import assert_raises
from sqlalchemy.testing.assertions import assert_raises_message
from sqlalchemy.testing.assertions import eq_
from sqlalchemy.testing.assertions import expect_raises_message
from sqlalchemy.testing.assertions import is_
from sqlalchemy.testing.fixtures import fixture_session
from sqlalchemy.testing.schema import Column
from . import _fixtures

# class GetFromIdentityTest(_fixtures.FixtureTest):
# class LoadOnIdentTest(_fixtures.FixtureTest):


class SelectStarTest(_fixtures.FixtureTest):
    run_setup_mappers = "once"
    run_inserts = "once"
    run_deletes = None

    @classmethod
    def setup_mappers(cls):
        cls._setup_stock_mapping()

    @testing.combinations(
        "plain", "text", "literal_column", argnames="exprtype"
    )
    @testing.combinations("core", "orm", argnames="coreorm")
    def test_single_star(self, exprtype, coreorm):
        """test for #8235"""
        User, Address = self.classes("User", "Address")

        if exprtype == "plain":
            star = "*"
        elif exprtype == "text":
            star = text("*")
        elif exprtype == "literal_column":
            star = literal_column("*")
        else:
            assert False

        stmt = (
            select(star)
            .select_from(User)
            .join(Address)
            .where(User.id == 7)
            .order_by(User.id, Address.id)
        )

        s = fixture_session()

        if coreorm == "core":
            result = s.connection().execute(stmt)
        elif coreorm == "orm":
            result = s.execute(stmt)
        else:
            assert False

        eq_(result.all(), [(7, "jack", 1, 7, "jack@bean.com")])

    @testing.combinations(
        "plain", "text", "literal_column", argnames="exprtype"
    )
    @testing.combinations(
        lambda User, star: (star, User.id),
        lambda User, star: (star, User),
        lambda User, star: (User.id, star),
        lambda User, star: (User, star),
        lambda User, star: (literal("some text"), star),
        lambda User, star: (star, star),
        lambda User, star: (star, text("some text")),
        argnames="testcase",
    )
    @testing.variation("stmt_type", ["select", "update", "insert", "delete"])
    def test_no_star_orm_combinations(self, exprtype, testcase, stmt_type):
        """test for #8235"""
        User = self.classes.User

        if exprtype == "plain":
            star = "*"
        elif exprtype == "text":
            star = text("*")
        elif exprtype == "literal_column":
            star = literal_column("*")
        else:
            assert False

        args = testing.resolve_lambda(testcase, User=User, star=star)

        if stmt_type.select:
            stmt = select(*args).select_from(User)
        elif stmt_type.insert:
            stmt = insert(User).returning(*args)
        elif stmt_type.update:
            stmt = update(User).values({"data": "foo"}).returning(*args)
        elif stmt_type.delete:
            stmt = delete(User).returning(*args)
        else:
            stmt_type.fail()

        s = fixture_session()

        with expect_raises_message(
            exc.CompileError,
            r"Can't generate ORM query that includes multiple expressions "
            r"at the same time as '\*';",
        ):
            s.execute(stmt)


class InstanceProcessorTest(_fixtures.FixtureTest):
    def test_state_no_load_path_comparison(self):
        # test issue #5110
        User, Order, Address = self.classes("User", "Order", "Address")
        users, orders, addresses = self.tables("users", "orders", "addresses")

        self.mapper_registry.map_imperatively(
            User,
            users,
            properties={
                "addresses": relationship(Address, lazy="joined"),
                "orders": relationship(
                    Order, lazy="joined", order_by=orders.c.id
                ),
            },
        )
        self.mapper_registry.map_imperatively(
            Order,
            orders,
            properties={"address": relationship(Address, lazy="joined")},
        )
        self.mapper_registry.map_imperatively(Address, addresses)

        s = fixture_session()

        def go():
            eq_(
                User(
                    id=7,
                    orders=[
                        Order(id=1, address=Address(id=1)),
                        Order(id=3, address=Address(id=1)),
                        Order(id=5, address=None),
                    ],
                ),
                s.get(User, 7, populate_existing=True),
            )

        self.assert_sql_count(testing.db, go, 1)


class InstancesTest(_fixtures.FixtureTest):
    run_setup_mappers = "once"
    run_inserts = "once"
    run_deletes = None

    @classmethod
    def setup_mappers(cls):
        cls._setup_stock_mapping()

    def test_cursor_close_exception_raised_in_iteration(self):
        """test #8710"""

        User = self.classes.User
        s = fixture_session()

        stmt = select(User).execution_options(yield_per=1)

        result = s.execute(stmt)
        raw_cursor = result.raw

        for row in result:
            with expect_raises_message(Exception, "whoops"):
                for row in result:
                    raise Exception("whoops")

        is_true(raw_cursor._soft_closed)

    def test_cursor_close_w_failed_rowproc(self):
        User = self.classes.User
        s = fixture_session()

        q = s.query(User)

        ctx = q._compile_context()
        cursor = mock.Mock()
        ctx.compile_state._entities = [
            mock.Mock(row_processor=mock.Mock(side_effect=Exception("boom")))
        ]
        assert_raises(Exception, loading.instances, cursor, ctx)
        assert cursor.close.called, "Cursor wasn't closed"

    def test_row_proc_not_created(self):
        User = self.classes.User
        s = fixture_session()

        q = s.query(User.id, User.name)
        stmt = select(User.id)

        assert_raises_message(
            exc.NoSuchColumnError,
            "Could not locate column in row for column 'users.name'",
            q.from_statement(stmt).all,
        )


class MergeResultTest(_fixtures.FixtureTest):
    run_setup_mappers = "once"
    run_inserts = "once"
    run_deletes = None

    @classmethod
    def setup_mappers(cls):
        cls._setup_stock_mapping()

    def _fixture(self):
        User = self.classes.User

        s = fixture_session()
        u1, u2, u3, u4 = (
            User(id=1, name="u1"),
            User(id=2, name="u2"),
            User(id=7, name="u3"),
            User(id=8, name="u4"),
        )
        s.query(User).filter(User.id.in_([7, 8])).all()
        s.close()
        return s, [u1, u2, u3, u4]

    def test_single_entity_frozen(self):
        s = fixture_session()
        User = self.classes.User

        stmt = select(User).where(User.id.in_([7, 8, 9])).order_by(User.id)
        result = s.execute(stmt)
        it = loading.merge_frozen_result(s, stmt, result.freeze())
        eq_([x.id for x in it().scalars()], [7, 8, 9])

    def test_single_column_frozen(self):
        User = self.classes.User

        s = fixture_session()

        stmt = select(User.id).where(User.id.in_([7, 8, 9])).order_by(User.id)
        result = s.execute(stmt)
        it = loading.merge_frozen_result(s, stmt, result.freeze())
        eq_([x.id for x in it()], [7, 8, 9])

    def test_entity_col_mix_plain_tuple_frozen(self):
        s = fixture_session()
        User = self.classes.User

        stmt = (
            select(User, User.id)
            .where(User.id.in_([7, 8, 9]))
            .order_by(User.id)
        )
        result = s.execute(stmt)

        it = loading.merge_frozen_result(s, stmt, result.freeze())
        it = list(it())
        eq_([(x.id, y) for x, y in it], [(7, 7), (8, 8), (9, 9)])
        eq_(list(it[0]._mapping.keys()), ["User", "id"])


class InterimRowsLoadTest(fixtures.DeclarativeMappedTest):
    """ORM loading fetches rows as plain processed tuples via
    Result._raw_all_tuples(); test that result processors apply and
    that engine-level row logging, which requires Row objects, still
    loads correctly via its fallback."""

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class UpperString(TypeDecorator):
            impl = String(50)
            cache_ok = True

            def process_result_value(self, value, dialect):
                return value.upper() if value is not None else None

        class A(Base):
            __tablename__ = "interim_a"
            id = Column(Integer, primary_key=True)
            data = Column(UpperString())

    @classmethod
    def insert_data(cls, connection):
        A = cls.classes.A
        s = Session(connection)
        s.add_all([A(id=1, data="one"), A(id=2, data=None)])
        s.commit()

    def _assert_load(self, connection):
        A = self.classes.A
        s = fixture_session(bind=connection)
        a1, a2 = s.query(A).order_by(A.id).all()
        eq_(a1.data, "ONE")
        is_(a2.data, None)

    def test_result_processors_applied(self, connection):
        self._assert_load(connection)

    def test_row_logging_fallback(self, debug_logging_engine):
        """with debug-level logging, the ORM row fetch falls back to
        constructing Row objects, and each row is logged"""

        testing_engine, buf = debug_logging_engine

        engine = testing_engine(echo="debug")

        with engine.connect() as conn:
            self._assert_load(conn)

        row_messages = [
            rec.getMessage()
            for rec in buf.buffer
            if rec.getMessage().startswith("Row ")
        ]
        eq_(
            row_messages,
            ["Row (1, 'ONE')", "Row (2, None)"],
        )
