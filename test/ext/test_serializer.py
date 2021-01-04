# coding: utf-8

from sqlalchemy import desc
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import Integer
from sqlalchemy import join
from sqlalchemy import literal_column
from sqlalchemy import MetaData
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy.ext import serializer
from sqlalchemy.orm import aliased
from sqlalchemy.orm import class_mapper
from sqlalchemy.orm import column_property
from sqlalchemy.orm import configure_mappers
from sqlalchemy.orm import joinedload
from sqlalchemy.orm import mapper
from sqlalchemy.orm import relationship
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table
from sqlalchemy.util import ue


def pickle_protocols():
    return iter([-1, 1, 2])
    # return iter([-1, 0, 1, 2])


class User(fixtures.ComparableEntity):
    pass


class Address(fixtures.ComparableEntity):
    pass


class SerializeTest(AssertsCompiledSQL, fixtures.MappedTest):

    run_setup_mappers = "once"
    run_inserts = "once"
    run_deletes = None

    @classmethod
    def define_tables(cls, metadata):
        global users, addresses
        users = Table(
            "users",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(50)),
        )
        addresses = Table(
            "addresses",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("email", String(50)),
            Column("user_id", Integer, ForeignKey("users.id")),
        )

    @classmethod
    def setup_mappers(cls):
        global Session
        Session = scoped_session(sessionmaker(testing.db))
        mapper(
            User,
            users,
            properties={
                "addresses": relationship(
                    Address, backref="user", order_by=addresses.c.id
                )
            },
        )
        mapper(Address, addresses)
        configure_mappers()

    @classmethod
    def insert_data(cls, connection):
        params = [
            dict(list(zip(("id", "name"), column_values)))
            for column_values in [
                (7, "jack"),
                (8, "ed"),
                (9, "fred"),
                (10, "chuck"),
            ]
        ]
        connection.execute(users.insert(), params)
        connection.execute(
            addresses.insert(),
            [
                dict(list(zip(("id", "user_id", "email"), column_values)))
                for column_values in [
                    (1, 7, "jack@bean.com"),
                    (2, 8, "ed@wood.com"),
                    (3, 8, "ed@bettyboop.com"),
                    (4, 8, "ed@lala.com"),
                    (5, 9, "fred@fred.com"),
                ]
            ],
        )

    def test_tables(self):
        assert (
            serializer.loads(
                serializer.dumps(users, -1), users.metadata, Session
            )
            is users
        )

    def test_columns(self):
        assert (
            serializer.loads(
                serializer.dumps(users.c.name, -1), users.metadata, Session
            )
            is users.c.name
        )

    def test_mapper(self):
        user_mapper = class_mapper(User)
        assert (
            serializer.loads(serializer.dumps(user_mapper, -1), None, None)
            is user_mapper
        )

    def test_attribute(self):
        assert (
            serializer.loads(serializer.dumps(User.name, -1), None, None)
            is User.name
        )

    def test_expression(self):
        expr = select(users).select_from(users.join(addresses)).limit(5)
        re_expr = serializer.loads(
            serializer.dumps(expr, -1), users.metadata, None
        )
        eq_(str(expr), str(re_expr))
        eq_(
            Session.connection().execute(re_expr).fetchall(),
            [(7, "jack"), (8, "ed"), (8, "ed"), (8, "ed"), (9, "fred")],
        )

    def test_query_one(self):
        q = (
            Session.query(User)
            .filter(User.name == "ed")
            .options(joinedload(User.addresses))
        )

        q2 = serializer.loads(serializer.dumps(q, -1), users.metadata, Session)

        def go():
            eq_(
                q2.all(),
                [
                    User(
                        name="ed",
                        addresses=[
                            Address(id=2),
                            Address(id=3),
                            Address(id=4),
                        ],
                    )
                ],
            )

        self.assert_sql_count(testing.db, go, 1)

        eq_(
            q2.join(User.addresses)
            .filter(Address.email == "ed@bettyboop.com")
            .enable_eagerloads(False)
            .with_entities(func.count(literal_column("*")))
            .scalar(),
            1,
        )
        u1 = Session.query(User).get(8)
        q = (
            Session.query(Address)
            .filter(Address.user == u1)
            .order_by(desc(Address.email))
        )
        q2 = serializer.loads(serializer.dumps(q, -1), users.metadata, Session)
        eq_(
            q2.all(),
            [
                Address(email="ed@wood.com"),
                Address(email="ed@lala.com"),
                Address(email="ed@bettyboop.com"),
            ],
        )

    @testing.requires.non_broken_pickle
    def test_query_two(self):
        q = (
            Session.query(User)
            .join(User.addresses)
            .filter(Address.email.like("%fred%"))
        )
        q2 = serializer.loads(serializer.dumps(q, -1), users.metadata, Session)
        eq_(q2.all(), [User(name="fred")])
        eq_(list(q2.with_entities(User.id, User.name)), [(9, "fred")])

    @testing.requires.non_broken_pickle
    def test_query_three(self):
        ua = aliased(User)
        q = (
            Session.query(ua)
            .join(ua.addresses)
            .filter(Address.email.like("%fred%"))
        )
        for prot in pickle_protocols():
            q2 = serializer.loads(
                serializer.dumps(q, prot), users.metadata, Session
            )
            eq_(q2.all(), [User(name="fred")])

            # try to pull out the aliased entity here...
            ua_2 = q2._compile_state()._entities[0].entity_zero.entity
            eq_(list(q2.with_entities(ua_2.id, ua_2.name)), [(9, "fred")])

    def test_annotated_one(self):
        j = join(users, addresses)._annotate({"foo": "bar"})
        query = select(addresses).select_from(j)

        str(query)
        for prot in pickle_protocols():
            pickled_failing = serializer.dumps(j, prot)
            serializer.loads(pickled_failing, users.metadata, None)

    @testing.requires.non_broken_pickle
    def test_orm_join(self):
        from sqlalchemy.orm.util import join

        j = join(User, Address, User.addresses)

        j2 = serializer.loads(serializer.dumps(j, -1), users.metadata)
        assert j2.left is j.left
        assert j2.right is j.right

    @testing.exclude(
        "sqlite", "<=", (3, 5, 9), "id comparison failing on the buildbot"
    )
    def test_aliases(self):
        u7, u8, u9, u10 = Session.query(User).order_by(User.id).all()
        ualias = aliased(User)
        q = (
            Session.query(User, ualias)
            .join(ualias, User.id < ualias.id)
            .filter(User.id < 9)
            .order_by(User.id, ualias.id)
        )
        eq_(
            list(q.all()), [(u7, u8), (u7, u9), (u7, u10), (u8, u9), (u8, u10)]
        )
        q2 = serializer.loads(serializer.dumps(q, -1), users.metadata, Session)
        eq_(
            list(q2.all()),
            [(u7, u8), (u7, u9), (u7, u10), (u8, u9), (u8, u10)],
        )

    @testing.requires.non_broken_pickle
    def test_any(self):
        r = User.addresses.any(Address.email == "x")
        ser = serializer.dumps(r, -1)
        x = serializer.loads(ser, users.metadata)
        eq_(str(r), str(x))

    def test_unicode(self):
        m = MetaData()
        t = Table(
            ue("\u6e2c\u8a66"), m, Column(ue("\u6e2c\u8a66_id"), Integer)
        )

        expr = select(t).where(t.c[ue("\u6e2c\u8a66_id")] == 5)

        expr2 = serializer.loads(serializer.dumps(expr, -1), m)

        self.assert_compile(
            expr2,
            ue(
                'SELECT "\u6e2c\u8a66"."\u6e2c\u8a66_id" FROM "\u6e2c\u8a66" '
                'WHERE "\u6e2c\u8a66"."\u6e2c\u8a66_id" = :\u6e2c\u8a66_id_1'
            ),
            dialect="default",
        )


class ColumnPropertyWParamTest(
    AssertsCompiledSQL, fixtures.DeclarativeMappedTest
):
    __dialect__ = "default"

    run_create_tables = None

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        global TestTable

        class TestTable(Base):
            __tablename__ = "test"

            id = Column(Integer, primary_key=True, autoincrement=True)
            _some_id = Column("some_id", String)
            some_primary_id = column_property(
                func.left(_some_id, 6).cast(Integer)
            )

    def test_deserailize_colprop(self):
        TestTable = self.classes.TestTable

        s = scoped_session(sessionmaker())

        expr = s.query(TestTable).filter(TestTable.some_primary_id == 123456)

        expr2 = serializer.loads(serializer.dumps(expr), TestTable.metadata, s)

        # note in the original, the same bound parameter is used twice
        self.assert_compile(
            expr,
            "SELECT test.some_id AS test_some_id, "
            "CAST(left(test.some_id, :left_1) AS INTEGER) AS anon_1, "
            "test.id AS test_id FROM test WHERE "
            "CAST(left(test.some_id, :left_1) AS INTEGER) = :param_1",
            checkparams={"left_1": 6, "param_1": 123456},
        )

        # in the deserialized, it's two separate parameter objects which
        # need to have different anonymous names.  they still have
        # the same value however
        self.assert_compile(
            expr2,
            "SELECT test.some_id AS test_some_id, "
            "CAST(left(test.some_id, :left_1) AS INTEGER) AS anon_1, "
            "test.id AS test_id FROM test WHERE "
            "CAST(left(test.some_id, :left_2) AS INTEGER) = :param_1",
            checkparams={"left_1": 6, "left_2": 6, "param_1": 123456},
        )


if __name__ == "__main__":
    testing.main()
