import sqlalchemy as sa
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy.orm import mapper
from sqlalchemy.orm import query
from sqlalchemy.orm import relationship
from sqlalchemy.orm import scoped_session
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import mock
from sqlalchemy.testing.mock import Mock
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table


class ScopedSessionTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "table1",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", String(30)),
        )
        Table(
            "table2",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("someid", None, ForeignKey("table1.id")),
        )

    def test_basic(self):
        table2, table1 = self.tables.table2, self.tables.table1

        Session = scoped_session(sa.orm.sessionmaker(testing.db))

        class CustomQuery(query.Query):
            pass

        class SomeObject(fixtures.ComparableEntity):
            query = Session.query_property()

        class SomeOtherObject(fixtures.ComparableEntity):
            query = Session.query_property()
            custom_query = Session.query_property(query_cls=CustomQuery)

        mapper(
            SomeObject,
            table1,
            properties={"options": relationship(SomeOtherObject)},
        )
        mapper(SomeOtherObject, table2)

        s = SomeObject(id=1, data="hello")
        sso = SomeOtherObject()
        s.options.append(sso)
        Session.add(s)
        Session.commit()
        Session.refresh(sso)
        Session.remove()

        eq_(
            SomeObject(
                id=1, data="hello", options=[SomeOtherObject(someid=1)]
            ),
            Session.query(SomeObject).one(),
        )
        eq_(
            SomeObject(
                id=1, data="hello", options=[SomeOtherObject(someid=1)]
            ),
            SomeObject.query.one(),
        )
        eq_(
            SomeOtherObject(someid=1),
            SomeOtherObject.query.filter(
                SomeOtherObject.someid == sso.someid
            ).one(),
        )
        assert isinstance(SomeOtherObject.query, query.Query)
        assert not isinstance(SomeOtherObject.query, CustomQuery)
        assert isinstance(SomeOtherObject.custom_query, query.Query)

    def test_config_errors(self):
        Session = scoped_session(sa.orm.sessionmaker())

        s = Session()  # noqa
        assert_raises_message(
            sa.exc.InvalidRequestError,
            "Scoped session is already present",
            Session,
            bind=testing.db,
        )

        assert_raises_message(
            sa.exc.SAWarning,
            "At least one scoped session is already present. ",
            Session.configure,
            bind=testing.db,
        )

    def test_call_with_kwargs(self):
        mock_scope_func = Mock()
        SessionMaker = sa.orm.sessionmaker()
        Session = scoped_session(sa.orm.sessionmaker(), mock_scope_func)

        s0 = SessionMaker()
        assert s0.autocommit == False

        mock_scope_func.return_value = 0
        s1 = Session()
        assert s1.autocommit == False

        assert_raises_message(
            sa.exc.InvalidRequestError,
            "Scoped session is already present",
            Session,
            autocommit=True,
        )

        mock_scope_func.return_value = 1
        s2 = Session(autocommit=True)
        assert s2.autocommit == True

    def test_methods_etc(self):
        mock_session = Mock()
        mock_session.bind = "the bind"

        sess = scoped_session(lambda: mock_session)

        sess.add("add")
        sess.delete("delete")

        sess.get("Cls", 5)

        eq_(sess.bind, "the bind")

        eq_(
            mock_session.mock_calls,
            [
                mock.call.add("add", True),
                mock.call.delete("delete"),
                mock.call.get(
                    "Cls", 5, mock.ANY, mock.ANY, mock.ANY, mock.ANY
                ),
            ],
        )

        with mock.patch(
            "sqlalchemy.orm.session.object_session"
        ) as mock_object_session:
            sess.object_session("foo")

        eq_(mock_object_session.mock_calls, [mock.call("foo")])
