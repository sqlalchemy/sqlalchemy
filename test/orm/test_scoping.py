from unittest.mock import Mock

import sqlalchemy as sa
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy import util
from sqlalchemy.orm import query
from sqlalchemy.orm import relationship
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import assert_warns_message
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.testing import mock
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

        self.mapper_registry.map_imperatively(
            SomeObject,
            table1,
            properties={"options": relationship(SomeOtherObject)},
        )
        self.mapper_registry.map_imperatively(SomeOtherObject, table2)

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

        assert_warns_message(
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
        assert s0.autoflush == True

        mock_scope_func.return_value = 0
        s1 = Session()
        assert s1.autoflush == True

        assert_raises_message(
            sa.exc.InvalidRequestError,
            "Scoped session is already present",
            Session,
            autoflush=False,
        )

        mock_scope_func.return_value = 1
        s2 = Session(autoflush=False)
        assert s2.autoflush == False

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
                mock.call.add("add", _warn=True),
                mock.call.delete("delete"),
                mock.call.get(
                    "Cls",
                    5,
                    options=None,
                    populate_existing=False,
                    with_for_update=None,
                    identity_token=None,
                    execution_options=util.EMPTY_DICT,
                    bind_arguments=None,
                ),
            ],
        )

        with mock.patch(
            "sqlalchemy.orm.session.object_session"
        ) as mock_object_session:
            sess.object_session("foo")

        eq_(mock_object_session.mock_calls, [mock.call("foo")])

    @testing.combinations(
        "style1",
        "style2",
        "style3",
        "style4",
    )
    def test_get_bind_custom_session_subclass(self, style):
        """test #6285"""

        class MySession(Session):
            if style == "style1":

                def get_bind(self, mapper=None, **kwargs):
                    return super().get_bind(mapper=mapper, **kwargs)

            elif style == "style2":
                # this was the workaround for #6285, ensure it continues
                # working as well
                def get_bind(self, mapper=None, *args, **kwargs):
                    return super().get_bind(mapper, *args, **kwargs)

            elif style == "style3":
                # py2k style
                def get_bind(self, mapper=None, *args, **kwargs):
                    return super().get_bind(mapper, *args, **kwargs)

            elif style == "style4":
                # py2k style
                def get_bind(self, mapper=None, **kwargs):
                    return super().get_bind(mapper=mapper, **kwargs)

        s1 = MySession(testing.db)
        is_(s1.get_bind(), testing.db)

        ss = scoped_session(sessionmaker(testing.db, class_=MySession))

        is_(ss.get_bind(), testing.db)

    def test_attributes(self):
        expected = [
            name
            for cls in Session.mro()
            for name in vars(cls)
            if not name.startswith("_")
        ]

        ignore_list = {
            "connection_callable",
            "transaction",
            "in_transaction",
            "in_nested_transaction",
            "get_transaction",
            "get_nested_transaction",
            "prepare",
            "invalidate",
            "bind_mapper",
            "bind_table",
            "enable_relationship_loading",
            "dispatch",
        }

        SM = scoped_session(sa.orm.sessionmaker(testing.db))

        missing = [
            name
            for name in expected
            if not hasattr(SM, name) and name not in ignore_list
        ]
        eq_(missing, [])
