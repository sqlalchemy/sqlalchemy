from sqlalchemy.testing import assert_raises, assert_raises_message
import sqlalchemy as sa
from sqlalchemy import testing
from sqlalchemy.orm import scoped_session
from sqlalchemy import Integer, String, ForeignKey
from sqlalchemy.testing.schema import Table, Column
from sqlalchemy.orm import mapper, relationship, query
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures



class _ScopedTest(fixtures.MappedTest):
    """Adds another lookup bucket to emulate Session globals."""

    run_setup_mappers = 'once'

    @classmethod
    def setup_class(cls):
        cls.scoping = _base.adict()
        super(_ScopedTest, cls).setup_class()

    @classmethod
    def teardown_class(cls):
        cls.scoping.clear()
        super(_ScopedTest, cls).teardown_class()


class ScopedSessionTest(fixtures.MappedTest):

    @classmethod
    def define_tables(cls, metadata):
        Table('table1', metadata,
              Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
              Column('data', String(30)))
        Table('table2', metadata,
              Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
              Column('someid', None, ForeignKey('table1.id')))

    def test_basic(self):
        table2, table1 = self.tables.table2, self.tables.table1

        Session = scoped_session(sa.orm.sessionmaker())

        class CustomQuery(query.Query):
            pass

        class SomeObject(fixtures.ComparableEntity):
            query = Session.query_property()
        class SomeOtherObject(fixtures.ComparableEntity):
            query = Session.query_property()
            custom_query = Session.query_property(query_cls=CustomQuery)

        mapper(SomeObject, table1, properties={
            'options':relationship(SomeOtherObject)})
        mapper(SomeOtherObject, table2)

        s = SomeObject(id=1, data="hello")
        sso = SomeOtherObject()
        s.options.append(sso)
        Session.add(s)
        Session.commit()
        Session.refresh(sso)
        Session.remove()

        eq_(SomeObject(id=1, data="hello", options=[SomeOtherObject(someid=1)]),
            Session.query(SomeObject).one())
        eq_(SomeObject(id=1, data="hello", options=[SomeOtherObject(someid=1)]),
            SomeObject.query.one())
        eq_(SomeOtherObject(someid=1),
            SomeOtherObject.query.filter(
                SomeOtherObject.someid == sso.someid).one())
        assert isinstance(SomeOtherObject.query, query.Query)
        assert not isinstance(SomeOtherObject.query, CustomQuery)
        assert isinstance(SomeOtherObject.custom_query, query.Query)

    def test_config_errors(self):
        Session = scoped_session(sa.orm.sessionmaker())

        s = Session()
        assert_raises_message(
            sa.exc.InvalidRequestError,
            "Scoped session is already present",
            Session, bind=testing.db
        )

        assert_raises_message(
            sa.exc.SAWarning,
            "At least one scoped session is already present. ",
            Session.configure, bind=testing.db
        )



