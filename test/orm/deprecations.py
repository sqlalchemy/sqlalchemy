"""The collection of modern alternatives to deprecated & removed functionality.

Collects specimens of old ORM code and explicitly covers the recommended
modern (i.e. not deprecated) alternative to them.  The tests snippets here can
be migrated directly to the wiki, docs, etc.

"""
import testenv; testenv.configure_for_tests()
from sqlalchemy import *
from sqlalchemy.orm import *
from testlib import *


users, addresses = None, None
session = None

class Base(object):
    def __init__(self, **kw):
        for k, v in kw.iteritems():
            setattr(self, k, v)

class User(Base): pass
class Address(Base): pass


class QueryAlternativesTest(ORMTest):
    '''Collects modern idioms for Queries

    The docstring for each test case serves as miniature documentation about
    the deprecated use case, and the test body illustrates (and covers) the
    intended replacement code to accomplish the same task.

    Documenting the "old way" including the argument signature helps these
    cases remain useful to readers even after the deprecated method has been
    removed from the modern codebase.

    Format:

    def test_deprecated_thing(self):
        """Query.methodname(old, arg, **signature)

        output = session.query(User).deprecatedmethod(inputs)

        """
        # 0.4+
        output = session.query(User).newway(inputs)
        assert output is correct

        # 0.5+
        output = session.query(User).evennewerway(inputs)
        assert output is correct

    '''
    keep_mappers = True
    keep_data = True

    def define_tables(self, metadata):
        global users_table, addresses_table
        users_table = Table(
            'users', metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String(64)))

        addresses_table = Table(
            'addresses', metadata,
            Column('id', Integer, primary_key=True),
            Column('user_id', Integer, ForeignKey('users.id')),
            Column('email_address', String(128)),
            Column('purpose', String(16)),
            Column('bounces', Integer, default=0))

    def setup_mappers(self):
        mapper(User, users_table, properties=dict(
            addresses=relation(Address, backref='user'),
            ))
        mapper(Address, addresses_table)

    def insert_data(self):
        user_cols = ('id', 'name')
        user_rows = ((1, 'jack'), (2, 'ed'), (3, 'fred'), (4, 'chuck'))
        users_table.insert().execute(
            [dict(zip(user_cols, row)) for row in user_rows])

        add_cols = ('id', 'user_id', 'email_address', 'purpose', 'bounces')
        add_rows = (
            (1, 1, 'jack@jack.home', 'Personal', 0),
            (2, 1, 'jack@jack.bizz', 'Work', 1),
            (3, 2, 'ed@foo.bar', 'Personal', 0),
            (4, 3, 'fred@the.fred', 'Personal', 10))

        addresses_table.insert().execute(
            [dict(zip(add_cols, row)) for row in add_rows])

    def setUp(self):
        super(QueryAlternativesTest, self).setUp()
        global session
        if session is None:
            session = create_session()

    def tearDown(self):
        super(QueryAlternativesTest, self).tearDown()
        session.clear()

    ######################################################################

    def test_apply_max(self):
        """Query.apply_max(col)

        max = session.query(Address).apply_max(Address.bounces)

        """
        # 0.5.0
        maxes = list(session.query(Address).values(func.max(Address.bounces)))
        max = maxes[0][0]
        assert max == 10

        max = session.query(func.max(Address.bounces)).one()[0]
        assert max == 10

    def test_apply_min(self):
        """Query.apply_min(col)

        min = session.query(Address).apply_min(Address.bounces)

        """
        # 0.5.0
        mins = list(session.query(Address).values(func.min(Address.bounces)))
        min = mins[0][0]
        assert min == 0

        min = session.query(func.min(Address.bounces)).one()[0]
        assert min == 0

    def test_apply_avg(self):
        """Query.apply_avg(col)

        avg = session.query(Address).apply_avg(Address.bounces)

        """
        avgs = list(session.query(Address).values(func.avg(Address.bounces)))
        avg = avgs[0][0]
        assert avg > 0 and avg < 10

        avg = session.query(func.avg(Address.bounces)).one()[0]
        assert avg > 0 and avg < 10

    def test_apply_sum(self):
        """Query.apply_sum(col)

        avg = session.query(Address).apply_avg(Address.bounces)

        """
        avgs = list(session.query(Address).values(func.avg(Address.bounces)))
        avg = avgs[0][0]
        assert avg > 0 and avg < 10

        avg = session.query(func.avg(Address.bounces)).one()[0]
        assert avg > 0 and avg < 10

    def test_count_by(self):
        """Query.count_by(*args, **params)

        num = session.query(Address).count_by(purpose='Personal')

        # old-style implicit *_by join
        num = session.query(User).count_by(purpose='Personal')

        """
        num = session.query(Address).filter_by(purpose='Personal').count()
        assert num == 3, num

        num = (session.query(User).join('addresses').
               filter(Address.purpose=='Personal')).count()
        assert num == 3, num

    def test_count_whereclause(self):
        """Query.count(whereclause=None, params=None, **kwargs)

        num = session.query(Address).count(address_table.c.bounces > 1)

        """
        num = session.query(Address).filter(Address.bounces > 1).count()
        assert num == 1, num

    def test_execute(self):
        """Query.execute(clauseelement, params=None, *args, **kwargs)

        users = session.query(User).execute(users_table.select())

        """
        users = session.query(User).from_statement(users_table.select()).all()
        assert len(users) == 4

    def test_get_by(self):
        """Query.get_by(*args, **params)

        user = session.query(User).get_by(name='ed')

        # 0.3-style implicit *_by join
        user = session.query(User).get_by(email_addresss='fred@the.fred')

        """
        user = session.query(User).filter_by(name='ed').first()
        assert user.name == 'ed'

        user = (session.query(User).join('addresses').
                filter(Address.email_address=='fred@the.fred')).first()
        assert user.name == 'fred'

        user = session.query(User).filter(
            User.addresses.any(Address.email_address=='fred@the.fred')).first()
        assert user.name == 'fred'

    def test_instances_entities(self):
        """Query.instances(cursor, *mappers_or_columns, **kwargs)

        sel = users_table.join(addresses_table).select(use_labels=True)
        res = session.query(User).instances(sel.execute(), Address)

        """
        sel = users_table.join(addresses_table).select(use_labels=True)
        res = session.query(User, Address).instances(sel.execute())

        assert len(res) == 4
        cola, colb = res[0]
        assert isinstance(cola, User) and isinstance(colb, Address)


    def test_join_by(self):
        """Query.join_by(*args, **params)

        TODO
        """

    def test_join_to(self):
        """Query.join_to(key)

        TODO
        """

    def test_join_via(self):
        """Query.join_via(keys)

        TODO
        """

    def test_list(self):
        """Query.list()

        users = session.query(User).list()

        """
        users = session.query(User).all()
        assert len(users) == 4

    def test_scalar(self):
        """Query.scalar()

        user = session.query(User).filter(User.id==1).scalar()

        """
        user = session.query(User).filter(User.id==1).first()
        assert user.id==1

    def test_select(self):
        """Query.select(arg=None, **kwargs)

        users = session.query(User).select(users_table.c.name != None)

        """
        users = session.query(User).filter(User.name != None).all()
        assert len(users) == 4

    def test_select_by(self):
        """Query.select_by(*args, **params)

        users = session.query(User).select_by(name='fred')

        # 0.3 magic join on *_by methods
        users = session.query(User).select_by(email_address='fred@the.fred')

        """
        users = session.query(User).filter_by(name='fred').all()
        assert len(users) == 1

        users = session.query(User).filter(User.name=='fred').all()
        assert len(users) == 1

        users = (session.query(User).join('addresses').
                 filter_by(email_address='fred@the.fred')).all()
        assert len(users) == 1

        users = session.query(User).filter(User.addresses.any(
            Address.email_address == 'fred@the.fred')).all()
        assert len(users) == 1

    def test_selectfirst(self):
        """Query.selectfirst(arg=None, **kwargs)

        bounced = session.query(Address).selectfirst(
          addresses_table.c.bounces > 0)

        """
        bounced = session.query(Address).filter(Address.bounces > 0).first()
        assert bounced.bounces > 0

    def test_selectfirst_by(self):
        """Query.selectfirst_by(*args, **params)

        onebounce = session.query(Address).selectfirst_by(bounces=1)

        # 0.3 magic join on *_by methods
        onebounce_user = session.query(User).selectfirst_by(bounces=1)

        """
        onebounce = session.query(Address).filter_by(bounces=1).first()
        assert onebounce.bounces == 1

        onebounce_user = (session.query(User).join('addresses').
                          filter_by(bounces=1)).first()
        assert onebounce_user.name == 'jack'

        onebounce_user = (session.query(User).join('addresses').
                          filter(Address.bounces == 1)).first()
        assert onebounce_user.name == 'jack'

        onebounce_user = session.query(User).filter(User.addresses.any(
            Address.bounces == 1)).first()
        assert onebounce_user.name == 'jack'


    def test_selectone(self):
        """Query.selectone(arg=None, **kwargs)

        ed = session.query(User).selectone(users_table.c.name == 'ed')

        """
        ed = session.query(User).filter(User.name == 'jack').one()

    def test_selectone_by(self):
        """Query.selectone_by

        ed = session.query(User).selectone_by(name='ed')

        # 0.3 magic join on *_by methods
        ed = session.query(User).selectone_by(email_address='ed@foo.bar')

        """
        ed = session.query(User).filter_by(name='jack').one()

        ed = session.query(User).filter(User.name == 'jack').one()

        ed = session.query(User).join('addresses').filter(
            Address.email_address == 'ed@foo.bar').one()

        ed = session.query(User).filter(User.addresses.any(
            Address.email_address == 'ed@foo.bar')).one()

    def test_select_statement(self):
        """Query.select_statement(statement, **params)

        users = session.query(User).select_statement(users_table.select())

        """
        users = session.query(User).from_statement(users_table.select()).all()
        assert len(users) == 4

    def test_select_text(self):
        """Query.select_text(text, **params)

        users = session.query(User).select_text('SELECT * FROM users')

        """
        users = session.query(User).from_statement('SELECT * FROM users').all()
        assert len(users) == 4

    def test_select_whereclause(self):
        """Query.select_whereclause(whereclause=None, params=None, **kwargs)


        users = session,query(User).select_whereclause(users.c.name=='ed')
        users = session.query(User).select_whereclause("name='ed'")

        """
        users = session.query(User).filter(User.name=='ed').all()
        assert len(users) == 1 and users[0].name == 'ed'

        users = session.query(User).filter("name='ed'").all()
        assert len(users) == 1 and users[0].name == 'ed'



if __name__ == '__main__':
    testenv.main()
