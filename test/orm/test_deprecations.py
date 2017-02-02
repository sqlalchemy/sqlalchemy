"""The collection of modern alternatives to deprecated & removed functionality.

Collects specimens of old ORM code and explicitly covers the recommended
modern (i.e. not deprecated) alternative to them.  The tests snippets here can
be migrated directly to the wiki, docs, etc.

"""
from sqlalchemy import Integer, String, ForeignKey, func, text
from sqlalchemy.testing.schema import Table
from sqlalchemy.testing.schema import Column
from sqlalchemy.orm import mapper, relationship, create_session, sessionmaker
from sqlalchemy.testing import fixtures


class QueryAlternativesTest(fixtures.MappedTest):
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

    run_inserts = 'once'
    run_deletes = None

    @classmethod
    def define_tables(cls, metadata):
        Table('users_table', metadata,
              Column('id', Integer, primary_key=True),
              Column('name', String(64)))

        Table('addresses_table', metadata,
              Column('id', Integer, primary_key=True),
              Column('user_id', Integer, ForeignKey('users_table.id')),
              Column('email_address', String(128)),
              Column('purpose', String(16)),
              Column('bounces', Integer, default=0))

    @classmethod
    def setup_classes(cls):
        class User(cls.Basic):
            pass

        class Address(cls.Basic):
            pass

    @classmethod
    def setup_mappers(cls):
        addresses_table, User, users_table, Address = \
            (cls.tables.addresses_table,
             cls.classes.User,
             cls.tables.users_table,
             cls.classes.Address)

        mapper(User, users_table, properties=dict(
            addresses=relationship(Address, backref='user'),
        ))
        mapper(Address, addresses_table)

    @classmethod
    def fixtures(cls):
        return dict(
            users_table=(
                ('id', 'name'),
                (1, 'jack'),
                (2, 'ed'),
                (3, 'fred'),
                (4, 'chuck')),

            addresses_table=(
                ('id', 'user_id', 'email_address', 'purpose', 'bounces'),
                (1, 1, 'jack@jack.home', 'Personal', 0),
                (2, 1, 'jack@jack.bizz', 'Work', 1),
                (3, 2, 'ed@foo.bar', 'Personal', 0),
                (4, 3, 'fred@the.fred', 'Personal', 10)))

    ######################################################################

    def test_override_get(self):
        """MapperExtension.get()

        x = session.query.get(5)

        """

        Address = self.classes.Address

        from sqlalchemy.orm.query import Query
        cache = {}

        class MyQuery(Query):
            def get(self, ident, **kwargs):
                if ident in cache:
                    return cache[ident]
                else:
                    x = super(MyQuery, self).get(ident)
                    cache[ident] = x
                    return x

        session = sessionmaker(query_cls=MyQuery)()

        ad1 = session.query(Address).get(1)
        assert ad1 in list(cache.values())

    def test_load(self):
        """x = session.query(Address).load(1)

            x = session.load(Address, 1)

        """

        Address = self.classes.Address

        session = create_session()
        ad1 = session.query(Address).populate_existing().get(1)
        assert bool(ad1)

    def test_apply_max(self):
        """Query.apply_max(col)

        max = session.query(Address).apply_max(Address.bounces)

        """

        Address = self.classes.Address

        session = create_session()

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

        Address = self.classes.Address

        session = create_session()

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

        Address = self.classes.Address

        session = create_session()

        avgs = list(session.query(Address).values(func.avg(Address.bounces)))
        avg = avgs[0][0]
        assert avg > 0 and avg < 10

        avg = session.query(func.avg(Address.bounces)).one()[0]
        assert avg > 0 and avg < 10

    def test_apply_sum(self):
        """Query.apply_sum(col)

        avg = session.query(Address).apply_avg(Address.bounces)

        """

        Address = self.classes.Address

        session = create_session()

        avgs = list(session.query(Address).values(func.sum(Address.bounces)))
        avg = avgs[0][0]
        assert avg == 11

        avg = session.query(func.sum(Address.bounces)).one()[0]
        assert avg == 11

    def test_count_by(self):
        """Query.count_by(*args, **params)

        num = session.query(Address).count_by(purpose='Personal')

        # old-style implicit *_by join
        num = session.query(User).count_by(purpose='Personal')

        """

        User, Address = self.classes.User, self.classes.Address

        session = create_session()

        num = session.query(Address).filter_by(purpose='Personal').count()
        assert num == 3, num

        num = (session.query(User).join('addresses').
               filter(Address.purpose == 'Personal')).count()
        assert num == 3, num

    def test_count_whereclause(self):
        """Query.count(whereclause=None, params=None, **kwargs)

        num = session.query(Address).count(address_table.c.bounces > 1)

        """

        Address = self.classes.Address

        session = create_session()

        num = session.query(Address).filter(Address.bounces > 1).count()
        assert num == 1, num

    def test_execute(self):
        """Query.execute(clauseelement, params=None, *args, **kwargs)

        users = session.query(User).execute(users_table.select())

        """

        User, users_table = self.classes.User, self.tables.users_table

        session = create_session()

        users = session.query(User).from_statement(users_table.select()).all()
        assert len(users) == 4

    def test_get_by(self):
        """Query.get_by(*args, **params)

        user = session.query(User).get_by(name='ed')

        # 0.3-style implicit *_by join
        user = session.query(User).get_by(email_addresss='fred@the.fred')

        """

        User, Address = self.classes.User, self.classes.Address

        session = create_session()

        user = session.query(User).filter_by(name='ed').first()
        assert user.name == 'ed'

        user = (session.query(User).join('addresses').
                filter(Address.email_address == 'fred@the.fred')).first()
        assert user.name == 'fred'

        user = session.query(User).filter(
            User.addresses.any(Address.email_address == 'fred@the.fred')
        ).first()
        assert user.name == 'fred'

    def test_instances_entities(self):
        """Query.instances(cursor, *mappers_or_columns, **kwargs)

        sel = users_table.join(addresses_table).select(use_labels=True)
        res = session.query(User).instances(sel.execute(), Address)

        """

        addresses_table, User, users_table, Address = \
            (self.tables.addresses_table,
             self.classes.User,
             self.tables.users_table,
             self.classes.Address)

        session = create_session()

        sel = users_table.join(addresses_table).select(use_labels=True)
        res = list(session.query(User, Address).instances(sel.execute()))

        assert len(res) == 4
        cola, colb = res[0]
        assert isinstance(cola, User) and isinstance(colb, Address)

    def test_join_by(self):
        """Query.join_by(*args, **params)

        TODO
        """

        session = create_session()

    def test_join_to(self):
        """Query.join_to(key)

        TODO
        """

        session = create_session()

    def test_join_via(self):
        """Query.join_via(keys)

        TODO
        """

        session = create_session()

    def test_list(self):
        """Query.list()

        users = session.query(User).list()

        """

        User = self.classes.User

        session = create_session()

        users = session.query(User).all()
        assert len(users) == 4

    def test_scalar(self):
        """Query.scalar()

        user = session.query(User).filter(User.id==1).scalar()

        """

        User = self.classes.User

        session = create_session()

        user = session.query(User).filter(User.id == 1).first()
        assert user.id == 1

    def test_select(self):
        """Query.select(arg=None, **kwargs)

        users = session.query(User).select(users_table.c.name != None)

        """

        User = self.classes.User

        session = create_session()

        users = session.query(User).filter(User.name != None).all()  # noqa
        assert len(users) == 4

    def test_select_by(self):
        """Query.select_by(*args, **params)

        users = session.query(User).select_by(name='fred')

        # 0.3 magic join on *_by methods
        users = session.query(User).select_by(email_address='fred@the.fred')

        """

        User, Address = self.classes.User, self.classes.Address

        session = create_session()

        users = session.query(User).filter_by(name='fred').all()
        assert len(users) == 1

        users = session.query(User).filter(User.name == 'fred').all()
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

        Address = self.classes.Address

        session = create_session()

        bounced = session.query(Address).filter(Address.bounces > 0).first()
        assert bounced.bounces > 0

    def test_selectfirst_by(self):
        """Query.selectfirst_by(*args, **params)

        onebounce = session.query(Address).selectfirst_by(bounces=1)

        # 0.3 magic join on *_by methods
        onebounce_user = session.query(User).selectfirst_by(bounces=1)

        """

        User, Address = self.classes.User, self.classes.Address

        session = create_session()

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

        User = self.classes.User

        session = create_session()

        ed = session.query(User).filter(User.name == 'jack').one()

    def test_selectone_by(self):
        """Query.selectone_by

        ed = session.query(User).selectone_by(name='ed')

        # 0.3 magic join on *_by methods
        ed = session.query(User).selectone_by(email_address='ed@foo.bar')

        """

        User, Address = self.classes.User, self.classes.Address

        session = create_session()

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

        User, users_table = self.classes.User, self.tables.users_table

        session = create_session()

        users = session.query(User).from_statement(users_table.select()).all()
        assert len(users) == 4

    def test_select_text(self):
        """Query.select_text(text, **params)

        users = session.query(User).select_text('SELECT * FROM users_table')

        """

        User = self.classes.User

        session = create_session()

        users = (session.query(User).
                 from_statement(text('SELECT * FROM users_table'))).all()
        assert len(users) == 4

    def test_select_whereclause(self):
        """Query.select_whereclause(whereclause=None, params=None, **kwargs)


        users = session,query(User).select_whereclause(users.c.name=='ed')
        users = session.query(User).select_whereclause("name='ed'")

        """

        User = self.classes.User

        session = create_session()

        users = session.query(User).filter(User.name == 'ed').all()
        assert len(users) == 1 and users[0].name == 'ed'

        users = session.query(User).filter(text("name='ed'")).all()
        assert len(users) == 1 and users[0].name == 'ed'
