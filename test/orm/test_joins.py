from test.lib.testing import eq_, assert_raises, assert_raises_message
import operator
from sqlalchemy import *
from sqlalchemy import exc as sa_exc, util
from sqlalchemy.sql import compiler, table, column
from sqlalchemy.engine import default
from sqlalchemy.orm import *
from sqlalchemy.orm import attributes

from test.lib.testing import eq_

import sqlalchemy as sa
from test.lib import testing, AssertsCompiledSQL, Column, engines

from test.orm import _fixtures

from test.lib import fixtures

from sqlalchemy.orm.util import join, outerjoin, with_parent

class QueryTest(_fixtures.FixtureTest):
    run_setup_mappers = 'once'
    run_inserts = 'once'
    run_deletes = None


    @classmethod
    def setup_mappers(cls):
        Node, composite_pk_table, users, Keyword, items, Dingaling, \
            order_items, item_keywords, Item, User, dingalings, \
            Address, keywords, CompositePk, nodes, Order, orders, \
            addresses = cls.classes.Node, \
            cls.tables.composite_pk_table, cls.tables.users, \
            cls.classes.Keyword, cls.tables.items, \
            cls.classes.Dingaling, cls.tables.order_items, \
            cls.tables.item_keywords, cls.classes.Item, \
            cls.classes.User, cls.tables.dingalings, \
            cls.classes.Address, cls.tables.keywords, \
            cls.classes.CompositePk, cls.tables.nodes, \
            cls.classes.Order, cls.tables.orders, cls.tables.addresses

        mapper(User, users, properties={
            'addresses':relationship(Address, backref='user', order_by=addresses.c.id),
            'orders':relationship(Order, backref='user', order_by=orders.c.id), # o2m, m2o
        })
        mapper(Address, addresses, properties={
            'dingaling':relationship(Dingaling, uselist=False, backref="address")  #o2o
        })
        mapper(Dingaling, dingalings)
        mapper(Order, orders, properties={
            'items':relationship(Item, secondary=order_items, order_by=items.c.id),  #m2m
            'address':relationship(Address),  # m2o
        })
        mapper(Item, items, properties={
            'keywords':relationship(Keyword, secondary=item_keywords) #m2m
        })
        mapper(Keyword, keywords)

        mapper(Node, nodes, properties={
            'children':relationship(Node, 
                backref=backref('parent', remote_side=[nodes.c.id])
            )
        })

        mapper(CompositePk, composite_pk_table)

        configure_mappers()

class InheritedJoinTest(fixtures.MappedTest, AssertsCompiledSQL):
    run_setup_mappers = 'once'

    @classmethod
    def define_tables(cls, metadata):
        Table('companies', metadata,
           Column('company_id', Integer, primary_key=True, test_needs_autoincrement=True),
           Column('name', String(50)))

        Table('people', metadata,
           Column('person_id', Integer, primary_key=True, test_needs_autoincrement=True),
           Column('company_id', Integer, ForeignKey('companies.company_id')),
           Column('name', String(50)),
           Column('type', String(30)))

        Table('engineers', metadata,
           Column('person_id', Integer, ForeignKey('people.person_id'), primary_key=True),
           Column('status', String(30)),
           Column('engineer_name', String(50)),
           Column('primary_language', String(50)),
          )

        Table('machines', metadata,
            Column('machine_id', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('name', String(50)),
            Column('engineer_id', Integer, ForeignKey('engineers.person_id')))

        Table('managers', metadata,
           Column('person_id', Integer, ForeignKey('people.person_id'), primary_key=True),
           Column('status', String(30)),
           Column('manager_name', String(50))
           )

        Table('boss', metadata,
            Column('boss_id', Integer, ForeignKey('managers.person_id'), primary_key=True),
            Column('golf_swing', String(30)),
            )

        Table('paperwork', metadata,
            Column('paperwork_id', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('description', String(50)),
            Column('person_id', Integer, ForeignKey('people.person_id')))

    @classmethod
    def setup_classes(cls):
        paperwork, people, companies, boss, managers, machines, engineers = (cls.tables.paperwork,
                                cls.tables.people,
                                cls.tables.companies,
                                cls.tables.boss,
                                cls.tables.managers,
                                cls.tables.machines,
                                cls.tables.engineers)

        class Company(cls.Comparable):
            pass
        class Person(cls.Comparable):
            pass
        class Engineer(Person):
            pass
        class Manager(Person):
            pass
        class Boss(Manager):
            pass
        class Machine(cls.Comparable):
            pass
        class Paperwork(cls.Comparable):
            pass

        mapper(Company, companies, properties={
            'employees':relationship(Person, order_by=people.c.person_id)
        })

        mapper(Machine, machines)

        mapper(Person, people, 
            polymorphic_on=people.c.type, 
            polymorphic_identity='person', 
            order_by=people.c.person_id, 
            properties={
                'paperwork':relationship(Paperwork, order_by=paperwork.c.paperwork_id)
            })
        mapper(Engineer, engineers, inherits=Person, polymorphic_identity='engineer', properties={
                'machines':relationship(Machine, order_by=machines.c.machine_id)
            })
        mapper(Manager, managers, 
                    inherits=Person, polymorphic_identity='manager')
        mapper(Boss, boss, inherits=Manager, polymorphic_identity='boss')
        mapper(Paperwork, paperwork)

    def test_single_prop(self):
        Company = self.classes.Company

        sess = create_session()

        self.assert_compile(
            sess.query(Company).join(Company.employees),
            "SELECT companies.company_id AS companies_company_id, companies.name AS companies_name "
            "FROM companies JOIN people ON companies.company_id = people.company_id"
            , use_default_dialect = True
        )

    def test_force_via_select_from(self):
        Company, Engineer = self.classes.Company, self.classes.Engineer

        sess = create_session()

        self.assert_compile(
            sess.query(Company).\
                filter(Company.company_id==Engineer.company_id).\
                filter(Engineer.primary_language=='java'),
            "SELECT companies.company_id AS companies_company_id, companies.name AS companies_name "
            "FROM companies, people, engineers "
            "WHERE companies.company_id = people.company_id AND engineers.primary_language "
            "= :primary_language_1",
            use_default_dialect=True
        )

        self.assert_compile(
            sess.query(Company).select_from(Company, Engineer).\
                filter(Company.company_id==Engineer.company_id).\
                filter(Engineer.primary_language=='java'),
            "SELECT companies.company_id AS companies_company_id, companies.name AS companies_name "
            "FROM companies, people JOIN engineers ON people.person_id = engineers.person_id "
            "WHERE companies.company_id = people.company_id AND engineers.primary_language ="
            " :primary_language_1",
            use_default_dialect=True

        )

    def test_single_prop_of_type(self):
        Company, Engineer = self.classes.Company, self.classes.Engineer

        sess = create_session()

        self.assert_compile(
            sess.query(Company).join(Company.employees.of_type(Engineer)),
            "SELECT companies.company_id AS companies_company_id, companies.name AS companies_name "
            "FROM companies JOIN (SELECT people.person_id AS people_person_id, "
            "people.company_id AS people_company_id, people.name AS people_name, "
            "people.type AS people_type, engineers.person_id AS "
            "engineers_person_id, engineers.status AS engineers_status, "
            "engineers.engineer_name AS engineers_engineer_name, "
            "engineers.primary_language AS engineers_primary_language "
            "FROM people JOIN engineers ON people.person_id = engineers.person_id) AS "
            "anon_1 ON companies.company_id = anon_1.people_company_id"
            , use_default_dialect = True
        )

    def test_prop_with_polymorphic(self):
        Person, Manager, Paperwork = (self.classes.Person,
                                self.classes.Manager,
                                self.classes.Paperwork)

        sess = create_session()

        self.assert_compile(
            sess.query(Person).with_polymorphic(Manager).
                    join('paperwork').filter(Paperwork.description.like('%review%')),
                "SELECT people.person_id AS people_person_id, people.company_id AS"
                " people_company_id, "
                "people.name AS people_name, people.type AS people_type, managers.person_id "
                "AS managers_person_id, "
                "managers.status AS managers_status, managers.manager_name AS "
                "managers_manager_name FROM people "
                "LEFT OUTER JOIN managers ON people.person_id = managers.person_id JOIN "
                "paperwork ON people.person_id = "
                "paperwork.person_id WHERE paperwork.description LIKE :description_1 "
                "ORDER BY people.person_id"
                , use_default_dialect=True
            )

        self.assert_compile(
            sess.query(Person).with_polymorphic(Manager).
                    join('paperwork', aliased=True).
                    filter(Paperwork.description.like('%review%')),
            "SELECT people.person_id AS people_person_id, people.company_id AS people_company_id, "
            "people.name AS people_name, people.type AS people_type, managers.person_id "
            "AS managers_person_id, "
            "managers.status AS managers_status, managers.manager_name AS managers_manager_name "
            "FROM people LEFT OUTER JOIN managers ON people.person_id = managers.person_id JOIN "
            "paperwork AS paperwork_1 ON people.person_id = paperwork_1.person_id "
            "WHERE paperwork_1.description LIKE :description_1 ORDER BY people.person_id"
            , use_default_dialect=True
        )

    def test_explicit_polymorphic_join(self):
        Company, Engineer = self.classes.Company, self.classes.Engineer

        sess = create_session()

        self.assert_compile(
            sess.query(Company).join(Engineer).filter(Engineer.engineer_name=='vlad'),
            "SELECT companies.company_id AS companies_company_id, companies.name AS "
            "companies_name "
            "FROM companies JOIN (SELECT people.person_id AS people_person_id, "
            "people.company_id AS "
            "people_company_id, people.name AS people_name, people.type AS people_type,"
            " engineers.person_id AS "
            "engineers_person_id, engineers.status AS engineers_status, "
            "engineers.engineer_name AS engineers_engineer_name, "
            "engineers.primary_language AS engineers_primary_language "
            "FROM people JOIN engineers ON people.person_id = engineers.person_id) "
            "AS anon_1 ON "
            "companies.company_id = anon_1.people_company_id "
            "WHERE anon_1.engineers_engineer_name = :engineer_name_1"
            , use_default_dialect=True
        )
        self.assert_compile(
            sess.query(Company).join(Engineer, Company.company_id==Engineer.company_id).
                    filter(Engineer.engineer_name=='vlad'),
            "SELECT companies.company_id AS companies_company_id, companies.name "
            "AS companies_name "
            "FROM companies JOIN (SELECT people.person_id AS people_person_id, "
            "people.company_id AS "
            "people_company_id, people.name AS people_name, people.type AS "
            "people_type, engineers.person_id AS "
            "engineers_person_id, engineers.status AS engineers_status, "
            "engineers.engineer_name AS engineers_engineer_name, "
            "engineers.primary_language AS engineers_primary_language "
            "FROM people JOIN engineers ON people.person_id = engineers.person_id) AS "
            "anon_1 ON "
            "companies.company_id = anon_1.people_company_id "
            "WHERE anon_1.engineers_engineer_name = :engineer_name_1"
            , use_default_dialect=True
        )

    def test_multiple_adaption(self):
        """test that multiple filter() adapters get chained together "
        and work correctly within a multiple-entry join()."""

        people, Company, Machine, engineers, machines, Engineer = (self.tables.people,
                                self.classes.Company,
                                self.classes.Machine,
                                self.tables.engineers,
                                self.tables.machines,
                                self.classes.Engineer)


        sess = create_session()

        self.assert_compile(
            sess.query(Company).join(people.join(engineers), Company.employees).
                filter(Engineer.name=='dilbert'),
            "SELECT companies.company_id AS companies_company_id, companies.name AS "
            "companies_name "
            "FROM companies JOIN (SELECT people.person_id AS people_person_id, "
            "people.company_id AS "
            "people_company_id, people.name AS people_name, people.type AS "
            "people_type, engineers.person_id "
            "AS engineers_person_id, engineers.status AS engineers_status, "
            "engineers.engineer_name AS engineers_engineer_name, "
            "engineers.primary_language AS engineers_primary_language FROM people "
            "JOIN engineers ON people.person_id = "
            "engineers.person_id) AS anon_1 ON companies.company_id = "
            "anon_1.people_company_id WHERE anon_1.people_name = :name_1"
            , use_default_dialect = True
        )

        mach_alias = machines.select()
        self.assert_compile(
            sess.query(Company).join(people.join(engineers), Company.employees).
                                join(mach_alias, Engineer.machines, from_joinpoint=True).
                filter(Engineer.name=='dilbert').filter(Machine.name=='foo'),
            "SELECT companies.company_id AS companies_company_id, companies.name AS "
            "companies_name "
            "FROM companies JOIN (SELECT people.person_id AS people_person_id, "
            "people.company_id AS "
            "people_company_id, people.name AS people_name, people.type AS people_type,"
            " engineers.person_id "
            "AS engineers_person_id, engineers.status AS engineers_status, "
            "engineers.engineer_name AS engineers_engineer_name, "
            "engineers.primary_language AS engineers_primary_language FROM people "
            "JOIN engineers ON people.person_id = "
            "engineers.person_id) AS anon_1 ON companies.company_id = "
            "anon_1.people_company_id JOIN "
            "(SELECT machines.machine_id AS machine_id, machines.name AS name, "
            "machines.engineer_id AS engineer_id "
            "FROM machines) AS anon_2 ON anon_1.engineers_person_id = anon_2.engineer_id "
            "WHERE anon_1.people_name = :name_1 AND anon_2.name = :name_2"
            , use_default_dialect = True
        )




class JoinTest(QueryTest, AssertsCompiledSQL):
    __dialect__ = 'default'

    def test_single_name(self):
        User = self.classes.User

        sess = create_session()

        self.assert_compile(
            sess.query(User).join("orders"),
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users JOIN orders ON users.id = orders.user_id"
        )

        assert_raises(
            sa_exc.InvalidRequestError,
            sess.query(User).join, "user",
        )

        self.assert_compile(
            sess.query(User).join("orders", "items"),
            "SELECT users.id AS users_id, users.name AS users_name FROM users "
            "JOIN orders ON users.id = orders.user_id JOIN order_items AS order_items_1 "
            "ON orders.id = order_items_1.order_id JOIN items ON items.id = order_items_1.item_id"
        )

        # test overlapping paths.   User->orders is used by both joins, but rendered once.
        self.assert_compile(
            sess.query(User).join("orders", "items").join("orders", "address"),
            "SELECT users.id AS users_id, users.name AS users_name FROM users JOIN orders "
            "ON users.id = orders.user_id JOIN order_items AS order_items_1 ON orders.id = "
            "order_items_1.order_id JOIN items ON items.id = order_items_1.item_id JOIN addresses "
            "ON addresses.id = orders.address_id"
        )

    def test_join_on_synonym(self):

        class User(object):
            pass
        class Address(object):
            pass
        users, addresses = (self.tables.users, self.tables.addresses)
        mapper(User, users, properties={
            'addresses':relationship(Address),
            'ad_syn':synonym("addresses")
        })
        mapper(Address, addresses)
        self.assert_compile(
            Session().query(User).join(User.ad_syn),
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users JOIN addresses ON users.id = addresses.user_id"
        )

    def test_multi_tuple_form(self):
        """test the 'tuple' form of join, now superseded 
        by the two-element join() form.

        Not deprecating this style as of yet.

        """

        Item, Order, User = (self.classes.Item,
                                self.classes.Order,
                                self.classes.User)


        sess = create_session()

        #assert_raises(
        #    sa.exc.SADeprecationWarning,
        #    sess.query(User).join, (Order, User.id==Order.user_id)
        #)

        self.assert_compile(
            sess.query(User).join((Order, User.id==Order.user_id)),
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users JOIN orders ON users.id = orders.user_id",
        )

        self.assert_compile(
            sess.query(User).join(
                                (Order, User.id==Order.user_id), 
                                (Item, Order.items)),
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users JOIN orders ON users.id = orders.user_id "
            "JOIN order_items AS order_items_1 ON orders.id = "
            "order_items_1.order_id JOIN items ON items.id = "
            "order_items_1.item_id",
        )

        # the old "backwards" form
        self.assert_compile(
            sess.query(User).join(("orders", Order)),
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users JOIN orders ON users.id = orders.user_id",
        )

    def test_single_prop(self):
        Item, Order, User, Address = (self.classes.Item,
                                self.classes.Order,
                                self.classes.User,
                                self.classes.Address)

        sess = create_session()
        self.assert_compile(
            sess.query(User).join(User.orders),
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users JOIN orders ON users.id = orders.user_id"
        )

        self.assert_compile(
            sess.query(User).join(Order.user),
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM orders JOIN users ON users.id = orders.user_id"
        )

        oalias1 = aliased(Order)
        oalias2 = aliased(Order)

        self.assert_compile(
            sess.query(User).join(oalias1.user),
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM orders AS orders_1 JOIN users ON users.id = orders_1.user_id"
        )

        # another nonsensical query.  (from [ticket:1537]).
        # in this case, the contract of "left to right" is honored
        self.assert_compile(
            sess.query(User).join(oalias1.user).join(oalias2.user),
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM orders AS orders_1 JOIN users ON users.id = orders_1.user_id, "
            "orders AS orders_2 JOIN users ON users.id = orders_2.user_id"
        )

        self.assert_compile(
            sess.query(User).join(User.orders, Order.items),
            "SELECT users.id AS users_id, users.name AS users_name FROM users "
            "JOIN orders ON users.id = orders.user_id JOIN order_items AS order_items_1 "
            "ON orders.id = order_items_1.order_id JOIN items ON items.id = order_items_1.item_id"
        )

        ualias = aliased(User)
        self.assert_compile(
            sess.query(ualias).join(ualias.orders),
            "SELECT users_1.id AS users_1_id, users_1.name AS users_1_name "
            "FROM users AS users_1 JOIN orders ON users_1.id = orders.user_id"
        )

        # this query is somewhat nonsensical.  the old system didn't render a correct
        # query for this.   In this case its the most faithful to what was asked -
        # there's no linkage between User.orders and "oalias", so two FROM elements
        # are generated.
        oalias = aliased(Order)
        self.assert_compile(
            sess.query(User).join(User.orders, oalias.items),
            "SELECT users.id AS users_id, users.name AS users_name FROM users "
            "JOIN orders ON users.id = orders.user_id, "
            "orders AS orders_1 JOIN order_items AS order_items_1 ON orders_1.id = order_items_1.order_id "
            "JOIN items ON items.id = order_items_1.item_id"
        )

        # same as before using an aliased() for User as well
        ualias = aliased(User)
        self.assert_compile(
            sess.query(ualias).join(ualias.orders, oalias.items),
            "SELECT users_1.id AS users_1_id, users_1.name AS users_1_name FROM users AS users_1 "
            "JOIN orders ON users_1.id = orders.user_id, "
            "orders AS orders_1 JOIN order_items AS order_items_1 ON orders_1.id = order_items_1.order_id "
            "JOIN items ON items.id = order_items_1.item_id"
        )

        self.assert_compile(
            sess.query(User).filter(User.name=='ed').from_self().join(User.orders),
            "SELECT anon_1.users_id AS anon_1_users_id, anon_1.users_name AS anon_1_users_name "
            "FROM (SELECT users.id AS users_id, users.name AS users_name "
            "FROM users "
            "WHERE users.name = :name_1) AS anon_1 JOIN orders ON anon_1.users_id = orders.user_id"
        )

        self.assert_compile(
            sess.query(User).join(User.addresses, aliased=True).filter(Address.email_address=='foo'),
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users JOIN addresses AS addresses_1 ON users.id = addresses_1.user_id "
            "WHERE addresses_1.email_address = :email_address_1"
        )

        self.assert_compile(
            sess.query(User).join(User.orders, Order.items, aliased=True).filter(Item.id==10),
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users JOIN orders AS orders_1 ON users.id = orders_1.user_id "
            "JOIN order_items AS order_items_1 ON orders_1.id = order_items_1.order_id "
            "JOIN items AS items_1 ON items_1.id = order_items_1.item_id "
            "WHERE items_1.id = :id_1"
        )

        # test #1 for [ticket:1706]
        ualias = aliased(User)
        self.assert_compile(
            sess.query(ualias).
                    join(oalias1, ualias.orders).\
                    join(Address, ualias.addresses),
            "SELECT users_1.id AS users_1_id, users_1.name AS "
            "users_1_name FROM users AS users_1 JOIN orders AS orders_1 "
            "ON users_1.id = orders_1.user_id JOIN addresses ON users_1.id "
            "= addresses.user_id"
        )

        # test #2 for [ticket:1706]
        ualias2 = aliased(User)
        self.assert_compile(
            sess.query(ualias).
                    join(Address, ualias.addresses).
                    join(ualias2, Address.user).
                    join(Order, ualias.orders),
            "SELECT users_1.id AS users_1_id, users_1.name AS users_1_name FROM users "
            "AS users_1 JOIN addresses ON users_1.id = addresses.user_id JOIN users AS users_2 "
            "ON users_2.id = addresses.user_id JOIN orders ON users_1.id = orders.user_id"
        )

    def test_overlapping_paths(self):
        User = self.classes.User

        for aliased in (True,False):
            # load a user who has an order that contains item id 3 and address id 1 (order 3, owned by jack)
            result = create_session().query(User).join('orders', 'items', aliased=aliased).\
                    filter_by(id=3).join('orders','address', aliased=aliased).filter_by(id=1).all()
            assert [User(id=7, name='jack')] == result

    def test_overlapping_paths_multilevel(self):
        User = self.classes.User

        s = Session()
        q = s.query(User).\
                    join('orders').\
                    join('addresses').\
                    join('orders', 'items').\
                    join('addresses', 'dingaling')
        self.assert_compile(
            q,
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users JOIN orders ON users.id = orders.user_id "
            "JOIN addresses ON users.id = addresses.user_id "
            "JOIN order_items AS order_items_1 ON orders.id = "
            "order_items_1.order_id "
            "JOIN items ON items.id = order_items_1.item_id "
            "JOIN dingalings ON addresses.id = dingalings.address_id"

        )

    def test_overlapping_paths_outerjoin(self):
        User = self.classes.User

        result = create_session().query(User).outerjoin('orders', 'items').\
                filter_by(id=3).outerjoin('orders','address').filter_by(id=1).all()
        assert [User(id=7, name='jack')] == result

    def test_from_joinpoint(self):
        Item, User, Order = (self.classes.Item,
                                self.classes.User,
                                self.classes.Order)

        sess = create_session()

        for oalias,ialias in [(True, True), (False, False), (True, False), (False, True)]:
            eq_(
                sess.query(User).join('orders', aliased=oalias).\
                                join('items', 
                                        from_joinpoint=True, 
                                        aliased=ialias).\
                                filter(Item.description == 'item 4').all(),
                [User(name='jack')]
            )

            # use middle criterion
            eq_(
                sess.query(User).join('orders', aliased=oalias).\
                                filter(Order.user_id==9).\
                                join('items', from_joinpoint=True, 
                                            aliased=ialias).\
                                filter(Item.description=='item 4').all(),
                []
            )

        orderalias = aliased(Order)
        itemalias = aliased(Item)
        eq_(
            sess.query(User).join(orderalias, 'orders'). 
                                join(itemalias, 'items', from_joinpoint=True).
                                filter(itemalias.description == 'item 4').all(),
            [User(name='jack')]
        )
        eq_(
            sess.query(User).join(orderalias, 'orders').
                                join(itemalias, 'items', from_joinpoint=True).
                                filter(orderalias.user_id==9).\
                                filter(itemalias.description=='item 4').all(),
            []
        )

    def test_join_nonmapped_column(self):
        """test that the search for a 'left' doesn't trip on non-mapped cols"""

        Order, User = self.classes.Order, self.classes.User

        sess = create_session()

        # intentionally join() with a non-existent "left" side
        self.assert_compile(
            sess.query(User.id, literal_column('foo')).join(Order.user),
            "SELECT users.id AS users_id, foo FROM "
            "orders JOIN users ON users.id = orders.user_id"
        )

    def test_backwards_join(self):
        User, Address = self.classes.User, self.classes.Address

        # a more controversial feature.  join from
        # User->Address, but the onclause is Address.user.

        sess = create_session()

        eq_(
            sess.query(User).join(Address.user).\
                            filter(Address.email_address=='ed@wood.com').all(),
            [User(id=8,name=u'ed')]
        )

        # its actually not so controversial if you view it in terms
        # of multiple entities.
        eq_(
            sess.query(User, Address).join(Address.user).filter(Address.email_address=='ed@wood.com').all(),
            [(User(id=8,name=u'ed'), Address(email_address='ed@wood.com'))]
        )

        # this was the controversial part.  now, raise an error if the feature is abused.
        # before the error raise was added, this would silently work.....
        assert_raises(
            sa_exc.InvalidRequestError,
            sess.query(User).join, Address, Address.user,
        )

        # but this one would silently fail 
        adalias = aliased(Address)
        assert_raises(
            sa_exc.InvalidRequestError,
            sess.query(User).join, adalias, Address.user,
        )

    def test_multiple_with_aliases(self):
        Order, User = self.classes.Order, self.classes.User

        sess = create_session()

        ualias = aliased(User)
        oalias1 = aliased(Order)
        oalias2 = aliased(Order)
        self.assert_compile(
            sess.query(ualias).join(oalias1, ualias.orders).
                                join(oalias2, ualias.orders).
                    filter(or_(oalias1.user_id==9, oalias2.user_id==7)),
            "SELECT users_1.id AS users_1_id, users_1.name AS users_1_name FROM users AS users_1 "
            "JOIN orders AS orders_1 ON users_1.id = orders_1.user_id JOIN orders AS orders_2 ON "
            "users_1.id = orders_2.user_id WHERE orders_1.user_id = :user_id_1 OR orders_2.user_id = :user_id_2",
            use_default_dialect=True
        )

    def test_select_from_orm_joins(self):
        User, Order = self.classes.User, self.classes.Order

        sess = create_session()

        ualias = aliased(User)
        oalias1 = aliased(Order)
        oalias2 = aliased(Order)

        self.assert_compile(
            join(User, oalias2, User.id==oalias2.user_id),
            "users JOIN orders AS orders_1 ON users.id = orders_1.user_id",
            use_default_dialect=True
        )

        self.assert_compile(
            join(ualias, oalias1, ualias.orders),
            "users AS users_1 JOIN orders AS orders_1 ON users_1.id = orders_1.user_id",
            use_default_dialect=True
        )

        self.assert_compile(
            sess.query(ualias).select_from(join(ualias, oalias1, ualias.orders)),
            "SELECT users_1.id AS users_1_id, users_1.name AS users_1_name FROM users AS users_1 "
            "JOIN orders AS orders_1 ON users_1.id = orders_1.user_id",
            use_default_dialect=True
        )

        self.assert_compile(
            sess.query(User, ualias).select_from(join(ualias, oalias1, ualias.orders)),
            "SELECT users.id AS users_id, users.name AS users_name, users_1.id AS users_1_id, "
            "users_1.name AS users_1_name FROM users, users AS users_1 JOIN orders AS orders_1 ON users_1.id = orders_1.user_id",
            use_default_dialect=True
        )

        # this fails (and we cant quite fix right now).
        if False:
            self.assert_compile(
                sess.query(User, ualias).\
                        join(oalias1, ualias.orders).\
                        join(oalias2, User.id==oalias2.user_id).\
                        filter(or_(oalias1.user_id==9, oalias2.user_id==7)),
                "SELECT users.id AS users_id, users.name AS users_name, users_1.id AS users_1_id, users_1.name AS "
                "users_1_name FROM users JOIN orders AS orders_2 ON users.id = orders_2.user_id, "
                "users AS users_1 JOIN orders AS orders_1 ON users_1.id = orders_1.user_id  "
                "WHERE orders_1.user_id = :user_id_1 OR orders_2.user_id = :user_id_2",
                use_default_dialect=True
            )

        # this is the same thing using explicit orm.join() (which now offers multiple again)
        self.assert_compile(
            sess.query(User, ualias).\
                    select_from(
                        join(ualias, oalias1, ualias.orders),
                        join(User, oalias2, User.id==oalias2.user_id),
                    ).\
                    filter(or_(oalias1.user_id==9, oalias2.user_id==7)),
            "SELECT users.id AS users_id, users.name AS users_name, users_1.id AS users_1_id, users_1.name AS "
            "users_1_name FROM users AS users_1 JOIN orders AS orders_1 ON users_1.id = orders_1.user_id, "
            "users JOIN orders AS orders_2 ON users.id = orders_2.user_id "
            "WHERE orders_1.user_id = :user_id_1 OR orders_2.user_id = :user_id_2",

            use_default_dialect=True
        )


    def test_overlapping_backwards_joins(self):
        User, Order = self.classes.User, self.classes.Order

        sess = create_session()

        oalias1 = aliased(Order)
        oalias2 = aliased(Order)

        # this is invalid SQL - joins from orders_1/orders_2 to User twice.
        # but that is what was asked for so they get it !
        self.assert_compile(
            sess.query(User).join(oalias1.user).join(oalias2.user),
            "SELECT users.id AS users_id, users.name AS users_name FROM orders AS orders_1 "
            "JOIN users ON users.id = orders_1.user_id, orders AS orders_2 JOIN users ON users.id = orders_2.user_id",
            use_default_dialect=True,
        )

    def test_replace_multiple_from_clause(self):
        """test adding joins onto multiple FROM clauses"""

        User, Order, Address = (self.classes.User,
                                self.classes.Order,
                                self.classes.Address)


        sess = create_session()

        self.assert_compile(
            sess.query(Address, User).join(Address.dingaling).join(User.orders, Order.items),
            "SELECT addresses.id AS addresses_id, addresses.user_id AS addresses_user_id, "
            "addresses.email_address AS addresses_email_address, users.id AS users_id, "
            "users.name AS users_name FROM addresses JOIN dingalings ON addresses.id = dingalings.address_id, "
            "users JOIN orders ON users.id = orders.user_id JOIN order_items AS order_items_1 "
            "ON orders.id = order_items_1.order_id JOIN items ON items.id = order_items_1.item_id",
            use_default_dialect = True
        )

    def test_multiple_adaption(self):
        Item, Order, User = (self.classes.Item,
                                self.classes.Order,
                                self.classes.User)

        sess = create_session()

        self.assert_compile(
            sess.query(User).join(User.orders, Order.items, aliased=True).filter(Order.id==7).filter(Item.id==8),
            "SELECT users.id AS users_id, users.name AS users_name FROM users JOIN orders AS orders_1 "
            "ON users.id = orders_1.user_id JOIN order_items AS order_items_1 ON orders_1.id = order_items_1.order_id "
            "JOIN items AS items_1 ON items_1.id = order_items_1.item_id WHERE orders_1.id = :id_1 AND items_1.id = :id_2",
            use_default_dialect=True
        )

    def test_onclause_conditional_adaption(self):
        Item, Order, orders, order_items, User = (self.classes.Item,
                                self.classes.Order,
                                self.tables.orders,
                                self.tables.order_items,
                                self.classes.User)

        sess = create_session()

        # this is now a very weird test, nobody should really
        # be using the aliased flag in this way.
        self.assert_compile(
            sess.query(User).join(User.orders, aliased=True).
                join(Item, 
                    and_(Order.id==order_items.c.order_id, order_items.c.item_id==Item.id),
                    from_joinpoint=True, aliased=True
                ),
            "SELECT users.id AS users_id, users.name AS users_name FROM users JOIN "
            "orders AS orders_1 ON users.id = orders_1.user_id JOIN items AS items_1 "
            "ON orders_1.id = order_items.order_id AND order_items.item_id = items_1.id",
            use_default_dialect=True
        )


        oalias = orders.select()
        self.assert_compile(
            sess.query(User).join(oalias, User.orders).
                join(Item, 
                    and_(Order.id==order_items.c.order_id, order_items.c.item_id==Item.id),
                    from_joinpoint=True
                ),
            "SELECT users.id AS users_id, users.name AS users_name FROM users JOIN "
            "(SELECT orders.id AS id, orders.user_id AS user_id, orders.address_id AS address_id, orders.description "
            "AS description, orders.isopen AS isopen FROM orders) AS anon_1 ON users.id = anon_1.user_id JOIN items "
            "ON anon_1.id = order_items.order_id AND order_items.item_id = items.id",
            use_default_dialect=True
        )

        # query.join(<stuff>, aliased=True).join(target, sql_expression)
        # or: query.join(path_to_some_joined_table_mapper).join(target, sql_expression)

    def test_pure_expression_error(self):
        addresses, users = self.tables.addresses, self.tables.users

        sess = create_session()

        self.assert_compile(
            sess.query(users).join(addresses),
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users JOIN addresses ON users.id = addresses.user_id"
        )


    def test_orderby_arg_bug(self):
        User, users, Order = (self.classes.User,
                                self.tables.users,
                                self.classes.Order)

        sess = create_session()
        # no arg error
        result = sess.query(User).join('orders', aliased=True).order_by(Order.id).reset_joinpoint().order_by(users.c.id).all()

    def test_no_onclause(self):
        Item, User, Order = (self.classes.Item,
                                self.classes.User,
                                self.classes.Order)

        sess = create_session()

        eq_(
            sess.query(User).select_from(join(User, Order).join(Item, Order.items)).filter(Item.description == 'item 4').all(),
            [User(name='jack')]
        )

        eq_(
            sess.query(User.name).select_from(join(User, Order).join(Item, Order.items)).filter(Item.description == 'item 4').all(),
            [('jack',)]
        )

        eq_(
            sess.query(User).join(Order).join(Item, Order.items)
                            .filter(Item.description == 'item 4').all(),
            [User(name='jack')]
        )

    def test_clause_onclause(self):
        Item, Order, users, order_items, User = (self.classes.Item,
                                self.classes.Order,
                                self.tables.users,
                                self.tables.order_items,
                                self.classes.User)

        sess = create_session()

        eq_(
            sess.query(User).join(Order, User.id==Order.user_id).
                            join(order_items, Order.id==order_items.c.order_id).
                            join(Item, order_items.c.item_id==Item.id).
                            filter(Item.description == 'item 4').all(),
            [User(name='jack')]
        )

        eq_(
            sess.query(User.name).join(Order, User.id==Order.user_id). 
                                join(order_items, Order.id==order_items.c.order_id).
                                join(Item, order_items.c.item_id==Item.id).
                                filter(Item.description == 'item 4').all(),
            [('jack',)]
        )

        ualias = aliased(User)
        eq_(
            sess.query(ualias.name).join(Order, ualias.id==Order.user_id).
                                    join(order_items, Order.id==order_items.c.order_id).
                                    join(Item, order_items.c.item_id==Item.id).
                                    filter(Item.description == 'item 4').all(),
            [('jack',)]
        )

        # explicit onclause with from_self(), means
        # the onclause must be aliased against the query's custom
        # FROM object
        eq_(
            sess.query(User).order_by(User.id).offset(2).
                            from_self().
                            join(Order, User.id==Order.user_id).
                            all(),
            [User(name='fred')]
        )

        # same with an explicit select_from()
        eq_(
            sess.query(User).select_from(select([users]).
                                order_by(User.id).offset(2).alias()).
                                join(Order, User.id==Order.user_id).
                                all(),
            [User(name='fred')]
        )


    def test_aliased_classes(self):
        User, Address = self.classes.User, self.classes.Address

        sess = create_session()

        (user7, user8, user9, user10) = sess.query(User).all()
        (address1, address2, address3, address4, address5) = sess.query(Address).all()
        expected = [(user7, address1),
            (user8, address2),
            (user8, address3),
            (user8, address4),
            (user9, address5),
            (user10, None)]

        q = sess.query(User)
        AdAlias = aliased(Address)
        q = q.add_entity(AdAlias).select_from(outerjoin(User, AdAlias))
        l = q.order_by(User.id, AdAlias.id).all()
        eq_(l, expected)

        sess.expunge_all()

        q = sess.query(User).add_entity(AdAlias)
        l = q.select_from(outerjoin(User, AdAlias)).filter(AdAlias.email_address=='ed@bettyboop.com').all()
        eq_(l, [(user8, address3)])

        l = q.select_from(outerjoin(User, AdAlias, 'addresses')).filter(AdAlias.email_address=='ed@bettyboop.com').all()
        eq_(l, [(user8, address3)])

        l = q.select_from(outerjoin(User, AdAlias, User.id==AdAlias.user_id)).filter(AdAlias.email_address=='ed@bettyboop.com').all()
        eq_(l, [(user8, address3)])

        # this is the first test where we are joining "backwards" - from AdAlias to User even though
        # the query is against User
        q = sess.query(User, AdAlias)
        l = q.join(AdAlias.user).filter(User.name=='ed').order_by(User.id, AdAlias.id)
        eq_(l.all(), [(user8, address2),(user8, address3),(user8, address4),])

        q = sess.query(User, AdAlias).select_from(join(AdAlias, User, AdAlias.user)).filter(User.name=='ed')
        eq_(l.all(), [(user8, address2),(user8, address3),(user8, address4),])

    def test_expression_onclauses(self):
        Order, User = self.classes.Order, self.classes.User

        sess = create_session()

        subq = sess.query(User).subquery()

        self.assert_compile(
            sess.query(User).join(subq, User.name==subq.c.name),
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users JOIN (SELECT users.id AS id, users.name "
            "AS name FROM users) AS anon_1 ON users.name = anon_1.name",
            use_default_dialect=True
        )


        subq = sess.query(Order).subquery()
        self.assert_compile(
            sess.query(User).join(subq, User.id==subq.c.user_id),
            "SELECT users.id AS users_id, users.name AS users_name FROM "
            "users JOIN (SELECT orders.id AS id, orders.user_id AS user_id, "
            "orders.address_id AS address_id, orders.description AS "
            "description, orders.isopen AS isopen FROM orders) AS "
            "anon_1 ON users.id = anon_1.user_id",
            use_default_dialect=True
        )

        self.assert_compile(
            sess.query(User).join(Order, User.id==Order.user_id),
            "SELECT users.id AS users_id, users.name AS users_name "
            "FROM users JOIN orders ON users.id = orders.user_id",
            use_default_dialect=True
        )


    def test_implicit_joins_from_aliases(self):
        Item, User, Order = (self.classes.Item,
                                self.classes.User,
                                self.classes.Order)

        sess = create_session()
        OrderAlias = aliased(Order)

        eq_(
            sess.query(OrderAlias).join('items').filter_by(description='item 3').\
                order_by(OrderAlias.id).all(),
            [
                Order(address_id=1,description=u'order 1',isopen=0,user_id=7,id=1), 
                Order(address_id=4,description=u'order 2',isopen=0,user_id=9,id=2), 
                Order(address_id=1,description=u'order 3',isopen=1,user_id=7,id=3)
            ]
        )

        eq_(
            sess.query(User, OrderAlias, Item.description).
                        join(OrderAlias, 'orders').
                        join('items', from_joinpoint=True).
                        filter_by(description='item 3').\
                order_by(User.id, OrderAlias.id).all(),
            [
                (User(name=u'jack',id=7), Order(address_id=1,description=u'order 1',isopen=0,user_id=7,id=1), u'item 3'), 
                (User(name=u'jack',id=7), Order(address_id=1,description=u'order 3',isopen=1,user_id=7,id=3), u'item 3'), 
                (User(name=u'fred',id=9), Order(address_id=4,description=u'order 2',isopen=0,user_id=9,id=2), u'item 3')
            ]
        )

    def test_aliased_classes_m2m(self):
        Item, Order = self.classes.Item, self.classes.Order

        sess = create_session()

        (order1, order2, order3, order4, order5) = sess.query(Order).all()
        (item1, item2, item3, item4, item5) = sess.query(Item).all()
        expected = [
            (order1, item1),
            (order1, item2),
            (order1, item3),
            (order2, item1),
            (order2, item2),
            (order2, item3),
            (order3, item3),
            (order3, item4),
            (order3, item5),
            (order4, item1),
            (order4, item5),
            (order5, item5),
        ]

        q = sess.query(Order)
        q = q.add_entity(Item).select_from(join(Order, Item, 'items')).order_by(Order.id, Item.id)
        l = q.all()
        eq_(l, expected)

        IAlias = aliased(Item)
        q = sess.query(Order, IAlias).select_from(join(Order, IAlias, 'items')).filter(IAlias.description=='item 3')
        l = q.all()
        eq_(l, 
            [
                (order1, item3),
                (order2, item3),
                (order3, item3),
            ]
        )

    def test_joins_from_adapted_entities(self):
        User = self.classes.User


        # test for #1853

        session = create_session()
        first = session.query(User)
        second = session.query(User)
        unioned = first.union(second)
        subquery = session.query(User.id).subquery()
        join = subquery, subquery.c.id == User.id
        joined = unioned.outerjoin(*join)
        self.assert_compile(joined,
                            'SELECT anon_1.users_id AS '
                            'anon_1_users_id, anon_1.users_name AS '
                            'anon_1_users_name FROM (SELECT users.id '
                            'AS users_id, users.name AS users_name '
                            'FROM users UNION SELECT users.id AS '
                            'users_id, users.name AS users_name FROM '
                            'users) AS anon_1 LEFT OUTER JOIN (SELECT '
                            'users.id AS id FROM users) AS anon_2 ON '
                            'anon_2.id = anon_1.users_id',
                            use_default_dialect=True)

        first = session.query(User.id)
        second = session.query(User.id)
        unioned = first.union(second)
        subquery = session.query(User.id).subquery()
        join = subquery, subquery.c.id == User.id
        joined = unioned.outerjoin(*join)
        self.assert_compile(joined,
                            'SELECT anon_1.users_id AS anon_1_users_id '
                            'FROM (SELECT users.id AS users_id FROM '
                            'users UNION SELECT users.id AS users_id '
                            'FROM users) AS anon_1 LEFT OUTER JOIN '
                            '(SELECT users.id AS id FROM users) AS '
                            'anon_2 ON anon_2.id = anon_1.users_id',
                            use_default_dialect=True)

    def test_reset_joinpoint(self):
        User = self.classes.User

        for aliased in (True, False):
            # load a user who has an order that contains item id 3 and address id 1 (order 3, owned by jack)
            result = create_session().query(User).join('orders', 'items', aliased=aliased).filter_by(id=3).reset_joinpoint().join('orders','address', aliased=aliased).filter_by(id=1).all()
            assert [User(id=7, name='jack')] == result

            result = create_session().query(User).outerjoin('orders', 'items', aliased=aliased).filter_by(id=3).reset_joinpoint().outerjoin('orders','address', aliased=aliased).filter_by(id=1).all()
            assert [User(id=7, name='jack')] == result

    def test_overlap_with_aliases(self):
        orders, User, users = (self.tables.orders,
                                self.classes.User,
                                self.tables.users)

        oalias = orders.alias('oalias')

        result = create_session().query(User).select_from(users.join(oalias)).filter(oalias.c.description.in_(["order 1", "order 2", "order 3"])).join('orders', 'items').order_by(User.id).all()
        assert [User(id=7, name='jack'), User(id=9, name='fred')] == result

        result = create_session().query(User).select_from(users.join(oalias)).filter(oalias.c.description.in_(["order 1", "order 2", "order 3"])).join('orders', 'items').filter_by(id=4).all()
        assert [User(id=7, name='jack')] == result

    def test_aliased(self):
        """test automatic generation of aliased joins."""

        Item, Order, User, Address = (self.classes.Item,
                                self.classes.Order,
                                self.classes.User,
                                self.classes.Address)


        sess = create_session()

        # test a basic aliasized path
        q = sess.query(User).join('addresses', aliased=True).filter_by(email_address='jack@bean.com')
        assert [User(id=7)] == q.all()

        q = sess.query(User).join('addresses', aliased=True).filter(Address.email_address=='jack@bean.com')
        assert [User(id=7)] == q.all()

        q = sess.query(User).join('addresses', aliased=True).filter(or_(Address.email_address=='jack@bean.com', Address.email_address=='fred@fred.com'))
        assert [User(id=7), User(id=9)] == q.all()

        # test two aliasized paths, one to 'orders' and the other to 'orders','items'.
        # one row is returned because user 7 has order 3 and also has order 1 which has item 1
        # this tests a o2m join and a m2m join.
        q = sess.query(User).join('orders', aliased=True).filter(Order.description=="order 3").join('orders', 'items', aliased=True).filter(Item.description=="item 1")
        assert q.count() == 1
        assert [User(id=7)] == q.all()

        # test the control version - same joins but not aliased.  rows are not returned because order 3 does not have item 1
        q = sess.query(User).join('orders').filter(Order.description=="order 3").join('orders', 'items').filter(Item.description=="item 1")
        assert [] == q.all()
        assert q.count() == 0

        # the left half of the join condition of the any() is aliased.
        q = sess.query(User).join('orders', aliased=True).filter(Order.items.any(Item.description=='item 4'))
        assert [User(id=7)] == q.all()

        # test that aliasing gets reset when join() is called
        q = sess.query(User).join('orders', aliased=True).filter(Order.description=="order 3").join('orders', aliased=True).filter(Order.description=="order 5")
        assert q.count() == 1
        assert [User(id=7)] == q.all()

    def test_aliased_order_by(self):
        User = self.classes.User

        sess = create_session()

        ualias = aliased(User)
        eq_(
            sess.query(User, ualias).filter(User.id > ualias.id).order_by(desc(ualias.id), User.name).all(),
            [
                (User(id=10,name=u'chuck'), User(id=9,name=u'fred')), 
                (User(id=10,name=u'chuck'), User(id=8,name=u'ed')), 
                (User(id=9,name=u'fred'), User(id=8,name=u'ed')), 
                (User(id=10,name=u'chuck'), User(id=7,name=u'jack')), 
                (User(id=8,name=u'ed'), User(id=7,name=u'jack')),
                (User(id=9,name=u'fred'), User(id=7,name=u'jack'))
            ]
        )

    def test_plain_table(self):
        addresses, User = self.tables.addresses, self.classes.User


        sess = create_session()

        eq_(
            sess.query(User.name).join(addresses, User.id==addresses.c.user_id).order_by(User.id).all(),
            [(u'jack',), (u'ed',), (u'ed',), (u'ed',), (u'fred',)]
        )

    def test_no_joinpoint_expr(self):
        User, users = self.classes.User, self.tables.users

        sess = create_session()

        # these are consistent regardless of
        # select_from() being present.

        assert_raises_message(
            sa_exc.InvalidRequestError,
            "Could not find a FROM clause to join from.  Tried joining "
            "to .*?, but got: "
            "Can't find any foreign key relationships "
            "between 'users' and 'users'.",
            sess.query(users.c.id).join, User
        )

        assert_raises_message(
            sa_exc.InvalidRequestError,
            "Could not find a FROM clause to join from.  Tried joining "
            "to .*?, but got: "
            "Can't find any foreign key relationships "
            "between 'users' and 'users'.",
            sess.query(users.c.id).select_from(users).join, User
        )

    def test_select_from(self):
        """Test that the left edge of the join can be set reliably with select_from()."""

        Item, Order, User = (self.classes.Item,
                                self.classes.Order,
                                self.classes.User)


        sess = create_session()
        self.assert_compile(
            sess.query(Item.id).select_from(User).join(User.orders).join(Order.items),
            "SELECT items.id AS items_id FROM users JOIN orders ON "
            "users.id = orders.user_id JOIN order_items AS order_items_1 "
            "ON orders.id = order_items_1.order_id JOIN items ON items.id = "
            "order_items_1.item_id",
            use_default_dialect=True
        )

        # here, the join really wants to add a second FROM clause
        # for "Item".  but select_from disallows that
        self.assert_compile(
            sess.query(Item.id).select_from(User).join(Item, User.id==Item.id),
            "SELECT items.id AS items_id FROM users JOIN items ON users.id = items.id",
            use_default_dialect=True
        )




    def test_from_self_resets_joinpaths(self):
        """test a join from from_self() doesn't confuse joins inside the subquery
        with the outside.
        """

        Item, Keyword = self.classes.Item, self.classes.Keyword

        sess = create_session()

        self.assert_compile(
            sess.query(Item).join(Item.keywords).from_self(Keyword).join(Item.keywords),
            "SELECT keywords.id AS keywords_id, keywords.name AS keywords_name FROM "
            "(SELECT items.id AS items_id, items.description AS items_description "
            "FROM items JOIN item_keywords AS item_keywords_1 ON items.id = "
            "item_keywords_1.item_id JOIN keywords ON keywords.id = item_keywords_1.keyword_id) "
            "AS anon_1 JOIN item_keywords AS item_keywords_2 ON "
            "anon_1.items_id = item_keywords_2.item_id "
            "JOIN keywords ON "
            "keywords.id = item_keywords_2.keyword_id",
            use_default_dialect=True
        )

class JoinFromSelectableTest(fixtures.MappedTest, AssertsCompiledSQL):
    __dialect__ = 'default'
    run_setup_mappers = 'once'

    @classmethod
    def define_tables(cls, metadata):
        Table('table1', metadata, 
            Column('id', Integer, primary_key=True)
        )
        Table('table2', metadata,
            Column('id', Integer, primary_key=True),
            Column('t1_id', Integer)
        )

    @classmethod
    def setup_classes(cls):
        table1, table2 = cls.tables.table1, cls.tables.table2
        class T1(cls.Comparable):
            pass

        class T2(cls.Comparable):
            pass

        mapper(T1, table1)
        mapper(T2, table2)

    def test_select_mapped_to_mapped_explicit_left(self):
        T1, T2 = self.classes.T1, self.classes.T2

        sess = Session()
        subq = sess.query(T2.t1_id, func.count(T2.id).label('count')).\
                    group_by(T2.t1_id).subquery()

        self.assert_compile(
            sess.query(subq.c.count, T1.id).select_from(subq).join(T1, subq.c.t1_id==T1.id),
            "SELECT anon_1.count AS anon_1_count, table1.id AS table1_id "
            "FROM (SELECT table2.t1_id AS t1_id, "
            "count(table2.id) AS count FROM table2 "
            "GROUP BY table2.t1_id) AS anon_1 JOIN table1 ON anon_1.t1_id = table1.id"
        )

    def test_select_mapped_to_mapped_implicit_left(self):
        T1, T2 = self.classes.T1, self.classes.T2

        sess = Session()
        subq = sess.query(T2.t1_id, func.count(T2.id).label('count')).\
                    group_by(T2.t1_id).subquery()

        self.assert_compile(
            sess.query(subq.c.count, T1.id).join(T1, subq.c.t1_id==T1.id),
            "SELECT anon_1.count AS anon_1_count, table1.id AS table1_id "
            "FROM (SELECT table2.t1_id AS t1_id, "
            "count(table2.id) AS count FROM table2 "
            "GROUP BY table2.t1_id) AS anon_1 JOIN table1 ON anon_1.t1_id = table1.id"
        )

    def test_select_mapped_to_select_explicit_left(self):
        T1, T2 = self.classes.T1, self.classes.T2

        sess = Session()
        subq = sess.query(T2.t1_id, func.count(T2.id).label('count')).\
                    group_by(T2.t1_id).subquery()

        self.assert_compile(
            sess.query(subq.c.count, T1.id).select_from(T1).join(subq, subq.c.t1_id==T1.id),
            "SELECT anon_1.count AS anon_1_count, table1.id AS table1_id "
            "FROM table1 JOIN (SELECT table2.t1_id AS t1_id, "
            "count(table2.id) AS count FROM table2 GROUP BY table2.t1_id) "
            "AS anon_1 ON anon_1.t1_id = table1.id"
        )

    def test_select_mapped_to_select_implicit_left(self):
        T1, T2 = self.classes.T1, self.classes.T2

        sess = Session()
        subq = sess.query(T2.t1_id, func.count(T2.id).label('count')).\
                    group_by(T2.t1_id).subquery()

        assert_raises_message(
            sa_exc.InvalidRequestError,
            r"Can't construct a join from ",
            sess.query(subq.c.count, T1.id).join, subq, subq.c.t1_id==T1.id,
        )

    def test_mapped_select_to_mapped_implicit_left(self):
        T1, T2 = self.classes.T1, self.classes.T2

        sess = Session()
        subq = sess.query(T2.t1_id, func.count(T2.id).label('count')).\
                    group_by(T2.t1_id).subquery()

        # this query is wrong, but verifying behavior stays the same
        # (or improves, like an error message)
        self.assert_compile(
            sess.query(T1.id, subq.c.count).join(T1, subq.c.t1_id==T1.id),
            "SELECT table1.id AS table1_id, anon_1.count AS anon_1_count FROM "
            "(SELECT table2.t1_id AS t1_id, count(table2.id) AS count FROM "
            "table2 GROUP BY table2.t1_id) AS anon_1, table1 JOIN table1 "
            "ON anon_1.t1_id = table1.id"
        )

    def test_mapped_select_to_mapped_explicit_left(self):
        T1, T2 = self.classes.T1, self.classes.T2

        sess = Session()
        subq = sess.query(T2.t1_id, func.count(T2.id).label('count')).\
                    group_by(T2.t1_id).subquery()

        self.assert_compile(
            sess.query(T1.id, subq.c.count).select_from(subq).join(T1, subq.c.t1_id==T1.id),
            "SELECT table1.id AS table1_id, anon_1.count AS anon_1_count "
            "FROM (SELECT table2.t1_id AS t1_id, count(table2.id) AS count "
            "FROM table2 GROUP BY table2.t1_id) AS anon_1 JOIN table1 "
            "ON anon_1.t1_id = table1.id"
        )

    def test_mapped_select_to_select_explicit_left(self):
        T1, T2 = self.classes.T1, self.classes.T2

        sess = Session()
        subq = sess.query(T2.t1_id, func.count(T2.id).label('count')).\
                    group_by(T2.t1_id).subquery()

        self.assert_compile(
            sess.query(T1.id, subq.c.count).select_from(T1).join(subq, subq.c.t1_id==T1.id),
            "SELECT table1.id AS table1_id, anon_1.count AS anon_1_count "
            "FROM table1 JOIN (SELECT table2.t1_id AS t1_id, count(table2.id) AS count "
            "FROM table2 GROUP BY table2.t1_id) AS anon_1 "
            "ON anon_1.t1_id = table1.id"
        )

    def test_mapped_select_to_select_implicit_left(self):
        T1, T2 = self.classes.T1, self.classes.T2

        sess = Session()
        subq = sess.query(T2.t1_id, func.count(T2.id).label('count')).\
                    group_by(T2.t1_id).subquery()

        self.assert_compile(
            sess.query(T1.id, subq.c.count).join(subq, subq.c.t1_id==T1.id),
            "SELECT table1.id AS table1_id, anon_1.count AS anon_1_count "
            "FROM table1 JOIN (SELECT table2.t1_id AS t1_id, count(table2.id) AS count "
            "FROM table2 GROUP BY table2.t1_id) AS anon_1 "
            "ON anon_1.t1_id = table1.id"
        )

class MultiplePathTest(fixtures.MappedTest, AssertsCompiledSQL):
    @classmethod
    def define_tables(cls, metadata):
        t1 = Table('t1', metadata,
            Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('data', String(30))
            )
        t2 = Table('t2', metadata,
            Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('data', String(30))
            )

        t1t2_1 = Table('t1t2_1', metadata,
            Column('t1id', Integer, ForeignKey('t1.id')),
            Column('t2id', Integer, ForeignKey('t2.id'))
            )

        t1t2_2 = Table('t1t2_2', metadata,
            Column('t1id', Integer, ForeignKey('t1.id')),
            Column('t2id', Integer, ForeignKey('t2.id'))
            )

    def test_basic(self):
        t2, t1t2_1, t1t2_2, t1 = (self.tables.t2,
                                self.tables.t1t2_1,
                                self.tables.t1t2_2,
                                self.tables.t1)

        class T1(object):pass
        class T2(object):pass

        mapper(T1, t1, properties={
            't2s_1':relationship(T2, secondary=t1t2_1),
            't2s_2':relationship(T2, secondary=t1t2_2),
        })
        mapper(T2, t2)

        q = create_session().query(T1).join('t2s_1').filter(t2.c.id==5).reset_joinpoint().join('t2s_2')
        self.assert_compile(
            q,
            "SELECT t1.id AS t1_id, t1.data AS t1_data FROM t1 JOIN t1t2_1 AS t1t2_1_1 "
            "ON t1.id = t1t2_1_1.t1id JOIN t2 ON t2.id = t1t2_1_1.t2id JOIN t1t2_2 AS t1t2_2_1 "
            "ON t1.id = t1t2_2_1.t1id JOIN t2 ON t2.id = t1t2_2_1.t2id WHERE t2.id = :id_1"
            , use_default_dialect=True
        )


class SelfRefMixedTest(fixtures.MappedTest, AssertsCompiledSQL):
    run_setup_mappers = 'once'
    __dialect__ = default.DefaultDialect()

    @classmethod
    def define_tables(cls, metadata):
        nodes = Table('nodes', metadata,
            Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('parent_id', Integer, ForeignKey('nodes.id'))
        )

        sub_table = Table('sub_table', metadata,
            Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('node_id', Integer, ForeignKey('nodes.id')),
        )

        assoc_table = Table('assoc_table', metadata,
            Column('left_id', Integer, ForeignKey('nodes.id')),
            Column('right_id', Integer, ForeignKey('nodes.id'))
        )

    @classmethod
    def setup_classes(cls):
        nodes, assoc_table, sub_table = (cls.tables.nodes,
                                cls.tables.assoc_table,
                                cls.tables.sub_table)

        class Node(cls.Comparable):
            pass

        class Sub(cls.Comparable):
            pass

        mapper(Node, nodes, properties={
            'children':relationship(Node, lazy='select', join_depth=3,
                backref=backref('parent', remote_side=[nodes.c.id])
            ),
            'subs' : relationship(Sub),
            'assoc':relationship(Node, 
                            secondary=assoc_table, 
                            primaryjoin=nodes.c.id==assoc_table.c.left_id, 
                            secondaryjoin=nodes.c.id==assoc_table.c.right_id)
        })
        mapper(Sub, sub_table)

    def test_o2m_aliased_plus_o2m(self):
        Node, Sub = self.classes.Node, self.classes.Sub

        sess = create_session()
        n1 = aliased(Node)

        self.assert_compile(
            sess.query(Node).join(n1, Node.children).join(Sub, n1.subs),
            "SELECT nodes.id AS nodes_id, nodes.parent_id AS nodes_parent_id "
            "FROM nodes JOIN nodes AS nodes_1 ON nodes.id = nodes_1.parent_id "
            "JOIN sub_table ON nodes_1.id = sub_table.node_id"
        )

        self.assert_compile(
            sess.query(Node).join(n1, Node.children).join(Sub, Node.subs),
            "SELECT nodes.id AS nodes_id, nodes.parent_id AS nodes_parent_id "
            "FROM nodes JOIN nodes AS nodes_1 ON nodes.id = nodes_1.parent_id "
            "JOIN sub_table ON nodes.id = sub_table.node_id"
        )

    def test_m2m_aliased_plus_o2m(self):
        Node, Sub = self.classes.Node, self.classes.Sub

        sess = create_session()
        n1 = aliased(Node)

        self.assert_compile(
            sess.query(Node).join(n1, Node.assoc).join(Sub, n1.subs),
            "SELECT nodes.id AS nodes_id, nodes.parent_id AS nodes_parent_id "
            "FROM nodes JOIN assoc_table AS assoc_table_1 ON nodes.id = "
            "assoc_table_1.left_id JOIN nodes AS nodes_1 ON nodes_1.id = "
            "assoc_table_1.right_id JOIN sub_table ON nodes_1.id = sub_table.node_id",
        )

        self.assert_compile(
            sess.query(Node).join(n1, Node.assoc).join(Sub, Node.subs),
            "SELECT nodes.id AS nodes_id, nodes.parent_id AS nodes_parent_id "
            "FROM nodes JOIN assoc_table AS assoc_table_1 ON nodes.id = "
            "assoc_table_1.left_id JOIN nodes AS nodes_1 ON nodes_1.id = "
            "assoc_table_1.right_id JOIN sub_table ON nodes.id = sub_table.node_id",
        )

class CreateJoinsTest(fixtures.ORMTest, AssertsCompiledSQL):
    __dialect__ = 'default'

    def _inherits_fixture(self):
        m = MetaData()
        base = Table('base', m, Column('id', Integer, primary_key=True))
        a = Table('a', m, 
                Column('id', Integer, ForeignKey('base.id'), primary_key=True),
                Column('b_id', Integer, ForeignKey('b.id')))
        b = Table('b', m, 
                Column('id', Integer, ForeignKey('base.id'), primary_key=True),
                Column('c_id', Integer, ForeignKey('c.id')))
        c = Table('c', m, 
                Column('id', Integer, ForeignKey('base.id'), primary_key=True))
        class Base(object):
            pass
        class A(Base):
            pass
        class B(Base):
            pass
        class C(Base):
            pass
        mapper(Base, base)
        mapper(A, a, inherits=Base, properties={'b':relationship(B, primaryjoin=a.c.b_id==b.c.id)})
        mapper(B, b, inherits=Base, properties={'c':relationship(C, primaryjoin=b.c.c_id==c.c.id)})
        mapper(C, c, inherits=Base)
        return A, B, C, Base

    def test_double_level_aliased_exists(self):
        A, B, C, Base = self._inherits_fixture()
        s = Session()
        self.assert_compile(
            s.query(A).filter(A.b.has(B.c.has(C.id==5))),
            "SELECT a.id AS a_id, base.id AS base_id, a.b_id AS a_b_id "
            "FROM base JOIN a ON base.id = a.id WHERE "
            "EXISTS (SELECT 1 FROM (SELECT base.id AS base_id, b.id AS "
            "b_id, b.c_id AS b_c_id FROM base JOIN b ON base.id = b.id) "
            "AS anon_1 WHERE a.b_id = anon_1.b_id AND (EXISTS "
            "(SELECT 1 FROM (SELECT base.id AS base_id, c.id AS c_id "
            "FROM base JOIN c ON base.id = c.id) AS anon_2 "
            "WHERE anon_1.b_c_id = anon_2.c_id AND anon_2.c_id = :id_1"
            ")))"
        )

class SelfReferentialTest(fixtures.MappedTest, AssertsCompiledSQL):
    run_setup_mappers = 'once'
    run_inserts = 'once'
    run_deletes = None

    @classmethod
    def define_tables(cls, metadata):
        Table('nodes', metadata,
            Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('parent_id', Integer, ForeignKey('nodes.id')),
            Column('data', String(30)))

    @classmethod
    def setup_classes(cls):
       class Node(cls.Comparable):
           def append(self, node):
               self.children.append(node)

    @classmethod
    def setup_mappers(cls):
        Node, nodes = cls.classes.Node, cls.tables.nodes

        mapper(Node, nodes, properties={
            'children':relationship(Node, lazy='select', join_depth=3,
                backref=backref('parent', remote_side=[nodes.c.id])
            ),
        })

    @classmethod
    def insert_data(cls):
        Node = cls.classes.Node

        sess = create_session()
        n1 = Node(data='n1')
        n1.append(Node(data='n11'))
        n1.append(Node(data='n12'))
        n1.append(Node(data='n13'))
        n1.children[1].append(Node(data='n121'))
        n1.children[1].append(Node(data='n122'))
        n1.children[1].append(Node(data='n123'))
        sess.add(n1)
        sess.flush()
        sess.close()

    def test_join(self):
        Node = self.classes.Node

        sess = create_session()

        node = sess.query(Node).join('children', aliased=True).filter_by(data='n122').first()
        assert node.data=='n12'

        ret = sess.query(Node.data).join(Node.children, aliased=True).filter_by(data='n122').all()
        assert ret == [('n12',)]


        node = sess.query(Node).join('children', 'children', aliased=True).filter_by(data='n122').first()
        assert node.data=='n1'

        node = sess.query(Node).filter_by(data='n122').join('parent', aliased=True).filter_by(data='n12').\
            join('parent', aliased=True, from_joinpoint=True).filter_by(data='n1').first()
        assert node.data == 'n122'

    def test_string_or_prop_aliased(self):
        """test that join('foo') behaves the same as join(Cls.foo) in a self
        referential scenario.

        """

        Node = self.classes.Node


        sess = create_session()
        nalias = aliased(Node, sess.query(Node).filter_by(data='n1').subquery())

        q1 = sess.query(nalias).join(nalias.children, aliased=True).\
                join(Node.children, from_joinpoint=True)

        q2 = sess.query(nalias).join(nalias.children, aliased=True).\
                join("children", from_joinpoint=True)

        for q in (q1, q2):
            self.assert_compile(
                q,
                "SELECT anon_1.id AS anon_1_id, anon_1.parent_id AS "
                "anon_1_parent_id, anon_1.data AS anon_1_data FROM "
                "(SELECT nodes.id AS id, nodes.parent_id AS parent_id, "
                "nodes.data AS data FROM nodes WHERE nodes.data = :data_1) "
                "AS anon_1 JOIN nodes AS nodes_1 ON anon_1.id = "
                "nodes_1.parent_id JOIN nodes ON nodes_1.id = nodes.parent_id",
                use_default_dialect=True
            )

        q1 = sess.query(Node).join(nalias.children, aliased=True).\
                join(Node.children, aliased=True, from_joinpoint=True).\
                join(Node.children, from_joinpoint=True)

        q2 = sess.query(Node).join(nalias.children, aliased=True).\
                join("children", aliased=True, from_joinpoint=True).\
                join("children", from_joinpoint=True)

        for q in (q1, q2):
            self.assert_compile(
                q,
                "SELECT nodes.id AS nodes_id, nodes.parent_id AS "
                "nodes_parent_id, nodes.data AS nodes_data FROM (SELECT "
                "nodes.id AS id, nodes.parent_id AS parent_id, nodes.data "
                "AS data FROM nodes WHERE nodes.data = :data_1) AS anon_1 "
                "JOIN nodes AS nodes_1 ON anon_1.id = nodes_1.parent_id "
                "JOIN nodes AS nodes_2 ON nodes_1.id = nodes_2.parent_id "
                "JOIN nodes ON nodes_2.id = nodes.parent_id",
                use_default_dialect=True
            )

    def test_from_self_inside_excludes_outside(self):
        """test the propagation of aliased() from inside to outside
        on a from_self()..
        """

        Node = self.classes.Node

        sess = create_session()

        n1 = aliased(Node)

        # n1 is not inside the from_self(), so all cols must be maintained
        # on the outside
        self.assert_compile(
            sess.query(Node).filter(Node.data=='n122').from_self(n1, Node.id),
            "SELECT nodes_1.id AS nodes_1_id, nodes_1.parent_id AS nodes_1_parent_id, "
            "nodes_1.data AS nodes_1_data, anon_1.nodes_id AS anon_1_nodes_id "
            "FROM nodes AS nodes_1, (SELECT nodes.id AS nodes_id, "
            "nodes.parent_id AS nodes_parent_id, nodes.data AS nodes_data FROM "
            "nodes WHERE nodes.data = :data_1) AS anon_1",
            use_default_dialect=True
        )

        parent = aliased(Node)
        grandparent = aliased(Node)
        q = sess.query(Node, parent, grandparent).\
            join(parent, Node.parent).\
            join(grandparent, parent.parent).\
                filter(Node.data=='n122').filter(parent.data=='n12').\
                filter(grandparent.data=='n1').from_self().limit(1)

        # parent, grandparent *are* inside the from_self(), so they 
        # should get aliased to the outside.
        self.assert_compile(
            q,
            "SELECT anon_1.nodes_id AS anon_1_nodes_id, "
            "anon_1.nodes_parent_id AS anon_1_nodes_parent_id, "
            "anon_1.nodes_data AS anon_1_nodes_data, "
            "anon_1.nodes_1_id AS anon_1_nodes_1_id, "
            "anon_1.nodes_1_parent_id AS anon_1_nodes_1_parent_id, "
            "anon_1.nodes_1_data AS anon_1_nodes_1_data, "
            "anon_1.nodes_2_id AS anon_1_nodes_2_id, "
            "anon_1.nodes_2_parent_id AS anon_1_nodes_2_parent_id, "
            "anon_1.nodes_2_data AS anon_1_nodes_2_data "
            "FROM (SELECT nodes.id AS nodes_id, nodes.parent_id "
            "AS nodes_parent_id, nodes.data AS nodes_data, "
            "nodes_1.id AS nodes_1_id, nodes_1.parent_id AS nodes_1_parent_id, "
            "nodes_1.data AS nodes_1_data, nodes_2.id AS nodes_2_id, "
            "nodes_2.parent_id AS nodes_2_parent_id, nodes_2.data AS "
            "nodes_2_data FROM nodes JOIN nodes AS nodes_1 ON "
            "nodes_1.id = nodes.parent_id JOIN nodes AS nodes_2 "
            "ON nodes_2.id = nodes_1.parent_id "
            "WHERE nodes.data = :data_1 AND nodes_1.data = :data_2 AND "
            "nodes_2.data = :data_3) AS anon_1 LIMIT :param_1",
            {'param_1':1},
            use_default_dialect=True
        )

    def test_explicit_join(self):
        Node = self.classes.Node

        sess = create_session()

        n1 = aliased(Node)
        n2 = aliased(Node)

        self.assert_compile(
            join(Node, n1, 'children').join(n2, 'children'),
            "nodes JOIN nodes AS nodes_1 ON nodes.id = nodes_1.parent_id JOIN nodes AS nodes_2 ON nodes_1.id = nodes_2.parent_id",
            use_default_dialect=True
        )

        self.assert_compile(
            join(Node, n1, Node.children).join(n2, n1.children),
            "nodes JOIN nodes AS nodes_1 ON nodes.id = nodes_1.parent_id JOIN nodes AS nodes_2 ON nodes_1.id = nodes_2.parent_id",
            use_default_dialect=True
        )

        # the join_to_left=False here is unfortunate.   the default on this flag should
        # be False.
        self.assert_compile(
            join(Node, n1, Node.children).join(n2, Node.children, join_to_left=False),
            "nodes JOIN nodes AS nodes_1 ON nodes.id = nodes_1.parent_id JOIN nodes AS nodes_2 ON nodes.id = nodes_2.parent_id",
            use_default_dialect=True
        )

        self.assert_compile(
            sess.query(Node).join(n1, Node.children).join(n2, n1.children),
            "SELECT nodes.id AS nodes_id, nodes.parent_id AS nodes_parent_id, nodes.data AS "
            "nodes_data FROM nodes JOIN nodes AS nodes_1 ON nodes.id = nodes_1.parent_id "
            "JOIN nodes AS nodes_2 ON nodes_1.id = nodes_2.parent_id",
            use_default_dialect=True
        )

        self.assert_compile(
            sess.query(Node).join(n1, Node.children).join(n2, Node.children),
            "SELECT nodes.id AS nodes_id, nodes.parent_id AS nodes_parent_id, nodes.data AS "
            "nodes_data FROM nodes JOIN nodes AS nodes_1 ON nodes.id = nodes_1.parent_id "
            "JOIN nodes AS nodes_2 ON nodes.id = nodes_2.parent_id",
            use_default_dialect=True
        )

        node = sess.query(Node).select_from(join(Node, n1, 'children')).filter(n1.data=='n122').first()
        assert node.data=='n12'

        node = sess.query(Node).select_from(join(Node, n1, 'children').join(n2, 'children')).\
            filter(n2.data=='n122').first()
        assert node.data=='n1'

        # mix explicit and named onclauses
        node = sess.query(Node).select_from(join(Node, n1, Node.id==n1.parent_id).join(n2, 'children')).\
            filter(n2.data=='n122').first()
        assert node.data=='n1'

        node = sess.query(Node).select_from(join(Node, n1, 'parent').join(n2, 'parent')).\
            filter(and_(Node.data=='n122', n1.data=='n12', n2.data=='n1')).first()
        assert node.data == 'n122'

        eq_(
            list(sess.query(Node).select_from(join(Node, n1, 'parent').join(n2, 'parent')).\
            filter(and_(Node.data=='n122', n1.data=='n12', n2.data=='n1')).values(Node.data, n1.data, n2.data)),
            [('n122', 'n12', 'n1')])

    def test_join_to_nonaliased(self):
        Node = self.classes.Node

        sess = create_session()

        n1 = aliased(Node)

        # using 'n1.parent' implicitly joins to unaliased Node
        eq_(
            sess.query(n1).join(n1.parent).filter(Node.data=='n1').all(),
            [Node(parent_id=1,data=u'n11',id=2), Node(parent_id=1,data=u'n12',id=3), Node(parent_id=1,data=u'n13',id=4)]
        )

        # explicit (new syntax)
        eq_(
            sess.query(n1).join(Node, n1.parent).filter(Node.data=='n1').all(),
            [Node(parent_id=1,data=u'n11',id=2), Node(parent_id=1,data=u'n12',id=3), Node(parent_id=1,data=u'n13',id=4)]
        )


    def test_multiple_explicit_entities(self):
        Node = self.classes.Node

        sess = create_session()

        parent = aliased(Node)
        grandparent = aliased(Node)
        eq_(
            sess.query(Node, parent, grandparent).\
                join(parent, Node.parent).\
                join(grandparent, parent.parent).\
                    filter(Node.data=='n122').filter(parent.data=='n12').\
                    filter(grandparent.data=='n1').first(),
            (Node(data='n122'), Node(data='n12'), Node(data='n1'))
        )

        eq_(
            sess.query(Node, parent, grandparent).\
                join(parent, Node.parent).\
                join(grandparent, parent.parent).\
                    filter(Node.data=='n122').filter(parent.data=='n12').\
                    filter(grandparent.data=='n1').from_self().first(),
            (Node(data='n122'), Node(data='n12'), Node(data='n1'))
        )

        # same, change order around
        eq_(
            sess.query(parent, grandparent, Node).\
                join(parent, Node.parent).\
                join(grandparent, parent.parent).\
                    filter(Node.data=='n122').filter(parent.data=='n12').\
                    filter(grandparent.data=='n1').from_self().first(),
            (Node(data='n12'), Node(data='n1'), Node(data='n122'))
        )

        eq_(
            sess.query(Node, parent, grandparent).\
                join(parent, Node.parent).\
                join(grandparent, parent.parent).\
                    filter(Node.data=='n122').filter(parent.data=='n12').\
                    filter(grandparent.data=='n1').\
                    options(joinedload(Node.children)).first(),
            (Node(data='n122'), Node(data='n12'), Node(data='n1'))
        )

        eq_(
            sess.query(Node, parent, grandparent).\
                join(parent, Node.parent).\
                join(grandparent, parent.parent).\
                    filter(Node.data=='n122').filter(parent.data=='n12').\
                    filter(grandparent.data=='n1').from_self().\
                    options(joinedload(Node.children)).first(),
            (Node(data='n122'), Node(data='n12'), Node(data='n1'))
        )


    def test_any(self):
        Node = self.classes.Node

        sess = create_session()
        eq_(sess.query(Node).filter(Node.children.any(Node.data=='n1')).all(), [])
        eq_(sess.query(Node).filter(Node.children.any(Node.data=='n12')).all(), [Node(data='n1')])
        eq_(sess.query(Node).filter(~Node.children.any()).order_by(Node.id).all(), 
                [Node(data='n11'), Node(data='n13'),Node(data='n121'),Node(data='n122'),Node(data='n123'),])

    def test_has(self):
        Node = self.classes.Node

        sess = create_session()

        eq_(sess.query(Node).filter(Node.parent.has(Node.data=='n12')).order_by(Node.id).all(), 
            [Node(data='n121'),Node(data='n122'),Node(data='n123')])
        eq_(sess.query(Node).filter(Node.parent.has(Node.data=='n122')).all(), [])
        eq_(sess.query(Node).filter(~Node.parent.has()).all(), [Node(data='n1')])

    def test_contains(self):
        Node = self.classes.Node

        sess = create_session()

        n122 = sess.query(Node).filter(Node.data=='n122').one()
        eq_(sess.query(Node).filter(Node.children.contains(n122)).all(), [Node(data='n12')])

        n13 = sess.query(Node).filter(Node.data=='n13').one()
        eq_(sess.query(Node).filter(Node.children.contains(n13)).all(), [Node(data='n1')])

    def test_eq_ne(self):
        Node = self.classes.Node

        sess = create_session()

        n12 = sess.query(Node).filter(Node.data=='n12').one()
        eq_(sess.query(Node).filter(Node.parent==n12).all(), [Node(data='n121'),Node(data='n122'),Node(data='n123')])

        eq_(sess.query(Node).filter(Node.parent != n12).all(), [Node(data='n1'), Node(data='n11'), Node(data='n12'), Node(data='n13')])

class SelfReferentialM2MTest(fixtures.MappedTest):
    run_setup_mappers = 'once'
    run_inserts = 'once'
    run_deletes = None

    @classmethod
    def define_tables(cls, metadata):
        nodes = Table('nodes', metadata,
            Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('data', String(30)))

        node_to_nodes =Table('node_to_nodes', metadata,
            Column('left_node_id', Integer, ForeignKey('nodes.id'),primary_key=True),
            Column('right_node_id', Integer, ForeignKey('nodes.id'),primary_key=True),
            )

    @classmethod
    def setup_classes(cls):
        class Node(cls.Comparable):
            pass

    @classmethod
    def insert_data(cls):
        Node, nodes, node_to_nodes = (cls.classes.Node,
                                cls.tables.nodes,
                                cls.tables.node_to_nodes)


        mapper(Node, nodes, properties={
            'children':relationship(Node, lazy='select', secondary=node_to_nodes,
                primaryjoin=nodes.c.id==node_to_nodes.c.left_node_id,
                secondaryjoin=nodes.c.id==node_to_nodes.c.right_node_id,
            )
        })
        sess = create_session()
        n1 = Node(data='n1')
        n2 = Node(data='n2')
        n3 = Node(data='n3')
        n4 = Node(data='n4')
        n5 = Node(data='n5')
        n6 = Node(data='n6')
        n7 = Node(data='n7')

        n1.children = [n2, n3, n4]
        n2.children = [n3, n6, n7]
        n3.children = [n5, n4]

        sess.add(n1)
        sess.add(n2)
        sess.add(n3)
        sess.add(n4)
        sess.flush()
        sess.close()

    def test_any(self):
        Node = self.classes.Node

        sess = create_session()
        eq_(sess.query(Node).filter(Node.children.any(Node.data == 'n3'
            )).all(), [Node(data='n1'), Node(data='n2')])

    def test_contains(self):
        Node = self.classes.Node

        sess = create_session()
        n4 = sess.query(Node).filter_by(data='n4').one()

        eq_(sess.query(Node).filter(Node.children.contains(n4)).order_by(Node.data).all(),
            [Node(data='n1'), Node(data='n3')])
        eq_(sess.query(Node).filter(not_(Node.children.contains(n4))).order_by(Node.data).all(),
            [Node(data='n2'), Node(data='n4'), Node(data='n5'),
            Node(data='n6'), Node(data='n7')])

    def test_explicit_join(self):
        Node = self.classes.Node

        sess = create_session()

        n1 = aliased(Node)
        eq_(
            sess.query(Node).select_from(join(Node, n1, 'children'
             )).filter(n1.data.in_(['n3', 'n7'
             ])).order_by(Node.id).all(),
            [Node(data='n1'), Node(data='n2')]
        )
