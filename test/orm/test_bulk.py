from sqlalchemy import testing
from sqlalchemy.testing import eq_
from sqlalchemy.testing.schema import Table, Column
from sqlalchemy.testing import fixtures
from sqlalchemy import Integer, String, ForeignKey, FetchedValue
from sqlalchemy.orm import mapper, Session
from sqlalchemy.testing.assertsql import CompiledSQL
from test.orm import _fixtures


class BulkTest(testing.AssertsExecutionResults):
    run_inserts = None
    run_define_tables = 'each'


class BulkInsertUpdateVersionId(BulkTest, fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table('version_table', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('version_id', Integer, nullable=False),
              Column('value', String(40), nullable=False))

    @classmethod
    def setup_classes(cls):
        class Foo(cls.Comparable):
            pass

    @classmethod
    def setup_mappers(cls):
        Foo, version_table = cls.classes.Foo, cls.tables.version_table

        mapper(Foo, version_table, version_id_col=version_table.c.version_id)

    def test_bulk_insert_via_save(self):
        Foo = self.classes.Foo

        s = Session()

        s.bulk_save_objects([Foo(value='value')])

        eq_(
            s.query(Foo).all(),
            [Foo(version_id=1, value='value')]
        )

    def test_bulk_update_via_save(self):
        Foo = self.classes.Foo

        s = Session()

        s.add(Foo(value='value'))
        s.commit()

        f1 = s.query(Foo).first()
        f1.value = 'new value'
        s.bulk_save_objects([f1])
        s.expunge_all()

        eq_(
            s.query(Foo).all(),
            [Foo(version_id=2, value='new value')]
        )


class BulkInsertUpdateTest(BulkTest, _fixtures.FixtureTest):

    @classmethod
    def setup_mappers(cls):
        User, Address, Order = cls.classes("User", "Address", "Order")
        u, a, o = cls.tables("users", "addresses", "orders")

        mapper(User, u)
        mapper(Address, a)
        mapper(Order, o)

    def test_bulk_save_return_defaults(self):
        User, = self.classes("User",)

        s = Session()
        objects = [
            User(name="u1"),
            User(name="u2"),
            User(name="u3")
        ]
        assert 'id' not in objects[0].__dict__

        with self.sql_execution_asserter() as asserter:
            s.bulk_save_objects(objects, return_defaults=True)

        asserter.assert_(
            CompiledSQL(
                "INSERT INTO users (name) VALUES (:name)",
                [{'name': 'u1'}]
            ),
            CompiledSQL(
                "INSERT INTO users (name) VALUES (:name)",
                [{'name': 'u2'}]
            ),
            CompiledSQL(
                "INSERT INTO users (name) VALUES (:name)",
                [{'name': 'u3'}]
            ),
        )
        eq_(objects[0].__dict__['id'], 1)

    def test_bulk_save_no_defaults(self):
        User, = self.classes("User",)

        s = Session()
        objects = [
            User(name="u1"),
            User(name="u2"),
            User(name="u3")
        ]
        assert 'id' not in objects[0].__dict__

        with self.sql_execution_asserter() as asserter:
            s.bulk_save_objects(objects)

        asserter.assert_(
            CompiledSQL(
                "INSERT INTO users (name) VALUES (:name)",
                [{'name': 'u1'}, {'name': 'u2'}, {'name': 'u3'}]
            ),
        )
        assert 'id' not in objects[0].__dict__

    def test_bulk_save_updated_include_unchanged(self):
        User, = self.classes("User",)

        s = Session(expire_on_commit=False)
        objects = [
            User(name="u1"),
            User(name="u2"),
            User(name="u3")
        ]
        s.add_all(objects)
        s.commit()

        objects[0].name = 'u1new'
        objects[2].name = 'u3new'

        s = Session()
        with self.sql_execution_asserter() as asserter:
            s.bulk_save_objects(objects, update_changed_only=False)

        asserter.assert_(
            CompiledSQL(
                "UPDATE users SET name=:name WHERE "
                "users.id = :users_id",
                [{'users_id': 1, 'name': 'u1new'},
                 {'users_id': 2, 'name': 'u2'},
                 {'users_id': 3, 'name': 'u3new'}]
            )
        )

    def test_bulk_update(self):
        User, = self.classes("User",)

        s = Session(expire_on_commit=False)
        objects = [
            User(name="u1"),
            User(name="u2"),
            User(name="u3")
        ]
        s.add_all(objects)
        s.commit()

        s = Session()
        with self.sql_execution_asserter() as asserter:
            s.bulk_update_mappings(
                User,
                [{'id': 1, 'name': 'u1new'},
                 {'id': 2, 'name': 'u2'},
                 {'id': 3, 'name': 'u3new'}]
            )

        asserter.assert_(
            CompiledSQL(
                "UPDATE users SET name=:name WHERE users.id = :users_id",
                [{'users_id': 1, 'name': 'u1new'},
                 {'users_id': 2, 'name': 'u2'},
                 {'users_id': 3, 'name': 'u3new'}]
            )
        )

    def test_bulk_insert(self):
        User, = self.classes("User",)

        s = Session()
        with self.sql_execution_asserter() as asserter:
            s.bulk_insert_mappings(
                User,
                [{'id': 1, 'name': 'u1new'},
                 {'id': 2, 'name': 'u2'},
                 {'id': 3, 'name': 'u3new'}]
            )

        asserter.assert_(
            CompiledSQL(
                "INSERT INTO users (id, name) VALUES (:id, :name)",
                [{'id': 1, 'name': 'u1new'},
                 {'id': 2, 'name': 'u2'},
                 {'id': 3, 'name': 'u3new'}]
            )
        )

    def test_bulk_insert_render_nulls(self):
        Order, = self.classes("Order",)

        s = Session()
        with self.sql_execution_asserter() as asserter:
            s.bulk_insert_mappings(
                Order,
                [{'id': 1, 'description': 'u1new'},
                 {'id': 2, 'description': None},
                 {'id': 3, 'description': 'u3new'}],
                render_nulls=True
            )

        asserter.assert_(
            CompiledSQL(
                "INSERT INTO orders (id, description) "
                "VALUES (:id, :description)",
                [{'id': 1, 'description': 'u1new'},
                 {'id': 2, 'description': None},
                 {'id': 3, 'description': 'u3new'}]
            )
        )


class BulkUDPostfetchTest(BulkTest, fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            'a', metadata,
            Column(
                'id', Integer,
                primary_key=True,
                test_needs_autoincrement=True),
            Column('x', Integer),
            Column('y', Integer,
                   server_default=FetchedValue(),
                   server_onupdate=FetchedValue()))

    @classmethod
    def setup_classes(cls):
        class A(cls.Comparable):
            pass

    @classmethod
    def setup_mappers(cls):
        A = cls.classes.A
        a = cls.tables.a

        mapper(A, a)

    def test_insert_w_fetch(self):
        A = self.classes.A

        s = Session()
        a1 = A(x=1)
        s.bulk_save_objects([a1])
        s.commit()

    def test_update_w_fetch(self):
        A = self.classes.A

        s = Session()
        a1 = A(x=1, y=2)
        s.add(a1)
        s.commit()

        eq_(a1.id, 1)  # force a load
        a1.x = 5
        s.expire(a1, ['y'])
        assert 'y' not in a1.__dict__
        s.bulk_save_objects([a1])
        s.commit()

        eq_(a1.x, 5)
        eq_(a1.y, 2)


class BulkUDTestAltColKeys(BulkTest, fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            'people_keys', metadata,
            Column(
                'person_id', Integer,
                primary_key=True, key='id'),
            Column('name', String(50), key='personname'))

        Table(
            'people_attrs', metadata,
            Column(
                'person_id', Integer,
                primary_key=True),
            Column('name', String(50)))

        Table(
            'people_both', metadata,
            Column(
                'person_id', Integer,
                primary_key=True, key="id_key"),
            Column('name', String(50), key='name_key'))

    @classmethod
    def setup_classes(cls):
        class PersonKeys(cls.Comparable):
            pass

        class PersonAttrs(cls.Comparable):
            pass

        class PersonBoth(cls.Comparable):
            pass

    @classmethod
    def setup_mappers(cls):
        PersonKeys, PersonAttrs, PersonBoth = cls.classes(
            "PersonKeys", "PersonAttrs", "PersonBoth")
        people_keys, people_attrs, people_both = cls.tables(
            "people_keys", "people_attrs", "people_both")

        mapper(PersonKeys, people_keys)
        mapper(PersonAttrs, people_attrs, properties={
            'id': people_attrs.c.person_id,
            'personname': people_attrs.c.name
        })

        mapper(PersonBoth, people_both, properties={
            'id': people_both.c.id_key,
            'personname': people_both.c.name_key
        })

    def test_insert_keys(self):
        self._test_insert(self.classes.PersonKeys)

    def test_insert_attrs(self):
        self._test_insert(self.classes.PersonAttrs)

    def test_insert_both(self):
        self._test_insert(self.classes.PersonBoth)

    def test_update_keys(self):
        self._test_update(self.classes.PersonKeys)

    def test_update_attrs(self):
        self._test_update(self.classes.PersonAttrs)

    def test_update_both(self):
        # want to make sure that before [ticket:3849], this did not have
        # a successful behavior or workaround
        self._test_update(self.classes.PersonBoth)

    def _test_insert(self, person_cls):
        Person = person_cls

        s = Session()
        s.bulk_insert_mappings(
            Person, [{"id": 5, "personname": "thename"}]
        )

        eq_(
            s.query(Person).first(),
            Person(id=5, personname="thename")
        )

    def _test_update(self, person_cls):
        Person = person_cls

        s = Session()
        s.add(Person(id=5, personname="thename"))
        s.commit()

        s.bulk_update_mappings(
            Person, [{"id": 5, "personname": "newname"}]
        )

        eq_(
            s.query(Person).first(),
            Person(id=5, personname="newname")
        )


class BulkInheritanceTest(BulkTest, fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            'people', metadata,
            Column(
                'person_id', Integer,
                primary_key=True,
                test_needs_autoincrement=True),
            Column('name', String(50)),
            Column('type', String(30)))

        Table(
            'engineers', metadata,
            Column(
                'person_id', Integer,
                ForeignKey('people.person_id'),
                primary_key=True),
            Column('status', String(30)),
            Column('primary_language', String(50)))

        Table(
            'managers', metadata,
            Column(
                'person_id', Integer,
                ForeignKey('people.person_id'),
                primary_key=True),
            Column('status', String(30)),
            Column('manager_name', String(50)))

        Table(
            'boss', metadata,
            Column(
                'boss_id', Integer,
                ForeignKey('managers.person_id'),
                primary_key=True),
            Column('golf_swing', String(30)))

    @classmethod
    def setup_classes(cls):
        class Base(cls.Comparable):
            pass

        class Person(Base):
            pass

        class Engineer(Person):
            pass

        class Manager(Person):
            pass

        class Boss(Manager):
            pass

    @classmethod
    def setup_mappers(cls):
        Person, Engineer, Manager, Boss = \
            cls.classes('Person', 'Engineer', 'Manager', 'Boss')
        p, e, m, b = cls.tables('people', 'engineers', 'managers', 'boss')

        mapper(
            Person, p, polymorphic_on=p.c.type,
            polymorphic_identity='person')
        mapper(Engineer, e, inherits=Person, polymorphic_identity='engineer')
        mapper(Manager, m, inherits=Person, polymorphic_identity='manager')
        mapper(Boss, b, inherits=Manager, polymorphic_identity='boss')

    def test_bulk_save_joined_inh_return_defaults(self):
        Person, Engineer, Manager, Boss = \
            self.classes('Person', 'Engineer', 'Manager', 'Boss')

        s = Session()
        objects = [
            Manager(name='m1', status='s1', manager_name='mn1'),
            Engineer(name='e1', status='s2', primary_language='l1'),
            Engineer(name='e2', status='s3', primary_language='l2'),
            Boss(
                name='b1', status='s3', manager_name='mn2',
                golf_swing='g1')
        ]
        assert 'person_id' not in objects[0].__dict__

        with self.sql_execution_asserter() as asserter:
            s.bulk_save_objects(objects, return_defaults=True)

        asserter.assert_(
            CompiledSQL(
                "INSERT INTO people (name, type) VALUES (:name, :type)",
                [{'type': 'manager', 'name': 'm1'}]
            ),
            CompiledSQL(
                "INSERT INTO managers (person_id, status, manager_name) "
                "VALUES (:person_id, :status, :manager_name)",
                [{'person_id': 1, 'status': 's1', 'manager_name': 'mn1'}]
            ),
            CompiledSQL(
                "INSERT INTO people (name, type) VALUES (:name, :type)",
                [{'type': 'engineer', 'name': 'e1'}]
            ),
            CompiledSQL(
                "INSERT INTO people (name, type) VALUES (:name, :type)",
                [{'type': 'engineer', 'name': 'e2'}]
            ),
            CompiledSQL(
                "INSERT INTO engineers (person_id, status, primary_language) "
                "VALUES (:person_id, :status, :primary_language)",
                [{'person_id': 2, 'status': 's2', 'primary_language': 'l1'},
                 {'person_id': 3, 'status': 's3', 'primary_language': 'l2'}]

            ),
            CompiledSQL(
                "INSERT INTO people (name, type) VALUES (:name, :type)",
                [{'type': 'boss', 'name': 'b1'}]
            ),
            CompiledSQL(
                "INSERT INTO managers (person_id, status, manager_name) "
                "VALUES (:person_id, :status, :manager_name)",
                [{'person_id': 4, 'status': 's3', 'manager_name': 'mn2'}]

            ),
            CompiledSQL(
                "INSERT INTO boss (boss_id, golf_swing) VALUES "
                "(:boss_id, :golf_swing)",
                [{'boss_id': 4, 'golf_swing': 'g1'}]
            )
        )
        eq_(objects[0].__dict__['person_id'], 1)
        eq_(objects[3].__dict__['person_id'], 4)
        eq_(objects[3].__dict__['boss_id'], 4)

    def test_bulk_save_joined_inh_no_defaults(self):
        Person, Engineer, Manager, Boss = \
            self.classes('Person', 'Engineer', 'Manager', 'Boss')

        s = Session()
        with self.sql_execution_asserter() as asserter:
            s.bulk_save_objects([
                Manager(
                    person_id=1,
                    name='m1', status='s1', manager_name='mn1'),
                Engineer(
                    person_id=2,
                    name='e1', status='s2', primary_language='l1'),
                Engineer(
                    person_id=3,
                    name='e2', status='s3', primary_language='l2'),
                Boss(
                    person_id=4, boss_id=4,
                    name='b1', status='s3', manager_name='mn2',
                    golf_swing='g1')
            ],

            )

        # the only difference here is that common classes are grouped together.
        # at the moment it doesn't lump all the "people" tables from
        # different classes together.
        asserter.assert_(
            CompiledSQL(
                "INSERT INTO people (person_id, name, type) VALUES "
                "(:person_id, :name, :type)",
                [{'person_id': 1, 'type': 'manager', 'name': 'm1'}]
            ),
            CompiledSQL(
                "INSERT INTO managers (person_id, status, manager_name) "
                "VALUES (:person_id, :status, :manager_name)",
                [{'status': 's1', 'person_id': 1, 'manager_name': 'mn1'}]
            ),
            CompiledSQL(
                "INSERT INTO people (person_id, name, type) VALUES "
                "(:person_id, :name, :type)",
                [{'person_id': 2, 'type': 'engineer', 'name': 'e1'},
                 {'person_id': 3, 'type': 'engineer', 'name': 'e2'}]
            ),
            CompiledSQL(
                "INSERT INTO engineers (person_id, status, primary_language) "
                "VALUES (:person_id, :status, :primary_language)",
                [{'person_id': 2, 'status': 's2', 'primary_language': 'l1'},
                 {'person_id': 3, 'status': 's3', 'primary_language': 'l2'}]
            ),
            CompiledSQL(
                "INSERT INTO people (person_id, name, type) VALUES "
                "(:person_id, :name, :type)",
                [{'person_id': 4, 'type': 'boss', 'name': 'b1'}]
            ),
            CompiledSQL(
                "INSERT INTO managers (person_id, status, manager_name) "
                "VALUES (:person_id, :status, :manager_name)",
                [{'status': 's3', 'person_id': 4, 'manager_name': 'mn2'}]
            ),
            CompiledSQL(
                "INSERT INTO boss (boss_id, golf_swing) VALUES "
                "(:boss_id, :golf_swing)",
                [{'boss_id': 4, 'golf_swing': 'g1'}]
            )
        )

    def test_bulk_insert_joined_inh_return_defaults(self):
        Person, Engineer, Manager, Boss = \
            self.classes('Person', 'Engineer', 'Manager', 'Boss')

        s = Session()
        with self.sql_execution_asserter() as asserter:
            s.bulk_insert_mappings(
                Boss,
                [
                    dict(
                        name='b1', status='s1', manager_name='mn1',
                        golf_swing='g1'
                    ),
                    dict(
                        name='b2', status='s2', manager_name='mn2',
                        golf_swing='g2'
                    ),
                    dict(
                        name='b3', status='s3', manager_name='mn3',
                        golf_swing='g3'
                    ),
                ], return_defaults=True
            )

        asserter.assert_(
            CompiledSQL(
                "INSERT INTO people (name) VALUES (:name)",
                [{'name': 'b1'}]
            ),
            CompiledSQL(
                "INSERT INTO people (name) VALUES (:name)",
                [{'name': 'b2'}]
            ),
            CompiledSQL(
                "INSERT INTO people (name) VALUES (:name)",
                [{'name': 'b3'}]
            ),
            CompiledSQL(
                "INSERT INTO managers (person_id, status, manager_name) "
                "VALUES (:person_id, :status, :manager_name)",
                [{'person_id': 1, 'status': 's1', 'manager_name': 'mn1'},
                 {'person_id': 2, 'status': 's2', 'manager_name': 'mn2'},
                 {'person_id': 3, 'status': 's3', 'manager_name': 'mn3'}]

            ),
            CompiledSQL(
                "INSERT INTO boss (boss_id, golf_swing) VALUES "
                "(:boss_id, :golf_swing)",
                [{'golf_swing': 'g1', 'boss_id': 1},
                 {'golf_swing': 'g2', 'boss_id': 2},
                 {'golf_swing': 'g3', 'boss_id': 3}]
            )
        )
