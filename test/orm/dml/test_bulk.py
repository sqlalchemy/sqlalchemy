from sqlalchemy import FetchedValue
from sqlalchemy import ForeignKey
from sqlalchemy import Identity
from sqlalchemy import insert
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy import update
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import mock
from sqlalchemy.testing.assertsql import CompiledSQL
from sqlalchemy.testing.assertsql import Conditional
from sqlalchemy.testing.fixtures import fixture_session
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table
from test.orm import _fixtures


class BulkTest(testing.AssertsExecutionResults):
    run_inserts = None
    run_define_tables = "each"


class BulkInsertUpdateVersionId(BulkTest, fixtures.MappedTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "version_table",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("version_id", Integer, nullable=False),
            Column("value", String(40), nullable=False),
        )

    @classmethod
    def setup_classes(cls):
        class Foo(cls.Comparable):
            pass

    @classmethod
    def setup_mappers(cls):
        Foo, version_table = cls.classes.Foo, cls.tables.version_table

        cls.mapper_registry.map_imperatively(
            Foo, version_table, version_id_col=version_table.c.version_id
        )

    @testing.emits_warning(r".*versioning cannot be verified")
    def test_bulk_insert_via_save(self):
        Foo = self.classes.Foo

        s = fixture_session()

        s.bulk_save_objects([Foo(value="value")])

        eq_(s.query(Foo).all(), [Foo(version_id=1, value="value")])

    @testing.emits_warning(r".*versioning cannot be verified")
    def test_bulk_update_via_save(self):
        Foo = self.classes.Foo

        s = fixture_session()

        s.add(Foo(value="value"))
        s.commit()

        f1 = s.query(Foo).first()
        f1.value = "new value"
        s.bulk_save_objects([f1])
        s.expunge_all()

        eq_(s.query(Foo).all(), [Foo(version_id=2, value="new value")])


class BulkInsertUpdateTest(BulkTest, _fixtures.FixtureTest):
    __backend__ = True

    @classmethod
    def setup_mappers(cls):
        User, Address, Order = cls.classes("User", "Address", "Order")
        u, a, o = cls.tables("users", "addresses", "orders")

        cls.mapper_registry.map_imperatively(User, u)
        cls.mapper_registry.map_imperatively(Address, a)
        cls.mapper_registry.map_imperatively(Order, o)

    @testing.combinations("save_objects", "insert_mappings", "insert_stmt")
    def test_bulk_save_return_defaults(self, statement_type):
        (User,) = self.classes("User")

        s = fixture_session()

        if statement_type == "save_objects":
            objects = [User(name="u1"), User(name="u2"), User(name="u3")]
            assert "id" not in objects[0].__dict__

            returning_users_id = " RETURNING users.id"
            with self.sql_execution_asserter() as asserter:
                s.bulk_save_objects(objects, return_defaults=True)
        elif statement_type == "insert_mappings":
            data = [dict(name="u1"), dict(name="u2"), dict(name="u3")]
            returning_users_id = " RETURNING users.id"
            with self.sql_execution_asserter() as asserter:
                s.bulk_insert_mappings(User, data, return_defaults=True)
        elif statement_type == "insert_stmt":
            data = [dict(name="u1"), dict(name="u2"), dict(name="u3")]

            # for statement, "return_defaults" is heuristic on if we are
            # a joined inh mapping if we don't otherwise include
            # .returning() on the statement itself
            returning_users_id = ""
            with self.sql_execution_asserter() as asserter:
                s.execute(insert(User), data)

        asserter.assert_(
            Conditional(
                testing.db.dialect.insert_executemany_returning
                or statement_type == "insert_stmt",
                [
                    CompiledSQL(
                        "INSERT INTO users (name) "
                        f"VALUES (:name){returning_users_id}",
                        [{"name": "u1"}, {"name": "u2"}, {"name": "u3"}],
                    ),
                ],
                [
                    CompiledSQL(
                        "INSERT INTO users (name) VALUES (:name)",
                        [{"name": "u1"}],
                    ),
                    CompiledSQL(
                        "INSERT INTO users (name) VALUES (:name)",
                        [{"name": "u2"}],
                    ),
                    CompiledSQL(
                        "INSERT INTO users (name) VALUES (:name)",
                        [{"name": "u3"}],
                    ),
                ],
            )
        )
        if statement_type == "save_objects":
            eq_(objects[0].__dict__["id"], 1)

    def test_bulk_save_mappings_preserve_order(self):
        (User,) = self.classes("User")

        s = fixture_session()

        # commit some object into db
        user1 = User(name="i1")
        user2 = User(name="i2")
        s.add(user1)
        s.add(user2)
        s.commit()

        # make some changes
        user1.name = "u1"
        user3 = User(name="i3")
        s.add(user3)
        user2.name = "u2"

        objects = [user1, user3, user2]

        from sqlalchemy import inspect

        def _bulk_save_mappings(
            mapper,
            mappings,
            isupdate,
            isstates,
            return_defaults,
            update_changed_only,
            render_nulls,
        ):
            mock_method(list(mappings), isupdate)

        mock_method = mock.Mock()
        with mock.patch.object(s, "_bulk_save_mappings", _bulk_save_mappings):
            s.bulk_save_objects(objects)
            eq_(
                mock_method.mock_calls,
                [
                    mock.call([inspect(user1)], True),
                    mock.call([inspect(user3)], False),
                    mock.call([inspect(user2)], True),
                ],
            )

        mock_method = mock.Mock()
        with mock.patch.object(s, "_bulk_save_mappings", _bulk_save_mappings):
            s.bulk_save_objects(objects, preserve_order=False)
            eq_(
                mock_method.mock_calls,
                [
                    mock.call([inspect(user3)], False),
                    mock.call([inspect(user1), inspect(user2)], True),
                ],
            )

    def test_bulk_save_no_defaults(self):
        (User,) = self.classes("User")

        s = fixture_session()
        objects = [User(name="u1"), User(name="u2"), User(name="u3")]
        assert "id" not in objects[0].__dict__

        with self.sql_execution_asserter() as asserter:
            s.bulk_save_objects(objects)

        asserter.assert_(
            CompiledSQL(
                "INSERT INTO users (name) VALUES (:name)",
                [{"name": "u1"}, {"name": "u2"}, {"name": "u3"}],
            )
        )
        assert "id" not in objects[0].__dict__

    def test_bulk_save_updated_include_unchanged(self):
        (User,) = self.classes("User")

        s = fixture_session(expire_on_commit=False)
        objects = [User(name="u1"), User(name="u2"), User(name="u3")]
        s.add_all(objects)
        s.commit()

        objects[0].name = "u1new"
        objects[2].name = "u3new"

        s = fixture_session()
        with self.sql_execution_asserter() as asserter:
            s.bulk_save_objects(objects, update_changed_only=False)

        asserter.assert_(
            CompiledSQL(
                "UPDATE users SET name=:name WHERE " "users.id = :users_id",
                [
                    {"users_id": 1, "name": "u1new"},
                    {"users_id": 2, "name": "u2"},
                    {"users_id": 3, "name": "u3new"},
                ],
            )
        )

    @testing.combinations("update_mappings", "update_stmt")
    def test_bulk_update(self, statement_type):
        User = self.classes.User

        s = fixture_session(expire_on_commit=False)
        objects = [User(name="u1"), User(name="u2"), User(name="u3")]
        s.add_all(objects)
        s.commit()

        s = fixture_session()
        data = [
            {"id": 1, "name": "u1new"},
            {"id": 2, "name": "u2"},
            {"id": 3, "name": "u3new"},
        ]

        if statement_type == "update_mappings":
            with self.sql_execution_asserter() as asserter:
                s.bulk_update_mappings(User, data)
        elif statement_type == "update_stmt":
            with self.sql_execution_asserter() as asserter:
                s.execute(update(User), data)

        asserter.assert_(
            CompiledSQL(
                "UPDATE users SET name=:name WHERE users.id = :users_id",
                [
                    {"users_id": 1, "name": "u1new"},
                    {"users_id": 2, "name": "u2"},
                    {"users_id": 3, "name": "u3new"},
                ],
            )
        )

    def test_bulk_insert(self):
        (User,) = self.classes("User")

        s = fixture_session()
        with self.sql_execution_asserter() as asserter:
            s.bulk_insert_mappings(
                User,
                [
                    {"id": 1, "name": "u1new"},
                    {"id": 2, "name": "u2"},
                    {"id": 3, "name": "u3new"},
                ],
            )

        asserter.assert_(
            CompiledSQL(
                "INSERT INTO users (id, name) VALUES (:id, :name)",
                [
                    {"id": 1, "name": "u1new"},
                    {"id": 2, "name": "u2"},
                    {"id": 3, "name": "u3new"},
                ],
            )
        )

    def test_bulk_insert_render_nulls(self):
        (Order,) = self.classes("Order")

        s = fixture_session()
        with self.sql_execution_asserter() as asserter:
            s.bulk_insert_mappings(
                Order,
                [
                    {"id": 1, "description": "u1new"},
                    {"id": 2, "description": None},
                    {"id": 3, "description": "u3new"},
                ],
                render_nulls=True,
            )

        asserter.assert_(
            CompiledSQL(
                "INSERT INTO orders (id, description) "
                "VALUES (:id, :description)",
                [
                    {"id": 1, "description": "u1new"},
                    {"id": 2, "description": None},
                    {"id": 3, "description": "u3new"},
                ],
            )
        )


class BulkUDPostfetchTest(BulkTest, fixtures.MappedTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "a",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("x", Integer),
            Column(
                "y",
                Integer,
                server_default=FetchedValue(),
                server_onupdate=FetchedValue(),
            ),
        )

    @classmethod
    def setup_classes(cls):
        class A(cls.Comparable):
            pass

    @classmethod
    def setup_mappers(cls):
        A = cls.classes.A
        a = cls.tables.a

        cls.mapper_registry.map_imperatively(A, a)

    def test_insert_w_fetch(self):
        A = self.classes.A

        s = fixture_session()
        a1 = A(x=1)
        s.bulk_save_objects([a1])
        s.commit()

    def test_update_w_fetch(self):
        A = self.classes.A

        s = fixture_session()
        a1 = A(x=1, y=2)
        s.add(a1)
        s.commit()

        eq_(a1.id, 1)  # force a load
        a1.x = 5
        s.expire(a1, ["y"])
        assert "y" not in a1.__dict__
        s.bulk_save_objects([a1])
        s.commit()

        eq_(a1.x, 5)
        eq_(a1.y, 2)


class BulkUDTestAltColKeys(BulkTest, fixtures.MappedTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "people_keys",
            metadata,
            Column("person_id", Integer, primary_key=True, key="id"),
            Column("name", String(50), key="personname"),
        )

        Table(
            "people_attrs",
            metadata,
            Column("person_id", Integer, primary_key=True),
            Column("name", String(50)),
        )

        Table(
            "people_both",
            metadata,
            Column("person_id", Integer, primary_key=True, key="id_key"),
            Column("name", String(50), key="name_key"),
        )

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
            "PersonKeys", "PersonAttrs", "PersonBoth"
        )
        people_keys, people_attrs, people_both = cls.tables(
            "people_keys", "people_attrs", "people_both"
        )

        cls.mapper_registry.map_imperatively(PersonKeys, people_keys)
        cls.mapper_registry.map_imperatively(
            PersonAttrs,
            people_attrs,
            properties={
                "id": people_attrs.c.person_id,
                "personname": people_attrs.c.name,
            },
        )

        cls.mapper_registry.map_imperatively(
            PersonBoth,
            people_both,
            properties={
                "id": people_both.c.id_key,
                "personname": people_both.c.name_key,
            },
        )

    def test_insert_keys(self):
        asserter = self._test_insert(self.classes.PersonKeys)
        asserter.assert_(
            CompiledSQL(
                "INSERT INTO people_keys (person_id, name) "
                "VALUES (:id, :personname)",
                [{"id": 5, "personname": "thename"}],
            )
        )

    def test_insert_attrs(self):
        asserter = self._test_insert(self.classes.PersonAttrs)
        asserter.assert_(
            CompiledSQL(
                "INSERT INTO people_attrs (person_id, name) "
                "VALUES (:person_id, :name)",
                [{"person_id": 5, "name": "thename"}],
            )
        )

    def test_insert_both(self):
        asserter = self._test_insert(self.classes.PersonBoth)
        asserter.assert_(
            CompiledSQL(
                "INSERT INTO people_both (person_id, name) "
                "VALUES (:id_key, :name_key)",
                [{"id_key": 5, "name_key": "thename"}],
            )
        )

    @testing.combinations(
        ("states",),
        ("dicts",),
    )
    def test_update_keys(self, type_):
        if type_ == "states":
            asserter = self._test_update_states(self.classes.PersonKeys)
        else:
            asserter = self._test_update(self.classes.PersonKeys)
        asserter.assert_(
            CompiledSQL(
                "UPDATE people_keys SET name=:personname "
                "WHERE people_keys.person_id = :people_keys_person_id",
                [{"personname": "newname", "people_keys_person_id": 5}],
            )
        )

    @testing.combinations(
        ("states",),
        ("dicts",),
    )
    @testing.requires.updateable_autoincrement_pks
    def test_update_attrs(self, type_):
        if type_ == "states":
            asserter = self._test_update_states(self.classes.PersonAttrs)
        else:
            asserter = self._test_update(self.classes.PersonAttrs)
        asserter.assert_(
            CompiledSQL(
                "UPDATE people_attrs SET name=:name "
                "WHERE people_attrs.person_id = :people_attrs_person_id",
                [{"name": "newname", "people_attrs_person_id": 5}],
            )
        )

    @testing.requires.updateable_autoincrement_pks
    def test_update_both(self):
        # want to make sure that before [ticket:3849], this did not have
        # a successful behavior or workaround
        asserter = self._test_update(self.classes.PersonBoth)
        asserter.assert_(
            CompiledSQL(
                "UPDATE people_both SET name=:name_key "
                "WHERE people_both.person_id = :people_both_person_id",
                [{"name_key": "newname", "people_both_person_id": 5}],
            )
        )

    def _test_insert(self, person_cls):
        Person = person_cls

        s = fixture_session()
        with self.sql_execution_asserter(testing.db) as asserter:
            s.bulk_insert_mappings(
                Person, [{"id": 5, "personname": "thename"}]
            )

        eq_(s.query(Person).first(), Person(id=5, personname="thename"))

        return asserter

    def _test_update(self, person_cls):
        Person = person_cls

        s = fixture_session()
        s.add(Person(id=5, personname="thename"))
        s.commit()

        with self.sql_execution_asserter(testing.db) as asserter:
            s.bulk_update_mappings(
                Person, [{"id": 5, "personname": "newname"}]
            )

        eq_(s.query(Person).first(), Person(id=5, personname="newname"))

        return asserter

    def _test_update_states(self, person_cls):
        Person = person_cls

        s = fixture_session()
        s.add(Person(id=5, personname="thename"))
        s.commit()

        p = s.get(Person, 5)
        with self.sql_execution_asserter(testing.db) as asserter:
            p.personname = "newname"
            s.bulk_save_objects([p])

        eq_(s.query(Person).first(), Person(id=5, personname="newname"))

        return asserter


class BulkInheritanceTest(BulkTest, fixtures.MappedTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "people",
            metadata,
            Column(
                "person_id",
                Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column("name", String(50)),
            Column("type", String(30)),
        )

        Table(
            "engineers",
            metadata,
            Column(
                "person_id",
                Integer,
                ForeignKey("people.person_id"),
                primary_key=True,
            ),
            Column("status", String(30)),
            Column("primary_language", String(50)),
        )

        Table(
            "managers",
            metadata,
            Column(
                "person_id",
                Integer,
                ForeignKey("people.person_id"),
                primary_key=True,
            ),
            Column("status", String(30)),
            Column("manager_name", String(50)),
        )

        Table(
            "boss",
            metadata,
            Column(
                "boss_id",
                Integer,
                ForeignKey("managers.person_id"),
                primary_key=True,
            ),
            Column("golf_swing", String(30)),
        )

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
        Person, Engineer, Manager, Boss = cls.classes(
            "Person", "Engineer", "Manager", "Boss"
        )
        p, e, m, b = cls.tables("people", "engineers", "managers", "boss")

        cls.mapper_registry.map_imperatively(
            Person, p, polymorphic_on=p.c.type, polymorphic_identity="person"
        )
        cls.mapper_registry.map_imperatively(
            Engineer, e, inherits=Person, polymorphic_identity="engineer"
        )
        cls.mapper_registry.map_imperatively(
            Manager, m, inherits=Person, polymorphic_identity="manager"
        )
        cls.mapper_registry.map_imperatively(
            Boss, b, inherits=Manager, polymorphic_identity="boss"
        )

    def test_bulk_save_joined_inh_return_defaults(self):
        Person, Engineer, Manager, Boss = self.classes(
            "Person", "Engineer", "Manager", "Boss"
        )

        s = fixture_session()

        objects = [
            Manager(name="m1", status="s1", manager_name="mn1"),
            Engineer(name="e1", status="s2", primary_language="l1"),
            Engineer(name="e2", status="s3", primary_language="l2"),
            Boss(name="b1", status="s3", manager_name="mn2", golf_swing="g1"),
        ]
        assert "person_id" not in objects[0].__dict__

        with self.sql_execution_asserter() as asserter:
            s.bulk_save_objects(objects, return_defaults=True)

        asserter.assert_(
            CompiledSQL(
                "INSERT INTO people (name, type) VALUES (:name, :type)",
                [{"type": "manager", "name": "m1"}],
            ),
            CompiledSQL(
                "INSERT INTO managers (person_id, status, manager_name) "
                "VALUES (:person_id, :status, :manager_name)",
                [{"person_id": 1, "status": "s1", "manager_name": "mn1"}],
            ),
            Conditional(
                testing.db.dialect.insert_executemany_returning,
                [
                    CompiledSQL(
                        "INSERT INTO people (name, type) "
                        "VALUES (:name, :type) RETURNING people.person_id",
                        [
                            {"type": "engineer", "name": "e1"},
                            {"type": "engineer", "name": "e2"},
                        ],
                    ),
                ],
                [
                    CompiledSQL(
                        "INSERT INTO people (name, type) "
                        "VALUES (:name, :type)",
                        [{"type": "engineer", "name": "e1"}],
                    ),
                    CompiledSQL(
                        "INSERT INTO people (name, type) "
                        "VALUES (:name, :type)",
                        [{"type": "engineer", "name": "e2"}],
                    ),
                ],
            ),
            CompiledSQL(
                "INSERT INTO engineers (person_id, status, primary_language) "
                "VALUES (:person_id, :status, :primary_language)",
                [
                    {"person_id": 2, "status": "s2", "primary_language": "l1"},
                    {"person_id": 3, "status": "s3", "primary_language": "l2"},
                ],
            ),
            CompiledSQL(
                "INSERT INTO people (name, type) VALUES (:name, :type)",
                [{"type": "boss", "name": "b1"}],
            ),
            CompiledSQL(
                "INSERT INTO managers (person_id, status, manager_name) "
                "VALUES (:person_id, :status, :manager_name)",
                [{"person_id": 4, "status": "s3", "manager_name": "mn2"}],
            ),
            CompiledSQL(
                "INSERT INTO boss (boss_id, golf_swing) VALUES "
                "(:boss_id, :golf_swing)",
                [{"boss_id": 4, "golf_swing": "g1"}],
            ),
        )
        eq_(objects[0].__dict__["person_id"], 1)
        eq_(objects[3].__dict__["person_id"], 4)
        eq_(objects[3].__dict__["boss_id"], 4)

    def test_bulk_save_joined_inh_no_defaults(self):
        Person, Engineer, Manager, Boss = self.classes(
            "Person", "Engineer", "Manager", "Boss"
        )

        s = fixture_session()
        with self.sql_execution_asserter() as asserter:
            s.bulk_save_objects(
                [
                    Manager(
                        person_id=1, name="m1", status="s1", manager_name="mn1"
                    ),
                    Engineer(
                        person_id=2,
                        name="e1",
                        status="s2",
                        primary_language="l1",
                    ),
                    Engineer(
                        person_id=3,
                        name="e2",
                        status="s3",
                        primary_language="l2",
                    ),
                    Boss(
                        person_id=4,
                        boss_id=4,
                        name="b1",
                        status="s3",
                        manager_name="mn2",
                        golf_swing="g1",
                    ),
                ]
            )

        # the only difference here is that common classes are grouped together.
        # at the moment it doesn't lump all the "people" tables from
        # different classes together.
        asserter.assert_(
            CompiledSQL(
                "INSERT INTO people (person_id, name, type) VALUES "
                "(:person_id, :name, :type)",
                [{"person_id": 1, "type": "manager", "name": "m1"}],
            ),
            CompiledSQL(
                "INSERT INTO managers (person_id, status, manager_name) "
                "VALUES (:person_id, :status, :manager_name)",
                [{"status": "s1", "person_id": 1, "manager_name": "mn1"}],
            ),
            CompiledSQL(
                "INSERT INTO people (person_id, name, type) VALUES "
                "(:person_id, :name, :type)",
                [
                    {"person_id": 2, "type": "engineer", "name": "e1"},
                    {"person_id": 3, "type": "engineer", "name": "e2"},
                ],
            ),
            CompiledSQL(
                "INSERT INTO engineers (person_id, status, primary_language) "
                "VALUES (:person_id, :status, :primary_language)",
                [
                    {"person_id": 2, "status": "s2", "primary_language": "l1"},
                    {"person_id": 3, "status": "s3", "primary_language": "l2"},
                ],
            ),
            CompiledSQL(
                "INSERT INTO people (person_id, name, type) VALUES "
                "(:person_id, :name, :type)",
                [{"person_id": 4, "type": "boss", "name": "b1"}],
            ),
            CompiledSQL(
                "INSERT INTO managers (person_id, status, manager_name) "
                "VALUES (:person_id, :status, :manager_name)",
                [{"status": "s3", "person_id": 4, "manager_name": "mn2"}],
            ),
            CompiledSQL(
                "INSERT INTO boss (boss_id, golf_swing) VALUES "
                "(:boss_id, :golf_swing)",
                [{"boss_id": 4, "golf_swing": "g1"}],
            ),
        )

    @testing.combinations("insert_mappings", "insert_stmt")
    def test_bulk_insert_joined_inh_return_defaults(self, statement_type):
        Person, Engineer, Manager, Boss = self.classes(
            "Person", "Engineer", "Manager", "Boss"
        )

        s = fixture_session()
        data = [
            dict(
                name="b1",
                status="s1",
                manager_name="mn1",
                golf_swing="g1",
            ),
            dict(
                name="b2",
                status="s2",
                manager_name="mn2",
                golf_swing="g2",
            ),
            dict(
                name="b3",
                status="s3",
                manager_name="mn3",
                golf_swing="g3",
            ),
        ]

        if statement_type == "insert_mappings":
            with self.sql_execution_asserter() as asserter:
                s.bulk_insert_mappings(
                    Boss,
                    data,
                    return_defaults=True,
                )
        elif statement_type == "insert_stmt":
            with self.sql_execution_asserter() as asserter:
                s.execute(insert(Boss), data)

        asserter.assert_(
            Conditional(
                testing.db.dialect.insert_executemany_returning,
                [
                    CompiledSQL(
                        "INSERT INTO people (name, type) "
                        "VALUES (:name, :type) RETURNING people.person_id",
                        [
                            {"name": "b1", "type": "boss"},
                            {"name": "b2", "type": "boss"},
                            {"name": "b3", "type": "boss"},
                        ],
                    ),
                ],
                [
                    CompiledSQL(
                        "INSERT INTO people (name, type) "
                        "VALUES (:name, :type)",
                        [{"name": "b1", "type": "boss"}],
                    ),
                    CompiledSQL(
                        "INSERT INTO people (name, type) "
                        "VALUES (:name, :type)",
                        [{"name": "b2", "type": "boss"}],
                    ),
                    CompiledSQL(
                        "INSERT INTO people (name, type) "
                        "VALUES (:name, :type)",
                        [{"name": "b3", "type": "boss"}],
                    ),
                ],
            ),
            CompiledSQL(
                "INSERT INTO managers (person_id, status, manager_name) "
                "VALUES (:person_id, :status, :manager_name)",
                [
                    {"person_id": 1, "status": "s1", "manager_name": "mn1"},
                    {"person_id": 2, "status": "s2", "manager_name": "mn2"},
                    {"person_id": 3, "status": "s3", "manager_name": "mn3"},
                ],
            ),
            CompiledSQL(
                "INSERT INTO boss (boss_id, golf_swing) VALUES "
                "(:boss_id, :golf_swing)",
                [
                    {"golf_swing": "g1", "boss_id": 1},
                    {"golf_swing": "g2", "boss_id": 2},
                    {"golf_swing": "g3", "boss_id": 3},
                ],
            ),
        )

    @testing.combinations("update_mappings", "update_stmt")
    def test_bulk_update(self, statement_type):
        Person, Engineer, Manager, Boss = self.classes(
            "Person", "Engineer", "Manager", "Boss"
        )

        s = fixture_session()

        b1, b2, b3 = (
            Boss(name="b1", status="s1", manager_name="mn1", golf_swing="g1"),
            Boss(name="b2", status="s2", manager_name="mn2", golf_swing="g2"),
            Boss(name="b3", status="s3", manager_name="mn3", golf_swing="g3"),
        )
        s.add_all([b1, b2, b3])
        s.commit()

        # slight non-convenient thing.  we have to fill in boss_id here
        # for update, this is not sent along automatically.  this is not a
        # new behavior in bulk
        new_data = [
            {
                "person_id": b1.person_id,
                "boss_id": b1.boss_id,
                "name": "b1_updated",
                "manager_name": "mn1_updated",
            },
            {
                "person_id": b3.person_id,
                "boss_id": b3.boss_id,
                "manager_name": "mn2_updated",
                "golf_swing": "g1_updated",
            },
        ]

        if statement_type == "update_mappings":
            with self.sql_execution_asserter() as asserter:
                s.bulk_update_mappings(Boss, new_data)
        elif statement_type == "update_stmt":
            with self.sql_execution_asserter() as asserter:
                s.execute(update(Boss), new_data)

        asserter.assert_(
            CompiledSQL(
                "UPDATE people SET name=:name WHERE "
                "people.person_id = :people_person_id",
                [{"name": "b1_updated", "people_person_id": 1}],
            ),
            CompiledSQL(
                "UPDATE managers SET manager_name=:manager_name WHERE "
                "managers.person_id = :managers_person_id",
                [
                    {"manager_name": "mn1_updated", "managers_person_id": 1},
                    {"manager_name": "mn2_updated", "managers_person_id": 3},
                ],
            ),
            CompiledSQL(
                "UPDATE boss SET golf_swing=:golf_swing WHERE "
                "boss.boss_id = :boss_boss_id",
                [{"golf_swing": "g1_updated", "boss_boss_id": 3}],
            ),
        )


class BulkIssue6793Test(BulkTest, fixtures.DeclarativeMappedTest):
    __backend__ = True

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class User(Base):
            __tablename__ = "users"
            id = Column(Integer, Identity(), primary_key=True)
            name = Column(String(255), nullable=False)

    def test_issue_6793(self):
        User = self.classes.User

        session = fixture_session()

        with self.sql_execution_asserter() as asserter:
            session.bulk_save_objects([User(name="A"), User(name="B")])

            session.add(User(name="C"))
            session.add(User(name="D"))
            session.flush()

        asserter.assert_(
            Conditional(
                testing.db.dialect.insert_executemany_returning,
                [
                    CompiledSQL(
                        "INSERT INTO users (name) VALUES (:name)",
                        [{"name": "A"}, {"name": "B"}],
                    ),
                    CompiledSQL(
                        "INSERT INTO users (name) VALUES (:name) "
                        "RETURNING users.id",
                        [{"name": "C"}, {"name": "D"}],
                    ),
                ],
                [
                    CompiledSQL(
                        "INSERT INTO users (name) VALUES (:name)",
                        [{"name": "A"}, {"name": "B"}],
                    ),
                    CompiledSQL(
                        "INSERT INTO users (name) VALUES (:name)",
                        [{"name": "C"}],
                    ),
                    CompiledSQL(
                        "INSERT INTO users (name) VALUES (:name)",
                        [{"name": "D"}],
                    ),
                ],
            )
        )
