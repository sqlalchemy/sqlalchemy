from sqlalchemy import exc as sa_exc
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy.ext import declarative as decl
from sqlalchemy.ext.declarative import AbstractConcreteBase
from sqlalchemy.ext.declarative import ConcreteBase
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.ext.declarative import has_inherited_table
from sqlalchemy.orm import clear_mappers
from sqlalchemy.orm import close_all_sessions
from sqlalchemy.orm import configure_mappers
from sqlalchemy.orm import exc as orm_exc
from sqlalchemy.orm import polymorphic_union
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import mock
from sqlalchemy.testing.assertions import expect_raises_message
from sqlalchemy.testing.fixtures import fixture_session
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table
from test.orm.test_events import _RemoveListeners

Base = None


class DeclarativeTestBase(fixtures.TestBase, testing.AssertsExecutionResults):
    __dialect__ = "default"

    def setup_test(self):
        global Base
        Base = decl.declarative_base(testing.db)

    def teardown_test(self):
        close_all_sessions()
        clear_mappers()
        Base.metadata.drop_all(testing.db)


class ConcreteInhTest(
    _RemoveListeners, DeclarativeTestBase, testing.AssertsCompiledSQL
):
    def _roundtrip(
        self,
        Employee,
        Manager,
        Engineer,
        Boss,
        polymorphic=True,
        explicit_type=False,
    ):
        Base.metadata.create_all(testing.db)
        sess = fixture_session()
        e1 = Engineer(name="dilbert", primary_language="java")
        e2 = Engineer(name="wally", primary_language="c++")
        m1 = Manager(name="dogbert", golf_swing="fore!")
        e3 = Engineer(name="vlad", primary_language="cobol")
        b1 = Boss(name="pointy haired")

        if polymorphic:
            for obj in [e1, e2, m1, e3, b1]:
                if explicit_type:
                    eq_(obj.type, obj.__mapper__.polymorphic_identity)
                else:
                    assert_raises_message(
                        AttributeError,
                        "does not implement attribute .?'type' "
                        "at the instance level.",
                        getattr,
                        obj,
                        "type",
                    )
        else:
            assert "type" not in Engineer.__dict__
            assert "type" not in Manager.__dict__
            assert "type" not in Boss.__dict__

        sess.add_all([e1, e2, m1, e3, b1])
        sess.flush()
        sess.expunge_all()
        if polymorphic:
            eq_(
                sess.query(Employee).order_by(Employee.name).all(),
                [
                    Engineer(name="dilbert"),
                    Manager(name="dogbert"),
                    Boss(name="pointy haired"),
                    Engineer(name="vlad"),
                    Engineer(name="wally"),
                ],
            )
        else:
            eq_(
                sess.query(Engineer).order_by(Engineer.name).all(),
                [
                    Engineer(name="dilbert"),
                    Engineer(name="vlad"),
                    Engineer(name="wally"),
                ],
            )
            eq_(sess.query(Manager).all(), [Manager(name="dogbert")])
            eq_(sess.query(Boss).all(), [Boss(name="pointy haired")])

        e1 = sess.query(Engineer).order_by(Engineer.name).first()
        sess.expire(e1)
        eq_(e1.name, "dilbert")

    def test_explicit(self):
        engineers = Table(
            "engineers",
            Base.metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("name", String(50)),
            Column("primary_language", String(50)),
        )
        managers = Table(
            "managers",
            Base.metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("name", String(50)),
            Column("golf_swing", String(50)),
        )
        boss = Table(
            "boss",
            Base.metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("name", String(50)),
            Column("golf_swing", String(50)),
        )
        punion = polymorphic_union(
            {"engineer": engineers, "manager": managers, "boss": boss},
            "type",
            "punion",
        )

        class Employee(Base, fixtures.ComparableEntity):

            __table__ = punion
            __mapper_args__ = {"polymorphic_on": punion.c.type}

        class Engineer(Employee):

            __table__ = engineers
            __mapper_args__ = {
                "polymorphic_identity": "engineer",
                "concrete": True,
            }

        class Manager(Employee):

            __table__ = managers
            __mapper_args__ = {
                "polymorphic_identity": "manager",
                "concrete": True,
            }

        class Boss(Manager):
            __table__ = boss
            __mapper_args__ = {
                "polymorphic_identity": "boss",
                "concrete": True,
            }

        self._roundtrip(Employee, Manager, Engineer, Boss)

    def test_concrete_inline_non_polymorphic(self):
        """test the example from the declarative docs."""

        class Employee(Base, fixtures.ComparableEntity):

            __tablename__ = "people"
            id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )
            name = Column(String(50))

        class Engineer(Employee):

            __tablename__ = "engineers"
            __mapper_args__ = {"concrete": True}
            id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )
            primary_language = Column(String(50))
            name = Column(String(50))

        class Manager(Employee):

            __tablename__ = "manager"
            __mapper_args__ = {"concrete": True}
            id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )
            golf_swing = Column(String(50))
            name = Column(String(50))

        class Boss(Manager):
            __tablename__ = "boss"
            __mapper_args__ = {"concrete": True}
            id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )
            golf_swing = Column(String(50))
            name = Column(String(50))

        self._roundtrip(Employee, Manager, Engineer, Boss, polymorphic=False)

    def test_abstract_concrete_base_didnt_configure(self):
        class Employee(AbstractConcreteBase, Base, fixtures.ComparableEntity):
            pass

        assert_raises_message(
            orm_exc.UnmappedClassError,
            "Class test.ext.declarative.test_inheritance.Employee is a "
            "subclass of AbstractConcreteBase and has a mapping pending "
            "until all subclasses are defined. Call the "
            r"sqlalchemy.orm.configure_mappers\(\) function after all "
            "subclasses have been defined to complete the "
            "mapping of this class.",
            Session().query,
            Employee,
        )

        Base.registry.configure()

        # no subclasses yet.
        assert_raises_message(
            orm_exc.UnmappedClassError,
            ".*and has a mapping pending",
            Session().query,
            Employee,
        )

        class Manager(Employee):
            __tablename__ = "manager"
            employee_id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )
            name = Column(String(50))
            golf_swing = Column(String(40))
            __mapper_args__ = {
                "polymorphic_identity": "manager",
                "concrete": True,
            }

        # didnt call configure_mappers() again
        assert_raises_message(
            orm_exc.UnmappedClassError,
            ".*and has a mapping pending",
            Session().query,
            Employee,
        )

        Base.registry.configure()

        self.assert_compile(
            Session().query(Employee),
            "SELECT pjoin.employee_id AS pjoin_employee_id, "
            "pjoin.name AS pjoin_name, pjoin.golf_swing AS pjoin_golf_swing, "
            "pjoin.type AS pjoin_type FROM (SELECT manager.employee_id "
            "AS employee_id, manager.name AS name, manager.golf_swing AS "
            "golf_swing, 'manager' AS type FROM manager) AS pjoin",
        )

    def test_abstract_concrete_extension(self):
        class Employee(AbstractConcreteBase, Base, fixtures.ComparableEntity):
            pass

        class Manager(Employee):
            __tablename__ = "manager"
            employee_id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )
            name = Column(String(50))
            golf_swing = Column(String(40))
            __mapper_args__ = {
                "polymorphic_identity": "manager",
                "concrete": True,
            }

        class Boss(Manager):
            __tablename__ = "boss"
            employee_id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )
            name = Column(String(50))
            golf_swing = Column(String(40))
            __mapper_args__ = {
                "polymorphic_identity": "boss",
                "concrete": True,
            }

        class Engineer(Employee):
            __tablename__ = "engineer"
            employee_id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )
            name = Column(String(50))
            primary_language = Column(String(40))
            __mapper_args__ = {
                "polymorphic_identity": "engineer",
                "concrete": True,
            }

        self._roundtrip(Employee, Manager, Engineer, Boss)

    def test_abstract_concrete_extension_descriptor_refresh(self):
        class Employee(AbstractConcreteBase, Base, fixtures.ComparableEntity):
            @declared_attr
            def name(cls):
                return Column(String(50))

        class Manager(Employee):
            __tablename__ = "manager"
            employee_id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )
            paperwork = Column(String(10))
            __mapper_args__ = {
                "polymorphic_identity": "manager",
                "concrete": True,
            }

        class Engineer(Employee):
            __tablename__ = "engineer"
            employee_id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )

            @property
            def paperwork(self):
                return "p"

            __mapper_args__ = {
                "polymorphic_identity": "engineer",
                "concrete": True,
            }

        Base.metadata.create_all(testing.db)
        sess = Session()
        sess.add(Engineer(name="d"))
        sess.commit()

        # paperwork is excluded because there's a descritor; so it is
        # not in the Engineers mapped properties at all, though is inside the
        # class manager.   Maybe it shouldn't be in the class manager either.
        assert "paperwork" in Engineer.__mapper__.class_manager
        assert "paperwork" not in Engineer.__mapper__.attrs.keys()

        # type currently does get mapped, as a
        # ConcreteInheritedProperty, which means, "ignore this thing inherited
        # from the concrete base".   if we didn't specify concrete=True, then
        # this one gets stuck in the error condition also.
        assert "type" in Engineer.__mapper__.class_manager
        assert "type" in Engineer.__mapper__.attrs.keys()

        e1 = sess.query(Engineer).first()
        eq_(e1.name, "d")
        sess.expire(e1)
        eq_(e1.name, "d")

    def test_concrete_extension(self):
        class Employee(ConcreteBase, Base, fixtures.ComparableEntity):
            __tablename__ = "employee"
            employee_id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )
            name = Column(String(50))
            __mapper_args__ = {
                "polymorphic_identity": "employee",
                "concrete": True,
            }

        class Manager(Employee):
            __tablename__ = "manager"
            employee_id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )
            name = Column(String(50))
            golf_swing = Column(String(40))
            __mapper_args__ = {
                "polymorphic_identity": "manager",
                "concrete": True,
            }

        class Boss(Manager):
            __tablename__ = "boss"
            employee_id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )
            name = Column(String(50))
            golf_swing = Column(String(40))
            __mapper_args__ = {
                "polymorphic_identity": "boss",
                "concrete": True,
            }

        class Engineer(Employee):
            __tablename__ = "engineer"
            employee_id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )
            name = Column(String(50))
            primary_language = Column(String(40))
            __mapper_args__ = {
                "polymorphic_identity": "engineer",
                "concrete": True,
            }

        self._roundtrip(Employee, Manager, Engineer, Boss)

    def test_concrete_extension_warn_for_overlap(self):
        class Employee(ConcreteBase, Base, fixtures.ComparableEntity):
            __tablename__ = "employee"

            employee_id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )
            name = Column(String(50))
            __mapper_args__ = {
                "polymorphic_identity": "employee",
                "concrete": True,
            }

        class Manager(Employee):
            __tablename__ = "manager"
            employee_id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )
            type = Column(String(50))
            __mapper_args__ = {
                "polymorphic_identity": "manager",
                "concrete": True,
            }

        with expect_raises_message(
            sa_exc.InvalidRequestError,
            "Polymorphic union can't use 'type' as the discriminator "
            "column due to mapped column "
            r"Column\('type', String\(length=50\), table=<manager>\); "
            "please "
            "apply the 'typecolname' argument; this is available on "
            "ConcreteBase as '_concrete_discriminator_name'",
        ):
            configure_mappers()

    def test_concrete_extension_warn_concrete_disc_resolves_overlap(self):
        class Employee(ConcreteBase, Base, fixtures.ComparableEntity):
            _concrete_discriminator_name = "_type"

            __tablename__ = "employee"

            employee_id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )
            name = Column(String(50))
            __mapper_args__ = {
                "polymorphic_identity": "employee",
                "concrete": True,
            }

        class Manager(Employee):
            __tablename__ = "manager"
            employee_id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )
            type = Column(String(50))
            __mapper_args__ = {
                "polymorphic_identity": "manager",
                "concrete": True,
            }

        configure_mappers()
        self.assert_compile(
            select(Employee),
            "SELECT pjoin.employee_id, pjoin.name, pjoin._type, pjoin.type "
            "FROM (SELECT employee.employee_id AS employee_id, "
            "employee.name AS name, CAST(NULL AS VARCHAR(50)) AS type, "
            "'employee' AS _type FROM employee UNION ALL "
            "SELECT manager.employee_id AS employee_id, "
            "CAST(NULL AS VARCHAR(50)) AS name, manager.type AS type, "
            "'manager' AS _type FROM manager) AS pjoin",
        )

    def test_abs_concrete_extension_warn_for_overlap(self):
        class Employee(AbstractConcreteBase, Base, fixtures.ComparableEntity):
            name = Column(String(50))
            __mapper_args__ = {
                "polymorphic_identity": "employee",
                "concrete": True,
            }

        class Manager(Employee):
            __tablename__ = "manager"
            employee_id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )
            type = Column(String(50))
            __mapper_args__ = {
                "polymorphic_identity": "manager",
                "concrete": True,
            }

        with expect_raises_message(
            sa_exc.InvalidRequestError,
            "Polymorphic union can't use 'type' as the discriminator "
            "column due to mapped column "
            r"Column\('type', String\(length=50\), table=<manager>\); "
            "please "
            "apply the 'typecolname' argument; this is available on "
            "ConcreteBase as '_concrete_discriminator_name'",
        ):
            configure_mappers()

    def test_abs_concrete_extension_warn_concrete_disc_resolves_overlap(self):
        class Employee(AbstractConcreteBase, Base, fixtures.ComparableEntity):
            _concrete_discriminator_name = "_type"

            name = Column(String(50))
            __mapper_args__ = {
                "polymorphic_identity": "employee",
                "concrete": True,
            }

        class Manager(Employee):
            __tablename__ = "manager"
            employee_id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )
            type = Column(String(50))
            __mapper_args__ = {
                "polymorphic_identity": "manager",
                "concrete": True,
            }

        configure_mappers()
        self.assert_compile(
            select(Employee),
            "SELECT pjoin.name, pjoin.employee_id, pjoin.type, pjoin._type "
            "FROM (SELECT manager.name AS name, manager.employee_id AS "
            "employee_id, manager.type AS type, 'manager' AS _type "
            "FROM manager) AS pjoin",
        )

    def test_has_inherited_table_doesnt_consider_base(self):
        class A(Base):
            __tablename__ = "a"
            id = Column(Integer, primary_key=True)

        assert not has_inherited_table(A)

        class B(A):
            __tablename__ = "b"
            id = Column(Integer, ForeignKey("a.id"), primary_key=True)

        assert has_inherited_table(B)

    def test_has_inherited_table_in_mapper_args(self):
        class Test(Base):
            __tablename__ = "test"
            id = Column(Integer, primary_key=True)
            type = Column(String(20))

            @declared_attr
            def __mapper_args__(cls):
                if not has_inherited_table(cls):
                    ret = {
                        "polymorphic_identity": "default",
                        "polymorphic_on": cls.type,
                    }
                else:
                    ret = {"polymorphic_identity": cls.__name__}
                return ret

        class PolyTest(Test):
            __tablename__ = "poly_test"
            id = Column(Integer, ForeignKey(Test.id), primary_key=True)

        configure_mappers()

        assert Test.__mapper__.polymorphic_on is Test.__table__.c.type
        assert PolyTest.__mapper__.polymorphic_on is Test.__table__.c.type

    def test_ok_to_override_type_from_abstract(self):
        class Employee(AbstractConcreteBase, Base, fixtures.ComparableEntity):
            pass

        class Manager(Employee):
            __tablename__ = "manager"
            employee_id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )
            name = Column(String(50))
            golf_swing = Column(String(40))

            @property
            def type(self):
                return "manager"

            __mapper_args__ = {
                "polymorphic_identity": "manager",
                "concrete": True,
            }

        class Boss(Manager):
            __tablename__ = "boss"
            employee_id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )
            name = Column(String(50))
            golf_swing = Column(String(40))

            @property
            def type(self):
                return "boss"

            __mapper_args__ = {
                "polymorphic_identity": "boss",
                "concrete": True,
            }

        class Engineer(Employee):
            __tablename__ = "engineer"
            employee_id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )
            name = Column(String(50))
            primary_language = Column(String(40))

            @property
            def type(self):
                return "engineer"

            __mapper_args__ = {
                "polymorphic_identity": "engineer",
                "concrete": True,
            }

        self._roundtrip(Employee, Manager, Engineer, Boss, explicit_type=True)


class ConcreteExtensionConfigTest(
    _RemoveListeners, testing.AssertsCompiledSQL, DeclarativeTestBase
):
    __dialect__ = "default"

    def test_classreg_setup(self):
        class A(Base, fixtures.ComparableEntity):
            __tablename__ = "a"
            id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )
            data = Column(String(50))
            collection = relationship(
                "BC", primaryjoin="BC.a_id == A.id", collection_class=set
            )

        class BC(AbstractConcreteBase, Base, fixtures.ComparableEntity):
            pass

        class B(BC):
            __tablename__ = "b"
            id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )

            a_id = Column(Integer, ForeignKey("a.id"))
            data = Column(String(50))
            b_data = Column(String(50))
            __mapper_args__ = {"polymorphic_identity": "b", "concrete": True}

        class C(BC):
            __tablename__ = "c"
            id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True
            )
            a_id = Column(Integer, ForeignKey("a.id"))
            data = Column(String(50))
            c_data = Column(String(50))
            __mapper_args__ = {"polymorphic_identity": "c", "concrete": True}

        Base.metadata.create_all(testing.db)
        sess = Session()
        sess.add_all(
            [
                A(
                    data="a1",
                    collection=set(
                        [
                            B(data="a1b1", b_data="a1b1"),
                            C(data="a1b2", c_data="a1c1"),
                            B(data="a1b2", b_data="a1b2"),
                            C(data="a1c2", c_data="a1c2"),
                        ]
                    ),
                ),
                A(
                    data="a2",
                    collection=set(
                        [
                            B(data="a2b1", b_data="a2b1"),
                            C(data="a2c1", c_data="a2c1"),
                            B(data="a2b2", b_data="a2b2"),
                            C(data="a2c2", c_data="a2c2"),
                        ]
                    ),
                ),
            ]
        )
        sess.commit()
        sess.expunge_all()

        eq_(
            sess.query(A).filter_by(data="a2").all(),
            [
                A(
                    data="a2",
                    collection=set(
                        [
                            B(data="a2b1", b_data="a2b1"),
                            B(data="a2b2", b_data="a2b2"),
                            C(data="a2c1", c_data="a2c1"),
                            C(data="a2c2", c_data="a2c2"),
                        ]
                    ),
                )
            ],
        )

        self.assert_compile(
            sess.query(A).join(A.collection),
            "SELECT a.id AS a_id, a.data AS a_data FROM a JOIN "
            "(SELECT c.id AS id, c.a_id AS a_id, c.data AS data, "
            "c.c_data AS c_data, CAST(NULL AS VARCHAR(50)) AS b_data, "
            "'c' AS type FROM c UNION ALL SELECT b.id AS id, b.a_id AS a_id, "
            "b.data AS data, CAST(NULL AS VARCHAR(50)) AS c_data, "
            "b.b_data AS b_data, 'b' AS type FROM b) AS pjoin "
            "ON pjoin.a_id = a.id",
        )

    def test_prop_on_base(self):
        """test [ticket:2670] """

        counter = mock.Mock()

        class Something(Base):
            __tablename__ = "something"
            id = Column(Integer, primary_key=True)

        class AbstractConcreteAbstraction(AbstractConcreteBase, Base):
            id = Column(Integer, primary_key=True)
            x = Column(Integer)
            y = Column(Integer)

            @declared_attr
            def something_id(cls):
                return Column(ForeignKey(Something.id))

            @declared_attr
            def something(cls):
                counter(cls, "something")
                return relationship("Something")

            @declared_attr
            def something_else(cls):
                counter(cls, "something_else")
                return relationship("Something", viewonly=True)

        class ConcreteConcreteAbstraction(AbstractConcreteAbstraction):
            __tablename__ = "cca"
            __mapper_args__ = {"polymorphic_identity": "ccb", "concrete": True}

        # concrete is mapped, the abstract base is not (yet)
        assert ConcreteConcreteAbstraction.__mapper__
        assert not hasattr(AbstractConcreteAbstraction, "__mapper__")

        session = Session()
        self.assert_compile(
            session.query(ConcreteConcreteAbstraction).filter(
                ConcreteConcreteAbstraction.something.has(id=1)
            ),
            "SELECT cca.id AS cca_id, cca.x AS cca_x, cca.y AS cca_y, "
            "cca.something_id AS cca_something_id FROM cca WHERE EXISTS "
            "(SELECT 1 FROM something WHERE something.id = cca.something_id "
            "AND something.id = :id_1)",
        )

        # now it is
        assert AbstractConcreteAbstraction.__mapper__

        self.assert_compile(
            session.query(ConcreteConcreteAbstraction).filter(
                ConcreteConcreteAbstraction.something_else.has(id=1)
            ),
            "SELECT cca.id AS cca_id, cca.x AS cca_x, cca.y AS cca_y, "
            "cca.something_id AS cca_something_id FROM cca WHERE EXISTS "
            "(SELECT 1 FROM something WHERE something.id = cca.something_id "
            "AND something.id = :id_1)",
        )

        self.assert_compile(
            session.query(AbstractConcreteAbstraction).filter(
                AbstractConcreteAbstraction.something.has(id=1)
            ),
            "SELECT pjoin.id AS pjoin_id, pjoin.x AS pjoin_x, "
            "pjoin.y AS pjoin_y, pjoin.something_id AS pjoin_something_id, "
            "pjoin.type AS pjoin_type FROM "
            "(SELECT cca.id AS id, cca.x AS x, cca.y AS y, "
            "cca.something_id AS something_id, 'ccb' AS type FROM cca) "
            "AS pjoin WHERE EXISTS (SELECT 1 FROM something "
            "WHERE something.id = pjoin.something_id "
            "AND something.id = :id_1)",
        )

        self.assert_compile(
            session.query(AbstractConcreteAbstraction).filter(
                AbstractConcreteAbstraction.something_else.has(id=1)
            ),
            "SELECT pjoin.id AS pjoin_id, pjoin.x AS pjoin_x, "
            "pjoin.y AS pjoin_y, pjoin.something_id AS pjoin_something_id, "
            "pjoin.type AS pjoin_type FROM "
            "(SELECT cca.id AS id, cca.x AS x, cca.y AS y, "
            "cca.something_id AS something_id, 'ccb' AS type FROM cca) "
            "AS pjoin WHERE EXISTS (SELECT 1 FROM something "
            "WHERE something.id = pjoin.something_id AND "
            "something.id = :id_1)",
        )

    def test_abstract_in_hierarchy(self):
        class Document(Base, AbstractConcreteBase):
            doctype = Column(String)

        class ContactDocument(Document):
            __abstract__ = True

            send_method = Column(String)

        class ActualDocument(ContactDocument):
            __tablename__ = "actual_documents"
            __mapper_args__ = {
                "concrete": True,
                "polymorphic_identity": "actual",
            }

            id = Column(Integer, primary_key=True)

        configure_mappers()
        session = Session()
        self.assert_compile(
            session.query(Document),
            "SELECT pjoin.doctype AS pjoin_doctype, "
            "pjoin.send_method AS pjoin_send_method, "
            "pjoin.id AS pjoin_id, pjoin.type AS pjoin_type "
            "FROM (SELECT actual_documents.doctype AS doctype, "
            "actual_documents.send_method AS send_method, "
            "actual_documents.id AS id, 'actual' AS type "
            "FROM actual_documents) AS pjoin",
        )

    def test_column_attr_names(self):
        """test #3480"""

        class Document(Base, AbstractConcreteBase):
            documentType = Column("documenttype", String)

        class Offer(Document):
            __tablename__ = "offers"

            id = Column(Integer, primary_key=True)
            __mapper_args__ = {"polymorphic_identity": "offer"}

        configure_mappers()
        session = Session()
        self.assert_compile(
            session.query(Document),
            "SELECT pjoin.documenttype AS pjoin_documenttype, "
            "pjoin.id AS pjoin_id, pjoin.type AS pjoin_type FROM "
            "(SELECT offers.documenttype AS documenttype, offers.id AS id, "
            "'offer' AS type FROM offers) AS pjoin",
        )

        self.assert_compile(
            session.query(Document.documentType),
            "SELECT pjoin.documenttype AS pjoin_documenttype FROM "
            "(SELECT offers.documenttype AS documenttype, offers.id AS id, "
            "'offer' AS type FROM offers) AS pjoin",
        )

    def test_configure_discriminator_col(self):
        """test #5513"""

        class Employee(AbstractConcreteBase, Base):
            _concrete_discriminator_name = "_alt_discriminator"
            employee_id = Column(Integer, primary_key=True)

        class Manager(Employee):
            __tablename__ = "manager"

            __mapper_args__ = {
                "polymorphic_identity": "manager",
                "concrete": True,
            }

        class Engineer(Employee):
            __tablename__ = "engineer"

            __mapper_args__ = {
                "polymorphic_identity": "engineer",
                "concrete": True,
            }

        configure_mappers()
        self.assert_compile(
            Session().query(Employee),
            "SELECT pjoin.employee_id AS pjoin_employee_id, "
            "pjoin._alt_discriminator AS pjoin__alt_discriminator "
            "FROM (SELECT engineer.employee_id AS employee_id, "
            "'engineer' AS _alt_discriminator FROM engineer "
            "UNION ALL SELECT manager.employee_id AS employee_id, "
            "'manager' AS _alt_discriminator "
            "FROM manager) AS pjoin",
        )
