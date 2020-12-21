import random
import threading
import time

from sqlalchemy import create_engine
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.ext.automap import generate_relationship
from sqlalchemy.orm import configure_mappers
from sqlalchemy.orm import exc as orm_exc
from sqlalchemy.orm import interfaces
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.mock import Mock
from sqlalchemy.testing.mock import patch
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table
from ..orm._fixtures import FixtureTest


class AutomapTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        FixtureTest.define_tables(metadata)

    def test_relationship_o2m_default(self):
        Base = automap_base(metadata=self.tables_test_metadata)
        Base.prepare()

        User = Base.classes.users
        Address = Base.classes.addresses

        a1 = Address(email_address="e1")
        u1 = User(name="u1", addresses_collection=[a1])
        assert a1.users is u1

    def test_relationship_explicit_override_o2m(self):
        Base = automap_base(metadata=self.tables_test_metadata)
        prop = relationship("addresses", collection_class=set)

        class User(Base):
            __tablename__ = "users"

            addresses_collection = prop

        Base.prepare()
        assert User.addresses_collection.property is prop
        Address = Base.classes.addresses

        a1 = Address(email_address="e1")
        u1 = User(name="u1", addresses_collection=set([a1]))
        assert a1.user is u1

    def test_exception_prepare_not_called(self):
        Base = automap_base(metadata=self.tables_test_metadata)

        class User(Base):
            __tablename__ = "users"

        s = Session()

        assert_raises_message(
            orm_exc.UnmappedClassError,
            "Class test.ext.test_automap.User is a subclass of AutomapBase.  "
            r"Mappings are not produced until the .prepare\(\) method is "
            "called on the class hierarchy.",
            s.query,
            User,
        )

    def test_relationship_explicit_override_m2o(self):
        Base = automap_base(metadata=self.tables_test_metadata)

        prop = relationship("users")

        class Address(Base):
            __tablename__ = "addresses"

            users = prop

        Base.prepare()
        User = Base.classes.users

        assert Address.users.property is prop
        a1 = Address(email_address="e1")
        u1 = User(name="u1", address_collection=[a1])
        assert a1.users is u1

    def test_relationship_self_referential(self):
        Base = automap_base(metadata=self.tables_test_metadata)
        Base.prepare()

        Node = Base.classes.nodes

        n1 = Node()
        n2 = Node()
        n1.nodes_collection.append(n2)
        assert n2.nodes is n1

    def test_prepare_accepts_optional_schema_arg(self):
        """
        The underlying reflect call accepts an optional schema argument.
        This is for determining which database schema to load.
        This test verifies that prepare can accept an optional schema
        argument and pass it to reflect.
        """
        Base = automap_base(metadata=self.tables_test_metadata)
        engine_mock = Mock()
        with patch.object(Base.metadata, "reflect") as reflect_mock:
            Base.prepare(autoload_with=engine_mock, schema="some_schema")
            reflect_mock.assert_called_once_with(
                engine_mock,
                schema="some_schema",
                extend_existing=True,
                autoload_replace=False,
            )

    def test_prepare_defaults_to_no_schema(self):
        """
        The underlying reflect call accepts an optional schema argument.
        This is for determining which database schema to load.
        This test verifies that prepare passes a default None if no schema is
        provided.
        """
        Base = automap_base(metadata=self.tables_test_metadata)
        engine_mock = Mock()
        with patch.object(Base.metadata, "reflect") as reflect_mock:
            Base.prepare(autoload_with=engine_mock)
            reflect_mock.assert_called_once_with(
                engine_mock,
                schema=None,
                extend_existing=True,
                autoload_replace=False,
            )

    def test_naming_schemes(self):
        Base = automap_base(metadata=self.tables_test_metadata)

        def classname_for_table(base, tablename, table):
            return str("cls_" + tablename)

        def name_for_scalar_relationship(
            base, local_cls, referred_cls, constraint
        ):
            return "scalar_" + referred_cls.__name__

        def name_for_collection_relationship(
            base, local_cls, referred_cls, constraint
        ):
            return "coll_" + referred_cls.__name__

        Base.prepare(
            classname_for_table=classname_for_table,
            name_for_scalar_relationship=name_for_scalar_relationship,
            name_for_collection_relationship=name_for_collection_relationship,
        )

        User = Base.classes.cls_users
        Address = Base.classes.cls_addresses

        u1 = User()
        a1 = Address()
        u1.coll_cls_addresses.append(a1)
        assert a1.scalar_cls_users is u1

    def test_relationship_m2m(self):
        Base = automap_base(metadata=self.tables_test_metadata)

        Base.prepare()

        Order, Item = Base.classes.orders, Base.classes["items"]

        o1 = Order()
        i1 = Item()
        o1.items_collection.append(i1)
        assert o1 in i1.orders_collection

    def test_relationship_explicit_override_forwards_m2m(self):
        Base = automap_base(metadata=self.tables_test_metadata)

        class Order(Base):
            __tablename__ = "orders"

            items_collection = relationship(
                "items", secondary="order_items", collection_class=set
            )

        Base.prepare()

        Item = Base.classes["items"]

        o1 = Order()
        i1 = Item()
        o1.items_collection.add(i1)

        # it is 'order_collection' because the class name is
        # "Order" !
        assert isinstance(i1.order_collection, list)
        assert o1 in i1.order_collection

    def test_relationship_pass_params(self):
        Base = automap_base(metadata=self.tables_test_metadata)

        mock = Mock()

        def _gen_relationship(
            base, direction, return_fn, attrname, local_cls, referred_cls, **kw
        ):
            mock(base, direction, attrname)
            return generate_relationship(
                base,
                direction,
                return_fn,
                attrname,
                local_cls,
                referred_cls,
                **kw
            )

        Base.prepare(generate_relationship=_gen_relationship)
        assert set(tuple(c[1]) for c in mock.mock_calls).issuperset(
            [
                (Base, interfaces.MANYTOONE, "nodes"),
                (Base, interfaces.MANYTOMANY, "keywords_collection"),
                (Base, interfaces.MANYTOMANY, "items_collection"),
                (Base, interfaces.MANYTOONE, "users"),
                (Base, interfaces.ONETOMANY, "addresses_collection"),
            ]
        )


class CascadeTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table("a", metadata, Column("id", Integer, primary_key=True))
        Table(
            "b",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("aid", ForeignKey("a.id"), nullable=True),
        )
        Table(
            "c",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("aid", ForeignKey("a.id"), nullable=False),
        )
        Table(
            "d",
            metadata,
            Column("id", Integer, primary_key=True),
            Column(
                "aid", ForeignKey("a.id", ondelete="cascade"), nullable=False
            ),
        )
        Table(
            "e",
            metadata,
            Column("id", Integer, primary_key=True),
            Column(
                "aid", ForeignKey("a.id", ondelete="set null"), nullable=True
            ),
        )

    def test_o2m_relationship_cascade(self):
        Base = automap_base(metadata=self.tables_test_metadata)
        Base.prepare()

        configure_mappers()

        b_rel = Base.classes.a.b_collection
        assert not b_rel.property.cascade.delete
        assert not b_rel.property.cascade.delete_orphan
        assert not b_rel.property.passive_deletes

        assert b_rel.property.cascade.save_update

        c_rel = Base.classes.a.c_collection
        assert c_rel.property.cascade.delete
        assert c_rel.property.cascade.delete_orphan
        assert not c_rel.property.passive_deletes

        assert c_rel.property.cascade.save_update

        d_rel = Base.classes.a.d_collection
        assert d_rel.property.cascade.delete
        assert d_rel.property.cascade.delete_orphan
        assert d_rel.property.passive_deletes

        assert d_rel.property.cascade.save_update

        e_rel = Base.classes.a.e_collection
        assert not e_rel.property.cascade.delete
        assert not e_rel.property.cascade.delete_orphan
        assert e_rel.property.passive_deletes

        assert e_rel.property.cascade.save_update


class AutomapInhTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "single",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("type", String(10)),
            test_needs_fk=True,
        )

        Table(
            "joined_base",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("type", String(10)),
            test_needs_fk=True,
        )

        Table(
            "joined_inh",
            metadata,
            Column(
                "id", Integer, ForeignKey("joined_base.id"), primary_key=True
            ),
            test_needs_fk=True,
        )

        FixtureTest.define_tables(metadata)

    def test_single_inheritance_reflect(self):
        Base = automap_base()

        class Single(Base):
            __tablename__ = "single"

            type = Column(String)

            __mapper_args__ = {
                "polymorphic_identity": "u0",
                "polymorphic_on": type,
            }

        class SubUser1(Single):
            __mapper_args__ = {"polymorphic_identity": "u1"}

        class SubUser2(Single):
            __mapper_args__ = {"polymorphic_identity": "u2"}

        Base.prepare(autoload_with=testing.db)

        assert SubUser2.__mapper__.inherits is Single.__mapper__

    def test_joined_inheritance_reflect(self):
        Base = automap_base()

        class Joined(Base):
            __tablename__ = "joined_base"

            type = Column(String)

            __mapper_args__ = {
                "polymorphic_identity": "u0",
                "polymorphic_on": type,
            }

        class SubJoined(Joined):
            __tablename__ = "joined_inh"
            __mapper_args__ = {"polymorphic_identity": "u1"}

        Base.prepare(autoload_with=testing.db)

        assert SubJoined.__mapper__.inherits is Joined.__mapper__

        assert not Joined.__mapper__.relationships
        assert not SubJoined.__mapper__.relationships

    def test_conditional_relationship(self):
        Base = automap_base()

        def _gen_relationship(*arg, **kw):
            return None

        Base.prepare(
            autoload_with=testing.db,
            generate_relationship=_gen_relationship,
        )


class ConcurrentAutomapTest(fixtures.TestBase):
    __only_on__ = "sqlite"

    def _make_tables(self, e):
        m = MetaData()
        for i in range(15):
            Table(
                "table_%d" % i,
                m,
                Column("id", Integer, primary_key=True),
                Column("data", String(50)),
                Column(
                    "t_%d_id" % (i - 1), ForeignKey("table_%d.id" % (i - 1))
                )
                if i > 4
                else None,
            )
        m.drop_all(e)
        m.create_all(e)

    def _automap(self, e):
        Base = automap_base()

        Base.prepare(autoload_with=e)

        time.sleep(0.01)
        configure_mappers()

    def _chaos(self):
        e = create_engine("sqlite://")
        try:
            self._make_tables(e)
            for i in range(2):
                try:
                    self._automap(e)
                except:
                    self._success = False
                    raise
                time.sleep(random.random())
        finally:
            e.dispose()

    def test_concurrent_automaps_w_configure(self):
        self._success = True
        threads = [threading.Thread(target=self._chaos) for i in range(30)]
        for t in threads:
            t.start()

        for t in threads:
            t.join()

        assert self._success, "One or more threads failed"
