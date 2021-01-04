from datetime import datetime

from sqlalchemy import DateTime
from sqlalchemy import Float
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import LargeBinary
from sqlalchemy import String
from sqlalchemy.orm import backref
from sqlalchemy.orm import deferred
from sqlalchemy.orm import mapper
from sqlalchemy.orm import relationship
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.fixtures import fixture_session
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table


class InheritTest(fixtures.MappedTest):
    """tests some various inheritance round trips involving a particular set of
    polymorphic inheritance relationships"""

    @classmethod
    def define_tables(cls, metadata):
        global products_table, specification_table, documents_table
        global Product, Detail, Assembly, SpecLine, Document, RasterDocument

        products_table = Table(
            "products",
            metadata,
            Column(
                "product_id",
                Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column("product_type", String(128)),
            Column("name", String(128)),
            Column("mark", String(128)),
        )

        specification_table = Table(
            "specification",
            metadata,
            Column(
                "spec_line_id",
                Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column(
                "leader_id",
                Integer,
                ForeignKey("products.product_id"),
                nullable=True,
            ),
            Column(
                "follower_id",
                Integer,
                ForeignKey("products.product_id"),
                nullable=True,
            ),
            Column("quantity", Float, default=1.0),
        )

        documents_table = Table(
            "documents",
            metadata,
            Column(
                "document_id",
                Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column("document_type", String(128)),
            Column("product_id", Integer, ForeignKey("products.product_id")),
            Column("create_date", DateTime, default=lambda: datetime.now()),
            Column(
                "last_updated",
                DateTime,
                default=lambda: datetime.now(),
                onupdate=lambda: datetime.now(),
            ),
            Column("name", String(128)),
            Column("data", LargeBinary),
            Column("size", Integer, default=0),
        )

        class Product(object):
            def __init__(self, name, mark=""):
                self.name = name
                self.mark = mark

            def __repr__(self):
                return "<%s %s>" % (self.__class__.__name__, self.name)

        class Detail(Product):
            def __init__(self, name):
                self.name = name

        class Assembly(Product):
            def __repr__(self):
                return (
                    Product.__repr__(self)
                    + " "
                    + " ".join(
                        [
                            x + "=" + repr(getattr(self, x, None))
                            for x in ["specification", "documents"]
                        ]
                    )
                )

        class SpecLine(object):
            def __init__(self, leader=None, follower=None, quantity=1):
                self.leader = leader
                self.follower = follower
                self.quantity = quantity

            def __repr__(self):
                return "<%s %.01f %s>" % (
                    self.__class__.__name__,
                    self.quantity or 0.0,
                    repr(self.follower),
                )

        class Document(object):
            def __init__(self, name, data=None):
                self.name = name
                self.data = data

            def __repr__(self):
                return "<%s %s>" % (self.__class__.__name__, self.name)

        class RasterDocument(Document):
            pass

    def test_one(self):
        product_mapper = mapper(
            Product,
            products_table,
            polymorphic_on=products_table.c.product_type,
            polymorphic_identity="product",
        )

        mapper(Detail, inherits=product_mapper, polymorphic_identity="detail")

        mapper(
            Assembly, inherits=product_mapper, polymorphic_identity="assembly"
        )

        mapper(
            SpecLine,
            specification_table,
            properties=dict(
                leader=relationship(
                    Assembly,
                    foreign_keys=[specification_table.c.leader_id],
                    primaryjoin=specification_table.c.leader_id
                    == products_table.c.product_id,
                    lazy="select",
                    backref=backref("specification"),
                    uselist=False,
                ),
                follower=relationship(
                    Product,
                    foreign_keys=[specification_table.c.follower_id],
                    primaryjoin=specification_table.c.follower_id
                    == products_table.c.product_id,
                    lazy="select",
                    uselist=False,
                ),
                quantity=specification_table.c.quantity,
            ),
        )

        session = fixture_session()

        a1 = Assembly(name="a1")

        p1 = Product(name="p1")
        a1.specification.append(SpecLine(follower=p1))

        d1 = Detail(name="d1")
        a1.specification.append(SpecLine(follower=d1))

        session.add(a1)
        orig = repr(a1)
        session.flush()
        session.expunge_all()

        a1 = session.query(Product).filter_by(name="a1").one()
        new = repr(a1)
        print(orig)
        print(new)
        assert (
            orig == new == "<Assembly a1> specification=[<SpecLine 1.0 "
            "<Product p1>>, <SpecLine 1.0 <Detail d1>>] documents=None"
        )

    def test_two(self):
        product_mapper = mapper(
            Product,
            products_table,
            polymorphic_on=products_table.c.product_type,
            polymorphic_identity="product",
        )

        mapper(Detail, inherits=product_mapper, polymorphic_identity="detail")

        mapper(
            SpecLine,
            specification_table,
            properties=dict(
                follower=relationship(
                    Product,
                    foreign_keys=[specification_table.c.follower_id],
                    primaryjoin=specification_table.c.follower_id
                    == products_table.c.product_id,
                    lazy="select",
                    uselist=False,
                )
            ),
        )

        session = fixture_session()

        s = SpecLine(follower=Product(name="p1"))
        s2 = SpecLine(follower=Detail(name="d1"))
        session.add(s)
        session.add(s2)
        orig = repr([s, s2])
        session.flush()
        session.expunge_all()
        new = repr(session.query(SpecLine).all())
        print(orig)
        print(new)
        assert (
            orig == new == "[<SpecLine 1.0 <Product p1>>, "
            "<SpecLine 1.0 <Detail d1>>]"
        )

    def test_three(self):
        product_mapper = mapper(
            Product,
            products_table,
            polymorphic_on=products_table.c.product_type,
            polymorphic_identity="product",
        )
        mapper(Detail, inherits=product_mapper, polymorphic_identity="detail")
        mapper(
            Assembly, inherits=product_mapper, polymorphic_identity="assembly"
        )

        mapper(
            SpecLine,
            specification_table,
            properties=dict(
                leader=relationship(
                    Assembly,
                    lazy="joined",
                    uselist=False,
                    foreign_keys=[specification_table.c.leader_id],
                    primaryjoin=specification_table.c.leader_id
                    == products_table.c.product_id,
                    backref=backref(
                        "specification", cascade="all, delete-orphan"
                    ),
                ),
                follower=relationship(
                    Product,
                    lazy="joined",
                    uselist=False,
                    foreign_keys=[specification_table.c.follower_id],
                    primaryjoin=specification_table.c.follower_id
                    == products_table.c.product_id,
                ),
                quantity=specification_table.c.quantity,
            ),
        )

        document_mapper = mapper(
            Document,
            documents_table,
            polymorphic_on=documents_table.c.document_type,
            polymorphic_identity="document",
            properties=dict(
                name=documents_table.c.name,
                data=deferred(documents_table.c.data),
                product=relationship(
                    Product,
                    lazy="select",
                    backref=backref("documents", cascade="all, delete-orphan"),
                ),
            ),
        )
        mapper(
            RasterDocument,
            inherits=document_mapper,
            polymorphic_identity="raster_document",
        )

        session = fixture_session()

        a1 = Assembly(name="a1")
        a1.specification.append(SpecLine(follower=Detail(name="d1")))
        a1.documents.append(Document("doc1"))
        a1.documents.append(RasterDocument("doc2"))
        session.add(a1)
        orig = repr(a1)
        session.flush()
        session.expunge_all()

        a1 = session.query(Product).filter_by(name="a1").one()
        new = repr(a1)
        print(orig)
        print(new)
        assert (
            orig == new == "<Assembly a1> specification="
            "[<SpecLine 1.0 <Detail d1>>] "
            "documents=[<Document doc1>, <RasterDocument doc2>]"
        )

    def test_four(self):
        """this tests the RasterDocument being attached to the Assembly, but
        *not* the Document.  this means only a "sub-class" task, i.e.
        corresponding to an inheriting mapper but not the base mapper,
        is created."""

        product_mapper = mapper(
            Product,
            products_table,
            polymorphic_on=products_table.c.product_type,
            polymorphic_identity="product",
        )
        mapper(Detail, inherits=product_mapper, polymorphic_identity="detail")
        mapper(
            Assembly, inherits=product_mapper, polymorphic_identity="assembly"
        )

        document_mapper = mapper(
            Document,
            documents_table,
            polymorphic_on=documents_table.c.document_type,
            polymorphic_identity="document",
            properties=dict(
                name=documents_table.c.name,
                data=deferred(documents_table.c.data),
                product=relationship(
                    Product,
                    lazy="select",
                    backref=backref("documents", cascade="all, delete-orphan"),
                ),
            ),
        )
        mapper(
            RasterDocument,
            inherits=document_mapper,
            polymorphic_identity="raster_document",
        )

        session = fixture_session()

        a1 = Assembly(name="a1")
        a1.documents.append(RasterDocument("doc2"))
        session.add(a1)
        orig = repr(a1)
        session.flush()
        session.expunge_all()

        a1 = session.query(Product).filter_by(name="a1").one()
        new = repr(a1)
        print(orig)
        print(new)
        assert (
            orig == new == "<Assembly a1> specification=None documents="
            "[<RasterDocument doc2>]"
        )

        del a1.documents[0]
        session.flush()
        session.expunge_all()

        a1 = session.query(Product).filter_by(name="a1").one()
        assert len(session.query(Document).all()) == 0

    def test_five(self):
        """tests the late compilation of mappers"""

        mapper(
            SpecLine,
            specification_table,
            properties=dict(
                leader=relationship(
                    Assembly,
                    lazy="joined",
                    uselist=False,
                    foreign_keys=[specification_table.c.leader_id],
                    primaryjoin=specification_table.c.leader_id
                    == products_table.c.product_id,
                    backref=backref("specification"),
                ),
                follower=relationship(
                    Product,
                    lazy="joined",
                    uselist=False,
                    foreign_keys=[specification_table.c.follower_id],
                    primaryjoin=specification_table.c.follower_id
                    == products_table.c.product_id,
                ),
                quantity=specification_table.c.quantity,
            ),
        )

        mapper(
            Product,
            products_table,
            polymorphic_on=products_table.c.product_type,
            polymorphic_identity="product",
            properties={
                "documents": relationship(
                    Document,
                    lazy="select",
                    backref="product",
                    cascade="all, delete-orphan",
                )
            },
        )

        mapper(Detail, inherits=Product, polymorphic_identity="detail")

        mapper(
            Document,
            documents_table,
            polymorphic_on=documents_table.c.document_type,
            polymorphic_identity="document",
            properties=dict(
                name=documents_table.c.name,
                data=deferred(documents_table.c.data),
            ),
        )

        mapper(
            RasterDocument,
            inherits=Document,
            polymorphic_identity="raster_document",
        )

        mapper(Assembly, inherits=Product, polymorphic_identity="assembly")

        session = fixture_session()

        a1 = Assembly(name="a1")
        a1.specification.append(SpecLine(follower=Detail(name="d1")))
        a1.documents.append(Document("doc1"))
        a1.documents.append(RasterDocument("doc2"))
        session.add(a1)
        orig = repr(a1)
        session.flush()
        session.expunge_all()

        a1 = session.query(Product).filter_by(name="a1").one()
        new = repr(a1)
        print(orig)
        print(new)
        assert (
            orig == new == "<Assembly a1> specification="
            "[<SpecLine 1.0 <Detail d1>>] documents=[<Document doc1>, "
            "<RasterDocument doc2>]"
        )
