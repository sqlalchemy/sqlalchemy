from typing import List
from typing import Optional

from sqlalchemy import Boolean
from sqlalchemy import ForeignKey
from sqlalchemy import inspect
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy.orm import clear_mappers
from sqlalchemy.orm import declared_attr
from sqlalchemy.orm import registry as declarative_registry
from sqlalchemy.orm import registry
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.fixtures import fixture_session
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table

try:
    import dataclasses
except ImportError:
    pass


class DataclassesTest(fixtures.MappedTest, testing.AssertsCompiledSQL):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "accounts",
            metadata,
            Column("account_id", Integer, primary_key=True),
            Column("widget_count", Integer, nullable=False),
        )
        Table(
            "widgets",
            metadata,
            Column("widget_id", Integer, primary_key=True),
            Column(
                "account_id",
                Integer,
                ForeignKey("accounts.account_id"),
                nullable=False,
            ),
            Column("type", String(30), nullable=False),
            Column("name", String(30), nullable=False),
            Column("magic", Boolean),
        )

    @classmethod
    def setup_classes(cls):
        @dataclasses.dataclass
        class Widget:
            name: Optional[str] = None

        @dataclasses.dataclass
        class SpecialWidget(Widget):
            magic: bool = False

        @dataclasses.dataclass
        class Account:
            account_id: int
            widgets: List[Widget] = dataclasses.field(default_factory=list)
            widget_count: int = dataclasses.field(init=False)

            def __post_init__(self):
                self.widget_count = len(self.widgets)

            def add_widget(self, widget: Widget):
                self.widgets.append(widget)
                self.widget_count += 1

        cls.classes.Account = Account
        cls.classes.Widget = Widget
        cls.classes.SpecialWidget = SpecialWidget

    @classmethod
    def setup_mappers(cls):
        accounts = cls.tables.accounts
        widgets = cls.tables.widgets

        Account = cls.classes.Account
        Widget = cls.classes.Widget
        SpecialWidget = cls.classes.SpecialWidget

        cls.mapper_registry.map_imperatively(
            Widget,
            widgets,
            polymorphic_on=widgets.c.type,
            polymorphic_identity="normal",
        )
        cls.mapper_registry.map_imperatively(
            SpecialWidget,
            widgets,
            inherits=Widget,
            polymorphic_identity="special",
        )
        cls.mapper_registry.map_imperatively(
            Account, accounts, properties={"widgets": relationship(Widget)}
        )

    def check_account_dataclass(self, obj):
        assert dataclasses.is_dataclass(obj)
        account_id, widgets, widget_count = dataclasses.fields(obj)
        eq_(account_id.name, "account_id")
        eq_(widget_count.name, "widget_count")
        eq_(widgets.name, "widgets")

    def check_widget_dataclass(self, obj):
        assert dataclasses.is_dataclass(obj)
        (name,) = dataclasses.fields(obj)
        eq_(name.name, "name")

    def check_special_widget_dataclass(self, obj):
        assert dataclasses.is_dataclass(obj)
        name, magic = dataclasses.fields(obj)
        eq_(name.name, "name")
        eq_(magic.name, "magic")

    def data_fixture(self):
        Account = self.classes.Account
        Widget = self.classes.Widget
        SpecialWidget = self.classes.SpecialWidget

        return Account(
            account_id=42,
            widgets=[Widget("Foo"), SpecialWidget("Bar", magic=True)],
        )

    def check_data_fixture(self, account):
        Widget = self.classes.Widget
        SpecialWidget = self.classes.SpecialWidget

        self.check_account_dataclass(account)
        eq_(account.account_id, 42)
        eq_(account.widget_count, 2)
        eq_(len(account.widgets), 2)

        foo, bar = account.widgets

        self.check_widget_dataclass(foo)
        assert isinstance(foo, Widget)
        eq_(foo.name, "Foo")

        self.check_special_widget_dataclass(bar)
        assert isinstance(bar, SpecialWidget)
        eq_(bar.name, "Bar")
        eq_(bar.magic, True)

    def test_classes_are_still_dataclasses(self):
        self.check_account_dataclass(self.classes.Account)
        self.check_widget_dataclass(self.classes.Widget)
        self.check_special_widget_dataclass(self.classes.SpecialWidget)

    def test_construction(self):
        SpecialWidget = self.classes.SpecialWidget

        account = self.data_fixture()
        self.check_data_fixture(account)

        widget = SpecialWidget()
        eq_(widget.name, None)
        eq_(widget.magic, False)

    def test_equality(self):
        Widget = self.classes.Widget
        SpecialWidget = self.classes.SpecialWidget

        eq_(Widget("Foo"), Widget("Foo"))
        assert Widget("Foo") != Widget("Bar")
        assert Widget("Foo") != SpecialWidget("Foo")

    def test_asdict_and_astuple_widget(self):
        Widget = self.classes.Widget
        widget = Widget("Foo")
        eq_(dataclasses.asdict(widget), {"name": "Foo"})
        eq_(dataclasses.astuple(widget), ("Foo",))

    def test_asdict_and_astuple_special_widget(self):
        SpecialWidget = self.classes.SpecialWidget
        widget = SpecialWidget("Bar", magic=True)
        eq_(dataclasses.asdict(widget), {"name": "Bar", "magic": True})
        eq_(dataclasses.astuple(widget), ("Bar", True))

    def test_round_trip(self):
        Account = self.classes.Account
        account = self.data_fixture()

        with fixture_session() as session:
            session.add(account)
            session.commit()

        with fixture_session() as session:
            a = session.get(Account, 42)
            self.check_data_fixture(a)

    def test_appending_to_relationship(self):
        Account = self.classes.Account
        Widget = self.classes.Widget
        account = self.data_fixture()

        with Session(testing.db) as session, session.begin():
            session.add(account)
            account.add_widget(Widget("Xyzzy"))

        with Session(testing.db) as session:
            a = session.get(Account, 42)
            eq_(a.widget_count, 3)
            eq_(len(a.widgets), 3)

    def test_filtering_on_relationship(self):
        Account = self.classes.Account
        Widget = self.classes.Widget
        account = self.data_fixture()

        with Session(testing.db) as session:
            session.add(account)
            session.commit()

        with Session(testing.db) as session:
            a = (
                session.query(Account)
                .join(Account.widgets)
                .filter(Widget.name == "Foo")
                .one()
            )
            self.check_data_fixture(a)


class PlainDeclarativeDataclassesTest(DataclassesTest):
    run_setup_classes = "each"
    run_setup_mappers = "each"

    @classmethod
    def setup_classes(cls):
        accounts = cls.tables.accounts
        widgets = cls.tables.widgets

        declarative = declarative_registry().mapped

        @declarative
        @dataclasses.dataclass
        class Widget:
            __table__ = widgets

            name: Optional[str] = None

            __mapper_args__ = dict(
                polymorphic_on=widgets.c.type,
                polymorphic_identity="normal",
            )

        @declarative
        @dataclasses.dataclass
        class SpecialWidget(Widget):
            magic: bool = False

            __mapper_args__ = dict(
                polymorphic_identity="special",
            )

        @declarative
        @dataclasses.dataclass
        class Account:
            __table__ = accounts

            account_id: int
            widgets: List[Widget] = dataclasses.field(default_factory=list)
            widget_count: int = dataclasses.field(init=False)

            __mapper_args__ = dict(
                properties=dict(widgets=relationship("Widget"))
            )

            def __post_init__(self):
                self.widget_count = len(self.widgets)

            def add_widget(self, widget: Widget):
                self.widgets.append(widget)
                self.widget_count += 1

        cls.classes.Account = Account
        cls.classes.Widget = Widget
        cls.classes.SpecialWidget = SpecialWidget

    @classmethod
    def setup_mappers(cls):
        pass


class FieldEmbeddedDeclarativeDataclassesTest(
    fixtures.DeclarativeMappedTest, DataclassesTest
):
    @classmethod
    def setup_classes(cls):
        declarative = cls.DeclarativeBasic.registry.mapped

        @declarative
        @dataclasses.dataclass
        class Widget:
            __tablename__ = "widgets"
            __sa_dataclass_metadata_key__ = "sa"

            widget_id = Column(Integer, primary_key=True)
            account_id = Column(
                Integer,
                ForeignKey("accounts.account_id"),
                nullable=False,
            )
            type = Column(String(30), nullable=False)

            name: Optional[str] = dataclasses.field(
                default=None,
                metadata={"sa": Column(String(30), nullable=False)},
            )
            __mapper_args__ = dict(
                polymorphic_on="type",
                polymorphic_identity="normal",
            )

        @declarative
        @dataclasses.dataclass
        class SpecialWidget(Widget):
            __sa_dataclass_metadata_key__ = "sa"

            magic: bool = dataclasses.field(
                default=False, metadata={"sa": Column(Boolean)}
            )

            __mapper_args__ = dict(
                polymorphic_identity="special",
            )

        @declarative
        @dataclasses.dataclass
        class Account:
            __tablename__ = "accounts"
            __sa_dataclass_metadata_key__ = "sa"

            account_id: int = dataclasses.field(
                metadata={"sa": Column(Integer, primary_key=True)},
            )
            widgets: List[Widget] = dataclasses.field(
                default_factory=list, metadata={"sa": relationship("Widget")}
            )
            widget_count: int = dataclasses.field(
                init=False,
                metadata={
                    "sa": Column("widget_count", Integer, nullable=False)
                },
            )

            def __post_init__(self):
                self.widget_count = len(self.widgets)

            def add_widget(self, widget: Widget):
                self.widgets.append(widget)
                self.widget_count += 1

        cls.classes.Account = Account
        cls.classes.Widget = Widget
        cls.classes.SpecialWidget = SpecialWidget

    @classmethod
    def setup_mappers(cls):
        pass

    @classmethod
    def define_tables(cls, metadata):
        pass

    def test_asdict_and_astuple_widget(self):
        Widget = self.classes.Widget

        widget = Widget("Foo")
        eq_(dataclasses.asdict(widget), {"name": "Foo"})
        eq_(dataclasses.astuple(widget), ("Foo",))

    def test_asdict_and_astuple_special_widget(self):
        SpecialWidget = self.classes.SpecialWidget
        widget = SpecialWidget("Bar", magic=True)
        eq_(dataclasses.asdict(widget), {"name": "Bar", "magic": True})
        eq_(dataclasses.astuple(widget), ("Bar", True))


class FieldEmbeddedWMixinTest(FieldEmbeddedDeclarativeDataclassesTest):
    @classmethod
    def setup_classes(cls):
        declarative = cls.DeclarativeBasic.registry.mapped

        @dataclasses.dataclass
        class SurrogateWidgetPK:
            __sa_dataclass_metadata_key__ = "sa"

            widget_id: int = dataclasses.field(
                init=False,
                metadata={"sa": Column(Integer, primary_key=True)},
            )

        @declarative
        @dataclasses.dataclass
        class Widget(SurrogateWidgetPK):
            __tablename__ = "widgets"
            __sa_dataclass_metadata_key__ = "sa"

            account_id = Column(
                Integer,
                ForeignKey("accounts.account_id"),
                nullable=False,
            )
            type = Column(String(30), nullable=False)

            name: Optional[str] = dataclasses.field(
                default=None,
                metadata={"sa": Column(String(30), nullable=False)},
            )
            __mapper_args__ = dict(
                polymorphic_on="type",
                polymorphic_identity="normal",
            )

        @declarative
        @dataclasses.dataclass
        class SpecialWidget(Widget):
            __sa_dataclass_metadata_key__ = "sa"

            magic: bool = dataclasses.field(
                default=False, metadata={"sa": Column(Boolean)}
            )

            __mapper_args__ = dict(
                polymorphic_identity="special",
            )

        @dataclasses.dataclass
        class SurrogateAccountPK:
            __sa_dataclass_metadata_key__ = "sa"

            account_id = Column(
                "we_dont_want_to_use_this", Integer, primary_key=True
            )

        @declarative
        @dataclasses.dataclass
        class Account(SurrogateAccountPK):
            __tablename__ = "accounts"
            __sa_dataclass_metadata_key__ = "sa"

            account_id: int = dataclasses.field(
                metadata={"sa": Column(Integer, primary_key=True)},
            )
            widgets: List[Widget] = dataclasses.field(
                default_factory=list, metadata={"sa": relationship("Widget")}
            )
            widget_count: int = dataclasses.field(
                init=False,
                metadata={
                    "sa": Column("widget_count", Integer, nullable=False)
                },
            )

            def __post_init__(self):
                self.widget_count = len(self.widgets)

            def add_widget(self, widget: Widget):
                self.widgets.append(widget)
                self.widget_count += 1

        cls.classes.Account = Account
        cls.classes.Widget = Widget
        cls.classes.SpecialWidget = SpecialWidget

    def check_widget_dataclass(self, obj):
        assert dataclasses.is_dataclass(obj)
        (
            id_,
            name,
        ) = dataclasses.fields(obj)
        eq_(name.name, "name")
        eq_(id_.name, "widget_id")

    def check_special_widget_dataclass(self, obj):
        assert dataclasses.is_dataclass(obj)
        id_, name, magic = dataclasses.fields(obj)
        eq_(id_.name, "widget_id")
        eq_(name.name, "name")
        eq_(magic.name, "magic")

    def test_asdict_and_astuple_widget(self):
        Widget = self.classes.Widget

        widget = Widget("Foo")
        eq_(dataclasses.asdict(widget), {"name": "Foo", "widget_id": None})
        eq_(
            dataclasses.astuple(widget),
            (
                None,
                "Foo",
            ),
        )

    def test_asdict_and_astuple_special_widget(self):
        SpecialWidget = self.classes.SpecialWidget
        widget = SpecialWidget("Bar", magic=True)
        eq_(
            dataclasses.asdict(widget),
            {"name": "Bar", "magic": True, "widget_id": None},
        )
        eq_(dataclasses.astuple(widget), (None, "Bar", True))


class FieldEmbeddedMixinWLambdaTest(fixtures.DeclarativeMappedTest):
    @classmethod
    def setup_classes(cls):
        declarative = cls.DeclarativeBasic.registry.mapped

        @dataclasses.dataclass
        class WidgetDC:
            __sa_dataclass_metadata_key__ = "sa"

            widget_id: int = dataclasses.field(
                init=False,
                metadata={"sa": Column(Integer, primary_key=True)},
            )

            # fk on mixin
            account_id: int = dataclasses.field(
                init=False,
                metadata={
                    "sa": lambda: Column(
                        Integer,
                        ForeignKey("accounts.account_id"),
                        nullable=False,
                    )
                },
            )

            has_a_default: str = dataclasses.field(
                default="some default",
                metadata={"sa": lambda: Column(String(50))},
            )

        @declarative
        @dataclasses.dataclass
        class Widget(WidgetDC):
            __tablename__ = "widgets"
            __sa_dataclass_metadata_key__ = "sa"

            type = Column(String(30), nullable=False)

            name: Optional[str] = dataclasses.field(
                default=None,
                metadata={"sa": Column(String(30), nullable=False)},
            )

            __mapper_args__ = dict(
                polymorphic_on="type",
                polymorphic_identity="normal",
            )

        @declarative
        @dataclasses.dataclass
        class SpecialWidget(Widget):
            __tablename__ = "special_widgets"
            __sa_dataclass_metadata_key__ = "sa"

            special_widget_id: int = dataclasses.field(
                init=False,
                metadata={
                    "sa": Column(
                        ForeignKey("widgets.widget_id"), primary_key=True
                    )
                },
            )

            magic: bool = dataclasses.field(
                default=False, metadata={"sa": Column(Boolean)}
            )

            __mapper_args__ = dict(
                polymorphic_identity="special",
            )

        @dataclasses.dataclass
        class AccountDC:
            __sa_dataclass_metadata_key__ = "sa"

            # relationship on mixin
            widgets: List[Widget] = dataclasses.field(
                default_factory=list,
                metadata={"sa": lambda: relationship("Widget")},
            )

            account_id: int = dataclasses.field(
                init=False,
                metadata={"sa": Column(Integer, primary_key=True)},
            )
            widget_count: int = dataclasses.field(
                init=False,
                metadata={
                    "sa": Column("widget_count", Integer, nullable=False)
                },
            )

        @declarative
        class Account(AccountDC):
            __tablename__ = "accounts"
            __sa_dataclass_metadata_key__ = "sa"

            def __post_init__(self):
                self.widget_count = len(self.widgets)

            def add_widget(self, widget: Widget):
                self.widgets.append(widget)
                self.widget_count += 1

        @declarative
        @dataclasses.dataclass
        class User:
            __tablename__ = "user"
            __sa_dataclass_metadata_key__ = "sa"

            user_id: int = dataclasses.field(
                init=False,
                metadata={"sa": Column(Integer, primary_key=True)},
            )

            # fk w declared attr on mapped class
            account_id: int = dataclasses.field(
                init=False,
                metadata={
                    "sa": lambda: Column(
                        Integer,
                        ForeignKey("accounts.account_id"),
                        nullable=False,
                    )
                },
            )

        cls.classes["Account"] = Account
        cls.classes["Widget"] = Widget
        cls.classes["User"] = User
        cls.classes["SpecialWidget"] = SpecialWidget

    def test_setup(self):
        Account, Widget, User, SpecialWidget = self.classes(
            "Account", "Widget", "User", "SpecialWidget"
        )

        assert "account_id" in Widget.__table__.c
        assert list(Widget.__table__.c.account_id.foreign_keys)[0].references(
            Account.__table__
        )
        assert inspect(Account).relationships.widgets.mapper is inspect(Widget)

        assert "account_id" not in SpecialWidget.__table__.c

        assert "has_a_default" in Widget.__table__.c
        assert "has_a_default" not in SpecialWidget.__table__.c

        assert "account_id" in User.__table__.c
        assert list(User.__table__.c.account_id.foreign_keys)[0].references(
            Account.__table__
        )

    def test_asdict_and_astuple_special_widget(self):
        SpecialWidget = self.classes.SpecialWidget
        widget = SpecialWidget(magic=True)
        eq_(
            dataclasses.asdict(widget),
            {
                "widget_id": None,
                "account_id": None,
                "has_a_default": "some default",
                "name": None,
                "special_widget_id": None,
                "magic": True,
            },
        )
        eq_(
            dataclasses.astuple(widget),
            (None, None, "some default", None, None, True),
        )


class FieldEmbeddedMixinWDeclaredAttrTest(FieldEmbeddedMixinWLambdaTest):
    @classmethod
    def setup_classes(cls):
        declarative = cls.DeclarativeBasic.registry.mapped

        @dataclasses.dataclass
        class WidgetDC:
            __sa_dataclass_metadata_key__ = "sa"

            widget_id: int = dataclasses.field(
                init=False,
                metadata={"sa": Column(Integer, primary_key=True)},
            )

            # fk on mixin
            account_id: int = dataclasses.field(
                init=False,
                metadata={
                    "sa": declared_attr(
                        lambda: Column(
                            Integer,
                            ForeignKey("accounts.account_id"),
                            nullable=False,
                        )
                    )
                },
            )

            has_a_default: str = dataclasses.field(
                default="some default",
                metadata={"sa": declared_attr(lambda: Column(String(50)))},
            )

        @declarative
        @dataclasses.dataclass
        class Widget(WidgetDC):
            __tablename__ = "widgets"
            __sa_dataclass_metadata_key__ = "sa"

            type = Column(String(30), nullable=False)

            name: Optional[str] = dataclasses.field(
                default=None,
                metadata={"sa": Column(String(30), nullable=False)},
            )
            __mapper_args__ = dict(
                polymorphic_on="type",
                polymorphic_identity="normal",
            )

        @declarative
        @dataclasses.dataclass
        class SpecialWidget(Widget):
            __tablename__ = "special_widgets"
            __sa_dataclass_metadata_key__ = "sa"

            special_widget_id: int = dataclasses.field(
                init=False,
                metadata={
                    "sa": Column(
                        ForeignKey("widgets.widget_id"), primary_key=True
                    )
                },
            )

            magic: bool = dataclasses.field(
                default=False, metadata={"sa": Column(Boolean)}
            )

            __mapper_args__ = dict(
                polymorphic_identity="special",
            )

        @dataclasses.dataclass
        class AccountDC:
            __sa_dataclass_metadata_key__ = "sa"

            # relationship on mixin
            widgets: List[Widget] = dataclasses.field(
                default_factory=list,
                metadata={"sa": declared_attr(lambda: relationship("Widget"))},
            )

            account_id: int = dataclasses.field(
                init=False,
                metadata={"sa": Column(Integer, primary_key=True)},
            )
            widget_count: int = dataclasses.field(
                init=False,
                metadata={
                    "sa": Column("widget_count", Integer, nullable=False)
                },
            )

        @declarative
        class Account(AccountDC):
            __tablename__ = "accounts"
            __sa_dataclass_metadata_key__ = "sa"

            def __post_init__(self):
                self.widget_count = len(self.widgets)

            def add_widget(self, widget: Widget):
                self.widgets.append(widget)
                self.widget_count += 1

        @declarative
        @dataclasses.dataclass
        class User:
            __tablename__ = "user"
            __sa_dataclass_metadata_key__ = "sa"

            user_id: int = dataclasses.field(
                init=False,
                metadata={"sa": Column(Integer, primary_key=True)},
            )

            # fk w declared attr on mapped class
            account_id: int = dataclasses.field(
                init=False,
                metadata={
                    "sa": declared_attr(
                        lambda: Column(
                            Integer,
                            ForeignKey("accounts.account_id"),
                            nullable=False,
                        )
                    )
                },
            )

        cls.classes["Account"] = Account
        cls.classes["Widget"] = Widget
        cls.classes["User"] = User
        cls.classes["SpecialWidget"] = SpecialWidget


class PropagationFromMixinTest(fixtures.TestBase):
    def test_propagate_w_plain_mixin_col(self, run_test):
        @dataclasses.dataclass
        class CommonMixin:
            __sa_dataclass_metadata_key__ = "sa"

            @declared_attr
            def __tablename__(cls):
                return cls.__name__.lower()

            __table_args__ = {"mysql_engine": "InnoDB"}
            timestamp = Column(Integer)

        run_test(CommonMixin)

    def test_propagate_w_field_mixin_col(self, run_test):
        @dataclasses.dataclass
        class CommonMixin:
            __sa_dataclass_metadata_key__ = "sa"

            @declared_attr
            def __tablename__(cls):
                return cls.__name__.lower()

            __table_args__ = {"mysql_engine": "InnoDB"}

            timestamp: int = dataclasses.field(
                init=False,
                metadata={"sa": Column(Integer, nullable=False)},
            )

        run_test(CommonMixin)

    def test_propagate_w_field_mixin_col_and_default(self, run_test):
        @dataclasses.dataclass
        class CommonMixin:
            __sa_dataclass_metadata_key__ = "sa"

            @declared_attr
            def __tablename__(cls):
                return cls.__name__.lower()

            __table_args__ = {"mysql_engine": "InnoDB"}

            timestamp: int = dataclasses.field(
                init=False,
                default=12,
                metadata={"sa": Column(Integer, nullable=False)},
            )

        run_test(CommonMixin)

    @testing.fixture()
    def run_test(self):
        def go(CommonMixin):
            declarative = registry().mapped

            @declarative
            @dataclasses.dataclass
            class BaseType(CommonMixin):
                discriminator = Column("type", String(50))
                __mapper_args__ = dict(polymorphic_on=discriminator)
                id = Column(Integer, primary_key=True)
                value = Column(Integer())

            @declarative
            @dataclasses.dataclass
            class Single(BaseType):
                __tablename__ = None
                __mapper_args__ = dict(polymorphic_identity="type1")

            @declarative
            @dataclasses.dataclass
            class Joined(BaseType):
                __mapper_args__ = dict(polymorphic_identity="type2")
                id = Column(
                    Integer, ForeignKey("basetype.id"), primary_key=True
                )

            eq_(BaseType.__table__.name, "basetype")
            eq_(
                list(BaseType.__table__.c.keys()),
                ["type", "id", "value", "timestamp"],
            )
            eq_(BaseType.__table__.kwargs, {"mysql_engine": "InnoDB"})
            assert Single.__table__ is BaseType.__table__
            eq_(Joined.__table__.name, "joined")
            eq_(list(Joined.__table__.c.keys()), ["id"])
            eq_(Joined.__table__.kwargs, {"mysql_engine": "InnoDB"})

        yield go

        clear_mappers()


class PropagationFromAbstractTest(fixtures.TestBase):
    def test_propagate_w_plain_mixin_col(self, run_test):
        @dataclasses.dataclass
        class BaseType:
            __sa_dataclass_metadata_key__ = "sa"

            __table_args__ = {"mysql_engine": "InnoDB"}

            discriminator: str = Column("type", String(50))
            __mapper_args__ = dict(polymorphic_on=discriminator)
            id: int = Column(Integer, primary_key=True)
            value: int = Column(Integer())

            timestamp: int = Column(Integer)

        run_test(BaseType)

    def test_propagate_w_field_mixin_col(self, run_test):
        @dataclasses.dataclass
        class BaseType:
            __sa_dataclass_metadata_key__ = "sa"

            __table_args__ = {"mysql_engine": "InnoDB"}

            discriminator: str = Column("type", String(50))
            __mapper_args__ = dict(polymorphic_on=discriminator)
            id: int = Column(Integer, primary_key=True)
            value: int = Column(Integer())

            timestamp: int = dataclasses.field(
                init=False,
                metadata={"sa": Column(Integer, nullable=False)},
            )

        run_test(BaseType)

    def test_propagate_w_field_mixin_col_and_default(self, run_test):
        @dataclasses.dataclass
        class BaseType:
            __sa_dataclass_metadata_key__ = "sa"

            __table_args__ = {"mysql_engine": "InnoDB"}

            discriminator: str = Column("type", String(50))
            __mapper_args__ = dict(polymorphic_on=discriminator)
            id: int = Column(Integer, primary_key=True)
            value: int = Column(Integer())

            timestamp: int = dataclasses.field(
                init=False,
                default=None,
                metadata={"sa": Column(Integer, nullable=False)},
            )

        run_test(BaseType)

    @testing.fixture()
    def run_test(self):
        def go(BaseType):
            declarative = registry().mapped

            @declarative
            @dataclasses.dataclass
            class Single(BaseType):
                __tablename__ = "single"
                __mapper_args__ = dict(polymorphic_identity="type1")

            @declarative
            @dataclasses.dataclass
            class Joined(Single):
                __tablename__ = "joined"
                __mapper_args__ = dict(polymorphic_identity="type2")
                id = Column(Integer, ForeignKey("single.id"), primary_key=True)

            eq_(Single.__table__.name, "single")
            eq_(
                list(Single.__table__.c.keys()),
                ["type", "id", "value", "timestamp"],
            )
            eq_(Single.__table__.kwargs, {"mysql_engine": "InnoDB"})

            eq_(Joined.__table__.name, "joined")
            eq_(list(Joined.__table__.c.keys()), ["id"])
            eq_(Joined.__table__.kwargs, {"mysql_engine": "InnoDB"})

        yield go

        clear_mappers()
