from typing import List, Optional

from sqlalchemy import Boolean
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy.orm import mapper
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session
from sqlalchemy import testing
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table

import pytest


# The dataclasses module was added in Python 3.7
dataclasses = pytest.importorskip("dataclasses")


class DataclassesTest(fixtures.MappedTest, testing.AssertsCompiledSQL):
    run_setup_classes = "each"
    run_setup_mappers = "each"

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
            Column("type", String, nullable=False),
            Column("name", String, nullable=False),
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

        mapper(
            Widget,
            widgets,
            polymorphic_on=widgets.c.type,
            polymorphic_identity="normal",
        )
        mapper(
            SpecialWidget,
            widgets,
            inherits=Widget,
            polymorphic_identity="special",
        )
        mapper(Account, accounts, properties={
            "widgets": relationship(Widget),
        })

    def check_account_dataclass(self, obj):
        assert dataclasses.is_dataclass(obj)
        account_id, widgets, widget_count = dataclasses.fields(obj)
        assert account_id.name == "account_id"
        assert widget_count.name == "widget_count"
        assert widgets.name == "widgets"

    def check_widget_dataclass(self, obj):
        assert dataclasses.is_dataclass(obj)
        name, = dataclasses.fields(obj)
        assert name.name == "name"

    def check_special_widget_dataclass(self, obj):
        assert dataclasses.is_dataclass(obj)
        name, magic = dataclasses.fields(obj)
        assert name.name == "name"
        assert magic.name == "magic"

    def data_fixture(self):
        Account = self.classes.Account
        Widget = self.classes.Widget
        SpecialWidget = self.classes.SpecialWidget

        return Account(
            account_id=42,
            widgets=[
                Widget("Foo"),
                SpecialWidget("Bar", magic=True),
            ],
        )

    def check_data_fixture(self, account):
        Widget = self.classes.Widget
        SpecialWidget = self.classes.SpecialWidget

        self.check_account_dataclass(account)
        assert account.account_id == 42
        assert account.widget_count == len(account.widgets) == 2

        foo, bar = account.widgets

        self.check_widget_dataclass(foo)
        assert isinstance(foo, Widget)
        assert foo.name == "Foo"

        self.check_special_widget_dataclass(bar)
        assert isinstance(bar, SpecialWidget)
        assert bar.name == "Bar"
        assert bar.magic == True

    def test_classes_are_still_dataclasses(self):
        self.check_account_dataclass(self.classes.Account)
        self.check_widget_dataclass(self.classes.Widget)
        self.check_special_widget_dataclass(self.classes.SpecialWidget)

    def test_construction(self):
        SpecialWidget = self.classes.SpecialWidget

        account = self.data_fixture()
        self.check_data_fixture(account)

        widget = SpecialWidget()
        assert widget.name == None
        assert widget.magic == False

    def test_equality(self):
        Widget = self.classes.Widget
        SpecialWidget = self.classes.SpecialWidget

        assert Widget("Foo") == Widget("Foo")
        assert Widget("Foo") != Widget("Bar")
        assert Widget("Foo") != SpecialWidget("Foo")

    def test_asdict_and_astuple(self):
        Widget = self.classes.Widget
        SpecialWidget = self.classes.SpecialWidget

        widget = Widget("Foo")
        assert dataclasses.asdict(widget) == {"name": "Foo"}
        assert dataclasses.astuple(widget) == ("Foo",)

        widget = SpecialWidget("Bar", magic=True)
        assert dataclasses.asdict(widget) == {
            "name": "Bar", "magic": True
        }
        assert dataclasses.astuple(widget) == ("Bar", True)

    def test_round_trip(self):
        Account = self.classes.Account
        account = self.data_fixture()

        session = Session(testing.db)
        session.add(account)
        session.commit()
        session.close()

        a = session.query(Account).get(42)
        self.check_data_fixture(a)

    def test_appending_to_relationship(self):
        Account = self.classes.Account
        Widget = self.classes.Widget
        account = self.data_fixture()

        session = Session(testing.db)
        session.add(account)
        session.commit()

        account.add_widget(Widget("Xyzzy"))
        session.commit()
        session.close()

        a = session.query(Account).get(42)
        assert a.widget_count == len(a.widgets) == 3

    def test_filtering_on_relationship(self):
        Account = self.classes.Account
        Widget = self.classes.Widget
        account = self.data_fixture()

        session = Session(testing.db)
        session.add(account)
        session.commit()
        session.close()

        a = (
            session.query(Account)
            .join(Account.widgets)
            .filter(Widget.name == "Foo")
            .one()
        )
        self.check_data_fixture(a)
