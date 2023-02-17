from __future__ import annotations

from decimal import Decimal
from typing import cast
from typing import List
from typing import Optional

from sqlalchemy import ForeignKey
from sqlalchemy import Numeric
from sqlalchemy import SQLColumnExpression
from sqlalchemy import String
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship


class Base(DeclarativeBase):
    pass


class SavingsAccount(Base):
    __tablename__ = "account"
    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("user.id"))
    balance: Mapped[Decimal] = mapped_column(Numeric(15, 5))


class UserStyleOne(Base):
    __tablename__ = "user"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(100))

    accounts: Mapped[List[SavingsAccount]] = relationship()

    @hybrid_property
    def _balance_getter(self) -> Optional[Decimal]:
        if self.accounts:
            return self.accounts[0].balance
        else:
            return None

    @_balance_getter.setter
    def _balance_setter(self, value: Optional[Decimal]) -> None:
        assert value is not None
        if not self.accounts:
            account = SavingsAccount(owner=self)
        else:
            account = self.accounts[0]
        account.balance = value

    @_balance_setter.expression
    def balance(cls) -> SQLColumnExpression[Optional[Decimal]]:
        return cast(
            "SQLColumnExpression[Optional[Decimal]]", SavingsAccount.balance
        )


class UserStyleTwo(Base):
    __tablename__ = "user"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(100))

    accounts: Mapped[List[SavingsAccount]] = relationship()

    @hybrid_property
    def balance(self) -> Optional[Decimal]:
        if self.accounts:
            return self.accounts[0].balance
        else:
            return None

    @balance.inplace.setter
    def _balance_setter(self, value: Optional[Decimal]) -> None:
        assert value is not None
        if not self.accounts:
            account = SavingsAccount(owner=self)
        else:
            account = self.accounts[0]
        account.balance = value

    @balance.inplace.expression
    def _balance_expression(cls) -> SQLColumnExpression[Optional[Decimal]]:
        return cast(
            "SQLColumnExpression[Optional[Decimal]]", SavingsAccount.balance
        )
