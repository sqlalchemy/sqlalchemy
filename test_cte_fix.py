from sqlalchemy import insert, literal
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    id: Mapped[int] = mapped_column(primary_key=True)


class A(Base):
    __tablename__ = "as"
    a: Mapped[int]


class AA(Base):
    __tablename__ = "aas"
    a: Mapped[int]


def test_cte_with_single_value():
    """Тест использования CTE с одиночным значением."""
    stmt = insert(A).values({"a": 1}).add_cte(
        insert(AA).values({"a": 5}).cte()
    )
    print("CTE с одиночным значением:")
    print(stmt)
    print()
    return stmt


def test_cte_with_multiple_values():
    """Тест использования CTE с множественными значениями."""
    stmt = insert(A).values({"a": 1}).add_cte(
        insert(AA).values([{"a": 5}, {"a": 6}]).cte()
    )
    print("CTE с множественными значениями:")
    print(stmt)
    print()
    return stmt


# Запускаем оба теста для демонстрации исправления
print("===== ТЕСТЫ CTE С ВСТАВКОЙ ЗНАЧЕНИЙ =====")
single_value_stmt = test_cte_with_single_value()
multiple_values_stmt = test_cte_with_multiple_values()
print("Оба теста выполнены успешно!")
