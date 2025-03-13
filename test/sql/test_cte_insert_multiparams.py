"""Test multi-parameter INSERT statements in the context of CTE."""

from sqlalchemy import insert
from sqlalchemy.sql import column
from sqlalchemy.sql import table
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.dialects import default
from sqlalchemy.dialects.default import DefaultDialect


class CTEMultiparamsTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = "default_enhanced"

    def test_cte_insert_multiparams_with_defaults(self):
        """Test multi-parameter INSERT with defaults in CTE context."""
        
        mytable = table(
            "mytable", 
            column("id"),
            column("value"),
            column("default_val"),
        )
        
        # Создаем INSERT запрос с default значениями
        stmt = (
            insert(mytable)
            .values([
                {"id": 1, "value": "value1"},  # default_val должен использовать default
                {"id": 2, "value": "value2", "default_val": "custom"},
                {"id": 3, "value": "value3"},  # default_val должен использовать default
            ])
        )
        
        # Компилируем с флагом visiting_cte=True для эмуляции CTE контекста
        dialect = DefaultDialect()
        compiled = stmt.compile(
            dialect=dialect,
            compile_kwargs={"visiting_cte": True}
        )
        
        params = compiled.params
        
        # Проверяем параметры с префиксом cte_
        assert "id_cte_m0" in params
        assert "value_cte_m0" in params
        assert "default_val_cte_default" in params
        assert "id_cte_m1" in params
        assert "value_cte_m1" in params
        assert "default_val_cte_m1" in params
        assert "id_cte_m2" in params
        assert "value_cte_m2" in params
        assert "default_val_cte_default" in params
        
        # Обычная проверка без CTE для сравнения
        self.assert_compile(
            stmt,
            "INSERT INTO mytable (id, value, default_val) VALUES "
            "(:id_m0, :value_m0, :default_val_default), "
            "(:id_m1, :value_m1, :default_val_m1), "
            "(:id_m2, :value_m2, :default_val_default)"
        )

    def test_insert_cte_with_multiparams(self):
        """Test that multi-parameter INSERT in CTE has correct parameter names."""
        mytable = table(
            "mytable", 
            column("id"),
            column("value"),
        )
        
        # Создаем INSERT запрос для CTE
        stmt = (
            insert(mytable)
            .values([
                {"id": 1, "value": "value1"},
                {"id": 2, "value": "value2"},
                {"id": 3, "value": "value3"},
            ])
        )
        
        # Компилируем с флагом visiting_cte=True для эмуляции CTE контекста
        dialect = DefaultDialect()
        compiled = stmt.compile(
            dialect=dialect,
            compile_kwargs={"visiting_cte": True}
        )
        
        params = compiled.params
        
        # Проверяем наличие префикса cte_ в параметрах для CTE контекста
        assert "id_cte_m0" in params
        assert "value_cte_m0" in params
        assert "id_cte_m1" in params
        assert "value_cte_m1" in params
        assert "id_cte_m2" in params
        assert "value_cte_m2" in params
        
        # Обычная проверка без CTE для сравнения
        self.assert_compile(
            stmt,
            "INSERT INTO mytable (id, value) VALUES "
            "(:id_m0, :value_m0), "
            "(:id_m1, :value_m1), "
            "(:id_m2, :value_m2)"
        )

    def test_insert_with_explicit_param_names(self):
        """Test INSERT without CTE uses standard parameter names."""
        mytable = table(
            "mytable", 
            column("id"),
            column("value"),
        )
        
        # Обычный INSERT без CTE контекста
        stmt = insert(mytable).values([
            {"id": 1, "value": "value1"},
            {"id": 2, "value": "value2"},
            {"id": 3, "value": "value3"},
        ])
        
        # Компилируем без флага visiting_cte
        dialect = DefaultDialect()
        compiled = stmt.compile(dialect=dialect)
        
        params = compiled.params
        
        # Проверяем отсутствие префикса cte_ в параметрах
        assert "id_m0" in params
        assert "value_m0" in params
        assert "id_m1" in params
        assert "value_m1" in params
        assert "id_m2" in params
        assert "value_m2" in params
        
        # Убеждаемся, что нет параметров с префиксом cte_
        for param in params:
            assert "cte" not in param, f"Parameter {param} should not have 'cte' prefix"
        
        # Обычная проверка без CTE 
        self.assert_compile(
            stmt,
            "INSERT INTO mytable (id, value) VALUES "
            "(:id_m0, :value_m0), "
            "(:id_m1, :value_m1), "
            "(:id_m2, :value_m2)"
        )

    def test_nested_cte_insert_with_multiparams(self):
        """Test that nested CTE with multi-parameter INSERT has correct names."""
        mytable = table(
            "mytable", 
            column("id"),
            column("value"),
        )
        
        # Создаем INSERT запрос для внутреннего CTE
        inner_stmt = (
            insert(mytable)
            .values([
                {"id": 1, "value": "inner1"},
                {"id": 2, "value": "inner2"},
            ])
        )
        
        # Компилируем с флагом visiting_cte=True для эмуляции CTE контекста
        dialect = DefaultDialect()
        inner_compiled = inner_stmt.compile(
            dialect=dialect,
            compile_kwargs={"visiting_cte": True}
        )
        
        # Проверяем параметры для внутреннего CTE
        assert "id_cte_m0" in inner_compiled.params
        assert "value_cte_m0" in inner_compiled.params
        assert "id_cte_m1" in inner_compiled.params
        assert "value_cte_m1" in inner_compiled.params
        
        # Создаем второй INSERT для внешнего CTE
        outer_stmt = (
            insert(mytable)
            .values([
                {"id": 100, "value": "outer1"},
                {"id": 200, "value": "outer2"},
            ])
        )
        
        # Компилируем с флагом visiting_cte=True для эмуляции CTE контекста
        outer_compiled = outer_stmt.compile(
            dialect=dialect,
            compile_kwargs={"visiting_cte": True}
        )
        
        # Проверяем параметры для внешнего CTE
        assert "id_cte_m0" in outer_compiled.params
        assert "value_cte_m0" in outer_compiled.params
        assert "id_cte_m1" in outer_compiled.params
        assert "value_cte_m1" in outer_compiled.params
