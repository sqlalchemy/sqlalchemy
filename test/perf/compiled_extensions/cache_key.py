from types import MethodType
from types import SimpleNamespace

import sqlalchemy as sa
from sqlalchemy import orm
from sqlalchemy.dialects.oracle.base import OracleDialect
from sqlalchemy.dialects.postgresql.base import PGDialect
from sqlalchemy.engine import ObjectKind
from sqlalchemy.engine import ObjectScope
from sqlalchemy.sql import cache_key
from sqlalchemy.sql.cache_key import HasCacheKey
from sqlalchemy.util.langhelpers import load_uncompiled_module
from sqlalchemy.util.langhelpers import walk_subclasses
from .base import Case
from .base import test_case


class CacheKey(Case):
    NUMBER = 50_000

    @staticmethod
    def python():
        from sqlalchemy.sql import _cache_key_cy

        py_mod = load_uncompiled_module(_cache_key_cy)
        assert not py_mod._is_compiled()
        # avoid enum issues
        py_mod._attr_info_kind = _cache_key_cy._attr_info_kind
        py_mod.CacheTraverseTarget = _cache_key_cy.CacheTraverseTarget
        return (HasCacheKey._generate_cache_key, py_mod)

    @staticmethod
    def cython():
        from sqlalchemy.sql import _cache_key_cy

        assert _cache_key_cy._is_compiled()
        return (HasCacheKey._generate_cache_key, _cache_key_cy)

    IMPLEMENTATIONS = {
        "python": python.__func__,
        "cython": cython.__func__,
    }

    @classmethod
    def update_results(cls, results):
        cls._divide_results(results, "cython", "python", "cy / py")

    @classmethod
    def init_class(cls):
        cls.objects = setup_objects()
        cls.statements = setup_statements(cls.objects)

        for name in (
            "parent_table",
            "parent_orm",
            "parent_orm_join",
            "many_types",
            "core_insert",
            "core_update",
            "core_union",
            "deep_and",
        ):
            cls.make_test_cases(name, cls.statements.__dict__[name])

        oracle = OracleDialect()
        oracle.server_version_info = (21, 0, 0)
        for name, stmt, num in (
            (
                "_all_objects_query",
                oracle._all_objects_query(
                    "scott", ObjectScope.DEFAULT, ObjectKind.ANY, False, False
                ),
                20_000,
            ),
            (
                "_table_options_query",
                oracle._table_options_query(
                    "scott", ObjectScope.DEFAULT, ObjectKind.ANY, False, False
                ),
                20_000,
            ),
            ("_column_query", oracle._column_query("scott"), 20_000),
            (
                "_comment_query",
                oracle._comment_query(
                    "scott", ObjectScope.DEFAULT, ObjectKind.ANY, False
                ),
                20_000,
            ),
            ("_index_query", oracle._index_query("scott"), 20_000),
            ("_constraint_query", oracle._constraint_query("scott"), 20_000),
        ):
            cls.make_test_cases("oracle" + name, stmt, num)

        pg = PGDialect()
        pg.server_version_info = (16, 0, 0)
        for name, stmt, num in (
            ("_has_table_query", pg._has_multi_table_query("scott"), 20_000),
            (
                "_columns_query",
                pg._columns_query(
                    "scott", False, ObjectScope.DEFAULT, ObjectKind.ANY
                ),
                20_000,
            ),
            (
                "_table_oids_query",
                pg._table_oids_query(
                    "scott", False, ObjectScope.DEFAULT, ObjectKind.ANY
                ),
                20_000,
            ),
            ("_index_query", pg._index_query, 20_000),
            ("_constraint_query", pg._constraint_query, 20_000),
            (
                "_foreing_key_query",
                pg._foreing_key_query(
                    "scott", False, ObjectScope.DEFAULT, ObjectKind.ANY
                ),
                20_000,
            ),
            (
                "_comment_query",
                pg._comment_query(
                    "scott", False, ObjectScope.DEFAULT, ObjectKind.ANY
                ),
                20_000,
            ),
            (
                "_check_constraint_query",
                pg._check_constraint_query(
                    "scott", False, ObjectScope.DEFAULT, ObjectKind.ANY
                ),
                20_000,
            ),
            ("_enum_query", pg._enum_query("scott"), 20_000),
            ("_domain_query", pg._domain_query("scott"), 20_000),
        ):
            cls.make_test_cases("pg" + name, stmt, num)

    def _reset_traversal(self):
        # need to reset all the subclasses object
        for cls in walk_subclasses(cache_key.HasCacheKey):
            if "_generated_cache_key_traversal" in cls.__dict__:
                delattr(cls, "_generated_cache_key_traversal")

    def init_objects(self):
        self.module = self.impl[1]
        self.impl = self.impl[0]
        self._reset_traversal()
        visitor_instance = cache_key._cache_key_traversal_visitor
        for meth in ["generate_class_attrs", "_generate_class_attrs"]:
            setattr(self, "orig_" + meth, getattr(visitor_instance, meth))
            generated_method = MethodType(
                getattr(self.module._BaseCacheKeyTraversal, meth),
                visitor_instance,
            )
            setattr(visitor_instance, meth, generated_method)
        # NOTE: this sets the cython/python version of the function
        cache_key.HasCacheKey._gen_cache_key = (
            self.module.BaseHasCacheKey._gen_cache_key
        )

    def end(self):
        cache_key._cache_key_traversal_visitor.generate_class_attrs = (
            self.orig_generate_class_attrs
        )
        cache_key._cache_key_traversal_visitor._generate_class_attrs = (
            self.orig__generate_class_attrs
        )
        delattr(cache_key.HasCacheKey, "_gen_cache_key")
        self._reset_traversal()

    @classmethod
    def make_test_cases(cls, name, obj, number=None):
        def go(self):
            assert self.impl(obj) is not None

        go.__name__ = name
        setattr(cls, name, test_case(go, number=number))

    @test_case
    def check_not_caching(self):
        c1 = self.impl(self.statements.parent_table)
        c2 = self.impl(self.statements.parent_table)
        assert c1 is not None
        assert c2 is not None
        assert c1 is not c2
        assert c1 == c2


def setup_objects():
    metadata = sa.MetaData()
    parent = sa.Table(
        "parent",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("data", sa.String(20)),
    )
    child = sa.Table(
        "child",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("data", sa.String(20)),
        sa.Column(
            "parent_id", sa.Integer, sa.ForeignKey("parent.id"), nullable=False
        ),
    )

    class Parent:
        pass

    class Child:
        pass

    registry = orm.registry()
    registry.map_imperatively(
        Parent,
        parent,
        properties={"children": orm.relationship(Child, backref="parent")},
    )
    registry.map_imperatively(Child, child)

    many_types = sa.Table(
        "large",
        metadata,
        sa.Column("col_ARRAY", sa.ARRAY(sa.Integer)),
        sa.Column("col_BIGINT", sa.BIGINT),
        sa.Column("col_BigInteger", sa.BigInteger),
        sa.Column("col_BINARY", sa.BINARY),
        sa.Column("col_BLOB", sa.BLOB),
        sa.Column("col_BOOLEAN", sa.BOOLEAN),
        sa.Column("col_Boolean", sa.Boolean),
        sa.Column("col_CHAR", sa.CHAR),
        sa.Column("col_CLOB", sa.CLOB),
        sa.Column("col_DATE", sa.DATE),
        sa.Column("col_Date", sa.Date),
        sa.Column("col_DATETIME", sa.DATETIME),
        sa.Column("col_DateTime", sa.DateTime),
        sa.Column("col_DECIMAL", sa.DECIMAL),
        sa.Column("col_DOUBLE", sa.DOUBLE),
        sa.Column("col_Double", sa.Double),
        sa.Column("col_DOUBLE_PRECISION", sa.DOUBLE_PRECISION),
        sa.Column("col_Enum", sa.Enum),
        sa.Column("col_FLOAT", sa.FLOAT),
        sa.Column("col_Float", sa.Float),
        sa.Column("col_INT", sa.INT),
        sa.Column("col_INTEGER", sa.INTEGER),
        sa.Column("col_Integer", sa.Integer),
        sa.Column("col_Interval", sa.Interval),
        sa.Column("col_JSON", sa.JSON),
        sa.Column("col_LargeBinary", sa.LargeBinary),
        sa.Column("col_NCHAR", sa.NCHAR),
        sa.Column("col_NUMERIC", sa.NUMERIC),
        sa.Column("col_Numeric", sa.Numeric),
        sa.Column("col_NVARCHAR", sa.NVARCHAR),
        sa.Column("col_PickleType", sa.PickleType),
        sa.Column("col_REAL", sa.REAL),
        sa.Column("col_SMALLINT", sa.SMALLINT),
        sa.Column("col_SmallInteger", sa.SmallInteger),
        sa.Column("col_String", sa.String),
        sa.Column("col_TEXT", sa.TEXT),
        sa.Column("col_Text", sa.Text),
        sa.Column("col_TIME", sa.TIME),
        sa.Column("col_Time", sa.Time),
        sa.Column("col_TIMESTAMP", sa.TIMESTAMP),
        sa.Column("col_TupleType", sa.TupleType),
        sa.Column("col_Unicode", sa.Unicode),
        sa.Column("col_UnicodeText", sa.UnicodeText),
        sa.Column("col_UUID", sa.UUID),
        sa.Column("col_Uuid", sa.Uuid),
        sa.Column("col_VARBINARY", sa.VARBINARY),
        sa.Column("col_VARCHAR", sa.VARCHAR),
    )

    registry.configure()

    return SimpleNamespace(**locals())


def setup_statements(setup: SimpleNamespace):
    parent_table = sa.select(setup.parent).where(setup.parent.c.id == 42)

    parent_orm = (
        sa.select(setup.Parent)
        .order_by(setup.Parent.id)
        .where(setup.Parent.data.like("cat"))
    )

    parent_orm_join = (
        sa.select(setup.Parent.id, setup.Child.id)
        .select_from(
            orm.join(setup.Parent, setup.Child, setup.Parent.children)
        )
        .where(setup.Child.id == 5)
    )

    many_types = sa.select(setup.many_types).where(
        setup.many_types.c.col_Boolean
    )

    core_insert = setup.parent.insert().values(id=1, data=sa.bindparam("d"))
    core_update = (
        setup.parent.update().where(setup.parent.c.id == 5).values(data="x")
    )
    core_union = sa.union(
        sa.select(setup.parent.c.id).where(setup.parent.c.data == "a"),
        sa.select(setup.child.c.id).where(setup.child.c.data == "b"),
    )
    deep_and = sa.select(setup.parent).where(
        sa.and_(
            *[setup.parent.c.data == str(i) for i in range(20)],
        )
    )

    return SimpleNamespace(**locals())
