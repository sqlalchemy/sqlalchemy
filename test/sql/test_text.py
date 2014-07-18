"""Test the TextClause and related constructs."""

from sqlalchemy.testing import fixtures, AssertsCompiledSQL, eq_, assert_raises_message
from sqlalchemy import text, select, Integer, String, Float, \
    bindparam, and_, func, literal_column, exc, MetaData, Table, Column
from sqlalchemy.types import NullType
from sqlalchemy.sql import table, column

table1 = table('mytable',
               column('myid', Integer),
               column('name', String),
               column('description', String),
               )

table2 = table(
    'myothertable',
    column('otherid', Integer),
    column('othername', String),
)


class CompileTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = 'default'

    def test_basic(self):
        self.assert_compile(
            text("select * from foo where lala = bar"),
            "select * from foo where lala = bar"
        )


class SelectCompositionTest(fixtures.TestBase, AssertsCompiledSQL):

    """test the usage of text() implicit within the select() construct
    when strings are passed."""

    __dialect__ = 'default'

    def test_select_composition_one(self):
        self.assert_compile(select(
            ["foobar(a)", "pk_foo_bar(syslaal)"],
            "a = 12",
            from_obj=["foobar left outer join lala on foobar.foo = lala.foo"]
        ),
            "SELECT foobar(a), pk_foo_bar(syslaal) FROM foobar "
            "left outer join lala on foobar.foo = lala.foo WHERE a = 12"
        )

    def test_select_composition_two(self):
        s = select()
        s.append_column("column1")
        s.append_column("column2")
        s.append_whereclause("column1=12")
        s.append_whereclause("column2=19")
        s = s.order_by("column1")
        s.append_from("table1")
        self.assert_compile(s, "SELECT column1, column2 FROM table1 WHERE "
                            "column1=12 AND column2=19 ORDER BY column1")

    def test_select_composition_three(self):
        self.assert_compile(
            select(["column1", "column2"],
                   from_obj=table1).alias('somealias').select(),
            "SELECT somealias.column1, somealias.column2 FROM "
            "(SELECT column1, column2 FROM mytable) AS somealias"
        )

    def test_select_composition_four(self):
        # test that use_labels doesn't interfere with literal columns
        self.assert_compile(
            select(["column1", "column2", table1.c.myid], from_obj=table1,
                   use_labels=True),
            "SELECT column1, column2, mytable.myid AS mytable_myid "
            "FROM mytable"
        )

    def test_select_composition_five(self):
        # test that use_labels doesn't interfere
        # with literal columns that have textual labels
        self.assert_compile(
            select(["column1 AS foobar", "column2 AS hoho", table1.c.myid],
                   from_obj=table1, use_labels=True),
            "SELECT column1 AS foobar, column2 AS hoho, "
            "mytable.myid AS mytable_myid FROM mytable"
        )

    def test_select_composition_six(self):
        # test that "auto-labeling of subquery columns"
        # doesn't interfere with literal columns,
        # exported columns don't get quoted
        self.assert_compile(
            select(["column1 AS foobar", "column2 AS hoho", table1.c.myid],
                   from_obj=[table1]).select(),
            "SELECT column1 AS foobar, column2 AS hoho, myid FROM "
            "(SELECT column1 AS foobar, column2 AS hoho, "
            "mytable.myid AS myid FROM mytable)"
        )

    def test_select_composition_seven(self):
        self.assert_compile(
            select(['col1', 'col2'], from_obj='tablename').alias('myalias'),
            "SELECT col1, col2 FROM tablename"
        )

    def test_select_composition_eight(self):
        self.assert_compile(select(
            [table1.alias('t'), "foo.f"],
            "foo.f = t.id",
            from_obj=["(select f from bar where lala=heyhey) foo"]
        ),
            "SELECT t.myid, t.name, t.description, foo.f FROM mytable AS t, "
            "(select f from bar where lala=heyhey) foo WHERE foo.f = t.id")

    def test_select_bundle_columns(self):
        self.assert_compile(select(
            [table1, table2.c.otherid, "sysdate()", "foo, bar, lala"],
            and_(
                "foo.id = foofoo(lala)",
                "datetime(foo) = Today",
                table1.c.myid == table2.c.otherid,
            )
        ),
            "SELECT mytable.myid, mytable.name, mytable.description, "
            "myothertable.otherid, sysdate(), foo, bar, lala "
            "FROM mytable, myothertable WHERE foo.id = foofoo(lala) AND "
            "datetime(foo) = Today AND mytable.myid = myothertable.otherid")


class BindParamTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = 'default'

    def test_legacy(self):
        t = text("select * from foo where lala=:bar and hoho=:whee",
                 bindparams=[bindparam('bar', 4), bindparam('whee', 7)])

        self.assert_compile(
            t,
            "select * from foo where lala=:bar and hoho=:whee",
            checkparams={'bar': 4, 'whee': 7},
        )

    def test_positional(self):
        t = text("select * from foo where lala=:bar and hoho=:whee")
        t = t.bindparams(bindparam('bar', 4), bindparam('whee', 7))

        self.assert_compile(
            t,
            "select * from foo where lala=:bar and hoho=:whee",
            checkparams={'bar': 4, 'whee': 7},
        )

    def test_kw(self):
        t = text("select * from foo where lala=:bar and hoho=:whee")
        t = t.bindparams(bar=4, whee=7)

        self.assert_compile(
            t,
            "select * from foo where lala=:bar and hoho=:whee",
            checkparams={'bar': 4, 'whee': 7},
        )

    def test_positional_plus_kw(self):
        t = text("select * from foo where lala=:bar and hoho=:whee")
        t = t.bindparams(bindparam('bar', 4), whee=7)

        self.assert_compile(
            t,
            "select * from foo where lala=:bar and hoho=:whee",
            checkparams={'bar': 4, 'whee': 7},
        )

    def test_literal_binds(self):
        t = text("select * from foo where lala=:bar and hoho=:whee")
        t = t.bindparams(bindparam('bar', 4), whee='whee')

        self.assert_compile(
            t,
            "select * from foo where lala=4 and hoho='whee'",
            checkparams={},
            literal_binds=True
        )

    def _assert_type_map(self, t, compare):
        map_ = dict(
            (b.key, b.type) for b in t._bindparams.values()
        )
        for k in compare:
            assert compare[k]._type_affinity is map_[k]._type_affinity

    def test_typing_construction(self):
        t = text("select * from table :foo :bar :bat")

        self._assert_type_map(t, {"foo": NullType(),
                                  "bar": NullType(),
                                  "bat": NullType()})

        t = t.bindparams(bindparam('foo', type_=String))

        self._assert_type_map(t, {"foo": String(),
                                  "bar": NullType(),
                                  "bat": NullType()})

        t = t.bindparams(bindparam('bar', type_=Integer))

        self._assert_type_map(t, {"foo": String(),
                                  "bar": Integer(),
                                  "bat": NullType()})

        t = t.bindparams(bat=45.564)

        self._assert_type_map(t, {"foo": String(),
                                  "bar": Integer(),
                                  "bat": Float()})

    def test_binds_compiled_named(self):
        self.assert_compile(
            text("select * from foo where lala=:bar and hoho=:whee").
            bindparams(bar=4, whee=7),
            "select * from foo where lala=%(bar)s and hoho=%(whee)s",
            checkparams={'bar': 4, 'whee': 7},
            dialect="postgresql"
        )

    def test_binds_compiled_positional(self):
        self.assert_compile(
            text("select * from foo where lala=:bar and hoho=:whee").
            bindparams(bar=4, whee=7),
            "select * from foo where lala=? and hoho=?",
            checkparams={'bar': 4, 'whee': 7},
            dialect="sqlite"
        )

    def test_missing_bind_kw(self):
        assert_raises_message(
            exc.ArgumentError,
            "This text\(\) construct doesn't define a bound parameter named 'bar'",
            text(":foo").bindparams,
            foo=5,
            bar=7)

    def test_missing_bind_posn(self):
        assert_raises_message(
            exc.ArgumentError,
            "This text\(\) construct doesn't define a bound parameter named 'bar'",
            text(":foo").bindparams,
            bindparam(
                'foo',
                value=5),
            bindparam(
                'bar',
                value=7))

    def test_escaping_colons(self):
        # test escaping out text() params with a backslash
        self.assert_compile(
            text("select * from foo where clock='05:06:07' "
                 "and mork='\:mindy'"),
            "select * from foo where clock='05:06:07' and mork=':mindy'",
            checkparams={},
            params={},
            dialect="postgresql"
        )

    def test_text_in_select_nonfrom(self):

        generate_series = text("generate_series(:x, :y, :z) as s(a)").\
            bindparams(x=None, y=None, z=None)

        s = select([
            (func.current_date() + literal_column("s.a")).label("dates")
        ]).select_from(generate_series)

        self.assert_compile(
            s,
            "SELECT CURRENT_DATE + s.a AS dates FROM "
            "generate_series(:x, :y, :z) as s(a)",
            checkparams={'y': None, 'x': None, 'z': None}
        )

        self.assert_compile(
            s.params(x=5, y=6, z=7),
            "SELECT CURRENT_DATE + s.a AS dates FROM "
            "generate_series(:x, :y, :z) as s(a)",
            checkparams={'y': 6, 'x': 5, 'z': 7}
        )


class AsFromTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = 'default'

    def test_basic_toplevel_resultmap_positional(self):
        t = text("select id, name from user").columns(
            column('id', Integer),
            column('name')
        )

        compiled = t.compile()
        eq_(compiled.result_map,
            {'id': ('id',
                    (t.c.id._proxies[0],
                     'id',
                     'id'),
                    t.c.id.type),
                'name': ('name',
                         (t.c.name._proxies[0],
                          'name',
                          'name'),
                         t.c.name.type)})

    def test_basic_toplevel_resultmap(self):
        t = text("select id, name from user").columns(id=Integer, name=String)

        compiled = t.compile()
        eq_(compiled.result_map,
            {'id': ('id',
                    (t.c.id._proxies[0],
                     'id',
                     'id'),
                    t.c.id.type),
                'name': ('name',
                         (t.c.name._proxies[0],
                          'name',
                          'name'),
                         t.c.name.type)})

    def test_basic_subquery_resultmap(self):
        t = text("select id, name from user").columns(id=Integer, name=String)

        stmt = select([table1.c.myid]).select_from(
            table1.join(t, table1.c.myid == t.c.id))
        compiled = stmt.compile()
        eq_(
            compiled.result_map,
            {
                "myid": ("myid",
                         (table1.c.myid, "myid", "myid"), table1.c.myid.type),
            }
        )

    def test_column_collection_ordered(self):
        t = text("select a, b, c from foo").columns(column('a'),
                                                    column('b'), column('c'))
        eq_(t.c.keys(), ['a', 'b', 'c'])

    def test_column_collection_pos_plus_bykey(self):
        # overlapping positional names + type names
        t = text("select a, b, c from foo").columns(
            column('a'),
            column('b'),
            b=Integer,
            c=String)
        eq_(t.c.keys(), ['a', 'b', 'c'])
        eq_(t.c.b.type._type_affinity, Integer)
        eq_(t.c.c.type._type_affinity, String)

    def _xy_table_fixture(self):
        m = MetaData()
        t = Table('t', m, Column('x', Integer), Column('y', Integer))
        return t

    def _mapping(self, stmt):
        compiled = stmt.compile()
        return dict(
            (elem, key)
            for key, elements in compiled.result_map.items()
            for elem in elements[1]
        )

    def test_select_label_alt_name(self):
        t = self._xy_table_fixture()
        l1, l2 = t.c.x.label('a'), t.c.y.label('b')
        s = text("select x AS a, y AS b FROM t").columns(l1, l2)
        mapping = self._mapping(s)
        assert l1 in mapping

        assert t.c.x not in mapping

    def test_select_alias_label_alt_name(self):
        t = self._xy_table_fixture()
        l1, l2 = t.c.x.label('a'), t.c.y.label('b')
        s = text("select x AS a, y AS b FROM t").columns(l1, l2).alias()
        mapping = self._mapping(s)
        assert l1 in mapping

        assert t.c.x not in mapping

    def test_select_column(self):
        t = self._xy_table_fixture()
        x, y = t.c.x, t.c.y
        s = text("select x, y FROM t").columns(x, y)
        mapping = self._mapping(s)
        assert t.c.x in mapping

    def test_select_alias_column(self):
        t = self._xy_table_fixture()
        x, y = t.c.x, t.c.y
        s = text("select x, y FROM t").columns(x, y).alias()
        mapping = self._mapping(s)
        assert t.c.x in mapping

    def test_select_table_alias_column(self):
        t = self._xy_table_fixture()
        x, y = t.c.x, t.c.y

        ta = t.alias()
        s = text("select ta.x, ta.y FROM t AS ta").columns(ta.c.x, ta.c.y)
        mapping = self._mapping(s)
        assert x not in mapping

    def test_select_label_alt_name_table_alias_column(self):
        t = self._xy_table_fixture()
        x, y = t.c.x, t.c.y

        ta = t.alias()
        l1, l2 = ta.c.x.label('a'), ta.c.y.label('b')

        s = text("SELECT ta.x AS a, ta.y AS b FROM t AS ta").columns(l1, l2)
        mapping = self._mapping(s)
        assert x not in mapping
        assert l1 in mapping
        assert ta.c.x not in mapping

    def test_cte(self):
        t = text("select id, name from user").columns(
            id=Integer,
            name=String).cte('t')

        s = select([table1]).where(table1.c.myid == t.c.id)
        self.assert_compile(
            s,
            "WITH t AS (select id, name from user) "
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable, t WHERE mytable.myid = t.id"
        )

    def test_alias(self):
        t = text("select id, name from user").columns(
            id=Integer,
            name=String).alias('t')

        s = select([table1]).where(table1.c.myid == t.c.id)
        self.assert_compile(
            s,
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable, (select id, name from user) AS t "
            "WHERE mytable.myid = t.id"
        )

    def test_scalar_subquery(self):
        t = text("select id from user").columns(id=Integer)
        subq = t.as_scalar()

        assert subq.type._type_affinity is Integer()._type_affinity

        s = select([table1.c.myid, subq]).where(table1.c.myid == subq)
        self.assert_compile(
            s,
            "SELECT mytable.myid, (select id from user) AS anon_1 "
            "FROM mytable WHERE mytable.myid = (select id from user)"
        )

    def test_build_bindparams(self):
        t = text("select id from user :foo :bar :bat")
        t = t.bindparams(bindparam("foo", type_=Integer))
        t = t.columns(id=Integer)
        t = t.bindparams(bar=String)
        t = t.bindparams(bindparam('bat', value='bat'))

        eq_(
            set(t.element._bindparams),
            set(["bat", "foo", "bar"])
        )
