#! coding:utf-8

from test.lib.testing import eq_, assert_raises, assert_raises_message
import datetime, re, operator, decimal
from sqlalchemy import *
from sqlalchemy import exc, sql, util, types, schema
from sqlalchemy.sql import table, column, label, compiler
from sqlalchemy.sql.expression import ClauseList, _literal_as_text
from sqlalchemy.engine import default
from sqlalchemy.databases import *
from test.lib import *
from sqlalchemy.ext.compiler import compiles

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

table3 = table(
    'thirdtable',
    column('userid', Integer),
    column('otherstuff', String),
)

metadata = MetaData()

# table with a schema
table4 = Table(
    'remotetable', metadata,
    Column('rem_id', Integer, primary_key=True),
    Column('datatype_id', Integer),
    Column('value', String(20)),
    schema = 'remote_owner'
)

# table with a 'multipart' schema
table5 = Table(
    'remotetable', metadata,
    Column('rem_id', Integer, primary_key=True),
    Column('datatype_id', Integer),
    Column('value', String(20)),
    schema = 'dbo.remote_owner'
)

users = table('users',
    column('user_id'),
    column('user_name'),
    column('password'),
)

addresses = table('addresses',
    column('address_id'),
    column('user_id'),
    column('street'),
    column('city'),
    column('state'),
    column('zip')
)

class SelectTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = 'default'

    def test_attribute_sanity(self):
        assert hasattr(table1, 'c')
        assert hasattr(table1.select(), 'c')
        assert not hasattr(table1.c.myid.self_group(), 'columns')
        assert hasattr(table1.select().self_group(), 'columns')
        assert not hasattr(table1.c.myid, 'columns')
        assert not hasattr(table1.c.myid, 'c')
        assert not hasattr(table1.select().c.myid, 'c')
        assert not hasattr(table1.select().c.myid, 'columns')
        assert not hasattr(table1.alias().c.myid, 'columns')
        assert not hasattr(table1.alias().c.myid, 'c')
        if util.compat.py32:
            assert_raises_message(
                exc.InvalidRequestError,
                'Scalar Select expression has no '
                'columns; use this object directly within a '
                'column-level expression.',
                lambda: hasattr(select([table1.c.myid]).as_scalar().self_group(), 'columns'))
            assert_raises_message(
                exc.InvalidRequestError,
                'Scalar Select expression has no '
                'columns; use this object directly within a '
                'column-level expression.',
                lambda: hasattr(select([table1.c.myid]).as_scalar(), 'columns'))
        else:
            assert not hasattr(select([table1.c.myid]).as_scalar().self_group(), 'columns')
            assert not hasattr(select([table1.c.myid]).as_scalar(), 'columns')

    def test_table_select(self):
        self.assert_compile(table1.select(), 
                            "SELECT mytable.myid, mytable.name, "
                            "mytable.description FROM mytable")

        self.assert_compile(select([table1, table2]), 
                            "SELECT mytable.myid, mytable.name, mytable.description, "
                            "myothertable.otherid, myothertable.othername FROM mytable, "
                            "myothertable")

    def test_invalid_col_argument(self):
        assert_raises(exc.ArgumentError, select, table1)
        assert_raises(exc.ArgumentError, select, table1.c.myid)

    def test_int_limit_offset_coercion(self):
        for given, exp in [
            ("5", 5),
            (5, 5),
            (5.2, 5),
            (decimal.Decimal("5"), 5),
            (None, None),
        ]:
            eq_(select().limit(given)._limit, exp)
            eq_(select().offset(given)._offset, exp)
            eq_(select(limit=given)._limit, exp)
            eq_(select(offset=given)._offset, exp)

        assert_raises(ValueError, select().limit, "foo")
        assert_raises(ValueError, select().offset, "foo")
        assert_raises(ValueError, select, offset="foo")
        assert_raises(ValueError, select, limit="foo")

    def test_limit_offset(self):
        for lim, offset, exp, params in [
            (5, 10, "LIMIT :param_1 OFFSET :param_2", 
                                {'param_1':5, 'param_2':10}),
            (None, 10, "LIMIT -1 OFFSET :param_1", {'param_1':10}),
            (5, None, "LIMIT :param_1", {'param_1':5}),
            (0, 0, "LIMIT :param_1 OFFSET :param_2", 
                                {'param_1':0, 'param_2':0}),
        ]:
            self.assert_compile(
                select([1]).limit(lim).offset(offset),
                "SELECT 1 " + exp,
                checkparams =params
            )

    def test_from_subquery(self):
        """tests placing select statements in the column clause of another select, for the
        purposes of selecting from the exported columns of that select."""

        s = select([table1], table1.c.name == 'jack')
        self.assert_compile(
            select(
                [s],
                s.c.myid == 7
            )
            ,
        "SELECT myid, name, description FROM (SELECT mytable.myid AS myid, "
        "mytable.name AS name, mytable.description AS description FROM mytable "
        "WHERE mytable.name = :name_1) WHERE myid = :myid_1")

        sq = select([table1])
        self.assert_compile(
            sq.select(),
            "SELECT myid, name, description FROM (SELECT mytable.myid AS myid, "
            "mytable.name AS name, mytable.description AS description FROM mytable)"
        )

        sq = select(
            [table1],
        ).alias('sq')

        self.assert_compile(
            sq.select(sq.c.myid == 7),
            "SELECT sq.myid, sq.name, sq.description FROM "
            "(SELECT mytable.myid AS myid, mytable.name AS name, "
            "mytable.description AS description FROM mytable) AS sq WHERE sq.myid = :myid_1"
        )

        sq = select(
            [table1, table2],
            and_(table1.c.myid ==7, table2.c.otherid==table1.c.myid),
            use_labels = True
        ).alias('sq')

        sqstring = "SELECT mytable.myid AS mytable_myid, mytable.name AS "\
                "mytable_name, mytable.description AS mytable_description, "\
                "myothertable.otherid AS myothertable_otherid, "\
                "myothertable.othername AS myothertable_othername FROM "\
                "mytable, myothertable WHERE mytable.myid = :myid_1 AND "\
                "myothertable.otherid = mytable.myid"

        self.assert_compile(
                sq.select(), 
                "SELECT sq.mytable_myid, sq.mytable_name, "
                "sq.mytable_description, sq.myothertable_otherid, "
                "sq.myothertable_othername FROM (%s) AS sq" % sqstring)

        sq2 = select(
            [sq],
            use_labels = True
        ).alias('sq2')

        self.assert_compile(
                sq2.select(), 
                "SELECT sq2.sq_mytable_myid, sq2.sq_mytable_name, "
                "sq2.sq_mytable_description, sq2.sq_myothertable_otherid, "
                "sq2.sq_myothertable_othername FROM (SELECT sq.mytable_myid AS "
                "sq_mytable_myid, sq.mytable_name AS sq_mytable_name, "
                "sq.mytable_description AS sq_mytable_description, "
                "sq.myothertable_otherid AS sq_myothertable_otherid, "
                "sq.myothertable_othername AS sq_myothertable_othername "
                "FROM (%s) AS sq) AS sq2" % sqstring)

    def test_select_from_clauselist(self):
        self.assert_compile(
            select([ClauseList(column('a'), column('b'))]).select_from('sometable'), 
            'SELECT a, b FROM sometable'
        )

    def test_use_labels(self):
        self.assert_compile(
            select([table1.c.myid==5], use_labels=True),
            "SELECT mytable.myid = :myid_1 AS anon_1 FROM mytable"
        )

        self.assert_compile(
            select([func.foo()], use_labels=True),
            "SELECT foo() AS foo_1"
        )

        self.assert_compile(
            select([not_(True)], use_labels=True),
            "SELECT NOT :param_1"       # TODO: should this make an anon label ??
        )

        self.assert_compile(
            select([cast("data", Integer)], use_labels=True),
            "SELECT CAST(:param_1 AS INTEGER) AS anon_1"
        )

        self.assert_compile(
            select([func.sum(func.lala(table1.c.myid).label('foo')).label('bar')]),
            "SELECT sum(lala(mytable.myid)) AS bar FROM mytable"
        )

    def test_paramstyles(self):
        stmt = text("select :foo, :bar, :bat from sometable")

        self.assert_compile(
            stmt,
            "select ?, ?, ? from sometable"
            , dialect=default.DefaultDialect(paramstyle='qmark')
        )
        self.assert_compile(
            stmt,
            "select :foo, :bar, :bat from sometable"
            , dialect=default.DefaultDialect(paramstyle='named')
        )
        self.assert_compile(
            stmt,
            "select %s, %s, %s from sometable"
            , dialect=default.DefaultDialect(paramstyle='format')
        )
        self.assert_compile(
            stmt,
            "select :1, :2, :3 from sometable"
            , dialect=default.DefaultDialect(paramstyle='numeric')
        )
        self.assert_compile(
            stmt,
            "select %(foo)s, %(bar)s, %(bat)s from sometable"
            , dialect=default.DefaultDialect(paramstyle='pyformat')
        )

    def test_dupe_columns(self):
        """test that deduping is performed against clause element identity, not rendered result."""

        self.assert_compile(
            select([column('a'), column('a'), column('a')]),
            "SELECT a, a, a"
            , dialect=default.DefaultDialect()
        )

        c = column('a')
        self.assert_compile(
            select([c, c, c]),
            "SELECT a"
            , dialect=default.DefaultDialect()
        )

        a, b = column('a'), column('b')
        self.assert_compile(
            select([a, b, b, b, a, a]),
            "SELECT a, b"
            , dialect=default.DefaultDialect()
        )

        self.assert_compile(
            select([bindparam('a'), bindparam('b'), bindparam('c')]),
            "SELECT :a AS anon_1, :b AS anon_2, :c AS anon_3"
            , dialect=default.DefaultDialect(paramstyle='named')
        )

        self.assert_compile(
            select([bindparam('a'), bindparam('b'), bindparam('c')]),
            "SELECT ? AS anon_1, ? AS anon_2, ? AS anon_3"
            , dialect=default.DefaultDialect(paramstyle='qmark'),
        )

        self.assert_compile(
            select(["a", "a", "a"]),
            "SELECT a, a, a"
        )

        s = select([bindparam('a'), bindparam('b'), bindparam('c')])
        s = s.compile(dialect=default.DefaultDialect(paramstyle='qmark'))
        eq_(s.positiontup, ['a', 'b', 'c'])

    def test_nested_uselabels(self):
        """test nested anonymous label generation.  this
        essentially tests the ANONYMOUS_LABEL regex.

        """

        s1 = table1.select()
        s2 = s1.alias()
        s3 = select([s2], use_labels=True)
        s4 = s3.alias()
        s5 = select([s4], use_labels=True)
        self.assert_compile(s5,
                            'SELECT anon_1.anon_2_myid AS '
                            'anon_1_anon_2_myid, anon_1.anon_2_name AS '
                            'anon_1_anon_2_name, anon_1.anon_2_descript'
                            'ion AS anon_1_anon_2_description FROM '
                            '(SELECT anon_2.myid AS anon_2_myid, '
                            'anon_2.name AS anon_2_name, '
                            'anon_2.description AS anon_2_description '
                            'FROM (SELECT mytable.myid AS myid, '
                            'mytable.name AS name, mytable.description '
                            'AS description FROM mytable) AS anon_2) '
                            'AS anon_1')

    def test_dont_overcorrelate(self):
        self.assert_compile(select([table1], from_obj=[table1,
                            table1.select()]),
                            "SELECT mytable.myid, mytable.name, "
                            "mytable.description FROM mytable, (SELECT "
                            "mytable.myid AS myid, mytable.name AS "
                            "name, mytable.description AS description "
                            "FROM mytable)")

    def test_full_correlate(self):
        # intentional
        t = table('t', column('a'), column('b'))
        s = select([t.c.a]).where(t.c.a==1).correlate(t).as_scalar()

        s2 = select([t.c.a, s])
        self.assert_compile(s2, """SELECT t.a, (SELECT t.a WHERE t.a = :a_1) AS anon_1 FROM t""")

        # unintentional
        t2 = table('t2', column('c'), column('d'))
        s = select([t.c.a]).where(t.c.a==t2.c.d).as_scalar()
        s2 =select([t, t2, s])
        assert_raises(exc.InvalidRequestError, str, s2)

        # intentional again
        s = s.correlate(t, t2)
        s2 =select([t, t2, s])
        self.assert_compile(s, "SELECT t.a WHERE t.a = t2.d")

    def test_exists(self):
        s = select([table1.c.myid]).where(table1.c.myid==5)

        self.assert_compile(exists(s), 
                    "EXISTS (SELECT mytable.myid FROM mytable WHERE mytable.myid = :myid_1)"
                )

        self.assert_compile(exists(s.as_scalar()), 
                    "EXISTS (SELECT mytable.myid FROM mytable WHERE mytable.myid = :myid_1)"
                )

        self.assert_compile(exists([table1.c.myid], table1.c.myid
                            == 5).select(),
                            'SELECT EXISTS (SELECT mytable.myid FROM '
                            'mytable WHERE mytable.myid = :myid_1)',
                            params={'mytable_myid': 5})
        self.assert_compile(select([table1, exists([1],
                            from_obj=table2)]),
                            'SELECT mytable.myid, mytable.name, '
                            'mytable.description, EXISTS (SELECT 1 '
                            'FROM myothertable) FROM mytable',
                            params={})
        self.assert_compile(select([table1, exists([1],
                            from_obj=table2).label('foo')]),
                            'SELECT mytable.myid, mytable.name, '
                            'mytable.description, EXISTS (SELECT 1 '
                            'FROM myothertable) AS foo FROM mytable',
                            params={})

        self.assert_compile(table1.select(exists().where(table2.c.otherid
                            == table1.c.myid).correlate(table1)),
                            'SELECT mytable.myid, mytable.name, '
                            'mytable.description FROM mytable WHERE '
                            'EXISTS (SELECT * FROM myothertable WHERE '
                            'myothertable.otherid = mytable.myid)')
        self.assert_compile(table1.select(exists().where(table2.c.otherid
                            == table1.c.myid).correlate(table1)),
                            'SELECT mytable.myid, mytable.name, '
                            'mytable.description FROM mytable WHERE '
                            'EXISTS (SELECT * FROM myothertable WHERE '
                            'myothertable.otherid = mytable.myid)')
        self.assert_compile(table1.select(exists().where(table2.c.otherid
                            == table1.c.myid).correlate(table1)).replace_selectable(table2,
                            table2.alias()),
                            'SELECT mytable.myid, mytable.name, '
                            'mytable.description FROM mytable WHERE '
                            'EXISTS (SELECT * FROM myothertable AS '
                            'myothertable_1 WHERE myothertable_1.otheri'
                            'd = mytable.myid)')
        self.assert_compile(table1.select(exists().where(table2.c.otherid
                            == table1.c.myid).correlate(table1)).select_from(table1.join(table2,
                            table1.c.myid
                            == table2.c.otherid)).replace_selectable(table2,
                            table2.alias()),
                            'SELECT mytable.myid, mytable.name, '
                            'mytable.description FROM mytable JOIN '
                            'myothertable AS myothertable_1 ON '
                            'mytable.myid = myothertable_1.otherid '
                            'WHERE EXISTS (SELECT * FROM myothertable '
                            'AS myothertable_1 WHERE '
                            'myothertable_1.otherid = mytable.myid)')

        self.assert_compile(
            select([
                or_(
                    exists().where(table2.c.otherid=='foo'),
                    exists().where(table2.c.otherid=='bar')
                )
            ]),
            "SELECT (EXISTS (SELECT * FROM myothertable "
            "WHERE myothertable.otherid = :otherid_1)) "
            "OR (EXISTS (SELECT * FROM myothertable WHERE "
            "myothertable.otherid = :otherid_2)) AS anon_1"
        )


    def test_where_subquery(self):
        s = select([addresses.c.street], addresses.c.user_id
                   == users.c.user_id, correlate=True).alias('s')
        self.assert_compile(select([users, s.c.street], from_obj=s),
                            "SELECT users.user_id, users.user_name, "
                            "users.password, s.street FROM users, "
                            "(SELECT addresses.street AS street FROM "
                            "addresses WHERE addresses.user_id = "
                            "users.user_id) AS s")
        self.assert_compile(table1.select(table1.c.myid
                            == select([table1.c.myid], table1.c.name
                            == 'jack')),
                            'SELECT mytable.myid, mytable.name, '
                            'mytable.description FROM mytable WHERE '
                            'mytable.myid = (SELECT mytable.myid FROM '
                            'mytable WHERE mytable.name = :name_1)')
        self.assert_compile(table1.select(table1.c.myid
                            == select([table2.c.otherid], table1.c.name
                            == table2.c.othername)),
                            'SELECT mytable.myid, mytable.name, '
                            'mytable.description FROM mytable WHERE '
                            'mytable.myid = (SELECT '
                            'myothertable.otherid FROM myothertable '
                            'WHERE mytable.name = myothertable.othernam'
                            'e)')
        self.assert_compile(table1.select(exists([1], table2.c.otherid
                            == table1.c.myid)),
                            'SELECT mytable.myid, mytable.name, '
                            'mytable.description FROM mytable WHERE '
                            'EXISTS (SELECT 1 FROM myothertable WHERE '
                            'myothertable.otherid = mytable.myid)')
        talias = table1.alias('ta')
        s = subquery('sq2', [talias], exists([1], table2.c.otherid
                     == talias.c.myid))
        self.assert_compile(select([s, table1]),
                            'SELECT sq2.myid, sq2.name, '
                            'sq2.description, mytable.myid, '
                            'mytable.name, mytable.description FROM '
                            '(SELECT ta.myid AS myid, ta.name AS name, '
                            'ta.description AS description FROM '
                            'mytable AS ta WHERE EXISTS (SELECT 1 FROM '
                            'myothertable WHERE myothertable.otherid = '
                            'ta.myid)) AS sq2, mytable')
        s = select([addresses.c.street], addresses.c.user_id
                   == users.c.user_id, correlate=True).alias('s')
        self.assert_compile(select([users, s.c.street], from_obj=s),
                            "SELECT users.user_id, users.user_name, "
                            "users.password, s.street FROM users, "
                            "(SELECT addresses.street AS street FROM "
                            "addresses WHERE addresses.user_id = "
                            "users.user_id) AS s")

        # test constructing the outer query via append_column(), which
        # occurs in the ORM's Query object

        s = select([], exists([1], table2.c.otherid == table1.c.myid),
                   from_obj=table1)
        s.append_column(table1)
        self.assert_compile(s,
                            'SELECT mytable.myid, mytable.name, '
                            'mytable.description FROM mytable WHERE '
                            'EXISTS (SELECT 1 FROM myothertable WHERE '
                            'myothertable.otherid = mytable.myid)')


    def test_orderby_subquery(self):
        self.assert_compile(table1.select(order_by=[select([table2.c.otherid],
                            table1.c.myid == table2.c.otherid)]),
                            'SELECT mytable.myid, mytable.name, '
                            'mytable.description FROM mytable ORDER BY '
                            '(SELECT myothertable.otherid FROM '
                            'myothertable WHERE mytable.myid = '
                            'myothertable.otherid)')
        self.assert_compile(table1.select(order_by=[desc(select([table2.c.otherid],
                            table1.c.myid == table2.c.otherid))]),
                            'SELECT mytable.myid, mytable.name, '
                            'mytable.description FROM mytable ORDER BY '
                            '(SELECT myothertable.otherid FROM '
                            'myothertable WHERE mytable.myid = '
                            'myothertable.otherid) DESC')

    @testing.uses_deprecated('scalar option')
    def test_scalar_select(self):
        assert_raises_message(
            exc.InvalidRequestError,
            r"Select objects don't have a type\.  Call as_scalar\(\) "
            "on this Select object to return a 'scalar' version of this Select\.",
            func.coalesce, select([table1.c.myid])
        )

        s = select([table1.c.myid], correlate=False).as_scalar()
        self.assert_compile(select([table1, s]),
                            'SELECT mytable.myid, mytable.name, '
                            'mytable.description, (SELECT mytable.myid '
                            'FROM mytable) AS anon_1 FROM mytable')
        s = select([table1.c.myid]).as_scalar()
        self.assert_compile(select([table2, s]),
                            'SELECT myothertable.otherid, '
                            'myothertable.othername, (SELECT '
                            'mytable.myid FROM mytable) AS anon_1 FROM '
                            'myothertable')
        s = select([table1.c.myid]).correlate(None).as_scalar()
        self.assert_compile(select([table1, s]),
                            'SELECT mytable.myid, mytable.name, '
                            'mytable.description, (SELECT mytable.myid '
                            'FROM mytable) AS anon_1 FROM mytable')

        # test that aliases use as_scalar() when used in an explicitly
        # scalar context

        s = select([table1.c.myid]).alias()
        self.assert_compile(select([table1.c.myid]).where(table1.c.myid
                            == s),
                            'SELECT mytable.myid FROM mytable WHERE '
                            'mytable.myid = (SELECT mytable.myid FROM '
                            'mytable)')
        self.assert_compile(select([table1.c.myid]).where(s
                            > table1.c.myid),
                            'SELECT mytable.myid FROM mytable WHERE '
                            'mytable.myid < (SELECT mytable.myid FROM '
                            'mytable)')
        s = select([table1.c.myid]).as_scalar()
        self.assert_compile(select([table2, s]),
                            'SELECT myothertable.otherid, '
                            'myothertable.othername, (SELECT '
                            'mytable.myid FROM mytable) AS anon_1 FROM '
                            'myothertable')

        # test expressions against scalar selects

        self.assert_compile(select([s - literal(8)]),
                            'SELECT (SELECT mytable.myid FROM mytable) '
                            '- :param_1 AS anon_1')
        self.assert_compile(select([select([table1.c.name]).as_scalar()
                            + literal('x')]),
                            'SELECT (SELECT mytable.name FROM mytable) '
                            '|| :param_1 AS anon_1')
        self.assert_compile(select([s > literal(8)]),
                            'SELECT (SELECT mytable.myid FROM mytable) '
                            '> :param_1 AS anon_1')
        self.assert_compile(select([select([table1.c.name]).label('foo'
                            )]),
                            'SELECT (SELECT mytable.name FROM mytable) '
                            'AS foo')

        # scalar selects should not have any attributes on their 'c' or
        # 'columns' attribute

        s = select([table1.c.myid]).as_scalar()
        try:
            s.c.foo
        except exc.InvalidRequestError, err:
            assert str(err) \
                == 'Scalar Select expression has no columns; use this '\
                'object directly within a column-level expression.'
        try:
            s.columns.foo
        except exc.InvalidRequestError, err:
            assert str(err) \
                == 'Scalar Select expression has no columns; use this '\
                'object directly within a column-level expression.'

        zips = table('zips',
            column('zipcode'),
            column('latitude'),
            column('longitude'),
        )
        places = table('places',
            column('id'),
            column('nm')
        )
        zip = '12345'
        qlat = select([zips.c.latitude], zips.c.zipcode == zip).correlate(None).as_scalar()
        qlng = select([zips.c.longitude], zips.c.zipcode == zip).correlate(None).as_scalar()

        q = select([places.c.id, places.c.nm, zips.c.zipcode, func.latlondist(qlat, qlng).label('dist')],
                         zips.c.zipcode==zip,
                         order_by = ['dist', places.c.nm]
                         )

        self.assert_compile(q,
                            'SELECT places.id, places.nm, '
                            'zips.zipcode, latlondist((SELECT '
                            'zips.latitude FROM zips WHERE '
                            'zips.zipcode = :zipcode_1), (SELECT '
                            'zips.longitude FROM zips WHERE '
                            'zips.zipcode = :zipcode_2)) AS dist FROM '
                            'places, zips WHERE zips.zipcode = '
                            ':zipcode_3 ORDER BY dist, places.nm')

        zalias = zips.alias('main_zip')
        qlat = select([zips.c.latitude], zips.c.zipcode == zalias.c.zipcode).as_scalar()
        qlng = select([zips.c.longitude], zips.c.zipcode == zalias.c.zipcode).as_scalar()
        q = select([places.c.id, places.c.nm, zalias.c.zipcode,
                   func.latlondist(qlat, qlng).label('dist')],
                   order_by=['dist', places.c.nm])
        self.assert_compile(q,
                            'SELECT places.id, places.nm, '
                            'main_zip.zipcode, latlondist((SELECT '
                            'zips.latitude FROM zips WHERE '
                            'zips.zipcode = main_zip.zipcode), (SELECT '
                            'zips.longitude FROM zips WHERE '
                            'zips.zipcode = main_zip.zipcode)) AS dist '
                            'FROM places, zips AS main_zip ORDER BY '
                            'dist, places.nm')

        a1 = table2.alias('t2alias')
        s1 = select([a1.c.otherid], table1.c.myid==a1.c.otherid).as_scalar()
        j1 = table1.join(table2, table1.c.myid==table2.c.otherid)
        s2 = select([table1, s1], from_obj=j1)
        self.assert_compile(s2,
                            'SELECT mytable.myid, mytable.name, '
                            'mytable.description, (SELECT '
                            't2alias.otherid FROM myothertable AS '
                            't2alias WHERE mytable.myid = '
                            't2alias.otherid) AS anon_1 FROM mytable '
                            'JOIN myothertable ON mytable.myid = '
                            'myothertable.otherid')

    def test_label_comparison(self):
        x = func.lala(table1.c.myid).label('foo')
        self.assert_compile(select([x], x == 5),
                            'SELECT lala(mytable.myid) AS foo FROM '
                            'mytable WHERE lala(mytable.myid) = '
                            ':param_1')

        self.assert_compile(
                label('bar', column('foo', type_=String))+ 'foo', 
                'foo || :param_1')


    def test_conjunctions(self):
        a, b, c = 'a', 'b', 'c'
        x = and_(a, b, c)
        assert isinstance(x.type,  Boolean)
        assert str(x) == 'a AND b AND c'
        self.assert_compile(
            select([x.label('foo')]),
            'SELECT a AND b AND c AS foo'
        )

        self.assert_compile(
            and_(table1.c.myid == 12, table1.c.name=='asdf', 
                table2.c.othername == 'foo', "sysdate() = today()"),
            "mytable.myid = :myid_1 AND mytable.name = :name_1 "\
            "AND myothertable.othername = :othername_1 AND sysdate() = today()"
        )

        self.assert_compile(
            and_(
                table1.c.myid == 12,
                or_(table2.c.othername=='asdf', 
                    table2.c.othername == 'foo', table2.c.otherid == 9),
                "sysdate() = today()",
            ),
            'mytable.myid = :myid_1 AND (myothertable.othername = '
             ':othername_1 OR myothertable.othername = :othername_2 OR '
             'myothertable.otherid = :otherid_1) AND sysdate() = '
             'today()', 
            checkparams = {'othername_1': 'asdf', 'othername_2':'foo', 'otherid_1': 9, 'myid_1': 12}
        )

    def test_nested_conjunctions_short_circuit(self):
        """test that empty or_(), and_() conjunctions are collapsed by
        an enclosing conjunction."""

        t = table('t', column('x'))

        self.assert_compile(
            select([t]).where(and_(t.c.x==5, 
                or_(and_(or_(t.c.x==7))))),
            "SELECT t.x FROM t WHERE t.x = :x_1 AND t.x = :x_2"
        )
        self.assert_compile(
            select([t]).where(and_(or_(t.c.x==12, 
                and_(or_(t.c.x==8))))),
            "SELECT t.x FROM t WHERE t.x = :x_1 OR t.x = :x_2"
        )
        self.assert_compile(
            select([t]).where(and_(or_(or_(t.c.x==12), 
                and_(or_(), or_(and_(t.c.x==8)), and_())))),
            "SELECT t.x FROM t WHERE t.x = :x_1 OR t.x = :x_2"
        )

    def test_distinct(self):
        self.assert_compile(
            select([table1.c.myid.distinct()]), 
            "SELECT DISTINCT mytable.myid FROM mytable"
        )

        self.assert_compile(
            select([distinct(table1.c.myid)]), 
            "SELECT DISTINCT mytable.myid FROM mytable"
        )

        self.assert_compile(
            select([table1.c.myid]).distinct(), 
            "SELECT DISTINCT mytable.myid FROM mytable"
        )

        self.assert_compile(
            select([func.count(table1.c.myid.distinct())]), 
            "SELECT count(DISTINCT mytable.myid) AS count_1 FROM mytable"
        )

        self.assert_compile(
            select([func.count(distinct(table1.c.myid))]), 
            "SELECT count(DISTINCT mytable.myid) AS count_1 FROM mytable"
        )

    def test_operators(self):
        for (py_op, sql_op) in ((operator.add, '+'), (operator.mul, '*'),
                                (operator.sub, '-'), 
                                # Py3K
                                #(operator.truediv, '/'),
                                # Py2K
                                (operator.div, '/'),
                                # end Py2K
                                ):
            for (lhs, rhs, res) in (
                (5, table1.c.myid, ':myid_1 %s mytable.myid'),
                (5, literal(5), ':param_1 %s :param_2'),
                (table1.c.myid, 'b', 'mytable.myid %s :myid_1'),
                (table1.c.myid, literal(2.7), 'mytable.myid %s :param_1'),
                (table1.c.myid, table1.c.myid, 'mytable.myid %s mytable.myid'),
                (literal(5), 8, ':param_1 %s :param_2'),
                (literal(6), table1.c.myid, ':param_1 %s mytable.myid'),
                (literal(7), literal(5.5), ':param_1 %s :param_2'),
                ):
                self.assert_compile(py_op(lhs, rhs), res % sql_op)

        dt = datetime.datetime.today()
        # exercise comparison operators
        for (py_op, fwd_op, rev_op) in ((operator.lt, '<', '>'),
                                        (operator.gt, '>', '<'),
                                        (operator.eq, '=', '='),
                                        (operator.ne, '!=', '!='),
                                        (operator.le, '<=', '>='),
                                        (operator.ge, '>=', '<=')):
            for (lhs, rhs, l_sql, r_sql) in (
                ('a', table1.c.myid, ':myid_1', 'mytable.myid'),
                ('a', literal('b'), ':param_2', ':param_1'), # note swap!
                (table1.c.myid, 'b', 'mytable.myid', ':myid_1'),
                (table1.c.myid, literal('b'), 'mytable.myid', ':param_1'),
                (table1.c.myid, table1.c.myid, 'mytable.myid', 'mytable.myid'),
                (literal('a'), 'b', ':param_1', ':param_2'),
                (literal('a'), table1.c.myid, ':param_1', 'mytable.myid'),
                (literal('a'), literal('b'), ':param_1', ':param_2'),
                (dt, literal('b'), ':param_2', ':param_1'),
                (literal('b'), dt, ':param_1', ':param_2'),
                ):

                # the compiled clause should match either (e.g.):
                # 'a' < 'b' -or- 'b' > 'a'.
                compiled = str(py_op(lhs, rhs))
                fwd_sql = "%s %s %s" % (l_sql, fwd_op, r_sql)
                rev_sql = "%s %s %s" % (r_sql, rev_op, l_sql)

                self.assert_(compiled == fwd_sql or compiled == rev_sql,
                             "\n'" + compiled + "'\n does not match\n'" +
                             fwd_sql + "'\n or\n'" + rev_sql + "'")

        for (py_op, op) in (
            (operator.neg, '-'),
            (operator.inv, 'NOT '),
        ):
            for expr, sql in (
                (table1.c.myid, "mytable.myid"),
                (literal("foo"), ":param_1"),
            ):

                compiled = str(py_op(expr))
                sql = "%s%s" % (op, sql)
                eq_(compiled, sql)

        self.assert_compile(
         table1.select((table1.c.myid != 12) & ~(table1.c.name=='john')),
         "SELECT mytable.myid, mytable.name, mytable.description FROM "
            "mytable WHERE mytable.myid != :myid_1 AND mytable.name != :name_1"
        )

        self.assert_compile(
         table1.select((table1.c.myid != 12) & 
                ~(table1.c.name.between('jack','john'))),
         "SELECT mytable.myid, mytable.name, mytable.description FROM "
             "mytable WHERE mytable.myid != :myid_1 AND "\
             "NOT (mytable.name BETWEEN :name_1 AND :name_2)"
        )

        self.assert_compile(
         table1.select((table1.c.myid != 12) & 
                ~and_(table1.c.name=='john', table1.c.name=='ed', table1.c.name=='fred')),
         "SELECT mytable.myid, mytable.name, mytable.description FROM "
         "mytable WHERE mytable.myid != :myid_1 AND "\
         "NOT (mytable.name = :name_1 AND mytable.name = :name_2 "
         "AND mytable.name = :name_3)"
        )

        self.assert_compile(
         table1.select((table1.c.myid != 12) & ~table1.c.name),
         "SELECT mytable.myid, mytable.name, mytable.description FROM "
            "mytable WHERE mytable.myid != :myid_1 AND NOT mytable.name"
        )

        self.assert_compile(
         literal("a") + literal("b") * literal("c"), ":param_1 || :param_2 * :param_3"
        )

        # test the op() function, also that its results are further usable in expressions
        self.assert_compile(
            table1.select(table1.c.myid.op('hoho')(12)==14),
            "SELECT mytable.myid, mytable.name, mytable.description FROM "
                    "mytable WHERE (mytable.myid hoho :myid_1) = :param_1"
        )

        # test that clauses can be pickled (operators need to be module-level, etc.)
        clause = (table1.c.myid == 12) & table1.c.myid.between(15, 20) & \
                            table1.c.myid.like('hoho')
        assert str(clause) == str(util.pickle.loads(util.pickle.dumps(clause)))


    def test_like(self):
        for expr, check, dialect in [
            (
                table1.c.myid.like('somstr'), 
                "mytable.myid LIKE :myid_1", None),
            (
                ~table1.c.myid.like('somstr'), 
                "mytable.myid NOT LIKE :myid_1", None),
            (
                table1.c.myid.like('somstr', escape='\\'), 
                "mytable.myid LIKE :myid_1 ESCAPE '\\'", 
                None),
            (
                ~table1.c.myid.like('somstr', escape='\\'), 
                "mytable.myid NOT LIKE :myid_1 ESCAPE '\\'", 
                None),
            (
                table1.c.myid.ilike('somstr', escape='\\'), 
                "lower(mytable.myid) LIKE lower(:myid_1) ESCAPE '\\'", 
                None),
            (
                ~table1.c.myid.ilike('somstr', escape='\\'), 
                "lower(mytable.myid) NOT LIKE lower(:myid_1) ESCAPE '\\'", 
                None),
            (
                table1.c.myid.ilike('somstr', escape='\\'), 
                    "mytable.myid ILIKE %(myid_1)s ESCAPE '\\\\'", 
                    postgresql.PGDialect()),
            (
                ~table1.c.myid.ilike('somstr', escape='\\'), 
                "mytable.myid NOT ILIKE %(myid_1)s ESCAPE '\\\\'", 
                postgresql.PGDialect()),
            (
                table1.c.name.ilike('%something%'), 
                "lower(mytable.name) LIKE lower(:name_1)", None),
            (
                table1.c.name.ilike('%something%'), 
                "mytable.name ILIKE %(name_1)s", postgresql.PGDialect()),
            (
                ~table1.c.name.ilike('%something%'), 
                "lower(mytable.name) NOT LIKE lower(:name_1)", None),
            (
                ~table1.c.name.ilike('%something%'), 
                "mytable.name NOT ILIKE %(name_1)s", 
                postgresql.PGDialect()),
        ]:
            self.assert_compile(expr, check, dialect=dialect)

    def test_match(self):
        for expr, check, dialect in [
            (table1.c.myid.match('somstr'), 
                        "mytable.myid MATCH ?", sqlite.SQLiteDialect()),
            (table1.c.myid.match('somstr'), 
                        "MATCH (mytable.myid) AGAINST (%s IN BOOLEAN MODE)", 
                        mysql.dialect()),
            (table1.c.myid.match('somstr'), 
                        "CONTAINS (mytable.myid, :myid_1)", 
                        mssql.dialect()),
            (table1.c.myid.match('somstr'), 
                        "mytable.myid @@ to_tsquery(%(myid_1)s)", 
                        postgresql.dialect()),
            (table1.c.myid.match('somstr'), 
                        "CONTAINS (mytable.myid, :myid_1)", 
                        oracle.dialect()),
        ]:
            self.assert_compile(expr, check, dialect=dialect)

    def test_composed_string_comparators(self):
        self.assert_compile(
            table1.c.name.contains('jo'), 
            "mytable.name LIKE '%%' || :name_1 || '%%'" , 
            checkparams = {'name_1': u'jo'},
        )
        self.assert_compile(
            table1.c.name.contains('jo'), 
            "mytable.name LIKE concat(concat('%%', %s), '%%')" , 
            checkparams = {'name_1': u'jo'},
            dialect=mysql.dialect()
        )
        self.assert_compile(
            table1.c.name.contains('jo', escape='\\'), 
            "mytable.name LIKE '%%' || :name_1 || '%%' ESCAPE '\\'" , 
            checkparams = {'name_1': u'jo'},
        )
        self.assert_compile(
            table1.c.name.startswith('jo', escape='\\'), 
            "mytable.name LIKE :name_1 || '%%' ESCAPE '\\'" )
        self.assert_compile(
            table1.c.name.endswith('jo', escape='\\'), 
            "mytable.name LIKE '%%' || :name_1 ESCAPE '\\'" )
        self.assert_compile(
            table1.c.name.endswith('hn'), 
            "mytable.name LIKE '%%' || :name_1", 
            checkparams = {'name_1': u'hn'}, )
        self.assert_compile(
            table1.c.name.endswith('hn'), 
            "mytable.name LIKE concat('%%', %s)",
            checkparams = {'name_1': u'hn'}, dialect=mysql.dialect()
        )
        self.assert_compile(
            table1.c.name.startswith(u"hi \xf6 \xf5"), 
            "mytable.name LIKE :name_1 || '%%'",
            checkparams = {'name_1': u'hi \xf6 \xf5'},
        )
        self.assert_compile(
                column('name').endswith(text("'foo'")), 
                "name LIKE '%%' || 'foo'"  )
        self.assert_compile(
                column('name').endswith(literal_column("'foo'")), 
                "name LIKE '%%' || 'foo'"  )
        self.assert_compile(
                column('name').startswith(text("'foo'")), 
                "name LIKE 'foo' || '%%'"  )
        self.assert_compile(
                column('name').startswith(text("'foo'")),
                 "name LIKE concat('foo', '%%')", dialect=mysql.dialect())
        self.assert_compile(
                column('name').startswith(literal_column("'foo'")), 
                "name LIKE 'foo' || '%%'"  )
        self.assert_compile(
                column('name').startswith(literal_column("'foo'")), 
                "name LIKE concat('foo', '%%')", dialect=mysql.dialect())

    def test_multiple_col_binds(self):
        self.assert_compile(
            select(["*"], or_(table1.c.myid == 12, table1.c.myid=='asdf',
                            table1.c.myid == 'foo')),
            "SELECT * FROM mytable WHERE mytable.myid = :myid_1 "
            "OR mytable.myid = :myid_2 OR mytable.myid = :myid_3"
        )

    def test_order_by_nulls(self):
        self.assert_compile(
            table2.select(order_by = [table2.c.otherid, table2.c.othername.desc().nullsfirst()]),
            "SELECT myothertable.otherid, myothertable.othername FROM "
            "myothertable ORDER BY myothertable.otherid, myothertable.othername DESC NULLS FIRST"
        )

        self.assert_compile(
            table2.select(order_by = [table2.c.otherid, table2.c.othername.desc().nullslast()]),
            "SELECT myothertable.otherid, myothertable.othername FROM "
            "myothertable ORDER BY myothertable.otherid, myothertable.othername DESC NULLS LAST"
        )

        self.assert_compile(
            table2.select(order_by = [table2.c.otherid.nullslast(), table2.c.othername.desc().nullsfirst()]),
            "SELECT myothertable.otherid, myothertable.othername FROM "
            "myothertable ORDER BY myothertable.otherid NULLS LAST, myothertable.othername DESC NULLS FIRST"
        )

        self.assert_compile(
            table2.select(order_by = [table2.c.otherid.nullsfirst(), table2.c.othername.desc()]),
            "SELECT myothertable.otherid, myothertable.othername FROM "
            "myothertable ORDER BY myothertable.otherid NULLS FIRST, myothertable.othername DESC"
        )

        self.assert_compile(
            table2.select(order_by = [table2.c.otherid.nullsfirst(), table2.c.othername.desc().nullslast()]),
            "SELECT myothertable.otherid, myothertable.othername FROM "
            "myothertable ORDER BY myothertable.otherid NULLS FIRST, myothertable.othername DESC NULLS LAST"
        )

    def test_orderby_groupby(self):
        self.assert_compile(
            table2.select(order_by = [table2.c.otherid, asc(table2.c.othername)]),
            "SELECT myothertable.otherid, myothertable.othername FROM "
            "myothertable ORDER BY myothertable.otherid, myothertable.othername ASC"
        )

        self.assert_compile(
            table2.select(order_by = [table2.c.otherid, table2.c.othername.desc()]),
            "SELECT myothertable.otherid, myothertable.othername FROM "
            "myothertable ORDER BY myothertable.otherid, myothertable.othername DESC"
        )

        # generative order_by
        self.assert_compile(
            table2.select().order_by(table2.c.otherid).order_by(table2.c.othername.desc()),
            "SELECT myothertable.otherid, myothertable.othername FROM "
            "myothertable ORDER BY myothertable.otherid, myothertable.othername DESC"
        )

        self.assert_compile(
            table2.select().order_by(table2.c.otherid).
                                order_by(table2.c.othername.desc()).order_by(None),
            "SELECT myothertable.otherid, myothertable.othername FROM myothertable"
        )

        self.assert_compile(
            select(
                    [table2.c.othername, func.count(table2.c.otherid)], 
                    group_by = [table2.c.othername]),
            "SELECT myothertable.othername, count(myothertable.otherid) AS count_1 "
            "FROM myothertable GROUP BY myothertable.othername"
        )

        # generative group by
        self.assert_compile(
            select([table2.c.othername, func.count(table2.c.otherid)]).
                        group_by(table2.c.othername),
            "SELECT myothertable.othername, count(myothertable.otherid) AS count_1 "
            "FROM myothertable GROUP BY myothertable.othername"
        )

        self.assert_compile(
            select([table2.c.othername, func.count(table2.c.otherid)]).
                        group_by(table2.c.othername).group_by(None),
            "SELECT myothertable.othername, count(myothertable.otherid) AS count_1 "
            "FROM myothertable"
        )

        self.assert_compile(
            select([table2.c.othername, func.count(table2.c.otherid)], 
                        group_by = [table2.c.othername], 
                        order_by = [table2.c.othername]),
            "SELECT myothertable.othername, count(myothertable.otherid) AS count_1 "
            "FROM myothertable GROUP BY myothertable.othername ORDER BY myothertable.othername"
        )

    def test_for_update(self):
        self.assert_compile(
                    table1.select(table1.c.myid==7, for_update=True), 
                    "SELECT mytable.myid, mytable.name, mytable.description "
                    "FROM mytable WHERE mytable.myid = :myid_1 FOR UPDATE")

        self.assert_compile(
                    table1.select(table1.c.myid==7, for_update="nowait"), 
                    "SELECT mytable.myid, mytable.name, mytable.description "
                    "FROM mytable WHERE mytable.myid = :myid_1 FOR UPDATE")

        self.assert_compile(
                    table1.select(table1.c.myid==7, for_update="nowait"), 
                    "SELECT mytable.myid, mytable.name, mytable.description "
                    "FROM mytable WHERE mytable.myid = :myid_1 FOR UPDATE NOWAIT", 
                    dialect=oracle.dialect())

        self.assert_compile(
                    table1.select(table1.c.myid==7, for_update="read"), 
                    "SELECT mytable.myid, mytable.name, mytable.description "
                    "FROM mytable WHERE mytable.myid = %s LOCK IN SHARE MODE", 
                    dialect=mysql.dialect())

        self.assert_compile(
                    table1.select(table1.c.myid==7, for_update=True), 
                    "SELECT mytable.myid, mytable.name, mytable.description "
                    "FROM mytable WHERE mytable.myid = %s FOR UPDATE", 
                    dialect=mysql.dialect())

        self.assert_compile(
                    table1.select(table1.c.myid==7, for_update=True), 
                    "SELECT mytable.myid, mytable.name, mytable.description "
                    "FROM mytable WHERE mytable.myid = :myid_1 FOR UPDATE", 
                    dialect=oracle.dialect())

    def test_alias(self):
        # test the alias for a table1.  column names stay the same, table name "changes" to "foo".
        self.assert_compile(
            select([table1.alias('foo')])
            ,"SELECT foo.myid, foo.name, foo.description FROM mytable AS foo")

        for dialect in (oracle.dialect(),):
            self.assert_compile(
                select([table1.alias('foo')])
                ,"SELECT foo.myid, foo.name, foo.description FROM mytable foo"
                ,dialect=dialect)

        self.assert_compile(
            select([table1.alias()]),
            "SELECT mytable_1.myid, mytable_1.name, mytable_1.description "
            "FROM mytable AS mytable_1")

        # create a select for a join of two tables.  use_labels 
        # means the column names will have labels tablename_columnname, 
        # which become the column keys accessible off the Selectable object.
        # also, only use one column from the second table and all columns 
        # from the first table1.
        q = select(
                        [table1, table2.c.otherid], 
                        table1.c.myid == table2.c.otherid, use_labels = True
                    )

        # make an alias of the "selectable".  column names 
        # stay the same (i.e. the labels), table name "changes" to "t2view".
        a = alias(q, 't2view')

        # select from that alias, also using labels.  two levels of labels should produce two underscores.
        # also, reference the column "mytable_myid" off of the t2view alias.
        self.assert_compile(
            a.select(a.c.mytable_myid == 9, use_labels = True),
            "SELECT t2view.mytable_myid AS t2view_mytable_myid, t2view.mytable_name "
            "AS t2view_mytable_name, t2view.mytable_description AS t2view_mytable_description, "
            "t2view.myothertable_otherid AS t2view_myothertable_otherid FROM "
            "(SELECT mytable.myid AS mytable_myid, mytable.name AS mytable_name, "
            "mytable.description AS mytable_description, myothertable.otherid AS "
            "myothertable_otherid FROM mytable, myothertable WHERE mytable.myid = "
            "myothertable.otherid) AS t2view WHERE t2view.mytable_myid = :mytable_myid_1"
        )


    def test_prefixes(self):
        self.assert_compile(
            table1.select().prefix_with("SQL_CALC_FOUND_ROWS").\
                                prefix_with("SQL_SOME_WEIRD_MYSQL_THING"),
            "SELECT SQL_CALC_FOUND_ROWS SQL_SOME_WEIRD_MYSQL_THING "
            "mytable.myid, mytable.name, mytable.description FROM mytable"
        )

    def test_text(self):
        self.assert_compile(
            text("select * from foo where lala = bar") ,
            "select * from foo where lala = bar"
        )

        # test bytestring
        self.assert_compile(select(
            ["foobar(a)", "pk_foo_bar(syslaal)"],
            "a = 12",
            from_obj = ["foobar left outer join lala on foobar.foo = lala.foo"]
            ),
            "SELECT foobar(a), pk_foo_bar(syslaal) FROM foobar "
            "left outer join lala on foobar.foo = lala.foo WHERE a = 12"
        )

        # test unicode
        self.assert_compile(select(
            [u"foobar(a)", u"pk_foo_bar(syslaal)"],
            u"a = 12",
            from_obj = [u"foobar left outer join lala on foobar.foo = lala.foo"]
            ), 
            "SELECT foobar(a), pk_foo_bar(syslaal) FROM foobar "
            "left outer join lala on foobar.foo = lala.foo WHERE a = 12"
        )

        # test building a select query programmatically with text
        s = select()
        s.append_column("column1")
        s.append_column("column2")
        s.append_whereclause("column1=12")
        s.append_whereclause("column2=19")
        s = s.order_by("column1")
        s.append_from("table1")
        self.assert_compile(s, "SELECT column1, column2 FROM table1 WHERE "
                                "column1=12 AND column2=19 ORDER BY column1")

        self.assert_compile(
            select(["column1", "column2"], from_obj=table1).alias('somealias').select(),
            "SELECT somealias.column1, somealias.column2 FROM "
            "(SELECT column1, column2 FROM mytable) AS somealias"
        )

        # test that use_labels doesnt interfere with literal columns
        self.assert_compile(
            select(["column1", "column2", table1.c.myid], from_obj=table1, use_labels=True),
            "SELECT column1, column2, mytable.myid AS mytable_myid FROM mytable"
        )

        # test that use_labels doesnt interfere with literal columns that have textual labels
        self.assert_compile(
            select(["column1 AS foobar", "column2 AS hoho", table1.c.myid], from_obj=table1, use_labels=True),
            "SELECT column1 AS foobar, column2 AS hoho, mytable.myid AS mytable_myid FROM mytable"
        )

        s1 = select(["column1 AS foobar", "column2 AS hoho", table1.c.myid], from_obj=[table1])
        # test that "auto-labeling of subquery columns" doesnt interfere with literal columns,
        # exported columns dont get quoted
        self.assert_compile(
            select(["column1 AS foobar", "column2 AS hoho", table1.c.myid], from_obj=[table1]).select(),
            "SELECT column1 AS foobar, column2 AS hoho, myid FROM "
            "(SELECT column1 AS foobar, column2 AS hoho, mytable.myid AS myid FROM mytable)"
        )

        self.assert_compile(
            select(['col1','col2'], from_obj='tablename').alias('myalias'),
            "SELECT col1, col2 FROM tablename"
        )

    def test_binds_in_text(self):
        self.assert_compile(
            text("select * from foo where lala=:bar and hoho=:whee", 
                bindparams=[bindparam('bar', 4), bindparam('whee', 7)]),
                "select * from foo where lala=:bar and hoho=:whee",
                checkparams={'bar':4, 'whee': 7},
        )

        self.assert_compile(
            text("select * from foo where clock='05:06:07'"),
                "select * from foo where clock='05:06:07'",
                checkparams={},
                params={},
        )

        dialect = postgresql.dialect()
        self.assert_compile(
            text("select * from foo where lala=:bar and hoho=:whee", 
                bindparams=[bindparam('bar',4), bindparam('whee',7)]),
                "select * from foo where lala=%(bar)s and hoho=%(whee)s",
                checkparams={'bar':4, 'whee': 7},
                dialect=dialect
        )

        # test escaping out text() params with a backslash
        self.assert_compile(
            text("select * from foo where clock='05:06:07' and mork='\:mindy'"),
            "select * from foo where clock='05:06:07' and mork=':mindy'",
            checkparams={},
            params={},
            dialect=dialect
        )

        dialect = sqlite.dialect()
        self.assert_compile(
            text("select * from foo where lala=:bar and hoho=:whee", 
                bindparams=[bindparam('bar',4), bindparam('whee',7)]),
                "select * from foo where lala=? and hoho=?",
                checkparams={'bar':4, 'whee':7},
                dialect=dialect
        )

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

        self.assert_compile(select(
            [alias(table1, 't'), "foo.f"],
            "foo.f = t.id",
            from_obj = ["(select f from bar where lala=heyhey) foo"]
        ),
        "SELECT t.myid, t.name, t.description, foo.f FROM mytable AS t, "
        "(select f from bar where lala=heyhey) foo WHERE foo.f = t.id")

        # test Text embedded within select_from(), using binds
        generate_series = text(
                            "generate_series(:x, :y, :z) as s(a)", 
                            bindparams=[bindparam('x'), bindparam('y'), bindparam('z')]
                        )

        s =select([
                    (func.current_date() + literal_column("s.a")).label("dates")
                ]).select_from(generate_series)
        self.assert_compile(
                    s, 
                    "SELECT CURRENT_DATE + s.a AS dates FROM generate_series(:x, :y, :z) as s(a)", 
                    checkparams={'y': None, 'x': None, 'z': None}
                )

        self.assert_compile(
                    s.params(x=5, y=6, z=7), 
                    "SELECT CURRENT_DATE + s.a AS dates FROM generate_series(:x, :y, :z) as s(a)", 
                    checkparams={'y': 6, 'x': 5, 'z': 7}
                )

    @testing.emits_warning('.*empty sequence.*')
    def test_render_binds_as_literal(self):
        """test a compiler that renders binds inline into 
        SQL in the columns clause."""

        dialect = default.DefaultDialect()
        class Compiler(dialect.statement_compiler):
            ansi_bind_rules = True
        dialect.statement_compiler = Compiler

        self.assert_compile(
            select([literal("someliteral")]),
            "SELECT 'someliteral' AS anon_1",
            dialect=dialect
        )

        self.assert_compile(
            select([table1.c.myid + 3]),
            "SELECT mytable.myid + 3 AS anon_1 FROM mytable",
            dialect=dialect
        )

        self.assert_compile(
            select([table1.c.myid.in_([4, 5, 6])]),
            "SELECT mytable.myid IN (4, 5, 6) AS anon_1 FROM mytable",
            dialect=dialect
        )

        self.assert_compile(
            select([func.mod(table1.c.myid, 5)]),
            "SELECT mod(mytable.myid, 5) AS mod_1 FROM mytable",
            dialect=dialect
        )

        self.assert_compile(
            select([literal("foo").in_([])]),
            "SELECT 'foo' != 'foo' AS anon_1",
            dialect=dialect
        )

        assert_raises(
            exc.CompileError,
            bindparam("foo").in_([]).compile, dialect=dialect
        )


    def test_literal(self):

        self.assert_compile(select([literal('foo')]), "SELECT :param_1 AS anon_1")

        self.assert_compile(select([literal("foo") + literal("bar")], from_obj=[table1]),
            "SELECT :param_1 || :param_2 AS anon_1 FROM mytable")

    def test_calculated_columns(self):
         value_tbl = table('values',
             column('id', Integer),
             column('val1', Float),
             column('val2', Float),
         )

         self.assert_compile(
             select([value_tbl.c.id, (value_tbl.c.val2 -
     value_tbl.c.val1)/value_tbl.c.val1]),
             "SELECT values.id, (values.val2 - values.val1) / values.val1 AS anon_1 FROM values"
         )

         self.assert_compile(
             select([value_tbl.c.id], (value_tbl.c.val2 -
     value_tbl.c.val1)/value_tbl.c.val1 > 2.0),
             "SELECT values.id FROM values WHERE (values.val2 - values.val1) / values.val1 > :param_1"
         )

         self.assert_compile(
             select([value_tbl.c.id], value_tbl.c.val1 / (value_tbl.c.val2 - value_tbl.c.val1) /value_tbl.c.val1 > 2.0),
             "SELECT values.id FROM values WHERE (values.val1 / (values.val2 - values.val1)) / values.val1 > :param_1"
         )

    def test_collate(self):
        for expr in (select([table1.c.name.collate('latin1_german2_ci')]),
                     select([collate(table1.c.name, 'latin1_german2_ci')])):
            self.assert_compile(
                expr, "SELECT mytable.name COLLATE latin1_german2_ci AS anon_1 FROM mytable")

        assert table1.c.name.collate('latin1_german2_ci').type is table1.c.name.type

        expr = select([table1.c.name.collate('latin1_german2_ci').label('k1')]).order_by('k1')
        self.assert_compile(expr,"SELECT mytable.name COLLATE latin1_german2_ci AS k1 FROM mytable ORDER BY k1")

        expr = select([collate('foo', 'latin1_german2_ci').label('k1')])
        self.assert_compile(expr,"SELECT :param_1 COLLATE latin1_german2_ci AS k1")

        expr = select([table1.c.name.collate('latin1_german2_ci').like('%x%')])
        self.assert_compile(expr,
                            "SELECT mytable.name COLLATE latin1_german2_ci "
                            "LIKE :param_1 AS anon_1 FROM mytable")

        expr = select([table1.c.name.like(collate('%x%', 'latin1_german2_ci'))])
        self.assert_compile(expr,
                            "SELECT mytable.name "
                            "LIKE :param_1 COLLATE latin1_german2_ci AS anon_1 "
                            "FROM mytable")

        expr = select([table1.c.name.collate('col1').like(
            collate('%x%', 'col2'))])
        self.assert_compile(expr,
                            "SELECT mytable.name COLLATE col1 "
                            "LIKE :param_1 COLLATE col2 AS anon_1 "
                            "FROM mytable")

        expr = select([func.concat('a', 'b').collate('latin1_german2_ci').label('x')])
        self.assert_compile(expr,
                            "SELECT concat(:param_1, :param_2) "
                            "COLLATE latin1_german2_ci AS x")


        expr = select([table1.c.name]).\
                        order_by(table1.c.name.collate('latin1_german2_ci'))
        self.assert_compile(expr, 
                            "SELECT mytable.name FROM mytable ORDER BY "
                            "mytable.name COLLATE latin1_german2_ci")

    def test_percent_chars(self):
        t = table("table%name",
            column("percent%"),
            column("%(oneofthese)s"),
            column("spaces % more spaces"),
        )
        self.assert_compile(
            t.select(use_labels=True),
            '''SELECT "table%name"."percent%" AS "table%name_percent%", '''\
            '''"table%name"."%(oneofthese)s" AS "table%name_%(oneofthese)s", '''\
            '''"table%name"."spaces % more spaces" AS "table%name_spaces % '''\
            '''more spaces" FROM "table%name"'''
        )


    def test_joins(self):
        self.assert_compile(
            join(table2, table1, table1.c.myid == table2.c.otherid).select(),
            "SELECT myothertable.otherid, myothertable.othername, "
            "mytable.myid, mytable.name, mytable.description FROM "
            "myothertable JOIN mytable ON mytable.myid = myothertable.otherid"
        )

        self.assert_compile(
            select(
             [table1],
                from_obj = [join(table1, table2, table1.c.myid == table2.c.otherid)]
            ),
        "SELECT mytable.myid, mytable.name, mytable.description FROM "
        "mytable JOIN myothertable ON mytable.myid = myothertable.otherid")

        self.assert_compile(
            select(
                [join(join(table1, table2, table1.c.myid == table2.c.otherid), 
                table3, table1.c.myid == table3.c.userid)]
            ),
            "SELECT mytable.myid, mytable.name, mytable.description, "
            "myothertable.otherid, myothertable.othername, thirdtable.userid, "
            "thirdtable.otherstuff FROM mytable JOIN myothertable ON mytable.myid ="
            " myothertable.otherid JOIN thirdtable ON mytable.myid = thirdtable.userid"
        )

        self.assert_compile(
            join(users, addresses, users.c.user_id==addresses.c.user_id).select(),
            "SELECT users.user_id, users.user_name, users.password, "
            "addresses.address_id, addresses.user_id, addresses.street, "
            "addresses.city, addresses.state, addresses.zip FROM users JOIN addresses "
            "ON users.user_id = addresses.user_id"
        )

        self.assert_compile(
                select([table1, table2, table3],

                from_obj = [join(table1, table2, table1.c.myid == table2.c.otherid).
                                    outerjoin(table3, table1.c.myid==table3.c.userid)]
                )
                ,"SELECT mytable.myid, mytable.name, mytable.description, "
                "myothertable.otherid, myothertable.othername, thirdtable.userid,"
                " thirdtable.otherstuff FROM mytable JOIN myothertable ON mytable.myid "
                "= myothertable.otherid LEFT OUTER JOIN thirdtable ON mytable.myid ="
                " thirdtable.userid"
            )
        self.assert_compile(
                select([table1, table2, table3],
                from_obj = [outerjoin(table1, 
                                join(table2, table3, table2.c.otherid == table3.c.userid),
                                table1.c.myid==table2.c.otherid)]
                )
                ,"SELECT mytable.myid, mytable.name, mytable.description, "
                "myothertable.otherid, myothertable.othername, thirdtable.userid,"
                " thirdtable.otherstuff FROM mytable LEFT OUTER JOIN (myothertable "
                "JOIN thirdtable ON myothertable.otherid = thirdtable.userid) ON "
                "mytable.myid = myothertable.otherid"
            )

        query = select(
                [table1, table2],
                or_(
                    table1.c.name == 'fred',
                    table1.c.myid == 10,
                    table2.c.othername != 'jack',
                    "EXISTS (select yay from foo where boo = lar)"
                ),
                from_obj = [ outerjoin(table1, table2, table1.c.myid == table2.c.otherid) ]
                )
        self.assert_compile(query,
            "SELECT mytable.myid, mytable.name, mytable.description, "
            "myothertable.otherid, myothertable.othername "
            "FROM mytable LEFT OUTER JOIN myothertable ON mytable.myid = "
            "myothertable.otherid WHERE mytable.name = :name_1 OR "
            "mytable.myid = :myid_1 OR myothertable.othername != :othername_1 "
            "OR EXISTS (select yay from foo where boo = lar)",
            )

    def test_compound_selects(self):
        assert_raises_message(
            exc.ArgumentError,
            "All selectables passed to CompoundSelect "
            "must have identical numbers of columns; "
            "select #1 has 2 columns, select #2 has 3",
            union, table3.select(), table1.select()
        )

        x = union(
              select([table1], table1.c.myid == 5),
              select([table1], table1.c.myid == 12),
              order_by = [table1.c.myid],
        )

        self.assert_compile(x, "SELECT mytable.myid, mytable.name, mytable.description "\
                                "FROM mytable WHERE mytable.myid = :myid_1 UNION "\
                                "SELECT mytable.myid, mytable.name, mytable.description "\
                                "FROM mytable WHERE mytable.myid = :myid_2 ORDER BY mytable.myid")

        x = union(
              select([table1]),
              select([table1])
        )
        x = union(x, select([table1]))
        self.assert_compile(x, "(SELECT mytable.myid, mytable.name, mytable.description "
                                "FROM mytable UNION SELECT mytable.myid, mytable.name, " 
                                "mytable.description FROM mytable) UNION SELECT mytable.myid,"
                                " mytable.name, mytable.description FROM mytable")

        u1 = union(
            select([table1.c.myid, table1.c.name]),
            select([table2]),
            select([table3])
        )
        self.assert_compile(u1, "SELECT mytable.myid, mytable.name "
                                "FROM mytable UNION SELECT myothertable.otherid, "
                                "myothertable.othername FROM myothertable "
                                "UNION SELECT thirdtable.userid, thirdtable.otherstuff "
                                "FROM thirdtable")

        assert u1.corresponding_column(table2.c.otherid) is u1.c.myid

        self.assert_compile(
            union(
                select([table1.c.myid, table1.c.name]),
                select([table2]),
                order_by=['myid'],
                offset=10,
                limit=5
            ),
            "SELECT mytable.myid, mytable.name "
            "FROM mytable UNION SELECT myothertable.otherid, myothertable.othername "
            "FROM myothertable ORDER BY myid LIMIT :param_1 OFFSET :param_2",
            {'param_1':5, 'param_2':10}
        )

        self.assert_compile(
            union(
                select([table1.c.myid, table1.c.name, func.max(table1.c.description)],
                            table1.c.name=='name2', 
                            group_by=[table1.c.myid, table1.c.name]),
                table1.select(table1.c.name=='name1')
            ),
            "SELECT mytable.myid, mytable.name, max(mytable.description) AS max_1 "
            "FROM mytable WHERE mytable.name = :name_1 GROUP BY mytable.myid, "
            "mytable.name UNION SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.name = :name_2"
        )

        self.assert_compile(
            union(
                select([literal(100).label('value')]),
                select([literal(200).label('value')])
                ),
                "SELECT :param_1 AS value UNION SELECT :param_2 AS value"
        )

        self.assert_compile(
            union_all(
                select([table1.c.myid]),
                union(
                    select([table2.c.otherid]),
                    select([table3.c.userid]),
                )
            )
            ,
            "SELECT mytable.myid FROM mytable UNION ALL "
            "(SELECT myothertable.otherid FROM myothertable UNION "
            "SELECT thirdtable.userid FROM thirdtable)"
        )


        s = select([column('foo'), column('bar')])

        # ORDER BY's even though not supported by all DB's, are rendered if requested
        self.assert_compile(union(s.order_by("foo"), s.order_by("bar")), 
            "SELECT foo, bar ORDER BY foo UNION SELECT foo, bar ORDER BY bar"
        )
        # self_group() is honored
        self.assert_compile(
            union(s.order_by("foo").self_group(), s.order_by("bar").limit(10).self_group()), 
            "(SELECT foo, bar ORDER BY foo) UNION (SELECT foo, bar ORDER BY bar LIMIT :param_1)",
            {'param_1':10}

        )

    def test_compound_grouping(self):
        s = select([column('foo'), column('bar')]).select_from('bat')

        self.assert_compile(
            union(union(union(s, s), s), s),
            "((SELECT foo, bar FROM bat UNION SELECT foo, bar FROM bat) "
            "UNION SELECT foo, bar FROM bat) UNION SELECT foo, bar FROM bat"
        )

        self.assert_compile(
            union(s, s, s, s),
            "SELECT foo, bar FROM bat UNION SELECT foo, bar "
            "FROM bat UNION SELECT foo, bar FROM bat UNION SELECT foo, bar FROM bat"
        )

        self.assert_compile(
            union(s, union(s, union(s, s))),
            "SELECT foo, bar FROM bat UNION (SELECT foo, bar FROM bat "
            "UNION (SELECT foo, bar FROM bat UNION SELECT foo, bar FROM bat))"
        )

        self.assert_compile(
            select([s.alias()]),
            'SELECT anon_1.foo, anon_1.bar FROM (SELECT foo, bar FROM bat) AS anon_1'
        )

        self.assert_compile(
            select([union(s, s).alias()]),
            'SELECT anon_1.foo, anon_1.bar FROM '
            '(SELECT foo, bar FROM bat UNION SELECT foo, bar FROM bat) AS anon_1'
        )

        self.assert_compile(
            select([except_(s, s).alias()]),
            'SELECT anon_1.foo, anon_1.bar FROM '
            '(SELECT foo, bar FROM bat EXCEPT SELECT foo, bar FROM bat) AS anon_1'
        )

        # this query sqlite specifically chokes on
        self.assert_compile(
            union(
                except_(s, s),
                s
            ),
            "(SELECT foo, bar FROM bat EXCEPT SELECT foo, bar FROM bat) "
            "UNION SELECT foo, bar FROM bat"
        )

        self.assert_compile(
            union(
                s,
                except_(s, s),
            ),
            "SELECT foo, bar FROM bat "
            "UNION (SELECT foo, bar FROM bat EXCEPT SELECT foo, bar FROM bat)"
        )

        # this solves it
        self.assert_compile(
            union(
                except_(s, s).alias().select(),
                s
            ),
            "SELECT anon_1.foo, anon_1.bar FROM "
            "(SELECT foo, bar FROM bat EXCEPT SELECT foo, bar FROM bat) AS anon_1 "
            "UNION SELECT foo, bar FROM bat"
        )

        self.assert_compile(
            except_(
                union(s, s),
                union(s, s)
            ),
            "(SELECT foo, bar FROM bat UNION SELECT foo, bar FROM bat) "
            "EXCEPT (SELECT foo, bar FROM bat UNION SELECT foo, bar FROM bat)"
        )
        s2 = union(s, s)
        s3 = union(s2, s2)
        self.assert_compile(s3, "(SELECT foo, bar FROM bat "
                                "UNION SELECT foo, bar FROM bat) "
                                "UNION (SELECT foo, bar FROM bat "
                                "UNION SELECT foo, bar FROM bat)")


        self.assert_compile(
            union(
                intersect(s, s),
                intersect(s, s)
            ),
            "(SELECT foo, bar FROM bat INTERSECT SELECT foo, bar FROM bat) "
            "UNION (SELECT foo, bar FROM bat INTERSECT SELECT foo, bar FROM bat)"
        )

    @testing.uses_deprecated()
    def test_binds(self):
        for (
             stmt,
             expected_named_stmt,
             expected_positional_stmt,
             expected_default_params_dict,
             expected_default_params_list,
             test_param_dict,
             expected_test_params_dict,
             expected_test_params_list
             ) in [
              (
                  select(
                      [table1, table2],
                     and_(
                         table1.c.myid == table2.c.otherid,
                         table1.c.name == bindparam('mytablename')
                     )),
                     "SELECT mytable.myid, mytable.name, mytable.description, "
                     "myothertable.otherid, myothertable.othername FROM mytable, "
                     "myothertable WHERE mytable.myid = myothertable.otherid "
                     "AND mytable.name = :mytablename",
                     "SELECT mytable.myid, mytable.name, mytable.description, "
                     "myothertable.otherid, myothertable.othername FROM mytable, "
                     "myothertable WHERE mytable.myid = myothertable.otherid AND "
                     "mytable.name = ?",
                 {'mytablename':None}, [None],
                 {'mytablename':5}, {'mytablename':5}, [5]
             ),
             (
                 select([table1], or_(table1.c.myid==bindparam('myid'), 
                                        table2.c.otherid==bindparam('myid'))),
                 "SELECT mytable.myid, mytable.name, mytable.description "
                        "FROM mytable, myothertable WHERE mytable.myid = :myid "
                        "OR myothertable.otherid = :myid",
                 "SELECT mytable.myid, mytable.name, mytable.description "
                        "FROM mytable, myothertable WHERE mytable.myid = ? "
                        "OR myothertable.otherid = ?",
                 {'myid':None}, [None, None],
                 {'myid':5}, {'myid':5}, [5,5]
             ),
             (
                 text("SELECT mytable.myid, mytable.name, mytable.description FROM "
                                "mytable, myothertable WHERE mytable.myid = :myid OR "
                                "myothertable.otherid = :myid"),
                 "SELECT mytable.myid, mytable.name, mytable.description FROM "
                                "mytable, myothertable WHERE mytable.myid = :myid OR "
                                "myothertable.otherid = :myid",
                 "SELECT mytable.myid, mytable.name, mytable.description FROM "
                                "mytable, myothertable WHERE mytable.myid = ? OR "
                                "myothertable.otherid = ?",
                 {'myid':None}, [None, None],
                 {'myid':5}, {'myid':5}, [5,5]
             ),
             (
                 select([table1], or_(table1.c.myid==bindparam('myid', unique=True), 
                                    table2.c.otherid==bindparam('myid', unique=True))),
                 "SELECT mytable.myid, mytable.name, mytable.description FROM "
                                "mytable, myothertable WHERE mytable.myid = "
                                ":myid_1 OR myothertable.otherid = :myid_2",
                 "SELECT mytable.myid, mytable.name, mytable.description FROM "
                                "mytable, myothertable WHERE mytable.myid = ? "
                                "OR myothertable.otherid = ?",
                 {'myid_1':None, 'myid_2':None}, [None, None],
                 {'myid_1':5, 'myid_2': 6}, {'myid_1':5, 'myid_2':6}, [5,6]
             ),
             (
                bindparam('test', type_=String) + text("'hi'"),
                ":test || 'hi'",
                "? || 'hi'",
                {'test':None}, [None],
                {}, {'test':None}, [None]
             ),
             (
                 select([table1], or_(table1.c.myid==bindparam('myid'), 
                                    table2.c.otherid==bindparam('myotherid'))).\
                                        params({'myid':8, 'myotherid':7}),
                 "SELECT mytable.myid, mytable.name, mytable.description FROM "
                                    "mytable, myothertable WHERE mytable.myid = "
                                    ":myid OR myothertable.otherid = :myotherid",
                 "SELECT mytable.myid, mytable.name, mytable.description FROM "
                                    "mytable, myothertable WHERE mytable.myid = "
                                    "? OR myothertable.otherid = ?",
                 {'myid':8, 'myotherid':7}, [8, 7],
                 {'myid':5}, {'myid':5, 'myotherid':7}, [5,7]
             ),
             (
                 select([table1], or_(table1.c.myid==bindparam('myid', value=7, unique=True), 
                                    table2.c.otherid==bindparam('myid', value=8, unique=True))),
                 "SELECT mytable.myid, mytable.name, mytable.description FROM "
                                    "mytable, myothertable WHERE mytable.myid = "
                                    ":myid_1 OR myothertable.otherid = :myid_2",
                 "SELECT mytable.myid, mytable.name, mytable.description FROM "
                                    "mytable, myothertable WHERE mytable.myid = "
                                    "? OR myothertable.otherid = ?",
                 {'myid_1':7, 'myid_2':8}, [7,8],
                 {'myid_1':5, 'myid_2':6}, {'myid_1':5, 'myid_2':6}, [5,6]
             ),
             ]:

                self.assert_compile(stmt, expected_named_stmt, params=expected_default_params_dict)
                self.assert_compile(stmt, expected_positional_stmt, dialect=sqlite.dialect())
                nonpositional = stmt.compile()
                positional = stmt.compile(dialect=sqlite.dialect())
                pp = positional.params
                assert [pp[k] for k in positional.positiontup] == expected_default_params_list
                assert nonpositional.construct_params(test_param_dict) == expected_test_params_dict, \
                                    "expected :%s got %s" % (str(expected_test_params_dict), \
                                    str(nonpositional.get_params(**test_param_dict)))
                pp = positional.construct_params(test_param_dict)
                assert [pp[k] for k in positional.positiontup] == expected_test_params_list

        # check that params() doesnt modify original statement
        s = select([table1], or_(table1.c.myid==bindparam('myid'), 
                                    table2.c.otherid==bindparam('myotherid')))
        s2 = s.params({'myid':8, 'myotherid':7})
        s3 = s2.params({'myid':9})
        assert s.compile().params == {'myid':None, 'myotherid':None}
        assert s2.compile().params == {'myid':8, 'myotherid':7}
        assert s3.compile().params == {'myid':9, 'myotherid':7}

        # test using same 'unique' param object twice in one compile
        s = select([table1.c.myid]).where(table1.c.myid==12).as_scalar()
        s2 = select([table1, s], table1.c.myid==s)
        self.assert_compile(s2,
            "SELECT mytable.myid, mytable.name, mytable.description, "
            "(SELECT mytable.myid FROM mytable WHERE mytable.myid = "\
            ":myid_1) AS anon_1 FROM mytable WHERE mytable.myid = "
            "(SELECT mytable.myid FROM mytable WHERE mytable.myid = :myid_1)")
        positional = s2.compile(dialect=sqlite.dialect())

        pp = positional.params
        assert [pp[k] for k in positional.positiontup] == [12, 12]

        # check that conflicts with "unique" params are caught
        s = select([table1], or_(table1.c.myid==7, 
                                        table1.c.myid==bindparam('myid_1')))
        assert_raises_message(exc.CompileError, 
                                "conflicts with unique bind parameter "
                                "of the same name", 
                                str, s)

        s = select([table1], or_(table1.c.myid==7, table1.c.myid==8, 
                                        table1.c.myid==bindparam('myid_1')))
        assert_raises_message(exc.CompileError, 
                                "conflicts with unique bind parameter "
                                "of the same name", 
                                str, s)

    def test_binds_no_hash_collision(self):
        """test that construct_params doesn't corrupt dict due to hash collisions"""

        total_params = 100000

        in_clause = [':in%d' % i for i in range(total_params)]
        params = dict(('in%d' % i, i) for i in range(total_params))
        sql = 'text clause %s' % ', '.join(in_clause)
        t = text(sql)
        eq_(len(t.bindparams), total_params)
        c = t.compile()
        pp = c.construct_params(params)
        eq_(len(set(pp)), total_params, '%s %s' % (len(set(pp)), len(pp)))
        eq_(len(set(pp.values())), total_params)


    def test_bind_as_col(self):
        t = table('foo', column('id'))

        s = select([t, literal('lala').label('hoho')])
        self.assert_compile(s, "SELECT foo.id, :param_1 AS hoho FROM foo")

        assert [str(c) for c in s.c] == ["id", "hoho"]

    def test_bind_callable(self):
        expr = column('x') == bindparam("key", callable_=lambda: 12)
        self.assert_compile(
            expr,
            "x = :key",
            {'x':12}
        )

    def test_bind_params_missing(self):
        assert_raises_message(exc.InvalidRequestError, 
            r"A value is required for bind parameter 'x'",
            select([table1]).where(
                    and_(
                        table1.c.myid==bindparam("x", required=True), 
                        table1.c.name==bindparam("y", required=True)
                    )
                ).compile().construct_params,
            params=dict(y=5)
        )

        assert_raises_message(exc.InvalidRequestError, 
            r"A value is required for bind parameter 'x'",
            select([table1]).where(
                    table1.c.myid==bindparam("x", required=True)
                ).compile().construct_params
        )

        assert_raises_message(exc.InvalidRequestError, 
            r"A value is required for bind parameter 'x', "
                "in parameter group 2",
            select([table1]).where(
                    and_(
                        table1.c.myid==bindparam("x", required=True), 
                        table1.c.name==bindparam("y", required=True)
                    )
                ).compile().construct_params,
            params=dict(y=5),
            _group_number=2
        )

        assert_raises_message(exc.InvalidRequestError, 
            r"A value is required for bind parameter 'x', "
                "in parameter group 2",
            select([table1]).where(
                    table1.c.myid==bindparam("x", required=True)
                ).compile().construct_params,
            _group_number=2
        )



    @testing.emits_warning('.*empty sequence.*')
    def test_in(self):
        self.assert_compile(table1.c.myid.in_(['a']),
        "mytable.myid IN (:myid_1)")

        self.assert_compile(~table1.c.myid.in_(['a']),
        "mytable.myid NOT IN (:myid_1)")

        self.assert_compile(table1.c.myid.in_(['a', 'b']),
        "mytable.myid IN (:myid_1, :myid_2)")

        self.assert_compile(table1.c.myid.in_(iter(['a', 'b'])),
        "mytable.myid IN (:myid_1, :myid_2)")

        self.assert_compile(table1.c.myid.in_([literal('a')]),
        "mytable.myid IN (:param_1)")

        self.assert_compile(table1.c.myid.in_([literal('a'), 'b']),
        "mytable.myid IN (:param_1, :myid_1)")

        self.assert_compile(table1.c.myid.in_([literal('a'), literal('b')]),
        "mytable.myid IN (:param_1, :param_2)")

        self.assert_compile(table1.c.myid.in_(['a', literal('b')]),
        "mytable.myid IN (:myid_1, :param_1)")

        self.assert_compile(table1.c.myid.in_([literal(1) + 'a']),
        "mytable.myid IN (:param_1 + :param_2)")

        self.assert_compile(table1.c.myid.in_([literal('a') +'a', 'b']),
        "mytable.myid IN (:param_1 || :param_2, :myid_1)")

        self.assert_compile(table1.c.myid.in_([literal('a') + literal('a'), literal('b')]),
        "mytable.myid IN (:param_1 || :param_2, :param_3)")

        self.assert_compile(table1.c.myid.in_([1, literal(3) + 4]),
        "mytable.myid IN (:myid_1, :param_1 + :param_2)")

        self.assert_compile(table1.c.myid.in_([literal('a') < 'b']),
        "mytable.myid IN (:param_1 < :param_2)")

        self.assert_compile(table1.c.myid.in_([table1.c.myid]),
        "mytable.myid IN (mytable.myid)")

        self.assert_compile(table1.c.myid.in_(['a', table1.c.myid]),
        "mytable.myid IN (:myid_1, mytable.myid)")

        self.assert_compile(table1.c.myid.in_([literal('a'), table1.c.myid]),
        "mytable.myid IN (:param_1, mytable.myid)")

        self.assert_compile(table1.c.myid.in_([literal('a'), table1.c.myid +'a']),
        "mytable.myid IN (:param_1, mytable.myid + :myid_1)")

        self.assert_compile(table1.c.myid.in_([literal(1), 'a' + table1.c.myid]),
        "mytable.myid IN (:param_1, :myid_1 + mytable.myid)")

        self.assert_compile(table1.c.myid.in_([1, 2, 3]),
        "mytable.myid IN (:myid_1, :myid_2, :myid_3)")

        self.assert_compile(table1.c.myid.in_(select([table2.c.otherid])),
        "mytable.myid IN (SELECT myothertable.otherid FROM myothertable)")

        self.assert_compile(~table1.c.myid.in_(select([table2.c.otherid])),
        "mytable.myid NOT IN (SELECT myothertable.otherid FROM myothertable)")

        # text
        self.assert_compile(
                table1.c.myid.in_(
                        text("SELECT myothertable.otherid FROM myothertable")
                    ),
                    "mytable.myid IN (SELECT myothertable.otherid "
                    "FROM myothertable)"
        )

        # test empty in clause
        self.assert_compile(table1.c.myid.in_([]),
        "mytable.myid != mytable.myid")

        self.assert_compile(
            select([table1.c.myid.in_(select([table2.c.otherid]))]),
            "SELECT mytable.myid IN (SELECT myothertable.otherid FROM myothertable) AS anon_1 FROM mytable"
        )
        self.assert_compile(
            select([table1.c.myid.in_(select([table2.c.otherid]).as_scalar())]),
            "SELECT mytable.myid IN (SELECT myothertable.otherid FROM myothertable) AS anon_1 FROM mytable"
        )

        self.assert_compile(table1.c.myid.in_(
            union(
                  select([table1.c.myid], table1.c.myid == 5),
                  select([table1.c.myid], table1.c.myid == 12),
            )
        ), "mytable.myid IN ("\
        "SELECT mytable.myid FROM mytable WHERE mytable.myid = :myid_1 "\
        "UNION SELECT mytable.myid FROM mytable WHERE mytable.myid = :myid_2)")

        # test that putting a select in an IN clause does not blow away its ORDER BY clause
        self.assert_compile(
            select([table1, table2],
                table2.c.otherid.in_(
                    select([table2.c.otherid], order_by=[table2.c.othername], limit=10, correlate=False)
                ),
                from_obj=[table1.join(table2, table1.c.myid==table2.c.otherid)], order_by=[table1.c.myid]
            ),
            "SELECT mytable.myid, mytable.name, mytable.description, myothertable.otherid, myothertable.othername FROM mytable "\
            "JOIN myothertable ON mytable.myid = myothertable.otherid WHERE myothertable.otherid IN (SELECT myothertable.otherid "\
            "FROM myothertable ORDER BY myothertable.othername LIMIT :param_1) ORDER BY mytable.myid",
            {'param_1':10}
        )

    def test_tuple(self):
        self.assert_compile(tuple_(table1.c.myid, table1.c.name).in_([(1, 'foo'), (5, 'bar')]),
            "(mytable.myid, mytable.name) IN ((:param_1, :param_2), (:param_3, :param_4))"
        )

        self.assert_compile(
            tuple_(table1.c.myid, table1.c.name).in_(
                        [tuple_(table2.c.otherid, table2.c.othername)]
                    ),
            "(mytable.myid, mytable.name) IN ((myothertable.otherid, myothertable.othername))"
        )

        self.assert_compile(
            tuple_(table1.c.myid, table1.c.name).in_(
                        select([table2.c.otherid, table2.c.othername])
                    ),
            "(mytable.myid, mytable.name) IN (SELECT "
            "myothertable.otherid, myothertable.othername FROM myothertable)"
        )


    def test_cast(self):
        tbl = table('casttest',
                    column('id', Integer),
                    column('v1', Float),
                    column('v2', Float),
                    column('ts', TIMESTAMP),
                    )

        def check_results(dialect, expected_results, literal):
            eq_(len(expected_results), 5, 
                            'Incorrect number of expected results')
            eq_(str(cast(tbl.c.v1, Numeric).compile(dialect=dialect)), 
                            'CAST(casttest.v1 AS %s)' % expected_results[0])
            eq_(str(cast(tbl.c.v1, Numeric(12, 9)).compile(dialect=dialect)), 
                            'CAST(casttest.v1 AS %s)' % expected_results[1])
            eq_(str(cast(tbl.c.ts, Date).compile(dialect=dialect)), 
                            'CAST(casttest.ts AS %s)' % expected_results[2])
            eq_(str(cast(1234, Text).compile(dialect=dialect)), 
                            'CAST(%s AS %s)' % (literal, expected_results[3]))
            eq_(str(cast('test', String(20)).compile(dialect=dialect)), 
                            'CAST(%s AS %s)' %(literal, expected_results[4]))
            # fixme: shoving all of this dialect-specific stuff in one test
            # is now officialy completely ridiculous AND non-obviously omits
            # coverage on other dialects.
            sel = select([tbl, cast(tbl.c.v1, Numeric)]).compile(dialect=dialect)
            if isinstance(dialect, type(mysql.dialect())):
                eq_(str(sel), 
                "SELECT casttest.id, casttest.v1, casttest.v2, casttest.ts, "
                "CAST(casttest.v1 AS DECIMAL) AS anon_1 \nFROM casttest")
            else:
                eq_(str(sel), 
                        "SELECT casttest.id, casttest.v1, casttest.v2, "
                        "casttest.ts, CAST(casttest.v1 AS NUMERIC) AS "
                        "anon_1 \nFROM casttest")

        # first test with PostgreSQL engine
        check_results(postgresql.dialect(), ['NUMERIC', 'NUMERIC(12, 9)'
                      , 'DATE', 'TEXT', 'VARCHAR(20)'], '%(param_1)s')

        # then the Oracle engine
        check_results(oracle.dialect(), ['NUMERIC', 'NUMERIC(12, 9)',
                      'DATE', 'CLOB', 'VARCHAR2(20 CHAR)'], ':param_1')

        # then the sqlite engine
        check_results(sqlite.dialect(), ['NUMERIC', 'NUMERIC(12, 9)',
                      'DATE', 'TEXT', 'VARCHAR(20)'], '?')

        # then the MySQL engine
        check_results(mysql.dialect(), ['DECIMAL', 'DECIMAL(12, 9)',
                      'DATE', 'CHAR', 'CHAR(20)'], '%s')

        self.assert_compile(cast(text('NULL'), Integer),
                            'CAST(NULL AS INTEGER)',
                            dialect=sqlite.dialect())
        self.assert_compile(cast(null(), Integer),
                            'CAST(NULL AS INTEGER)',
                            dialect=sqlite.dialect())
        self.assert_compile(cast(literal_column('NULL'), Integer),
                            'CAST(NULL AS INTEGER)',
                            dialect=sqlite.dialect())

    def test_over(self):
        self.assert_compile(
            func.row_number().over(
                order_by=[table1.c.name, table1.c.description]
            ),
            "row_number() OVER (ORDER BY mytable.name, mytable.description)"
        )
        self.assert_compile(
            func.row_number().over(
                partition_by=[table1.c.name, table1.c.description]
            ),
            "row_number() OVER (PARTITION BY mytable.name, "
            "mytable.description)"
        )
        self.assert_compile(
            func.row_number().over(
                partition_by=[table1.c.name],
                order_by=[table1.c.description]
            ),
            "row_number() OVER (PARTITION BY mytable.name "
            "ORDER BY mytable.description)"
        )
        self.assert_compile(
            func.row_number().over(
                partition_by=table1.c.name,
                order_by=table1.c.description
            ),
            "row_number() OVER (PARTITION BY mytable.name "
            "ORDER BY mytable.description)"
        )

        self.assert_compile(
            func.row_number().over(
                partition_by=table1.c.name,
                order_by=[table1.c.name, table1.c.description]
            ),
            "row_number() OVER (PARTITION BY mytable.name "
            "ORDER BY mytable.name, mytable.description)"
        )

        self.assert_compile(
            select([func.row_number().over(
                order_by=table1.c.description
            ).label('foo')]),
            "SELECT row_number() OVER (ORDER BY mytable.description) "
            "AS foo FROM mytable"
        )

        # test from_obj generation.
        # from func:
        self.assert_compile(
            select([
                func.max(table1.c.name).over(
                    partition_by=['foo']
                )
            ]),
            "SELECT max(mytable.name) OVER (PARTITION BY foo) "
            "AS anon_1 FROM mytable"
        )
        # from partition_by
        self.assert_compile(
            select([
                func.row_number().over(
                    partition_by=[table1.c.name]
                )
            ]),
            "SELECT row_number() OVER (PARTITION BY mytable.name) "
            "AS anon_1 FROM mytable"
        )
        # from order_by
        self.assert_compile(
            select([
                func.row_number().over(
                    order_by=table1.c.name
                )
            ]),
            "SELECT row_number() OVER (ORDER BY mytable.name) "
            "AS anon_1 FROM mytable"
        )

        # this tests that _from_objects 
        # concantenates OK
        self.assert_compile(
            select([column("x") + over(func.foo())]),
            "SELECT x + foo() OVER () AS anon_1"
        )


    def test_date_between(self):
        import datetime
        table = Table('dt', metadata,
            Column('date', Date))
        self.assert_compile(
            table.select(table.c.date.between(datetime.date(2006,6,1), 
                                            datetime.date(2006,6,5))),
            "SELECT dt.date FROM dt WHERE dt.date BETWEEN :date_1 AND :date_2", 
            checkparams={'date_1':datetime.date(2006,6,1), 
                            'date_2':datetime.date(2006,6,5)})

        self.assert_compile(
            table.select(sql.between(table.c.date, datetime.date(2006,6,1),
                                        datetime.date(2006,6,5))),
            "SELECT dt.date FROM dt WHERE dt.date BETWEEN :date_1 AND :date_2", 
            checkparams={'date_1':datetime.date(2006,6,1), 
                            'date_2':datetime.date(2006,6,5)})

    def test_operator_precedence(self):
        table = Table('op', metadata,
            Column('field', Integer))
        self.assert_compile(table.select((table.c.field == 5) == None),
            "SELECT op.field FROM op WHERE (op.field = :field_1) IS NULL")
        self.assert_compile(table.select((table.c.field + 5) == table.c.field),
            "SELECT op.field FROM op WHERE op.field + :field_1 = op.field")
        self.assert_compile(table.select((table.c.field + 5) * 6),
            "SELECT op.field FROM op WHERE (op.field + :field_1) * :param_1")
        self.assert_compile(table.select((table.c.field * 5) + 6),
            "SELECT op.field FROM op WHERE op.field * :field_1 + :param_1")
        self.assert_compile(table.select(5 + table.c.field.in_([5,6])),
            "SELECT op.field FROM op WHERE :param_1 + (op.field IN (:field_1, :field_2))")
        self.assert_compile(table.select((5 + table.c.field).in_([5,6])),
            "SELECT op.field FROM op WHERE :field_1 + op.field IN (:param_1, :param_2)")
        self.assert_compile(table.select(not_(and_(table.c.field == 5, table.c.field == 7))),
            "SELECT op.field FROM op WHERE NOT (op.field = :field_1 AND op.field = :field_2)")
        self.assert_compile(table.select(not_(table.c.field == 5)),
            "SELECT op.field FROM op WHERE op.field != :field_1")
        self.assert_compile(table.select(not_(table.c.field.between(5, 6))),
            "SELECT op.field FROM op WHERE NOT (op.field BETWEEN :field_1 AND :field_2)")
        self.assert_compile(table.select(not_(table.c.field) == 5),
            "SELECT op.field FROM op WHERE (NOT op.field) = :param_1")
        self.assert_compile(table.select((table.c.field == table.c.field).between(False, True)),
            "SELECT op.field FROM op WHERE (op.field = op.field) BETWEEN :param_1 AND :param_2")
        self.assert_compile(table.select(between((table.c.field == table.c.field), False, True)),
            "SELECT op.field FROM op WHERE (op.field = op.field) BETWEEN :param_1 AND :param_2")

    def test_associativity(self):
        f = column('f')
        self.assert_compile( f - f, "f - f" )
        self.assert_compile( f - f - f, "(f - f) - f" )

        self.assert_compile( (f - f) - f, "(f - f) - f" )
        self.assert_compile( (f - f).label('foo') - f, "(f - f) - f" )

        self.assert_compile( f - (f - f), "f - (f - f)" )
        self.assert_compile( f - (f - f).label('foo'), "f - (f - f)" )

        # because - less precedent than /
        self.assert_compile( f / (f - f), "f / (f - f)" )
        self.assert_compile( f / (f - f).label('foo'), "f / (f - f)" )

        self.assert_compile( f / f - f, "f / f - f" )
        self.assert_compile( (f / f) - f, "f / f - f" )
        self.assert_compile( (f / f).label('foo') - f, "f / f - f" )

        # because / more precedent than -
        self.assert_compile( f - (f / f), "f - f / f" )
        self.assert_compile( f - (f / f).label('foo'), "f - f / f" )
        self.assert_compile( f - f / f, "f - f / f" )
        self.assert_compile( (f - f) / f, "(f - f) / f" )

        self.assert_compile( ((f - f) / f) - f, "(f - f) / f - f")
        self.assert_compile( (f - f) / (f - f), "(f - f) / (f - f)")

        # higher precedence
        self.assert_compile( (f / f) - (f / f), "f / f - f / f")

        self.assert_compile( (f / f) - (f - f), "f / f - (f - f)")
        self.assert_compile( (f / f) / (f - f), "(f / f) / (f - f)")
        self.assert_compile( f / (f / (f - f)), "f / (f / (f - f))")


    def test_delayed_col_naming(self):
        my_str = Column(String)

        sel1 = select([my_str])

        assert_raises_message(
            exc.InvalidRequestError,
            "Cannot initialize a sub-selectable with this Column",
            lambda: sel1.c
        )

        # calling label or as_scalar doesn't compile
        # anything.
        sel2 = select([func.substr(my_str, 2, 3)]).label('my_substr')

        assert_raises_message(
            exc.CompileError,
            "Cannot compile Column object until it's 'name' is assigned.",
            str, sel2
        )

        sel3 = select([my_str]).as_scalar()
        assert_raises_message(
            exc.CompileError,
            "Cannot compile Column object until it's 'name' is assigned.",
            str, sel3
        )

        my_str.name = 'foo'

        self.assert_compile(
            sel1,
            "SELECT foo",
        )
        self.assert_compile(
            sel2,
            '(SELECT substr(foo, :substr_2, :substr_3) AS substr_1)',
        )

        self.assert_compile(
            sel3,
            "(SELECT foo)"
        )

    def test_naming(self):
        f1 = func.hoho(table1.c.name)
        s1 = select([table1.c.myid, table1.c.myid.label('foobar'),
                    f1,
                    func.lala(table1.c.name).label('gg')])

        eq_(
            s1.c.keys(),
            ['myid', 'foobar', str(f1), 'gg']
        )

        meta = MetaData()
        t1 = Table('mytable', meta, Column('col1', Integer))

        exprs = (
            table1.c.myid==12,
            func.hoho(table1.c.myid),
            cast(table1.c.name, Numeric)
        )
        for col, key, expr, label in (
            (table1.c.name, 'name', 'mytable.name', None),
            (exprs[0], str(exprs[0]), 'mytable.myid = :myid_1', 'anon_1'),
            (exprs[1], str(exprs[1]), 'hoho(mytable.myid)', 'hoho_1'),
            (exprs[2], str(exprs[2]), 'CAST(mytable.name AS NUMERIC)', 'anon_1'),
            (t1.c.col1, 'col1', 'mytable.col1', None),
            (column('some wacky thing'), 'some wacky thing', '"some wacky thing"', '')
        ):
            if getattr(col, 'table', None) is not None:
                t = col.table
            else:
                t = table1

            s1 = select([col], from_obj=t)
            assert s1.c.keys() == [key], s1.c.keys()

            if label:
                self.assert_compile(s1, "SELECT %s AS %s FROM mytable" % (expr, label))
            else:
                self.assert_compile(s1, "SELECT %s FROM mytable" % (expr,))

            s1 = select([s1])
            if label:
                self.assert_compile(s1, 
                            "SELECT %s FROM (SELECT %s AS %s FROM mytable)" % 
                            (label, expr, label))
            elif col.table is not None:
                # sqlite rule labels subquery columns
                self.assert_compile(s1, 
                            "SELECT %s FROM (SELECT %s AS %s FROM mytable)" % 
                            (key,expr, key))
            else:
                self.assert_compile(s1, 
                            "SELECT %s FROM (SELECT %s FROM mytable)" % 
                            (expr,expr))

    def test_hints(self):
        s = select([table1.c.myid]).with_hint(table1, "test hint %(name)s")

        s2 = select([table1.c.myid]).\
            with_hint(table1, "index(%(name)s idx)", 'oracle').\
            with_hint(table1, "WITH HINT INDEX idx", 'sybase')

        a1 = table1.alias()
        s3 = select([a1.c.myid]).with_hint(a1, "index(%(name)s hint)")

        subs4 = select([
            table1, table2
        ]).select_from(table1.join(table2, table1.c.myid==table2.c.otherid)).\
            with_hint(table1, 'hint1')

        s4 = select([table3]).select_from(
                        table3.join(
                                subs4, 
                                subs4.c.othername==table3.c.otherstuff
                            )
                    ).\
                    with_hint(table3, 'hint3')

        subs5 = select([
            table1, table2
        ]).select_from(table1.join(table2, table1.c.myid==table2.c.otherid))
        s5 = select([table3]).select_from(
                        table3.join(
                                subs5, 
                                subs5.c.othername==table3.c.otherstuff
                            )
                    ).\
                    with_hint(table3, 'hint3').\
                    with_hint(table1, 'hint1')

        t1 = table('QuotedName', column('col1'))
        s6 = select([t1.c.col1]).where(t1.c.col1>10).with_hint(t1, '%(name)s idx1')
        a2 = t1.alias('SomeName')
        s7 = select([a2.c.col1]).where(a2.c.col1>10).with_hint(a2, '%(name)s idx1')

        mysql_d, oracle_d, sybase_d = \
                            mysql.dialect(), \
                            oracle.dialect(), \
                            sybase.dialect()

        for stmt, dialect, expected in [
          (s, mysql_d, 
            "SELECT mytable.myid FROM mytable test hint mytable"),
          (s, oracle_d, 
            "SELECT /*+ test hint mytable */ mytable.myid FROM mytable"),
          (s, sybase_d, 
            "SELECT mytable.myid FROM mytable test hint mytable"),
          (s2, mysql_d, 
            "SELECT mytable.myid FROM mytable"),
          (s2, oracle_d, 
            "SELECT /*+ index(mytable idx) */ mytable.myid FROM mytable"),
          (s2, sybase_d, 
            "SELECT mytable.myid FROM mytable WITH HINT INDEX idx"),
          (s3, mysql_d, 
            "SELECT mytable_1.myid FROM mytable AS mytable_1 "
            "index(mytable_1 hint)"),
          (s3, oracle_d, 
            "SELECT /*+ index(mytable_1 hint) */ mytable_1.myid FROM "
            "mytable mytable_1"),
          (s3, sybase_d, 
            "SELECT mytable_1.myid FROM mytable AS mytable_1 "
            "index(mytable_1 hint)"),
          (s4, mysql_d, 
            "SELECT thirdtable.userid, thirdtable.otherstuff FROM thirdtable "
            "hint3 INNER JOIN (SELECT mytable.myid, mytable.name, "
            "mytable.description, myothertable.otherid, "
            "myothertable.othername FROM mytable hint1 INNER "
            "JOIN myothertable ON mytable.myid = myothertable.otherid) "
            "ON othername = thirdtable.otherstuff"),
          (s4, sybase_d, 
            "SELECT thirdtable.userid, thirdtable.otherstuff FROM thirdtable "
            "hint3 JOIN (SELECT mytable.myid, mytable.name, "
            "mytable.description, myothertable.otherid, "
            "myothertable.othername FROM mytable hint1 "
            "JOIN myothertable ON mytable.myid = myothertable.otherid) "
            "ON othername = thirdtable.otherstuff"),
          (s4, oracle_d, 
            "SELECT /*+ hint3 */ thirdtable.userid, thirdtable.otherstuff "
            "FROM thirdtable JOIN (SELECT /*+ hint1 */ mytable.myid,"
            " mytable.name, mytable.description, myothertable.otherid,"
            " myothertable.othername FROM mytable JOIN myothertable ON"
            " mytable.myid = myothertable.otherid) ON othername ="
            " thirdtable.otherstuff"),
# TODO: figure out dictionary ordering solution here
#            (s5, oracle_d, 
#              "SELECT /*+ hint3 */ /*+ hint1 */ thirdtable.userid, "
#              "thirdtable.otherstuff "
#              "FROM thirdtable JOIN (SELECT mytable.myid,"
#              " mytable.name, mytable.description, myothertable.otherid,"
#              " myothertable.othername FROM mytable JOIN myothertable ON"
#              " mytable.myid = myothertable.otherid) ON othername ="
#              " thirdtable.otherstuff"),
             (s6, oracle_d, 
                """SELECT /*+ "QuotedName" idx1 */ "QuotedName".col1 """
                """FROM "QuotedName" WHERE "QuotedName".col1 > :col1_1"""),
             (s7, oracle_d, 
                 """SELECT /*+ SomeName idx1 */ "SomeName".col1 FROM """
                 """"QuotedName" "SomeName" WHERE "SomeName".col1 > :col1_1"""),
        ]:
            self.assert_compile(
                stmt,
                expected,
                dialect=dialect
            )

    def test_literal_as_text_fromstring(self):
        self.assert_compile(
            and_("a", "b"),
            "a AND b"
        )

    def test_literal_as_text_nonstring_raise(self):
        assert_raises(exc.ArgumentError,
            and_, ("a",), ("b",)
        )


class CRUDTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = 'default'

    def test_insert(self):
        # generic insert, will create bind params for all columns
        self.assert_compile(insert(table1), 
                            "INSERT INTO mytable (myid, name, description) "
                            "VALUES (:myid, :name, :description)")

        # insert with user-supplied bind params for specific columns,
        # cols provided literally
        self.assert_compile(
            insert(table1, {
                            table1.c.myid : bindparam('userid'), 
                            table1.c.name : bindparam('username')}),
            "INSERT INTO mytable (myid, name) VALUES (:userid, :username)")

        # insert with user-supplied bind params for specific columns, cols
        # provided as strings
        self.assert_compile(
            insert(table1, dict(myid = 3, name = 'jack')),
            "INSERT INTO mytable (myid, name) VALUES (:myid, :name)"
        )

        # test with a tuple of params instead of named
        self.assert_compile(
            insert(table1, (3, 'jack', 'mydescription')),
            "INSERT INTO mytable (myid, name, description) VALUES (:myid, :name, :description)",
            checkparams = {'myid':3, 'name':'jack', 'description':'mydescription'}
        )

        self.assert_compile(
            insert(table1, values={
                                    table1.c.myid : bindparam('userid')
                                }).values({table1.c.name : bindparam('username')}),
            "INSERT INTO mytable (myid, name) VALUES (:userid, :username)"
        )

        self.assert_compile(
                    insert(table1, values=dict(myid=func.lala())), 
                    "INSERT INTO mytable (myid) VALUES (lala())")

    def test_inline_insert(self):
        metadata = MetaData()
        table = Table('sometable', metadata,
            Column('id', Integer, primary_key=True),
            Column('foo', Integer, default=func.foobar()))
        self.assert_compile(
                    table.insert(values={}, inline=True), 
                    "INSERT INTO sometable (foo) VALUES (foobar())")
        self.assert_compile(
                    table.insert(inline=True), 
                    "INSERT INTO sometable (foo) VALUES (foobar())", params={})

    def test_update(self):
        self.assert_compile(
                update(table1, table1.c.myid == 7), 
                "UPDATE mytable SET name=:name WHERE mytable.myid = :myid_1", 
                params = {table1.c.name:'fred'})
        self.assert_compile(
                table1.update().where(table1.c.myid==7).
                            values({table1.c.myid:5}), 
                "UPDATE mytable SET myid=:myid WHERE mytable.myid = :myid_1", 
                checkparams={'myid':5, 'myid_1':7})
        self.assert_compile(
                update(table1, table1.c.myid == 7), 
                "UPDATE mytable SET name=:name WHERE mytable.myid = :myid_1", 
                params = {'name':'fred'})
        self.assert_compile(
                update(table1, values = {table1.c.name : table1.c.myid}), 
                "UPDATE mytable SET name=mytable.myid")
        self.assert_compile(
                update(table1, 
                        whereclause = table1.c.name == bindparam('crit'), 
                        values = {table1.c.name : 'hi'}), 
                "UPDATE mytable SET name=:name WHERE mytable.name = :crit", 
                params = {'crit' : 'notthere'}, 
                checkparams={'crit':'notthere', 'name':'hi'})
        self.assert_compile(
                update(table1, table1.c.myid == 12, 
                            values = {table1.c.name : table1.c.myid}), 
                "UPDATE mytable SET name=mytable.myid, description="
                ":description WHERE mytable.myid = :myid_1", 
                params = {'description':'test'}, 
                checkparams={'description':'test', 'myid_1':12})
        self.assert_compile(
                update(table1, table1.c.myid == 12, 
                                values = {table1.c.myid : 9}), 
                "UPDATE mytable SET myid=:myid, description=:description "
                "WHERE mytable.myid = :myid_1", 
                params = {'myid_1': 12, 'myid': 9, 'description': 'test'})
        self.assert_compile(
                update(table1, table1.c.myid ==12), 
                "UPDATE mytable SET myid=:myid WHERE mytable.myid = :myid_1", 
                params={'myid':18}, checkparams={'myid':18, 'myid_1':12})
        s = table1.update(table1.c.myid == 12, values = {table1.c.name : 'lala'})
        c = s.compile(column_keys=['id', 'name'])
        self.assert_compile(
                update(table1, table1.c.myid == 12, 
                        values = {table1.c.name : table1.c.myid}
                    ).values({table1.c.name:table1.c.name + 'foo'}), 
                "UPDATE mytable SET name=(mytable.name || :name_1), "
                "description=:description WHERE mytable.myid = :myid_1", 
                params = {'description':'test'})
        eq_(str(s), str(c))

        self.assert_compile(update(table1,
            (table1.c.myid == func.hoho(4)) &
            (table1.c.name == literal('foo') + table1.c.name + literal('lala')),
            values = {
            table1.c.name : table1.c.name + "lala",
            table1.c.myid : func.do_stuff(table1.c.myid, literal('hoho'))
            }), "UPDATE mytable SET myid=do_stuff(mytable.myid, :param_1), "
            "name=(mytable.name || :name_1) "
            "WHERE mytable.myid = hoho(:hoho_1) AND mytable.name = :param_2 || "
            "mytable.name || :param_3")

    def test_correlated_update(self):
        # test against a straight text subquery
        u = update(table1, values = {
                    table1.c.name : 
                    text("(select name from mytable where id=mytable.id)")})
        self.assert_compile(u, 
                    "UPDATE mytable SET name=(select name from mytable "
                    "where id=mytable.id)")

        mt = table1.alias()
        u = update(table1, values = {
                                table1.c.name : 
                                select([mt.c.name], mt.c.myid==table1.c.myid)
                            })
        self.assert_compile(u, 
                    "UPDATE mytable SET name=(SELECT mytable_1.name FROM "
                    "mytable AS mytable_1 WHERE mytable_1.myid = mytable.myid)")

        # test against a regular constructed subquery
        s = select([table2], table2.c.otherid == table1.c.myid)
        u = update(table1, table1.c.name == 'jack', values = {table1.c.name : s})
        self.assert_compile(u, 
                    "UPDATE mytable SET name=(SELECT myothertable.otherid, "
                    "myothertable.othername FROM myothertable WHERE "
                    "myothertable.otherid = mytable.myid) WHERE mytable.name = :name_1")

        # test a non-correlated WHERE clause
        s = select([table2.c.othername], table2.c.otherid == 7)
        u = update(table1, table1.c.name==s)
        self.assert_compile(u, 
                    "UPDATE mytable SET myid=:myid, name=:name, "
                    "description=:description WHERE mytable.name = "
                    "(SELECT myothertable.othername FROM myothertable "
                    "WHERE myothertable.otherid = :otherid_1)")

        # test one that is actually correlated...
        s = select([table2.c.othername], table2.c.otherid == table1.c.myid)
        u = table1.update(table1.c.name==s)
        self.assert_compile(u, 
                "UPDATE mytable SET myid=:myid, name=:name, "
                "description=:description WHERE mytable.name = "
                "(SELECT myothertable.othername FROM myothertable "
                "WHERE myothertable.otherid = mytable.myid)")

        # test correlated FROM implicit in WHERE and SET clauses
        u = table1.update().values(name=table2.c.othername)\
                  .where(table2.c.otherid == table1.c.myid)
        self.assert_compile(u,
                "UPDATE mytable SET name=myothertable.othername "
                "FROM myothertable WHERE myothertable.otherid = mytable.myid")
        u = table1.update().values(name='foo')\
                  .where(table2.c.otherid == table1.c.myid)
        self.assert_compile(u,
                "UPDATE mytable SET name=:name "
                "FROM myothertable WHERE myothertable.otherid = mytable.myid")

    def test_delete(self):
        self.assert_compile(
                        delete(table1, table1.c.myid == 7), 
                        "DELETE FROM mytable WHERE mytable.myid = :myid_1")
        self.assert_compile(
                        table1.delete().where(table1.c.myid == 7), 
                        "DELETE FROM mytable WHERE mytable.myid = :myid_1")
        self.assert_compile(
                        table1.delete().where(table1.c.myid == 7).\
                                        where(table1.c.name=='somename'), 
                        "DELETE FROM mytable WHERE mytable.myid = :myid_1 "
                        "AND mytable.name = :name_1")

    def test_correlated_delete(self):
        # test a non-correlated WHERE clause
        s = select([table2.c.othername], table2.c.otherid == 7)
        u = delete(table1, table1.c.name==s)
        self.assert_compile(u, "DELETE FROM mytable WHERE mytable.name = "\
        "(SELECT myothertable.othername FROM myothertable WHERE myothertable.otherid = :otherid_1)")

        # test one that is actually correlated...
        s = select([table2.c.othername], table2.c.otherid == table1.c.myid)
        u = table1.delete(table1.c.name==s)
        self.assert_compile(u, 
                    "DELETE FROM mytable WHERE mytable.name = (SELECT "
                    "myothertable.othername FROM myothertable WHERE "
                    "myothertable.otherid = mytable.myid)")

    def test_binds_that_match_columns(self):
        """test bind params named after column names 
        replace the normal SET/VALUES generation."""

        t = table('foo', column('x'), column('y'))

        u = t.update().where(t.c.x==bindparam('x'))

        assert_raises(exc.CompileError, u.compile)

        self.assert_compile(u, "UPDATE foo SET  WHERE foo.x = :x", params={})

        assert_raises(exc.CompileError, u.values(x=7).compile)

        self.assert_compile(u.values(y=7), "UPDATE foo SET y=:y WHERE foo.x = :x")

        assert_raises(exc.CompileError, u.values(x=7).compile, column_keys=['x', 'y'])
        assert_raises(exc.CompileError, u.compile, column_keys=['x', 'y'])

        self.assert_compile(u.values(x=3 + bindparam('x')), 
                            "UPDATE foo SET x=(:param_1 + :x) WHERE foo.x = :x")

        self.assert_compile(u.values(x=3 + bindparam('x')), 
                            "UPDATE foo SET x=(:param_1 + :x) WHERE foo.x = :x",
                            params={'x':1})

        self.assert_compile(u.values(x=3 + bindparam('x')), 
                            "UPDATE foo SET x=(:param_1 + :x), y=:y WHERE foo.x = :x",
                            params={'x':1, 'y':2})

        i = t.insert().values(x=3 + bindparam('x'))
        self.assert_compile(i, "INSERT INTO foo (x) VALUES ((:param_1 + :x))")
        self.assert_compile(i, 
                            "INSERT INTO foo (x, y) VALUES ((:param_1 + :x), :y)",
                            params={'x':1, 'y':2})

        i = t.insert().values(x=bindparam('y'))
        self.assert_compile(i, "INSERT INTO foo (x) VALUES (:y)")

        i = t.insert().values(x=bindparam('y'), y=5)
        assert_raises(exc.CompileError, i.compile)

        i = t.insert().values(x=3 + bindparam('y'), y=5)
        assert_raises(exc.CompileError, i.compile)

        i = t.insert().values(x=3 + bindparam('x2'))
        self.assert_compile(i, "INSERT INTO foo (x) VALUES ((:param_1 + :x2))")
        self.assert_compile(i, "INSERT INTO foo (x) VALUES ((:param_1 + :x2))", params={})
        self.assert_compile(i, "INSERT INTO foo (x, y) VALUES ((:param_1 + :x2), :y)",
                                    params={'x':1, 'y':2})
        self.assert_compile(i, "INSERT INTO foo (x, y) VALUES ((:param_1 + :x2), :y)",
                                    params={'x2':1, 'y':2})

    def test_labels_no_collision(self):

        t = table('foo', column('id'), column('foo_id'))

        self.assert_compile(
            t.update().where(t.c.id==5),
            "UPDATE foo SET id=:id, foo_id=:foo_id WHERE foo.id = :id_1"
        )

        self.assert_compile(
            t.update().where(t.c.id==bindparam(key=t.c.id._label)),
            "UPDATE foo SET id=:id, foo_id=:foo_id WHERE foo.id = :foo_id_1"
        )

class DDLTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = 'default'

    def _illegal_type_fixture(self):
        class MyType(types.TypeEngine):
            pass
        @compiles(MyType)
        def compile(element, compiler, **kw):
            raise exc.CompileError("Couldn't compile type")
        return MyType

    def test_reraise_of_column_spec_issue(self):
        MyType = self._illegal_type_fixture()
        t1 = Table('t', MetaData(),
            Column('x', MyType())
        )
        assert_raises_message(
            exc.CompileError,
            r"\(in table 't', column 'x'\): Couldn't compile type",
            schema.CreateTable(t1).compile
        )

    # there's some unicode issue in the assertion
    # regular expression that appears to be resolved
    # in 2.6, not exactly sure what it is
    @testing.requires.python26
    def test_reraise_of_column_spec_issue_unicode(self):
        MyType = self._illegal_type_fixture()
        t1 = Table('t', MetaData(),
            Column(u'méil', MyType())
        )
        assert_raises_message(
            exc.CompileError,
            ur"\(in table 't', column 'méil'\): Couldn't compile type",
            schema.CreateTable(t1).compile
        )


class InlineDefaultTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = 'default'

    def test_insert(self):
        m = MetaData()
        foo =  Table('foo', m,
            Column('id', Integer))

        t = Table('test', m,
            Column('col1', Integer, default=func.foo(1)),
            Column('col2', Integer, default=select([func.coalesce(func.max(foo.c.id))])),
            )

        self.assert_compile(t.insert(inline=True, values={}), 
                        "INSERT INTO test (col1, col2) VALUES (foo(:foo_1), "
                        "(SELECT coalesce(max(foo.id)) AS coalesce_1 FROM "
                        "foo))")

    def test_update(self):
        m = MetaData()
        foo =  Table('foo', m,
            Column('id', Integer))

        t = Table('test', m,
            Column('col1', Integer, onupdate=func.foo(1)),
            Column('col2', Integer, onupdate=select([func.coalesce(func.max(foo.c.id))])),
            Column('col3', String(30))
            )

        self.assert_compile(t.update(inline=True, values={'col3':'foo'}), 
                        "UPDATE test SET col1=foo(:foo_1), col2=(SELECT "
                        "coalesce(max(foo.id)) AS coalesce_1 FROM foo), "
                        "col3=:col3")

class SchemaTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = 'default'

    def test_select(self):
        self.assert_compile(table4.select(), 
                "SELECT remote_owner.remotetable.rem_id, remote_owner.remotetable.datatype_id,"
                " remote_owner.remotetable.value FROM remote_owner.remotetable")

        self.assert_compile(table4.select(and_(table4.c.datatype_id==7, table4.c.value=='hi')),
                "SELECT remote_owner.remotetable.rem_id, remote_owner.remotetable.datatype_id,"
                " remote_owner.remotetable.value FROM remote_owner.remotetable WHERE "
                "remote_owner.remotetable.datatype_id = :datatype_id_1 AND"
                " remote_owner.remotetable.value = :value_1")

        s = table4.select(and_(table4.c.datatype_id==7, table4.c.value=='hi'), use_labels=True)
        self.assert_compile(s, "SELECT remote_owner.remotetable.rem_id AS"
            " remote_owner_remotetable_rem_id, remote_owner.remotetable.datatype_id AS"
            " remote_owner_remotetable_datatype_id, remote_owner.remotetable.value "
            "AS remote_owner_remotetable_value FROM remote_owner.remotetable WHERE "
            "remote_owner.remotetable.datatype_id = :datatype_id_1 AND "
            "remote_owner.remotetable.value = :value_1")

        # multi-part schema name
        self.assert_compile(table5.select(), 
                'SELECT "dbo.remote_owner".remotetable.rem_id, '
                '"dbo.remote_owner".remotetable.datatype_id, "dbo.remote_owner".remotetable.value '
                'FROM "dbo.remote_owner".remotetable'
        )

        # multi-part schema name labels - convert '.' to '_'
        self.assert_compile(table5.select(use_labels=True), 
                'SELECT "dbo.remote_owner".remotetable.rem_id AS'
                ' dbo_remote_owner_remotetable_rem_id, "dbo.remote_owner".remotetable.datatype_id'
                ' AS dbo_remote_owner_remotetable_datatype_id,'
                ' "dbo.remote_owner".remotetable.value AS dbo_remote_owner_remotetable_value FROM'
                ' "dbo.remote_owner".remotetable'
        )

    def test_alias(self):
        a = alias(table4, 'remtable')
        self.assert_compile(a.select(a.c.datatype_id==7), 
                            "SELECT remtable.rem_id, remtable.datatype_id, remtable.value FROM"
                            " remote_owner.remotetable AS remtable "
                            "WHERE remtable.datatype_id = :datatype_id_1")

    def test_update(self):
        self.assert_compile(
                table4.update(table4.c.value=='test', values={table4.c.datatype_id:12}), 
                "UPDATE remote_owner.remotetable SET datatype_id=:datatype_id "
                "WHERE remote_owner.remotetable.value = :value_1")

    def test_insert(self):
        self.assert_compile(table4.insert(values=(2, 5, 'test')), 
                    "INSERT INTO remote_owner.remotetable (rem_id, datatype_id, value) VALUES "
                    "(:rem_id, :datatype_id, :value)")


class CoercionTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = 'default'

    def _fixture(self):
        m = MetaData()
        return Table('foo', m,
            Column('id', Integer))

    def test_null_constant(self):
        t = self._fixture()
        self.assert_compile(_literal_as_text(None), "NULL")

    def test_false_constant(self):
        t = self._fixture()
        self.assert_compile(_literal_as_text(False), "false")

    def test_true_constant(self):
        t = self._fixture()
        self.assert_compile(_literal_as_text(True), "true")

    def test_val_and_false(self):
        t = self._fixture()
        self.assert_compile(and_(t.c.id == 1, False),
                            "foo.id = :id_1 AND false")

    def test_val_and_true_coerced(self):
        t = self._fixture()
        self.assert_compile(and_(t.c.id == 1, True),
                            "foo.id = :id_1 AND true")

    def test_val_is_null_coerced(self):
        t = self._fixture()
        self.assert_compile(and_(t.c.id == None),
                            "foo.id IS NULL")

    def test_val_and_None(self):
        # current convention is None in and_() or
        # other clauselist is ignored.  May want
        # to revise this at some point.
        t = self._fixture()
        self.assert_compile(and_(t.c.id == 1, None),
                            "foo.id = :id_1")

    def test_None_and_val(self):
        # current convention is None in and_() or
        # other clauselist is ignored.  May want
        # to revise this at some point.
        t = self._fixture()
        self.assert_compile(and_(t.c.id == 1, None),
                            "foo.id = :id_1")

    def test_None_and_nothing(self):
        # current convention is None in and_()
        # returns None May want
        # to revise this at some point.
        assert and_(None) is None

    def test_val_and_null(self):
        t = self._fixture()
        self.assert_compile(and_(t.c.id == 1, null()),
                            "foo.id = :id_1 AND NULL")

