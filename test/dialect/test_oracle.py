# coding: utf-8


from sqlalchemy.testing import eq_
from sqlalchemy import *
from sqlalchemy import types as sqltypes, exc, schema
from sqlalchemy.sql import table, column
from sqlalchemy.sql.elements import quoted_name
from sqlalchemy.testing import (fixtures,
                                AssertsExecutionResults,
                                AssertsCompiledSQL)
from sqlalchemy import testing
from sqlalchemy.util import u, b
from sqlalchemy import util
from sqlalchemy.testing import assert_raises, assert_raises_message
from sqlalchemy.testing.engines import testing_engine
from sqlalchemy.dialects.oracle import cx_oracle, base as oracle
from sqlalchemy.engine import default
import decimal
from sqlalchemy.engine import url
from sqlalchemy.testing.schema import Table, Column
import datetime
import os
from sqlalchemy import sql
from sqlalchemy.testing.mock import Mock


class DialectTest(fixtures.TestBase):
    def test_cx_oracle_version_parse(self):
        dialect = cx_oracle.OracleDialect_cx_oracle()

        eq_(
            dialect._parse_cx_oracle_ver("5.2"),
            (5, 2)
        )

        eq_(
            dialect._parse_cx_oracle_ver("5.0.1"),
            (5, 0, 1)
        )

        eq_(
            dialect._parse_cx_oracle_ver("6.0b1"),
            (6, 0)
        )

    def test_twophase_arg(self):

        mock_dbapi = Mock(version="5.0.3")
        dialect = cx_oracle.OracleDialect_cx_oracle(dbapi=mock_dbapi)
        args = dialect.create_connect_args(
            url.make_url("oracle+cx_oracle://a:b@host/db"))

        eq_(args[1]['twophase'], True)

        mock_dbapi = Mock(version="5.0.3")
        dialect = cx_oracle.OracleDialect_cx_oracle(
            dbapi=mock_dbapi, allow_twophase=False)
        args = dialect.create_connect_args(
            url.make_url("oracle+cx_oracle://a:b@host/db"))

        eq_(args[1]['twophase'], False)

        mock_dbapi = Mock(version="6.0b1")
        dialect = cx_oracle.OracleDialect_cx_oracle(dbapi=mock_dbapi)
        args = dialect.create_connect_args(
            url.make_url("oracle+cx_oracle://a:b@host/db"))

        assert 'twophase' not in args[1]


class OutParamTest(fixtures.TestBase, AssertsExecutionResults):
    __only_on__ = 'oracle+cx_oracle'
    __backend__ = True

    @classmethod
    def setup_class(cls):
        testing.db.execute("""
        create or replace procedure foo(x_in IN number, x_out OUT number,
        y_out OUT number, z_out OUT varchar) IS
        retval number;
        begin
            retval := 6;
            x_out := 10;
            y_out := x_in * 15;
            z_out := NULL;
        end;
        """)

    def test_out_params(self):
        result = testing.db.execute(text('begin foo(:x_in, :x_out, :y_out, '
                                         ':z_out); end;',
                                    bindparams=[bindparam('x_in', Float),
                                                outparam('x_out', Integer),
                                                outparam('y_out', Float),
                                                outparam('z_out', String)]),
                                    x_in=5)
        eq_(result.out_parameters,
            {'x_out': 10, 'y_out': 75, 'z_out': None})
        assert isinstance(result.out_parameters['x_out'], int)

    @classmethod
    def teardown_class(cls):
        testing.db.execute("DROP PROCEDURE foo")


class CXOracleArgsTest(fixtures.TestBase):
    __only_on__ = 'oracle+cx_oracle'
    __backend__ = True

    def test_autosetinputsizes(self):
        dialect = cx_oracle.dialect()
        assert dialect.auto_setinputsizes

        dialect = cx_oracle.dialect(auto_setinputsizes=False)
        assert not dialect.auto_setinputsizes

    def test_exclude_inputsizes_none(self):
        dialect = cx_oracle.dialect(exclude_setinputsizes=None)
        eq_(dialect.exclude_setinputsizes, set())

    def test_exclude_inputsizes_custom(self):
        import cx_Oracle
        dialect = cx_oracle.dialect(dbapi=cx_Oracle,
                                    exclude_setinputsizes=('NCLOB',))
        eq_(dialect.exclude_setinputsizes, set([cx_Oracle.NCLOB]))


class QuotedBindRoundTripTest(fixtures.TestBase):

    __only_on__ = 'oracle'
    __backend__ = True

    @testing.provide_metadata
    def test_table_round_trip(self):
        oracle.RESERVED_WORDS.remove('UNION')

        metadata = self.metadata
        table = Table("t1", metadata,
                      Column("option", Integer),
                      Column("plain", Integer, quote=True),
                      # test that quote works for a reserved word
                      # that the dialect isn't aware of when quote
                      # is set
                      Column("union", Integer, quote=True))
        metadata.create_all()

        table.insert().execute(
            {"option": 1, "plain": 1, "union": 1}
        )
        eq_(
            testing.db.execute(table.select()).first(),
            (1, 1, 1)
        )
        table.update().values(option=2, plain=2, union=2).execute()
        eq_(
            testing.db.execute(table.select()).first(),
            (2, 2, 2)
        )

    def test_numeric_bind_round_trip(self):
        eq_(
            testing.db.scalar(
                select([
                    literal_column("2", type_=Integer()) +
                    bindparam("2_1", value=2)])
            ),
            4
        )

    @testing.provide_metadata
    def test_numeric_bind_in_crud(self):
        t = Table(
            "asfd", self.metadata,
            Column("100K", Integer)
        )
        t.create()

        testing.db.execute(t.insert(), {"100K": 10})
        eq_(
            testing.db.scalar(t.select()), 10
        )


class CompileTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = "oracle"  # oracle.dialect()

    def test_true_false(self):
        self.assert_compile(
            sql.false(), "0"
        )
        self.assert_compile(
            sql.true(),
            "1"
        )

    def test_owner(self):
        meta = MetaData()
        parent = Table('parent', meta, Column('id', Integer,
                       primary_key=True), Column('name', String(50)),
                       schema='ed')
        child = Table('child', meta, Column('id', Integer,
                      primary_key=True), Column('parent_id', Integer,
                      ForeignKey('ed.parent.id')), schema='ed')
        self.assert_compile(parent.join(child),
                            'ed.parent JOIN ed.child ON ed.parent.id = '
                            'ed.child.parent_id')

    def test_subquery(self):
        t = table('sometable', column('col1'), column('col2'))
        s = select([t])
        s = select([s.c.col1, s.c.col2])

        self.assert_compile(s, "SELECT col1, col2 FROM (SELECT "
                               "sometable.col1 AS col1, sometable.col2 "
                               "AS col2 FROM sometable)")

    def test_bindparam_quote(self):
        """test that bound parameters take on quoting for reserved words,
        column names quote flag enabled."""
        # note: this is only in cx_oracle at the moment.  not sure
        # what other hypothetical oracle dialects might need

        self.assert_compile(
            bindparam("option"), ':"option"'
        )
        self.assert_compile(
            bindparam("plain"), ':plain'
        )
        t = Table("s", MetaData(), Column('plain', Integer, quote=True))
        self.assert_compile(
            t.insert().values(plain=5),
            'INSERT INTO s ("plain") VALUES (:"plain")'
        )
        self.assert_compile(
            t.update().values(plain=5), 'UPDATE s SET "plain"=:"plain"'
        )

    def test_cte(self):
        part = table(
            'part',
            column('part'),
            column('sub_part'),
            column('quantity')
        )

        included_parts = select([
            part.c.sub_part, part.c.part, part.c.quantity
        ]).where(part.c.part == "p1").\
            cte(name="included_parts", recursive=True).\
            suffix_with(
                "search depth first by part set ord1",
                "cycle part set y_cycle to 1 default 0", dialect='oracle')

        incl_alias = included_parts.alias("pr1")
        parts_alias = part.alias("p")
        included_parts = included_parts.union_all(
            select([
                parts_alias.c.sub_part,
                parts_alias.c.part, parts_alias.c.quantity
            ]).where(parts_alias.c.part == incl_alias.c.sub_part)
        )

        q = select([
            included_parts.c.sub_part,
            func.sum(included_parts.c.quantity).label('total_quantity')]).\
            group_by(included_parts.c.sub_part)

        self.assert_compile(
            q,
            "WITH included_parts(sub_part, part, quantity) AS "
            "(SELECT part.sub_part AS sub_part, part.part AS part, "
            "part.quantity AS quantity FROM part WHERE part.part = :part_1 "
            "UNION ALL SELECT p.sub_part AS sub_part, p.part AS part, "
            "p.quantity AS quantity FROM part p, included_parts pr1 "
            "WHERE p.part = pr1.sub_part) "
            "search depth first by part set ord1 cycle part set "
            "y_cycle to 1 default 0  "
            "SELECT included_parts.sub_part, sum(included_parts.quantity) "
            "AS total_quantity FROM included_parts "
            "GROUP BY included_parts.sub_part"
        )

    def test_limit(self):
        t = table('sometable', column('col1'), column('col2'))
        s = select([t])
        c = s.compile(dialect=oracle.OracleDialect())
        assert t.c.col1 in set(c._create_result_map()['col1'][1])
        s = select([t]).limit(10).offset(20)
        self.assert_compile(s,
                            'SELECT col1, col2 FROM (SELECT col1, '
                            'col2, ROWNUM AS ora_rn FROM (SELECT '
                            'sometable.col1 AS col1, sometable.col2 AS '
                            'col2 FROM sometable) WHERE ROWNUM <= '
                            ':param_1 + :param_2) WHERE ora_rn > :param_2',
                            checkparams={'param_1': 10, 'param_2': 20})

        c = s.compile(dialect=oracle.OracleDialect())
        eq_(len(c._result_columns), 2)
        assert t.c.col1 in set(c._create_result_map()['col1'][1])

        s2 = select([s.c.col1, s.c.col2])
        self.assert_compile(s2,
                            'SELECT col1, col2 FROM (SELECT col1, col2 '
                            'FROM (SELECT col1, col2, ROWNUM AS ora_rn '
                            'FROM (SELECT sometable.col1 AS col1, '
                            'sometable.col2 AS col2 FROM sometable) '
                            'WHERE ROWNUM <= :param_1 + :param_2) '
                            'WHERE ora_rn > :param_2)',
                            checkparams={'param_1': 10, 'param_2': 20})

        self.assert_compile(s2,
                            'SELECT col1, col2 FROM (SELECT col1, col2 '
                            'FROM (SELECT col1, col2, ROWNUM AS ora_rn '
                            'FROM (SELECT sometable.col1 AS col1, '
                            'sometable.col2 AS col2 FROM sometable) '
                            'WHERE ROWNUM <= :param_1 + :param_2) '
                            'WHERE ora_rn > :param_2)')
        c = s2.compile(dialect=oracle.OracleDialect())
        eq_(len(c._result_columns), 2)
        assert s.c.col1 in set(c._create_result_map()['col1'][1])

        s = select([t]).limit(10).offset(20).order_by(t.c.col2)
        self.assert_compile(s,
                            'SELECT col1, col2 FROM (SELECT col1, '
                            'col2, ROWNUM AS ora_rn FROM (SELECT '
                            'sometable.col1 AS col1, sometable.col2 AS '
                            'col2 FROM sometable ORDER BY '
                            'sometable.col2) WHERE ROWNUM <= '
                            ':param_1 + :param_2) WHERE ora_rn > :param_2',
                            checkparams={'param_1': 10, 'param_2': 20}
                            )
        c = s.compile(dialect=oracle.OracleDialect())
        eq_(len(c._result_columns), 2)
        assert t.c.col1 in set(c._create_result_map()['col1'][1])

        s = select([t], for_update=True).limit(10).order_by(t.c.col2)
        self.assert_compile(s,
                            'SELECT col1, col2 FROM (SELECT '
                            'sometable.col1 AS col1, sometable.col2 AS '
                            'col2 FROM sometable ORDER BY '
                            'sometable.col2) WHERE ROWNUM <= :param_1 '
                            'FOR UPDATE')

        s = select([t],
                   for_update=True).limit(10).offset(20).order_by(t.c.col2)
        self.assert_compile(s,
                            'SELECT col1, col2 FROM (SELECT col1, '
                            'col2, ROWNUM AS ora_rn FROM (SELECT '
                            'sometable.col1 AS col1, sometable.col2 AS '
                            'col2 FROM sometable ORDER BY '
                            'sometable.col2) WHERE ROWNUM <= '
                            ':param_1 + :param_2) WHERE ora_rn > :param_2 FOR '
                            'UPDATE')

    def test_for_update(self):
        table1 = table('mytable',
                       column('myid'), column('name'), column('description'))

        self.assert_compile(
            table1.select(table1.c.myid == 7).with_for_update(),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = :myid_1 FOR UPDATE")

        self.assert_compile(
            table1
            .select(table1.c.myid == 7)
            .with_for_update(of=table1.c.myid),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = :myid_1 "
            "FOR UPDATE OF mytable.myid")

        self.assert_compile(
            table1.select(table1.c.myid == 7).with_for_update(nowait=True),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = :myid_1 FOR UPDATE NOWAIT")

        self.assert_compile(
            table1
            .select(table1.c.myid == 7)
            .with_for_update(nowait=True, of=table1.c.myid),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = :myid_1 "
            "FOR UPDATE OF mytable.myid NOWAIT")

        self.assert_compile(
            table1
            .select(table1.c.myid == 7)
            .with_for_update(nowait=True, of=[table1.c.myid, table1.c.name]),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = :myid_1 FOR UPDATE OF "
            "mytable.myid, mytable.name NOWAIT")

        self.assert_compile(
            table1.select(table1.c.myid == 7)
            .with_for_update(skip_locked=True,
                             of=[table1.c.myid, table1.c.name]),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = :myid_1 FOR UPDATE OF "
            "mytable.myid, mytable.name SKIP LOCKED")

        # key_share has no effect
        self.assert_compile(
            table1.select(table1.c.myid == 7).with_for_update(key_share=True),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = :myid_1 FOR UPDATE")

        # read has no effect
        self.assert_compile(
            table1
            .select(table1.c.myid == 7)
            .with_for_update(read=True, key_share=True),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = :myid_1 FOR UPDATE")

        ta = table1.alias()
        self.assert_compile(
            ta
            .select(ta.c.myid == 7)
            .with_for_update(of=[ta.c.myid, ta.c.name]),
            "SELECT mytable_1.myid, mytable_1.name, mytable_1.description "
            "FROM mytable mytable_1 "
            "WHERE mytable_1.myid = :myid_1 FOR UPDATE OF "
            "mytable_1.myid, mytable_1.name"
        )

    def test_for_update_of_w_limit_adaption_col_present(self):
        table1 = table('mytable', column('myid'), column('name'))

        self.assert_compile(
            select([table1.c.myid, table1.c.name]).
            where(table1.c.myid == 7).
            with_for_update(nowait=True, of=table1.c.name).
            limit(10),
            "SELECT myid, name FROM "
            "(SELECT mytable.myid AS myid, mytable.name AS name "
            "FROM mytable WHERE mytable.myid = :myid_1) "
            "WHERE ROWNUM <= :param_1 FOR UPDATE OF name NOWAIT",
        )

    def test_for_update_of_w_limit_adaption_col_unpresent(self):
        table1 = table('mytable', column('myid'), column('name'))

        self.assert_compile(
            select([table1.c.myid]).
            where(table1.c.myid == 7).
            with_for_update(nowait=True, of=table1.c.name).
            limit(10),
            "SELECT myid FROM "
            "(SELECT mytable.myid AS myid, mytable.name AS name "
            "FROM mytable WHERE mytable.myid = :myid_1) "
            "WHERE ROWNUM <= :param_1 FOR UPDATE OF name NOWAIT",
        )

    def test_for_update_of_w_limit_offset_adaption_col_present(self):
        table1 = table('mytable', column('myid'), column('name'))

        self.assert_compile(
            select([table1.c.myid, table1.c.name]).
            where(table1.c.myid == 7).
            with_for_update(nowait=True, of=table1.c.name).
            limit(10).offset(50),
            "SELECT myid, name FROM (SELECT myid, name, ROWNUM AS ora_rn "
            "FROM (SELECT mytable.myid AS myid, mytable.name AS name "
            "FROM mytable WHERE mytable.myid = :myid_1) "
            "WHERE ROWNUM <= :param_1 + :param_2) WHERE ora_rn > :param_2 "
            "FOR UPDATE OF name NOWAIT",
        )

    def test_for_update_of_w_limit_offset_adaption_col_unpresent(self):
        table1 = table('mytable', column('myid'), column('name'))

        self.assert_compile(
            select([table1.c.myid]).
            where(table1.c.myid == 7).
            with_for_update(nowait=True, of=table1.c.name).
            limit(10).offset(50),
            "SELECT myid FROM (SELECT myid, ROWNUM AS ora_rn, name "
            "FROM (SELECT mytable.myid AS myid, mytable.name AS name "
            "FROM mytable WHERE mytable.myid = :myid_1) "
            "WHERE ROWNUM <= :param_1 + :param_2) WHERE ora_rn > :param_2 "
            "FOR UPDATE OF name NOWAIT",
        )

    def test_for_update_of_w_limit_offset_adaption_partial_col_unpresent(self):
        table1 = table('mytable', column('myid'), column('foo'), column('bar'))

        self.assert_compile(
            select([table1.c.myid, table1.c.bar]).
            where(table1.c.myid == 7).
            with_for_update(nowait=True, of=[table1.c.foo, table1.c.bar]).
            limit(10).offset(50),
            "SELECT myid, bar FROM (SELECT myid, bar, ROWNUM AS ora_rn, "
            "foo FROM (SELECT mytable.myid AS myid, mytable.bar AS bar, "
            "mytable.foo AS foo FROM mytable WHERE mytable.myid = :myid_1) "
            "WHERE ROWNUM <= :param_1 + :param_2) WHERE ora_rn > :param_2 "
            "FOR UPDATE OF foo, bar NOWAIT"
        )

    def test_limit_preserves_typing_information(self):
        class MyType(TypeDecorator):
            impl = Integer

        stmt = select([type_coerce(column('x'), MyType).label('foo')]).limit(1)
        dialect = oracle.dialect()
        compiled = stmt.compile(dialect=dialect)
        assert isinstance(compiled._create_result_map()['foo'][-1], MyType)

    def test_use_binds_for_limits_disabled(self):
        t = table('sometable', column('col1'), column('col2'))
        dialect = oracle.OracleDialect(use_binds_for_limits=False)

        self.assert_compile(
            select([t]).limit(10),
            "SELECT col1, col2 FROM (SELECT sometable.col1 AS col1, "
            "sometable.col2 AS col2 FROM sometable) WHERE ROWNUM <= 10",
            dialect=dialect)

        self.assert_compile(
            select([t]).offset(10),
            "SELECT col1, col2 FROM (SELECT col1, col2, ROWNUM AS ora_rn "
            "FROM (SELECT sometable.col1 AS col1, sometable.col2 AS col2 "
            "FROM sometable)) WHERE ora_rn > 10",
            dialect=dialect)

        self.assert_compile(
            select([t]).limit(10).offset(10),
            "SELECT col1, col2 FROM (SELECT col1, col2, ROWNUM AS ora_rn "
            "FROM (SELECT sometable.col1 AS col1, sometable.col2 AS col2 "
            "FROM sometable) WHERE ROWNUM <= 20) WHERE ora_rn > 10",
            dialect=dialect)

    def test_use_binds_for_limits_enabled(self):
        t = table('sometable', column('col1'), column('col2'))
        dialect = oracle.OracleDialect(use_binds_for_limits=True)

        self.assert_compile(
            select([t]).limit(10),
            "SELECT col1, col2 FROM (SELECT sometable.col1 AS col1, "
            "sometable.col2 AS col2 FROM sometable) WHERE ROWNUM "
            "<= :param_1",
            dialect=dialect)

        self.assert_compile(
            select([t]).offset(10),
            "SELECT col1, col2 FROM (SELECT col1, col2, ROWNUM AS ora_rn "
            "FROM (SELECT sometable.col1 AS col1, sometable.col2 AS col2 "
            "FROM sometable)) WHERE ora_rn > :param_1",
            dialect=dialect)

        self.assert_compile(
            select([t]).limit(10).offset(10),
            "SELECT col1, col2 FROM (SELECT col1, col2, ROWNUM AS ora_rn "
            "FROM (SELECT sometable.col1 AS col1, sometable.col2 AS col2 "
            "FROM sometable) WHERE ROWNUM <= :param_1 + :param_2) "
            "WHERE ora_rn > :param_2",
            dialect=dialect,
            checkparams={'param_1': 10, 'param_2': 10})

    def test_long_labels(self):
        dialect = default.DefaultDialect()
        dialect.max_identifier_length = 30

        ora_dialect = oracle.dialect()

        m = MetaData()
        a_table = Table(
            'thirty_characters_table_xxxxxx',
            m,
            Column('id', Integer, primary_key=True)
        )

        other_table = Table(
            'other_thirty_characters_table_',
            m,
            Column('id', Integer, primary_key=True),
            Column('thirty_characters_table_id',
                   Integer,
                   ForeignKey('thirty_characters_table_xxxxxx.id'),
                   primary_key=True))

        anon = a_table.alias()
        self.assert_compile(select([other_table,
                            anon]).
                            select_from(
                                other_table.outerjoin(anon)).apply_labels(),
                            'SELECT other_thirty_characters_table_.id '
                            'AS other_thirty_characters__1, '
                            'other_thirty_characters_table_.thirty_char'
                            'acters_table_id AS other_thirty_characters'
                            '__2, thirty_characters_table__1.id AS '
                            'thirty_characters_table__3 FROM '
                            'other_thirty_characters_table_ LEFT OUTER '
                            'JOIN thirty_characters_table_xxxxxx AS '
                            'thirty_characters_table__1 ON '
                            'thirty_characters_table__1.id = '
                            'other_thirty_characters_table_.thirty_char'
                            'acters_table_id', dialect=dialect)
        self.assert_compile(select([other_table,
                            anon]).select_from(
                                other_table.outerjoin(anon)).apply_labels(),
                            'SELECT other_thirty_characters_table_.id '
                            'AS other_thirty_characters__1, '
                            'other_thirty_characters_table_.thirty_char'
                            'acters_table_id AS other_thirty_characters'
                            '__2, thirty_characters_table__1.id AS '
                            'thirty_characters_table__3 FROM '
                            'other_thirty_characters_table_ LEFT OUTER '
                            'JOIN thirty_characters_table_xxxxxx '
                            'thirty_characters_table__1 ON '
                            'thirty_characters_table__1.id = '
                            'other_thirty_characters_table_.thirty_char'
                            'acters_table_id', dialect=ora_dialect)

    def test_outer_join(self):
        table1 = table('mytable',
                       column('myid', Integer),
                       column('name', String),
                       column('description', String))

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

        query = select([table1, table2],
                       or_(table1.c.name == 'fred',
                           table1.c.myid == 10, table2.c.othername != 'jack',
                           text('EXISTS (select yay from foo where boo = lar)')
                           ),
                       from_obj=[outerjoin(table1,
                                           table2,
                                           table1.c.myid == table2.c.otherid)])
        self.assert_compile(query,
                            'SELECT mytable.myid, mytable.name, '
                            'mytable.description, myothertable.otherid,'
                            ' myothertable.othername FROM mytable, '
                            'myothertable WHERE (mytable.name = '
                            ':name_1 OR mytable.myid = :myid_1 OR '
                            'myothertable.othername != :othername_1 OR '
                            'EXISTS (select yay from foo where boo = '
                            'lar)) AND mytable.myid = '
                            'myothertable.otherid(+)',
                            dialect=oracle.OracleDialect(use_ansi=False))
        query = table1.outerjoin(table2,
                                 table1.c.myid == table2.c.otherid) \
                      .outerjoin(table3, table3.c.userid == table2.c.otherid)
        self.assert_compile(query.select(),
                            'SELECT mytable.myid, mytable.name, '
                            'mytable.description, myothertable.otherid,'
                            ' myothertable.othername, '
                            'thirdtable.userid, thirdtable.otherstuff '
                            'FROM mytable LEFT OUTER JOIN myothertable '
                            'ON mytable.myid = myothertable.otherid '
                            'LEFT OUTER JOIN thirdtable ON '
                            'thirdtable.userid = myothertable.otherid')

        self.assert_compile(query.select(),
                            'SELECT mytable.myid, mytable.name, '
                            'mytable.description, myothertable.otherid,'
                            ' myothertable.othername, '
                            'thirdtable.userid, thirdtable.otherstuff '
                            'FROM mytable, myothertable, thirdtable '
                            'WHERE thirdtable.userid(+) = '
                            'myothertable.otherid AND mytable.myid = '
                            'myothertable.otherid(+)',
                            dialect=oracle.dialect(use_ansi=False))
        query = table1.join(table2,
                            table1.c.myid == table2.c.otherid) \
                      .join(table3, table3.c.userid == table2.c.otherid)
        self.assert_compile(query.select(),
                            'SELECT mytable.myid, mytable.name, '
                            'mytable.description, myothertable.otherid,'
                            ' myothertable.othername, '
                            'thirdtable.userid, thirdtable.otherstuff '
                            'FROM mytable, myothertable, thirdtable '
                            'WHERE thirdtable.userid = '
                            'myothertable.otherid AND mytable.myid = '
                            'myothertable.otherid',
                            dialect=oracle.dialect(use_ansi=False))
        query = table1.join(table2,
                            table1.c.myid == table2.c.otherid) \
                      .outerjoin(table3, table3.c.userid == table2.c.otherid)
        self.assert_compile(query.select().order_by(table1.c.name).
                            limit(10).offset(5),
                            'SELECT myid, name, description, otherid, '
                            'othername, userid, otherstuff FROM '
                            '(SELECT myid, name, description, otherid, '
                            'othername, userid, otherstuff, ROWNUM AS '
                            'ora_rn FROM (SELECT mytable.myid AS myid, '
                            'mytable.name AS name, mytable.description '
                            'AS description, myothertable.otherid AS '
                            'otherid, myothertable.othername AS '
                            'othername, thirdtable.userid AS userid, '
                            'thirdtable.otherstuff AS otherstuff FROM '
                            'mytable, myothertable, thirdtable WHERE '
                            'thirdtable.userid(+) = '
                            'myothertable.otherid AND mytable.myid = '
                            'myothertable.otherid ORDER BY mytable.name) '
                            'WHERE ROWNUM <= :param_1 + :param_2) '
                            'WHERE ora_rn > :param_2',
                            checkparams={'param_1': 10, 'param_2': 5},
                            dialect=oracle.dialect(use_ansi=False))

        subq = select([table1]).select_from(
            table1.outerjoin(table2, table1.c.myid == table2.c.otherid)) \
            .alias()
        q = select([table3]).select_from(
            table3.outerjoin(subq, table3.c.userid == subq.c.myid))

        self.assert_compile(q,
                            'SELECT thirdtable.userid, '
                            'thirdtable.otherstuff FROM thirdtable '
                            'LEFT OUTER JOIN (SELECT mytable.myid AS '
                            'myid, mytable.name AS name, '
                            'mytable.description AS description FROM '
                            'mytable LEFT OUTER JOIN myothertable ON '
                            'mytable.myid = myothertable.otherid) '
                            'anon_1 ON thirdtable.userid = anon_1.myid',
                            dialect=oracle.dialect(use_ansi=True))

        self.assert_compile(q,
                            'SELECT thirdtable.userid, '
                            'thirdtable.otherstuff FROM thirdtable, '
                            '(SELECT mytable.myid AS myid, '
                            'mytable.name AS name, mytable.description '
                            'AS description FROM mytable, myothertable '
                            'WHERE mytable.myid = myothertable.otherid('
                            '+)) anon_1 WHERE thirdtable.userid = '
                            'anon_1.myid(+)',
                            dialect=oracle.dialect(use_ansi=False))

        q = select([table1.c.name]).where(table1.c.name == 'foo')
        self.assert_compile(q,
                            'SELECT mytable.name FROM mytable WHERE '
                            'mytable.name = :name_1',
                            dialect=oracle.dialect(use_ansi=False))
        subq = select([table3.c.otherstuff]) \
            .where(table3.c.otherstuff == table1.c.name).label('bar')
        q = select([table1.c.name, subq])
        self.assert_compile(q,
                            'SELECT mytable.name, (SELECT '
                            'thirdtable.otherstuff FROM thirdtable '
                            'WHERE thirdtable.otherstuff = '
                            'mytable.name) AS bar FROM mytable',
                            dialect=oracle.dialect(use_ansi=False))

    def test_nonansi_nested_right_join(self):
        a = table('a', column('a'))
        b = table('b', column('b'))
        c = table('c', column('c'))

        j = a.join(b.join(c, b.c.b == c.c.c), a.c.a == b.c.b)

        self.assert_compile(
            select([j]),
            "SELECT a.a, b.b, c.c FROM a, b, c "
            "WHERE a.a = b.b AND b.b = c.c",
            dialect=oracle.OracleDialect(use_ansi=False)
        )

        j = a.outerjoin(b.join(c, b.c.b == c.c.c), a.c.a == b.c.b)

        self.assert_compile(
            select([j]),
            "SELECT a.a, b.b, c.c FROM a, b, c "
            "WHERE a.a = b.b(+) AND b.b = c.c",
            dialect=oracle.OracleDialect(use_ansi=False)
        )

        j = a.join(b.outerjoin(c, b.c.b == c.c.c), a.c.a == b.c.b)

        self.assert_compile(
            select([j]),
            "SELECT a.a, b.b, c.c FROM a, b, c "
            "WHERE a.a = b.b AND b.b = c.c(+)",
            dialect=oracle.OracleDialect(use_ansi=False)
        )

    def test_alias_outer_join(self):
        address_types = table('address_types', column('id'),
                              column('name'))
        addresses = table('addresses', column('id'), column('user_id'),
                          column('address_type_id'),
                          column('email_address'))
        at_alias = address_types.alias()
        s = select([at_alias, addresses]) \
            .select_from(
                addresses.outerjoin(
                    at_alias,
                    addresses.c.address_type_id == at_alias.c.id)) \
            .where(addresses.c.user_id == 7) \
            .order_by(addresses.c.id, address_types.c.id)
        self.assert_compile(s,
                            'SELECT address_types_1.id, '
                            'address_types_1.name, addresses.id, '
                            'addresses.user_id, addresses.address_type_'
                            'id, addresses.email_address FROM '
                            'addresses LEFT OUTER JOIN address_types '
                            'address_types_1 ON addresses.address_type_'
                            'id = address_types_1.id WHERE '
                            'addresses.user_id = :user_id_1 ORDER BY '
                            'addresses.id, address_types.id')

    def test_returning_insert(self):
        t1 = table('t1', column('c1'), column('c2'), column('c3'))
        self.assert_compile(
            t1.insert().values(c1=1).returning(t1.c.c2, t1.c.c3),
            "INSERT INTO t1 (c1) VALUES (:c1) RETURNING "
            "t1.c2, t1.c3 INTO :ret_0, :ret_1")

    def test_returning_insert_functional(self):
        t1 = table('t1',
                   column('c1'),
                   column('c2', String()),
                   column('c3', String()))
        fn = func.lower(t1.c.c2, type_=String())
        stmt = t1.insert().values(c1=1).returning(fn, t1.c.c3)
        compiled = stmt.compile(dialect=oracle.dialect())
        eq_(compiled._create_result_map(),
            {'ret_1': ('ret_1', (t1.c.c3, 'c3', 'c3'), t1.c.c3.type),
            'ret_0': ('ret_0', (fn, 'lower', None), fn.type)})
        self.assert_compile(
            stmt,
            "INSERT INTO t1 (c1) VALUES (:c1) RETURNING "
            "lower(t1.c2), t1.c3 INTO :ret_0, :ret_1")

    def test_returning_insert_labeled(self):
        t1 = table('t1', column('c1'), column('c2'), column('c3'))
        self.assert_compile(
            t1.insert().values(c1=1).returning(
                        t1.c.c2.label('c2_l'), t1.c.c3.label('c3_l')),
            "INSERT INTO t1 (c1) VALUES (:c1) RETURNING "
            "t1.c2, t1.c3 INTO :ret_0, :ret_1")

    def test_compound(self):
        t1 = table('t1', column('c1'), column('c2'), column('c3'))
        t2 = table('t2', column('c1'), column('c2'), column('c3'))
        self.assert_compile(union(t1.select(), t2.select()),
                            'SELECT t1.c1, t1.c2, t1.c3 FROM t1 UNION '
                            'SELECT t2.c1, t2.c2, t2.c3 FROM t2')
        self.assert_compile(except_(t1.select(), t2.select()),
                            'SELECT t1.c1, t1.c2, t1.c3 FROM t1 MINUS '
                            'SELECT t2.c1, t2.c2, t2.c3 FROM t2')

    def test_no_paren_fns(self):
        for fn, expected in [
            (func.uid(), "uid"),
            (func.UID(), "UID"),
            (func.sysdate(), "sysdate"),
            (func.row_number(), "row_number()"),
            (func.rank(), "rank()"),
            (func.now(), "CURRENT_TIMESTAMP"),
            (func.current_timestamp(), "CURRENT_TIMESTAMP"),
            (func.user(), "USER"),
        ]:
            self.assert_compile(fn, expected)

    def test_create_index_alt_schema(self):
        m = MetaData()
        t1 = Table('foo', m,
                   Column('x', Integer),
                   schema="alt_schema")
        self.assert_compile(
            schema.CreateIndex(Index("bar", t1.c.x)),
            "CREATE INDEX alt_schema.bar ON alt_schema.foo (x)"
        )

    def test_create_index_expr(self):
        m = MetaData()
        t1 = Table('foo', m,
                   Column('x', Integer))
        self.assert_compile(
            schema.CreateIndex(Index("bar", t1.c.x > 5)),
            "CREATE INDEX bar ON foo (x > 5)"
        )

    def test_table_options(self):
        m = MetaData()

        t = Table(
            'foo', m,
            Column('x', Integer),
            prefixes=["GLOBAL TEMPORARY"],
            oracle_on_commit="PRESERVE ROWS"
        )

        self.assert_compile(
            schema.CreateTable(t),
            "CREATE GLOBAL TEMPORARY TABLE "
            "foo (x INTEGER) ON COMMIT PRESERVE ROWS"
        )

    def test_create_table_compress(self):
        m = MetaData()
        tbl1 = Table('testtbl1', m, Column('data', Integer),
                     oracle_compress=True)
        tbl2 = Table('testtbl2', m, Column('data', Integer),
                     oracle_compress="OLTP")

        self.assert_compile(schema.CreateTable(tbl1),
                            "CREATE TABLE testtbl1 (data INTEGER) COMPRESS")
        self.assert_compile(schema.CreateTable(tbl2),
                            "CREATE TABLE testtbl2 (data INTEGER) "
                            "COMPRESS FOR OLTP")

    def test_create_index_bitmap_compress(self):
        m = MetaData()
        tbl = Table('testtbl', m, Column('data', Integer))
        idx1 = Index('idx1', tbl.c.data, oracle_compress=True)
        idx2 = Index('idx2', tbl.c.data, oracle_compress=1)
        idx3 = Index('idx3', tbl.c.data, oracle_bitmap=True)

        self.assert_compile(schema.CreateIndex(idx1),
                            "CREATE INDEX idx1 ON testtbl (data) COMPRESS")
        self.assert_compile(schema.CreateIndex(idx2),
                            "CREATE INDEX idx2 ON testtbl (data) COMPRESS 1")
        self.assert_compile(schema.CreateIndex(idx3),
                            "CREATE BITMAP INDEX idx3 ON testtbl (data)")


class CompatFlagsTest(fixtures.TestBase, AssertsCompiledSQL):

    def _dialect(self, server_version, **kw):
        def server_version_info(conn):
            return server_version

        dialect = oracle.dialect(
                        dbapi=Mock(version="0.0.0", paramstyle="named"),
                        **kw)
        dialect._get_server_version_info = server_version_info
        dialect._check_unicode_returns = Mock()
        dialect._check_unicode_description = Mock()
        dialect._get_default_schema_name = Mock()
        return dialect

    def test_ora8_flags(self):
        dialect = self._dialect((8, 2, 5))

        # before connect, assume modern DB
        assert dialect._supports_char_length
        assert dialect._supports_nchar
        assert dialect.use_ansi

        dialect.initialize(Mock())
        assert not dialect.implicit_returning
        assert not dialect._supports_char_length
        assert not dialect._supports_nchar
        assert not dialect.use_ansi
        self.assert_compile(String(50), "VARCHAR2(50)", dialect=dialect)
        self.assert_compile(Unicode(50), "VARCHAR2(50)", dialect=dialect)
        self.assert_compile(UnicodeText(), "CLOB", dialect=dialect)

        dialect = self._dialect((8, 2, 5), implicit_returning=True)
        dialect.initialize(testing.db.connect())
        assert dialect.implicit_returning

    def test_default_flags(self):
        """test with no initialization or server version info"""

        dialect = self._dialect(None)

        assert dialect._supports_char_length
        assert dialect._supports_nchar
        assert dialect.use_ansi
        self.assert_compile(String(50), "VARCHAR2(50 CHAR)", dialect=dialect)
        self.assert_compile(Unicode(50), "NVARCHAR2(50)", dialect=dialect)
        self.assert_compile(UnicodeText(), "NCLOB", dialect=dialect)

    def test_ora10_flags(self):
        dialect = self._dialect((10, 2, 5))

        dialect.initialize(Mock())
        assert dialect._supports_char_length
        assert dialect._supports_nchar
        assert dialect.use_ansi
        self.assert_compile(String(50), "VARCHAR2(50 CHAR)", dialect=dialect)
        self.assert_compile(Unicode(50), "NVARCHAR2(50)", dialect=dialect)
        self.assert_compile(UnicodeText(), "NCLOB", dialect=dialect)


class MultiSchemaTest(fixtures.TestBase, AssertsCompiledSQL):
    __only_on__ = 'oracle'
    __backend__ = True

    @classmethod
    def setup_class(cls):
        # currently assuming full DBA privs for the user.
        # don't really know how else to go here unless
        # we connect as the other user.

        for stmt in ("""
create table %(test_schema)s.parent(
    id integer primary key,
    data varchar2(50)
);

create table %(test_schema)s.child(
    id integer primary key,
    data varchar2(50),
    parent_id integer references %(test_schema)s.parent(id)
);

create table local_table(
    id integer primary key,
    data varchar2(50)
);

create synonym %(test_schema)s.ptable for %(test_schema)s.parent;
create synonym %(test_schema)s.ctable for %(test_schema)s.child;

create synonym %(test_schema)s_pt for %(test_schema)s.parent;

create synonym %(test_schema)s.local_table for local_table;

-- can't make a ref from local schema to the
-- remote schema's table without this,
-- *and* cant give yourself a grant !
-- so we give it to public.  ideas welcome.
grant references on %(test_schema)s.parent to public;
grant references on %(test_schema)s.child to public;
""" % {"test_schema": testing.config.test_schema}).split(";"):
            if stmt.strip():
                testing.db.execute(stmt)

    @classmethod
    def teardown_class(cls):
        for stmt in ("""
drop table %(test_schema)s.child;
drop table %(test_schema)s.parent;
drop table local_table;
drop synonym %(test_schema)s.ctable;
drop synonym %(test_schema)s.ptable;
drop synonym %(test_schema)s_pt;
drop synonym %(test_schema)s.local_table;

""" % {"test_schema": testing.config.test_schema}).split(";"):
            if stmt.strip():
                testing.db.execute(stmt)

    @testing.provide_metadata
    def test_create_same_names_explicit_schema(self):
        schema = testing.db.dialect.default_schema_name
        meta = self.metadata
        parent = Table('parent', meta,
                       Column('pid', Integer, primary_key=True),
                       schema=schema)
        child = Table('child', meta,
                      Column('cid', Integer, primary_key=True),
                      Column('pid',
                             Integer,
                             ForeignKey('%s.parent.pid' % schema)),
                      schema=schema)
        meta.create_all()
        parent.insert().execute({'pid': 1})
        child.insert().execute({'cid': 1, 'pid': 1})
        eq_(child.select().execute().fetchall(), [(1, 1)])

    def test_reflect_alt_table_owner_local_synonym(self):
        meta = MetaData(testing.db)
        parent = Table('%s_pt' % testing.config.test_schema,
                       meta,
                       autoload=True,
                       oracle_resolve_synonyms=True)
        self.assert_compile(parent.select(),
                            "SELECT %(test_schema)s_pt.id, "
                            "%(test_schema)s_pt.data FROM %(test_schema)s_pt"
                            % {"test_schema": testing.config.test_schema})
        select([parent]).execute().fetchall()

    def test_reflect_alt_synonym_owner_local_table(self):
        meta = MetaData(testing.db)
        parent = Table(
            'local_table', meta, autoload=True,
            oracle_resolve_synonyms=True, schema=testing.config.test_schema)
        self.assert_compile(
            parent.select(),
            "SELECT %(test_schema)s.local_table.id, "
            "%(test_schema)s.local_table.data "
            "FROM %(test_schema)s.local_table" %
            {"test_schema": testing.config.test_schema}
        )
        select([parent]).execute().fetchall()

    @testing.provide_metadata
    def test_create_same_names_implicit_schema(self):
        meta = self.metadata
        parent = Table('parent',
                       meta,
                       Column('pid', Integer, primary_key=True))
        child = Table('child', meta,
                      Column('cid', Integer, primary_key=True),
                      Column('pid', Integer, ForeignKey('parent.pid')))
        meta.create_all()
        parent.insert().execute({'pid': 1})
        child.insert().execute({'cid': 1, 'pid': 1})
        eq_(child.select().execute().fetchall(), [(1, 1)])

    def test_reflect_alt_owner_explicit(self):
        meta = MetaData(testing.db)
        parent = Table(
            'parent', meta, autoload=True,
            schema=testing.config.test_schema)
        child = Table(
            'child', meta, autoload=True,
            schema=testing.config.test_schema)

        self.assert_compile(
            parent.join(child),
            "%(test_schema)s.parent JOIN %(test_schema)s.child ON "
            "%(test_schema)s.parent.id = %(test_schema)s.child.parent_id" % {
                "test_schema": testing.config.test_schema
            })
        select([parent, child]).\
            select_from(parent.join(child)).\
            execute().fetchall()

    def test_reflect_local_to_remote(self):
        testing.db.execute(
            'CREATE TABLE localtable (id INTEGER '
            'PRIMARY KEY, parent_id INTEGER REFERENCES '
            '%(test_schema)s.parent(id))' % {
                "test_schema": testing.config.test_schema})
        try:
            meta = MetaData(testing.db)
            lcl = Table('localtable', meta, autoload=True)
            parent = meta.tables['%s.parent' % testing.config.test_schema]
            self.assert_compile(parent.join(lcl),
                                '%(test_schema)s.parent JOIN localtable ON '
                                '%(test_schema)s.parent.id = '
                                'localtable.parent_id' % {
                                    "test_schema": testing.config.test_schema}
                                )
            select([parent,
                   lcl]).select_from(parent.join(lcl)).execute().fetchall()
        finally:
            testing.db.execute('DROP TABLE localtable')

    def test_reflect_alt_owner_implicit(self):
        meta = MetaData(testing.db)
        parent = Table(
            'parent', meta, autoload=True,
            schema=testing.config.test_schema)
        child = Table(
            'child', meta, autoload=True,
            schema=testing.config.test_schema)
        self.assert_compile(
            parent.join(child),
            '%(test_schema)s.parent JOIN %(test_schema)s.child '
            'ON %(test_schema)s.parent.id = '
            '%(test_schema)s.child.parent_id' % {
                "test_schema": testing.config.test_schema})
        select([parent,
               child]).select_from(parent.join(child)).execute().fetchall()

    def test_reflect_alt_owner_synonyms(self):
        testing.db.execute('CREATE TABLE localtable (id INTEGER '
                           'PRIMARY KEY, parent_id INTEGER REFERENCES '
                           '%s.ptable(id))' % testing.config.test_schema)
        try:
            meta = MetaData(testing.db)
            lcl = Table('localtable', meta, autoload=True,
                        oracle_resolve_synonyms=True)
            parent = meta.tables['%s.ptable' % testing.config.test_schema]
            self.assert_compile(
                parent.join(lcl),
                '%(test_schema)s.ptable JOIN localtable ON '
                '%(test_schema)s.ptable.id = '
                'localtable.parent_id' % {
                    "test_schema": testing.config.test_schema})
            select([parent,
                   lcl]).select_from(parent.join(lcl)).execute().fetchall()
        finally:
            testing.db.execute('DROP TABLE localtable')

    def test_reflect_remote_synonyms(self):
        meta = MetaData(testing.db)
        parent = Table('ptable', meta, autoload=True,
                       schema=testing.config.test_schema,
                       oracle_resolve_synonyms=True)
        child = Table('ctable', meta, autoload=True,
                      schema=testing.config.test_schema,
                      oracle_resolve_synonyms=True)
        self.assert_compile(
            parent.join(child),
            '%(test_schema)s.ptable JOIN '
            '%(test_schema)s.ctable '
            'ON %(test_schema)s.ptable.id = '
            '%(test_schema)s.ctable.parent_id' % {
                "test_schema": testing.config.test_schema})
        select([parent,
               child]).select_from(parent.join(child)).execute().fetchall()


class ConstraintTest(fixtures.TablesTest):

    __only_on__ = 'oracle'
    __backend__ = True
    run_deletes = None

    @classmethod
    def define_tables(cls, metadata):
        Table('foo', metadata, Column('id', Integer, primary_key=True))

    def test_oracle_has_no_on_update_cascade(self):
        bar = Table('bar', self.metadata,
                    Column('id', Integer, primary_key=True),
                    Column('foo_id',
                           Integer,
                           ForeignKey('foo.id', onupdate='CASCADE')))
        assert_raises(exc.SAWarning, bar.create)

        bat = Table('bat', self.metadata,
                    Column('id', Integer, primary_key=True),
                    Column('foo_id', Integer),
                    ForeignKeyConstraint(['foo_id'], ['foo.id'],
                                         onupdate='CASCADE'))
        assert_raises(exc.SAWarning, bat.create)


class TwoPhaseTest(fixtures.TablesTest):
    """test cx_oracle two phase, which remains in a semi-broken state
    so requires a carefully written test."""

    __only_on__ = 'oracle+cx_oracle'
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table('datatable', metadata,
              Column('id', Integer, primary_key=True),
              Column('data', String(50)))

    def _connection(self):
        conn = testing.db.connect()
        conn.detach()
        return conn

    def _assert_data(self, rows):
        eq_(
            testing.db.scalar("select count(*) from datatable"),
            rows
        )

    def test_twophase_prepare_false(self):
        conn = self._connection()
        for i in range(2):
            trans = conn.begin_twophase()
            conn.execute("select 1 from dual")
            trans.prepare()
            trans.commit()
        conn.close()
        self._assert_data(0)

    def test_twophase_prepare_true(self):
        conn = self._connection()
        for i in range(2):
            trans = conn.begin_twophase()
            conn.execute("insert into datatable (id, data) "
                         "values (%s, 'somedata')" % i)
            trans.prepare()
            trans.commit()
        conn.close()
        self._assert_data(2)

    def test_twophase_rollback(self):
        conn = self._connection()
        trans = conn.begin_twophase()
        conn.execute("insert into datatable (id, data) "
                     "values (%s, 'somedata')" % 1)
        trans.rollback()

        trans = conn.begin_twophase()
        conn.execute("insert into datatable (id, data) "
                     "values (%s, 'somedata')" % 1)
        trans.prepare()
        trans.commit()

        conn.close()
        self._assert_data(1)

    def test_not_prepared(self):
        conn = self._connection()
        trans = conn.begin_twophase()
        conn.execute("insert into datatable (id, data) "
                     "values (%s, 'somedata')" % 1)
        trans.commit()
        conn.close()
        self._assert_data(1)


class DialectTypesTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = oracle.OracleDialect()

    def test_no_clobs_for_string_params(self):
        """test that simple string params get a DBAPI type of
        VARCHAR, not CLOB. This is to prevent setinputsizes
        from setting up cx_oracle.CLOBs on
        string-based bind params [ticket:793]."""

        class FakeDBAPI(object):
            def __getattr__(self, attr):
                return attr

        dialect = oracle.OracleDialect()
        dbapi = FakeDBAPI()

        b = bindparam("foo", "hello world!")
        eq_(
            b.type.dialect_impl(dialect).get_dbapi_type(dbapi),
            'STRING'
        )

        b = bindparam("foo", "hello world!")
        eq_(
            b.type.dialect_impl(dialect).get_dbapi_type(dbapi),
            'STRING'
        )

    def test_long(self):
        self.assert_compile(oracle.LONG(), "LONG")

    def test_type_adapt(self):
        dialect = cx_oracle.dialect()

        for start, test in [
            (Date(), cx_oracle._OracleDate),
            (oracle.OracleRaw(), cx_oracle._OracleRaw),
            (String(), String),
            (VARCHAR(), cx_oracle._OracleString),
            (DATE(), cx_oracle._OracleDate),
            (oracle.DATE(), oracle.DATE),
            (String(50), cx_oracle._OracleString),
            (Unicode(), cx_oracle._OracleNVarChar),
            (Text(), cx_oracle._OracleText),
            (UnicodeText(), cx_oracle._OracleUnicodeText),
            (NCHAR(), cx_oracle._OracleNVarChar),
            (oracle.RAW(50), cx_oracle._OracleRaw),
        ]:
            assert isinstance(start.dialect_impl(dialect), test), \
                    "wanted %r got %r" % (test, start.dialect_impl(dialect))

    def test_raw_compile(self):
        self.assert_compile(oracle.RAW(), "RAW")
        self.assert_compile(oracle.RAW(35), "RAW(35)")

    def test_char_length(self):
        self.assert_compile(VARCHAR(50), "VARCHAR(50 CHAR)")

        oracle8dialect = oracle.dialect()
        oracle8dialect.server_version_info = (8, 0)
        self.assert_compile(VARCHAR(50), "VARCHAR(50)", dialect=oracle8dialect)

        self.assert_compile(NVARCHAR(50), "NVARCHAR2(50)")
        self.assert_compile(CHAR(50), "CHAR(50)")

    def test_varchar_types(self):
        dialect = oracle.dialect()
        for typ, exp in [
            (String(50), "VARCHAR2(50 CHAR)"),
            (Unicode(50), "NVARCHAR2(50)"),
            (NVARCHAR(50), "NVARCHAR2(50)"),
            (VARCHAR(50), "VARCHAR(50 CHAR)"),
            (oracle.NVARCHAR2(50), "NVARCHAR2(50)"),
            (oracle.VARCHAR2(50), "VARCHAR2(50 CHAR)"),
            (String(), "VARCHAR2"),
            (Unicode(), "NVARCHAR2"),
            (NVARCHAR(), "NVARCHAR2"),
            (VARCHAR(), "VARCHAR"),
            (oracle.NVARCHAR2(), "NVARCHAR2"),
            (oracle.VARCHAR2(), "VARCHAR2"),
        ]:
            self.assert_compile(typ, exp, dialect=dialect)

    def test_interval(self):
        for type_, expected in [(oracle.INTERVAL(),
                                'INTERVAL DAY TO SECOND'),
                                (oracle.INTERVAL(day_precision=3),
                                'INTERVAL DAY(3) TO SECOND'),
                                (oracle.INTERVAL(second_precision=5),
                                'INTERVAL DAY TO SECOND(5)'),
                                (oracle.INTERVAL(day_precision=2,
                                 second_precision=5),
                                'INTERVAL DAY(2) TO SECOND(5)')]:
            self.assert_compile(type_, expected)


class TypesTest(fixtures.TestBase):
    __only_on__ = 'oracle'
    __dialect__ = oracle.OracleDialect()
    __backend__ = True

    @testing.fails_on('+zxjdbc', 'zxjdbc lacks the FIXED_CHAR dbapi type')
    def test_fixed_char(self):
        m = MetaData(testing.db)
        t = Table('t1', m,
                  Column('id', Integer, primary_key=True),
                  Column('data', CHAR(30), nullable=False))

        t.create()
        try:
            t.insert().execute(
                dict(id=1, data="value 1"),
                dict(id=2, data="value 2"),
                dict(id=3, data="value 3")
            )

            eq_(
                t.select().where(t.c.data == 'value 2').execute().fetchall(),
                [(2, 'value 2                       ')]
            )

            m2 = MetaData(testing.db)
            t2 = Table('t1', m2, autoload=True)
            assert type(t2.c.data.type) is CHAR
            eq_(
                t2.select().where(t2.c.data == 'value 2').execute().fetchall(),
                [(2, 'value 2                       ')]
            )

        finally:
            t.drop()

    @testing.requires.returning
    @testing.provide_metadata
    def test_int_not_float(self):
        m = self.metadata
        t1 = Table('t1', m, Column('foo', Integer))
        t1.create()
        r = t1.insert().values(foo=5).returning(t1.c.foo).execute()
        x = r.scalar()
        assert x == 5
        assert isinstance(x, int)

        x = t1.select().scalar()
        assert x == 5
        assert isinstance(x, int)

    @testing.provide_metadata
    def test_rowid(self):
        metadata = self.metadata
        t = Table('t1', metadata, Column('x', Integer))
        t.create()
        t.insert().execute(x=5)
        s1 = select([t])
        s2 = select([column('rowid')]).select_from(s1)
        rowid = s2.scalar()

        # the ROWID type is not really needed here,
        # as cx_oracle just treats it as a string,
        # but we want to make sure the ROWID works...
        rowid_col = column('rowid', oracle.ROWID)
        s3 = select([t.c.x, rowid_col]) \
            .where(rowid_col == cast(rowid, oracle.ROWID))
        eq_(s3.select().execute().fetchall(), [(5, rowid)])

    @testing.fails_on('+zxjdbc',
                      'Not yet known how to pass values of the '
                      'INTERVAL type')
    @testing.provide_metadata
    def test_interval(self):
        metadata = self.metadata
        interval_table = Table('intervaltable', metadata, Column('id',
                               Integer, primary_key=True,
                               test_needs_autoincrement=True),
                               Column('day_interval',
                               oracle.INTERVAL(day_precision=3)))
        metadata.create_all()
        interval_table.insert().\
            execute(day_interval=datetime.timedelta(days=35, seconds=5743))
        row = interval_table.select().execute().first()
        eq_(row['day_interval'], datetime.timedelta(days=35,
            seconds=5743))

    @testing.provide_metadata
    def test_numerics(self):
        m = self.metadata
        t1 = Table('t1', m,
                   Column('intcol', Integer),
                   Column('numericcol', Numeric(precision=9, scale=2)),
                   Column('floatcol1', Float()),
                   Column('floatcol2', FLOAT()),
                   Column('doubleprec', oracle.DOUBLE_PRECISION),
                   Column('numbercol1', oracle.NUMBER(9)),
                   Column('numbercol2', oracle.NUMBER(9, 3)),
                   Column('numbercol3', oracle.NUMBER))
        t1.create()
        t1.insert().execute(
            intcol=1,
            numericcol=5.2,
            floatcol1=6.5,
            floatcol2=8.5,
            doubleprec=9.5,
            numbercol1=12,
            numbercol2=14.85,
            numbercol3=15.76
            )

        m2 = MetaData(testing.db)
        t2 = Table('t1', m2, autoload=True)

        for row in (
            t1.select().execute().first(),
            t2.select().execute().first()
        ):
            for i, (val, type_) in enumerate((
                (1, int),
                (decimal.Decimal("5.2"), decimal.Decimal),
                (6.5, float),
                (8.5, float),
                (9.5, float),
                (12, int),
                (decimal.Decimal("14.85"), decimal.Decimal),
                (15.76, float),
            )):
                eq_(row[i], val)
                assert isinstance(row[i], type_), '%r is not %r' \
                    % (row[i], type_)

    def test_numeric_no_decimal_mode(self):
        engine = testing_engine(options=dict(coerce_to_decimal=False))
        value = engine.scalar("SELECT 5.66 FROM DUAL")
        assert isinstance(value, float)

        value = testing.db.scalar("SELECT 5.66 FROM DUAL")
        assert isinstance(value, decimal.Decimal)

    @testing.only_on("oracle+cx_oracle", "cx_oracle-specific feature")
    @testing.fails_if(
                    testing.requires.python3,
                    "cx_oracle always returns unicode on py3k")
    def test_coerce_to_unicode(self):
        engine = testing_engine(options=dict(coerce_to_unicode=True))
        value = engine.scalar("SELECT 'hello' FROM DUAL")
        assert isinstance(value, util.text_type)

        value = testing.db.scalar("SELECT 'hello' FROM DUAL")
        assert isinstance(value, util.binary_type)

    @testing.provide_metadata
    def test_numerics_broken_inspection(self):
        """Numeric scenarios where Oracle type info is 'broken',
        returning us precision, scale of the form (0, 0) or (0, -127).
        We convert to Decimal and let int()/float() processors take over.

        """

        metadata = self.metadata

        # this test requires cx_oracle 5

        foo = Table('foo', metadata,
                    Column('idata', Integer),
                    Column('ndata', Numeric(20, 2)),
                    Column('ndata2', Numeric(20, 2)),
                    Column('nidata', Numeric(5, 0)),
                    Column('fdata', Float()))
        foo.create()

        foo.insert().execute({
                'idata': 5,
                'ndata': decimal.Decimal("45.6"),
                'ndata2': decimal.Decimal("45.0"),
                'nidata': decimal.Decimal('53'),
                'fdata': 45.68392
            })

        stmt = "SELECT idata, ndata, ndata2, nidata, fdata FROM foo"

        row = testing.db.execute(stmt).fetchall()[0]
        eq_(
            [type(x) for x in row],
            [int, decimal.Decimal, decimal.Decimal, int, float]
        )
        eq_(
            row,
            (5, decimal.Decimal('45.6'), decimal.Decimal('45'),
                53, 45.683920000000001)
        )

        # with a nested subquery,
        # both Numeric values that don't have decimal places, regardless
        # of their originating type, come back as ints with no useful
        # typing information beyond "numeric".  So native handler
        # must convert to int.
        # this means our Decimal converters need to run no matter what.
        # totally sucks.

        stmt = """
        SELECT
            (SELECT (SELECT idata FROM foo) FROM DUAL) AS idata,
            (SELECT CAST((SELECT ndata FROM foo) AS NUMERIC(20, 2)) FROM DUAL)
            AS ndata,
            (SELECT CAST((SELECT ndata2 FROM foo) AS NUMERIC(20, 2)) FROM DUAL)
            AS ndata2,
            (SELECT CAST((SELECT nidata FROM foo) AS NUMERIC(5, 0)) FROM DUAL)
            AS nidata,
            (SELECT CAST((SELECT fdata FROM foo) AS FLOAT) FROM DUAL) AS fdata
        FROM dual
        """
        row = testing.db.execute(stmt).fetchall()[0]
        eq_(
            [type(x) for x in row],
            [int, decimal.Decimal, int, int, decimal.Decimal]
        )
        eq_(
            row,
            (5, decimal.Decimal('45.6'), 45, 53, decimal.Decimal('45.68392'))
        )

        row = testing.db.execute(text(stmt,
                                      typemap={
                                          'idata': Integer(),
                                          'ndata': Numeric(20, 2),
                                          'ndata2': Numeric(20, 2),
                                          'nidata': Numeric(5, 0),
                                          'fdata': Float()})).fetchall()[0]
        eq_(
            [type(x) for x in row],
            [int, decimal.Decimal, decimal.Decimal, decimal.Decimal, float]
        )
        eq_(
            row,
            (5, decimal.Decimal('45.6'), decimal.Decimal('45'),
                decimal.Decimal('53'), 45.683920000000001)
        )

        stmt = """
        SELECT
                anon_1.idata AS anon_1_idata,
                anon_1.ndata AS anon_1_ndata,
                anon_1.ndata2 AS anon_1_ndata2,
                anon_1.nidata AS anon_1_nidata,
                anon_1.fdata AS anon_1_fdata
        FROM (SELECT idata, ndata, ndata2, nidata, fdata
        FROM (
            SELECT
                (SELECT (SELECT idata FROM foo) FROM DUAL) AS idata,
                (SELECT CAST((SELECT ndata FROM foo) AS NUMERIC(20, 2))
                FROM DUAL) AS ndata,
                (SELECT CAST((SELECT ndata2 FROM foo) AS NUMERIC(20, 2))
                FROM DUAL) AS ndata2,
                (SELECT CAST((SELECT nidata FROM foo) AS NUMERIC(5, 0))
                FROM DUAL) AS nidata,
                (SELECT CAST((SELECT fdata FROM foo) AS FLOAT) FROM DUAL)
                AS fdata
            FROM dual
        )
        WHERE ROWNUM >= 0) anon_1
        """
        row = testing.db.execute(stmt).fetchall()[0]
        eq_(
            [type(x) for x in row],
            [int, decimal.Decimal, int, int, decimal.Decimal]
        )
        eq_(
            row,
            (5, decimal.Decimal('45.6'), 45, 53, decimal.Decimal('45.68392'))
        )

        row = testing.db.execute(text(stmt,
                                      typemap={
                                          'anon_1_idata': Integer(),
                                          'anon_1_ndata': Numeric(20, 2),
                                          'anon_1_ndata2': Numeric(20, 2),
                                          'anon_1_nidata': Numeric(5, 0),
                                          'anon_1_fdata': Float()
                                      })).fetchall()[0]
        eq_(
            [type(x) for x in row],
            [int, decimal.Decimal, decimal.Decimal, decimal.Decimal, float]
        )
        eq_(
            row,
            (5, decimal.Decimal('45.6'), decimal.Decimal('45'),
                decimal.Decimal('53'), 45.683920000000001)
        )

        row = testing.db.execute(text(
            stmt,
            typemap={
                'anon_1_idata': Integer(),
                'anon_1_ndata': Numeric(20, 2, asdecimal=False),
                'anon_1_ndata2': Numeric(20, 2, asdecimal=False),
                'anon_1_nidata': Numeric(5, 0, asdecimal=False),
                'anon_1_fdata': Float(asdecimal=True)
            })).fetchall()[0]
        eq_(
            [type(x) for x in row],
            [int, float, float, float, decimal.Decimal]
        )
        eq_(
            row,
            (5, 45.6, 45, 53, decimal.Decimal('45.68392'))
        )

    @testing.provide_metadata
    def test_reflect_dates(self):
        metadata = self.metadata
        Table(
            "date_types", metadata,
            Column('d1', sqltypes.DATE),
            Column('d2', oracle.DATE),
            Column('d3', TIMESTAMP),
            Column('d4', TIMESTAMP(timezone=True)),
            Column('d5', oracle.INTERVAL(second_precision=5)),
        )
        metadata.create_all()
        m = MetaData(testing.db)
        t1 = Table(
            "date_types", m,
            autoload=True)
        assert isinstance(t1.c.d1.type, oracle.DATE)
        assert isinstance(t1.c.d1.type, DateTime)
        assert isinstance(t1.c.d2.type, oracle.DATE)
        assert isinstance(t1.c.d2.type, DateTime)
        assert isinstance(t1.c.d3.type, TIMESTAMP)
        assert not t1.c.d3.type.timezone
        assert isinstance(t1.c.d4.type, TIMESTAMP)
        assert t1.c.d4.type.timezone
        assert isinstance(t1.c.d5.type, oracle.INTERVAL)

    def test_reflect_all_types_schema(self):
        types_table = Table('all_types', MetaData(testing.db),
                            Column('owner', String(30), primary_key=True),
                            Column('type_name', String(30), primary_key=True),
                            autoload=True, oracle_resolve_synonyms=True)
        for row in types_table.select().execute().fetchall():
            [row[k] for k in row.keys()]

    @testing.provide_metadata
    def test_raw_roundtrip(self):
        metadata = self.metadata
        raw_table = Table('raw', metadata,
                          Column('id', Integer, primary_key=True),
                          Column('data', oracle.RAW(35)))
        metadata.create_all()
        testing.db.execute(raw_table.insert(), id=1, data=b("ABCDEF"))
        eq_(
            testing.db.execute(raw_table.select()).first(),
            (1, b("ABCDEF"))
        )

    @testing.provide_metadata
    def test_reflect_nvarchar(self):
        metadata = self.metadata
        Table('tnv', metadata, Column('data', sqltypes.NVARCHAR(255)))
        metadata.create_all()
        m2 = MetaData(testing.db)
        t2 = Table('tnv', m2, autoload=True)
        assert isinstance(t2.c.data.type, sqltypes.NVARCHAR)

        if testing.against('oracle+cx_oracle'):
            # nvarchar returns unicode natively.  cx_oracle
            # _OracleNVarChar type should be at play here.
            assert isinstance(
                t2.c.data.type.dialect_impl(testing.db.dialect),
                cx_oracle._OracleNVarChar)

        data = u('m’a réveillé.')
        t2.insert().execute(data=data)
        res = t2.select().execute().first()['data']
        eq_(res, data)
        assert isinstance(res, util.text_type)

    @testing.provide_metadata
    def test_char_length(self):
        metadata = self.metadata
        t1 = Table('t1', metadata,
                   Column("c1", VARCHAR(50)),
                   Column("c2", NVARCHAR(250)),
                   Column("c3", CHAR(200)))
        t1.create()
        m2 = MetaData(testing.db)
        t2 = Table('t1', m2, autoload=True)
        eq_(t2.c.c1.type.length, 50)
        eq_(t2.c.c2.type.length, 250)
        eq_(t2.c.c3.type.length, 200)

    @testing.provide_metadata
    def test_long_type(self):
        metadata = self.metadata

        t = Table('t', metadata, Column('data', oracle.LONG))
        metadata.create_all(testing.db)
        testing.db.execute(t.insert(), data='xyz')
        eq_(
            testing.db.scalar(select([t.c.data])),
            "xyz"
        )

    def test_longstring(self):
        metadata = MetaData(testing.db)
        testing.db.execute("""
        CREATE TABLE Z_TEST
        (
          ID        NUMERIC(22) PRIMARY KEY,
          ADD_USER  VARCHAR2(20)  NOT NULL
        )
        """)
        try:
            t = Table("z_test", metadata, autoload=True)
            t.insert().execute(id=1.0, add_user='foobar')
            assert t.select().execute().fetchall() == [(1, 'foobar')]
        finally:
            testing.db.execute("DROP TABLE Z_TEST")

    @testing.fails_on('+zxjdbc', 'auto_convert_lobs not applicable')
    def test_lobs_without_convert(self):
        engine = testing_engine(options=dict(auto_convert_lobs=False))
        metadata = MetaData()
        t = Table("z_test",
                  metadata,
                  Column('id', Integer, primary_key=True),
                  Column('data', Text),
                  Column('bindata', LargeBinary))
        t.create(engine)
        try:
            engine.execute(t.insert(),
                           id=1,
                           data='this is text',
                           bindata=b('this is binary'))
            row = engine.execute(t.select()).first()
            eq_(row['data'].read(), 'this is text')
            eq_(row['bindata'].read(), b('this is binary'))
        finally:
            t.drop(engine)


class EuroNumericTest(fixtures.TestBase):
    """
    test the numeric output_type_handler when using non-US locale for NLS_LANG.
    """

    __only_on__ = 'oracle+cx_oracle'
    __backend__ = True

    def setup(self):
        self.old_nls_lang = os.environ.get('NLS_LANG', False)
        os.environ['NLS_LANG'] = "GERMAN"
        self.engine = testing_engine()

    def teardown(self):
        if self.old_nls_lang is not False:
            os.environ['NLS_LANG'] = self.old_nls_lang
        else:
            del os.environ['NLS_LANG']
        self.engine.dispose()

    def test_output_type_handler(self):
        for stmt, exp, kw in [
            ("SELECT 0.1 FROM DUAL", decimal.Decimal("0.1"), {}),
            ("SELECT 15 FROM DUAL", 15, {}),
            ("SELECT CAST(15 AS NUMERIC(3, 1)) FROM DUAL",
             decimal.Decimal("15"), {}),
            ("SELECT CAST(0.1 AS NUMERIC(5, 2)) FROM DUAL",
             decimal.Decimal("0.1"), {}),
            ("SELECT :num FROM DUAL", decimal.Decimal("2.5"),
             {'num': decimal.Decimal("2.5")})
        ]:
            test_exp = self.engine.scalar(stmt, **kw)
            eq_(
                test_exp,
                exp
            )
            assert type(test_exp) is type(exp)


class SystemTableTablenamesTest(fixtures.TestBase):
    __only_on__ = 'oracle'
    __backend__ = True

    def setup(self):
        testing.db.execute("create table my_table (id integer)")
        testing.db.execute(
            "create global temporary table my_temp_table (id integer)"
        )
        testing.db.execute(
            "create table foo_table (id integer) tablespace SYSTEM"
        )

    def teardown(self):
        testing.db.execute("drop table my_temp_table")
        testing.db.execute("drop table my_table")
        testing.db.execute("drop table foo_table")

    def test_table_names_no_system(self):
        insp = inspect(testing.db)
        eq_(
            insp.get_table_names(), ["my_table"]
        )

    def test_temp_table_names_no_system(self):
        insp = inspect(testing.db)
        eq_(
            insp.get_temp_table_names(), ["my_temp_table"]
        )

    def test_table_names_w_system(self):
        engine = testing_engine(options={"exclude_tablespaces": ["FOO"]})
        insp = inspect(engine)
        eq_(
            set(insp.get_table_names()).intersection(["my_table",
                                                      "foo_table"]),
            set(["my_table", "foo_table"])
        )


class DontReflectIOTTest(fixtures.TestBase):
    """test that index overflow tables aren't included in
    table_names."""

    __only_on__ = 'oracle'
    __backend__ = True

    def setup(self):
        testing.db.execute("""
        CREATE TABLE admin_docindex(
                token char(20),
                doc_id NUMBER,
                token_frequency NUMBER,
                token_offsets VARCHAR2(2000),
                CONSTRAINT pk_admin_docindex PRIMARY KEY (token, doc_id))
            ORGANIZATION INDEX
            TABLESPACE users
            PCTTHRESHOLD 20
            OVERFLOW TABLESPACE users
        """)

    def teardown(self):
        testing.db.execute("drop table admin_docindex")

    def test_reflect_all(self):
        m = MetaData(testing.db)
        m.reflect()
        eq_(
            set(t.name for t in m.tables.values()),
            set(['admin_docindex'])
        )


class BufferedColumnTest(fixtures.TestBase, AssertsCompiledSQL):
    __only_on__ = 'oracle'
    __backend__ = True

    @classmethod
    def setup_class(cls):
        global binary_table, stream, meta
        meta = MetaData(testing.db)
        binary_table = Table('binary_table', meta,
                             Column('id', Integer, primary_key=True),
                             Column('data', LargeBinary))
        meta.create_all()
        stream = os.path.join(
                        os.path.dirname(__file__), "..",
                        'binary_data_one.dat')
        with open(stream, "rb") as file_:
            stream = file_.read(12000)

        for i in range(1, 11):
            binary_table.insert().execute(id=i, data=stream)

    @classmethod
    def teardown_class(cls):
        meta.drop_all()

    def test_fetch(self):
        result = binary_table.select().order_by(binary_table.c.id).\
                                    execute().fetchall()
        eq_(result, [(i, stream) for i in range(1, 11)])

    @testing.fails_on('+zxjdbc', 'FIXME: zxjdbc should support this')
    def test_fetch_single_arraysize(self):
        eng = testing_engine(options={'arraysize': 1})
        result = eng.execute(binary_table.select().
                             order_by(binary_table.c.id)).fetchall()
        eq_(result, [(i, stream) for i in range(1, 11)])


class UnsupportedIndexReflectTest(fixtures.TestBase):
    __only_on__ = 'oracle'
    __backend__ = True

    @testing.emits_warning("No column names")
    @testing.provide_metadata
    def test_reflect_functional_index(self):
        metadata = self.metadata
        Table('test_index_reflect', metadata,
              Column('data', String(20), primary_key=True))
        metadata.create_all()

        testing.db.execute('CREATE INDEX DATA_IDX ON '
                           'TEST_INDEX_REFLECT (UPPER(DATA))')
        m2 = MetaData(testing.db)
        Table('test_index_reflect', m2, autoload=True)


def all_tables_compression_missing():
    try:
        testing.db.execute('SELECT compression FROM all_tables')
        if "Enterprise Edition" not in testing.db.scalar(
                "select * from v$version"):
            return True
        return False
    except Exception:
        return True


def all_tables_compress_for_missing():
    try:
        testing.db.execute('SELECT compress_for FROM all_tables')
        if "Enterprise Edition" not in testing.db.scalar(
                "select * from v$version"):
            return True
        return False
    except Exception:
        return True


class TableReflectionTest(fixtures.TestBase):
    __only_on__ = 'oracle'
    __backend__ = True

    @testing.provide_metadata
    @testing.fails_if(all_tables_compression_missing)
    def test_reflect_basic_compression(self):
        metadata = self.metadata

        tbl = Table('test_compress', metadata,
                    Column('data', Integer, primary_key=True),
                    oracle_compress=True)
        metadata.create_all()

        m2 = MetaData(testing.db)

        tbl = Table('test_compress', m2, autoload=True)
        # Don't hardcode the exact value, but it must be non-empty
        assert tbl.dialect_options['oracle']['compress']

    @testing.provide_metadata
    @testing.fails_if(all_tables_compress_for_missing)
    def test_reflect_oltp_compression(self):
        metadata = self.metadata

        tbl = Table('test_compress', metadata,
                    Column('data', Integer, primary_key=True),
                    oracle_compress="OLTP")
        metadata.create_all()

        m2 = MetaData(testing.db)

        tbl = Table('test_compress', m2, autoload=True)
        assert tbl.dialect_options['oracle']['compress'] == "OLTP"


class RoundTripIndexTest(fixtures.TestBase):
    __only_on__ = 'oracle'
    __backend__ = True

    @testing.provide_metadata
    def test_basic(self):
        metadata = self.metadata

        table = Table("sometable", metadata,
                      Column("id_a", Unicode(255), primary_key=True),
                      Column("id_b",
                             Unicode(255),
                             primary_key=True,
                             unique=True),
                      Column("group", Unicode(255), primary_key=True),
                      Column("col", Unicode(255)),
                      UniqueConstraint('col', 'group'))

        # "group" is a keyword, so lower case
        normalind = Index('tableind', table.c.id_b, table.c.group)
        compress1 = Index('compress1', table.c.id_a, table.c.id_b,
                          oracle_compress=True)
        compress2 = Index('compress2', table.c.id_a, table.c.id_b, table.c.col,
                          oracle_compress=1)

        metadata.create_all()
        mirror = MetaData(testing.db)
        mirror.reflect()
        metadata.drop_all()
        mirror.create_all()

        inspect = MetaData(testing.db)
        inspect.reflect()

        def obj_definition(obj):
            return (obj.__class__,
                    tuple([c.name for c in obj.columns]),
                    getattr(obj, 'unique', None))

        # find what the primary k constraint name should be
        primaryconsname = testing.db.scalar(
            text(
                """SELECT constraint_name
               FROM all_constraints
               WHERE table_name = :table_name
               AND owner = :owner
               AND constraint_type = 'P' """),
            table_name=table.name.upper(),
            owner=testing.db.dialect.default_schema_name.upper())

        reflectedtable = inspect.tables[table.name]

        # make a dictionary of the reflected objects:

        reflected = dict([(obj_definition(i), i) for i in
                         reflectedtable.indexes
                         | reflectedtable.constraints])

        # assert we got primary key constraint and its name, Error
        # if not in dict

        assert reflected[(PrimaryKeyConstraint, ('id_a', 'id_b',
                         'group'), None)].name.upper() \
            == primaryconsname.upper()

        # Error if not in dict

        eq_(
            reflected[(Index, ('id_b', 'group'), False)].name,
            normalind.name
        )
        assert (Index, ('id_b', ), True) in reflected
        assert (Index, ('col', 'group'), True) in reflected

        idx = reflected[(Index, ('id_a', 'id_b', ), False)]
        assert idx.dialect_options['oracle']['compress'] == 2

        idx = reflected[(Index, ('id_a', 'id_b', 'col', ), False)]
        assert idx.dialect_options['oracle']['compress'] == 1

        eq_(len(reflectedtable.constraints), 1)
        eq_(len(reflectedtable.indexes), 5)


class SequenceTest(fixtures.TestBase, AssertsCompiledSQL):

    def test_basic(self):
        seq = Sequence('my_seq_no_schema')
        dialect = oracle.OracleDialect()
        assert dialect.identifier_preparer.format_sequence(seq) \
            == 'my_seq_no_schema'
        seq = Sequence('my_seq', schema='some_schema')
        assert dialect.identifier_preparer.format_sequence(seq) \
            == 'some_schema.my_seq'
        seq = Sequence('My_Seq', schema='Some_Schema')
        assert dialect.identifier_preparer.format_sequence(seq) \
            == '"Some_Schema"."My_Seq"'


class ExecuteTest(fixtures.TestBase):

    __only_on__ = 'oracle'
    __backend__ = True

    def test_basic(self):
        eq_(testing.db.execute('/*+ this is a comment */ SELECT 1 FROM '
            'DUAL').fetchall(), [(1, )])

    def test_sequences_are_integers(self):
        seq = Sequence('foo_seq')
        seq.create(testing.db)
        try:
            val = testing.db.execute(seq)
            eq_(val, 1)
            assert type(val) is int
        finally:
            seq.drop(testing.db)

    @testing.provide_metadata
    def test_limit_offset_for_update(self):
        metadata = self.metadata
        # oracle can't actually do the ROWNUM thing with FOR UPDATE
        # very well.

        t = Table('t1',
                  metadata,
                  Column('id', Integer, primary_key=True),
                  Column('data', Integer))
        metadata.create_all()

        t.insert().execute(
            {'id': 1, 'data': 1},
            {'id': 2, 'data': 7},
            {'id': 3, 'data': 12},
            {'id': 4, 'data': 15},
            {'id': 5, 'data': 32},
        )

        # here, we can't use ORDER BY.
        eq_(
            t.select(for_update=True).limit(2).execute().fetchall(),
            [(1, 1),
             (2, 7)]
        )

        # here, its impossible.  But we'd prefer it to raise ORA-02014
        # instead of issuing a syntax error.
        assert_raises_message(
            exc.DatabaseError,
            "ORA-02014",
            t.select(for_update=True).limit(2).offset(3).execute
        )


class UnicodeSchemaTest(fixtures.TestBase):
    __only_on__ = 'oracle'
    __backend__ = True

    @testing.provide_metadata
    def test_quoted_column_non_unicode(self):
        metadata = self.metadata
        table = Table("atable", metadata,
                      Column("_underscorecolumn",
                             Unicode(255),
                             primary_key=True))
        metadata.create_all()

        table.insert().execute(
            {'_underscorecolumn': u('’é')},
        )
        result = testing.db.execute(
            table.select().where(table.c._underscorecolumn == u('’é'))
        ).scalar()
        eq_(result, u('’é'))

    @testing.provide_metadata
    def test_quoted_column_unicode(self):
        metadata = self.metadata
        table = Table("atable", metadata,
                      Column(u("méil"), Unicode(255), primary_key=True))
        metadata.create_all()

        table.insert().execute(
            {u('méil'): u('’é')},
        )
        result = testing.db.execute(
            table.select().where(table.c[u('méil')] == u('’é'))
        ).scalar()
        eq_(result, u('’é'))


class DBLinkReflectionTest(fixtures.TestBase):
    __requires__ = 'oracle_test_dblink',
    __only_on__ = 'oracle'
    __backend__ = True

    @classmethod
    def setup_class(cls):
        from sqlalchemy.testing import config
        cls.dblink = config.file_config.get('sqla_testing', 'oracle_db_link')

        # note that the synonym here is still not totally functional
        # when accessing via a different username as we do with the
        # multiprocess test suite, so testing here is minimal
        with testing.db.connect() as conn:
            conn.execute("create table test_table "
                         "(id integer primary key, data varchar2(50))")
            conn.execute("create synonym test_table_syn "
                         "for test_table@%s" % cls.dblink)

    @classmethod
    def teardown_class(cls):
        with testing.db.connect() as conn:
            conn.execute("drop synonym test_table_syn")
            conn.execute("drop table test_table")

    def test_reflection(self):
        """test the resolution of the synonym/dblink. """
        m = MetaData()

        t = Table('test_table_syn', m, autoload=True,
                  autoload_with=testing.db, oracle_resolve_synonyms=True)
        eq_(list(t.c.keys()), ['id', 'data'])
        eq_(list(t.primary_key), [t.c.id])


class ServiceNameTest(fixtures.TestBase):
    __only_on__ = 'oracle+cx_oracle'
    __backend__ = True

    def test_cx_oracle_service_name(self):
        url_string = 'oracle+cx_oracle://scott:tiger@host/?service_name=hr'
        eng = create_engine(url_string, _initialize=False)
        cargs, cparams = eng.dialect.create_connect_args(eng.url)

        assert 'SERVICE_NAME=hr' in cparams['dsn']
        assert 'SID=hr' not in cparams['dsn']

    def test_cx_oracle_service_name_bad(self):
        url_string = 'oracle+cx_oracle://scott:tiger@host/hr1?service_name=hr2'
        assert_raises(
            exc.InvalidRequestError,
            create_engine, url_string,
            _initialize=False
        )

