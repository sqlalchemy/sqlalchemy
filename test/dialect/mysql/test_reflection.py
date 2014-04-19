# coding: utf-8

from sqlalchemy.testing import eq_
from sqlalchemy import *
from sqlalchemy import sql
from sqlalchemy.dialects.mysql import base as mysql
from sqlalchemy.testing import fixtures, AssertsExecutionResults
from sqlalchemy import testing

class ReflectionTest(fixtures.TestBase, AssertsExecutionResults):

    __only_on__ = 'mysql'

    def test_default_reflection(self):
        """Test reflection of column defaults."""

        from sqlalchemy.dialects.mysql import VARCHAR
        def_table = Table(
            'mysql_def',
            MetaData(testing.db),
            Column('c1', VARCHAR(10, collation='utf8_unicode_ci'),
                   DefaultClause(''), nullable=False),
            Column('c2', String(10), DefaultClause('0')),
            Column('c3', String(10), DefaultClause('abc')),
            Column('c4', TIMESTAMP, DefaultClause('2009-04-05 12:00:00'
                   )),
            Column('c5', TIMESTAMP),
            Column('c6', TIMESTAMP,
                   DefaultClause(sql.text("CURRENT_TIMESTAMP "
                                          "ON UPDATE CURRENT_TIMESTAMP"))),
            )
        def_table.create()
        try:
            reflected = Table('mysql_def', MetaData(testing.db),
                              autoload=True)
        finally:
            def_table.drop()
        assert def_table.c.c1.server_default.arg == ''
        assert def_table.c.c2.server_default.arg == '0'
        assert def_table.c.c3.server_default.arg == 'abc'
        assert def_table.c.c4.server_default.arg \
            == '2009-04-05 12:00:00'
        assert str(reflected.c.c1.server_default.arg) == "''"
        assert str(reflected.c.c2.server_default.arg) == "'0'"
        assert str(reflected.c.c3.server_default.arg) == "'abc'"
        assert str(reflected.c.c4.server_default.arg) \
            == "'2009-04-05 12:00:00'"
        assert reflected.c.c5.default is None
        assert reflected.c.c5.server_default is None
        assert reflected.c.c6.default is None
        eq_(
            str(reflected.c.c6.server_default.arg).upper(),
            "CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"
        )
        reflected.create()
        try:
            reflected2 = Table('mysql_def', MetaData(testing.db),
                               autoload=True)
        finally:
            reflected.drop()
        assert str(reflected2.c.c1.server_default.arg) == "''"
        assert str(reflected2.c.c2.server_default.arg) == "'0'"
        assert str(reflected2.c.c3.server_default.arg) == "'abc'"
        assert str(reflected2.c.c4.server_default.arg) \
            == "'2009-04-05 12:00:00'"
        assert reflected.c.c5.default is None
        assert reflected.c.c5.server_default is None
        assert reflected.c.c6.default is None
        eq_(
            str(reflected.c.c6.server_default.arg).upper(),
            "CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"
        )

    def test_reflection_with_table_options(self):
        comment = r"""Comment types type speedily ' " \ '' Fun!"""

        def_table = Table('mysql_def', MetaData(testing.db),
            Column('c1', Integer()),
            mysql_engine='MEMORY',
            mysql_comment=comment,
            mysql_default_charset='utf8',
            mysql_auto_increment='5',
            mysql_avg_row_length='3',
            mysql_password='secret',
            mysql_connection='fish',
        )

        def_table.create()
        try:
            reflected = Table('mysql_def', MetaData(testing.db),
                          autoload=True)
        finally:
            def_table.drop()

        assert def_table.kwargs['mysql_engine'] == 'MEMORY'
        assert def_table.kwargs['mysql_comment'] == comment
        assert def_table.kwargs['mysql_default_charset'] == 'utf8'
        assert def_table.kwargs['mysql_auto_increment'] == '5'
        assert def_table.kwargs['mysql_avg_row_length'] == '3'
        assert def_table.kwargs['mysql_password'] == 'secret'
        assert def_table.kwargs['mysql_connection'] == 'fish'

        assert reflected.kwargs['mysql_engine'] == 'MEMORY'
        assert reflected.kwargs['mysql_comment'] == comment
        assert reflected.kwargs['mysql_default charset'] == 'utf8'
        assert reflected.kwargs['mysql_avg_row_length'] == '3'
        assert reflected.kwargs['mysql_connection'] == 'fish'

        # This field doesn't seem to be returned by mysql itself.
        #assert reflected.kwargs['mysql_password'] == 'secret'

        # This is explicitly ignored when reflecting schema.
        #assert reflected.kwargs['mysql_auto_increment'] == '5'

    def test_reflection_on_include_columns(self):
        """Test reflection of include_columns to be sure they respect case."""

        case_table = Table('mysql_case', MetaData(testing.db),
            Column('c1', String(10)),
            Column('C2', String(10)),
            Column('C3', String(10)))

        try:
            case_table.create()
            reflected = Table('mysql_case', MetaData(testing.db),
                              autoload=True, include_columns=['c1', 'C2'])
            for t in case_table, reflected:
                assert 'c1' in t.c.keys()
                assert 'C2' in t.c.keys()
            reflected2 = Table('mysql_case', MetaData(testing.db),
                              autoload=True, include_columns=['c1', 'c2'])
            assert 'c1' in reflected2.c.keys()
            for c in ['c2', 'C2', 'C3']:
                assert c not in reflected2.c.keys()
        finally:
            case_table.drop()

    @testing.exclude('mysql', '<', (5, 0, 0), 'early types are squirrely')
    @testing.uses_deprecated('Using String type with no length')
    @testing.uses_deprecated('Manually quoting ENUM value literals')
    def test_type_reflection(self):
        # (ask_for, roundtripped_as_if_different)
        specs = [(String(1), mysql.MSString(1), ),
                 (String(3), mysql.MSString(3), ),
                 (Text(), mysql.MSText(), ),
                 (Unicode(1), mysql.MSString(1), ),
                 (Unicode(3), mysql.MSString(3), ),
                 (UnicodeText(), mysql.MSText(), ),
                 (mysql.MSChar(1), ),
                 (mysql.MSChar(3), ),
                 (NCHAR(2), mysql.MSChar(2), ),
                 (mysql.MSNChar(2), mysql.MSChar(2), ), # N is CREATE only
                 (mysql.MSNVarChar(22), mysql.MSString(22), ),
                 (SmallInteger(), mysql.MSSmallInteger(), ),
                 (SmallInteger(), mysql.MSSmallInteger(4), ),
                 (mysql.MSSmallInteger(), ),
                 (mysql.MSSmallInteger(4), mysql.MSSmallInteger(4), ),
                 (mysql.MSMediumInteger(), mysql.MSMediumInteger(), ),
                 (mysql.MSMediumInteger(8), mysql.MSMediumInteger(8), ),
                 (LargeBinary(3), mysql.TINYBLOB(), ),
                 (LargeBinary(), mysql.BLOB() ),
                 (mysql.MSBinary(3), mysql.MSBinary(3), ),
                 (mysql.MSVarBinary(3),),
                 (mysql.MSTinyBlob(),),
                 (mysql.MSBlob(),),
                 (mysql.MSBlob(1234), mysql.MSBlob()),
                 (mysql.MSMediumBlob(),),
                 (mysql.MSLongBlob(),),
                 (mysql.ENUM("''","'fleem'"), ),
                 ]

        columns = [Column('c%i' % (i + 1), t[0]) for i, t in enumerate(specs)]

        db = testing.db
        m = MetaData(db)
        t_table = Table('mysql_types', m, *columns)
        try:
            m.create_all()

            m2 = MetaData(db)
            rt = Table('mysql_types', m2, autoload=True)
            try:
                db.execute('CREATE OR REPLACE VIEW mysql_types_v '
                           'AS SELECT * from mysql_types')
                rv = Table('mysql_types_v', m2, autoload=True)

                expected = [len(c) > 1 and c[1] or c[0] for c in specs]

                # Early 5.0 releases seem to report more "general" for columns
                # in a view, e.g. char -> varchar, tinyblob -> mediumblob
                #
                # Not sure exactly which point version has the fix.
                if db.dialect.server_version_info < (5, 0, 11):
                    tables = rt,
                else:
                    tables = rt, rv

                for table in tables:
                    for i, reflected in enumerate(table.c):
                        assert isinstance(reflected.type,
                                type(expected[i])), \
                            'element %d: %r not instance of %r' % (i,
                                reflected.type, type(expected[i]))
            finally:
                db.execute('DROP VIEW mysql_types_v')
        finally:
            m.drop_all()

    def test_autoincrement(self):
        meta = MetaData(testing.db)
        try:
            Table('ai_1', meta,
                  Column('int_y', Integer, primary_key=True),
                  Column('int_n', Integer, DefaultClause('0'),
                         primary_key=True),
                         mysql_engine='MyISAM')
            Table('ai_2', meta,
                  Column('int_y', Integer, primary_key=True),
                  Column('int_n', Integer, DefaultClause('0'),
                         primary_key=True),
                         mysql_engine='MyISAM')
            Table('ai_3', meta,
                  Column('int_n', Integer, DefaultClause('0'),
                         primary_key=True, autoincrement=False),
                  Column('int_y', Integer, primary_key=True),
                         mysql_engine='MyISAM')
            Table('ai_4', meta,
                  Column('int_n', Integer, DefaultClause('0'),
                         primary_key=True, autoincrement=False),
                  Column('int_n2', Integer, DefaultClause('0'),
                         primary_key=True, autoincrement=False),
                         mysql_engine='MyISAM')
            Table('ai_5', meta,
                  Column('int_y', Integer, primary_key=True),
                  Column('int_n', Integer, DefaultClause('0'),
                         primary_key=True, autoincrement=False),
                         mysql_engine='MyISAM')
            Table('ai_6', meta,
                  Column('o1', String(1), DefaultClause('x'),
                         primary_key=True),
                  Column('int_y', Integer, primary_key=True),
                         mysql_engine='MyISAM')
            Table('ai_7', meta,
                  Column('o1', String(1), DefaultClause('x'),
                         primary_key=True),
                  Column('o2', String(1), DefaultClause('x'),
                         primary_key=True),
                  Column('int_y', Integer, primary_key=True),
                         mysql_engine='MyISAM')
            Table('ai_8', meta,
                  Column('o1', String(1), DefaultClause('x'),
                         primary_key=True),
                  Column('o2', String(1), DefaultClause('x'),
                         primary_key=True),
                         mysql_engine='MyISAM')
            meta.create_all()

            table_names = ['ai_1', 'ai_2', 'ai_3', 'ai_4',
                           'ai_5', 'ai_6', 'ai_7', 'ai_8']
            mr = MetaData(testing.db)
            mr.reflect(only=table_names)

            for tbl in [mr.tables[name] for name in table_names]:
                for c in tbl.c:
                    if c.name.startswith('int_y'):
                        assert c.autoincrement
                    elif c.name.startswith('int_n'):
                        assert not c.autoincrement
                tbl.insert().execute()
                if 'int_y' in tbl.c:
                    assert select([tbl.c.int_y]).scalar() == 1
                    assert list(tbl.select().execute().first()).count(1) == 1
                else:
                    assert 1 not in list(tbl.select().execute().first())
        finally:
            meta.drop_all()

    @testing.exclude('mysql', '<', (5, 0, 0), 'no information_schema support')
    def test_system_views(self):
        dialect = testing.db.dialect
        connection = testing.db.connect()
        view_names = dialect.get_view_names(connection, "information_schema")
        self.assert_('TABLES' in view_names)


class RawReflectionTest(fixtures.TestBase):
    def setup(self):
        dialect = mysql.dialect()
        self.parser = mysql.MySQLTableDefinitionParser(dialect, dialect.identifier_preparer)

    def test_key_reflection(self):
        regex = self.parser._re_key

        assert regex.match('  PRIMARY KEY (`id`),')
        assert regex.match('  PRIMARY KEY USING BTREE (`id`),')
        assert regex.match('  PRIMARY KEY (`id`) USING BTREE,')
        assert regex.match('  PRIMARY KEY (`id`)')
        assert regex.match('  PRIMARY KEY USING BTREE (`id`)')
        assert regex.match('  PRIMARY KEY (`id`) USING BTREE')
        assert regex.match('  PRIMARY KEY (`id`) USING BTREE KEY_BLOCK_SIZE 16')
        assert regex.match('  PRIMARY KEY (`id`) USING BTREE KEY_BLOCK_SIZE=16')
        assert regex.match('  PRIMARY KEY (`id`) USING BTREE KEY_BLOCK_SIZE  = 16')
        assert not regex.match('  PRIMARY KEY (`id`) USING BTREE KEY_BLOCK_SIZE = = 16')

    def test_fk_reflection(self):
        regex = self.parser._re_constraint

        m = regex.match('  CONSTRAINT `addresses_user_id_fkey` '
                        'FOREIGN KEY (`user_id`) '
                        'REFERENCES `users` (`id`) '
                        'ON DELETE CASCADE ON UPDATE CASCADE')
        eq_(m.groups(), ('addresses_user_id_fkey', '`user_id`',
                            '`users`', '`id`', None, 'CASCADE', 'CASCADE'))


        m = regex.match('  CONSTRAINT `addresses_user_id_fkey` '
                        'FOREIGN KEY (`user_id`) '
                        'REFERENCES `users` (`id`) '
                        'ON DELETE CASCADE ON UPDATE SET NULL')
        eq_(m.groups(), ('addresses_user_id_fkey', '`user_id`',
                            '`users`', '`id`', None, 'CASCADE', 'SET NULL'))


