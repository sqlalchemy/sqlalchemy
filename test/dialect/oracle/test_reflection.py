# coding: utf-8


from sqlalchemy.testing import eq_
from sqlalchemy import exc
from sqlalchemy.sql import table
from sqlalchemy.testing import fixtures, AssertsCompiledSQL
from sqlalchemy import testing
from sqlalchemy import Integer, Text, LargeBinary, Unicode, UniqueConstraint,\
    Index, MetaData, select, inspect, ForeignKey, String, func, \
    TypeDecorator, bindparam, Numeric, TIMESTAMP, CHAR, text, \
    literal_column, VARCHAR, create_engine, Date, NVARCHAR, \
    ForeignKeyConstraint, Sequence, Float, DateTime, cast, UnicodeText, \
    union, except_, type_coerce, or_, outerjoin, DATE, NCHAR, outparam, \
    PrimaryKeyConstraint, FLOAT
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing.engines import testing_engine
from sqlalchemy.testing.schema import Table, Column


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

    def test_reflect_check_include_all(self):
        insp = inspect(testing.db)
        eq_(insp.get_check_constraints('foo'), [])
        eq_(
            [rec['sqltext']
             for rec in insp.get_check_constraints('foo', include_all=True)],
            ['"ID" IS NOT NULL'])


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

        s_table = Table(
            "sometable", metadata,
            Column("id_a", Unicode(255), primary_key=True),
            Column("id_b",
                   Unicode(255),
                   primary_key=True,
                   unique=True),
            Column("group", Unicode(255), primary_key=True),
            Column("col", Unicode(255)),
            UniqueConstraint('col', 'group'))

        # "group" is a keyword, so lower case
        normalind = Index('tableind', s_table.c.id_b, s_table.c.group)
        Index('compress1', s_table.c.id_a, s_table.c.id_b,
              oracle_compress=True)
        Index('compress2', s_table.c.id_a, s_table.c.id_b, s_table.c.col,
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
            table_name=s_table.name.upper(),
            owner=testing.db.dialect.default_schema_name.upper())

        reflectedtable = inspect.tables[s_table.name]

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


