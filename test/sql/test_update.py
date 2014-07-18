from sqlalchemy import *
from sqlalchemy import testing
from sqlalchemy.dialects import mysql
from sqlalchemy.engine import default
from sqlalchemy.testing import AssertsCompiledSQL, eq_, fixtures
from sqlalchemy.testing.schema import Table, Column


class _UpdateFromTestBase(object):

    @classmethod
    def define_tables(cls, metadata):
        Table('mytable', metadata,
              Column('myid', Integer),
              Column('name', String(30)),
              Column('description', String(50)))
        Table('myothertable', metadata,
              Column('otherid', Integer),
              Column('othername', String(30)))
        Table('users', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('name', String(30), nullable=False))
        Table('addresses', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('user_id', None, ForeignKey('users.id')),
              Column('name', String(30), nullable=False),
              Column('email_address', String(50), nullable=False))
        Table('dingalings', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('address_id', None, ForeignKey('addresses.id')),
              Column('data', String(30)))

    @classmethod
    def fixtures(cls):
        return dict(
            users=(
                ('id', 'name'),
                (7, 'jack'),
                (8, 'ed'),
                (9, 'fred'),
                (10, 'chuck')
            ),
            addresses = (
                ('id', 'user_id', 'name', 'email_address'),
                (1, 7, 'x', 'jack@bean.com'),
                (2, 8, 'x', 'ed@wood.com'),
                (3, 8, 'x', 'ed@bettyboop.com'),
                (4, 8, 'x', 'ed@lala.com'),
                (5, 9, 'x', 'fred@fred.com')
            ),
            dingalings = (
                ('id', 'address_id', 'data'),
                (1, 2, 'ding 1/2'),
                (2, 5, 'ding 2/5')
            ),
        )


class UpdateTest(_UpdateFromTestBase, fixtures.TablesTest, AssertsCompiledSQL):
    __dialect__ = 'default'

    def test_update_1(self):
        table1 = self.tables.mytable

        self.assert_compile(
            update(table1, table1.c.myid == 7),
            'UPDATE mytable SET name=:name WHERE mytable.myid = :myid_1',
            params={table1.c.name: 'fred'})

    def test_update_2(self):
        table1 = self.tables.mytable

        self.assert_compile(
            table1.update().
            where(table1.c.myid == 7).
            values({table1.c.myid: 5}),
            'UPDATE mytable SET myid=:myid WHERE mytable.myid = :myid_1',
            checkparams={'myid': 5, 'myid_1': 7})

    def test_update_3(self):
        table1 = self.tables.mytable

        self.assert_compile(
            update(table1, table1.c.myid == 7),
            'UPDATE mytable SET name=:name WHERE mytable.myid = :myid_1',
            params={'name': 'fred'})

    def test_update_4(self):
        table1 = self.tables.mytable

        self.assert_compile(
            update(table1, values={table1.c.name: table1.c.myid}),
            'UPDATE mytable SET name=mytable.myid')

    def test_update_5(self):
        table1 = self.tables.mytable

        self.assert_compile(
            update(table1,
                   whereclause=table1.c.name == bindparam('crit'),
                   values={table1.c.name: 'hi'}),
            'UPDATE mytable SET name=:name WHERE mytable.name = :crit',
            params={'crit': 'notthere'},
            checkparams={'crit': 'notthere', 'name': 'hi'})

    def test_update_6(self):
        table1 = self.tables.mytable

        self.assert_compile(
            update(table1,
                   table1.c.myid == 12,
                   values={table1.c.name: table1.c.myid}),
            'UPDATE mytable '
            'SET name=mytable.myid, description=:description '
            'WHERE mytable.myid = :myid_1',
            params={'description': 'test'},
            checkparams={'description': 'test', 'myid_1': 12})

    def test_update_7(self):
        table1 = self.tables.mytable

        self.assert_compile(
            update(table1, table1.c.myid == 12, values={table1.c.myid: 9}),
            'UPDATE mytable '
            'SET myid=:myid, description=:description '
            'WHERE mytable.myid = :myid_1',
            params={'myid_1': 12, 'myid': 9, 'description': 'test'})

    def test_update_8(self):
        table1 = self.tables.mytable

        self.assert_compile(
            update(table1, table1.c.myid == 12),
            'UPDATE mytable SET myid=:myid WHERE mytable.myid = :myid_1',
            params={'myid': 18}, checkparams={'myid': 18, 'myid_1': 12})

    def test_update_9(self):
        table1 = self.tables.mytable

        s = table1.update(table1.c.myid == 12, values={table1.c.name: 'lala'})
        c = s.compile(column_keys=['id', 'name'])
        eq_(str(s), str(c))

    def test_update_10(self):
        table1 = self.tables.mytable

        v1 = {table1.c.name: table1.c.myid}
        v2 = {table1.c.name: table1.c.name + 'foo'}
        self.assert_compile(
            update(table1, table1.c.myid == 12, values=v1).values(v2),
            'UPDATE mytable '
            'SET '
            'name=(mytable.name || :name_1), '
            'description=:description '
            'WHERE mytable.myid = :myid_1',
            params={'description': 'test'})

    def test_update_11(self):
        table1 = self.tables.mytable

        values = {
            table1.c.name: table1.c.name + 'lala',
            table1.c.myid: func.do_stuff(table1.c.myid, literal('hoho'))
        }
        self.assert_compile(
            update(
                table1,
                (table1.c.myid == func.hoho(4)) & (
                    table1.c.name == literal('foo') +
                    table1.c.name +
                    literal('lala')),
                values=values),
            'UPDATE mytable '
            'SET '
            'myid=do_stuff(mytable.myid, :param_1), '
            'name=(mytable.name || :name_1) '
            'WHERE '
            'mytable.myid = hoho(:hoho_1) AND '
            'mytable.name = :param_2 || mytable.name || :param_3')

    def test_where_empty(self):
        table1 = self.tables.mytable
        self.assert_compile(
            table1.update().where(
                and_()),
            "UPDATE mytable SET myid=:myid, name=:name, description=:description")
        self.assert_compile(
            table1.update().where(
                or_()),
            "UPDATE mytable SET myid=:myid, name=:name, description=:description")

    def test_prefix_with(self):
        table1 = self.tables.mytable

        stmt = table1.update().\
            prefix_with('A', 'B', dialect='mysql').\
            prefix_with('C', 'D')

        self.assert_compile(stmt,
                            'UPDATE C D mytable SET myid=:myid, name=:name, '
                            'description=:description')

        self.assert_compile(
            stmt,
            'UPDATE A B C D mytable SET myid=%s, name=%s, description=%s',
            dialect=mysql.dialect())

    def test_update_to_expression(self):
        """test update from an expression.

        this logic is triggered currently by a left side that doesn't
        have a key.  The current supported use case is updating the index
        of a Postgresql ARRAY type.

        """
        table1 = self.tables.mytable
        expr = func.foo(table1.c.myid)
        eq_(expr.key, None)
        self.assert_compile(table1.update().values({expr: 'bar'}),
                            'UPDATE mytable SET foo(myid)=:param_1')

    def test_update_bound_ordering(self):
        """test that bound parameters between the UPDATE and FROM clauses
        order correctly in different SQL compilation scenarios.

        """
        table1 = self.tables.mytable
        table2 = self.tables.myothertable
        sel = select([table2]).where(table2.c.otherid == 5).alias()
        upd = table1.update().\
            where(table1.c.name == sel.c.othername).\
            values(name='foo')

        dialect = default.DefaultDialect()
        dialect.positional = True
        self.assert_compile(
            upd,
            "UPDATE mytable SET name=:name FROM (SELECT "
            "myothertable.otherid AS otherid, "
            "myothertable.othername AS othername "
            "FROM myothertable "
            "WHERE myothertable.otherid = :otherid_1) AS anon_1 "
            "WHERE mytable.name = anon_1.othername",
            checkpositional=('foo', 5),
            dialect=dialect
        )

        self.assert_compile(
            upd,
            "UPDATE mytable, (SELECT myothertable.otherid AS otherid, "
            "myothertable.othername AS othername "
            "FROM myothertable "
            "WHERE myothertable.otherid = %s) AS anon_1 SET mytable.name=%s "
            "WHERE mytable.name = anon_1.othername",
            checkpositional=(5, 'foo'),
            dialect=mysql.dialect()
        )


class UpdateFromCompileTest(_UpdateFromTestBase, fixtures.TablesTest,
                            AssertsCompiledSQL):
    __dialect__ = 'default'

    run_create_tables = run_inserts = run_deletes = None

    def test_alias_one(self):
        table1 = self.tables.mytable
        talias1 = table1.alias('t1')

        # this case is nonsensical.  the UPDATE is entirely
        # against the alias, but we name the table-bound column
        # in values.   The behavior here isn't really defined
        self.assert_compile(
            update(talias1, talias1.c.myid == 7).
            values({table1.c.name: "fred"}),
            'UPDATE mytable AS t1 '
            'SET name=:name '
            'WHERE t1.myid = :myid_1')

    def test_alias_two(self):
        table1 = self.tables.mytable
        talias1 = table1.alias('t1')

        # Here, compared to
        # test_alias_one(), here we actually have UPDATE..FROM,
        # which is causing the "table1.c.name" param to be handled
        # as an "extra table", hence we see the full table name rendered.
        self.assert_compile(
            update(talias1, table1.c.myid == 7).
            values({table1.c.name: 'fred'}),
            'UPDATE mytable AS t1 '
            'SET name=:mytable_name '
            'FROM mytable '
            'WHERE mytable.myid = :myid_1',
            checkparams={'mytable_name': 'fred', 'myid_1': 7},
        )

    def test_alias_two_mysql(self):
        table1 = self.tables.mytable
        talias1 = table1.alias('t1')

        self.assert_compile(
            update(talias1, table1.c.myid == 7).
            values({table1.c.name: 'fred'}),
            "UPDATE mytable AS t1, mytable SET mytable.name=%s "
            "WHERE mytable.myid = %s",
            checkparams={'mytable_name': 'fred', 'myid_1': 7},
            dialect='mysql')

    def test_update_from_multitable_same_name_mysql(self):
        users, addresses = self.tables.users, self.tables.addresses

        self.assert_compile(
            users.update().
            values(name='newname').
            values({addresses.c.name: "new address"}).
            where(users.c.id == addresses.c.user_id),
            "UPDATE users, addresses SET addresses.name=%s, "
            "users.name=%s WHERE users.id = addresses.user_id",
            checkparams={'addresses_name': 'new address', 'name': 'newname'},
            dialect='mysql'
        )

    def test_render_table(self):
        users, addresses = self.tables.users, self.tables.addresses

        self.assert_compile(
            users.update().
            values(name='newname').
            where(users.c.id == addresses.c.user_id).
            where(addresses.c.email_address == 'e1'),
            'UPDATE users '
            'SET name=:name FROM addresses '
            'WHERE '
            'users.id = addresses.user_id AND '
            'addresses.email_address = :email_address_1',
            checkparams={'email_address_1': 'e1', 'name': 'newname'})

    def test_render_multi_table(self):
        users = self.tables.users
        addresses = self.tables.addresses
        dingalings = self.tables.dingalings

        checkparams = {
            'email_address_1': 'e1',
            'id_1': 2,
            'name': 'newname'
        }

        self.assert_compile(
            users.update().
            values(name='newname').
            where(users.c.id == addresses.c.user_id).
            where(addresses.c.email_address == 'e1').
            where(addresses.c.id == dingalings.c.address_id).
            where(dingalings.c.id == 2),
            'UPDATE users '
            'SET name=:name '
            'FROM addresses, dingalings '
            'WHERE '
            'users.id = addresses.user_id AND '
            'addresses.email_address = :email_address_1 AND '
            'addresses.id = dingalings.address_id AND '
            'dingalings.id = :id_1',
            checkparams=checkparams)

    def test_render_table_mysql(self):
        users, addresses = self.tables.users, self.tables.addresses

        self.assert_compile(
            users.update().
            values(name='newname').
            where(users.c.id == addresses.c.user_id).
            where(addresses.c.email_address == 'e1'),
            'UPDATE users, addresses '
            'SET users.name=%s '
            'WHERE '
            'users.id = addresses.user_id AND '
            'addresses.email_address = %s',
            checkparams={'email_address_1': 'e1', 'name': 'newname'},
            dialect=mysql.dialect())

    def test_render_subquery(self):
        users, addresses = self.tables.users, self.tables.addresses

        checkparams = {
            'email_address_1': 'e1',
            'id_1': 7,
            'name': 'newname'
        }

        cols = [
            addresses.c.id,
            addresses.c.user_id,
            addresses.c.email_address
        ]

        subq = select(cols).where(addresses.c.id == 7).alias()
        self.assert_compile(
            users.update().
            values(name='newname').
            where(users.c.id == subq.c.user_id).
            where(subq.c.email_address == 'e1'),
            'UPDATE users '
            'SET name=:name FROM ('
            'SELECT '
            'addresses.id AS id, '
            'addresses.user_id AS user_id, '
            'addresses.email_address AS email_address '
            'FROM addresses '
            'WHERE addresses.id = :id_1'
            ') AS anon_1 '
            'WHERE users.id = anon_1.user_id '
            'AND anon_1.email_address = :email_address_1',
            checkparams=checkparams)


class UpdateFromRoundTripTest(_UpdateFromTestBase, fixtures.TablesTest):
    __backend__ = True

    @testing.requires.update_from
    def test_exec_two_table(self):
        users, addresses = self.tables.users, self.tables.addresses

        testing.db.execute(
            addresses.update().
            values(email_address=users.c.name).
            where(users.c.id == addresses.c.user_id).
            where(users.c.name == 'ed'))

        expected = [
            (1, 7, 'x', 'jack@bean.com'),
            (2, 8, 'x', 'ed'),
            (3, 8, 'x', 'ed'),
            (4, 8, 'x', 'ed'),
            (5, 9, 'x', 'fred@fred.com')]
        self._assert_addresses(addresses, expected)

    @testing.requires.update_from
    def test_exec_two_table_plus_alias(self):
        users, addresses = self.tables.users, self.tables.addresses

        a1 = addresses.alias()
        testing.db.execute(
            addresses.update().
            values(email_address=users.c.name).
            where(users.c.id == a1.c.user_id).
            where(users.c.name == 'ed').
            where(a1.c.id == addresses.c.id)
        )

        expected = [
            (1, 7, 'x', 'jack@bean.com'),
            (2, 8, 'x', 'ed'),
            (3, 8, 'x', 'ed'),
            (4, 8, 'x', 'ed'),
            (5, 9, 'x', 'fred@fred.com')]
        self._assert_addresses(addresses, expected)

    @testing.requires.update_from
    def test_exec_three_table(self):
        users = self.tables.users
        addresses = self.tables.addresses
        dingalings = self.tables.dingalings

        testing.db.execute(
            addresses.update().
            values(email_address=users.c.name).
            where(users.c.id == addresses.c.user_id).
            where(users.c.name == 'ed').
            where(addresses.c.id == dingalings.c.address_id).
            where(dingalings.c.id == 1))

        expected = [
            (1, 7, 'x', 'jack@bean.com'),
            (2, 8, 'x', 'ed'),
            (3, 8, 'x', 'ed@bettyboop.com'),
            (4, 8, 'x', 'ed@lala.com'),
            (5, 9, 'x', 'fred@fred.com')]
        self._assert_addresses(addresses, expected)

    @testing.only_on('mysql', 'Multi table update')
    def test_exec_multitable(self):
        users, addresses = self.tables.users, self.tables.addresses

        values = {
            addresses.c.email_address: users.c.name,
            users.c.name: 'ed2'
        }

        testing.db.execute(
            addresses.update().
            values(values).
            where(users.c.id == addresses.c.user_id).
            where(users.c.name == 'ed'))

        expected = [
            (1, 7, 'x', 'jack@bean.com'),
            (2, 8, 'x', 'ed'),
            (3, 8, 'x', 'ed'),
            (4, 8, 'x', 'ed'),
            (5, 9, 'x', 'fred@fred.com')]
        self._assert_addresses(addresses, expected)

        expected = [
            (7, 'jack'),
            (8, 'ed2'),
            (9, 'fred'),
            (10, 'chuck')]
        self._assert_users(users, expected)

    @testing.only_on('mysql', 'Multi table update')
    def test_exec_multitable_same_name(self):
        users, addresses = self.tables.users, self.tables.addresses

        values = {
            addresses.c.name: 'ad_ed2',
            users.c.name: 'ed2'
        }

        testing.db.execute(
            addresses.update().
            values(values).
            where(users.c.id == addresses.c.user_id).
            where(users.c.name == 'ed'))

        expected = [
            (1, 7, 'x', 'jack@bean.com'),
            (2, 8, 'ad_ed2', 'ed@wood.com'),
            (3, 8, 'ad_ed2', 'ed@bettyboop.com'),
            (4, 8, 'ad_ed2', 'ed@lala.com'),
            (5, 9, 'x', 'fred@fred.com')]
        self._assert_addresses(addresses, expected)

        expected = [
            (7, 'jack'),
            (8, 'ed2'),
            (9, 'fred'),
            (10, 'chuck')]
        self._assert_users(users, expected)

    def _assert_addresses(self, addresses, expected):
        stmt = addresses.select().order_by(addresses.c.id)
        eq_(testing.db.execute(stmt).fetchall(), expected)

    def _assert_users(self, users, expected):
        stmt = users.select().order_by(users.c.id)
        eq_(testing.db.execute(stmt).fetchall(), expected)


class UpdateFromMultiTableUpdateDefaultsTest(_UpdateFromTestBase,
                                             fixtures.TablesTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table('users', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('name', String(30), nullable=False),
              Column('some_update', String(30), onupdate='im the update'))

        Table('addresses', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('user_id', None, ForeignKey('users.id')),
              Column('email_address', String(50), nullable=False),
              )

        Table('foobar', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('user_id', None, ForeignKey('users.id')),
              Column('data', String(30)),
              Column('some_update', String(30), onupdate='im the other update')
              )

    @classmethod
    def fixtures(cls):
        return dict(
            users=(
                ('id', 'name', 'some_update'),
                (8, 'ed', 'value'),
                (9, 'fred', 'value'),
            ),
            addresses=(
                ('id', 'user_id', 'email_address'),
                (2, 8, 'ed@wood.com'),
                (3, 8, 'ed@bettyboop.com'),
                (4, 9, 'fred@fred.com')
            ),
            foobar=(
                ('id', 'user_id', 'data'),
                (2, 8, 'd1'),
                (3, 8, 'd2'),
                (4, 9, 'd3')
            )
        )

    @testing.only_on('mysql', 'Multi table update')
    def test_defaults_second_table(self):
        users, addresses = self.tables.users, self.tables.addresses

        values = {
            addresses.c.email_address: users.c.name,
            users.c.name: 'ed2'
        }

        ret = testing.db.execute(
            addresses.update().
            values(values).
            where(users.c.id == addresses.c.user_id).
            where(users.c.name == 'ed'))

        eq_(set(ret.prefetch_cols()), set([users.c.some_update]))

        expected = [
            (2, 8, 'ed'),
            (3, 8, 'ed'),
            (4, 9, 'fred@fred.com')]
        self._assert_addresses(addresses, expected)

        expected = [
            (8, 'ed2', 'im the update'),
            (9, 'fred', 'value')]
        self._assert_users(users, expected)

    @testing.only_on('mysql', 'Multi table update')
    def test_defaults_second_table_same_name(self):
        users, foobar = self.tables.users, self.tables.foobar

        values = {
            foobar.c.data: foobar.c.data + 'a',
            users.c.name: 'ed2'
        }

        ret = testing.db.execute(
            users.update().
            values(values).
            where(users.c.id == foobar.c.user_id).
            where(users.c.name == 'ed'))

        eq_(
            set(ret.prefetch_cols()),
            set([users.c.some_update, foobar.c.some_update])
        )

        expected = [
            (2, 8, 'd1a', 'im the other update'),
            (3, 8, 'd2a', 'im the other update'),
            (4, 9, 'd3', None)]
        self._assert_foobar(foobar, expected)

        expected = [
            (8, 'ed2', 'im the update'),
            (9, 'fred', 'value')]
        self._assert_users(users, expected)

    @testing.only_on('mysql', 'Multi table update')
    def test_no_defaults_second_table(self):
        users, addresses = self.tables.users, self.tables.addresses

        ret = testing.db.execute(
            addresses.update().
            values({'email_address': users.c.name}).
            where(users.c.id == addresses.c.user_id).
            where(users.c.name == 'ed'))

        eq_(ret.prefetch_cols(), [])

        expected = [
            (2, 8, 'ed'),
            (3, 8, 'ed'),
            (4, 9, 'fred@fred.com')]
        self._assert_addresses(addresses, expected)

        # users table not actually updated, so no onupdate
        expected = [
            (8, 'ed', 'value'),
            (9, 'fred', 'value')]
        self._assert_users(users, expected)

    def _assert_foobar(self, foobar, expected):
        stmt = foobar.select().order_by(foobar.c.id)
        eq_(testing.db.execute(stmt).fetchall(), expected)

    def _assert_addresses(self, addresses, expected):
        stmt = addresses.select().order_by(addresses.c.id)
        eq_(testing.db.execute(stmt).fetchall(), expected)

    def _assert_users(self, users, expected):
        stmt = users.select().order_by(users.c.id)
        eq_(testing.db.execute(stmt).fetchall(), expected)
