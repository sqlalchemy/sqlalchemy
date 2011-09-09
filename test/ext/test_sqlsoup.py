from sqlalchemy.ext import sqlsoup
from test.lib.testing import eq_, assert_raises, \
    assert_raises_message
from sqlalchemy import create_engine, or_, desc, select, func, exc, \
    Table, util, Column, Integer
from sqlalchemy.orm import scoped_session, sessionmaker
import datetime
from test.lib import fixtures

class SQLSoupTest(fixtures.TestBase):

    __requires__ = 'sqlite',

    @classmethod
    def setup_class(cls):
        global engine
        engine = create_engine('sqlite://')
        for sql in _ddl:
            engine.execute(sql)

    @classmethod
    def teardown_class(cls):
        engine.dispose()

    def setup(self):
        for sql in _data:
            engine.execute(sql)

    def teardown(self):
        sqlsoup.Session.remove()
        for sql in _teardown:
            engine.execute(sql)

    def test_map_to_attr_present(self):
        db = sqlsoup.SqlSoup(engine)

        users = db.users
        assert_raises_message(
            exc.InvalidRequestError,
            "Attribute 'users' is already mapped",
            db.map_to, 'users', tablename='users'
        )

    def test_map_to_table_not_string(self):
        db = sqlsoup.SqlSoup(engine)

        table = Table('users', db._metadata, Column('id', Integer, primary_key=True))
        assert_raises_message(
            exc.ArgumentError,
            "'tablename' argument must be a string.",
            db.map_to, 'users', tablename=table
        )

    def test_map_to_table_or_selectable(self):
        db = sqlsoup.SqlSoup(engine)

        table = Table('users', db._metadata, Column('id', Integer, primary_key=True))
        assert_raises_message(
            exc.ArgumentError,
            "'tablename' and 'selectable' arguments are mutually exclusive",
            db.map_to, 'users', tablename='users', selectable=table
        )

    def test_map_to_no_pk_selectable(self):
        db = sqlsoup.SqlSoup(engine)

        table = Table('users', db._metadata, Column('id', Integer))
        assert_raises_message(
            sqlsoup.PKNotFoundError,
            "table 'users' does not have a primary ",
            db.map_to, 'users', selectable=table
        )
    def test_map_to_invalid_schema(self):
        db = sqlsoup.SqlSoup(engine)

        table = Table('users', db._metadata, Column('id', Integer))
        assert_raises_message(
            exc.ArgumentError,
            "'tablename' argument is required when "
                                "using 'schema'.",
            db.map_to, 'users', selectable=table, schema='hoho'
        )
    def test_map_to_nothing(self):
        db = sqlsoup.SqlSoup(engine)

        assert_raises_message(
            exc.ArgumentError,
            "'tablename' or 'selectable' argument is "
                                    "required.",
            db.map_to, 'users', 
        )

    def test_map_to_string_not_selectable(self):
        db = sqlsoup.SqlSoup(engine)

        assert_raises_message(
            exc.ArgumentError,
            "'selectable' argument must be a "
                                    "table, select, join, or other "
                                    "selectable construct.",
            db.map_to, 'users', selectable='users'
        )

    def test_bad_names(self):
        db = sqlsoup.SqlSoup(engine)

        # print db.bad_names.c.id

        print db.bad_names.c.query

    def test_load(self):
        db = sqlsoup.SqlSoup(engine)
        MappedUsers = db.users
        users = db.users.all()
        users.sort()
        eq_(users, [MappedUsers(name=u'Joe Student',
            email=u'student@example.edu', password=u'student',
            classname=None, admin=0),
            MappedUsers(name=u'Bhargan Basepair',
            email=u'basepair@example.edu', password=u'basepair',
            classname=None, admin=1)])

    def test_order_by(self):
        db = sqlsoup.SqlSoup(engine)
        MappedUsers = db.users
        users = db.users.order_by(db.users.name).all()
        eq_(users, [MappedUsers(name=u'Bhargan Basepair',
            email=u'basepair@example.edu', password=u'basepair',
            classname=None, admin=1), MappedUsers(name=u'Joe Student',
            email=u'student@example.edu', password=u'student',
            classname=None, admin=0)])

    def test_whereclause(self):
        db = sqlsoup.SqlSoup(engine)
        MappedUsers = db.users
        where = or_(db.users.name == 'Bhargan Basepair', db.users.email
                    == 'student@example.edu')
        users = \
            db.users.filter(where).order_by(desc(db.users.name)).all()
        eq_(users, [MappedUsers(name=u'Joe Student',
            email=u'student@example.edu', password=u'student',
            classname=None, admin=0),
            MappedUsers(name=u'Bhargan Basepair',
            email=u'basepair@example.edu', password=u'basepair',
            classname=None, admin=1)])

    def test_first(self):
        db = sqlsoup.SqlSoup(engine)
        MappedUsers = db.users
        user = db.users.filter(db.users.name == 'Bhargan Basepair'
                               ).one()
        eq_(user, MappedUsers(name=u'Bhargan Basepair',
            email=u'basepair@example.edu', password=u'basepair',
            classname=None, admin=1))
        db.rollback()
        user = db.users.get('Bhargan Basepair')
        eq_(user, MappedUsers(name=u'Bhargan Basepair',
            email=u'basepair@example.edu', password=u'basepair',
            classname=None, admin=1))
        db.rollback()
        user = db.users.filter_by(name='Bhargan Basepair').one()
        eq_(user, MappedUsers(name=u'Bhargan Basepair',
            email=u'basepair@example.edu', password=u'basepair',
            classname=None, admin=1))
        db.rollback()

    def test_crud(self):

        # note we're testing autoflush here too...

        db = sqlsoup.SqlSoup(engine)
        MappedLoans = db.loans
        user = db.users.filter_by(name='Bhargan Basepair').one()
        book_id = db.books.filter_by(title='Regional Variation in Moss'
                ).first().id
        loan_insert = db.loans.insert(book_id=book_id,
                user_name=user.name)
        loan = db.loans.filter_by(book_id=2,
                                  user_name='Bhargan Basepair').one()
        eq_(loan, loan_insert)
        l2 = MappedLoans(book_id=2, user_name=u'Bhargan Basepair',
                         loan_date=loan.loan_date)
        eq_(loan, l2)
        db.expunge(l2)
        db.delete(loan)
        loan = db.loans.filter_by(book_id=2,
                                  user_name='Bhargan Basepair').first()
        assert loan is None

    def test_cls_crud(self):
        db = sqlsoup.SqlSoup(engine)
        MappedUsers = db.users
        db.users.filter_by(name='Bhargan Basepair'
                           ).update(dict(name='Some New Name'))
        u1 = db.users.filter_by(name='Some New Name').one()
        eq_(u1, MappedUsers(name=u'Some New Name',
            email=u'basepair@example.edu', password=u'basepair',
            classname=None, admin=1))

    def test_map_table(self):
        db = sqlsoup.SqlSoup(engine)
        users = Table('users', db._metadata, autoload=True)
        MappedUsers = db.map(users)
        users = MappedUsers.order_by(db.users.name).all()
        eq_(users, [MappedUsers(name=u'Bhargan Basepair',
            email=u'basepair@example.edu', password=u'basepair',
            classname=None, admin=1), MappedUsers(name=u'Joe Student',
            email=u'student@example.edu', password=u'student',
            classname=None, admin=0)])

    def test_mapped_join(self):
        db = sqlsoup.SqlSoup(engine)
        join1 = MappedJoin = db.join(db.users, db.loans, isouter=True)
        mj = join1.filter_by(name='Joe Student').all()
        eq_(mj, [MappedJoin(
            name=u'Joe Student',
            email=u'student@example.edu',
            password=u'student',
            classname=None,
            admin=0,
            book_id=1,
            user_name=u'Joe Student',
            loan_date=datetime.datetime(2006, 7, 12, 0, 0),
            )])
        db.rollback()
        join2 = MappedJoin = db.join(join1, db.books)
        mj = join2.all()
        eq_(mj, [MappedJoin(
            name=u'Joe Student',
            email=u'student@example.edu',
            password=u'student',
            classname=None,
            admin=0,
            book_id=1,
            user_name=u'Joe Student',
            loan_date=datetime.datetime(2006, 7, 12, 0, 0),
            id=1,
            title=u'Mustards I Have Known',
            published_year=u'1989',
            authors=u'Jones',
            )])
        eq_(db.with_labels(join1).c.keys(), [
            u'users_name',
            u'users_email',
            u'users_password',
            u'users_classname',
            u'users_admin',
            u'loans_book_id',
            u'loans_user_name',
            u'loans_loan_date',
            ])
        labeled_loans = db.with_labels(db.loans)
        eq_(db.join(db.users, labeled_loans, isouter=True).c.keys(), [
            u'name',
            u'email',
            u'password',
            u'classname',
            u'admin',
            u'loans_book_id',
            u'loans_user_name',
            u'loans_loan_date',
            ])

    def test_relations(self):
        db = sqlsoup.SqlSoup(engine)
        db.users.relate('loans', db.loans)
        MappedLoans = db.loans
        MappedUsers = db.users
        eq_(db.users.get('Joe Student').loans, [MappedLoans(book_id=1,
            user_name=u'Joe Student', loan_date=datetime.datetime(2006,
            7, 12, 0, 0))])
        db.rollback()
        eq_(db.users.filter(~db.users.loans.any()).all(),
            [MappedUsers(name=u'Bhargan Basepair',
            email='basepair@example.edu', password=u'basepair',
            classname=None, admin=1)])
        db.rollback()
        del db._cache['users']
        db.users.relate('loans', db.loans, order_by=db.loans.loan_date,
                        cascade='all, delete-orphan')

    def test_relate_m2o(self):
        db = sqlsoup.SqlSoup(engine)
        db.loans.relate('user', db.users)
        u1 = db.users.filter(db.users.c.name=='Joe Student').one()
        eq_(db.loans.first().user, u1)

    def test_explicit_session(self):
        Session = scoped_session(sessionmaker())
        db = sqlsoup.SqlSoup(engine, session=Session)
        try:
            MappedUsers = db.users
            sess = Session()
            assert db.users._query.session is db.users.session is sess
            row = db.users.insert(name='new name', email='new email')
            assert row in sess
        finally:
            sess.rollback()
            sess.close()

    def test_selectable(self):
        db = sqlsoup.SqlSoup(engine)
        MappedBooks = db.books
        b = db.books._table
        s = select([b.c.published_year, func.count('*').label('n')],
                   from_obj=[b], group_by=[b.c.published_year])
        s = s.alias('years_with_count')
        years_with_count = db.map(s, primary_key=[s.c.published_year])
        eq_(years_with_count.filter_by(published_year='1989').all(),
            [MappedBooks(published_year=u'1989', n=1)])

    def test_raw_sql(self):
        db = sqlsoup.SqlSoup(engine)
        rp = db.execute('select name, email from users order by name')
        eq_(rp.fetchall(), [('Bhargan Basepair', 'basepair@example.edu'
            ), ('Joe Student', 'student@example.edu')])

        # test that execute() shares the same transactional context as
        # the session

        db.execute("update users set email='foo bar'")
        eq_(db.execute('select distinct email from users').fetchall(),
            [('foo bar', )])
        db.rollback()
        eq_(db.execute('select distinct email from users').fetchall(),
            [(u'basepair@example.edu', ), (u'student@example.edu', )])

    def test_connection(self):
        db = sqlsoup.SqlSoup(engine)
        conn = db.connection()
        rp = conn.execute('select name, email from users order by name')
        eq_(rp.fetchall(), [('Bhargan Basepair', 'basepair@example.edu'
            ), ('Joe Student', 'student@example.edu')])

    def test_entity(self):
        db = sqlsoup.SqlSoup(engine)
        tablename = 'loans'
        eq_(db.entity(tablename), db.loans)

    def test_entity_with_different_base(self):
        class subclass(object):
            pass

        db = sqlsoup.SqlSoup(engine, base=subclass)
        assert issubclass(db.entity('loans'), subclass)

    def test_filter_by_order_by(self):
        db = sqlsoup.SqlSoup(engine)
        MappedUsers = db.users
        users = \
            db.users.filter_by(classname=None).order_by(db.users.name).all()
        eq_(users, [MappedUsers(name=u'Bhargan Basepair',
            email=u'basepair@example.edu', password=u'basepair',
            classname=None, admin=1), MappedUsers(name=u'Joe Student',
            email=u'student@example.edu', password=u'student',
            classname=None, admin=0)])

    def test_no_pk_reflected(self):
        db = sqlsoup.SqlSoup(engine)
        assert_raises(sqlsoup.PKNotFoundError, getattr, db, 'nopk')

    def test_nosuchtable(self):
        db = sqlsoup.SqlSoup(engine)
        assert_raises(exc.NoSuchTableError, getattr, db, 'nosuchtable')

    def test_dont_persist_alias(self):
        db = sqlsoup.SqlSoup(engine)
        MappedBooks = db.books
        b = db.books._table
        s = select([b.c.published_year, func.count('*').label('n')],
                   from_obj=[b], group_by=[b.c.published_year])
        s = s.alias('years_with_count')
        years_with_count = db.map(s, primary_key=[s.c.published_year])
        assert_raises(exc.InvalidRequestError, years_with_count.insert,
                      published_year='2007', n=1)

    def test_clear(self):
        db = sqlsoup.SqlSoup(engine)
        eq_(db.loans.count(), 1)
        _ = db.loans.insert(book_id=1, user_name='Bhargan Basepair')
        db.expunge_all()
        db.flush()
        eq_(db.loans.count(), 1)


_ddl = \
    u"""
CREATE TABLE books (
    id                   integer PRIMARY KEY, -- auto-increments in sqlite
    title                text NOT NULL,
    published_year       char(4) NOT NULL,
    authors              text NOT NULL
);

CREATE TABLE users (
    name                 varchar(32) PRIMARY KEY,
    email                varchar(128) NOT NULL,
    password             varchar(128) NOT NULL,
    classname            text,
    admin                int NOT NULL -- 0 = false
);

CREATE TABLE loans (
    book_id              int PRIMARY KEY REFERENCES books(id),
    user_name            varchar(32) references users(name)
        ON DELETE SET NULL ON UPDATE CASCADE,
    loan_date            datetime DEFAULT current_timestamp
);

CREATE TABLE nopk (
    i                    int
);

CREATE TABLE bad_names (
   id int primary key,
   query  varchar(100)
)
""".split(';'
        )
_data = \
    """
insert into users(name, email, password, admin)
values('Bhargan Basepair', 'basepair@example.edu', 'basepair', 1);
insert into users(name, email, password, admin)
values('Joe Student', 'student@example.edu', 'student', 0);

insert into books(title, published_year, authors)
values('Mustards I Have Known', '1989', 'Jones');
insert into books(title, published_year, authors)
values('Regional Variation in Moss', '1971', 'Flim and Flam');

insert into loans(book_id, user_name, loan_date)
values (
    (select min(id) from books),
    (select name from users where name like 'Joe%'),
    '2006-07-12 0:0:0')
;
""".split(';'
        )
_teardown = \
    """
delete from loans;
delete from books;
delete from users;
delete from nopk;
""".split(';'
        )
