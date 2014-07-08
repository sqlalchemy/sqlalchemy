from sqlalchemy.testing import eq_, assert_raises, assert_raises_message
from sqlalchemy.testing import fixtures
from sqlalchemy import Integer, String, ForeignKey, or_, and_, exc, \
    select, func, Boolean, case
from sqlalchemy.orm import mapper, relationship, backref, Session, \
    joinedload, aliased
from sqlalchemy import testing

from sqlalchemy.testing.schema import Table, Column


class UpdateDeleteTest(fixtures.MappedTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table('users', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('name', String(32)),
              Column('age', Integer))

    @classmethod
    def setup_classes(cls):
        class User(cls.Comparable):
            pass

    @classmethod
    def insert_data(cls):
        users = cls.tables.users

        users.insert().execute([
            dict(id=1, name='john', age=25),
            dict(id=2, name='jack', age=47),
            dict(id=3, name='jill', age=29),
            dict(id=4, name='jane', age=37),
        ])

    @classmethod
    def setup_mappers(cls):
        User = cls.classes.User
        users = cls.tables.users

        mapper(User, users)

    def test_illegal_eval(self):
        User = self.classes.User
        s = Session()
        assert_raises_message(
            exc.ArgumentError,
            "Valid strategies for session synchronization "
            "are 'evaluate', 'fetch', False",
            s.query(User).update,
            {},
            synchronize_session="fake"
        )

    def test_illegal_operations(self):
        User = self.classes.User

        s = Session()

        for q, mname in (
            (s.query(User).limit(2), "limit"),
            (s.query(User).offset(2), "offset"),
            (s.query(User).limit(2).offset(2), "limit"),
            (s.query(User).order_by(User.id), "order_by"),
            (s.query(User).group_by(User.id), "group_by"),
            (s.query(User).distinct(), "distinct")
        ):
            assert_raises_message(
                exc.InvalidRequestError,
                r"Can't call Query.update\(\) when %s\(\) has been called" % mname,
                q.update,
                {'name': 'ed'})
            assert_raises_message(
                exc.InvalidRequestError,
                r"Can't call Query.delete\(\) when %s\(\) has been called" % mname,
                q.delete)

    def test_delete(self):
        User = self.classes.User

        sess = Session()

        john, jack, jill, jane = sess.query(User).order_by(User.id).all()
        sess.query(User).filter(
            or_(User.name == 'john', User.name == 'jill')).delete()

        assert john not in sess and jill not in sess

        eq_(sess.query(User).order_by(User.id).all(), [jack, jane])

    def test_delete_against_metadata(self):
        User = self.classes.User
        users = self.tables.users

        sess = Session()
        sess.query(users).delete(synchronize_session=False)
        eq_(sess.query(User).count(), 0)

    def test_delete_with_bindparams(self):
        User = self.classes.User

        sess = Session()

        john, jack, jill, jane = sess.query(User).order_by(User.id).all()
        sess.query(User).filter('name = :name').params(
            name='john').delete('fetch')
        assert john not in sess

        eq_(sess.query(User).order_by(User.id).all(), [jack, jill, jane])

    def test_delete_rollback(self):
        User = self.classes.User

        sess = Session()
        john, jack, jill, jane = sess.query(User).order_by(User.id).all()
        sess.query(User).filter(or_(User.name == 'john', User.name == 'jill')).\
            delete(synchronize_session='evaluate')
        assert john not in sess and jill not in sess
        sess.rollback()
        assert john in sess and jill in sess

    def test_delete_rollback_with_fetch(self):
        User = self.classes.User

        sess = Session()
        john, jack, jill, jane = sess.query(User).order_by(User.id).all()
        sess.query(User).filter(or_(User.name == 'john', User.name == 'jill')).\
            delete(synchronize_session='fetch')
        assert john not in sess and jill not in sess
        sess.rollback()
        assert john in sess and jill in sess

    def test_delete_without_session_sync(self):
        User = self.classes.User

        sess = Session()

        john, jack, jill, jane = sess.query(User).order_by(User.id).all()
        sess.query(User).filter(or_(User.name == 'john', User.name == 'jill')).\
            delete(synchronize_session=False)

        assert john in sess and jill in sess

        eq_(sess.query(User).order_by(User.id).all(), [jack, jane])

    def test_delete_with_fetch_strategy(self):
        User = self.classes.User

        sess = Session()

        john, jack, jill, jane = sess.query(User).order_by(User.id).all()
        sess.query(User).filter(or_(User.name == 'john', User.name == 'jill')).\
            delete(synchronize_session='fetch')

        assert john not in sess and jill not in sess

        eq_(sess.query(User).order_by(User.id).all(), [jack, jane])

    @testing.fails_on('mysql', 'FIXME: unknown')
    def test_delete_invalid_evaluation(self):
        User = self.classes.User

        sess = Session()

        john, jack, jill, jane = sess.query(User).order_by(User.id).all()

        assert_raises(exc.InvalidRequestError,
                      sess.query(User).
                      filter(
                          User.name == select([func.max(User.name)])).delete,
                      synchronize_session='evaluate'
                      )

        sess.query(User).filter(User.name == select([func.max(User.name)])).\
            delete(synchronize_session='fetch')

        assert john not in sess

        eq_(sess.query(User).order_by(User.id).all(), [jack, jill, jane])

    def test_update(self):
        User, users = self.classes.User, self.tables.users

        sess = Session()

        john, jack, jill, jane = sess.query(User).order_by(User.id).all()
        sess.query(User).filter(User.age > 29).\
            update({'age': User.age - 10}, synchronize_session='evaluate')

        eq_([john.age, jack.age, jill.age, jane.age], [25, 37, 29, 27])
        eq_(sess.query(User.age).order_by(
            User.id).all(), list(zip([25, 37, 29, 27])))

        sess.query(User).filter(User.age > 29).\
            update({User.age: User.age - 10}, synchronize_session='evaluate')
        eq_([john.age, jack.age, jill.age, jane.age], [25, 27, 29, 27])
        eq_(sess.query(User.age).order_by(
            User.id).all(), list(zip([25, 27, 29, 27])))

        sess.query(User).filter(User.age > 27).\
            update(
                {users.c.age: User.age - 10}, synchronize_session='evaluate')
        eq_([john.age, jack.age, jill.age, jane.age], [25, 27, 19, 27])
        eq_(sess.query(User.age).order_by(
            User.id).all(), list(zip([25, 27, 19, 27])))

        sess.query(User).filter(User.age == 25).\
            update({User.age: User.age - 10}, synchronize_session='fetch')
        eq_([john.age, jack.age, jill.age, jane.age], [15, 27, 19, 27])
        eq_(sess.query(User.age).order_by(
            User.id).all(), list(zip([15, 27, 19, 27])))

    def test_update_against_metadata(self):
        User, users = self.classes.User, self.tables.users

        sess = Session()

        sess.query(users).update({users.c.age: 29}, synchronize_session=False)
        eq_(sess.query(User.age).order_by(
            User.id).all(), list(zip([29, 29, 29, 29])))

    def test_update_with_bindparams(self):
        User = self.classes.User

        sess = Session()

        john, jack, jill, jane = sess.query(User).order_by(User.id).all()

        sess.query(User).filter('age > :x').params(x=29).\
            update({'age': User.age - 10}, synchronize_session='fetch')

        eq_([john.age, jack.age, jill.age, jane.age], [25, 37, 29, 27])
        eq_(sess.query(User.age).order_by(
            User.id).all(), list(zip([25, 37, 29, 27])))

    def test_update_without_load(self):
        User = self.classes.User

        sess = Session()

        sess.query(User).filter(User.id == 3).\
            update({'age': 44}, synchronize_session='fetch')
        eq_(sess.query(User.age).order_by(
            User.id).all(), list(zip([25, 47, 44, 37])))

    def test_update_changes_resets_dirty(self):
        User = self.classes.User

        sess = Session(autoflush=False)

        john, jack, jill, jane = sess.query(User).order_by(User.id).all()

        john.age = 50
        jack.age = 37

        # autoflush is false.  therefore our '50' and '37' are getting
        # blown away by this operation.

        sess.query(User).filter(User.age > 29).\
            update({'age': User.age - 10}, synchronize_session='evaluate')

        for x in (john, jack, jill, jane):
            assert not sess.is_modified(x)

        eq_([john.age, jack.age, jill.age, jane.age], [25, 37, 29, 27])

        john.age = 25
        assert john in sess.dirty
        assert jack in sess.dirty
        assert jill not in sess.dirty
        assert not sess.is_modified(john)
        assert not sess.is_modified(jack)

    def test_update_changes_with_autoflush(self):
        User = self.classes.User

        sess = Session()

        john, jack, jill, jane = sess.query(User).order_by(User.id).all()

        john.age = 50
        jack.age = 37

        sess.query(User).filter(User.age > 29).\
            update({'age': User.age - 10}, synchronize_session='evaluate')

        for x in (john, jack, jill, jane):
            assert not sess.is_modified(x)

        eq_([john.age, jack.age, jill.age, jane.age], [40, 27, 29, 27])

        john.age = 25
        assert john in sess.dirty
        assert jack not in sess.dirty
        assert jill not in sess.dirty
        assert sess.is_modified(john)
        assert not sess.is_modified(jack)

    def test_update_with_expire_strategy(self):
        User = self.classes.User

        sess = Session()

        john, jack, jill, jane = sess.query(User).order_by(User.id).all()
        sess.query(User).filter(User.age > 29).\
            update({'age': User.age - 10}, synchronize_session='fetch')

        eq_([john.age, jack.age, jill.age, jane.age], [25, 37, 29, 27])
        eq_(sess.query(User.age).order_by(
            User.id).all(), list(zip([25, 37, 29, 27])))

    @testing.fails_if(lambda: not testing.db.dialect.supports_sane_rowcount)
    def test_update_returns_rowcount(self):
        User = self.classes.User

        sess = Session()

        rowcount = sess.query(User).filter(
            User.age > 29).update({'age': User.age + 0})
        eq_(rowcount, 2)

        rowcount = sess.query(User).filter(
            User.age > 29).update({'age': User.age - 10})
        eq_(rowcount, 2)

    @testing.fails_if(lambda: not testing.db.dialect.supports_sane_rowcount)
    def test_delete_returns_rowcount(self):
        User = self.classes.User

        sess = Session()

        rowcount = sess.query(User).filter(User.age > 26).\
            delete(synchronize_session=False)
        eq_(rowcount, 3)

    def test_update_all(self):
        User = self.classes.User

        sess = Session()

        john, jack, jill, jane = sess.query(User).order_by(User.id).all()
        sess.query(User).update({'age': 42}, synchronize_session='evaluate')

        eq_([john.age, jack.age, jill.age, jane.age], [42, 42, 42, 42])
        eq_(sess.query(User.age).order_by(
            User.id).all(), list(zip([42, 42, 42, 42])))

    def test_delete_all(self):
        User = self.classes.User

        sess = Session()

        john, jack, jill, jane = sess.query(User).order_by(User.id).all()
        sess.query(User).delete(synchronize_session='evaluate')

        assert not (
            john in sess or jack in sess or jill in sess or jane in sess)
        eq_(sess.query(User).count(), 0)

    def test_autoflush_before_evaluate_update(self):
        User = self.classes.User

        sess = Session()
        john = sess.query(User).filter_by(name='john').one()
        john.name = 'j2'

        sess.query(User).filter_by(name='j2').\
            update({'age': 42},
                   synchronize_session='evaluate')
        eq_(john.age, 42)

    def test_autoflush_before_fetch_update(self):
        User = self.classes.User

        sess = Session()
        john = sess.query(User).filter_by(name='john').one()
        john.name = 'j2'

        sess.query(User).filter_by(name='j2').\
            update({'age': 42},
                   synchronize_session='fetch')
        eq_(john.age, 42)

    def test_autoflush_before_evaluate_delete(self):
        User = self.classes.User

        sess = Session()
        john = sess.query(User).filter_by(name='john').one()
        john.name = 'j2'

        sess.query(User).filter_by(name='j2').\
            delete(
            synchronize_session='evaluate')
        assert john not in sess

    def test_autoflush_before_fetch_delete(self):
        User = self.classes.User

        sess = Session()
        john = sess.query(User).filter_by(name='john').one()
        john.name = 'j2'

        sess.query(User).filter_by(name='j2').\
            delete(
            synchronize_session='fetch')
        assert john not in sess

    def test_evaluate_before_update(self):
        User = self.classes.User

        sess = Session()
        john = sess.query(User).filter_by(name='john').one()
        sess.expire(john, ['age'])

        # eval must be before the update.  otherwise
        # we eval john, age has been expired and doesn't
        # match the new value coming in
        sess.query(User).filter_by(name='john').filter_by(age=25).\
            update({'name': 'j2', 'age': 40},
                   synchronize_session='evaluate')
        eq_(john.name, 'j2')
        eq_(john.age, 40)

    def test_fetch_before_update(self):
        User = self.classes.User

        sess = Session()
        john = sess.query(User).filter_by(name='john').one()
        sess.expire(john, ['age'])

        sess.query(User).filter_by(name='john').filter_by(age=25).\
            update({'name': 'j2', 'age': 40},
                   synchronize_session='fetch')
        eq_(john.name, 'j2')
        eq_(john.age, 40)

    def test_evaluate_before_delete(self):
        User = self.classes.User

        sess = Session()
        john = sess.query(User).filter_by(name='john').one()
        sess.expire(john, ['age'])

        sess.query(User).filter_by(name='john').\
            filter_by(age=25).\
            delete(
            synchronize_session='evaluate')
        assert john not in sess

    def test_fetch_before_delete(self):
        User = self.classes.User

        sess = Session()
        john = sess.query(User).filter_by(name='john').one()
        sess.expire(john, ['age'])

        sess.query(User).filter_by(name='john').\
            filter_by(age=25).\
            delete(
            synchronize_session='fetch')
        assert john not in sess


class UpdateDeleteIgnoresLoadersTest(fixtures.MappedTest):

    @classmethod
    def define_tables(cls, metadata):
        Table('users', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('name', String(32)),
              Column('age', Integer))

        Table('documents', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('user_id', None, ForeignKey('users.id')),
              Column('title', String(32)))

    @classmethod
    def setup_classes(cls):
        class User(cls.Comparable):
            pass

        class Document(cls.Comparable):
            pass

    @classmethod
    def insert_data(cls):
        users = cls.tables.users

        users.insert().execute([
            dict(id=1, name='john', age=25),
            dict(id=2, name='jack', age=47),
            dict(id=3, name='jill', age=29),
            dict(id=4, name='jane', age=37),
        ])

        documents = cls.tables.documents

        documents.insert().execute([
            dict(id=1, user_id=1, title='foo'),
            dict(id=2, user_id=1, title='bar'),
            dict(id=3, user_id=2, title='baz'),
        ])

    @classmethod
    def setup_mappers(cls):
        documents, Document, User, users = (cls.tables.documents,
                                            cls.classes.Document,
                                            cls.classes.User,
                                            cls.tables.users)

        mapper(User, users)
        mapper(Document, documents, properties={
            'user': relationship(User, lazy='joined',
                                 backref=backref('documents', lazy='select'))
        })

    def test_update_with_eager_relationships(self):
        Document = self.classes.Document

        sess = Session()

        foo, bar, baz = sess.query(Document).order_by(Document.id).all()
        sess.query(Document).filter(Document.user_id == 1).\
            update({'title': Document.title + Document.title},
                   synchronize_session='fetch')

        eq_([foo.title, bar.title, baz.title], ['foofoo', 'barbar', 'baz'])
        eq_(sess.query(Document.title).order_by(Document.id).all(),
            list(zip(['foofoo', 'barbar', 'baz'])))

    def test_update_with_explicit_joinedload(self):
        User = self.classes.User

        sess = Session()

        john, jack, jill, jane = sess.query(User).order_by(User.id).all()
        sess.query(User).options(joinedload(User.documents)).filter(User.age > 29).\
            update({'age': User.age - 10}, synchronize_session='fetch')

        eq_([john.age, jack.age, jill.age, jane.age], [25, 37, 29, 27])
        eq_(sess.query(User.age).order_by(
            User.id).all(), list(zip([25, 37, 29, 27])))

    def test_delete_with_eager_relationships(self):
        Document = self.classes.Document

        sess = Session()

        sess.query(Document).filter(Document.user_id == 1).\
            delete(synchronize_session=False)

        eq_(sess.query(Document.title).all(), list(zip(['baz'])))


class UpdateDeleteFromTest(fixtures.MappedTest):

    @classmethod
    def define_tables(cls, metadata):
        Table('users', metadata,
              Column('id', Integer, primary_key=True),
              Column('samename', String(10)),
              )
        Table('documents', metadata,
              Column('id', Integer, primary_key=True),
              Column('user_id', None, ForeignKey('users.id')),
              Column('title', String(32)),
              Column('flag', Boolean),
              Column('samename', String(10)),
              )

    @classmethod
    def setup_classes(cls):
        class User(cls.Comparable):
            pass

        class Document(cls.Comparable):
            pass

    @classmethod
    def insert_data(cls):
        users = cls.tables.users

        users.insert().execute([
            dict(id=1, ),
            dict(id=2, ),
            dict(id=3, ),
            dict(id=4, ),
        ])

        documents = cls.tables.documents

        documents.insert().execute([
            dict(id=1, user_id=1, title='foo'),
            dict(id=2, user_id=1, title='bar'),
            dict(id=3, user_id=2, title='baz'),
            dict(id=4, user_id=2, title='hoho'),
            dict(id=5, user_id=3, title='lala'),
            dict(id=6, user_id=3, title='bleh'),
        ])

    @classmethod
    def setup_mappers(cls):
        documents, Document, User, users = (cls.tables.documents,
                                            cls.classes.Document,
                                            cls.classes.User,
                                            cls.tables.users)

        mapper(User, users)
        mapper(Document, documents, properties={
            'user': relationship(User, backref='documents')
        })

    @testing.requires.update_from
    def test_update_from_joined_subq_test(self):
        Document = self.classes.Document
        s = Session()

        subq = s.query(func.max(Document.title).label('title')).\
            group_by(Document.user_id).subquery()

        s.query(Document).filter(Document.title == subq.c.title).\
            update({'flag': True}, synchronize_session=False)

        eq_(
            set(s.query(Document.id, Document.flag)),
            set([
                (1, True), (2, None),
                (3, None), (4, True),
                (5, True), (6, None),
            ])
        )

    def test_no_eval_against_multi_table_criteria(self):
        User = self.classes.User
        Document = self.classes.Document

        s = Session()

        q = s.query(User).filter(User.id == Document.user_id)
        assert_raises_message(
            exc.SAWarning,
            r"Can't do in-Python evaluation of criteria against alternate "
            r"class .*Document.*; "
            "expiration of objects will not be accurate "
            "and/or may fail.  synchronize_session should be set to "
            "False or 'fetch'. "
            "This warning will be an exception "
            "in 1.0.",
            q.update,
            {"name": "ed"}
        )

    @testing.requires.update_where_target_in_subquery
    def test_update_using_in(self):
        Document = self.classes.Document
        s = Session()

        subq = s.query(func.max(Document.title).label('title')).\
            group_by(Document.user_id).subquery()

        s.query(Document).filter(Document.title.in_(subq)).\
            update({'flag': True}, synchronize_session=False)

        eq_(
            set(s.query(Document.id, Document.flag)),
            set([
                (1, True), (2, None),
                (3, None), (4, True),
                (5, True), (6, None),
            ])
        )

    @testing.requires.update_where_target_in_subquery
    @testing.requires.standalone_binds
    def test_update_using_case(self):
        Document = self.classes.Document
        s = Session()

        subq = s.query(func.max(Document.title).label('title')).\
            group_by(Document.user_id).subquery()

        # this would work with Firebird if you do literal_column('1')
        # instead
        case_stmt = case([(Document.title.in_(subq), True)], else_=False)
        s.query(Document).update(
            {'flag': case_stmt}, synchronize_session=False)

        eq_(
            set(s.query(Document.id, Document.flag)),
            set([
                (1, True), (2, False),
                (3, False), (4, True),
                (5, True), (6, False),
            ])
        )

    @testing.only_on('mysql', 'Multi table update')
    def test_update_from_multitable_same_names(self):
        Document = self.classes.Document
        User = self.classes.User

        s = Session()

        s.query(Document).\
            filter(User.id == Document.user_id).\
            filter(User.id == 2).update({
                Document.samename: 'd_samename',
                User.samename: 'u_samename'
            }, synchronize_session=False
        )
        eq_(
            s.query(User.id, Document.samename, User.samename).
            filter(User.id == Document.user_id).
            order_by(User.id).all(),
            [
                (1, None, None),
                (1, None, None),
                (2, 'd_samename', 'u_samename'),
                (2, 'd_samename', 'u_samename'),
                (3, None, None),
                (3, None, None),
            ]
        )


class ExpressionUpdateTest(fixtures.MappedTest):

    @classmethod
    def define_tables(cls, metadata):
        data = Table('data', metadata,
                     Column('id', Integer, primary_key=True,
                            test_needs_autoincrement=True),
                     Column('counter', Integer, nullable=False, default=0)
                     )

    @classmethod
    def setup_classes(cls):
        class Data(cls.Comparable):
            pass

    @classmethod
    def setup_mappers(cls):
        data = cls.tables.data
        mapper(cls.classes.Data, data, properties={'cnt': data.c.counter})

    @testing.provide_metadata
    def test_update_attr_names(self):
        Data = self.classes.Data

        d1 = Data()
        sess = Session()
        sess.add(d1)
        sess.commit()
        eq_(d1.cnt, 0)

        sess.query(Data).update({Data.cnt: Data.cnt + 1})
        sess.flush()

        eq_(d1.cnt, 1)

        sess.query(Data).update({Data.cnt: Data.cnt + 1}, 'fetch')
        sess.flush()

        eq_(d1.cnt, 2)
        sess.close()


class InheritTest(fixtures.DeclarativeMappedTest):

    run_inserts = 'each'

    run_deletes = 'each'
    __backend__ = True

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class Person(Base):
            __tablename__ = 'person'
            id = Column(
                Integer, primary_key=True, test_needs_autoincrement=True)
            type = Column(String(50))
            name = Column(String(50))

        class Engineer(Person):
            __tablename__ = 'engineer'
            id = Column(Integer, ForeignKey('person.id'), primary_key=True)
            engineer_name = Column(String(50))

        class Manager(Person):
            __tablename__ = 'manager'
            id = Column(Integer, ForeignKey('person.id'), primary_key=True)
            manager_name = Column(String(50))

    @classmethod
    def insert_data(cls):
        Engineer, Person, Manager = cls.classes.Engineer, \
            cls.classes.Person, cls.classes.Manager
        s = Session(testing.db)
        s.add_all([
            Engineer(name='e1', engineer_name='e1'),
            Manager(name='m1', manager_name='m1'),
            Engineer(name='e2', engineer_name='e2'),
            Person(name='p1'),
        ])
        s.commit()

    def test_illegal_metadata(self):
        person = self.classes.Person.__table__
        engineer = self.classes.Engineer.__table__

        sess = Session()
        assert_raises_message(
            exc.InvalidRequestError,
            "This operation requires only one Table or entity be "
            "specified as the target.",
            sess.query(person.join(engineer)).update, {}
        )

    def test_update_subtable_only(self):
        Engineer = self.classes.Engineer
        s = Session(testing.db)
        s.query(Engineer).update({'engineer_name': 'e5'})

        eq_(
            s.query(Engineer.engineer_name).all(),
            [('e5', ), ('e5', )]
        )

    @testing.requires.update_from
    def test_update_from(self):
        Engineer = self.classes.Engineer
        Person = self.classes.Person
        s = Session(testing.db)
        s.query(Engineer).filter(Engineer.id == Person.id).\
            filter(Person.name == 'e2').update({'engineer_name': 'e5'})

        eq_(
            set(s.query(Person.name, Engineer.engineer_name)),
            set([('e1', 'e1', ), ('e2', 'e5')])
        )

    @testing.only_on('mysql', 'Multi table update')
    def test_update_from_multitable(self):
        Engineer = self.classes.Engineer
        Person = self.classes.Person
        s = Session(testing.db)
        s.query(Engineer).filter(Engineer.id == Person.id).\
            filter(Person.name == 'e2').update({Person.name: 'e22',
                                                Engineer.engineer_name: 'e55'})

        eq_(
            set(s.query(Person.name, Engineer.engineer_name)),
            set([('e1', 'e1', ), ('e22', 'e55')])
        )
