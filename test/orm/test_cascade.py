
from sqlalchemy.test.testing import assert_raises, assert_raises_message
from sqlalchemy import Integer, String, ForeignKey, Sequence, \
    exc as sa_exc
from sqlalchemy.test.schema import Table, Column
from sqlalchemy.orm import mapper, relationship, create_session, \
    sessionmaker, class_mapper, backref
from sqlalchemy.orm import attributes, exc as orm_exc
from sqlalchemy.test import testing
from sqlalchemy.test.testing import eq_
from test.orm import _base, _fixtures


class O2MCascadeTest(_fixtures.FixtureTest):
    run_inserts = None

    @classmethod
    @testing.resolve_artifact_names
    def setup_mappers(cls):
        mapper(Address, addresses)
        mapper(User, users,
               properties=dict(addresses=relationship(Address,
               cascade='all, delete-orphan', backref='user'),
               orders=relationship(mapper(Order, orders),
               cascade='all, delete-orphan', order_by=orders.c.id)))
        mapper(Dingaling, dingalings, properties={'address'
               : relationship(Address)})

    @testing.resolve_artifact_names
    def test_list_assignment(self):
        sess = create_session()
        u = User(name='jack', orders=[
                 Order(description='someorder'),
                 Order(description='someotherorder')])
        sess.add(u)
        sess.flush()
        sess.expunge_all()

        u = sess.query(User).get(u.id)
        eq_(u, User(name='jack',
                    orders=[Order(description='someorder'),
                            Order(description='someotherorder')]))

        u.orders=[Order(description="order 3"), Order(description="order 4")]
        sess.flush()
        sess.expunge_all()

        u = sess.query(User).get(u.id)
        eq_(u, User(name='jack',
                    orders=[Order(description="order 3"),
                            Order(description="order 4")]))

        eq_(sess.query(Order).order_by(Order.id).all(),
            [Order(description="order 3"), Order(description="order 4")])

        o5 = Order(description="order 5")
        sess.add(o5)
        assert_raises_message(orm_exc.FlushError, "is an orphan", sess.flush)

    @testing.resolve_artifact_names
    def test_save_update_sends_pending(self):
        """test that newly added and deleted collection items are
        cascaded on save-update"""

        sess = sessionmaker(expire_on_commit=False)()
        o1, o2, o3 = Order(description='o1'), Order(description='o2'), \
            Order(description='o3')
        u = User(name='jack', orders=[o1, o2])
        sess.add(u)
        sess.commit()
        sess.close()
        u.orders.append(o3)
        u.orders.remove(o1)
        sess.add(u)
        assert o1 in sess
        assert o2 in sess
        assert o3 in sess
        sess.commit()

    
    @testing.resolve_artifact_names
    def test_delete(self):
        sess = create_session()
        u = User(name='jack',
                 orders=[Order(description='someorder'),
                         Order(description='someotherorder')])
        sess.add(u)
        sess.flush()

        sess.delete(u)
        sess.flush()
        assert users.count().scalar() == 0
        assert orders.count().scalar() == 0

    @testing.resolve_artifact_names
    def test_delete_unloaded_collections(self):
        """Unloaded collections are still included in a delete-cascade
        by default."""
        sess = create_session()
        u = User(name='jack',
                 addresses=[Address(email_address="address1"),
                            Address(email_address="address2")])
        sess.add(u)
        sess.flush()
        sess.expunge_all()
        assert addresses.count().scalar() == 2
        assert users.count().scalar() == 1

        u = sess.query(User).get(u.id)

        assert 'addresses' not in u.__dict__
        sess.delete(u)
        sess.flush()
        assert addresses.count().scalar() == 0
        assert users.count().scalar() == 0

    @testing.resolve_artifact_names
    def test_cascades_onlycollection(self):
        """Cascade only reaches instances that are still part of the
        collection, not those that have been removed"""

        sess = create_session()
        u = User(name='jack',
                 orders=[Order(description='someorder'),
                         Order(description='someotherorder')])
        sess.add(u)
        sess.flush()

        o = u.orders[0]
        del u.orders[0]
        sess.delete(u)
        assert u in sess.deleted
        assert o not in sess.deleted
        assert o in sess

        u2 = User(name='newuser', orders=[o])
        sess.add(u2)
        sess.flush()
        sess.expunge_all()
        assert users.count().scalar() == 1
        assert orders.count().scalar() == 1
        eq_(sess.query(User).all(),
            [User(name='newuser',
                  orders=[Order(description='someorder')])])

    @testing.resolve_artifact_names
    def test_cascade_nosideeffects(self):
        """test that cascade leaves the state of unloaded
        scalars/collections unchanged."""
        
        sess = create_session()
        u = User(name='jack')
        sess.add(u)
        assert 'orders' not in u.__dict__

        sess.flush()
        
        assert 'orders' not in u.__dict__

        a = Address(email_address='foo@bar.com')
        sess.add(a)
        assert 'user' not in a.__dict__
        a.user = u
        sess.flush()
        
        d = Dingaling(data='d1')
        d.address_id = a.id
        sess.add(d)
        assert 'address' not in d.__dict__
        sess.flush()
        assert d.address is a
        
    @testing.resolve_artifact_names
    def test_cascade_delete_plusorphans(self):
        sess = create_session()
        u = User(name='jack',
                 orders=[Order(description='someorder'),
                         Order(description='someotherorder')])
        sess.add(u)
        sess.flush()
        assert users.count().scalar() == 1
        assert orders.count().scalar() == 2

        del u.orders[0]
        sess.delete(u)
        sess.flush()
        assert users.count().scalar() == 0
        assert orders.count().scalar() == 0

    @testing.resolve_artifact_names
    def test_collection_orphans(self):
        sess = create_session()
        u = User(name='jack',
                 orders=[Order(description='someorder'),
                         Order(description='someotherorder')])
        sess.add(u)
        sess.flush()

        assert users.count().scalar() == 1
        assert orders.count().scalar() == 2

        u.orders[:] = []

        sess.flush()

        assert users.count().scalar() == 1
        assert orders.count().scalar() == 0

class O2OCascadeTest(_fixtures.FixtureTest):
    run_inserts = None
    
    @classmethod
    @testing.resolve_artifact_names
    def setup_mappers(cls):
        mapper(Address, addresses)
        mapper(User, users, properties={'address'
               : relationship(Address, backref=backref('user',
               single_parent=True), uselist=False)})

    @testing.resolve_artifact_names
    def test_single_parent_raise(self):
        a1 = Address(email_address='some address')
        u1 = User(name='u1', address=a1)
        assert_raises(sa_exc.InvalidRequestError, Address,
                      email_address='asd', user=u1)
        a2 = Address(email_address='asd')
        u1.address = a2
        assert u1.address is not a1
        assert a1.user is None
        
        
        
class O2MBackrefTest(_fixtures.FixtureTest):
    run_inserts = None

    @classmethod
    @testing.resolve_artifact_names
    def setup_mappers(cls):
        mapper(User, users,
               properties=dict(orders=relationship(mapper(Order,
               orders), cascade='all, delete-orphan', backref='user')))

    @testing.resolve_artifact_names
    def test_lazyload_bug(self):
        sess = create_session()

        u = User(name="jack")
        sess.add(u)
        sess.expunge(u)

        o1 = Order(description='someorder')
        o1.user = u
        sess.add(u)
        assert u in sess
        assert o1 in sess


class NoSaveCascadeTest(_fixtures.FixtureTest):
    """test that backrefs don't force save-update cascades to occur
    when the cascade initiated from the forwards side."""
    
    @testing.resolve_artifact_names
    def test_unidirectional_cascade_o2m(self):
        mapper(Order, orders)
        mapper(User, users, properties = dict(
            orders = relationship(
                Order, backref=backref("user", cascade=None))
        ))
        
        sess = create_session()
        
        o1 = Order()
        sess.add(o1)
        u1 = User(orders=[o1])
        assert u1 not in sess
        assert o1 in sess
        
        sess.expunge_all()
        
        o1 = Order()
        u1 = User(orders=[o1])
        sess.add(o1)
        assert u1 not in sess
        assert o1 in sess

    @testing.resolve_artifact_names
    def test_unidirectional_cascade_m2o(self):
        mapper(Order, orders, properties={
            'user':relationship(User, backref=backref("orders", cascade=None))
        })
        mapper(User, users)
        
        sess = create_session()
        
        u1 = User()
        sess.add(u1)
        o1 = Order()
        o1.user = u1
        assert o1 not in sess
        assert u1 in sess
        
        sess.expunge_all()

        u1 = User()
        o1 = Order()
        o1.user = u1
        sess.add(u1)
        assert o1 not in sess
        assert u1 in sess

    @testing.resolve_artifact_names
    def test_unidirectional_cascade_m2m(self):
        mapper(Item, items, properties={'keywords'
               : relationship(Keyword, secondary=item_keywords,
               cascade='none', backref='items')})
        mapper(Keyword, keywords)

        sess = create_session()

        i1 = Item()
        k1 = Keyword()
        sess.add(i1)
        i1.keywords.append(k1)
        assert i1 in sess
        assert k1 not in sess
        
        sess.expunge_all()
        
        i1 = Item()
        k1 = Keyword()
        sess.add(i1)
        k1.items.append(i1)
        assert i1 in sess
        assert k1 not in sess
        
    
class O2MCascadeNoOrphanTest(_fixtures.FixtureTest):
    run_inserts = None

    @classmethod
    @testing.resolve_artifact_names
    def setup_mappers(cls):
        mapper(User, users, properties = dict(
            orders = relationship(
                mapper(Order, orders), cascade="all")
        ))

    @testing.resolve_artifact_names
    def test_cascade_delete_noorphans(self):
        sess = create_session()
        u = User(name='jack',
                 orders=[Order(description='someorder'),
                         Order(description='someotherorder')])
        sess.add(u)
        sess.flush()
        assert users.count().scalar() == 1
        assert orders.count().scalar() == 2

        del u.orders[0]
        sess.delete(u)
        sess.flush()
        assert users.count().scalar() == 0
        assert orders.count().scalar() == 1


class M2OCascadeTest(_base.MappedTest):

    @classmethod
    def define_tables(cls, metadata):
        Table('extra', metadata, Column('id', Integer,
              primary_key=True, test_needs_autoincrement=True),
              Column('prefs_id', Integer, ForeignKey('prefs.id')))
        Table('prefs', metadata, Column('id', Integer,
              primary_key=True, test_needs_autoincrement=True),
              Column('data', String(40)))
        Table(
            'users',
            metadata,
            Column('id', Integer, primary_key=True,
                   test_needs_autoincrement=True),
            Column('name', String(40)),
            Column('pref_id', Integer, ForeignKey('prefs.id')),
            Column('foo_id', Integer, ForeignKey('foo.id')),
            )
        Table('foo', metadata, Column('id', Integer, primary_key=True,
              test_needs_autoincrement=True), Column('data',
              String(40)))

    @classmethod
    def setup_classes(cls):
        class User(_fixtures.Base):
            pass
        class Pref(_fixtures.Base):
            pass
        class Extra(_fixtures.Base):
            pass
        class Foo(_fixtures.Base):
            pass
            
    @classmethod
    @testing.resolve_artifact_names
    def setup_mappers(cls):
        mapper(Extra, extra)
        mapper(Pref, prefs, properties=dict(extra=relationship(Extra,
               cascade='all, delete')))
        mapper(User, users, properties=dict(pref=relationship(Pref,
               lazy='joined', cascade='all, delete-orphan',
               single_parent=True), foo=relationship(Foo)))  # straight m2o
        mapper(Foo, foo)

    @classmethod
    @testing.resolve_artifact_names
    def insert_data(cls):
        u1 = User(name='ed', pref=Pref(data="pref 1", extra=[Extra()]))
        u2 = User(name='jack', pref=Pref(data="pref 2", extra=[Extra()]))
        u3 = User(name="foo", pref=Pref(data="pref 3", extra=[Extra()]))
        sess = create_session()
        sess.add_all((u1, u2, u3))
        sess.flush()
        sess.close()

    @testing.fails_on('maxdb', 'FIXME: unknown')
    @testing.resolve_artifact_names
    def test_orphan(self):
        sess = create_session()
        assert prefs.count().scalar() == 3
        assert extra.count().scalar() == 3
        jack = sess.query(User).filter_by(name="jack").one()
        jack.pref = None
        sess.flush()
        assert prefs.count().scalar() == 2
        assert extra.count().scalar() == 2

    @testing.resolve_artifact_names
    def test_cascade_on_deleted(self):
        """test a bug introduced by r6711"""

        sess = sessionmaker(expire_on_commit=True)()
        
        
        u1 = User(name='jack', foo=Foo(data='f1'))
        sess.add(u1)
        sess.commit()

        u1.foo = None

        # the error condition relies upon
        # these things being true
        assert User.foo.impl.active_history is False
        eq_(
            attributes.get_history(u1, 'foo'),
            ([None], (), [attributes.PASSIVE_NO_RESULT])
        )
        
        sess.add(u1)
        assert u1 in sess
        sess.commit()

    @testing.resolve_artifact_names
    def test_save_update_sends_pending(self):
        """test that newly added and deleted scalar items are cascaded
        on save-update"""

        sess = sessionmaker(expire_on_commit=False)()
        p1, p2 = Pref(data='p1'), Pref(data='p2')
        
        
        u = User(name='jack', pref=p1)
        sess.add(u)
        sess.commit()
        sess.close()

        u.pref = p2
        
        sess.add(u)
        assert p1 in sess
        assert p2 in sess
        sess.commit()

    @testing.fails_on('maxdb', 'FIXME: unknown')
    @testing.resolve_artifact_names
    def test_orphan_on_update(self):
        sess = create_session()
        jack = sess.query(User).filter_by(name="jack").one()
        p = jack.pref
        e = jack.pref.extra[0]
        sess.expunge_all()

        jack.pref = None
        sess.add(jack)
        sess.add(p)
        sess.add(e)
        assert p in sess
        assert e in sess
        sess.flush()
        assert prefs.count().scalar() == 2
        assert extra.count().scalar() == 2

    @testing.resolve_artifact_names
    def test_pending_expunge(self):
        sess = create_session()
        someuser = User(name='someuser')
        sess.add(someuser)
        sess.flush()
        someuser.pref = p1 = Pref(data='somepref')
        assert p1 in sess
        someuser.pref = Pref(data='someotherpref')
        assert p1 not in sess
        sess.flush()
        eq_(sess.query(Pref).with_parent(someuser).all(),
            [Pref(data="someotherpref")])

    @testing.resolve_artifact_names
    def test_double_assignment(self):
        """Double assignment will not accidentally reset the 'parent' flag."""

        sess = create_session()
        jack = sess.query(User).filter_by(name="jack").one()

        newpref = Pref(data="newpref")
        jack.pref = newpref
        jack.pref = newpref
        sess.flush()
        eq_(sess.query(Pref).order_by(Pref.id).all(),
            [Pref(data="pref 1"), Pref(data="pref 3"), Pref(data="newpref")])

class M2OCascadeDeleteTest(_base.MappedTest):

    @classmethod
    def define_tables(cls, metadata):
        Table('t1', metadata, Column('id', Integer, primary_key=True,
              test_needs_autoincrement=True), 
              Column('data',String(50)), 
              Column('t2id', Integer, ForeignKey('t2.id')))
              
        Table('t2', metadata, 
            Column('id', Integer, primary_key=True,
              test_needs_autoincrement=True), 
              Column('data',String(50)), 
              Column('t3id', Integer, ForeignKey('t3.id')))
              
        Table('t3', metadata, 
            Column('id', Integer, primary_key=True,
              test_needs_autoincrement=True), 
              Column('data', String(50)))

    @classmethod
    def setup_classes(cls):
        class T1(_fixtures.Base):
            pass
        class T2(_fixtures.Base):
            pass
        class T3(_fixtures.Base):
            pass

    @classmethod
    @testing.resolve_artifact_names
    def setup_mappers(cls):
        mapper(T1, t1, properties={'t2': relationship(T2, cascade="all")})
        mapper(T2, t2, properties={'t3': relationship(T3, cascade="all")})
        mapper(T3, t3)

    @testing.resolve_artifact_names
    def test_cascade_delete(self):
        sess = create_session()
        x = T1(data='t1a', t2=T2(data='t2a', t3=T3(data='t3a')))
        sess.add(x)
        sess.flush()

        sess.delete(x)
        sess.flush()
        eq_(sess.query(T1).all(), [])
        eq_(sess.query(T2).all(), [])
        eq_(sess.query(T3).all(), [])

    @testing.resolve_artifact_names
    def test_cascade_delete_postappend_onelevel(self):
        sess = create_session()
        x1 = T1(data='t1', )
        x2 = T2(data='t2')
        x3 = T3(data='t3')
        sess.add_all((x1, x2, x3))
        sess.flush()

        sess.delete(x1)
        x1.t2 = x2
        x2.t3 = x3
        sess.flush()
        eq_(sess.query(T1).all(), [])
        eq_(sess.query(T2).all(), [])
        eq_(sess.query(T3).all(), [])

    @testing.resolve_artifact_names
    def test_cascade_delete_postappend_twolevel(self):
        sess = create_session()
        x1 = T1(data='t1', t2=T2(data='t2'))
        x3 = T3(data='t3')
        sess.add_all((x1, x3))
        sess.flush()

        sess.delete(x1)
        x1.t2.t3 = x3
        sess.flush()
        eq_(sess.query(T1).all(), [])
        eq_(sess.query(T2).all(), [])
        eq_(sess.query(T3).all(), [])

    @testing.resolve_artifact_names
    def test_preserves_orphans_onelevel(self):
        sess = create_session()
        x2 = T1(data='t1b', t2=T2(data='t2b', t3=T3(data='t3b')))
        sess.add(x2)
        sess.flush()
        x2.t2 = None

        sess.delete(x2)
        sess.flush()
        eq_(sess.query(T1).all(), [])
        eq_(sess.query(T2).all(), [T2()])
        eq_(sess.query(T3).all(), [T3()])

    @testing.future
    @testing.resolve_artifact_names
    def test_preserves_orphans_onelevel_postremove(self):
        sess = create_session()
        x2 = T1(data='t1b', t2=T2(data='t2b', t3=T3(data='t3b')))
        sess.add(x2)
        sess.flush()

        sess.delete(x2)
        x2.t2 = None
        sess.flush()
        eq_(sess.query(T1).all(), [])
        eq_(sess.query(T2).all(), [T2()])
        eq_(sess.query(T3).all(), [T3()])

    @testing.resolve_artifact_names
    def test_preserves_orphans_twolevel(self):
        sess = create_session()
        x = T1(data='t1a', t2=T2(data='t2a', t3=T3(data='t3a')))
        sess.add(x)
        sess.flush()

        x.t2.t3 = None
        sess.delete(x)
        sess.flush()
        eq_(sess.query(T1).all(), [])
        eq_(sess.query(T2).all(), [])
        eq_(sess.query(T3).all(), [T3()])


class M2OCascadeDeleteOrphanTest(_base.MappedTest):

    @classmethod
    def define_tables(cls, metadata):
        Table('t1', metadata,
              Column('id', Integer, primary_key=True,
                            test_needs_autoincrement=True),
              Column('data', String(50)),
              Column('t2id', Integer, ForeignKey('t2.id')))
              
        Table('t2', metadata,
              Column('id', Integer, primary_key=True,
                                test_needs_autoincrement=True),
              Column('data', String(50)),
              Column('t3id', Integer, ForeignKey('t3.id')))
              
        Table('t3', metadata,
              Column('id', Integer, primary_key=True,
                                test_needs_autoincrement=True),
              Column('data', String(50)))

    @classmethod
    def setup_classes(cls):
        class T1(_fixtures.Base):
            pass
        class T2(_fixtures.Base):
            pass
        class T3(_fixtures.Base):
            pass

    @classmethod
    @testing.resolve_artifact_names
    def setup_mappers(cls):
        mapper(T1, t1, properties=dict(t2=relationship(T2,
               cascade='all, delete-orphan', single_parent=True)))
        mapper(T2, t2, properties=dict(t3=relationship(T3,
               cascade='all, delete-orphan', single_parent=True,
               backref=backref('t2', uselist=False))))
        mapper(T3, t3)

    @testing.resolve_artifact_names
    def test_cascade_delete(self):
        sess = create_session()
        x = T1(data='t1a', t2=T2(data='t2a', t3=T3(data='t3a')))
        sess.add(x)
        sess.flush()

        sess.delete(x)
        sess.flush()
        eq_(sess.query(T1).all(), [])
        eq_(sess.query(T2).all(), [])
        eq_(sess.query(T3).all(), [])

    @testing.resolve_artifact_names
    def test_deletes_orphans_onelevel(self):
        sess = create_session()
        x2 = T1(data='t1b', t2=T2(data='t2b', t3=T3(data='t3b')))
        sess.add(x2)
        sess.flush()
        x2.t2 = None

        sess.delete(x2)
        sess.flush()
        eq_(sess.query(T1).all(), [])
        eq_(sess.query(T2).all(), [])
        eq_(sess.query(T3).all(), [])

    @testing.resolve_artifact_names
    def test_deletes_orphans_twolevel(self):
        sess = create_session()
        x = T1(data='t1a', t2=T2(data='t2a', t3=T3(data='t3a')))
        sess.add(x)
        sess.flush()

        x.t2.t3 = None
        sess.delete(x)
        sess.flush()
        eq_(sess.query(T1).all(), [])
        eq_(sess.query(T2).all(), [])
        eq_(sess.query(T3).all(), [])

    @testing.resolve_artifact_names
    def test_finds_orphans_twolevel(self):
        sess = create_session()
        x = T1(data='t1a', t2=T2(data='t2a', t3=T3(data='t3a')))
        sess.add(x)
        sess.flush()

        x.t2.t3 = None
        sess.flush()
        eq_(sess.query(T1).all(), [T1()])
        eq_(sess.query(T2).all(), [T2()])
        eq_(sess.query(T3).all(), [])

    @testing.resolve_artifact_names
    def test_single_parent_raise(self):

        sess = create_session()
        
        y = T2(data='T2a')
        x = T1(data='T1a', t2=y)
        assert_raises(sa_exc.InvalidRequestError, T1, data='T1b', t2=y)

    @testing.resolve_artifact_names
    def test_single_parent_backref(self):

        sess = create_session()
        
        y = T3(data='T3a')
        x = T2(data='T2a', t3=y)

        # cant attach the T3 to another T2
        assert_raises(sa_exc.InvalidRequestError, T2, data='T2b', t3=y)
        
        # set via backref tho is OK, unsets from previous parent
        # first
        z = T2(data='T2b')
        y.t2 = z

        assert z.t3 is y
        assert x.t3 is None

class M2MCascadeTest(_base.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table('a', metadata,
            Column('id', Integer, primary_key=True,
                            test_needs_autoincrement=True),
            Column('data', String(30)),
            test_needs_fk=True
            )
        Table('b', metadata,
            Column('id', Integer, primary_key=True,
                            test_needs_autoincrement=True),
            Column('data', String(30)),
            test_needs_fk=True
            
            )
        Table('atob', metadata,
            Column('aid', Integer, ForeignKey('a.id')),
            Column('bid', Integer, ForeignKey('b.id')),
            test_needs_fk=True
            
            )
        Table('c', metadata,
              Column('id', Integer, primary_key=True,
                            test_needs_autoincrement=True),
              Column('data', String(30)),
              Column('bid', Integer, ForeignKey('b.id')),
              test_needs_fk=True
              
              )

    @classmethod
    def setup_classes(cls):
        class A(_fixtures.Base):
            pass
        class B(_fixtures.Base):
            pass
        class C(_fixtures.Base):
            pass

    @testing.resolve_artifact_names
    def test_delete_orphan(self):

        # if no backref here, delete-orphan failed until [ticket:427]
        # was fixed

        mapper(A, a, properties={'bs': relationship(B, secondary=atob,
               cascade='all, delete-orphan', single_parent=True)})
        mapper(B, b)

        sess = create_session()
        b1 = B(data='b1')
        a1 = A(data='a1', bs=[b1])
        sess.add(a1)
        sess.flush()

        a1.bs.remove(b1)
        sess.flush()
        assert atob.count().scalar() ==0
        assert b.count().scalar() == 0
        assert a.count().scalar() == 1

    @testing.resolve_artifact_names
    def test_delete_orphan_dynamic(self):
        mapper(A, a, properties={'bs': relationship(B, secondary=atob,
               cascade='all, delete-orphan', single_parent=True,
               lazy='dynamic')})  # if no backref here, delete-orphan
                                  # failed until [ticket:427] was fixed
        mapper(B, b)

        sess = create_session()
        b1 = B(data='b1')
        a1 = A(data='a1', bs=[b1])
        sess.add(a1)
        sess.flush()

        a1.bs.remove(b1)
        sess.flush()
        assert atob.count().scalar() ==0
        assert b.count().scalar() == 0
        assert a.count().scalar() == 1

    @testing.resolve_artifact_names
    def test_delete_orphan_cascades(self):
        mapper(A, a, properties={
            # if no backref here, delete-orphan failed until [ticket:427] was
            # fixed
            'bs':relationship(B, secondary=atob, cascade="all, delete-orphan",
                                    single_parent=True)
        })
        mapper(B, b, properties={'cs':
                            relationship(C, cascade="all, delete-orphan")})
        mapper(C, c)

        sess = create_session()
        b1 = B(data='b1', cs=[C(data='c1')])
        a1 = A(data='a1', bs=[b1])
        sess.add(a1)
        sess.flush()

        a1.bs.remove(b1)
        sess.flush()
        assert atob.count().scalar() ==0
        assert b.count().scalar() == 0
        assert a.count().scalar() == 1
        assert c.count().scalar() == 0

    @testing.resolve_artifact_names
    def test_cascade_delete(self):
        mapper(A, a, properties={
            'bs':relationship(B, secondary=atob, cascade="all, delete-orphan",
                                    single_parent=True)
        })
        mapper(B, b)

        sess = create_session()
        a1 = A(data='a1', bs=[B(data='b1')])
        sess.add(a1)
        sess.flush()

        sess.delete(a1)
        sess.flush()
        assert atob.count().scalar() ==0
        assert b.count().scalar() == 0
        assert a.count().scalar() == 0

    @testing.resolve_artifact_names
    def test_single_parent_raise(self):
        mapper(A, a, properties={
            'bs':relationship(B, secondary=atob, cascade="all, delete-orphan",
                                    single_parent=True)
        })
        mapper(B, b)

        sess = create_session()
        b1 =B(data='b1')
        a1 = A(data='a1', bs=[b1])
        
        assert_raises(sa_exc.InvalidRequestError,
                A, data='a2', bs=[b1]
            )

    @testing.resolve_artifact_names
    def test_single_parent_backref(self):
        """test that setting m2m via a uselist=False backref bypasses the single_parent raise"""
        
        mapper(A, a, properties={
            'bs':relationship(B, 
                secondary=atob, 
                cascade="all, delete-orphan", single_parent=True,
                backref=backref('a', uselist=False))
        })
        mapper(B, b)

        sess = create_session()
        b1 =B(data='b1')
        a1 = A(data='a1', bs=[b1])
        
        assert_raises(
            sa_exc.InvalidRequestError,
            A, data='a2', bs=[b1]
        )
        
        a2 = A(data='a2')
        b1.a = a2
        assert b1 not in a1.bs
        assert b1 in a2.bs

class UnsavedOrphansTest(_base.MappedTest):
    """Pending entities that are orphans"""

    @classmethod
    def define_tables(cls, metadata):
        Table('users', metadata,
            Column('user_id', Integer,primary_key=True,
                                test_needs_autoincrement=True),
            Column('name', String(40)))

        Table('addresses', metadata,
            Column('address_id', Integer,primary_key=True,
                                test_needs_autoincrement=True),
            Column('user_id', Integer, ForeignKey('users.user_id')),
            Column('email_address', String(40)))

    @classmethod
    def setup_classes(cls):
        class User(_fixtures.Base):
            pass
        class Address(_fixtures.Base):
            pass

    @testing.resolve_artifact_names
    def test_pending_standalone_orphan(self):
        """An entity that never had a parent on a delete-orphan cascade
        can't be saved."""

        mapper(Address, addresses)
        mapper(User, users, properties=dict(
            addresses=relationship(Address, cascade="all,delete-orphan",
                                                backref="user")
        ))
        s = create_session()
        a = Address()
        s.add(a)
        try:
            s.flush()
        except orm_exc.FlushError, e:
            pass
        assert a.address_id is None, "Error: address should not be persistent"

    @testing.resolve_artifact_names
    def test_pending_collection_expunge(self):
        """Removing a pending item from a collection expunges it from
        the session."""

        mapper(Address, addresses)
        mapper(User, users, properties=dict(
            addresses=relationship(Address, cascade="all,delete-orphan", 
                                        backref="user")
        ))
        s = create_session()

        u = User()
        s.add(u)
        s.flush()
        a = Address()

        u.addresses.append(a)
        assert a in s

        u.addresses.remove(a)
        assert a not in s

        s.delete(u)
        s.flush()

        assert a.address_id is None, "Error: address should not be persistent"

    @testing.resolve_artifact_names
    def test_nonorphans_ok(self):
        mapper(Address, addresses)
        mapper(User, users, properties=dict(
            addresses=relationship(Address, cascade="all,delete",
                                            backref="user")
        ))
        s = create_session()
        u = User(name='u1', addresses=[Address(email_address='ad1')])
        s.add(u)
        a1 = u.addresses[0]
        u.addresses.remove(a1)
        assert a1 in s
        s.flush()
        s.expunge_all()
        eq_(s.query(Address).all(), [Address(email_address='ad1')])


class UnsavedOrphansTest2(_base.MappedTest):
    """same test as UnsavedOrphans only three levels deep"""

    @classmethod
    def define_tables(cls, meta):
        Table('orders', meta,
            Column('id', Integer, primary_key=True,
                                    test_needs_autoincrement=True),
            Column('name', String(50)))

        Table('items', meta,
            Column('id', Integer, primary_key=True,
                                    test_needs_autoincrement=True),
            Column('order_id', Integer, ForeignKey('orders.id'),
                   nullable=False),
            Column('name', String(50)))

        Table('attributes', meta,
            Column('id', Integer, primary_key=True,
                                    test_needs_autoincrement=True),
            Column('item_id', Integer, ForeignKey('items.id'),
                   nullable=False),
            Column('name', String(50)))

    @testing.resolve_artifact_names
    def test_pending_expunge(self):
        class Order(_fixtures.Base):
            pass
        class Item(_fixtures.Base):
            pass
        class Attribute(_fixtures.Base):
            pass

        mapper(Attribute, attributes)
        mapper(Item, items, properties=dict(
            attributes=relationship(Attribute, cascade="all,delete-orphan",
                                    backref="item")
        ))
        mapper(Order, orders, properties=dict(
            items=relationship(Item, cascade="all,delete-orphan",
                                    backref="order")
        ))

        s = create_session()
        order = Order(name="order1")
        s.add(order)

        attr = Attribute(name="attr1")
        item = Item(name="item1", attributes=[attr])

        order.items.append(item)
        order.items.remove(item)

        assert item not in s
        assert attr not in s

        s.flush()
        assert orders.count().scalar() == 1
        assert items.count().scalar() == 0
        assert attributes.count().scalar() == 0

class UnsavedOrphansTest3(_base.MappedTest):
    """test not expunging double parents"""

    @classmethod
    def define_tables(cls, meta):
        Table('sales_reps', meta,
            Column('sales_rep_id', Integer,primary_key=True,
                                    test_needs_autoincrement=True),
            Column('name', String(50)))
        Table('accounts', meta,
            Column('account_id', Integer,primary_key=True,
                                    test_needs_autoincrement=True),
            Column('balance', Integer))
        Table('customers', meta,
            Column('customer_id', Integer,primary_key=True,
                                    test_needs_autoincrement=True),
            Column('name', String(50)),
            Column('sales_rep_id', Integer,
                   ForeignKey('sales_reps.sales_rep_id')),
            Column('account_id', Integer,
                   ForeignKey('accounts.account_id')))

    @testing.resolve_artifact_names
    def test_double_parent_expunge_o2m(self):
        """test the delete-orphan uow event for multiple delete-orphan
        parent relationships."""
        
        class Customer(_fixtures.Base):
            pass
        class Account(_fixtures.Base):
            pass
        class SalesRep(_fixtures.Base):
            pass

        mapper(Customer, customers)
        mapper(Account, accounts, properties=dict(
            customers=relationship(Customer,
                               cascade="all,delete-orphan",
                               backref="account")))
        mapper(SalesRep, sales_reps, properties=dict(
            customers=relationship(Customer,
                               cascade="all,delete-orphan",
                               backref="sales_rep")))
        s = create_session()

        a = Account(balance=0)
        sr = SalesRep(name="John")
        s.add_all((a, sr))
        s.flush()

        c = Customer(name="Jane")

        a.customers.append(c)
        sr.customers.append(c)
        assert c in s

        a.customers.remove(c)
        assert c in s, "Should not expunge customer yet, still has one parent"

        sr.customers.remove(c)
        assert c not in s, \
            'Should expunge customer when both parents are gone'

    @testing.resolve_artifact_names
    def test_double_parent_expunge_o2o(self):
        """test the delete-orphan uow event for multiple delete-orphan
        parent relationships."""

        class Customer(_fixtures.Base):
            pass
        class Account(_fixtures.Base):
            pass
        class SalesRep(_fixtures.Base):
            pass

        mapper(Customer, customers)
        mapper(Account, accounts, properties=dict(
            customer=relationship(Customer,
                               cascade="all,delete-orphan",
                               backref="account", uselist=False)))
        mapper(SalesRep, sales_reps, properties=dict(
            customer=relationship(Customer,
                               cascade="all,delete-orphan",
                               backref="sales_rep", uselist=False)))
        s = create_session()

        a = Account(balance=0)
        sr = SalesRep(name="John")
        s.add_all((a, sr))
        s.flush()

        c = Customer(name="Jane")

        a.customer = c
        sr.customer = c
        assert c in s

        a.customer = None
        assert c in s, "Should not expunge customer yet, still has one parent"

        sr.customer = None
        assert c not in s, \
            'Should expunge customer when both parents are gone'

        
class DoubleParentOrphanTest(_base.MappedTest):
    """test orphan detection for an entity with two parent relationships"""

    @classmethod
    def define_tables(cls, metadata):
        Table('addresses', metadata,
            Column('address_id', Integer, primary_key=True,
                                    test_needs_autoincrement=True),
            Column('street', String(30)),
        )

        Table('homes', metadata,
            Column('home_id', Integer, primary_key=True, key="id",
                                    test_needs_autoincrement=True),
            Column('description', String(30)),
            Column('address_id', Integer, ForeignKey('addresses.address_id'),
                   nullable=False),
        )

        Table('businesses', metadata,
            Column('business_id', Integer, primary_key=True, key="id",
                                    test_needs_autoincrement=True),
            Column('description', String(30), key="description"),
            Column('address_id', Integer, ForeignKey('addresses.address_id'),
                   nullable=False),
        )

    @testing.resolve_artifact_names
    def test_non_orphan(self):
        """test that an entity can have two parent delete-orphan
        cascades, and persists normally."""

        class Address(_fixtures.Base):
            pass
        class Home(_fixtures.Base):
            pass
        class Business(_fixtures.Base):
            pass

        mapper(Address, addresses)
        mapper(Home, homes, properties={'address'
               : relationship(Address, cascade='all,delete-orphan',
               single_parent=True)})
        mapper(Business, businesses, properties={'address'
               : relationship(Address, cascade='all,delete-orphan',
               single_parent=True)})

        session = create_session()
        h1 = Home(description='home1', address=Address(street='address1'))
        b1 = Business(description='business1',
                      address=Address(street='address2'))
        session.add_all((h1,b1))
        session.flush()
        session.expunge_all()

        eq_(session.query(Home).get(h1.id), Home(description='home1',
            address=Address(street='address1')))
        eq_(session.query(Business).get(b1.id),
            Business(description='business1',
            address=Address(street='address2')))

    @testing.resolve_artifact_names
    def test_orphan(self):
        """test that an entity can have two parent delete-orphan
        cascades, and is detected as an orphan when saved without a
        parent."""

        class Address(_fixtures.Base):
            pass

        class Home(_fixtures.Base):
            pass

        class Business(_fixtures.Base):
            pass

        mapper(Address, addresses)
        mapper(Home, homes, properties={'address'
               : relationship(Address, cascade='all,delete-orphan',
               single_parent=True)})
        mapper(Business, businesses, properties={'address'
               : relationship(Address, cascade='all,delete-orphan',
               single_parent=True)})
        session = create_session()
        a1 = Address()
        session.add(a1)
        try:
            session.flush()
            assert False
        except orm_exc.FlushError, e:
            assert True

class CollectionAssignmentOrphanTest(_base.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table('table_a', metadata, 
            Column('id', Integer,
              primary_key=True, test_needs_autoincrement=True),
              Column('name', String(30)))
        Table('table_b', metadata, 
            Column('id', Integer,
              primary_key=True, test_needs_autoincrement=True),
              Column('name', String(30)), 
              Column('a_id', Integer, ForeignKey('table_a.id')))

    @testing.resolve_artifact_names
    def test_basic(self):
        class A(_fixtures.Base):
            pass
        class B(_fixtures.Base):
            pass

        mapper(A, table_a, properties={
            'bs':relationship(B, cascade="all, delete-orphan")
            })
        mapper(B, table_b)

        a1 = A(name='a1', bs=[B(name='b1'), B(name='b2'), B(name='b3')])

        sess = create_session()
        sess.add(a1)
        sess.flush()

        sess.expunge_all()

        eq_(sess.query(A).get(a1.id),
            A(name='a1', bs=[B(name='b1'), B(name='b2'), B(name='b3')]))

        a1 = sess.query(A).get(a1.id)
        assert not class_mapper(B)._is_orphan(
            attributes.instance_state(a1.bs[0]))
        a1.bs[0].foo='b2modified'
        a1.bs[1].foo='b3modified'
        sess.flush()

        sess.expunge_all()
        eq_(sess.query(A).get(a1.id),
            A(name='a1', bs=[B(name='b1'), B(name='b2'), B(name='b3')]))

class O2MConflictTest(_base.MappedTest):
    """test that O2M dependency detects a change in parent, does the
    right thing, and even updates the collection/attribute.
    
    """
    
    @classmethod
    def define_tables(cls, metadata):
        Table("parent", metadata,
            Column("id", Integer, primary_key=True,
                                test_needs_autoincrement=True)
        )
        Table("child", metadata,
            Column("id", Integer, primary_key=True,
                                    test_needs_autoincrement=True),
            Column('parent_id', Integer, ForeignKey('parent.id'),
                                    nullable=False)
        )
    
    @classmethod
    def setup_classes(cls):
        class Parent(_base.ComparableEntity):
            pass
        class Child(_base.ComparableEntity):
            pass
    
    @testing.resolve_artifact_names
    def _do_delete_old_test(self):
        sess = create_session()
        
        p1, p2, c1 = Parent(), Parent(), Child()
        if Parent.child.property.uselist:
            p1.child.append(c1)
        else:
            p1.child = c1
        sess.add_all([p1, c1])
        sess.flush()
        
        sess.delete(p1)
        
        if Parent.child.property.uselist:
            p2.child.append(c1)
        else:
            p2.child = c1
        sess.add(p2)

        sess.flush()
        eq_(sess.query(Child).filter(Child.parent_id==p2.id).all(), [c1])

    @testing.resolve_artifact_names
    def _do_move_test(self):
        sess = create_session()

        p1, p2, c1 = Parent(), Parent(), Child()
        if Parent.child.property.uselist:
            p1.child.append(c1)
        else:
            p1.child = c1
        sess.add_all([p1, c1])
        sess.flush()

        if Parent.child.property.uselist:
            p2.child.append(c1)
        else:
            p2.child = c1
        sess.add(p2)

        sess.flush()
        eq_(sess.query(Child).filter(Child.parent_id==p2.id).all(), [c1])
        
    @testing.resolve_artifact_names
    def test_o2o_delete_old(self):
        mapper(Parent, parent, properties={
            'child':relationship(Child, uselist=False)
        })
        mapper(Child, child)
        self._do_delete_old_test()
        self._do_move_test()

    @testing.resolve_artifact_names
    def test_o2m_delete_old(self):
        mapper(Parent, parent, properties={
            'child':relationship(Child, uselist=True)
        })
        mapper(Child, child)
        self._do_delete_old_test()
        self._do_move_test()

    @testing.resolve_artifact_names
    def test_o2o_backref_delete_old(self):
        mapper(Parent, parent, properties={
            'child':relationship(Child, uselist=False, backref='parent')
        })
        mapper(Child, child)
        self._do_delete_old_test()
        self._do_move_test()
        
    @testing.resolve_artifact_names
    def test_o2o_delcascade_delete_old(self):
        mapper(Parent, parent, properties={
            'child':relationship(Child, uselist=False, cascade="all, delete")
        })
        mapper(Child, child)
        self._do_delete_old_test()
        self._do_move_test()

    @testing.resolve_artifact_names
    def test_o2o_delorphan_delete_old(self):
        mapper(Parent, parent, properties={
            'child':relationship(Child, uselist=False, 
                                    cascade="all, delete, delete-orphan")
        })
        mapper(Child, child)
        self._do_delete_old_test()
        self._do_move_test()

    @testing.resolve_artifact_names
    def test_o2o_delorphan_backref_delete_old(self):
        mapper(Parent, parent, properties={
            'child':relationship(Child, uselist=False, 
                                        cascade="all, delete, delete-orphan", 
                                        backref='parent')
        })
        mapper(Child, child)
        self._do_delete_old_test()
        self._do_move_test()

    @testing.resolve_artifact_names
    def test_o2o_backref_delorphan_delete_old(self):
        mapper(Parent, parent)
        mapper(Child, child, properties = {
            'parent' : relationship(Parent, uselist=False, single_parent=True, 
                                backref=backref('child', uselist=False), 
                                cascade="all,delete,delete-orphan")
        })
        self._do_delete_old_test()
        self._do_move_test()

    @testing.resolve_artifact_names
    def test_o2m_backref_delorphan_delete_old(self):
        mapper(Parent, parent)
        mapper(Child, child, properties = {
            'parent' : relationship(Parent, uselist=False, single_parent=True, 
                                backref=backref('child', uselist=True), 
                                cascade="all,delete,delete-orphan")
        })
        self._do_delete_old_test()
        self._do_move_test()
        

class PartialFlushTest(_base.MappedTest):
    """test cascade behavior as it relates to object lists passed to flush().
    
    """
    @classmethod
    def define_tables(cls, metadata):
        Table("base", metadata,
            Column("id", Integer, primary_key=True,
                                test_needs_autoincrement=True),
            Column("descr", String(50))
        )

        Table("noninh_child", metadata, 
            Column('id', Integer, primary_key=True,
                                test_needs_autoincrement=True),
            Column('base_id', Integer, ForeignKey('base.id'))
        )

        Table("parent", metadata,
            Column("id", Integer, ForeignKey("base.id"), primary_key=True)
        )
        Table("inh_child", metadata,
            Column("id", Integer, ForeignKey("base.id"), primary_key=True),
            Column("parent_id", Integer, ForeignKey("parent.id"))
        )

    @testing.uses_deprecated()
    @testing.resolve_artifact_names
    def test_o2m_m2o(self):
        class Base(_base.ComparableEntity):
            pass
        class Child(_base.ComparableEntity):
            pass

        mapper(Base, base, properties={
            'children':relationship(Child, backref='parent')
        })
        mapper(Child, noninh_child)

        sess = create_session()

        c1, c2 = Child(), Child()
        b1 = Base(descr='b1', children=[c1, c2])
        sess.add(b1)

        assert c1 in sess.new
        assert c2 in sess.new
        sess.flush([b1])

        # c1, c2 get cascaded into the session on o2m.
        # not sure if this is how I like this 
        # to work but that's how it works for now.
        assert c1 in sess and c1 not in sess.new
        assert c2 in sess and c2 not in sess.new
        assert b1 in sess and b1 not in sess.new

        sess = create_session()
        c1, c2 = Child(), Child()
        b1 = Base(descr='b1', children=[c1, c2])
        sess.add(b1)
        sess.flush([c1])
        # m2o, otoh, doesn't cascade up the other way.
        assert c1 in sess and c1 not in sess.new
        assert c2 in sess and c2 in sess.new
        assert b1 in sess and b1 in sess.new

        sess = create_session()
        c1, c2 = Child(), Child()
        b1 = Base(descr='b1', children=[c1, c2])
        sess.add(b1)
        sess.flush([c1, c2])
        # m2o, otoh, doesn't cascade up the other way.
        assert c1 in sess and c1 not in sess.new
        assert c2 in sess and c2 not in sess.new
        assert b1 in sess and b1 in sess.new

    @testing.uses_deprecated()
    @testing.resolve_artifact_names
    def test_circular_sort(self):
        """test ticket 1306"""
        
        class Base(_base.ComparableEntity):
            pass
        class Parent(Base):
            pass
        class Child(Base):
            pass

        mapper(Base,base)

        mapper(Child, inh_child,
            inherits=Base,
            properties={'parent': relationship(
                Parent,
                backref='children', 
                primaryjoin=inh_child.c.parent_id == parent.c.id
            )}
        )


        mapper(Parent,parent, inherits=Base)

        sess = create_session()
        p1 = Parent()

        c1, c2, c3 = Child(), Child(), Child()
        p1.children = [c1, c2, c3]
        sess.add(p1)
        
        sess.flush([c1])
        assert p1 in sess.new
        assert c1 not in sess.new
        assert c2 in sess.new
        
