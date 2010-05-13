from sqlalchemy.test.testing import eq_
import pickle
import sqlalchemy as sa
from sqlalchemy.test import testing
from sqlalchemy.test.testing import assert_raises_message
from sqlalchemy import Integer, String, ForeignKey, exc, MetaData
from sqlalchemy.test.schema import Table, Column
from sqlalchemy.orm import mapper, relationship, create_session, \
                            sessionmaker, attributes, interfaces,\
                            clear_mappers, exc as orm_exc,\
                            compile_mappers
from test.orm import _base, _fixtures


User, EmailUser = None, None


class PickleTest(_fixtures.FixtureTest):
    run_inserts = None
    
    @testing.resolve_artifact_names
    def test_transient(self):
        mapper(User, users, properties={
            'addresses':relationship(Address, backref="user")
        })
        mapper(Address, addresses)

        sess = create_session()
        u1 = User(name='ed')
        u1.addresses.append(Address(email_address='ed@bar.com'))

        u2 = pickle.loads(pickle.dumps(u1))
        sess.add(u2)
        sess.flush()

        sess.expunge_all()

        eq_(u1, sess.query(User).get(u2.id))

    @testing.resolve_artifact_names
    def test_no_mappers(self):
        
        umapper = mapper(User, users)
        u1 = User(name='ed')
        u1_pickled = pickle.dumps(u1, -1)

        clear_mappers()

        assert_raises_message(
            orm_exc.UnmappedInstanceError,
            "Cannot deserialize object of type <class 'test.orm._fixtures.User'> - no mapper()",
            pickle.loads, u1_pickled)

    @testing.resolve_artifact_names
    def test_no_instrumentation(self):

        umapper = mapper(User, users)
        u1 = User(name='ed')
        u1_pickled = pickle.dumps(u1, -1)

        clear_mappers()

        umapper = mapper(User, users)

        u1 = pickle.loads(u1_pickled)
        # this fails unless the InstanceState
        # compiles the mapper
        eq_(str(u1), "User(name='ed')")
        
    @testing.resolve_artifact_names
    def test_serialize_path(self):
        umapper = mapper(User, users, properties={
            'addresses':relationship(Address, backref="user")
        })
        amapper = mapper(Address, addresses)
        
        # this is a "relationship" path with mapper, key, mapper, key
        p1 = (umapper, 'addresses', amapper, 'email_address')
        eq_(
            interfaces.deserialize_path(interfaces.serialize_path(p1)),
            p1
        )
        
        # this is a "mapper" path with mapper, key, mapper, no key
        # at the end.
        p2 = (umapper, 'addresses', amapper, )
        eq_(
            interfaces.deserialize_path(interfaces.serialize_path(p2)),
            p2
        )
        
        # test a blank path
        p3 = ()
        eq_(
            interfaces.deserialize_path(interfaces.serialize_path(p3)),
            p3
        )
        
    @testing.resolve_artifact_names
    def test_class_deferred_cols(self):
        mapper(User, users, properties={
            'name':sa.orm.deferred(users.c.name),
            'addresses':relationship(Address, backref="user")
        })
        mapper(Address, addresses, properties={
            'email_address':sa.orm.deferred(addresses.c.email_address)
        })
        sess = create_session()
        u1 = User(name='ed')
        u1.addresses.append(Address(email_address='ed@bar.com'))
        sess.add(u1)
        sess.flush()
        sess.expunge_all()
        u1 = sess.query(User).get(u1.id)
        assert 'name' not in u1.__dict__
        assert 'addresses' not in u1.__dict__

        u2 = pickle.loads(pickle.dumps(u1))
        sess2 = create_session()
        sess2.add(u2)
        eq_(u2.name, 'ed')
        eq_(u2, User(name='ed', addresses=[Address(email_address='ed@bar.com')]))

        u2 = pickle.loads(pickle.dumps(u1))
        sess2 = create_session()
        u2 = sess2.merge(u2, load=False)
        eq_(u2.name, 'ed')
        eq_(u2, User(name='ed', addresses=[Address(email_address='ed@bar.com')]))

    @testing.resolve_artifact_names
    def test_instance_deferred_cols(self):
        mapper(User, users, properties={
            'addresses':relationship(Address, backref="user")
        })
        mapper(Address, addresses)

        sess = create_session()
        u1 = User(name='ed')
        u1.addresses.append(Address(email_address='ed@bar.com'))
        sess.add(u1)
        sess.flush()
        sess.expunge_all()

        u1 = sess.query(User).\
                options(sa.orm.defer('name'), 
                        sa.orm.defer('addresses.email_address')).\
                        get(u1.id)
        assert 'name' not in u1.__dict__
        assert 'addresses' not in u1.__dict__

        u2 = pickle.loads(pickle.dumps(u1))
        sess2 = create_session()
        sess2.add(u2)
        eq_(u2.name, 'ed')
        assert 'addresses' not in u2.__dict__
        ad = u2.addresses[0]
        assert 'email_address' not in ad.__dict__
        eq_(ad.email_address, 'ed@bar.com')
        eq_(u2, User(name='ed', addresses=[Address(email_address='ed@bar.com')]))

        u2 = pickle.loads(pickle.dumps(u1))
        sess2 = create_session()
        u2 = sess2.merge(u2, load=False)
        eq_(u2.name, 'ed')
        assert 'addresses' not in u2.__dict__
        ad = u2.addresses[0]
        
        # mapper options now transmit over merge(),
        # new as of 0.6, so email_address is deferred.
        assert 'email_address' not in ad.__dict__  
        
        eq_(ad.email_address, 'ed@bar.com')
        eq_(u2, User(name='ed', addresses=[Address(email_address='ed@bar.com')]))

    @testing.resolve_artifact_names
    def test_pickle_protocols(self):
        mapper(User, users, properties={
            'addresses':relationship(Address, backref="user")
        })
        mapper(Address, addresses)

        sess = sessionmaker()()
        u1 = User(name='ed')
        u1.addresses.append(Address(email_address='ed@bar.com'))
        sess.add(u1)
        sess.commit()

        u1 = sess.query(User).first()
        u1.addresses
        for protocol in -1, 0, 1, 2:
            u2 = pickle.loads(pickle.dumps(u1, protocol))
            eq_(u1, u2)
        
    @testing.resolve_artifact_names
    def test_options_with_descriptors(self):
        mapper(User, users, properties={
            'addresses':relationship(Address, backref="user")
        })
        mapper(Address, addresses)
        sess = create_session()
        u1 = User(name='ed')
        u1.addresses.append(Address(email_address='ed@bar.com'))
        sess.add(u1)
        sess.flush()
        sess.expunge_all()

        for opt in [
            sa.orm.joinedload(User.addresses),
            sa.orm.joinedload("addresses"),
            sa.orm.defer("name"),
            sa.orm.defer(User.name),
            sa.orm.joinedload("addresses", User.addresses),
        ]:
            opt2 = pickle.loads(pickle.dumps(opt))
            eq_(opt.key, opt2.key)
        
        u1 = sess.query(User).options(opt).first()
        
        u2 = pickle.loads(pickle.dumps(u1))
        
    def test_collection_setstate(self):
        """test a particular cycle that requires CollectionAdapter 
        to not rely upon InstanceState to deserialize."""
        
        global Child1, Child2, Parent, Screen
        
        m = MetaData()
        c1 = Table('c1', m, 
            Column('parent_id', String, 
                        ForeignKey('p.id'), primary_key=True)
        )
        c2 = Table('c2', m,
            Column('parent_id', String, 
                        ForeignKey('p.id'), primary_key=True)
        )
        p = Table('p', m,
            Column('id', String, primary_key=True)
        )
        class Child1(_base.ComparableEntity):
            pass

        class Child2(_base.ComparableEntity):
            pass

        class Parent(_base.ComparableEntity):
            pass
        
        mapper(Parent, p, properties={
            'children1':relationship(Child1),
            'children2':relationship(Child2)
        })
        mapper(Child1, c1)
        mapper(Child2, c2)
        class Screen(object):
           def __init__(self, obj, parent=None):
               self.obj = obj
               self.parent = parent

        obj = Parent()
        screen1 = Screen(obj)
        screen1.errors = [obj.children1, obj.children2]
        screen2 = Screen(Child2(), screen1)
        pickle.loads(pickle.dumps(screen2))
        
class PolymorphicDeferredTest(_base.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table('users', metadata,
            Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('name', String(30)),
            Column('type', String(30)))
        Table('email_users', metadata,
            Column('id', Integer, ForeignKey('users.id'), primary_key=True),
            Column('email_address', String(30)))

    @classmethod
    def setup_classes(cls):
        global User, EmailUser
        class User(_base.BasicEntity):
            pass

        class EmailUser(User):
            pass

    @classmethod
    def teardown_class(cls):
        global User, EmailUser
        User, EmailUser = None, None
        super(PolymorphicDeferredTest, cls).teardown_class()

    @testing.resolve_artifact_names
    def test_polymorphic_deferred(self):
        mapper(User, users, polymorphic_identity='user', polymorphic_on=users.c.type)
        mapper(EmailUser, email_users, inherits=User, polymorphic_identity='emailuser')

        eu = EmailUser(name="user1", email_address='foo@bar.com')
        sess = create_session()
        sess.add(eu)
        sess.flush()
        sess.expunge_all()

        eu = sess.query(User).first()
        eu2 = pickle.loads(pickle.dumps(eu))
        sess2 = create_session()
        sess2.add(eu2)
        assert 'email_address' not in eu2.__dict__
        eq_(eu2.email_address, 'foo@bar.com')

class CustomSetupTeardownTest(_fixtures.FixtureTest):
    @testing.resolve_artifact_names
    def test_rebuild_state(self):
        """not much of a 'test', but illustrate how to 
        remove instance-level state before pickling.
        
        """
        mapper(User, users)

        u1 = User()
        attributes.manager_of_class(User).teardown_instance(u1)
        assert not u1.__dict__
        u2 = pickle.loads(pickle.dumps(u1))
        attributes.manager_of_class(User).setup_instance(u2)
        assert attributes.instance_state(u2)
    
class UnpickleSA05Test(_fixtures.FixtureTest):
    """test loading picklestrings from SQLA 0.5."""
    
    __requires__ = ('python2',)
    
    @testing.resolve_artifact_names
    def test_one(self):
        mapper(User, users, properties={
            'addresses':relationship(Address, backref="user")
        })
        mapper(Address, addresses)
        data = \
                 '\x80\x02]q\x00(ctest.orm._fixtures\nUser\nq\x01)\x81q\x02}q\x03(U\x12_sa_instance_stateq\x04csqlalchemy.orm.state\nInstanceState\nq\x05)\x81q\x06}q\x07(U\x08instanceq\x08h\x02U\x03keyq\th\x01K\x07\x85q\n\x86q\x0bubU\taddressesq\x0ccsqlalchemy.orm.collections\nInstrumentedList\nq\r)\x81q\x0ectest.orm._fixtures\nAddress\nq\x0f)\x81q\x10}q\x11(U\remail_addressq\x12X\r\x00\x00\x00jack@bean.comq\x13h\x04h\x05)\x81q\x14}q\x15(h\x08h\x10h\th\x0fK\x01\x85q\x16\x86q\x17ubU\x07user_idq\x18K\x07U\x02idq\x19K\x01uba}q\x1aU\x0b_sa_adapterq\x1bcsqlalchemy.orm.collections\nCollectionAdapter\nq\x1c)\x81q\x1d}q\x1e(U\x04dataq\x1fh\x0eU\x0bowner_stateq h\x06U\x03keyq!h\x0cubsbh\x19K\x07U\x04nameq"X\x04\x00\x00\x00jackq#ubh\x01)\x81q$}q%(h\x04h\x05)\x81q&}q\'(h\x08h$h\th\x01K\x08\x85q(\x86q)ubh\x0ch\r)\x81q*(h\x0f)\x81q+}q,(h\x12X\x0b\x00\x00\x00ed@wood.comq-h\x04h\x05)\x81q.}q/(h\x08h+h\th\x0fK\x02\x85q0\x86q1ubh\x18K\x08h\x19K\x02ubh\x0f)\x81q2}q3(h\x12X\x10\x00\x00\x00ed@bettyboop.comq4h\x04h\x05)\x81q5}q6(h\x08h2h\th\x0fK\x03\x85q7\x86q8ubh\x18K\x08h\x19K\x03ubh\x0f)\x81q9}q:(h\x12X\x0b\x00\x00\x00ed@lala.comq;h\x04h\x05)\x81q<}q=(h\x08h9h\th\x0fK\x04\x85q>\x86q?ubh\x18K\x08h\x19K\x04ube}q@h\x1bh\x1c)\x81qA}qB(h\x1fh*h h&h!h\x0cubsbh\x19K\x08h"X\x02\x00\x00\x00edqCubh\x01)\x81qD}qE(h\x04h\x05)\x81qF}qG(h\x08hDh\th\x01K\t\x85qH\x86qIubh\x0ch\r)\x81qJh\x0f)\x81qK}qL(h\x12X\r\x00\x00\x00fred@fred.comqMh\x04h\x05)\x81qN}qO(h\x08hKh\th\x0fK\x05\x85qP\x86qQubh\x18K\th\x19K\x05uba}qRh\x1bh\x1c)\x81qS}qT(h\x1fhJh hFh!h\x0cubsbh\x19K\th"X\x04\x00\x00\x00fredqUubh\x01)\x81qV}qW(h\x04h\x05)\x81qX}qY(h\x08hVh\th\x01K\n\x85qZ\x86q[ubh\x0ch\r)\x81q\\}q]h\x1bh\x1c)\x81q^}q_(h\x1fh\\h hXh!h\x0cubsbh\x19K\nh"X\x05\x00\x00\x00chuckq`ube.'

        sess = create_session()
        result = list(sess.query(User).merge_result(pickle.loads(data)))
        eq_(result, self.static.user_address_result)

    @testing.resolve_artifact_names
    def test_two(self):
        mapper(User, users, properties={
            'addresses':relationship(Address, backref="user")
        })
        mapper(Address, addresses)
        data = \
'\x80\x02]q\x00(ctest.orm._fixtures\nUser\nq\x01)\x81q\x02}q\x03(U\x12_sa_instance_stateq\x04csqlalchemy.orm.state\nInstanceState\nq\x05)\x81q\x06}q\x07(U\x08instanceq\x08h\x02U\tload_pathq\t]q\nU\x03keyq\x0bh\x01K\x07\x85q\x0c\x86q\rU\x0cload_optionsq\x0ec__builtin__\nset\nq\x0f]q\x10csqlalchemy.orm.strategies\nEagerLazyOption\nq\x11)\x81q\x12}q\x13(U\x06mapperq\x14NU\x04lazyq\x15\x89U\x14propagate_to_loadersq\x16\x88U\x03keyq\x17]q\x18h\x01U\taddressesq\x19\x86q\x1aaU\x07chainedq\x1b\x89uba\x85q\x1cRq\x1dubh\x19csqlalchemy.orm.collections\nInstrumentedList\nq\x1e)\x81q\x1fctest.orm._fixtures\nAddress\nq )\x81q!}q"(U\remail_addressq#X\r\x00\x00\x00jack@bean.comq$h\x04h\x05)\x81q%}q&(h\x08h!h\t]q\'h\x01h\x19\x86q(ah\x0bh K\x01\x85q)\x86q*h\x0eh\x1dubU\x07user_idq+K\x07U\x02idq,K\x01uba}q-U\x0b_sa_adapterq.csqlalchemy.orm.collections\nCollectionAdapter\nq/)\x81q0}q1(U\x04dataq2h\x1fU\x0bowner_stateq3h\x06h\x17h\x19ubsbU\x04nameq4X\x04\x00\x00\x00jackq5h,K\x07ubh\x01)\x81q6}q7(h\x04h\x05)\x81q8}q9(h\x08h6h\t]q:h\x0bh\x01K\x08\x85q;\x86q<h\x0eh\x1dubh\x19h\x1e)\x81q=(h )\x81q>}q?(h#X\x0b\x00\x00\x00ed@wood.comq@h\x04h\x05)\x81qA}qB(h\x08h>h\t]qCh\x01h\x19\x86qDah\x0bh K\x02\x85qE\x86qFh\x0eh\x1dubh+K\x08h,K\x02ubh )\x81qG}qH(h#X\x10\x00\x00\x00ed@bettyboop.comqIh\x04h\x05)\x81qJ}qK(h\x08hGh\t]qLh\x01h\x19\x86qMah\x0bh K\x03\x85qN\x86qOh\x0eh\x1dubh+K\x08h,K\x03ubh )\x81qP}qQ(h#X\x0b\x00\x00\x00ed@lala.comqRh\x04h\x05)\x81qS}qT(h\x08hPh\t]qUh\x01h\x19\x86qVah\x0bh K\x04\x85qW\x86qXh\x0eh\x1dubh+K\x08h,K\x04ube}qYh.h/)\x81qZ}q[(h2h=h3h8h\x17h\x19ubsbh4X\x02\x00\x00\x00edq\\h,K\x08ubh\x01)\x81q]}q^(h\x04h\x05)\x81q_}q`(h\x08h]h\t]qah\x0bh\x01K\t\x85qb\x86qch\x0eh\x1dubh\x19h\x1e)\x81qdh )\x81qe}qf(h#X\r\x00\x00\x00fred@fred.comqgh\x04h\x05)\x81qh}qi(h\x08heh\t]qjh\x01h\x19\x86qkah\x0bh K\x05\x85ql\x86qmh\x0eh\x1dubh+K\th,K\x05uba}qnh.h/)\x81qo}qp(h2hdh3h_h\x17h\x19ubsbh4X\x04\x00\x00\x00fredqqh,K\tubh\x01)\x81qr}qs(h\x04h\x05)\x81qt}qu(h\x08hrh\t]qvh\x0bh\x01K\n\x85qw\x86qxh\x0eh\x1dubh\x19h\x1e)\x81qy}qzh.h/)\x81q{}q|(h2hyh3hth\x17h\x19ubsbh4X\x05\x00\x00\x00chuckq}h,K\nube.'

        sess = create_session()
        result = list(sess.query(User).merge_result(pickle.loads(data)))
        eq_(result, self.static.user_address_result)
