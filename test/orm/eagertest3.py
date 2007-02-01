from testbase import PersistTest, AssertMixin
import testbase
from sqlalchemy import *
from sqlalchemy.ext.selectresults import SelectResults
import random

class EagerTest(AssertMixin):
    def setUpAll(self):
        global dbmeta, owners, categories, tests, options, Owner, Category, Test, Option, false
        dbmeta = BoundMetaData(testbase.db)
        
        # determine a literal value for "false" based on the dialect
        false = Boolean().dialect_impl(testbase.db.dialect).convert_bind_param(False, testbase.db.dialect)
        
        owners = Table ( 'owners', dbmeta ,
        	Column ( 'id', Integer, primary_key=True, nullable=False ),
        	Column('data', String(30)) )
        categories=Table( 'categories', dbmeta,
        	Column ( 'id', Integer,primary_key=True, nullable=False ),
        	Column ( 'name', VARCHAR(20), index=True ) )
        tests = Table ( 'tests', dbmeta ,
        	Column ( 'id', Integer, primary_key=True, nullable=False ),
        	Column ( 'owner_id',Integer, ForeignKey('owners.id'), nullable=False,index=True ),
        	Column ( 'category_id', Integer, ForeignKey('categories.id'),nullable=False,index=True ))
        options = Table ( 'options', dbmeta ,
        	Column ( 'test_id', Integer, ForeignKey ( 'tests.id' ), primary_key=True, nullable=False ),
        	Column ( 'owner_id', Integer, ForeignKey ( 'owners.id' ), primary_key=True, nullable=False ),
        	Column ( 'someoption', Boolean, PassiveDefault(str(false)), nullable=False ) )

        dbmeta.create_all()

        class Owner(object):
        	pass
        class Category(object):
        	pass
        class Test(object):
        	pass
        class Option(object):
        	pass
        mapper(Owner,owners)
        mapper(Category,categories)
        mapper(Option,options,properties={'owner':relation(Owner),'test':relation(Test)})
        mapper(Test,tests,properties={
            'owner':relation(Owner,backref='tests'),
            'category':relation(Category),
            'owner_option': relation(Option,primaryjoin=and_(tests.c.id==options.c.test_id,tests.c.owner_id==options.c.owner_id),
                foreignkey=[options.c.test_id, options.c.owner_id],
            uselist=False ) 
        })

        s=create_session()

        # an owner
        o=Owner()
        s.save(o)

        # owner a has 3 tests, one of which he has specified options for
        c=Category()
        c.name='Some Category'
        s.save(c)

        for i in range(3):
        	t=Test()
        	t.owner=o
        	t.category=c
        	s.save(t)
        	if i==1:
        		op=Option()
        		op.someoption=True
        		t.owner_option=op
        	if i==2:
        		op=Option()
        		t.owner_option=op

        s.flush()
        s.close()

    def tearDownAll(self):
        clear_mappers()
        dbmeta.drop_all()

    def test_noorm(self):
        """test the control case"""
        # I want to display a list of tests owned by owner 1 
        # if someoption is false or he hasn't specified it yet (null)
        # but not if he set it to true (example someoption is for hiding)

        # desired output for owner 1
        # test_id, cat_name
        # 1 'Some Category'
        # 3  "

        # not orm style correct query
        print "Obtaining correct results without orm"
        result = select( [tests.c.id,categories.c.name],
        	and_(tests.c.owner_id==1,or_(options.c.someoption==None,options.c.someoption==False)),
        	order_by=[tests.c.id],
        	from_obj=[tests.join(categories).outerjoin(options,and_(tests.c.id==options.c.test_id,tests.c.owner_id==options.c.owner_id))] ).execute().fetchall()
        print result
        assert result == [(1, u'Some Category'), (3, u'Some Category')]
    
    def test_withouteagerload(self):
        s = create_session()
        l=s.query(Test).select ( and_(tests.c.owner_id==1,or_(options.c.someoption==None,options.c.someoption==False)),
        	from_obj=[tests.outerjoin(options,and_(tests.c.id==options.c.test_id,tests.c.owner_id==options.c.owner_id))])
        result = ["%d %s" % ( t.id,t.category.name ) for t in l]
        print result
        assert result == [u'1 Some Category', u'3 Some Category']

    def test_witheagerload(self):
        """test that an eagerload locates the correct "from" clause with 
        which to attach to, when presented with a query that already has a complicated from clause."""
        s = create_session()
        q=s.query(Test).options(eagerload('category'))
        l=q.select ( and_(tests.c.owner_id==1,or_(options.c.someoption==None,options.c.someoption==False)),
        	from_obj=[tests.outerjoin(options,and_(tests.c.id==options.c.test_id,tests.c.owner_id==options.c.owner_id))])
        result = ["%d %s" % ( t.id,t.category.name ) for t in l]
        print result
        assert result == [u'1 Some Category', u'3 Some Category']

    def test_dslish(self):
        """test the same as witheagerload except building the query via SelectResults"""
        s = create_session()
        q=SelectResults(s.query(Test).options(eagerload('category')))
        l=q.select ( 
            and_(tests.c.owner_id==1,or_(options.c.someoption==None,options.c.someoption==False))
            ).outerjoin_to('owner_option')
            
        result = ["%d %s" % ( t.id,t.category.name ) for t in l]
        print result
        assert result == [u'1 Some Category', u'3 Some Category']

    def test_withoutouterjoin_literal(self):
        s = create_session()
        q=s.query(Test).options(eagerload('category'))
        l=q.select( (tests.c.owner_id==1) & ('options.someoption is null or options.someoption=%s' % false) & q.join_to('owner_option') )
        result = ["%d %s" % ( t.id,t.category.name ) for t in l]	
        print result
        assert result == [u'3 Some Category']

    def test_withoutouterjoin(self):
        s = create_session()
        q=s.query(Test).options(eagerload('category'))
        l=q.select( (tests.c.owner_id==1) & ((options.c.someoption==None) | (options.c.someoption==False)) & q.join_to('owner_option') )
        result = ["%d %s" % ( t.id,t.category.name ) for t in l]	
        print result
        assert result == [u'3 Some Category']

class EagerTest2(AssertMixin):
    def setUpAll(self):
        global metadata, middle, left, right
        metadata = BoundMetaData(testbase.db)
        middle = Table('middle', metadata,
            Column('id', Integer, primary_key = True),
            Column('data', String(50)),
        )

        left = Table('left', metadata,
            Column('id', Integer, ForeignKey(middle.c.id), primary_key=True),
            Column('tag', String(50), primary_key=True),
        )

        right = Table('right', metadata,
            Column('id', Integer, ForeignKey(middle.c.id), primary_key=True),
            Column('tag', String(50), primary_key=True),
        )
        metadata.create_all()
    def tearDownAll(self):
        metadata.drop_all()
    def tearDown(self):
        for t in metadata.table_iterator(reverse=True):
            t.delete().execute()
    def testeagerterminate(self):
        """test that eager query generation does not include the same mapper's table twice.
        
        or, that bi-directional eager loads dont include each other in eager query generation."""
        class Middle(object):
            def __init__(self, data): self.data = data
        class Left(object):
            def __init__(self, data): self.tag = data
        class Right(object):
            def __init__(self, data): self.tag = data

        # set up bi-directional eager loads
        mapper(Left, left)
        mapper(Right, right)
        mapper(Middle, middle, properties = {
            'left': relation(Left, lazy=False, backref=backref('middle',lazy=False)),
            'right': relation(Right, lazy=False, backref=backref('middle', lazy=False)),
            }
        )
        session = create_session(bind_to=testbase.db)
        p = Middle('test1')
        p.left.append(Left('tag1'))
        p.right.append(Right('tag2'))
        session.save(p)
        session.flush()
        session.clear()
        obj = session.query(Left).get_by(tag='tag1')
        print obj.middle.right[0]

class EagerTest3(testbase.ORMTest):
    """test eager loading combined with nested SELECT statements, functions, and aggregates"""
    def define_tables(self, metadata):
        global datas, foo, stats
        datas=Table( 'datas',metadata,
         Column ( 'id', Integer, primary_key=True,nullable=False ),
         Column ( 'a', Integer , nullable=False ) )

        foo=Table('foo',metadata,
         Column ( 'data_id', Integer, ForeignKey('datas.id'),nullable=False,primary_key=True ),
         Column ( 'bar', Integer ) )

        stats=Table('stats',metadata,
        Column ( 'id', Integer, primary_key=True, nullable=False ),
        Column ( 'data_id', Integer, ForeignKey('datas.id')),
        Column ( 'somedata', Integer, nullable=False ))
        
    def test_nesting_with_functions(self):
        class Data(object): pass
        class Foo(object):pass
        class Stat(object): pass

        Data.mapper=mapper(Data,datas)
        Foo.mapper=mapper(Foo,foo,properties={'data':relation(Data,backref=backref('foo',uselist=False))})
        Stat.mapper=mapper(Stat,stats,properties={'data':relation(Data)})

        s=create_session()
        data = []
        for x in range(5):
            d=Data()
            d.a=x
            s.save(d)
            data.append(d)
            
        for x in range(10):
            rid=random.randint(0,len(data) - 1)
            somedata=random.randint(1,50000)
            stat=Stat()
            stat.data = data[rid]
            stat.somedata=somedata
            s.save(stat)

        s.flush()

        arb_data=select(
            [stats.c.data_id,func.max(stats.c.somedata).label('max')],
            stats.c.data_id<=25,
            group_by=[stats.c.data_id]).alias('arb')
        
        arb_result = arb_data.execute().fetchall()
        # order the result list descending based on 'max'
        arb_result.sort(lambda a, b:cmp(b['max'],a['max']))
        # extract just the "data_id" from it
        arb_result = [row['data_id'] for row in arb_result]
        
        # now query for Data objects using that above select, adding the 
        # "order by max desc" separately
        q=s.query(Data).options(eagerload('foo')).select(
            from_obj=[datas.join(arb_data,arb_data.c.data_id==datas.c.id)],
            order_by=[desc(arb_data.c.max)],limit=10)
        
        # extract "data_id" from the list of result objects
        verify_result = [d.id for d in q]
        
        # assert equality including ordering (may break if the DB "ORDER BY" and python's sort() used differing
        # algorithms and there are repeated 'somedata' values in the list)
        assert verify_result == arb_result
        
if __name__ == "__main__":    
    testbase.main()
