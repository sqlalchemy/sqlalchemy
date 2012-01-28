from sqlalchemy import *
from sqlalchemy.orm import *
from sqlalchemy.ext.declarative import declarative_base, declared_attr

class DeclarativeReflectedBase(object):
    _mapper_args = []

    @classmethod
    def __mapper_cls__(cls, *args, **kw):
        """Declarative will use this function in lieu of 
        calling mapper() directly.
        
        Collect each series of arguments and invoke
        them when prepare() is called.
        """

        cls._mapper_args.append((args, kw))

    @classmethod
    def prepare(cls, engine):
        """Reflect all the tables and map !"""
        for args, kw in cls._mapper_args:
            klass = args[0]
            klass.__table__ = table = Table(
                                        klass.__tablename__, 
                                        cls.metadata, 
                                        extend_existing=True,
                                        autoload_replace=False,
                                        autoload=True, 
                                        autoload_with=engine,
                                        )
            klass.__mapper__ = mapper(klass, table, **kw)


if __name__ == '__main__':
    Base= declarative_base(cls=DeclarativeReflectedBase)

    class Foo(Base):
        __tablename__ = 'foo'
        bars = relationship("Bar")

    class Bar(Base):
        __tablename__ = 'bar'

        # illustrate overriding of "bar.foo_id" to have 
        # a foreign key constraint otherwise not
        # reflected, such as when using MySQL
        foo_id = Column(Integer, ForeignKey('foo.id'))

    e = create_engine('sqlite://', echo=True)
    e.execute("""
    create table foo(
        id integer primary key,
        data varchar(30)
    )
    """)

    e.execute("""
    create table bar(
        id integer primary key,
        data varchar(30),
        foo_id integer
    )
    """)

    Base.prepare(e)

    s = Session(e)

    s.add_all([
        Foo(bars=[Bar(data='b1'), Bar(data='b2')], data='f1'),
        Foo(bars=[Bar(data='b3'), Bar(data='b4')], data='f2')
    ])
    s.commit()
    for f in s.query(Foo):
        print f.data, ",".join([b.data for b in f.bars])