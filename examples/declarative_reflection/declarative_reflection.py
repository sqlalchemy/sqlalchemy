from sqlalchemy import *
from sqlalchemy.orm import *
from sqlalchemy.orm.util import _is_mapped_class
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
        while cls._mapper_args:
            args, kw  = cls._mapper_args.pop()
            klass = args[0]
            # autoload Table, which is already
            # present in the metadata.  This
            # will fill in db-loaded columns
            # into the existing Table object.
            if args[1] is not None:
                table = args[1]
                Table(table.name, 
                    cls.metadata, 
                    extend_existing=True,
                    autoload_replace=False,
                    autoload=True, 
                    autoload_with=engine,
                    schema=table.schema)

            # see if we need 'inherits' in the
            # mapper args.  Declarative will have 
            # skipped this since mappings weren't
            # available yet.
            for c in klass.__bases__:
                if _is_mapped_class(c):
                    kw['inherits'] = c
                    break

            klass.__mapper__ = mapper(*args, **kw)

if __name__ == '__main__':
    Base = declarative_base()

    # create a separate base so that we can
    # define a subset of classes as "Reflected",
    # instead of everything.
    class Reflected(DeclarativeReflectedBase, Base):
        __abstract__ = True

    class Foo(Reflected):
        __tablename__ = 'foo'
        bars = relationship("Bar")

    class Bar(Reflected):
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

    Reflected.prepare(e)

    s = Session(e)

    s.add_all([
        Foo(bars=[Bar(data='b1'), Bar(data='b2')], data='f1'),
        Foo(bars=[Bar(data='b3'), Bar(data='b4')], data='f2')
    ])
    s.commit()
    for f in s.query(Foo):
        print f.data, ",".join([b.data for b in f.bars])