from sqlalchemy import *

class NoSuchTableError(SQLAlchemyError): pass

# metaclass is necessary to expose class methods with getattr, e.g.
# we want to pass db.users.select through to users._mapper.select
class TableClassType(type):
    def insert(cls, **kwargs):
        o = cls()
        o.__dict__.update(kwargs)
        return o
    def __getattr__(cls, attr):
        if attr == '_mapper':
            # called during mapper init
            raise AttributeError()
        return getattr(cls._mapper, attr)

def class_for_table(table):
    klass = TableClassType('Class_' + table.name.capitalize(), (object,), {})
    def __repr__(self):
        import locale
        encoding = locale.getdefaultlocale()[1]
        L = []
        for k in self.__class__.c.keys():
            value = getattr(self, k, '')
            if isinstance(value, unicode):
                value = value.encode(encoding)
            L.append("%s=%r" % (k, value))
        return '%s(%s)' % (self.__class__.__name__, ','.join(L))
    klass.__repr__ = __repr__
    klass._mapper = mapper(klass, table)
    return klass

class SqlSoup:
    def __init__(self, *args, **kwargs):
        """
        args may either be an SQLEngine or a set of arguments suitable
        for passing to create_engine
        """
        from sqlalchemy.sql import Engine
        # meh, sometimes having method overloading instead of kwargs would be easier
        if isinstance(args[0], Engine):
            engine = args.pop(0)
            if args or kwargs:
                raise ArgumentError('Extra arguments not allowed when engine is given')
        else:
            engine = create_engine(*args, **kwargs)
        self._engine = engine
        self._cache = {}
    def delete(self, *args, **kwargs):
        objectstore.delete(*args, **kwargs)
    def commit(self):
        objectstore.get_session().commit()
    def rollback(self):
        objectstore.clear()
    def _reset(self):
        # for debugging
        self._cache = {}
        self.rollback()
    def __getattr__(self, attr):
        try:
            t = self._cache[attr]
        except KeyError:
            table = Table(attr, self._engine, autoload=True)
            if table.columns:
                t = class_for_table(table)
            else:
                t = None
            self._cache[attr] = t
        if not t:
            raise NoSuchTableError('%s does not exist' % attr)
        return t
