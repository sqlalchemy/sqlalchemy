from sqlalchemy.dialects.maxdb.base import MaxDBDialect

class MaxDBDialect_sapdb(MaxDBDialect):
    driver = 'sapdb'
    
    @classmethod
    def dbapi(cls):
        from sapdb import dbapi as _dbapi
        return _dbapi

    def create_connect_args(self, url):
        opts = url.translate_connect_args(username='user')
        opts.update(url.query)
        return [], opts


dialect = MaxDBDialect_sapdb