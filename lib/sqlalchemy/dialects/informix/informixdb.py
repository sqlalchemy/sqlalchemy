from sqlalchemy.dialects.informix.base import InformixDialect
from sqlalchemy.engine import default

# for offset

class informix_cursor(object):
    def __init__( self , con ):
        self.__cursor = con.cursor()
        self.rowcount = 0

    def offset( self , n ):
        if n > 0:
            self.fetchmany( n )
            self.rowcount = self.__cursor.rowcount - n
            if self.rowcount < 0:
                self.rowcount = 0
        else:
            self.rowcount = self.__cursor.rowcount

    def execute( self , sql , params ):
        if params is None or len( params ) == 0:
            params = []

        return self.__cursor.execute( sql , params )

    def __getattr__( self , name ):
        if name not in ( 'offset' , '__cursor' , 'rowcount' , '__del__' , 'execute' ):
            return getattr( self.__cursor , name )


class InfoExecutionContext(default.DefaultExecutionContext):
    # cursor.sqlerrd
    # 0 - estimated number of rows returned
    # 1 - serial value after insert or ISAM error code
    # 2 - number of rows processed
    # 3 - estimated cost
    # 4 - offset of the error into the SQL statement
    # 5 - rowid after insert
    def post_exec(self):
        if getattr(self.compiled, "isinsert", False) and self.inserted_primary_key is None:
            self._last_inserted_ids = [self.cursor.sqlerrd[1]]
        elif hasattr( self.compiled , 'offset' ):
            self.cursor.offset( self.compiled.offset )

    def create_cursor( self ):
        return informix_cursor( self.connection.connection )


class Informix_informixdb(InformixDialect):
    driver = 'informixdb'
    default_paramstyle = 'qmark'
    execution_context_cls = InfoExecutionContext
    
    @classmethod
    def dbapi(cls):
        return __import__('informixdb')

    def create_connect_args(self, url):
        if url.host:
            dsn = '%s@%s' % ( url.database , url.host )
        else:
            dsn = url.database

        if url.username:
            opt = { 'user':url.username , 'password': url.password }
        else:
            opt = {}

        return ([dsn], opt)


    def is_disconnect(self, e):
        if isinstance(e, self.dbapi.OperationalError):
            return 'closed the connection' in str(e) or 'connection not open' in str(e)
        else:
            return False


dialect = Informix_informixdb