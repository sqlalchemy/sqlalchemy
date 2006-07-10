import re
import cgi
import sqlalchemy.exceptions as exceptions

class URL(object):
    def __init__(self, drivername, username=None, password=None, host=None, port=None, database=None, query=None):
        self.drivername = drivername
        self.username = username
        self.password = password
        self.host = host
        if port is not None:
            self.port = int(port)
        else:
            self.port = None
        self.database= database
        self.query = query or {}
    def __str__(self):
        s = self.drivername + "://"
        if self.username is not None:
            s += self.username
            if self.password is not None:
                s += ':' + self.password
            s += "@"
        if self.host is not None:
            s += self.host
        if self.port is not None:
            s += ':' + str(self.port)
        if self.database is not None:
            s += '/' + self.database
        if len(self.query):
            keys = self.query.keys()
            keys.sort()
            s += '?' + "&".join(["%s=%s" % (k, self.query[k]) for k in keys])
        return s
    def get_module(self):
        return getattr(__import__('sqlalchemy.databases.%s' % self.drivername).databases, self.drivername)
    def translate_connect_args(self, names):
        """translates this URL's attributes into a dictionary of connection arguments used by a specific dbapi.
        the names parameter is a list of argument names in the form ('host', 'database', 'user', 'password', 'port')
        where the given strings match the corresponding argument names for the dbapi.  Will return a dictionary
        with the dbapi-specific parameters."""
        a = {}
        attribute_names = ['host', 'database', 'username', 'password', 'port']
        for n in names:
            sname = attribute_names.pop(0)
            if n is None:
                continue
            if getattr(self, sname, None):
                a[n] = getattr(self, sname)
        return a
    

def make_url(name_or_url):
    if isinstance(name_or_url, str) or isinstance(name_or_url, unicode):
        return _parse_rfc1738_args(name_or_url)
    else:
        return name_or_url
        
def _parse_rfc1738_args(name):
    pattern = re.compile(r'''
            (\w+)://
            (?:
                ([^:/]*)
                (?::([^/]*))?
            @)?
            (?:
                ([^/:]*)
                (?::([^/]*))?
            )?
            (?:/(.*))?
            '''
            , re.X)
    
    m = pattern.match(name)
    if m is not None:
        (name, username, password, host, port, database) = m.group(1, 2, 3, 4, 5, 6)
        if database is not None:
            tokens = database.split(r"?", 2)
            database = tokens[0]
            query = (len(tokens) > 1 and dict( cgi.parse_qsl(tokens[1]) ) or None)
        else:
            query = None
        opts = {'username':username,'password':password,'host':host,'port':port,'database':database, 'query':query}
        return URL(name, **opts)
    else:
        raise exceptions.ArgumentError("Could not parse rfc1738 URL from string '%s'" % name)

def _parse_keyvalue_args(name):
    m = re.match( r'(\w+)://(.*)', name)
    if m is not None:
        (name, args) = m.group(1, 2)
        opts = dict( cgi.parse_qsl( args ) )
        return URL(name, *opts)
    else:
        return None
    
