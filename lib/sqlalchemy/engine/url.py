"""Provide the URL object as well as the make_url parsing function."""

import re, cgi, sys, urllib
from sqlalchemy import exceptions


class URL(object):
    """Represent the components of a URL used to connect to a database.

    This object is suitable to be passed directly to a ``create_engine()``
    call.  The fields of the URL are parsed from a string by the
    ``module-level make_url()`` function.  the string format of the URL is
    an RFC-1738-style string.

    Attributes on URL include:

    drivername
      The name of the database backend.  this name will correspond to a module in sqlalchemy/databases

    username
      The user name for the connection.

    password
      database password.

    host
      The name of the host.

    port
      The port number.

    database
      The database.

    query
      A dictionary containing key/value pairs representing the URL's query string.
    """

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
                s += ':' + urllib.quote_plus(self.password)
            s += "@"
        if self.host is not None:
            s += self.host
        if self.port is not None:
            s += ':' + str(self.port)
        if self.database is not None:
            s += '/' + self.database
        if self.query:
            keys = self.query.keys()
            keys.sort()
            s += '?' + "&".join(["%s=%s" % (k, self.query[k]) for k in keys])
        return s
    
    def __eq__(self, other):
        return \
            isinstance(other, URL) and \
            self.drivername == other.drivername and \
            self.username == other.username and \
            self.password == other.password and \
            self.host == other.host and \
            self.database == other.database and \
            self.query == other.query
            
    def get_dialect(self):
        """Return the SQLAlchemy database dialect class corresponding to this URL's driver name."""
        dialect=None
        if self.drivername == 'ansi':
            import sqlalchemy.ansisql
            return sqlalchemy.ansisql.dialect

        try:
            module=getattr(__import__('sqlalchemy.databases.%s' % self.drivername).databases, self.drivername)
            dialect=module.dialect
        except ImportError:
            if sys.exc_info()[2].tb_next is None:
                import pkg_resources
                for res in pkg_resources.iter_entry_points('sqlalchemy.databases'):
                    if res.name==self.drivername:
                        dialect=res.load()
            else:
               raise
        if dialect is not None:
            return dialect
        raise ImportError('unknown database %r' % self.drivername) 
  
    def translate_connect_args(self, names):
        """Translate this URL's attributes into a dictionary of connection arguments.

        Given a list of argument names corresponding to the URL
        attributes (`host`, `database`, `username`, `password`,
        `port`), will assemble the attribute values of this URL into
        the dictionary using the given names.
        """

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
    """Given a string or unicode instance, produce a new URL instance.

    The given string is parsed according to the rfc1738 spec.  If an
    existing URL object is passed, just returns the object.
    """

    if isinstance(name_or_url, basestring):
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
            if query is not None:
                query = dict([(k.encode('ascii'), query[k]) for k in query])
        else:
            query = None
        opts = {'username':username,'password':password,'host':host,'port':port,'database':database, 'query':query}
        if opts['password'] is not None:
            opts['password'] = urllib.unquote_plus(opts['password'])
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
