"""Provides the [sqlalchemy.engine.url#URL] class which encapsulates
information about a database connection specification.

The URL object is created automatically when [sqlalchemy.engine#create_engine()] is called
with a string argument; alternatively, the URL is a public-facing construct which can
be used directly and is also accepted directly by ``create_engine()``.
"""

import re, cgi, sys, urllib
from sqlalchemy import exceptions


class URL(object):
    """Represent the components of a URL used to connect to a database.

    This object is suitable to be passed directly to a
    ``create_engine()`` call.  The fields of the URL are parsed from a
    string by the ``module-level make_url()`` function.  the string
    format of the URL is an RFC-1738-style string.

    Attributes on URL include:

    drivername
      the name of the database backend.  This name will correspond to
      a module in sqlalchemy/databases or a third party plug-in.

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
      A dictionary containing key/value pairs representing the URL's
      query string.
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
        
        try:
            module = getattr(__import__('sqlalchemy.databases.%s' % self.drivername).databases, self.drivername)
            return module.dialect
        except ImportError:
            if sys.exc_info()[2].tb_next is None:
                import pkg_resources
                for res in pkg_resources.iter_entry_points('sqlalchemy.databases'):
                    if res.name == self.drivername:
                        return res.load()
            raise
  
    def translate_connect_args(self, names=[], **kw):
        """Translate url attributes into a dictionary of connection arguments.

        Returns attributes of this url (`host`, `database`, `username`,
        `password`, `port`) as a plain dictionary.  The attribute names are
        used as the keys by default.  Unset or false attributes are omitted
        from the final dictionary.

        \**kw
          Optional, alternate key names for url attributes::

            # return 'username' as 'user'
            username='user'

            # omit 'database'
            database=None
          
        names
          Deprecated.  A list of key names. Equivalent to the keyword
          usage, must be provided in the order above.
        """

        translated = {}
        attribute_names = ['host', 'database', 'username', 'password', 'port']
        for sname in attribute_names:
            if names:
                name = names.pop(0)
            elif sname in kw:
                name = kw[sname]
            else:
                name = sname
            if name is not None and getattr(self, sname, False):
                translated[name] = getattr(self, sname)
        return translated

def make_url(name_or_url):
    """Given a string or unicode instance, produce a new URL instance.

    The given string is parsed according to the RFC 1738 spec.  If an
    existing URL object is passed, just returns the object.
    """

    if isinstance(name_or_url, basestring):
        return _parse_rfc1738_args(name_or_url)
    else:
        return name_or_url

def _parse_rfc1738_args(name):
    pattern = re.compile(r'''
            (?P<name>\w+)://
            (?:
                (?P<username>[^:/]*)
                (?::(?P<password>[^/]*))?
            @)?
            (?:
                (?P<host>[^/:]*)
                (?::(?P<port>[^/]*))?
            )?
            (?:/(?P<database>.*))?
            '''
            , re.X)

    m = pattern.match(name)
    if m is not None:
        components = m.groupdict()
        if components['database'] is not None:
            tokens = components['database'].split('?', 2)
            components['database'] = tokens[0]
            query = (len(tokens) > 1 and dict(cgi.parse_qsl(tokens[1]))) or None
            if query is not None:
                query = dict([(k.encode('ascii'), query[k]) for k in query])
        else:
            query = None
        components['query'] = query

        if components['password'] is not None:
            components['password'] = urllib.unquote_plus(components['password'])

        name = components.pop('name')
        return URL(name, **components)
    else:
        raise exceptions.ArgumentError(
            "Could not parse rfc1738 URL from string '%s'" % name)

def _parse_keyvalue_args(name):
    m = re.match( r'(\w+)://(.*)', name)
    if m is not None:
        (name, args) = m.group(1, 2)
        opts = dict( cgi.parse_qsl( args ) )
        return URL(name, *opts)
    else:
        return None
