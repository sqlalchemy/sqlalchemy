# engine/url.py
# Copyright (C) 2005-2019 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Provides the :class:`~sqlalchemy.engine.url.URL` class which encapsulates
information about a database connection specification.

The URL object is created automatically when
:func:`~sqlalchemy.engine.create_engine` is called with a string
argument; alternatively, the URL is a public-facing construct which can
be used directly and is also accepted directly by ``create_engine()``.
"""

import re

from .interfaces import Dialect
from .. import exc
from .. import util
from ..dialects import plugins
from ..dialects import registry


class URL(object):
    """
    Represent the components of a URL used to connect to a database.

    This object is suitable to be passed directly to a
    :func:`~sqlalchemy.create_engine` call.  The fields of the URL are parsed
    from a string by the :func:`.make_url` function.  the string
    format of the URL is an RFC-1738-style string.

    All initialization parameters are available as public attributes.

    :param drivername: the name of the database backend.
      This name will correspond to a module in sqlalchemy/databases
      or a third party plug-in.

    :param username: The user name.

    :param password: database password.

    :param host: The name of the host.

    :param port: The port number.

    :param database: The database name.

    :param query: A dictionary of options to be passed to the
      dialect and/or the DBAPI upon connect.

    """

    def __init__(
        self,
        drivername,
        username=None,
        password=None,
        host=None,
        port=None,
        database=None,
        query=None,
    ):
        self.drivername = drivername
        self.username = username
        self.password_original = password
        self.host = host
        if port is not None:
            self.port = int(port)
        else:
            self.port = None
        self.database = database
        self.query = query or {}

    def __to_string__(self, hide_password=True):
        s = self.drivername + "://"
        if self.username is not None:
            s += _rfc_1738_quote(self.username)
            if self.password is not None:
                s += ":" + (
                    "***" if hide_password else _rfc_1738_quote(self.password)
                )
            s += "@"
        if self.host is not None:
            if ":" in self.host:
                s += "[%s]" % self.host
            else:
                s += self.host
        if self.port is not None:
            s += ":" + str(self.port)
        if self.database is not None:
            s += "/" + self.database
        if self.query:
            keys = list(self.query)
            keys.sort()
            s += "?" + "&".join(
                "%s=%s" % (k, element)
                for k in keys
                for element in util.to_list(self.query[k])
            )
        return s

    def __str__(self):
        return self.__to_string__(hide_password=False)

    def __repr__(self):
        return self.__to_string__()

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, other):
        return (
            isinstance(other, URL)
            and self.drivername == other.drivername
            and self.username == other.username
            and self.password == other.password
            and self.host == other.host
            and self.database == other.database
            and self.query == other.query
        )

    @property
    def password(self):
        if self.password_original is None:
            return None
        else:
            return util.text_type(self.password_original)

    @password.setter
    def password(self, password):
        self.password_original = password

    def get_backend_name(self):
        if "+" not in self.drivername:
            return self.drivername
        else:
            return self.drivername.split("+")[0]

    def get_driver_name(self):
        if "+" not in self.drivername:
            return self.get_dialect().driver
        else:
            return self.drivername.split("+")[1]

    def _instantiate_plugins(self, kwargs):
        plugin_names = util.to_list(self.query.get("plugin", ()))
        plugin_names += kwargs.get("plugins", [])

        return [
            plugins.load(plugin_name)(self, kwargs)
            for plugin_name in plugin_names
        ]

    def _get_entrypoint(self):
        """Return the "entry point" dialect class.

        This is normally the dialect itself except in the case when the
        returned class implements the get_dialect_cls() method.

        """
        if "+" not in self.drivername:
            name = self.drivername
        else:
            name = self.drivername.replace("+", ".")
        cls = registry.load(name)
        # check for legacy dialects that
        # would return a module with 'dialect' as the
        # actual class
        if (
            hasattr(cls, "dialect")
            and isinstance(cls.dialect, type)
            and issubclass(cls.dialect, Dialect)
        ):
            return cls.dialect
        else:
            return cls

    def get_dialect(self):
        """Return the SQLAlchemy database dialect class corresponding
        to this URL's driver name.
        """
        entrypoint = self._get_entrypoint()
        dialect_cls = entrypoint.get_dialect_cls(self)
        return dialect_cls

    def translate_connect_args(self, names=[], **kw):
        r"""Translate url attributes into a dictionary of connection arguments.

        Returns attributes of this url (`host`, `database`, `username`,
        `password`, `port`) as a plain dictionary.  The attribute names are
        used as the keys by default.  Unset or false attributes are omitted
        from the final dictionary.

        :param \**kw: Optional, alternate key names for url attributes.

        :param names: Deprecated.  Same purpose as the keyword-based alternate
            names, but correlates the name to the original positionally.
        """

        translated = {}
        attribute_names = ["host", "database", "username", "password", "port"]
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

    if isinstance(name_or_url, util.string_types):
        return _parse_rfc1738_args(name_or_url)
    else:
        return name_or_url


def _parse_rfc1738_args(name):
    pattern = re.compile(
        r"""
            (?P<name>[\w\+]+)://
            (?:
                (?P<username>[^:/]*)
                (?::(?P<password>.*))?
            @)?
            (?:
                (?:
                    \[(?P<ipv6host>[^/]+)\] |
                    (?P<ipv4host>[^/:]+)
                )?
                (?::(?P<port>[^/]*))?
            )?
            (?:/(?P<database>.*))?
            """,
        re.X,
    )

    m = pattern.match(name)
    if m is not None:
        components = m.groupdict()
        if components["database"] is not None:
            tokens = components["database"].split("?", 2)
            components["database"] = tokens[0]

            if len(tokens) > 1:
                query = {}

                for key, value in util.parse_qsl(tokens[1]):
                    if util.py2k:
                        key = key.encode("ascii")
                    if key in query:
                        query[key] = util.to_list(query[key])
                        query[key].append(value)
                    else:
                        query[key] = value
            else:
                query = None
        else:
            query = None
        components["query"] = query

        if components["username"] is not None:
            components["username"] = _rfc_1738_unquote(components["username"])

        if components["password"] is not None:
            components["password"] = _rfc_1738_unquote(components["password"])

        ipv4host = components.pop("ipv4host")
        ipv6host = components.pop("ipv6host")
        components["host"] = ipv4host or ipv6host
        name = components.pop("name")
        return URL(name, **components)
    else:
        raise exc.ArgumentError(
            "Could not parse rfc1738 URL from string '%s'" % name
        )


def _rfc_1738_quote(text):
    return re.sub(r"[:@/]", lambda m: "%%%X" % ord(m.group(0)), text)


def _rfc_1738_unquote(text):
    return util.unquote(text)


def _parse_keyvalue_args(name):
    m = re.match(r"(\w+)://(.*)", name)
    if m is not None:
        (name, args) = m.group(1, 2)
        opts = dict(util.parse_qsl(args))
        return URL(name, *opts)
    else:
        return None
