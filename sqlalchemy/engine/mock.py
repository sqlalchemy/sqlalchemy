# engine/mock.py
# Copyright (C) 2005-2019 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from operator import attrgetter

from . import base
from . import url as _url
from .. import util
from ..sql import schema
from ..sql import ddl


class MockConnection(base.Connectable):
    def __init__(self, dialect, execute):
        self._dialect = dialect
        self.execute = execute

    engine = property(lambda s: s)
    dialect = property(attrgetter("_dialect"))
    name = property(lambda s: s._dialect.name)

    schema_for_object = schema._schema_getter(None)

    def connect(self, **kwargs):
        return self

    def execution_options(self, **kw):
        return self

    def compiler(self, statement, parameters, **kwargs):
        return self._dialect.compiler(
            statement, parameters, engine=self, **kwargs
        )

    def create(self, entity, **kwargs):
        kwargs["checkfirst"] = False

        ddl.SchemaGenerator(self.dialect, self, **kwargs).traverse_single(
            entity
        )

    def drop(self, entity, **kwargs):
        kwargs["checkfirst"] = False

        ddl.SchemaDropper(self.dialect, self, **kwargs).traverse_single(entity)

    def _run_visitor(
        self, visitorcallable, element, connection=None, **kwargs
    ):
        kwargs["checkfirst"] = False
        visitorcallable(self.dialect, self, **kwargs).traverse_single(element)

    def execute(self, object_, *multiparams, **params):
        raise NotImplementedError()


def create_mock_engine(url, executor, **kw):
    """Create a "mock" engine used for echoing DDL.

    .. versionadded:: 2.0

    """

    # create url.URL object
    u = _url.make_url(url)

    dialect_cls = u.get_dialect()

    dialect_args = {}
    # consume dialect arguments from kwargs
    for k in util.get_cls_kwargs(dialect_cls):
        if k in kw:
            dialect_args[k] = kwargs.pop(k)

    # create dialect
    dialect = dialect_cls(**dialect_args)

    return MockConnection(dialect, executor)
