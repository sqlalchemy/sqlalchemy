# ext/serializer.py
# Copyright (C) 2005-2026 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php

"""Serializer/Deserializer objects for usage with SQLAlchemy query structures,
allowing "contextual" deserialization.

.. legacy::

    The serializer extension is **legacy** and should not be used for
    new development.

Any SQLAlchemy query structure, either based on sqlalchemy.sql.*
or sqlalchemy.orm.* can be used.  The mappers, Tables, Columns, Session
etc. which are referenced by the structure are not persisted in serialized
form, but are instead re-associated with the query structure
when it is deserialized.

.. warning:: The serializer extension uses pickle to serialize and
   deserialize objects, so the same security consideration mentioned
   in the `python documentation
   <https://docs.python.org/3/library/pickle.html>`_ apply.

Usage is nearly the same as that of the standard Python pickle module::

    from sqlalchemy.ext.serializer import loads, dumps

    metadata = MetaData(bind=some_engine)
    Session = scoped_session(sessionmaker())

    # ... define mappers

    query = (
        Session.query(MyClass)
        .filter(MyClass.somedata == "foo")
        .order_by(MyClass.sortkey)
    )

    # pickle the query
    serialized = dumps(query)

    # unpickle.  Pass in metadata + scoped_session
    query2 = loads(serialized, metadata, Session)

    print(query2.all())

Similar restrictions as when using raw pickle apply; mapped classes must be
themselves be pickleable, meaning they are importable from a module-level
namespace.

The serializer module is only appropriate for query structures.  It is not
needed for:

* instances of user-defined classes.   These contain no references to engines,
  sessions or expression constructs in the typical case and can be serialized
  directly.

* Table metadata that is to be loaded entirely from the serialized structure
  (i.e. is not already declared in the application).   Regular
  pickle.loads()/dumps() can be used to fully dump any ``MetaData`` object,
  typically one which was reflected from an existing database at some previous
  point in time.  The serializer module is specifically for the opposite case,
  where the Table metadata is already present in memory.

"""

from __future__ import annotations

from io import BytesIO
import pickle
import re
from typing import Any
from typing import cast
from typing import IO
from typing import Mapping
from typing import Protocol

from .. import Column
from .. import Table
from ..engine import Connection
from ..engine import Engine
from ..orm import class_mapper
from ..orm.interfaces import MapperProperty
from ..orm.mapper import Mapper
from ..orm.session import Session
from ..util import b64decode
from ..util import b64encode

__all__ = ["Serializer", "Deserializer", "dumps", "loads"]


class _HasTables(Protocol):
    @property
    def tables(self) -> Mapping[str, Table]: ...


class _HasBind(Protocol):
    @property
    def bind(self) -> Engine | Connection | None: ...


class _ScopedSession(Protocol):
    def __call__(self) -> _HasBind: ...


class _HasClauseElement(Protocol):
    def __clause_element__(self) -> object: ...


class Serializer(pickle.Pickler):

    def persistent_id(self, obj: object) -> str | None:
        # print "serializing:", repr(obj)
        if isinstance(obj, Mapper):
            id_ = "mapper:" + b64encode(pickle.dumps(obj.class_))
        elif isinstance(obj, MapperProperty):
            id_ = (
                "mapperprop:"
                + b64encode(pickle.dumps(obj.parent.class_))
                + ":"
                + obj.key
            )
        elif isinstance(obj, Table):
            if "parententity" in obj._annotations:
                id_ = "mapper_selectable:" + b64encode(
                    pickle.dumps(obj._annotations["parententity"].class_)
                )
            else:
                id_ = f"table:{obj.key}"
        elif isinstance(obj, Column) and isinstance(obj.table, Table):
            id_ = f"column:{obj.table.key}:{obj.key}"
        elif isinstance(obj, Session):
            id_ = "session:"
        elif isinstance(obj, Engine):
            id_ = "engine:"
        else:
            return None
        return id_


our_ids = re.compile(
    r"(mapperprop|mapper|mapper_selectable|table|column|"
    r"session|attribute|engine):(.*)"
)


class Deserializer(pickle.Unpickler):

    def __init__(
        self,
        file: IO[bytes],
        metadata: _HasTables | None = None,
        scoped_session: _ScopedSession | None = None,
        engine: Engine | None = None,
    ) -> None:
        super().__init__(file)
        self.metadata = metadata
        self.scoped_session = scoped_session
        self.engine = engine

    def get_engine(self) -> Engine | Connection | None:
        if self.engine is not None:
            return self.engine
        elif self.scoped_session is not None:
            return self.scoped_session().bind
        else:
            return None

    def persistent_load(self, id_: object) -> object:
        m = our_ids.match(str(id_))
        if not m:
            return None
        else:
            type_, args = m.group(1, 2)
            if type_ == "attribute":
                key, clsarg = args.split(":")
                cls = cast(type[object], pickle.loads(b64decode(clsarg)))
                return getattr(cls, key)
            elif type_ == "mapper":
                cls = cast(type[object], pickle.loads(b64decode(args)))
                return class_mapper(cls)
            elif type_ == "mapper_selectable":
                cls = cast(type[object], pickle.loads(b64decode(args)))
                return cast(
                    _HasClauseElement, class_mapper(cls)
                ).__clause_element__()
            elif type_ == "mapperprop":
                mapper, keyname = args.split(":")
                cls = cast(type[object], pickle.loads(b64decode(mapper)))
                return class_mapper(cls).attrs[keyname]
            elif type_ == "table":
                metadata = cast(_HasTables, self.metadata)
                return metadata.tables[args]
            elif type_ == "column":
                metadata = cast(_HasTables, self.metadata)
                table, colname = args.split(":")
                return metadata.tables[table].c[colname]
            elif type_ == "session":
                session_factory = cast(_ScopedSession, self.scoped_session)
                return session_factory()
            elif type_ == "engine":
                return self.get_engine()
            else:
                raise Exception("Unknown token: %s" % type_)


def dumps(
    obj: object, protocol: int | None = pickle.HIGHEST_PROTOCOL
) -> bytes:
    buf = BytesIO()
    pickler = Serializer(buf, protocol)
    pickler.dump(obj)
    return buf.getvalue()


def loads(
    data: bytes,
    metadata: _HasTables | None = None,
    scoped_session: _ScopedSession | None = None,
    engine: Engine | None = None,
) -> Any:
    buf = BytesIO(data)
    unpickler = Deserializer(buf, metadata, scoped_session, engine)
    return unpickler.load()
