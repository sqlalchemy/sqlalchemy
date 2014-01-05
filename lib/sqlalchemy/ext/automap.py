# ext/automap.py
# Copyright (C) 2005-2013 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Define an extension to the :mod:`sqlalchemy.ext.declarative` system
which automatically generates mapped classes and attributes from a database
schema, typically one which is reflected.

.. versionadded:: 0.9.1 Added :mod:`sqlalchemy.ext.automap`.

.. note::

    The :mod:`sqlalchemy.ext.automap` extension should be considered
    **experimental** as of 0.9.1.   Featureset and API stability is
    not guaranteed at this time.

Features:

* The given :class:`.MetaData` structure may or may not be reflected.
  :mod:`.automap` isn't dependent on this.

* Classes which are known to be present in the :mod:`.automap` structure
  can be pre-declared with known attributes and settings.

* The system integrates with the featureset of :mod:`.declarative`, including
  support of mixins, abstract bases, interoperability with non-automapped
  classes.

* The system can build out classes for an entire :class:`.MetaData` structure
  or for individual :class:`.Table` objects.

* Relationships between classes are generated based on foreign keys, including
  that simple many-to-many relationships are also detectable.

* Hooks are provided for many key points, including:

    * A function which converts the name of table into a mapped class

    * A function which receives a :class:`.Column` object to be mapped and
      produces the element to be part of the mapping.

    * A function which receives two classes which should generate a
      :func:`.relationship` and produces the actual :func:`.relationship`.

    * Functions which produce attribute names; given a scalar column,
      or a class name for a scalar or collection reference, produce an attribute
      name.

"""
from sqlalchemy.ext.declarative import declarative_base, DeferredReflection
from sqlalchemy.ext.declarative.base import _DeferredMapperConfig
from sqlalchemy.schema import ForeignKeyConstraint
from sqlalchemy.orm import relationship, backref
from sqlalchemy import util

def _classname_for_table(table):
    return str(table.name)

def automap_base(**kw):
    Base = declarative_base(**kw)

    class BaseThing(DeferredReflection, Base):
        __abstract__ = True

        registry = util.Properties({})

        @classmethod
        def prepare(cls, engine):
            cls.metadata.reflect(
                        engine,
                        extend_existing=True,
                        autoload_replace=False
                    )

            table_to_map_config = dict(
                                    (m.local_table, m)
                                    for m in _DeferredMapperConfig.classes_for_base(cls)
                                )

            for table in cls.metadata.tables.values():
                if table not in table_to_map_config:
                    mapped_cls = type(
                        _classname_for_table(table),
                        (BaseThing, ),
                        {"__table__": table}
                    )
                    map_config = _DeferredMapperConfig.config_for_cls(mapped_cls)
                    table_to_map_config[table] = map_config

            for map_config in table_to_map_config.values():
                _relationships_for_fks(map_config, table_to_map_config)
                cls.registry[map_config.cls.__name__] = map_config.cls
            super(BaseThing, cls).prepare(engine)


        @classmethod
        def _sa_decl_prepare(cls, local_table, engine):
            pass

    return BaseThing

def _relationships_for_fks(map_config, table_to_map_config):
    local_table = map_config.local_table
    local_cls = map_config.cls
    for constraint in local_table.constraints:
        if isinstance(constraint, ForeignKeyConstraint):
            fks = constraint.elements
            referred_table = fks[0].column.table
            referred_cls = table_to_map_config[referred_table].cls

            map_config.properties[referred_cls.__name__.lower()] = \
                relationship(referred_cls,
                        foreign_keys=[fk.parent for fk in constraint.elements],
                        backref=backref(
                                local_cls.__name__.lower() + "_collection",
                            )
                        )

