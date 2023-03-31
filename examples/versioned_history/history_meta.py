"""Versioned mixin class and other utilities."""

import datetime

from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import event
from sqlalchemy import ForeignKeyConstraint
from sqlalchemy import inspect
from sqlalchemy import Integer
from sqlalchemy import PrimaryKeyConstraint
from sqlalchemy import util
from sqlalchemy.orm import attributes
from sqlalchemy.orm import object_mapper
from sqlalchemy.orm.exc import UnmappedColumnError
from sqlalchemy.orm.relationships import RelationshipProperty


def col_references_table(col, table):
    for fk in col.foreign_keys:
        if fk.references(table):
            return True
    return False


def _is_versioning_col(col):
    return "version_meta" in col.info


def _history_mapper(local_mapper):

    cls = local_mapper.class_

    if cls.__dict__.get("_history_mapper_configured", False):
        return

    cls._history_mapper_configured = True

    super_mapper = local_mapper.inherits
    polymorphic_on = None
    super_fks = []
    properties = util.OrderedDict()

    if super_mapper:
        super_history_mapper = super_mapper.class_.__history_mapper__
    else:
        super_history_mapper = None

    if (
        not super_mapper
        or local_mapper.local_table is not super_mapper.local_table
    ):
        version_meta = {"version_meta": True}  # add column.info to identify
        # columns specific to versioning

        history_table = local_mapper.local_table.to_metadata(
            local_mapper.local_table.metadata,
            name=local_mapper.local_table.name + "_history",
        )

        for orig_c, history_c in zip(
            local_mapper.local_table.c, history_table.c
        ):
            orig_c.info["history_copy"] = history_c
            history_c.unique = False
            history_c.default = history_c.server_default = None
            history_c.autoincrement = False

            if super_mapper and col_references_table(
                orig_c, super_mapper.local_table
            ):
                assert super_history_mapper is not None
                super_fks.append(
                    (
                        history_c.key,
                        list(super_history_mapper.local_table.primary_key)[0],
                    )
                )
            if orig_c is local_mapper.polymorphic_on:
                polymorphic_on = history_c

            orig_prop = local_mapper.get_property_by_column(orig_c)
            # carry over column re-mappings
            if (
                len(orig_prop.columns) > 1
                or orig_prop.columns[0].key != orig_prop.key
            ):
                properties[orig_prop.key] = tuple(
                    col.info["history_copy"] for col in orig_prop.columns
                )

        for const in list(history_table.constraints):
            if not isinstance(
                const, (PrimaryKeyConstraint, ForeignKeyConstraint)
            ):
                history_table.constraints.discard(const)

        # "version" stores the integer version id.  This column is
        # required.
        history_table.append_column(
            Column(
                "version",
                Integer,
                primary_key=True,
                autoincrement=False,
                info=version_meta,
            )
        )

        # "changed" column stores the UTC timestamp of when the
        # history row was created.
        # This column is optional and can be omitted.
        history_table.append_column(
            Column(
                "changed",
                DateTime,
                default=datetime.datetime.utcnow,
                info=version_meta,
            )
        )

        if super_mapper:
            super_fks.append(
                ("version", super_history_mapper.local_table.c.version)
            )

        if super_fks:
            history_table.append_constraint(
                ForeignKeyConstraint(*zip(*super_fks))
            )

    else:
        history_table = None
        super_history_table = super_mapper.local_table.metadata.tables[
            super_mapper.local_table.name + "_history"
        ]

        # single table inheritance.  take any additional columns that may have
        # been added and add them to the history table.
        for column in local_mapper.local_table.c:
            if column.key not in super_history_table.c:
                col = Column(
                    column.name, column.type, nullable=column.nullable
                )
                super_history_table.append_column(col)

    if not super_mapper:
        local_mapper.local_table.append_column(
            Column("version", Integer, default=1, nullable=False),
            replace_existing=True,
        )
        local_mapper.add_property(
            "version", local_mapper.local_table.c.version
        )

        if cls.use_mapper_versioning:
            local_mapper.version_id_col = local_mapper.local_table.c.version

    # set the "active_history" flag
    # on on column-mapped attributes so that the old version
    # of the info is always loaded (currently sets it on all attributes)
    for prop in local_mapper.iterate_properties:
        prop.active_history = True

    super_mapper = local_mapper.inherits

    if super_history_mapper:
        bases = (super_history_mapper.class_,)

        if history_table is not None:
            properties["changed"] = (history_table.c.changed,) + tuple(
                super_history_mapper.attrs.changed.columns
            )

    else:
        bases = local_mapper.base_mapper.class_.__bases__

    versioned_cls = type(
        "%sHistory" % cls.__name__,
        bases,
        {
            "_history_mapper_configured": True,
            "__table__": history_table,
            "__mapper_args__": dict(
                inherits=super_history_mapper,
                polymorphic_identity=local_mapper.polymorphic_identity,
                polymorphic_on=polymorphic_on,
                properties=properties,
            ),
        },
    )

    cls.__history_mapper__ = versioned_cls.__mapper__


class Versioned:
    use_mapper_versioning = False
    """if True, also assign the version column to be tracked by the mapper"""

    __table_args__ = {"sqlite_autoincrement": True}
    """Use sqlite_autoincrement, to ensure unique integer values
    are used for new rows even for rows that have been deleted."""

    def __init_subclass__(cls) -> None:
        insp = inspect(cls, raiseerr=False)

        if insp is not None:
            _history_mapper(insp)
        else:

            @event.listens_for(cls, "after_mapper_constructed")
            def _mapper_constructed(mapper, class_):
                _history_mapper(mapper)

        super().__init_subclass__()


def versioned_objects(iter_):
    for obj in iter_:
        if hasattr(obj, "__history_mapper__"):
            yield obj


def create_version(obj, session, deleted=False):
    obj_mapper = object_mapper(obj)
    history_mapper = obj.__history_mapper__
    history_cls = history_mapper.class_

    obj_state = attributes.instance_state(obj)

    attr = {}

    obj_changed = False

    for om, hm in zip(
        obj_mapper.iterate_to_root(), history_mapper.iterate_to_root()
    ):
        if hm.single:
            continue

        for hist_col in hm.local_table.c:
            if _is_versioning_col(hist_col):
                continue

            obj_col = om.local_table.c[hist_col.key]

            # get the value of the
            # attribute based on the MapperProperty related to the
            # mapped column.  this will allow usage of MapperProperties
            # that have a different keyname than that of the mapped column.
            try:
                prop = obj_mapper.get_property_by_column(obj_col)
            except UnmappedColumnError:
                # in the case of single table inheritance, there may be
                # columns on the mapped table intended for the subclass only.
                # the "unmapped" status of the subclass column on the
                # base class is a feature of the declarative module.
                continue

            # expired object attributes and also deferred cols might not
            # be in the dict.  force it to load no matter what by
            # using getattr().
            if prop.key not in obj_state.dict:
                getattr(obj, prop.key)

            a, u, d = attributes.get_history(obj, prop.key)

            if d:
                attr[prop.key] = d[0]
                obj_changed = True
            elif u:
                attr[prop.key] = u[0]
            elif a:
                # if the attribute had no value.
                attr[prop.key] = a[0]
                obj_changed = True

    if not obj_changed:
        # not changed, but we have relationships.  OK
        # check those too
        for prop in obj_mapper.iterate_properties:
            if (
                isinstance(prop, RelationshipProperty)
                and attributes.get_history(
                    obj, prop.key, passive=attributes.PASSIVE_NO_INITIALIZE
                ).has_changes()
            ):
                for p in prop.local_columns:
                    if p.foreign_keys:
                        obj_changed = True
                        break
                if obj_changed is True:
                    break

    if not obj_changed and not deleted:
        return

    attr["version"] = obj.version
    hist = history_cls()
    for key, value in attr.items():
        setattr(hist, key, value)
    session.add(hist)
    obj.version += 1


def versioned_session(session):
    @event.listens_for(session, "before_flush")
    def before_flush(session, flush_context, instances):
        for obj in versioned_objects(session.dirty):
            create_version(obj, session)
        for obj in versioned_objects(session.deleted):
            create_version(obj, session, deleted=True)
