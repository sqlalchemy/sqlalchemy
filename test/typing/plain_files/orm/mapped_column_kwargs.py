from sqlalchemy import Boolean
from sqlalchemy import func
from sqlalchemy import Integer
from sqlalchemy import literal_column
from sqlalchemy import text
from sqlalchemy import true
from sqlalchemy.orm import mapped_column
from sqlalchemy.sql.schema import FetchedValue
from sqlalchemy.sql.schema import SchemaConst


mapped_column()
mapped_column(
    init=True,
    repr=True,
    default=42,
    compare=True,
    kw_only=True,
    primary_key=True,
    deferred=True,
    deferred_group="str",
    deferred_raiseload=True,
    use_existing_column=True,
    name="str",
    type_=Integer(),
    doc="str",
    key="str",
    index=True,
    unique=True,
    info={"str": 42},
    active_history=True,
    quote=True,
    system=True,
    comment="str",
    sort_order=-1,
    any_kwarg="str",
    another_kwarg=42,
)

mapped_column(default_factory=lambda: 1)
mapped_column(default_factory=lambda: "str")

mapped_column(nullable=True)
mapped_column(nullable=SchemaConst.NULL_UNSPECIFIED)

mapped_column(autoincrement=True)
mapped_column(autoincrement="auto")
mapped_column(autoincrement="ignore_fk")

mapped_column(onupdate=1)
mapped_column(onupdate="str")

mapped_column(insert_default=1)
mapped_column(insert_default="str")

mapped_column(server_default=FetchedValue())
mapped_column(server_default=true())
mapped_column(server_default=func.now())
mapped_column(server_default="NOW()")
mapped_column(server_default=text("NOW()"))
mapped_column(server_default=literal_column("false", Boolean))

mapped_column(server_onupdate=FetchedValue())
mapped_column(server_onupdate=true())
mapped_column(server_onupdate=func.now())
mapped_column(server_onupdate="NOW()")
mapped_column(server_onupdate=text("NOW()"))
mapped_column(server_onupdate=literal_column("false", Boolean))

mapped_column(
    default=None,
    nullable=None,
    primary_key=None,
    deferred_group=None,
    deferred_raiseload=None,
    name=None,
    type_=None,
    doc=None,
    key=None,
    index=None,
    unique=None,
    info=None,
    onupdate=None,
    insert_default=None,
    server_default=None,
    server_onupdate=None,
    quote=None,
    comment=None,
    any_kwarg=None,
)
