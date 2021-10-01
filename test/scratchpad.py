"""Testing for new sqlalchemy functions"""

from sqlalchemy import (
    Table,
    Column,
    String,
    MetaData,
    create_engine,
    Integer,
    TIMESTAMP,
    DATE
)
from sqlalchemy.schema import (
    CreateTable
)

metadata_obj = MetaData()
uri = "mariadb://scott:tiger@127.0.0.1:3306/versioning_test"
engine = create_engine(uri)

testtab = Table('user_versioned', metadata_obj,
            Column('user_id', Integer, primary_key=True),
            Column('user_name', String(16), nullable=False),
            Column('start', TIMESTAMP, system_versioning="start"),
            Column('start', TIMESTAMP, system_versioning="end"),
            system_versioning=True,
            )

print(CreateTable(testtab).compile(engine))


# metadata_obj.create_all(engine)
# Table(
#     "table_name",
#     Column("version_me_by_default", String(50)),     # Version true by default if turned on by table
#     Column("dont_version_me", String(50), system_versioning=False),
#     # Column("start_timestamp", system_versioning="start"),
#     # Column("end_timestamp",  system_versioning="end"),
#     # system_versioning=True,
#     # partitioning=partition(on=partition.system_time)  # default partitioning as of mariadb 10.5.0
#     # partitioning=partition(on=partition.system_time, interval=timedelta(months=1), partitions=12)
#     # partitioning=partition(limit=12000)

# )
