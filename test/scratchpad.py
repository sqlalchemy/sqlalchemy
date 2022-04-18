"""Testing for new sqlalchemy functions"""

from sqlalchemy import Column
from sqlalchemy import create_engine
from sqlalchemy import DATE
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import Period
from sqlalchemy import Table
from sqlalchemy import TIMESTAMP
from sqlalchemy import VARCHAR
from sqlalchemy.schema import CreateTable

uri = "mariadb://scott:tiger@127.0.0.1:3306/versioning_test"
engine = create_engine(uri)
m = MetaData()

# testtab = Table(
#     "user_versioned",
#     metadata_obj,
#     Column("user_id", Integer, primary_key=True),
#     Column("user_name", String(16), nullable=False),
#     Column("start", TIMESTAMP, system_versioning="start"),
#     Column("end", TIMESTAMP, system_versioning="end"),
#     system_versioning=True,
# )

# t1 = Table("t1", m, Column("x", Integer), SystemTimePeriod())
t1 = Table(
    "Emp",
    m,
    Column("ENo", Integer),
    Column("ESart", DATE),
    Column("EEnd", DATE),
    Column("EDept", Integer),
    Period("SYSTEM_TIME", "Sys_start", "Sys_end"),
    Column("Sys_start", TIMESTAMP(12)),
    Column("Sys_end", TIMESTAMP(12)),
    Column("EName", VARCHAR(30)),
    # SystemTimePeriod("Sys_start", "Sys_end"),
    # PrimaryKeyConstraint("ENo", "Eperiod", without_overlaps=True),
    # ForeignKeyConstraint(
    #     ("EDept", "PERIOD EPeriod"),
    #     ("Dept.DNo", "PERIOD Dept.DPeriod"),
    # ),
)
print(CreateTable(t1).compile(engine))


# metadata_obj.create_all(engine)
# Table(
#     "table_name",
# Version true by default if turned on by table
#     Column("version_me_by_default", String(50)),
#     Column("dont_version_me", String(50), system_versioning=False),
#     # Column("start_timestamp", system_versioning="start"),
#     # Column("end_timestamp",  system_versioning="end"),
#     # system_versioning=True,
# default partitioning as of mariadb 10.5.0
#     # partitioning=partition(on=partition.system_time)
#     # partitioning=partition(on=partition.system_time,
#           interval=timedelta(months=1), partitions=12)
#     # partitioning=partition(limit=12000)

# )
