from sqlalchemy import *

ischema = MetaData()

tables = Table("SYSTABLE", ischema,
    Column("table_id", Integer, primary_key=True),
    Column("file_id", SMALLINT),
    Column("table_name", CHAR(128)),
    Column("table_type", CHAR(10)),
    Column("creator", Integer),
    #schema="information_schema"
    )

domains = Table("SYSDOMAIN", ischema,
    Column("domain_id", Integer, primary_key=True),
    Column("domain_name", CHAR(128)),
    Column("type_id", SMALLINT),
    Column("precision", SMALLINT, quote=True),
    #schema="information_schema"
    )

columns = Table("SYSCOLUMN", ischema,
    Column("column_id", Integer, primary_key=True),
    Column("table_id", Integer, ForeignKey(tables.c.table_id)),
    Column("pkey", CHAR(1)),
    Column("column_name", CHAR(128)),
    Column("nulls", CHAR(1)),
    Column("width", SMALLINT),
    Column("domain_id", SMALLINT, ForeignKey(domains.c.domain_id)),
    # FIXME: should be mx.BIGINT
    Column("max_identity", Integer),
    # FIXME: should be mx.ODBC.Windows.LONGVARCHAR
    Column("default", String),
    Column("scale", Integer),
    #schema="information_schema"
    )

foreignkeys = Table("SYSFOREIGNKEY", ischema,
    Column("foreign_table_id", Integer, ForeignKey(tables.c.table_id), primary_key=True),
    Column("foreign_key_id", SMALLINT, primary_key=True),
    Column("primary_table_id", Integer, ForeignKey(tables.c.table_id)),
    #schema="information_schema"
    )
fkcols = Table("SYSFKCOL", ischema,
    Column("foreign_table_id", Integer, ForeignKey(columns.c.table_id), primary_key=True),
    Column("foreign_key_id", SMALLINT, ForeignKey(foreignkeys.c.foreign_key_id), primary_key=True),
    Column("foreign_column_id", Integer, ForeignKey(columns.c.column_id), primary_key=True),
    Column("primary_column_id", Integer),
    #schema="information_schema"
    )

