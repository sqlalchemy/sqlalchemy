from sqlalchemy import Table, MetaData, Column, ForeignKey, String, Integer

ischema = MetaData()
 
schemata = Table("SCHEMATA", ischema,
    Column("CATALOG_NAME", String, key="catalog_name"),
    Column("SCHEMA_NAME", String, key="schema_name"),
    Column("SCHEMA_OWNER", String, key="schema_owner"),
    schema="INFORMATION_SCHEMA")

tables = Table("TABLES", ischema,
    Column("TABLE_CATALOG", String, key="table_catalog"),
    Column("TABLE_SCHEMA", String, key="table_schema"),
    Column("TABLE_NAME", String, key="table_name"),
    Column("TABLE_TYPE", String, key="table_type"),
    schema="INFORMATION_SCHEMA")

columns = Table("COLUMNS", ischema,
    Column("TABLE_SCHEMA", String, key="table_schema"),
    Column("TABLE_NAME", String, key="table_name"),
    Column("COLUMN_NAME", String, key="column_name"),
    Column("IS_NULLABLE", Integer, key="is_nullable"),
    Column("DATA_TYPE", String, key="data_type"),
    Column("ORDINAL_POSITION", Integer, key="ordinal_position"),
    Column("CHARACTER_MAXIMUM_LENGTH", Integer, key="character_maximum_length"),
    Column("NUMERIC_PRECISION", Integer, key="numeric_precision"),
    Column("NUMERIC_SCALE", Integer, key="numeric_scale"),
    Column("COLUMN_DEFAULT", Integer, key="column_default"),
    Column("COLLATION_NAME", String, key="collation_name"),
    schema="INFORMATION_SCHEMA")

constraints = Table("TABLE_CONSTRAINTS", ischema,
    Column("TABLE_SCHEMA", String, key="table_schema"),
    Column("TABLE_NAME", String, key="table_name"),
    Column("CONSTRAINT_NAME", String, key="constraint_name"),
    Column("CONSTRAINT_TYPE", String, key="constraint_type"),
    schema="INFORMATION_SCHEMA")

column_constraints = Table("CONSTRAINT_COLUMN_USAGE", ischema,
    Column("TABLE_SCHEMA", String, key="table_schema"),
    Column("TABLE_NAME", String, key="table_name"),
    Column("COLUMN_NAME", String, key="column_name"),
    Column("CONSTRAINT_NAME", String, key="constraint_name"),
    schema="INFORMATION_SCHEMA")

key_constraints = Table("KEY_COLUMN_USAGE", ischema,
    Column("TABLE_SCHEMA", String, key="table_schema"),
    Column("TABLE_NAME", String, key="table_name"),
    Column("COLUMN_NAME", String, key="column_name"),
    Column("CONSTRAINT_NAME", String, key="constraint_name"),
    Column("ORDINAL_POSITION", Integer, key="ordinal_position"),
    schema="INFORMATION_SCHEMA")

ref_constraints = Table("REFERENTIAL_CONSTRAINTS", ischema,
    Column("CONSTRAINT_CATALOG", String, key="constraint_catalog"),
    Column("CONSTRAINT_SCHEMA", String, key="constraint_schema"),
    Column("CONSTRAINT_NAME", String, key="constraint_name"),
    Column("UNIQUE_CONSTRAINT_CATLOG", String, key="unique_constraint_catalog"),
    Column("UNIQUE_CONSTRAINT_SCHEMA", String, key="unique_constraint_schema"),
    Column("UNIQUE_CONSTRAINT_NAME", String, key="unique_constraint_name"),
    Column("MATCH_OPTION", String, key="match_option"),
    Column("UPDATE_RULE", String, key="update_rule"),
    Column("DELETE_RULE", String, key="delete_rule"),
    schema="INFORMATION_SCHEMA")

