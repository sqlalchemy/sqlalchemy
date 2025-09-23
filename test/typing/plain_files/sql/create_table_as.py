"""Typing tests for CREATE TABLE AS."""

from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy.sql.ddl import CreateTableAs

# Setup
metadata = MetaData()
users = Table(
    "users",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("name", String(50)),
    Column("email", String(100)),
    Column("status", String(20)),
)

# Test 1: Basic CreateTableAs with string table name
stmt1 = select(users.c.id, users.c.name).where(users.c.id > 10)
ctas1 = CreateTableAs(stmt1, "active_users")

# Test 2: CreateTableAs with MetaData (creates Table object)
ctas2 = CreateTableAs(stmt1, "active_users_table", metadata=metadata)

# Test 3: Using .into() method on Select
ctas3 = stmt1.into("users_copy")

# Test 4: With schema parameter
ctas4 = CreateTableAs(stmt1, "users_backup", schema="backup")

# Test 5: With temporary flag
ctas5 = CreateTableAs(stmt1, "temp_users", temporary=True)

# Test 6: With if_not_exists flag
ctas6 = CreateTableAs(stmt1, "users_safe", if_not_exists=True)

# Test 7: Combining flags
ctas7 = CreateTableAs(
    stmt1, "temp_backup", temporary=True, if_not_exists=True, schema="temp"
)

# Test 8: Access table property
dest_table1 = ctas1.table
dest_table2 = ctas2.table

# Test 9: Access columns from generated table
id_column = dest_table1.c.id
name_column = dest_table1.c.name

# Test 10: Use generated table in another select
new_select = select(dest_table1.c.id, dest_table1.c.name).where(
    dest_table1.c.id < 100
)

# Test 11: With column labels
labeled_stmt = select(
    users.c.id.label("user_id"),
    users.c.name.label("user_name"),
    users.c.email.label("user_email"),
)
ctas_labeled = CreateTableAs(labeled_stmt, "labeled_users")
labeled_table = ctas_labeled.table
user_id_col = labeled_table.c.user_id
user_name_col = labeled_table.c.user_name

# Test 12: With WHERE clause
filtered_stmt = select(users.c.id, users.c.status).where(
    users.c.status == "active"
)
ctas_filtered = CreateTableAs(filtered_stmt, "active_status")

# Test 13: With JOIN
orders = Table(
    "orders",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("user_id", Integer),
    Column("amount", Integer),
)
join_stmt = select(users.c.id, users.c.name, orders.c.amount).select_from(
    users.join(orders, users.c.id == orders.c.user_id)
)
ctas_join = CreateTableAs(join_stmt, "user_orders")

# Test 14: With UNION
stmt_a = select(users.c.id, users.c.name).where(users.c.status == "active")
stmt_b = select(users.c.id, users.c.name).where(users.c.status == "pending")
union_stmt = stmt_a.union(stmt_b)
ctas_union = CreateTableAs(union_stmt, "combined_users")

# Test 15: .into() with metadata
ctas_into_meta = stmt1.into("users_copy_meta", metadata=metadata)

# Test 16: .into() with all options
ctas_into_full = stmt1.into(
    "full_copy", metadata=metadata, schema="backup", temporary=True
)

# Test 17: Verify generated table can be used in expressions
generated = ctas1.table
count_stmt = select(generated.c.id).where(generated.c.id > 5)

# Test 18: Chained operations
final_stmt = (
    select(users.c.id, users.c.name)
    .where(users.c.status == "active")
    .into("final_result")
)
final_table = final_stmt.table
final_id = final_table.c.id
