# PYTHON_VERSION >= 3.10

import sqlalchemy as sa

Book = sa.table(
    "book",
    sa.column("id", sa.Integer),
    sa.column("name", sa.String),
)
Book.append_column(sa.column("other"))
Book.corresponding_column(Book.c.id)

values = sa.values(
    sa.column("id", sa.Integer), sa.column("name", sa.String), name="my_values"
)
value_expr = values.data([(1, "name1"), (2, "name2"), (3, "name3")])

data: list[tuple[int, str]] = [(1, "name1"), (2, "name2"), (3, "name3")]
value_expr2 = values.data(data)

sa.select(Book)
sa.select(sa.literal_column("42"), sa.column("foo")).select_from(sa.table("t"))
