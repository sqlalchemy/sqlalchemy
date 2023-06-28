import sqlalchemy as sa

Book = sa.table(
    "book",
    sa.column("id", sa.Integer),
    sa.column("name", sa.String),
)
Book.append_column(sa.column("other"))
Book.corresponding_column(Book.c.id)

value_expr = sa.values(
    sa.column("id", sa.Integer), sa.column("name", sa.String), name="my_values"
).data([(1, "name1"), (2, "name2"), (3, "name3")])

sa.select(Book)
sa.select(sa.literal_column("42"), sa.column("foo")).select_from(sa.table("t"))
