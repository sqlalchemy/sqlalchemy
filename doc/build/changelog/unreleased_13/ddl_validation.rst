.. change::
   :tags: bug, sql
   :tickets: 4481

   Added "SQL phrase validation" to key DDL phrases that are accepted as plain
   strings, including :paramref:`.ForeignKeyConstraint.on_delete`,
   :paramref:`.ForeignKeyConstraint.on_update`,
   :paramref:`.ExcludeConstraint.using`,
   :paramref:`.ForeignKeyConstraint.initially`, for areas where a series of SQL
   keywords only are expected.Any non-space characters that suggest the phrase
   would need to be quoted will raise a :class:`.CompileError`.   This change
   is related to the series of changes committed as part of :ticket:`4481`.
