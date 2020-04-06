Individual per-changelog files go here in `.rst` format, which are pulled in by
changelog (https://github.com/sqlalchemyorg/changelog; version 0.4.0 or higher) 
to be rendered into the `changelog_xx.rst` file.

At release time, the files in `unreleased_xx/`` are removed and written directly
into the changelog.

Rationale is so that multiple changes being merged into gerrit don't produce
conflicts.   Note that gerrit does not support custom merge handlers unlike
git itself.

Each changelog file should be named `{ID}.rst`, wherein `ID` is the unique
identifier of the issue in the issue tracker

In the example below, the `tags` and `tickets` contain a comma-separated listing
because there are more than one element.

================================================================================
								Example Below
================================================================================


.. change::
    :tags: sql, orm
    :tickets: 1, 2, 3

    Please use reStructuredText and Sphinx markup when possible.  For example
    method :meth:`.Index.create` and parameter :paramref:`.Index.create.checkfirst`, 
    and :class:`.Table` which will subject to the relevant markup.  Also please
    note the indentions required for the text.
