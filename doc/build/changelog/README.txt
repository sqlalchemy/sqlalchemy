Individual per-changelog files are placed into their corresponding release's
directory (for example: changelog files for the `1.4` branch are placed in the
`./unreleased_14/` directory, such as `./unreleased_14/4710.rst`).

Files are in `.rst` format, which are pulled in by
changelog (https://github.com/sqlalchemyorg/changelog; version 0.4.0 or higher)
to be rendered into their corresponding `changelog_xx.rst` file
(for example: `./changelog_14.rst`). At release time, the files in the
`unreleased_xx/` directory are removed and written directly into the changelog.

Rationale is so that multiple changes being merged into Gerrit don't produce
conflicts.   Note that Gerrit does not support alternate merge handlers unlike
git itself (and the alternate merge handlers don't work that well either).

Each changelog file should be named `{ID}.rst`, wherein `ID` is the unique
identifier of the issue in the Github issue tracker.

In the example below, the `tags` and `tickets` contain a comma-separated listing
because there are more than one element.

================================================================================
								Example Below
================================================================================


.. change::
    :tags: bug, sql, orm
    :tickets: 4839, 3257

    Please use reStructuredText and Sphinx markup when possible.  For example
    method :meth:`.Index.create` and parameter
    :paramref:`.Index.create.checkfirst`, and :class:`.Table` which will
    subject to the relevant markup.  Also please note the indentions required
    for the text.
