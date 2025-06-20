Individual per-changelog files go here
in .rst format, which are pulled in by
changelog (version 0.4.0 or higher) to
be rendered into the changelog_xx.rst file.
At release time, the files here are removed and written
directly into the changelog.

Rationale is so that multiple changes being merged
into gerrit don't produce conflicts.   Note that
gerrit does not support custom merge handlers unlike
git itself.

