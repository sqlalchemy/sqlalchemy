---
name: Bug report
about: Create a report to help us improve
title: ''
labels: requires triage
assignees: ''

---

**GUIDELINES FOR REPORTING BUGS**

If you are new to SQLAlchemy bug reports, please review our many examples
of [well written bug reports](https://github.com/sqlalchemy/sqlalchemy/issues?q=is%3Aissue+label%3A%22great+mcve%22).   Each of these reports include the following features:

1. a **succinct description of the problem** - typically a line or two at most
2. As SQLAlchemy provides an intricate, fine-grained API around the concepts of
   SQL, database connections, and object persistence, we very much need to
   have **code to reproduce the problem**, otherwise known as
   a [Minimal, Complete, and Verifiable](http://stackoverflow.com/help/mcve)
   example.  This is code we can actually run, so that
   we can understand the issue ourselves, as well as that you, the poster,
   has isolated the issue from your own code.   This upfront effort to
   isolate and demonstrate issues is essentially
   **your contribution to the project**, we use these cases to create real
   test cases, and we appreciate them very much!
3. **complete stack traces for all errors** - error messages without context
   are usually not enough information.  Often we can diagnose
   complex issues just from a complete stack trace alone
4. Other things as applicable:   **SQL log output**, see [logging](https://docs.sqlalchemy.org/en/14/core/engines.html#configuring-logging) for background on this, **database backend and DBAPI driver**,
   **operating system**, **comparative performance timings** for
   performance issues.


**Describe the bug**
<!-- A clear and concise description of what the bug is. -->

**To Reproduce**
Provide your [Minimal, Complete, and Verifiable](http://stackoverflow.com/help/mcve) example
here.

```py
# Insert code here
```

**Error**
Provide the complete text of any errors received **including the complete
stack trace**.   If the message is a warning, run your program with the
``-Werror`` flag:   ``python -Werror myprogram.py``

```
# Copy complete stack trace and error message here, including SQL log output
if applicable.
```

**Versions.**
 - OS:
 - Python:
 - SQLAlchemy:
 - Database:
 - DBAPI:

**Additional context**
<!-- Add any other context about the problem here. -->

**Have a nice day!**
