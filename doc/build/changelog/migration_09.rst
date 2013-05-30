==============================
What's New in SQLAlchemy 0.9?
==============================

.. admonition:: About this Document

    This document describes changes between SQLAlchemy version 0.8,
    undergoing maintenance releases as of May, 2013,
    and SQLAlchemy version 0.9, which is expected for release
    in late 2013.

    Document date: May 29, 2013

Introduction
============

This guide introduces what's new in SQLAlchemy version 0.9,
and also documents changes which affect users migrating
their applications from the 0.8 series of SQLAlchemy to 0.9.

Version 0.9 is a faster-than-usual push from version 0.8,
featuring a more versatile codebase with regards to modern
Python versions.   The upgrade path at the moment requires no changes
to user code, however this is subject to change.

Platform Support
================

Targeting Python 2.6 and Up Now, Python 3 without 2to3
-------------------------------------------------------

The first achievement of the 0.9 release is to remove the dependency
on the 2to3 tool for Python 3 compatibility.  To make this
more straightforward, the lowest Python release targeted now
is 2.6, which features a wide degree of cross-compatibility with
Python 3.   All SQLAlchemy modules and unit tests are now interpreted
equally well with any Python interpreter from 2.6 forward, including
the 3.1 and 3.2 interpreters.

At the moment, the C extensions are still not fully ported to
Python 3.
