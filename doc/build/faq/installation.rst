Installation
=================

.. contents::
    :local:
    :class: faq
    :backlinks: none

.. _faq_asyncio_installation:

I'm getting an error about greenlet not being installed when I try to use asyncio
----------------------------------------------------------------------------------

The ``greenlet`` dependency is not install by default in the 2.1 series.
To install including ``greenlet``, you need to add the ``asyncio``
`setuptools extra <https://packaging.python.org/en/latest/tutorials/installing-packages/#installing-setuptools-extras>`_
to the ``pip install`` command:

.. sourcecode:: text

    pip install sqlalchemy[asyncio]

For more background, see :ref:`asyncio_install`.


.. seealso::

    :ref:`asyncio_install`


