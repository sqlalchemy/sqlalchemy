Installation
=================

.. contents::
    :local:
    :class: faq
    :backlinks: none

.. _faq_asyncio_installation:

I'm getting an error about greenlet not being installed when I try to use asyncio
----------------------------------------------------------------------------------

The ``greenlet`` dependency does not install by default for CPU architectures
for which ``greenlet`` does not supply a `pre-built binary wheel <https://pypi.org/project/greenlet/#files>`_.
Notably, **this includes Apple M1**.    To install including ``greenlet``,
add the ``asyncio`` `setuptools extra <https://packaging.python.org/en/latest/tutorials/installing-packages/#installing-setuptools-extras>`_
to the ``pip install`` command:

.. sourcecode:: text

    pip install sqlalchemy[asyncio]

For more background, see :ref:`asyncio_install`.


.. seealso::

    :ref:`asyncio_install`


