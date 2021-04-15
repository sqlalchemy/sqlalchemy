.. change::
    :tags: bug, regression, schema
    :tickets: 6282

    Fixed issue in the ``SchemaEventTarget._set_parent`` method that was
    missing ``**kw`` in its argument signature which was added to this private
    method as part of :ticket:`6152`. The method is not invoked internally, but
    could potentially be used by a third party system, as well as that the lack
    of a proper signature could be misleading in debugging other issues.

