.. change::
    :tags: general, changed

    The :meth:`_orm.Query.instances` method is deprecated.  The behavioral
    contract of this method, which is that it can iterate objects through
    arbitrary result sets, is long obsolete and no longer tested.
    Arbitrary statements can return objects by using constructs such
    as :meth`.Select.from_statement` or :func:`_orm.aliased`.