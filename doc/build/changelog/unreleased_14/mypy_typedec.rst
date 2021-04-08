.. change::
    :tags: bug, mypy

    Fixed issue in Mypy plugin where the plugin wasn’t inferring the correct 
    type for columns of subclasses that don’t directly descend from 
    ``TypeEngine``, in particular that of  ``TypeDecorator`` and 
    ``UserDefinedType``.
