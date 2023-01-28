.. change::
    :tags: bug, sql
    :tickets: 7664

    Corrected the fix for :ticket:`7664`, released in version 2.0.0, to also
    include :class:`.DropSchema` which was inadvertently missed in this fix,
    allowing stringification without a dialect. The fixes for both constructs
    is backported to the 1.4 series as of 1.4.47.

