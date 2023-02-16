.. change::
    :tags: bug, orm

    Marked the internal ``EvaluatorCompiler`` module as private to the ORM, and
    renamed it to ``_EvaluatorCompiler``. For users that may have been relying
    upon this, the name ``EvaluatorCompiler`` is still present, however this
    use is not supported and will be removed in a future release.
