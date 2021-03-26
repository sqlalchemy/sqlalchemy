import enum


class MatchExpressionModifier(enum.Enum):
    in_natural_language_mode = 'IN NATURAL LANGUAGE MODE'

    in_natural_language_mode_with_query_expansion = \
        'IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION'

    in_boolean_mode = 'IN BOOLEAN MODE'
    with_query_expansion = 'WITH QUERY EXPANSION'
