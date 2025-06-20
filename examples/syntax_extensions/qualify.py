from __future__ import annotations

from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import ClauseElement
from sqlalchemy.sql import coercions
from sqlalchemy.sql import ColumnElement
from sqlalchemy.sql import ColumnExpressionArgument
from sqlalchemy.sql import roles
from sqlalchemy.sql import Select
from sqlalchemy.sql import SyntaxExtension
from sqlalchemy.sql import visitors


def qualify(predicate: ColumnExpressionArgument[bool]) -> Qualify:
    """Return a QUALIFY construct

    E.g.::

        stmt = select(qt_table).ext(
            qualify(func.row_number().over(order_by=qt_table.c.o))
        )

    """
    return Qualify(predicate)


class Qualify(SyntaxExtension, ClauseElement):
    """Define the QUALIFY class."""

    predicate: ColumnElement[bool]
    """A single column expression that is the predicate within the QUALIFY."""

    _traverse_internals = [
        ("predicate", visitors.InternalTraversal.dp_clauseelement)
    ]
    """This structure defines how SQLAlchemy can do a deep traverse of internal
    contents of this structure.  This is mostly used for cache key generation.
    If the traversal is not written yet, the ``inherit_cache=False`` class
    level attribute may be used to skip caching for the construct.
    """

    def __init__(self, predicate: ColumnExpressionArgument):
        self.predicate = coercions.expect(
            roles.WhereHavingRole, predicate, apply_propagate_attrs=self
        )

    def apply_to_select(self, select_stmt: Select) -> None:
        """Called when the :meth:`.Select.ext` method is called.

        The extension should apply itself to the :class:`.Select`, typically
        using :meth:`.HasStatementExtensions.apply_syntax_extension_point`,
        which receives a callable that receives a list of current elements to
        be concatenated together and then returns a new list of elements to be
        concatenated together in the final structure.  The
        :meth:`.SyntaxExtension.append_replacing_same_type` callable is
        usually used for this.

        """
        select_stmt.apply_syntax_extension_point(
            self.append_replacing_same_type, "post_criteria"
        )


@compiles(Qualify)
def _compile_qualify(element, compiler, **kw):
    """a compiles extension that delivers the SQL text for Qualify"""
    return f"QUALIFY {compiler.process(element.predicate, **kw)}"
