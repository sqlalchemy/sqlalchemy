<%flags>inherit='document_base.myt'</%flags>
<&|doclib.myt:item, name="sql", description="Constructing SQL Queries via Python Expressions" &>

    <&|doclib.myt:item, name="select", description="Simple Select" &>

        <&|doclib.myt:item, name="columns", description="Table/Column Specification" &>
        </&>

        <&|doclib.myt:item, name="whereclause", description="WHERE Clause" &>

            <&|doclib.myt:item, name="operators", description="Operators" &>
            </&>

        </&>

        <&|doclib.myt:item, name="orderby", description="Order By" &>
        </&>

    </&>

    <&|doclib.myt:item, name="join", description="Inner and Outer Joins" &>
    </&>
    <&|doclib.myt:item, name="alias", description="Table Aliases" &>
    </&>
    <&|doclib.myt:item, name="subqueries", description="Subqueries" &>
        <&|doclib.myt:item, name="fromclause", description="Subqueries as FROM Clauses" &>
        </&>
        <&|doclib.myt:item, name="correlated", description="Correlated Subqueries" &>
        </&>
        <&|doclib.myt:item, name="exists", description="EXISTS Clauses" &>
        </&>
    </&>
    <&|doclib.myt:item, name="unions", description="Unions" &>
    </&>
    <&|doclib.myt:item, name="bindparams", description="Custom Bind Parameters" &>
    </&>
    <&|doclib.myt:item, name="textual", description="Literal Text Blocks" &>
    </&>
    <&|doclib.myt:item, name="insert", description="Inserts" &>
    </&>
    <&|doclib.myt:item, name="update", description="Updates" &>
        <&|doclib.myt:item, name="correlated", description="Correlated Updates" &>
        </&>
    </&>
    <&|doclib.myt:item, name="delete", description="Deletes" &>
    </&>
    <&|doclib.myt:item, name="precompile", description="Compiled Query Objects" &>
    </&>
</&>