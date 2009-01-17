from sqlalchemy.dialects.oracle import base, cx_oracle

base.dialect = cx_oracle.dialect