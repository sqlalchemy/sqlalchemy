from sqlalchemy.dialects.oracle import base, cx_oracle, zxjdbc

base.dialect = cx_oracle.dialect
