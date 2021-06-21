from sqlalchemy.ext.declarative import declarative_base


class CustomBase:
    x = 5


sql_base = declarative_base(cls=CustomBase)
