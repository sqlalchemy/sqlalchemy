from sqlalchemy.dialects.postgresql import base, psycopg2, pg8000, zxjdbc

base.dialect = psycopg2.dialect